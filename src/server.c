/*
 * server.c - thread-pool key-value server.
 *
 * The main thread is the acceptor: it poll()s the listening socket and a
 * signalfd, and hands each accepted connection to the pool's work queue.
 * A worker then owns that connection with plain blocking I/O until the
 * client disconnects, so at most `-t` connections are served concurrently
 * and the rest wait in the queue (or are refused with "ERR server busy").
 *
 * Shutdown (SIGINT/SIGTERM): stop accepting, shutdown(SHUT_RD) every live
 * connection so blocked recv() calls return, answer requests that were
 * already received, drain the queue the same way, join, free everything.
 */
#include <errno.h>
#include <getopt.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "buffer.h"
#include "hashtable.h"
#include "net.h"
#include "protocol.h"
#include "threadpool.h"

#define DEFAULT_PORT 7379
#define DEFAULT_THREADS 8
#define DEFAULT_QUEUE 1024
#define DEFAULT_IDLE_SEC 60

static struct {
    hashtable_t *table;

    /* Live connections, one slot per worker, so shutdown can wake them. */
    pthread_mutex_t reg_lock;
    int *active;
    size_t nslots;
    int stopping;

    atomic_ulong accepted;
    atomic_ulong rejected;
} srv;

/* Record fd as live. If shutdown already began, stop reading from it right
 * away; the worker still answers whatever the client already sent. */
static void conn_register(int fd)
{
    int stopping;
    pthread_mutex_lock(&srv.reg_lock);
    for (size_t i = 0; i < srv.nslots; i++) {
        if (srv.active[i] < 0) {
            srv.active[i] = fd;
            break;
        }
    }
    stopping = srv.stopping;
    pthread_mutex_unlock(&srv.reg_lock);
    if (stopping)
        shutdown(fd, SHUT_RD);
}

/* Must run before close(fd), so shutdown never touches a reused fd number. */
static void conn_unregister(int fd)
{
    pthread_mutex_lock(&srv.reg_lock);
    for (size_t i = 0; i < srv.nslots; i++) {
        if (srv.active[i] == fd) {
            srv.active[i] = -1;
            break;
        }
    }
    pthread_mutex_unlock(&srv.reg_lock);
}

/* Worker job: serve one client until EOF, error, QUIT or idle timeout. */
static void handle_conn(void *arg)
{
    int fd = (int)(intptr_t)arg;
    char *in = malloc(PROTO_READ_BUF);
    size_t inlen = 0;
    buf_t out;
    buf_init(&out);
    conn_register(fd);

    while (in) {
        ssize_t n = recv(fd, in + inlen, PROTO_READ_BUF - inlen, 0);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            break; /* EOF, reset, or SO_RCVTIMEO idle timeout */
        inlen += (size_t)n;

        /* Requests may arrive split across reads or many per read; the
         * protocol consumes only complete lines and we keep the tail. */
        size_t off = 0;
        int closing = 0;
        for (;;) {
            size_t used = proto_process(srv.table, in + off, inlen - off,
                                        &out, &closing);
            off += used;
            if (out.len > 0) {
                if (net_send_all(fd, out.data, out.len) < 0)
                    goto done;
                out.len = 0;
            }
            if (closing || used == 0)
                break;
        }
        if (closing)
            break;
        memmove(in, in + off, inlen - off);
        inlen -= off;
    }

done:
    conn_unregister(fd);
    close(fd);
    free(in);
    buf_free(&out);
}

static void set_timeouts(int fd, int seconds)
{
    if (seconds <= 0)
        return;
    struct timeval tv = { .tv_sec = seconds, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "usage: %s [-a addr] [-p port] [-t threads] [-q queue] [-i idle_sec]\n"
            "  -a  bind address           (default 127.0.0.1)\n"
            "  -p  TCP port               (default %d)\n"
            "  -t  worker threads         (default %d)\n"
            "  -q  pending-connection queue capacity (default %d)\n"
            "  -i  idle/send timeout in seconds, 0 = none (default %d)\n",
            prog, DEFAULT_PORT, DEFAULT_THREADS, DEFAULT_QUEUE, DEFAULT_IDLE_SEC);
}

int main(int argc, char **argv)
{
    const char *addr = "127.0.0.1";
    long port = DEFAULT_PORT, nthreads = DEFAULT_THREADS;
    long qcap = DEFAULT_QUEUE, idle = DEFAULT_IDLE_SEC;
    int opt;

    while ((opt = getopt(argc, argv, "a:p:t:q:i:h")) != -1) {
        switch (opt) {
        case 'a': addr = optarg; break;
        case 'p': port = net_parse_long(optarg, 1, 65535); break;
        case 't': nthreads = net_parse_long(optarg, 1, 4096); break;
        case 'q': qcap = net_parse_long(optarg, 1, 1 << 20); break;
        case 'i': idle = net_parse_long(optarg, 0, 86400); break;
        default: usage(argv[0]); return opt == 'h' ? 0 : 2;
        }
        if (port < 0 || nthreads < 0 || qcap < 0 || idle < 0) {
            usage(argv[0]);
            return 2;
        }
    }

    /* A peer that disconnects mid-write must not kill the process. We also
     * pass MSG_NOSIGNAL on every send; this covers anything else. */
    signal(SIGPIPE, SIG_IGN);

    /* Block SIGINT/SIGTERM and receive them synchronously via signalfd in the
     * accept loop: no async-signal-safety concerns, no EINTR races. */
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);
    int sfd = signalfd(-1, &mask, SFD_CLOEXEC);
    if (sfd < 0) {
        perror("signalfd");
        return 1;
    }

    int lfd = net_listen(addr, (int)port, 1024);
    if (lfd < 0)
        return 1;

    srv.table = ht_create(1024, 64);
    srv.nslots = (size_t)nthreads;
    srv.active = malloc(srv.nslots * sizeof *srv.active);
    pthread_mutex_init(&srv.reg_lock, NULL);
    threadpool_t *pool = tp_create((size_t)nthreads, (size_t)qcap);
    if (!srv.table || !srv.active || !pool) {
        fprintf(stderr, "startup failed: out of memory\n");
        return 1;
    }
    for (size_t i = 0; i < srv.nslots; i++)
        srv.active[i] = -1;

    fprintf(stderr, "server: listening on %s:%ld (%ld workers, queue %ld)\n",
            addr, port, nthreads, qcap);

    int backoff = 0;
    for (;;) {
        struct pollfd pfd[2] = {
            { .fd = sfd, .events = POLLIN },
            { .fd = lfd, .events = POLLIN },
        };
        /* While out of fds, ignore the listener briefly instead of spinning. */
        int rc = poll(pfd, backoff ? 1 : 2, backoff ? 100 : -1);
        backoff = 0;
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            perror("poll");
            break;
        }
        if (pfd[0].revents & POLLIN) {
            struct signalfd_siginfo si;
            if (read(sfd, &si, sizeof si) == (ssize_t)sizeof si)
                fprintf(stderr, "server: caught %s, shutting down\n",
                        strsignal((int)si.ssi_signo));
            break;
        }
        if (!(pfd[1].revents & POLLIN))
            continue;

        for (;;) {
            /* Client sockets stay blocking: workers use blocking I/O. */
            int cfd = accept4(lfd, NULL, NULL, SOCK_CLOEXEC);
            if (cfd < 0) {
                if (errno == EMFILE || errno == ENFILE) {
                    perror("accept");
                    backoff = 1;
                }
                break; /* EAGAIN: backlog drained; others: transient */
            }
            net_set_nodelay(cfd);
            set_timeouts(cfd, (int)idle);
            if (tp_submit(pool, handle_conn, (void *)(intptr_t)cfd) == 0) {
                atomic_fetch_add(&srv.accepted, 1);
            } else {
                /* Load shedding: tell the client rather than leave it hanging. */
                atomic_fetch_add(&srv.rejected, 1);
                send(cfd, "ERR server busy\n", 16, MSG_NOSIGNAL | MSG_DONTWAIT);
                close(cfd);
            }
        }
    }

    close(lfd);

    pthread_mutex_lock(&srv.reg_lock);
    srv.stopping = 1;
    size_t live = 0;
    for (size_t i = 0; i < srv.nslots; i++) {
        if (srv.active[i] >= 0) {
            shutdown(srv.active[i], SHUT_RD);
            live++;
        }
    }
    pthread_mutex_unlock(&srv.reg_lock);
    fprintf(stderr, "server: draining %zu active and %zu queued connections\n",
            live, tp_pending(pool));

    tp_shutdown(pool); /* workers finish the queue, then exit; joined here */

    fprintf(stderr, "server: done (%lu connections served, %lu rejected, %zu keys freed)\n",
            atomic_load(&srv.accepted), atomic_load(&srv.rejected),
            ht_count(srv.table));
    ht_destroy(srv.table);
    pthread_mutex_destroy(&srv.reg_lock);
    free(srv.active);
    close(sfd);
    return 0;
}
