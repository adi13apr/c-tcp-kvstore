/*
 * epoll_server.c - event-driven key-value server.
 *
 * One or more event-loop threads (-t, default 1), each with its own epoll
 * instance; a connection lives on exactly one loop for its whole life, so
 * per-connection state needs no locking. Loop 0 also owns the non-blocking
 * listening socket and deals accepted connections round-robin: to itself
 * directly, or to another loop by appending the fd to that loop's inbox and
 * waking it through its eventfd. The hash table is the only state the loops
 * share.
 *
 * Epoll is level-triggered and interest is toggled per connection:
 *   EPOLLIN  while the peer may send and pending output is under the
 *            high-water mark (backpressure for clients that don't read);
 *   EPOLLOUT only while a response is partially written.
 *
 * Shutdown (SIGINT/SIGTERM, received by the main thread via sigwait): the
 * main thread signals a shared eventfd; each loop stops accepting and
 * shutdown(SHUT_RD)s its connections so requests already sent are read and
 * answered, then exits once every connection has flushed or a grace period
 * expires.
 */
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "buffer.h"
#include "hashtable.h"
#include "net.h"
#include "protocol.h"

#define DEFAULT_PORT 7379
#define MAX_EVENTS 256
#define ACCEPT_BATCH 64      /* accepts per wakeup before serving others */
#define READ_CHUNK 16384
#define TRIM_CAP (64 * 1024) /* free idle buffers bigger than this */
#define GRACE_MS 3000        /* max time to flush clients on shutdown */

typedef struct conn {
    int fd;
    buf_t in;          /* unparsed input (at most one partial line kept) */
    buf_t out;         /* responses; out.data[out_off..len) is unsent */
    size_t out_off;
    uint32_t events;   /* interest currently registered with epoll */
    int read_eof;      /* peer finished sending (or we shut down reading) */
    int closing;       /* close as soon as output is flushed */
    struct conn *prev, *next;
} conn_t;

typedef struct loop loop_t;

typedef struct {
    loop_t *loops;
    size_t nloops;
    size_t next;       /* round-robin cursor, used only by loop 0 */
    int lfd;
    int stop_fd;
    hashtable_t *table;
} server_t;

struct loop {
    server_t *srv;
    int id;
    int epfd;
    int wake_fd;       /* eventfd: "your inbox has new connections" */
    int spare_fd;      /* reserved fd for shedding connections at EMFILE */

    pthread_mutex_t inbox_lock;
    int *inbox;        /* fds handed over by loop 0, not yet registered */
    size_t inbox_len, inbox_cap;

    conn_t *conns;
    size_t nconns;
    int stopping;
    struct timespec deadline;
    unsigned long accepted;
    pthread_t tid;
};

/* Distinct addresses used as epoll tags for the non-connection fds. */
static char LISTEN_TAG, STOP_TAG, WAKE_TAG;

static size_t out_pending(const conn_t *c)
{
    return c->out.len - c->out_off;
}

static void conn_close(loop_t *lp, conn_t *c)
{
    if (c->prev)
        c->prev->next = c->next;
    else
        lp->conns = c->next;
    if (c->next)
        c->next->prev = c->prev;
    lp->nconns--;

    close(c->fd); /* also removes it from the epoll set */
    buf_free(&c->in);
    buf_free(&c->out);
    free(c);
}

/* Start serving fd on this loop (takes ownership of fd). */
static void conn_add(loop_t *lp, int fd)
{
    conn_t *c = calloc(1, sizeof *c);
    if (!c) {
        close(fd);
        return;
    }
    c->fd = fd;
    buf_init(&c->in);
    buf_init(&c->out);
    c->events = EPOLLIN;
    struct epoll_event ev = { .events = EPOLLIN, .data.ptr = c };
    if (epoll_ctl(lp->epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        close(fd);
        free(c);
        return;
    }
    c->next = lp->conns;
    if (lp->conns)
        lp->conns->prev = c;
    lp->conns = c;
    lp->nconns++;
    lp->accepted++;
    if (lp->stopping)
        shutdown(fd, SHUT_RD); /* handed over just as we began stopping */
}

/* One read per readiness event; level-triggering reports leftovers again.
 * Returns -1 if the connection should be dropped. */
static int conn_read(conn_t *c)
{
    if (c->in.cap - c->in.len < READ_CHUNK &&
        buf_reserve(&c->in, READ_CHUNK) < 0)
        return -1;
    size_t room = c->in.cap - c->in.len;
    if (room > PROTO_READ_BUF - c->in.len)
        room = PROTO_READ_BUF - c->in.len; /* never buffer more than a line */
    if (room == 0)
        return -1; /* unreachable: the parser rejects over-long lines */

    ssize_t n = recv(c->fd, c->in.data + c->in.len, room, 0);
    if (n > 0) {
        c->in.len += (size_t)n;
    } else if (n == 0) {
        c->read_eof = 1;
    } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        return -1; /* ECONNRESET etc. */
    }
    return 0;
}

/* Write as much pending output as the socket takes without blocking. */
static int conn_flush(conn_t *c)
{
    while (c->out_off < c->out.len) {
        ssize_t n = send(c->fd, c->out.data + c->out_off,
                         c->out.len - c->out_off, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return 0;
            return -1; /* EPIPE / ECONNRESET: peer is gone */
        }
        c->out_off += (size_t)n;
    }
    c->out.len = c->out_off = 0;
    return 0;
}

/* Alternate between executing buffered requests and flushing responses
 * until the socket stops accepting output or no complete request remains. */
static int conn_pump(loop_t *lp, conn_t *c)
{
    for (;;) {
        int can_parse = !c->closing && c->out.len < PROTO_OUT_HIGH_WATER;
        size_t used = 0;
        if (can_parse) {
            int close_after;
            used = proto_process(lp->srv->table, c->in.data, c->in.len,
                                 &c->out, &close_after);
            buf_consume(&c->in, used);
            c->closing |= close_after;
        }
        if (conn_flush(c) < 0)
            return -1;
        if (out_pending(c) > 0)
            return 0; /* socket full: resume on EPOLLOUT */
        /* Output drained. If parsing was paused by the high-water mark, go
         * round again: complete requests may still be buffered, and no new
         * EPOLLIN will arrive for them if the client already sent it all. */
        if (c->closing || (can_parse && used == 0))
            return 0;
    }
}

/* Close if finished, otherwise register exactly the interest we need. */
static void conn_update(loop_t *lp, conn_t *c)
{
    if ((c->closing || c->read_eof) && out_pending(c) == 0) {
        conn_close(lp, c);
        return;
    }

    uint32_t want = 0;
    if (!c->read_eof && !c->closing && c->out.len < PROTO_OUT_HIGH_WATER)
        want |= EPOLLIN;
    if (out_pending(c) > 0)
        want |= EPOLLOUT;
    if (want != c->events) {
        struct epoll_event ev = { .events = want, .data.ptr = c };
        if (epoll_ctl(lp->epfd, EPOLL_CTL_MOD, c->fd, &ev) < 0) {
            conn_close(lp, c);
            return;
        }
        c->events = want;
    }

    /* Idle connections shouldn't pin a 64 KiB burst buffer forever. */
    if (c->in.len == 0 && c->in.cap > TRIM_CAP)
        buf_free(&c->in);
    if (c->out.len == 0 && c->out.cap > TRIM_CAP)
        buf_free(&c->out);
}

static void on_conn_event(loop_t *lp, conn_t *c, uint32_t events)
{
    if (events & (EPOLLERR | EPOLLHUP)) {
        conn_close(lp, c); /* reset by peer: nothing left to deliver */
        return;
    }
    if ((events & EPOLLIN) && conn_read(c) < 0) {
        conn_close(lp, c);
        return;
    }
    if (conn_pump(lp, c) < 0) {
        conn_close(lp, c);
        return;
    }
    conn_update(lp, c);
}

/* Queue fd for another loop and wake it. Falls back to serving it here. */
static void hand_off(loop_t *self, loop_t *to, int fd)
{
    int queued = 0;
    pthread_mutex_lock(&to->inbox_lock);
    if (to->inbox_len == to->inbox_cap) {
        size_t cap = to->inbox_cap ? to->inbox_cap * 2 : 64;
        int *p = realloc(to->inbox, cap * sizeof *p);
        if (p) {
            to->inbox = p;
            to->inbox_cap = cap;
        }
    }
    if (to->inbox_len < to->inbox_cap) {
        to->inbox[to->inbox_len++] = fd;
        queued = 1;
    }
    pthread_mutex_unlock(&to->inbox_lock);

    if (!queued) {
        conn_add(self, fd);
        return;
    }
    uint64_t one = 1;
    if (write(to->wake_fd, &one, sizeof one) < 0) {
        /* Counter overflow is impossible in practice; the loop also drains
         * its inbox on every wakeup, so a lost write only delays it. */
    }
}

/* Register every connection loop 0 has handed us. */
static void drain_inbox(loop_t *lp)
{
    uint64_t cnt;
    if (read(lp->wake_fd, &cnt, sizeof cnt) < 0) {
        /* EAGAIN: already reset by an earlier drain */
    }
    pthread_mutex_lock(&lp->inbox_lock);
    int *fds = lp->inbox;
    size_t n = lp->inbox_len;
    lp->inbox = NULL;
    lp->inbox_len = lp->inbox_cap = 0;
    pthread_mutex_unlock(&lp->inbox_lock);

    for (size_t i = 0; i < n; i++)
        conn_add(lp, fds[i]);
    free(fds);
}

static void on_accept(loop_t *lp)
{
    server_t *s = lp->srv;
    for (int i = 0; i < ACCEPT_BATCH; i++) {
        int fd = accept4(s->lfd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if ((errno == EMFILE || errno == ENFILE) && lp->spare_fd >= 0) {
                /* Out of fds: level-triggered epoll would report the
                 * listener forever. Use the reserved fd to accept and
                 * immediately drop one client, then re-reserve. */
                close(lp->spare_fd);
                fd = accept(s->lfd, NULL, NULL);
                if (fd >= 0)
                    close(fd);
                lp->spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
            }
            return; /* EAGAIN: backlog drained */
        }
        net_set_nodelay(fd);

        loop_t *to = &s->loops[s->next];
        s->next = (s->next + 1) % s->nloops;
        if (to == lp)
            conn_add(lp, fd);
        else
            hand_off(lp, to, fd);
    }
}

static long ms_until(const struct timespec *t)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    long ms = (t->tv_sec - now.tv_sec) * 1000 + (t->tv_nsec - now.tv_nsec) / 1000000;
    return ms > 0 ? ms : 0;
}

static void begin_stop(loop_t *lp)
{
    lp->stopping = 1;
    clock_gettime(CLOCK_MONOTONIC, &lp->deadline);
    lp->deadline.tv_sec += GRACE_MS / 1000;

    /* The stop eventfd stays readable (nobody resets it) so every loop sees
     * it; drop it and the listener from our set so they stop waking us. */
    epoll_ctl(lp->epfd, EPOLL_CTL_DEL, lp->srv->stop_fd, NULL);
    if (lp->id == 0)
        epoll_ctl(lp->epfd, EPOLL_CTL_DEL, lp->srv->lfd, NULL);

    /* Reads now return whatever the client already sent, then EOF, so the
     * normal event path answers those requests and closes the connection. */
    drain_inbox(lp);
    for (conn_t *c = lp->conns; c; c = c->next)
        shutdown(c->fd, SHUT_RD);
}

static void *loop_main(void *arg)
{
    loop_t *lp = arg;
    struct epoll_event evs[MAX_EVENTS];

    while (!lp->stopping || (lp->nconns > 0 && ms_until(&lp->deadline) > 0)) {
        int timeout = lp->stopping ? (int)ms_until(&lp->deadline) : -1;
        int n = epoll_wait(lp->epfd, evs, MAX_EVENTS, timeout);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n; i++) {
            void *tag = evs[i].data.ptr;
            if (tag == &STOP_TAG) {
                if (!lp->stopping)
                    begin_stop(lp);
            } else if (tag == &LISTEN_TAG) {
                if (!lp->stopping)
                    on_accept(lp);
            } else if (tag == &WAKE_TAG) {
                drain_inbox(lp);
            } else {
                on_conn_event(lp, tag, evs[i].events);
            }
        }
    }

    if (lp->nconns > 0)
        fprintf(stderr, "epoll_server: loop %d: dropping %zu connections after grace period\n",
                lp->id, lp->nconns);
    while (lp->conns)
        conn_close(lp, lp->conns);
    return NULL;
}

static int epoll_add_tag(int epfd, int fd, void *tag)
{
    struct epoll_event ev = { .events = EPOLLIN, .data.ptr = tag };
    return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
}

static int loop_init(loop_t *lp, server_t *s, int id)
{
    memset(lp, 0, sizeof *lp);
    lp->srv = s;
    lp->id = id;
    pthread_mutex_init(&lp->inbox_lock, NULL);
    lp->spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    lp->wake_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    lp->epfd = epoll_create1(EPOLL_CLOEXEC);
    if (lp->epfd < 0 || lp->wake_fd < 0)
        return -1;
    if (epoll_add_tag(lp->epfd, s->stop_fd, &STOP_TAG) < 0 ||
        epoll_add_tag(lp->epfd, lp->wake_fd, &WAKE_TAG) < 0)
        return -1;
    if (id == 0 && epoll_add_tag(lp->epfd, s->lfd, &LISTEN_TAG) < 0)
        return -1;
    return 0;
}

static void loop_destroy(loop_t *lp)
{
    /* A hand-off can race with the target loop exiting; close leftovers. */
    for (size_t i = 0; i < lp->inbox_len; i++)
        close(lp->inbox[i]);
    free(lp->inbox);
    pthread_mutex_destroy(&lp->inbox_lock);
    if (lp->epfd >= 0)
        close(lp->epfd);
    if (lp->wake_fd >= 0)
        close(lp->wake_fd);
    if (lp->spare_fd >= 0)
        close(lp->spare_fd);
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "usage: %s [-a addr] [-p port] [-t loops]\n"
            "  -a  bind address           (default 127.0.0.1)\n"
            "  -p  TCP port               (default %d)\n"
            "  -t  event-loop threads     (default 1)\n",
            prog, DEFAULT_PORT);
}

int main(int argc, char **argv)
{
    const char *addr = "127.0.0.1";
    long port = DEFAULT_PORT, nloops = 1;
    int opt;

    while ((opt = getopt(argc, argv, "a:p:t:h")) != -1) {
        switch (opt) {
        case 'a': addr = optarg; break;
        case 'p': port = net_parse_long(optarg, 1, 65535); break;
        case 't': nloops = net_parse_long(optarg, 1, 1024); break;
        default: usage(argv[0]); return opt == 'h' ? 0 : 2;
        }
        if (port < 0 || nloops < 0) {
            usage(argv[0]);
            return 2;
        }
    }

    signal(SIGPIPE, SIG_IGN); /* belt and braces; sends use MSG_NOSIGNAL */

    /* Block termination signals everywhere (loop threads inherit the mask);
     * the main thread collects them synchronously with sigwait(). */
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

    server_t s = { .nloops = (size_t)nloops };
    s.lfd = net_listen(addr, (int)port, 1024);
    if (s.lfd < 0)
        return 1;
    s.stop_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    s.table = ht_create(1024, 64);
    s.loops = calloc(s.nloops, sizeof *s.loops);
    if (s.stop_fd < 0 || !s.table || !s.loops) {
        fprintf(stderr, "startup failed\n");
        return 1;
    }

    /* Initialise every loop before starting any: loop 0 may hand a
     * connection to loop N as soon as it runs. */
    int rc = 0;
    size_t inited = 0, started = 0;
    for (; inited < s.nloops; inited++) {
        if (loop_init(&s.loops[inited], &s, (int)inited) < 0) {
            perror("event loop init");
            loop_destroy(&s.loops[inited]);
            rc = 1;
            break;
        }
    }
    for (; rc == 0 && started < s.nloops; started++) {
        if (pthread_create(&s.loops[started].tid, NULL, loop_main,
                           &s.loops[started]) != 0) {
            perror("pthread_create");
            rc = 1;
            break;
        }
    }

    if (rc == 0) {
        fprintf(stderr, "epoll_server: listening on %s:%ld (%ld event loop%s)\n",
                addr, port, nloops, nloops == 1 ? "" : "s");
        int sig;
        sigwait(&mask, &sig);
        fprintf(stderr, "epoll_server: caught %s, shutting down\n", strsignal(sig));
    }

    uint64_t one = 1;
    if (write(s.stop_fd, &one, sizeof one) != (ssize_t)sizeof one)
        perror("eventfd write");

    unsigned long accepted = 0;
    for (size_t i = 0; i < started; i++)
        pthread_join(s.loops[i].tid, NULL);
    for (size_t i = 0; i < inited; i++) {
        accepted += s.loops[i].accepted;
        loop_destroy(&s.loops[i]);
    }

    fprintf(stderr, "epoll_server: done (%lu connections served, %zu keys freed)\n",
            accepted, ht_count(s.table));
    free(s.loops);
    ht_destroy(s.table);
    close(s.stop_fd);
    close(s.lfd);
    return rc;
}
