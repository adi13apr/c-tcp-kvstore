/*
 * client.c - closed-loop load generator for the key-value servers.
 *
 * Opens -c connections (one thread each). Every thread repeatedly sends a
 * batch of -P requests (a random GET/SET mix over -k keys), waits for all -P
 * responses, and records the batch's round-trip time. All threads start
 * together on a barrier; throughput is total responses / wall time.
 *
 * "First reply" is the time from the start until a connection receives its
 * first response; its worst case exposes connections left waiting (e.g. in
 * the thread-pool server's queue), which per-request latency hides because
 * each connection contributes only one such sample.
 *
 * -I opens extra connections that stay idle for the whole run.
 *
 * Output ends with a machine-readable line used by bench/run_bench.py:
 *   RESULT rps=... p50_us=... p99_us=... p999_us=... max_us=...
 *          first_p50_us=... first_max_us=... errors=...
 */
#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "net.h"

typedef struct {
    const char *host;
    int port;
    long conns;
    long requests; /* per connection */
    long pipeline;
    long get_pct;
    long keyspace;
    long value_size;
    long idle;
    int populate;
} opts_t;

typedef struct {
    int id;
    const opts_t *o;
    pthread_barrier_t *start;
    uint32_t *lat_us; /* one sample per batch */
    long nlat;
    double first_us;  /* start -> first response, or -1 */
    long done;        /* responses received */
    long errors;      /* ERR responses, plus 1 if the connection failed */
} worker_t;

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static int dial(const opts_t *o)
{
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)o->port);
    if (inet_pton(AF_INET, o->host, &sa.sin_addr) != 1)
        return -1;
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0)
        return -1;
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) < 0) {
        close(fd);
        return -1;
    }
    net_set_nodelay(fd);
    return fd;
}

/* Line reader over a blocking socket. */
typedef struct {
    int fd;
    char *buf;
    size_t cap, start, end;
} reader_t;

/* Read one response line; returns its first byte, or -1 on EOF/error. */
static int read_line(reader_t *r)
{
    for (;;) {
        char *nl = memchr(r->buf + r->start, '\n', r->end - r->start);
        if (nl) {
            int first = (unsigned char)r->buf[r->start];
            r->start = (size_t)(nl - r->buf) + 1;
            return first;
        }
        if (r->start > 0) { /* compact the partial line to the front */
            memmove(r->buf, r->buf + r->start, r->end - r->start);
            r->end -= r->start;
            r->start = 0;
        }
        if (r->end == r->cap) {
            char *p = realloc(r->buf, r->cap * 2);
            if (!p)
                return -1;
            r->buf = p;
            r->cap *= 2;
        }
        ssize_t n = recv(r->fd, r->buf + r->end, r->cap - r->end, 0);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            return -1;
        r->end += (size_t)n;
    }
}

static uint64_t xorshift(uint64_t *s)
{
    uint64_t x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return *s = x;
}

/* Append one request; returns the new length. */
static size_t build_request(char *dst, const opts_t *o, uint64_t *rng,
                            const char *value)
{
    uint64_t r = xorshift(rng);
    long key = (long)(r % (uint64_t)o->keyspace);
    if ((long)((r >> 32) % 100) < o->get_pct)
        return (size_t)sprintf(dst, "GET key:%ld\n", key);
    return (size_t)sprintf(dst, "SET key:%ld %s\n", key, value);
}

static void *worker_main(void *arg)
{
    worker_t *w = arg;
    const opts_t *o = w->o;
    size_t req_max = 32 + (size_t)o->value_size;
    char *value = malloc((size_t)o->value_size + 1);
    char *out = malloc(req_max * (size_t)o->pipeline);
    reader_t r = { .fd = -1, .buf = malloc(65536), .cap = 65536 };
    uint64_t rng = 0x9e3779b97f4a7c15ULL * (uint64_t)(w->id + 1);
    int ok = value && out && r.buf;

    if (ok) {
        memset(value, 'v', (size_t)o->value_size);
        value[o->value_size] = '\0';
        r.fd = dial(o);
        ok = r.fd >= 0;
    }
    pthread_barrier_wait(w->start); /* even on failure, or main would hang */
    double started = now_sec();

    for (long sent = 0; ok && sent < o->requests;) {
        long batch = o->pipeline;
        if (batch > o->requests - sent)
            batch = o->requests - sent;
        size_t len = 0;
        for (long i = 0; i < batch; i++)
            len += build_request(out + len, o, &rng, value);

        double t0 = now_sec();
        if (net_send_all(r.fd, out, len) < 0) {
            ok = 0;
            break;
        }
        for (long i = 0; i < batch; i++) {
            int c = read_line(&r);
            if (c < 0) {
                ok = 0;
                break;
            }
            if (c == 'E') /* ERR ... */
                w->errors++;
            w->done++;
        }
        if (!ok)
            break;
        double t1 = now_sec();
        double us = (t1 - t0) * 1e6;
        w->lat_us[w->nlat++] = us > UINT32_MAX ? UINT32_MAX : (uint32_t)us;
        if (w->first_us < 0)
            w->first_us = (t1 - started) * 1e6;
        sent += batch;
    }

    if (!ok)
        w->errors++;
    if (r.fd >= 0)
        close(r.fd);
    free(r.buf);
    free(out);
    free(value);
    return NULL;
}

/* SET every key once so GETs hit, pipelining 1000 at a time. */
static int populate(const opts_t *o)
{
    int fd = dial(o);
    if (fd < 0)
        return -1;
    size_t per = 32 + (size_t)o->value_size;
    char *out = malloc(per * 1000);
    char *value = malloc((size_t)o->value_size + 1);
    reader_t r = { .fd = fd, .buf = malloc(65536), .cap = 65536 };
    int rc = (out && value && r.buf) ? 0 : -1;
    if (value) {
        memset(value, 'v', (size_t)o->value_size);
        value[o->value_size] = '\0';
    }

    for (long k = 0; rc == 0 && k < o->keyspace; k += 1000) {
        long n = o->keyspace - k < 1000 ? o->keyspace - k : 1000;
        size_t len = 0;
        for (long i = 0; i < n; i++)
            len += (size_t)sprintf(out + len, "SET key:%ld %s\n", k + i, value);
        if (net_send_all(fd, out, len) < 0)
            rc = -1;
        for (long i = 0; rc == 0 && i < n; i++)
            if (read_line(&r) != 'O')
                rc = -1;
    }
    close(fd);
    free(r.buf);
    free(out);
    free(value);
    return rc;
}

static int cmp_double(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

static uint32_t pct(const uint32_t *v, size_t n, double p)
{
    if (n == 0)
        return 0;
    size_t i = (size_t)(p / 100.0 * (double)(n - 1) + 0.5);
    return v[i];
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "usage: %s [-h host] [-p port] [-c conns] [-n reqs] [-P depth]\n"
            "          [-r get%%] [-k keys] [-d bytes] [-I idle] [-W]\n"
            "  -h  server IPv4 address           (default 127.0.0.1)\n"
            "  -p  server port                   (default 7379)\n"
            "  -c  concurrent connections        (default 50)\n"
            "  -n  requests per connection       (default 10000)\n"
            "  -P  pipeline depth                (default 1)\n"
            "  -r  percentage of GETs            (default 80)\n"
            "  -k  keyspace size                 (default 10000)\n"
            "  -d  value size in bytes           (default 32)\n"
            "  -I  extra connections held idle   (default 0)\n"
            "  -W  skip pre-populating the keyspace\n",
            prog);
}

int main(int argc, char **argv)
{
    opts_t o = { .host = "127.0.0.1", .port = 7379, .conns = 50,
                 .requests = 10000, .pipeline = 1, .get_pct = 80,
                 .keyspace = 10000, .value_size = 32, .populate = 1 };
    int opt;
    long port = o.port;

    while ((opt = getopt(argc, argv, "h:p:c:n:P:r:k:d:I:W")) != -1) {
        switch (opt) {
        case 'h': o.host = optarg; break;
        case 'p': port = net_parse_long(optarg, 1, 65535); break;
        case 'c': o.conns = net_parse_long(optarg, 1, 20000); break;
        case 'n': o.requests = net_parse_long(optarg, 1, 1L << 40); break;
        case 'P': o.pipeline = net_parse_long(optarg, 1, 10000); break;
        case 'r': o.get_pct = net_parse_long(optarg, 0, 100); break;
        case 'k': o.keyspace = net_parse_long(optarg, 1, 1L << 32); break;
        case 'd': o.value_size = net_parse_long(optarg, 0, 60000); break;
        case 'I': o.idle = net_parse_long(optarg, 0, 20000); break;
        case 'W': o.populate = 0; break;
        default: usage(argv[0]); return 2;
        }
        if (port < 0 || o.conns < 0 || o.requests < 0 || o.pipeline < 0 ||
            o.get_pct < 0 || o.keyspace < 0 || o.value_size < 0 || o.idle < 0) {
            usage(argv[0]);
            return 2;
        }
    }
    o.port = (int)port;
    signal(SIGPIPE, SIG_IGN);

    if (o.populate && populate(&o) < 0) {
        fprintf(stderr, "client: cannot populate %s:%d\n", o.host, o.port);
        return 1;
    }

    /* Connected before the active ones, so a FIFO server sees them first. */
    int *idle = calloc((size_t)o.idle + 1, sizeof *idle);
    for (long i = 0; i < o.idle; i++) {
        if ((idle[i] = dial(&o)) < 0) {
            fprintf(stderr, "client: cannot open idle connection %ld\n", i);
            return 1;
        }
    }

    worker_t *ws = calloc((size_t)o.conns, sizeof *ws);
    pthread_t *th = calloc((size_t)o.conns, sizeof *th);
    long batches = (o.requests + o.pipeline - 1) / o.pipeline;
    pthread_barrier_t start;
    pthread_barrier_init(&start, NULL, (unsigned)o.conns + 1);
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, 256 * 1024); /* allow thousands of conns */

    for (long i = 0; i < o.conns; i++) {
        ws[i] = (worker_t){ .id = (int)i, .o = &o, .start = &start, .first_us = -1,
                            .lat_us = malloc((size_t)batches * sizeof(uint32_t)) };
        if (!ws[i].lat_us || pthread_create(&th[i], &attr, worker_main, &ws[i]) != 0) {
            fprintf(stderr, "client: cannot start connection %ld\n", i);
            return 1;
        }
    }
    pthread_barrier_wait(&start);
    double t0 = now_sec();
    for (long i = 0; i < o.conns; i++)
        pthread_join(th[i], NULL);
    double elapsed = now_sec() - t0;
    for (long i = 0; i < o.idle; i++)
        close(idle[i]);
    free(idle);

    long done = 0, errors = 0;
    size_t nlat = 0;
    for (long i = 0; i < o.conns; i++)
        nlat += (size_t)ws[i].nlat;
    uint32_t *lat = malloc((nlat ? nlat : 1) * sizeof *lat);
    double *first = malloc((size_t)o.conns * sizeof *first);
    size_t off = 0, nfirst = 0;
    for (long i = 0; i < o.conns; i++) {
        if (ws[i].first_us >= 0)
            first[nfirst++] = ws[i].first_us;
        done += ws[i].done;
        errors += ws[i].errors;
        memcpy(lat + off, ws[i].lat_us, (size_t)ws[i].nlat * sizeof *lat);
        off += (size_t)ws[i].nlat;
        free(ws[i].lat_us);
    }
    qsort(lat, nlat, sizeof *lat, cmp_u32);
    qsort(first, nfirst, sizeof *first, cmp_double);
    double first_p50 = nfirst ? first[nfirst / 2] : 0;
    double first_max = nfirst ? first[nfirst - 1] : 0;

    double rps = elapsed > 0 ? (double)done / elapsed : 0;
    uint32_t p50 = pct(lat, nlat, 50), p99 = pct(lat, nlat, 99),
             p999 = pct(lat, nlat, 99.9), mx = nlat ? lat[nlat - 1] : 0;

    printf("target       %s:%d\n", o.host, o.port);
    printf("workload     %ld conns x %ld reqs, pipeline %ld, %ld%% GET, %ld keys, %ldB values",
           o.conns, o.requests, o.pipeline, o.get_pct, o.keyspace, o.value_size);
    if (o.idle)
        printf(", +%ld idle conns", o.idle);
    printf("\n");
    printf("completed    %ld requests in %.3f s\n", done, elapsed);
    printf("throughput   %.0f req/s\n", rps);
    printf("latency      p50 %.3f ms  p99 %.3f ms  p99.9 %.3f ms  max %.3f ms (per round trip)\n",
           p50 / 1e3, p99 / 1e3, p999 / 1e3, mx / 1e3);
    printf("first reply  median %.3f ms  worst %.3f ms (start -> a connection's first response)\n",
           first_p50 / 1e3, first_max / 1e3);
    printf("errors       %ld\n", errors);
    printf("RESULT rps=%.0f p50_us=%u p99_us=%u p999_us=%u max_us=%u "
           "first_p50_us=%.0f first_max_us=%.0f errors=%ld\n",
           rps, p50, p99, p999, mx, first_p50, first_max, errors);

    free(first);
    free(lat);
    free(ws);
    free(th);
    pthread_attr_destroy(&attr);
    pthread_barrier_destroy(&start);
    return errors ? 1 : 0;
}
