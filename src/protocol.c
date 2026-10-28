#include "protocol.h"

#include <string.h>
#include <strings.h>

#define LIT(s) s, sizeof(s) - 1

typedef struct {
    const char *p;
    size_t n;
} span_t;

static int is_sep(char c)
{
    return c == ' ' || c == '\t';
}

/* Skip separators, then return the next run of non-separator bytes. */
static span_t next_token(const char **pp, const char *end)
{
    const char *p = *pp;
    while (p < end && is_sep(*p))
        p++;
    const char *start = p;
    while (p < end && !is_sep(*p))
        p++;
    *pp = p;
    return (span_t){ start, (size_t)(p - start) };
}

static int only_blanks(const char *p, const char *end)
{
    while (p < end && is_sep(*p))
        p++;
    return p == end;
}

static int cmd_is(span_t cmd, const char *name)
{
    size_t n = strlen(name);
    return cmd.n == n && strncasecmp(cmd.p, name, n) == 0;
}

struct get_ctx {
    buf_t *out;
    int oom;
};

/* Runs under the stripe read lock: copy straight into the output buffer. */
static void emit_value(void *ctx, const char *val, size_t vlen)
{
    struct get_ctx *g = ctx;
    if (buf_reserve(g->out, sizeof("VALUE \n") - 1 + vlen) < 0) {
        g->oom = 1;
        return;
    }
    buf_append(g->out, LIT("VALUE "));
    buf_append(g->out, val, vlen);
    buf_append(g->out, LIT("\n"));
}

/* Execute one request line (terminator already stripped).
 * Returns 0 to continue, 1 if the connection should close. */
static int execute(hashtable_t *t, const char *line, size_t n, buf_t *out)
{
    const char *p = line, *end = line + n;
    span_t cmd = next_token(&p, end);
    int rc = 0;

    if (cmd.n == 0)
        return 0; /* blank line: ignore, handy for interactive use */

    if (cmd_is(cmd, "GET") || cmd_is(cmd, "DEL")) {
        span_t key = next_token(&p, end);
        if (key.n == 0 || !only_blanks(p, end)) {
            rc = buf_append(out, LIT("ERR wrong number of arguments\n"));
        } else if (cmd_is(cmd, "GET")) {
            struct get_ctx g = { out, 0 };
            if (!ht_get(t, key.p, key.n, emit_value, &g))
                rc = buf_append(out, LIT("NOT_FOUND\n"));
            else if (g.oom)
                rc = -1;
        } else if (ht_del(t, key.p, key.n)) {
            rc = buf_append(out, LIT("DELETED\n"));
        } else {
            rc = buf_append(out, LIT("NOT_FOUND\n"));
        }
    } else if (cmd_is(cmd, "SET")) {
        span_t key = next_token(&p, end);
        if (key.n == 0 || p == end) {
            rc = buf_append(out, LIT("ERR wrong number of arguments\n"));
        } else {
            p++; /* exactly one separator; the value is the rest, verbatim */
            if (ht_set(t, key.p, key.n, p, (size_t)(end - p)) < 0)
                rc = -1;
            else
                rc = buf_append(out, LIT("OK\n"));
        }
    } else if (cmd_is(cmd, "PING")) {
        if (only_blanks(p, end))
            rc = buf_append(out, LIT("PONG\n"));
        else
            rc = buf_append(out, LIT("ERR wrong number of arguments\n"));
    } else if (cmd_is(cmd, "QUIT")) {
        buf_append(out, LIT("BYE\n"));
        return 1;
    } else {
        rc = buf_append(out, LIT("ERR unknown command\n"));
    }

    if (rc < 0) {
        /* Best effort: this tiny append may still succeed. */
        buf_append(out, LIT("ERR out of memory\n"));
        return 1;
    }
    return 0;
}

size_t proto_process(hashtable_t *t, const char *in, size_t len,
                     buf_t *out, int *close_after)
{
    size_t pos = 0;
    *close_after = 0;

    while (pos < len && out->len < PROTO_OUT_HIGH_WATER) {
        const char *start = in + pos;
        size_t rest = len - pos;
        const char *nl = memchr(start, '\n', rest);

        if (!nl) {
            /* Even if "\n" arrived next, the line would exceed the limit. */
            if (rest > PROTO_MAX_LINE + 1) {
                buf_append(out, LIT("ERR line too long\n"));
                *close_after = 1;
            }
            break;
        }

        size_t n = (size_t)(nl - start);
        pos += n + 1;
        if (n > 0 && start[n - 1] == '\r')
            n--;

        if (n > PROTO_MAX_LINE) {
            buf_append(out, LIT("ERR line too long\n"));
            *close_after = 1;
            break;
        }
        if (execute(t, start, n, out)) {
            *close_after = 1;
            break;
        }
    }
    return pos;
}
