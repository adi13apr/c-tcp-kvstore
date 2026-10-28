/* Unit tests for the request parser/executor (no sockets involved). */
#include "protocol.h"
#include "test.h"

#include <stdlib.h>
#include <string.h>

static hashtable_t *T;
static buf_t OUT;

/* Feed `in`, return the output as a NUL-terminated string (static buffer). */
static const char *run(const char *in, size_t *consumed, int *closed)
{
    static char res[4096];
    int c;
    OUT.len = 0;
    size_t n = proto_process(T, in, strlen(in), &OUT, &c);
    if (consumed)
        *consumed = n;
    if (closed)
        *closed = c;
    size_t m = OUT.len < sizeof res - 1 ? OUT.len : sizeof res - 1;
    memcpy(res, OUT.data, m);
    res[m] = '\0';
    return res;
}

#define EXPECT(in, want) CHECK(strcmp(run(in, NULL, NULL), want) == 0)

static void test_commands(void)
{
    EXPECT("PING\n", "PONG\n");
    EXPECT("ping\r\n", "PONG\n");
    EXPECT("GET nope\n", "NOT_FOUND\n");
    EXPECT("SET k v\n", "OK\n");
    EXPECT("GET k\n", "VALUE v\n");
    EXPECT("SET k hello  world \n", "OK\n");
    EXPECT("GET k\n", "VALUE hello  world \n");
    EXPECT("SET empty \n", "OK\n");
    EXPECT("GET empty\n", "VALUE \n");
    EXPECT("DEL k\n", "DELETED\n");
    EXPECT("DEL k\n", "NOT_FOUND\n");
    EXPECT("  get   empty  \n", "VALUE \n");
    EXPECT("\n\r\n", "");
}

static void test_errors(void)
{
    EXPECT("FOO\n", "ERR unknown command\n");
    EXPECT("GET\n", "ERR wrong number of arguments\n");
    EXPECT("GET a b\n", "ERR wrong number of arguments\n");
    EXPECT("DEL\n", "ERR wrong number of arguments\n");
    EXPECT("SET k\n", "ERR wrong number of arguments\n");
    EXPECT("SET\n", "ERR wrong number of arguments\n");
    EXPECT("PING x\n", "ERR wrong number of arguments\n");
}

static void test_pipeline_and_partial(void)
{
    size_t used;
    int closed;
    const char *r = run("SET a 1\nGET a\nGET", &used, &closed);
    CHECK(strcmp(r, "OK\nVALUE 1\n") == 0);
    CHECK(used == strlen("SET a 1\nGET a\n")); /* partial line left over */
    CHECK(closed == 0);

    r = run("GET a", &used, &closed);
    CHECK(r[0] == '\0' && used == 0 && closed == 0);
}

static void test_quit_stops_processing(void)
{
    size_t used;
    int closed;
    const char *r = run("PING\nQUIT\nPING\n", &used, &closed);
    CHECK(strcmp(r, "PONG\nBYE\n") == 0);
    CHECK(closed == 1);
    CHECK(used == strlen("PING\nQUIT\n"));
}

static void test_line_limits(void)
{
    size_t used;
    int closed;
    char *big = malloc(PROTO_MAX_LINE + 16);

    /* exactly at the limit, CRLF terminated: accepted */
    size_t pre = strlen("SET big ");
    memcpy(big, "SET big ", pre);
    memset(big + pre, 'x', PROTO_MAX_LINE - pre);
    strcpy(big + PROTO_MAX_LINE, "\r\n");
    CHECK(strcmp(run(big, &used, &closed), "OK\n") == 0 && closed == 0);
    CHECK(used == PROTO_MAX_LINE + 2);

    /* one byte over, terminated: rejected */
    memset(big + pre, 'x', PROTO_MAX_LINE + 1 - pre);
    strcpy(big + PROTO_MAX_LINE + 1, "\n");
    CHECK(strcmp(run(big, NULL, &closed), "ERR line too long\n") == 0 && closed);

    /* unterminated and already too long: rejected without waiting */
    memset(big, 'y', PROTO_MAX_LINE + 2);
    big[PROTO_MAX_LINE + 2] = '\0';
    CHECK(strcmp(run(big, NULL, &closed), "ERR line too long\n") == 0 && closed);

    /* unterminated but could still fit: wait for more */
    big[PROTO_MAX_LINE + 1] = '\0';
    CHECK(run(big, &used, &closed)[0] == '\0' && used == 0 && closed == 0);
    free(big);
}

static void test_high_water(void)
{
    /* a 1 KiB value fetched many times overflows the high-water mark */
    char val[1024];
    memset(val, 'v', sizeof val);
    ht_set(T, "hw", 2, val, sizeof val);

    size_t reqs = 1000, per = strlen("GET hw\n");
    char *in = malloc(reqs * per + 1);
    for (size_t i = 0; i < reqs; i++)
        memcpy(in + i * per, "GET hw\n", per);
    in[reqs * per] = '\0';

    int closed;
    OUT.len = 0;
    size_t used = proto_process(T, in, reqs * per, &OUT, &closed);
    CHECK(used < reqs * per);
    CHECK(used % per == 0);
    CHECK(OUT.len >= PROTO_OUT_HIGH_WATER);
    CHECK(OUT.len < PROTO_OUT_HIGH_WATER + sizeof val + 16);
    free(in);
}

int main(void)
{
    T = ht_create(64, 8);
    buf_init(&OUT);
    RUN(test_commands);
    RUN(test_errors);
    RUN(test_pipeline_and_partial);
    RUN(test_quit_stops_processing);
    RUN(test_line_limits);
    RUN(test_high_water);
    buf_free(&OUT);
    ht_destroy(T);
    return test_summary("test_protocol");
}
