/* Unit + concurrency tests for the striped hash table. */
#include "hashtable.h"
#include "test.h"

#include <pthread.h>
#include <stdio.h>
#include <string.h>

static void copy_val(void *ctx, const char *v, size_t n)
{
    char *out = ctx;
    memcpy(out, v, n);
    out[n] = '\0';
}

static int get_str(hashtable_t *t, const char *k, char *out)
{
    return ht_get(t, k, strlen(k), copy_val, out);
}

static void test_basic(void)
{
    hashtable_t *t = ht_create(4, 4);
    char v[64];

    CHECK(t != NULL);
    CHECK(ht_count(t) == 0);
    CHECK(get_str(t, "missing", v) == 0);

    CHECK(ht_set(t, "a", 1, "1", 1) == 0);
    CHECK(ht_set(t, "b", 1, "two", 3) == 0);
    CHECK(ht_count(t) == 2);
    CHECK(get_str(t, "a", v) == 1 && strcmp(v, "1") == 0);
    CHECK(get_str(t, "b", v) == 1 && strcmp(v, "two") == 0);

    /* overwrite keeps count and replaces value */
    CHECK(ht_set(t, "a", 1, "uno", 3) == 0);
    CHECK(ht_count(t) == 2);
    CHECK(get_str(t, "a", v) == 1 && strcmp(v, "uno") == 0);

    /* empty value and binary key */
    CHECK(ht_set(t, "e", 1, "", 0) == 0);
    CHECK(get_str(t, "e", v) == 1 && v[0] == '\0');
    CHECK(ht_set(t, "x\0y", 3, "bin", 3) == 0);
    CHECK(ht_get(t, "x\0y", 3, NULL, NULL) == 1);
    CHECK(ht_get(t, "x\0z", 3, NULL, NULL) == 0);

    CHECK(ht_del(t, "a", 1) == 1);
    CHECK(ht_del(t, "a", 1) == 0);
    CHECK(get_str(t, "a", v) == 0);
    CHECK(ht_count(t) == 3);
    ht_destroy(t);
}

static void test_grow(void)
{
    hashtable_t *t = ht_create(1, 2);
    char k[32], v[32], got[32];
    const int n = 50000;

    for (int i = 0; i < n; i++) {
        int kl = snprintf(k, sizeof k, "key:%d", i);
        int vl = snprintf(v, sizeof v, "val:%d", i);
        CHECK(ht_set(t, k, kl, v, vl) == 0);
    }
    CHECK(ht_count(t) == (size_t)n);
    CHECK(ht_bucket_count(t) >= (size_t)n);
    for (int i = 0; i < n; i++) {
        snprintf(k, sizeof k, "key:%d", i);
        snprintf(v, sizeof v, "val:%d", i);
        CHECK(get_str(t, k, got) == 1 && strcmp(got, v) == 0);
    }
    for (int i = 0; i < n; i += 2) {
        int kl = snprintf(k, sizeof k, "key:%d", i);
        CHECK(ht_del(t, k, kl) == 1);
    }
    CHECK(ht_count(t) == (size_t)n / 2);
    ht_destroy(t);
}

#define NTHREADS 8
#define PER_THREAD 20000

struct worker_arg {
    hashtable_t *t;
    int id;
    int errors;
};

/* Each thread owns a key range (so final state is deterministic) and also
 * hammers a small shared key set to force contention on the same stripes. */
static void *worker(void *p)
{
    struct worker_arg *a = p;
    char k[32], v[32], got[32];

    for (int i = 0; i < PER_THREAD; i++) {
        int kl = snprintf(k, sizeof k, "t%d:%d", a->id, i);
        int vl = snprintf(v, sizeof v, "v%d", i);
        if (ht_set(a->t, k, kl, v, vl) != 0)
            a->errors++;
        if (ht_get(a->t, k, kl, copy_val, got) != 1 || strcmp(got, v) != 0)
            a->errors++;

        int sl = snprintf(k, sizeof k, "shared:%d", i % 16);
        ht_set(a->t, k, sl, "s", 1);
        ht_get(a->t, k, sl, NULL, NULL);
        if (i % 3 == 0)
            ht_del(a->t, k, sl);
    }
    /* delete the odd half of our own range */
    for (int i = 1; i < PER_THREAD; i += 2) {
        int kl = snprintf(k, sizeof k, "t%d:%d", a->id, i);
        if (ht_del(a->t, k, kl) != 1)
            a->errors++;
    }
    return NULL;
}

static void test_concurrent(void)
{
    /* start tiny so many resizes happen while threads are running */
    hashtable_t *t = ht_create(1, 16);
    pthread_t th[NTHREADS];
    struct worker_arg args[NTHREADS];

    for (int i = 0; i < NTHREADS; i++) {
        args[i] = (struct worker_arg){ .t = t, .id = i };
        pthread_create(&th[i], NULL, worker, &args[i]);
    }
    for (int i = 0; i < NTHREADS; i++) {
        pthread_join(th[i], NULL);
        CHECK(args[i].errors == 0);
    }

    /* our own keys: exactly the even half survives */
    char k[32];
    size_t own = 0;
    for (int id = 0; id < NTHREADS; id++) {
        for (int i = 0; i < PER_THREAD; i++) {
            int kl = snprintf(k, sizeof k, "t%d:%d", id, i);
            int found = ht_get(t, k, kl, NULL, NULL);
            CHECK(found == (i % 2 == 0));
            own += (size_t)found;
        }
    }
    size_t shared = 0;
    for (int i = 0; i < 16; i++) {
        int kl = snprintf(k, sizeof k, "shared:%d", i);
        shared += (size_t)ht_get(t, k, kl, NULL, NULL);
    }
    CHECK(own == (size_t)NTHREADS * PER_THREAD / 2);
    CHECK(ht_count(t) == own + shared);
    ht_destroy(t);
}

int main(void)
{
    RUN(test_basic);
    RUN(test_grow);
    RUN(test_concurrent);
    return test_summary("test_hashtable");
}
