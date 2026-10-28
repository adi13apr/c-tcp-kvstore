#include "hashtable.h"

#include <pthread.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define CACHE_LINE 64
#define MAX_BUCKETS ((size_t)1 << 30)

typedef struct entry {
    struct entry *next;
    uint64_t hash;
    size_t klen;
    size_t vlen;
    char data[]; /* key bytes followed by value bytes */
} entry_t;

/* One lock per cache line so neighbouring stripes don't false-share. */
typedef struct {
    alignas(CACHE_LINE) pthread_rwlock_t lock;
} stripe_t;

struct hashtable {
    entry_t **buckets;      /* written only while holding every stripe */
    atomic_size_t nbuckets; /* atomic so the grow check can peek unlocked */
    size_t nstripes;
    stripe_t *stripes;
    atomic_size_t count;
};

/* FNV-1a, finished with the murmur3 avalanche so the low bits we mask on
 * are well mixed. */
static uint64_t hash_bytes(const char *p, size_t n)
{
    uint64_t h = 0xcbf29ce484222325ULL;
    for (size_t i = 0; i < n; i++) {
        h ^= (unsigned char)p[i];
        h *= 0x100000001b3ULL;
    }
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

static size_t next_pow2(size_t n)
{
    size_t p = 1;
    while (p < n && p < MAX_BUCKETS)
        p <<= 1;
    return p;
}

static pthread_rwlock_t *stripe_lock(hashtable_t *t, uint64_t h)
{
    return &t->stripes[h & (t->nstripes - 1)].lock;
}

static size_t load_nbuckets(hashtable_t *t)
{
    return atomic_load_explicit(&t->nbuckets, memory_order_relaxed);
}

static int key_eq(const entry_t *e, uint64_t h, const char *key, size_t klen)
{
    return e->hash == h && e->klen == klen && memcmp(e->data, key, klen) == 0;
}

hashtable_t *ht_create(size_t initial_buckets, size_t nstripes)
{
    hashtable_t *t = calloc(1, sizeof *t);
    if (!t)
        return NULL;

    t->nstripes = next_pow2(nstripes ? nstripes : 1);
    size_t nb = next_pow2(initial_buckets);
    if (nb < t->nstripes)
        nb = t->nstripes;
    atomic_init(&t->nbuckets, nb);
    atomic_init(&t->count, 0);

    t->buckets = calloc(nb, sizeof *t->buckets);
    t->stripes = aligned_alloc(CACHE_LINE, t->nstripes * sizeof *t->stripes);
    if (!t->buckets || !t->stripes) {
        free(t->buckets);
        free(t->stripes);
        free(t);
        return NULL;
    }

    /* glibc rwlocks prefer readers by default, which can starve SETs under a
     * GET-heavy load; ask for writer preference instead. */
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr,
                                  PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    for (size_t i = 0; i < t->nstripes; i++)
        pthread_rwlock_init(&t->stripes[i].lock, &attr);
    pthread_rwlockattr_destroy(&attr);
    return t;
}

void ht_destroy(hashtable_t *t)
{
    if (!t)
        return;
    size_t nb = load_nbuckets(t);
    for (size_t i = 0; i < nb; i++) {
        entry_t *e = t->buckets[i];
        while (e) {
            entry_t *next = e->next;
            free(e);
            e = next;
        }
    }
    for (size_t i = 0; i < t->nstripes; i++)
        pthread_rwlock_destroy(&t->stripes[i].lock);
    free(t->stripes);
    free(t->buckets);
    free(t);
}

/* Double the bucket array if it is still `old_nb` long. Takes every stripe in
 * index order (the only multi-lock path, so no deadlock is possible). */
static void grow(hashtable_t *t, size_t old_nb)
{
    size_t new_nb = old_nb * 2;
    entry_t **nbk = calloc(new_nb, sizeof *nbk);
    if (!nbk)
        return; /* stay at the current size; lookups just get slower */

    for (size_t i = 0; i < t->nstripes; i++)
        pthread_rwlock_wrlock(&t->stripes[i].lock);

    entry_t **old = NULL;
    if (load_nbuckets(t) == old_nb) {
        old = t->buckets;
        for (size_t i = 0; i < old_nb; i++) {
            entry_t *e = old[i];
            while (e) {
                entry_t *next = e->next;
                size_t idx = e->hash & (new_nb - 1);
                e->next = nbk[idx];
                nbk[idx] = e;
                e = next;
            }
        }
        t->buckets = nbk;
        atomic_store_explicit(&t->nbuckets, new_nb, memory_order_relaxed);
        nbk = NULL;
    }

    for (size_t i = t->nstripes; i-- > 0;)
        pthread_rwlock_unlock(&t->stripes[i].lock);

    free(old);
    free(nbk); /* someone else already grew it */
}

int ht_set(hashtable_t *t, const char *key, size_t klen,
           const char *val, size_t vlen)
{
    uint64_t h = hash_bytes(key, klen);

    /* Allocate and free outside the lock to keep the critical section short. */
    entry_t *e = malloc(sizeof *e + klen + vlen);
    if (!e)
        return -1;
    e->hash = h;
    e->klen = klen;
    e->vlen = vlen;
    memcpy(e->data, key, klen);
    if (vlen)
        memcpy(e->data + klen, val, vlen);

    pthread_rwlock_t *lk = stripe_lock(t, h);
    pthread_rwlock_wrlock(lk);
    size_t nb = load_nbuckets(t);
    entry_t **pp = &t->buckets[h & (nb - 1)];
    entry_t *old = NULL;
    for (; *pp; pp = &(*pp)->next) {
        if (key_eq(*pp, h, key, klen)) {
            old = *pp;
            break;
        }
    }
    e->next = old ? old->next : NULL;
    *pp = e; /* replaces old in place, or appends at the chain tail */
    pthread_rwlock_unlock(lk);

    if (old) {
        free(old);
    } else {
        size_t c = atomic_fetch_add(&t->count, 1) + 1;
        if (c > nb && nb < MAX_BUCKETS) /* load factor > 1 */
            grow(t, nb);
    }
    return 0;
}

int ht_get(hashtable_t *t, const char *key, size_t klen,
           ht_visit_fn fn, void *ctx)
{
    uint64_t h = hash_bytes(key, klen);
    pthread_rwlock_t *lk = stripe_lock(t, h);
    int found = 0;

    pthread_rwlock_rdlock(lk);
    for (entry_t *e = t->buckets[h & (load_nbuckets(t) - 1)]; e; e = e->next) {
        if (key_eq(e, h, key, klen)) {
            if (fn)
                fn(ctx, e->data + e->klen, e->vlen);
            found = 1;
            break;
        }
    }
    pthread_rwlock_unlock(lk);
    return found;
}

int ht_del(hashtable_t *t, const char *key, size_t klen)
{
    uint64_t h = hash_bytes(key, klen);
    pthread_rwlock_t *lk = stripe_lock(t, h);
    entry_t *victim = NULL;

    pthread_rwlock_wrlock(lk);
    entry_t **pp = &t->buckets[h & (load_nbuckets(t) - 1)];
    for (; *pp; pp = &(*pp)->next) {
        if (key_eq(*pp, h, key, klen)) {
            victim = *pp;
            *pp = victim->next;
            break;
        }
    }
    pthread_rwlock_unlock(lk);

    if (!victim)
        return 0;
    free(victim);
    atomic_fetch_sub(&t->count, 1);
    return 1;
}

size_t ht_count(hashtable_t *t)
{
    return atomic_load(&t->count);
}

size_t ht_bucket_count(hashtable_t *t)
{
    return load_nbuckets(t);
}
