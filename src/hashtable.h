/*
 * hashtable.h - thread-safe chained hash table with striped read-write locks.
 *
 * Bucket i is guarded by stripe (i & (nstripes - 1)). Because both counts are
 * powers of two and nbuckets >= nstripes, a key's stripe depends only on its
 * hash, never on the current table size, so a resize (which takes every
 * stripe in write mode) can swap the bucket array underneath waiting threads.
 */
#ifndef KV_HASHTABLE_H
#define KV_HASHTABLE_H

#include <stddef.h>

typedef struct hashtable hashtable_t;

/* Called with the stripe's read lock held; must not call back into the table. */
typedef void (*ht_visit_fn)(void *ctx, const char *val, size_t vlen);

/* Both sizes are rounded up to powers of two. Returns NULL on failure. */
hashtable_t *ht_create(size_t initial_buckets, size_t nstripes);
void ht_destroy(hashtable_t *t);

/* Insert or replace. Returns 0, or -1 on ENOMEM. */
int ht_set(hashtable_t *t, const char *key, size_t klen,
           const char *val, size_t vlen);

/* Returns 1 and calls fn(ctx, value) if found, 0 otherwise. fn may be NULL. */
int ht_get(hashtable_t *t, const char *key, size_t klen,
           ht_visit_fn fn, void *ctx);

/* Returns 1 if the key was removed, 0 if it was not present. */
int ht_del(hashtable_t *t, const char *key, size_t klen);

size_t ht_count(hashtable_t *t);
size_t ht_bucket_count(hashtable_t *t);

#endif
