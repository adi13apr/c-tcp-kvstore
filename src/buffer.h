/*
 * buffer.h - growable byte buffer used for per-connection I/O.
 */
#ifndef KV_BUFFER_H
#define KV_BUFFER_H

#include <stddef.h>

typedef struct {
    char *data;
    size_t len; /* bytes in use */
    size_t cap; /* bytes allocated */
} buf_t;

void buf_init(buf_t *b);
void buf_free(buf_t *b);

/* Ensure at least `extra` free bytes after len. Returns 0, or -1 on ENOMEM. */
int buf_reserve(buf_t *b, size_t extra);

/* Append n bytes. Returns 0, or -1 on ENOMEM. */
int buf_append(buf_t *b, const void *p, size_t n);

/* Drop the first n bytes, shifting the remainder to the front. */
void buf_consume(buf_t *b, size_t n);

#endif
