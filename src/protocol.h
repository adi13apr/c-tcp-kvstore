/*
 * protocol.h - line-oriented text protocol, shared by both servers.
 *
 * Requests are lines terminated by "\n" (an optional preceding "\r" is
 * stripped). See README.md for the full grammar.
 *
 *   PING            -> PONG
 *   SET <key> <val> -> OK              (val = rest of line, may contain spaces)
 *   GET <key>       -> VALUE <val> | NOT_FOUND
 *   DEL <key>       -> DELETED | NOT_FOUND
 *   QUIT            -> BYE, then the server closes the connection
 *   anything else   -> ERR <reason>
 */
#ifndef KV_PROTOCOL_H
#define KV_PROTOCOL_H

#include <stddef.h>

#include "buffer.h"
#include "hashtable.h"

/* Longest accepted request line, not counting the "\r\n" terminator. */
#define PROTO_MAX_LINE (64 * 1024)
/* A read buffer this large always holds one maximal line plus "\r\n". */
#define PROTO_READ_BUF (PROTO_MAX_LINE + 2)
/* proto_process() stops once this much output is pending, so a pipelined
 * burst of GETs cannot make the output buffer grow without bound. */
#define PROTO_OUT_HIGH_WATER (256 * 1024)

/*
 * Execute every complete request in in[0..len), appending responses to out.
 *
 * Returns the number of input bytes consumed; the caller drops those and
 * keeps the rest (a partial line) for the next read. Processing stops early
 * when out reaches PROTO_OUT_HIGH_WATER, so call again after flushing.
 *
 * Sets *close_after to 1 when the connection must be closed once `out` has
 * been flushed (QUIT, over-long line, or out of memory); nothing after that
 * request is consumed.
 */
size_t proto_process(hashtable_t *t, const char *in, size_t len,
                     buf_t *out, int *close_after);

#endif
