/*
 * conformance_batch.c — runs this client's batch codec against
 * docs/conformance/wire_v1_batch_vectors.json.
 *
 * A THIRD binary rather than a branch in either existing scanner, for the
 * reason there are already two: each is sized and shaped for one file. This
 * file's arrays are "frame_vectors", "body_vectors" and "ack_vectors"; its
 * largest object is 1626 bytes and its longest hex string 558 characters
 * (ack_batch_at_the_hard_cap, a 2048-record reply). Measured, not guessed.
 *
 * What makes this worth writing in a third language: a bitmap written with the
 * opposite bit order is a WELL-FORMED frame — header CRC valid, payload CRC
 * valid, payload_len correct — that reports failures as successes. No checksum,
 * length check or cap detects it. Only an asymmetric vector at N > 8 does, and
 * only against an implementation that did not copy its bit arithmetic from the
 * one being checked.
 *
 * Run: ./conformance_batch ../../docs/conformance/wire_v1_batch_vectors.json
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "weir_wire.h"

#define OBJ_CAP   4096
#define HEX_CAP   1024
#define RAW_CAP   512
#define REC_CAP   64

static int g_pass = 0, g_fail = 0;

static void check(int cond, const char *vec, const char *msg) {
    if (cond) {
        g_pass++;
    } else {
        g_fail++;
        printf("  FAIL  %-38s %s\n", vec, msg);
    }
}

static int hex_nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static long hex_decode(const char *hex, uint8_t *out, size_t cap) {
    size_t hlen = strlen(hex);
    if (hlen % 2 != 0) return -1;
    size_t n = hlen / 2;
    if (n > cap) return -1;
    for (size_t i = 0; i < n; i++) {
        int hi = hex_nibble(hex[2 * i]), lo = hex_nibble(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) return -1;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return (long)n;
}

static char *slurp(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return NULL; }
    long sz = ftell(f);
    if (sz < 0) { fclose(f); return NULL; }
    rewind(f);
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t got = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[got] = '\0';
    return buf;
}

static int json_str(const char *s, const char *key, char *out, size_t cap) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\"", key);
    const char *p = strstr(s, pat);
    if (!p) return 0;
    p = strchr(p + strlen(pat), ':');
    if (!p) return 0;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (*p != '"') return 0;
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i + 1 < cap) out[i++] = *p++;
    out[i] = '\0';
    return *p == '"';
}

static int json_long(const char *s, const char *key, long *out) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\"", key);
    const char *p = strstr(s, pat);
    if (!p) return 0;
    p = strchr(p + strlen(pat), ':');
    if (!p) return 0;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    char *end = NULL;
    long v = strtol(p, &end, 10);
    if (end == p) return 0;
    *out = v;
    return 1;
}

/* The named array's objects, one slice at a time. */
static int for_each_object(const char *doc, const char *array_key,
                           void (*fn)(const char *obj)) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\"", array_key);
    const char *arr = strstr(doc, pat);
    if (!arr) {
        printf("  FAIL  could not find array \"%s\" in the document\n", array_key);
        g_fail++;
        return 0;
    }
    arr = strchr(arr, '[');
    if (!arr) return 0;

    int seen = 0;
    const char *p = arr;
    for (;;) {
        const char *start = strchr(p, '{');
        if (!start) break;
        const char *end = strchr(start, '}');
        if (!end) break;
        size_t n = (size_t)(end - start) + 1;
        if (n >= OBJ_CAP) {
            printf("  FAIL  object of %zu bytes exceeds OBJ_CAP %d — raise it\n", n, OBJ_CAP);
            g_fail++;
            return seen;
        }
        char obj[OBJ_CAP];
        memcpy(obj, start, n);
        obj[n] = '\0';
        fn(obj);
        seen++;
        p = end + 1;
        const char *next_obj = strchr(p, '{');
        const char *close = strchr(p, ']');
        if (!next_obj || (close && close < next_obj)) break;
    }
    return seen;
}

/* Vector tag -> this client's result code. A vector naming a tag absent here is
 * a failure, not something to render as "unknown". */
static weir_result tag_to_result(const char *tag) {
    if (!strcmp(tag, "Truncated"))          return WEIR_ERR_BATCH_TRUNCATED;
    if (!strcmp(tag, "UnsupportedVersion")) return WEIR_ERR_BATCH_BAD_VERSION;
    if (!strcmp(tag, "EmptyBatch"))         return WEIR_ERR_BATCH_EMPTY;
    if (!strcmp(tag, "TooManyRecords"))     return WEIR_ERR_BATCH_TOO_MANY;
    if (!strcmp(tag, "EmptyRecord"))        return WEIR_ERR_BATCH_EMPTY_RECORD;
    if (!strcmp(tag, "RecordTooLarge"))     return WEIR_ERR_BATCH_RECORD_TOO_LARGE;
    if (!strcmp(tag, "TruncatedRecord"))    return WEIR_ERR_BATCH_TRUNC_RECORD;
    if (!strcmp(tag, "LengthMismatch"))     return WEIR_ERR_BATCH_LENGTH_MISMATCH;
    if (!strcmp(tag, "PaddingNotZero"))     return WEIR_ERR_BATCH_PADDING_NOT_ZERO;
    return (weir_result)999; /* never equal to a real code */
}

static int g_frames = 0, g_bodies = 0, g_acks = 0;
static int g_body_ok = 0, g_body_reject = 0, g_ack_ok = 0, g_ack_reject = 0;

static void on_frame(const char *obj) {
    char name[128], hex[HEX_CAP], mt[64], phex[HEX_CAP];
    if (!json_str(obj, "name", name, sizeof name)) return;
    if (!json_str(obj, "hex", hex, sizeof hex)) {
        check(0, name, "hex field missing or longer than HEX_CAP");
        return;
    }
    (void)json_str(obj, "message_type", mt, sizeof mt);
    g_frames++;

    uint8_t raw[RAW_CAP];
    long n = hex_decode(hex, raw, sizeof raw);
    check(n > 0, name, "hex did not decode");
    if (n <= 0) return;

    /* Additivity, made executable: a decoder knowing only 0x01-0x05 must reject
     * these as an unknown message type rather than misparsing them. */
    check(raw[5] > 0x05, name, "type byte is inside the v1-only table");

    if (!strcmp(mt, "AckBatch") && json_str(obj, "payload_hex", phex, sizeof phex)) {
        size_t plen = strlen(phex) / 2;
        check(plen > (size_t)WEIR_MAX_RESPONSE_PAYLOAD, name,
              "payload does not exceed the default cap, so the widening is untested");
        check(plen <= weir_max_response_payload(WEIR_MSG_ACK_BATCH), name,
              "payload exceeds this client's AckBatch cap");
    }
}

static void on_body(const char *obj) {
    char name[128], hex[HEX_CAP], want[64];
    if (!json_str(obj, "name", name, sizeof name)) return;
    if (!json_str(obj, "hex", hex, sizeof hex)) {
        check(0, name, "hex field missing or longer than HEX_CAP");
        return;
    }
    if (!json_str(obj, "decode", want, sizeof want)) return;
    g_bodies++;

    uint8_t raw[RAW_CAP];
    long n = hex_decode(hex, raw, sizeof raw);
    check(n >= 0, name, "hex did not decode");
    if (n < 0) return;

    weir_batch_record recs[REC_CAP];
    size_t count = 0;
    weir_result r = weir_decode_batch_body(raw, (size_t)n, WEIR_MAX_BATCH_RECORDS,
                                           WEIR_MAX_PAYLOAD_HARD_CAP,
                                           recs, REC_CAP, &count);

    if (strcmp(want, "ok") != 0) {
        char msg[192];
        snprintf(msg, sizeof msg, "verdict %d, want %s (%d)", r, want, tag_to_result(want));
        check(r == tag_to_result(want), name, msg);
        g_body_reject++;
        return;
    }
    if (r != WEIR_OK) {
        char msg[128];
        snprintf(msg, sizeof msg, "expected ok, got %d", r);
        check(0, name, msg);
        return;
    }
    g_pass++;
    g_body_ok++;

    /* Re-encode must reproduce the vector byte for byte. Decode alone would
     * accept an encoder emitting a different but self-consistent layout. */
    uint8_t back[RAW_CAP];
    size_t back_len = 0;
    if (weir_encode_batch_body(recs, count, back, sizeof back, &back_len) != WEIR_OK
        || back_len != (size_t)n || memcmp(back, raw, back_len) != 0) {
        check(0, name, "re-encode is not byte-identical");
    } else {
        g_pass++;
    }
}

static void on_ack(const char *obj) {
    char name[128], hex[HEX_CAP], want[64];
    long expected = 0;
    if (!json_str(obj, "name", name, sizeof name)) return;
    if (!json_str(obj, "hex", hex, sizeof hex)) {
        check(0, name, "hex field missing or longer than HEX_CAP");
        return;
    }
    if (!json_str(obj, "decode", want, sizeof want)) return;
    if (!json_long(obj, "expected", &expected)) {
        check(0, name, "expected field missing");
        return;
    }
    g_acks++;

    uint8_t raw[RAW_CAP];
    long n = hex_decode(hex, raw, sizeof raw);
    check(n >= 0, name, "hex did not decode");
    if (n < 0) return;

    static uint8_t accepted[WEIR_MAX_BATCH_RECORDS];
    weir_result r = weir_decode_ack_batch(raw, (size_t)n, (size_t)expected,
                                          accepted, sizeof accepted);

    if (strcmp(want, "ok") != 0) {
        char msg[192];
        snprintf(msg, sizeof msg, "verdict %d, want %s (%d)", r, want, tag_to_result(want));
        check(r == tag_to_result(want), name, msg);
        g_ack_reject++;
        return;
    }
    if (r != WEIR_OK) {
        char msg[128];
        snprintf(msg, sizeof msg, "expected ok, got %d", r);
        check(0, name, msg);
        return;
    }
    g_pass++;
    g_ack_ok++;

    uint8_t back[RAW_CAP];
    size_t back_len = 0;
    if (weir_encode_ack_batch(accepted, (size_t)expected, back, sizeof back, &back_len) != WEIR_OK
        || back_len != (size_t)n || memcmp(back, raw, back_len) != 0) {
        check(0, name,
              "re-encode is not byte-identical — if this is the N=9 asymmetric "
              "vector, this client's bitmap bit order is inverted");
    } else {
        g_pass++;
    }
}

int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1]
                                : "../../docs/conformance/wire_v1_batch_vectors.json";
    char *doc = slurp(path);
    if (!doc) {
        fprintf(stderr, "cannot read %s\n", path);
        return 2;
    }

    for_each_object(doc, "frame_vectors", on_frame);
    for_each_object(doc, "body_vectors", on_body);
    for_each_object(doc, "ack_vectors", on_ack);

    /* Both outcomes must actually have been exercised — a scanner that silently
     * found nothing would otherwise report a clean run. */
    check(g_body_ok > 0 && g_body_reject > 0, "coverage",
          "body vectors did not cover both acceptance and rejection");
    check(g_ack_ok > 0 && g_ack_reject > 0, "coverage",
          "ack vectors did not cover both acceptance and rejection");

    /* The bitmap convention against a hand-written literal, so the order is
     * visible in the source rather than only in the vectors. */
    {
        const uint8_t accepted[9] = {1, 0, 1, 0, 0, 0, 0, 0, 1};
        const uint8_t want[5] = {WEIR_ACK_BATCH_VERSION, 9, 0, 0x05, 0x01};
        uint8_t got[16];
        size_t got_len = 0;
        weir_result r = weir_encode_ack_batch(accepted, 9, got, sizeof got, &got_len);
        check(r == WEIR_OK && got_len == sizeof want && memcmp(got, want, sizeof want) == 0,
              "bitmap literal", "not LSB-first — MSB-first would give a0 80");
    }

    /* The cap is widened for AckBatch and AckTracked and for nothing else.
     * Swept over ALL 256 type bytes, not over a list of the types that exist
     * today: a list stops covering the space the moment a byte is assigned. */
    for (int b = 0; b <= 0xFF; b++) {
        size_t want = (size_t)WEIR_MAX_RESPONSE_PAYLOAD;
        if (b == WEIR_MSG_ACK_TRACKED) want = (size_t)WEIR_MAX_TRACKED_ACK_PAYLOAD;
        if (b == WEIR_MSG_ACK_BATCH)   want = (size_t)WEIR_MAX_ACK_BATCH_PAYLOAD;
        if (weir_max_response_payload((uint8_t)b) != want) {
            char msg[128];
            snprintf(msg, sizeof msg, "type %#04x has the wrong cap", (unsigned)b);
            check(0, "response cap", msg);
            break;
        }
    }
    g_pass++;

    /* The reason the hard cap is 2048: the reply stays inside the bound this
     * client already had, so batching adds no new largest response. */
    check(WEIR_MAX_ACK_BATCH_PAYLOAD <= WEIR_MAX_TRACKED_ACK_PAYLOAD, "response cap",
          "AckBatch now exceeds AckTracked — batching introduced a new maximum");

    /* The shared response struct must NOT have grown: a program that never
     * batches should pay nothing for this feature. */
    check(WEIR_MAX_RESPONSE_PAYLOAD == 2, "buffer sizing",
          "the shared response buffer grew");

    free(doc);

    printf("\nframes: %d   bodies: %d   acks: %d   checks passed: %d   failed: %d\n",
           g_frames, g_bodies, g_acks, g_pass, g_fail);
    printf("RESULT: %s\n", g_fail == 0
        ? "PASS — C batch codec matches the batch vectors."
        : "FAIL");
    return g_fail == 0 ? 0 : 1;
}
