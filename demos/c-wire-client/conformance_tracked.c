/*
 * conformance_tracked.c — runs this client's coordinate codec against
 * docs/conformance/wire_v1_tracked_vectors.json.
 *
 * A SECOND binary rather than a branch in conformance.c, deliberately. That
 * scanner is sized and shaped for the frozen file: it locates the array with
 * strstr(doc, "\"vectors\"") — which matches nothing here, since this file's
 * arrays are "frame_vectors" and "coordinate_vectors" — and its per-object
 * buffer is 1024 bytes against a 1248-byte vector, its hex buffer 256 against
 * a 596-character string. Widening those for a second schema would leave one
 * scanner guessing which file it is reading, and would put the frozen set's
 * green light at the mercy of tracked-file churn. Two small parsers beat one
 * with two schemas.
 *
 * Run: ./conformance_tracked ../../docs/conformance/wire_v1_tracked_vectors.json
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "weir_wire.h"

/* Sized for THIS file: the largest coordinate vector object is 1248 bytes and
 * the longest hex string 596 characters (coordinate_max_segment, a 255-byte
 * segment). Measured, not guessed. */
#define OBJ_CAP   4096
#define HEX_CAP   1024
#define RAW_CAP   512

static int g_pass = 0, g_fail = 0;

static void check(int cond, const char *vec, const char *msg) {
    if (cond) {
        g_pass++;
    } else {
        g_fail++;
        printf("  FAIL  %-34s %s\n", vec, msg);
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

/* Extract "key": "value" from one object slice. */
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

/* Extract "key": <digits> as an unsigned 64-bit decimal.
 *
 * strtoull, not strtol: the vectors carry u64::MAX, which is the whole reason
 * this field is interesting. C is one of only two languages in demos/ that gets
 * this right without effort — Python is the other; Go, TypeScript and Java all
 * mishandle it by default. */
static int json_u64(const char *s, const char *key, uint64_t *out) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\"", key);
    const char *p = strstr(s, pat);
    if (!p) return 0;
    p = strchr(p + strlen(pat), ':');
    if (!p) return 0;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    char *end = NULL;
    unsigned long long v = strtoull(p, &end, 10);
    if (end == p) return 0;
    *out = (uint64_t)v;
    return 1;
}

/* Walk the objects of the named top-level array, calling fn on each slice. */
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
        /* Objects here are flat, so the first '}' closes this one. */
        const char *end = strchr(start, '}');
        if (!end) break;
        size_t n = (size_t)(end - start) + 1;
        if (n >= OBJ_CAP) {
            printf("  FAIL  object of %zu bytes exceeds OBJ_CAP %d — raise it\n",
                   n, OBJ_CAP);
            g_fail++;
            return seen;
        }
        char obj[OBJ_CAP];
        memcpy(obj, start, n);
        obj[n] = '\0';
        fn(obj);
        seen++;
        p = end + 1;
        /* Stop at the end of this array rather than running into the next. */
        const char *next_obj = strchr(p, '{');
        const char *close = strchr(p, ']');
        if (!next_obj || (close && close < next_obj)) break;
    }
    return seen;
}

static int g_frames = 0, g_coords = 0;

static void on_frame(const char *obj) {
    char name[128], hex[HEX_CAP], mt[64];
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
     * these as an unknown message type rather than misparsing them. That is the
     * whole basis for calling the extension additive within wire v1. */
    check(raw[5] > 0x05, name, "type byte is inside the v1-only table");

    /* The response cap is what tracked push actually changes on the read path. */
    if (strcmp(mt, "AckTracked") == 0) {
        char phex[HEX_CAP];
        if (json_str(obj, "payload_hex", phex, sizeof phex)) {
            size_t plen = strlen(phex) / 2;
            check(plen > (size_t)WEIR_MAX_RESPONSE_PAYLOAD, name,
                  "payload does not exceed the default cap, so the widening is untested");
            check(plen <= (size_t)WEIR_MAX_TRACKED_ACK_PAYLOAD, name,
                  "payload exceeds the tracked cap");
        }
    }
}

static void on_coord(const char *obj) {
    char name[128], hex[HEX_CAP], want[64], seg[512], rid[128];
    if (!json_str(obj, "name", name, sizeof name)) return;
    if (!json_str(obj, "hex", hex, sizeof hex)) {
        check(0, name, "hex field missing or longer than HEX_CAP");
        return;
    }
    if (!json_str(obj, "decode", want, sizeof want)) return;
    g_coords++;

    uint8_t raw[RAW_CAP];
    long n = hex_decode(hex, raw, sizeof raw);
    check(n >= 0, name, "hex did not decode");
    if (n < 0) return;

    weir_coordinate c;
    weir_result r = weir_decode_coordinate(raw, (size_t)n, &c);

    const char *got;
    switch (r) {
        case WEIR_OK:                        got = "ok"; break;
        case WEIR_ERR_COORD_TRUNCATED:       got = "Truncated"; break;
        case WEIR_ERR_COORD_BAD_VERSION:     got = "UnsupportedVersion"; break;
        case WEIR_ERR_COORD_SEGMENT_TOO_LONG:got = "SegmentTooLong"; break;
        case WEIR_ERR_COORD_LENGTH_MISMATCH: got = "LengthMismatch"; break;
        case WEIR_ERR_COORD_NOT_UTF8:        got = "SegmentNotUtf8"; break;
        default:                             got = "unexpected"; break;
    }
    if (strcmp(got, want) != 0) {
        char msg[192];
        snprintf(msg, sizeof msg, "verdict %s, want %s", got, want);
        check(0, name, msg);
        return;
    }
    g_pass++;

    if (r != WEIR_OK) return;

    if (json_str(obj, "segment", seg, sizeof seg)) {
        check(strcmp(c.segment, seg) == 0, name, "segment differs");
    }
    uint64_t want_index = 0;
    if (json_u64(obj, "index", &want_index)) {
        check(c.index == want_index, name, "index differs");
    }
    if (json_str(obj, "record_id_hex", rid, sizeof rid)) {
        char hexbuf[65];
        for (int i = 0; i < 32; i++) snprintf(hexbuf + i * 2, 3, "%02x", c.record_id[i]);
        check(strcmp(hexbuf, rid) == 0, name, "record_id differs");
    }
}

int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1]
                                : "../../docs/conformance/wire_v1_tracked_vectors.json";
    char *doc = slurp(path);
    if (!doc) {
        fprintf(stderr, "cannot read %s\n", path);
        return 2;
    }

    for_each_object(doc, "frame_vectors", on_frame);
    for_each_object(doc, "coordinate_vectors", on_coord);

    /* The cap is widened for exactly the types that need it.
     *
     * Swept over ALL 256 type bytes rather than over a list of the types that
     * exist today. The list this replaced held 0x01-0x06 and 0xFF, so when
     * AckBatch (0x09) was given a wider cap the check still passed while the
     * property it named -- "only AckTracked is widened" -- had quietly become
     * false. A sweep states the property over the whole space and cannot go
     * stale as bytes are assigned. */
    check(weir_max_response_payload(WEIR_MSG_ACK_TRACKED)
              == (size_t)WEIR_MAX_TRACKED_ACK_PAYLOAD,
          "response cap", "AckTracked did not get the wider bound");
    for (int b = 0; b <= 0xFF; b++) {
        size_t want = (size_t)WEIR_MAX_RESPONSE_PAYLOAD;
        if (b == WEIR_MSG_ACK_TRACKED) want = (size_t)WEIR_MAX_TRACKED_ACK_PAYLOAD;
        if (b == WEIR_MSG_ACK_BATCH)   want = (size_t)WEIR_MAX_ACK_BATCH_PAYLOAD;
        if (weir_max_response_payload((uint8_t)b) != want) {
            char msg[128];
            snprintf(msg, sizeof msg, "type %#04x has the wrong response cap", (unsigned)b);
            check(0, "response cap", msg);
            break;
        }
    }

    /* The shared response struct must NOT have grown: a program that never
     * pushes tracked should pay nothing for this feature. */
    check(WEIR_MAX_RESPONSE_PAYLOAD == 2, "buffer sizing",
          "the shared response buffer grew; use weir_tracked_response instead");

    free(doc);

    printf("\nframe vectors: %d   coordinate vectors: %d   checks passed: %d   failed: %d\n",
           g_frames, g_coords, g_pass, g_fail);
    printf("RESULT: %s\n", g_fail == 0
        ? "PASS — C coordinate codec matches the tracked vectors."
        : "FAIL");
    return g_fail == 0 ? 0 : 1;
}
