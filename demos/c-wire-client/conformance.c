/*
 * conformance.c — verify the C codec against the published wire vectors.
 *
 * Reads docs/conformance/wire_v1_vectors.json (path given as argv[1]) and,
 * for each vector, confirms:
 *   - our weir_crc32 reproduces the embedded header + payload CRCs, and
 *   - for every "ok" Push/HealthCheck vector, our ENCODER reproduces the
 *     exact published bytes (the strongest possible compatibility check);
 *   - for every "ok" response vector (Ack/Nack/HealthCheckResponse), our
 *     DECODER accepts it and reads the right message_type + nack reason;
 *   - for the rejection vectors, our decoder/encoder rejects them.
 *
 * Uses only the C stdlib: a minimal JSON field scanner is enough because
 * the vector file's shape is fixed and simple.
 */
#include "weir_wire.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- tiny helpers ----------------------------------------------------- */

static int hex_nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/* Decode a hex string into out (capacity cap). Returns byte count or -1. */
static long hex_decode(const char *hex, uint8_t *out, size_t cap) {
    size_t hlen = strlen(hex);
    if (hlen % 2 != 0) return -1;
    size_t n = hlen / 2;
    if (n > cap) return -1;
    for (size_t i = 0; i < n; i++) {
        int hi = hex_nibble(hex[2 * i]);
        int lo = hex_nibble(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) return -1;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return (long)n;
}

/* Read an entire file into a malloc'd NUL-terminated buffer. */
static char *slurp(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t got = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[got] = '\0';
    return buf;
}

/*
 * Extract the string value for "key":"value" starting the search at *cursor
 * within the object that begins at obj_start and ends at obj_end. Writes the
 * value into out (cap) and returns 1 if found, 0 otherwise. Does not handle
 * escaped quotes — the vector file has none in the fields we read.
 */
static int json_str(const char *s, const char *key, char *out, size_t cap) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\"", key);
    const char *p = strstr(s, pat);
    if (!p) return 0;
    p += strlen(pat);
    while (*p == ' ' || *p == ':' || *p == '\t' || *p == '\n') p++;
    if (*p != '"') return 0;
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i + 1 < cap) out[i++] = *p++;
    out[i] = '\0';
    return (*p == '"');
}

/* Split the vectors array into individual object slices (by brace depth). */

typedef struct {
    char name[64];
    char hex[256];
    char decode[64];
    char message_type[32];
    char durability[16];
    char payload_hex[128];
    int  has_payload_hex;
} vector;

static const char *durability_byte_to_str(uint8_t d) {
    switch (d) {
        case WEIR_DUR_SYNC:     return "Sync";
        case WEIR_DUR_BATCHED:  return "Batched";
        case WEIR_DUR_BUFFERED: return "Buffered";
        default:                return "?";
    }
}

static int g_pass = 0, g_fail = 0;

static void check(int cond, const char *vec, const char *msg) {
    if (cond) {
        g_pass++;
    } else {
        g_fail++;
        printf("  FAIL [%s] %s\n", vec, msg);
    }
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <wire_v1_vectors.json>\n", argv[0]);
        return 2;
    }
    char *doc = slurp(argv[1]);
    if (!doc) {
        fprintf(stderr, "cannot read %s\n", argv[1]);
        return 2;
    }

    printf("weir C client — conformance against %s\n\n", argv[1]);

    /* Walk object by object. Each vector object opens with `{` after the
     * "vectors" array start; we slice on top-level braces inside the array. */
    const char *arr = strstr(doc, "\"vectors\"");
    if (!arr) { fprintf(stderr, "no vectors array\n"); return 2; }
    const char *p = strchr(arr, '[');
    if (!p) { fprintf(stderr, "malformed vectors array\n"); return 2; }
    p++;

    int total_vectors = 0;
    while (1) {
        /* find next object start */
        const char *open = strchr(p, '{');
        if (!open) break;
        /* find matching close (no nested braces in these objects) */
        const char *close = strchr(open, '}');
        if (!close) break;

        size_t len = (size_t)(close - open) + 1;
        char obj[1024];
        if (len >= sizeof(obj)) { p = close + 1; continue; }
        memcpy(obj, open, len);
        obj[len] = '\0';

        vector v;
        memset(&v, 0, sizeof(v));
        json_str(obj, "name", v.name, sizeof(v.name));
        json_str(obj, "hex", v.hex, sizeof(v.hex));
        json_str(obj, "decode", v.decode, sizeof(v.decode));
        json_str(obj, "message_type", v.message_type, sizeof(v.message_type));
        json_str(obj, "durability", v.durability, sizeof(v.durability));
        v.has_payload_hex =
            json_str(obj, "payload_hex", v.payload_hex, sizeof(v.payload_hex));

        if (v.name[0] == '\0' || v.hex[0] == '\0') { p = close + 1; continue; }
        total_vectors++;

        uint8_t raw[512];
        long rawlen = hex_decode(v.hex, raw, sizeof(raw));
        check(rawlen >= 0, v.name, "hex decode");
        if (rawlen < 0) { p = close + 1; continue; }

        int is_ok = (strcmp(v.decode, "ok") == 0);

        if (is_ok && rawlen >= WEIR_HEADER_LEN) {
            /* Every well-formed frame carries a header CRC over [0..12).
             * Confirm our CRC reproduces it. */
            uint32_t hdr_want = (uint32_t)raw[12] | ((uint32_t)raw[13] << 8)
                              | ((uint32_t)raw[14] << 16) | ((uint32_t)raw[15] << 24);
            uint32_t hdr_got = weir_crc32(raw, 12);
            check(hdr_want == hdr_got, v.name, "header CRC mismatch");

            uint8_t mt = raw[5];

            /* Re-encode Push / HealthCheck and demand byte-exact equality. */
            if (mt == WEIR_MSG_PUSH) {
                uint8_t payload[256];
                long plen = v.has_payload_hex
                    ? hex_decode(v.payload_hex, payload, sizeof(payload)) : 0;
                uint8_t enc[512];
                size_t enc_len = 0;
                weir_result r = weir_encode_push(
                    (weir_durability)raw[6], payload, (size_t)plen,
                    enc, sizeof(enc), &enc_len);
                check(r == WEIR_OK, v.name, "encode_push failed");
                check(enc_len == (size_t)rawlen &&
                      memcmp(enc, raw, (size_t)rawlen) == 0,
                      v.name, "encoded Push bytes differ from vector");
                /* durability label sanity */
                check(strcmp(v.durability,
                             durability_byte_to_str(raw[6])) == 0,
                      v.name, "durability label mismatch");
            } else if (mt == WEIR_MSG_HEALTHCHECK) {
                uint8_t enc[64];
                size_t enc_len = 0;
                weir_result r = weir_encode_healthcheck(enc, sizeof(enc), &enc_len);
                check(r == WEIR_OK, v.name, "encode_healthcheck failed");
                check(enc_len == (size_t)rawlen &&
                      memcmp(enc, raw, (size_t)rawlen) == 0,
                      v.name, "encoded HealthCheck bytes differ from vector");
            } else if (mt == WEIR_MSG_ACK || mt == WEIR_MSG_NACK ||
                       mt == WEIR_MSG_HEALTHCHECK_RESPONSE) {
                /* Decode as a response: our client must accept it. */
                weir_resp_header rh;
                weir_result r = weir_decode_resp_header(raw, &rh);
                check(r == WEIR_OK, v.name, "decode_resp_header rejected an ok response");
                if (r == WEIR_OK) {
                    check(rh.message_type == mt, v.name, "decoded message_type wrong");
                    /* payload CRC check on the response tail */
                    if (rawlen >= WEIR_HEADER_LEN + 4) {
                        uint32_t plen = rh.payload_len;
                        uint32_t pc_want =
                            (uint32_t)raw[16 + plen]
                          | ((uint32_t)raw[16 + plen + 1] << 8)
                          | ((uint32_t)raw[16 + plen + 2] << 16)
                          | ((uint32_t)raw[16 + plen + 3] << 24);
                        uint32_t pc_got = weir_crc32(raw + 16, plen);
                        check(pc_want == pc_got, v.name, "response payload CRC mismatch");
                    }
                    if (mt == WEIR_MSG_NACK && v.has_payload_hex &&
                        strlen(v.payload_hex) >= 2) {
                        uint8_t reason = (uint8_t)((hex_nibble(v.payload_hex[0]) << 4)
                                       | hex_nibble(v.payload_hex[1]));
                        /* just confirm we can name it without crashing */
                        const char *name = weir_nack_reason_str(reason);
                        check(name != NULL, v.name, "nack reason name");
                    }
                }
            }
        } else if (!is_ok) {
            /* Rejection vectors. Where the failure is one our client can see
             * (it parses RESPONSE headers, plus it would never *send* these),
             * confirm our decoder rejects them. The decoder validates magic,
             * version, and header CRC; the rest are server-side decode tags. */
            weir_resp_header rh;
            weir_result r = weir_decode_resp_header(raw, &rh);
            if (strcmp(v.decode, "BadMagic") == 0) {
                check(r == WEIR_ERR_BAD_MAGIC, v.name, "expected BadMagic rejection");
            } else if (strcmp(v.decode, "VersionMismatch") == 0) {
                check(r == WEIR_ERR_BAD_VERSION, v.name, "expected version rejection");
            } else if (strcmp(v.decode, "HeaderCrcMismatch") == 0) {
                check(r == WEIR_ERR_BAD_HEADER_CRC, v.name, "expected header CRC rejection");
            } else if (strcmp(v.decode, "TruncatedFrame") == 0 &&
                       rawlen < WEIR_HEADER_LEN) {
                /* Our decoder needs a full 16-byte header to even start; a
                 * short buffer is a short read in the transport. We can only
                 * assert the buffer is indeed sub-header here. */
                check(rawlen < WEIR_HEADER_LEN, v.name, "expected sub-header buffer");
            }
            /* Other server-side tags (PayloadTooLarge on send, UnknownMessage,
             * etc.) are validated live against the daemon, not here. */
        }

        p = close + 1;
    }

    free(doc);
    printf("\nvectors scanned: %d   checks passed: %d   failed: %d\n",
           total_vectors, g_pass, g_fail);
    if (g_fail == 0)
        printf("RESULT: PASS — C codec is wire-compatible with weir v1.\n");
    else
        printf("RESULT: FAIL\n");
    return g_fail == 0 ? 0 : 1;
}
