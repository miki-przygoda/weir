/*
 * weir_wire.c — implementation of the weir v1 wire codec.
 * Pure C11, no external dependencies. Built from docs/wire_protocol.md.
 */
#include "weir_wire.h"

#include <string.h>

/* ---- CRC-32 / ISO-3309 (zlib variant) ---------------------------------
 *
 * Reflected (refin=refout=true) implementation: the reflected form of
 * poly 0x04C11DB7 is 0xEDB88320, processed LSB-first. init/xorout
 * 0xFFFFFFFF. Table built lazily on first use.
 */
static uint32_t crc_table[256];
static int crc_table_ready = 0;

static void crc_table_init(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++) {
            c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        crc_table[i] = c;
    }
    crc_table_ready = 1;
}

uint32_t weir_crc32(const uint8_t *data, size_t len) {
    if (!crc_table_ready) crc_table_init();
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        crc = crc_table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

/* ---- little-endian helpers -------------------------------------------- */
static void put_u32_le(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v & 0xFF);
    p[1] = (uint8_t)((v >> 8) & 0xFF);
    p[2] = (uint8_t)((v >> 16) & 0xFF);
    p[3] = (uint8_t)((v >> 24) & 0xFF);
}
static uint32_t get_u32_le(const uint8_t *p) {
    return (uint32_t)p[0]
         | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16)
         | ((uint32_t)p[3] << 24);
}

/* Fill bytes [0..16) of a header and the [12..16) header CRC. */
static void write_header(uint8_t *h, weir_msg_type mt, weir_durability dur,
                         uint32_t payload_len) {
    h[0] = WEIR_MAGIC_0; h[1] = WEIR_MAGIC_1;
    h[2] = WEIR_MAGIC_2; h[3] = WEIR_MAGIC_3;
    h[4] = WEIR_WIRE_VERSION;
    h[5] = (uint8_t)mt;
    h[6] = (uint8_t)dur;
    h[7] = 0x00;                       /* flags: must be zero on write */
    put_u32_le(h + 8, payload_len);
    put_u32_le(h + 12, weir_crc32(h, 12)); /* header CRC over [0..12) */
}

/* Push and PushTracked differ only in the type byte; share the guards so they
 * cannot drift apart. */
static weir_result encode_record(weir_msg_type mt, weir_durability dur,
                                 const uint8_t *payload, size_t payload_len,
                                 uint8_t *out, size_t out_cap, size_t *out_len) {
    if (payload_len == 0) return WEIR_ERR_EMPTY_PAYLOAD;
    if (payload_len > WEIR_MAX_PAYLOAD_HARD_CAP) return WEIR_ERR_PAYLOAD_TOO_LARGE;

    size_t total = WEIR_HEADER_LEN + payload_len + WEIR_CRC_LEN;
    if (out_cap < total) return WEIR_ERR_BUF_TOO_SMALL;

    write_header(out, mt, dur, (uint32_t)payload_len);
    memcpy(out + WEIR_HEADER_LEN, payload, payload_len);
    put_u32_le(out + WEIR_HEADER_LEN + payload_len,
               weir_crc32(payload, payload_len));

    if (out_len) *out_len = total;
    return WEIR_OK;
}

weir_result weir_encode_push(weir_durability dur,
                             const uint8_t *payload, size_t payload_len,
                             uint8_t *out, size_t out_cap, size_t *out_len) {
    return encode_record(WEIR_MSG_PUSH, dur, payload, payload_len,
                         out, out_cap, out_len);
}

weir_result weir_encode_push_tracked(const uint8_t *payload, size_t payload_len,
                                     weir_durability dur,
                                     uint8_t *out, size_t out_cap, size_t *out_len) {
    return encode_record(WEIR_MSG_PUSH_TRACKED, dur, payload, payload_len,
                         out, out_cap, out_len);
}

size_t weir_max_response_payload(uint8_t message_type) {
    switch (message_type) {
        case WEIR_MSG_ACK_TRACKED: return (size_t)WEIR_MAX_TRACKED_ACK_PAYLOAD;
        case WEIR_MSG_ACK_BATCH:   return (size_t)WEIR_MAX_ACK_BATCH_PAYLOAD;
        default:                   return (size_t)WEIR_MAX_RESPONSE_PAYLOAD;
    }
}

/*
 * Strict UTF-8 validation. C has none in its standard library, and the spec
 * commits to rejecting a non-UTF-8 segment, so it has to be written out.
 *
 * Rejects overlong encodings, surrogates (U+D800..U+DFFF) and anything above
 * U+10FFFF -- all three are sequences a permissive decoder would accept and a
 * conformant one must not.
 */
static int utf8_valid(const uint8_t *s, size_t n) {
    size_t i = 0;
    while (i < n) {
        uint8_t c = s[i];
        size_t need;
        uint32_t cp;
        if (c < 0x80u) { i++; continue; }
        else if ((c & 0xE0u) == 0xC0u) { need = 1; cp = c & 0x1Fu; }
        else if ((c & 0xF0u) == 0xE0u) { need = 2; cp = c & 0x0Fu; }
        else if ((c & 0xF8u) == 0xF0u) { need = 3; cp = c & 0x07u; }
        else return 0;                       /* 0x80-0xBF lead, or 0xF8+ */
        if (i + need >= n) return 0;         /* continuation bytes run past the end */
        for (size_t k = 1; k <= need; k++) {
            uint8_t cc = s[i + k];
            if ((cc & 0xC0u) != 0x80u) return 0;
            cp = (cp << 6) | (uint32_t)(cc & 0x3Fu);
        }
        if (need == 1 && cp < 0x80u) return 0;        /* overlong */
        if (need == 2 && cp < 0x800u) return 0;       /* overlong */
        if (need == 3 && cp < 0x10000u) return 0;     /* overlong */
        if (cp >= 0xD800u && cp <= 0xDFFFu) return 0; /* surrogate */
        if (cp > 0x10FFFFu) return 0;                 /* out of range */
        i += need + 1;
    }
    return 1;
}

weir_result weir_decode_coordinate(const uint8_t *buf, size_t len,
                                   weir_coordinate *out) {
    if (len < (size_t)WEIR_COORDINATE_FIXED_LEN) return WEIR_ERR_COORD_TRUNCATED;
    if (buf[0] != WEIR_COORDINATE_VERSION)       return WEIR_ERR_COORD_BAD_VERSION;

    uint64_t index = 0;
    for (int i = 7; i >= 0; i--) index = (index << 8) | buf[1 + i];

    size_t seg_len = (size_t)buf[41] | ((size_t)buf[42] << 8);
    if (seg_len > (size_t)WEIR_MAX_SEGMENT_NAME_LEN) {
        return WEIR_ERR_COORD_SEGMENT_TOO_LONG;
    }
    size_t want = (size_t)WEIR_COORDINATE_FIXED_LEN + seg_len;
    if (len < want) return WEIR_ERR_COORD_TRUNCATED;
    if (len != want) return WEIR_ERR_COORD_LENGTH_MISMATCH;

    const uint8_t *seg = buf + WEIR_COORDINATE_FIXED_LEN;
    if (!utf8_valid(seg, seg_len)) return WEIR_ERR_COORD_NOT_UTF8;

    if (out) {
        memcpy(out->segment, seg, seg_len);
        out->segment[seg_len] = '\0';
        out->segment_len = seg_len;
        out->index = index;
        memcpy(out->record_id, buf + 9, 32);
    }
    return WEIR_OK;
}

weir_result weir_encode_healthcheck(uint8_t *out, size_t out_cap,
                                    size_t *out_len) {
    size_t total = WEIR_HEADER_LEN + 0 + WEIR_CRC_LEN;
    if (out_cap < total) return WEIR_ERR_BUF_TOO_SMALL;
    /* Durability must still be a valid byte even though it is unused; the
     * daemon validates the whole header before dispatching. Use Durable. */
    write_header(out, WEIR_MSG_HEALTHCHECK, WEIR_DUR_DURABLE, 0);
    /* CRC over zero payload bytes == 0x00000000. */
    put_u32_le(out + WEIR_HEADER_LEN, weir_crc32(NULL, 0));
    if (out_len) *out_len = total;
    return WEIR_OK;
}

weir_result weir_decode_resp_header(const uint8_t hdr[WEIR_HEADER_LEN],
                                    weir_resp_header *out) {
    if (hdr[0] != WEIR_MAGIC_0 || hdr[1] != WEIR_MAGIC_1 ||
        hdr[2] != WEIR_MAGIC_2 || hdr[3] != WEIR_MAGIC_3) {
        return WEIR_ERR_BAD_MAGIC;
    }
    if (hdr[4] != WEIR_WIRE_VERSION) return WEIR_ERR_BAD_VERSION;

    uint32_t want = get_u32_le(hdr + 12);
    uint32_t got  = weir_crc32(hdr, 12);
    if (want != got) return WEIR_ERR_BAD_HEADER_CRC;

    uint32_t plen = get_u32_le(hdr + 8);
    /* Cap by the type the header declares. Safe to trust hdr[5] here: the
     * header CRC was verified three lines up, so the type byte cannot have been
     * flipped into unlocking a bigger read. */
    if ((size_t)plen > weir_max_response_payload(hdr[5])) return WEIR_ERR_RESP_TOO_LARGE;

    if (out) {
        out->version      = hdr[4];
        out->message_type = hdr[5];
        out->durability   = hdr[6];
        out->flags        = hdr[7];
        out->payload_len  = plen;
    }
    return WEIR_OK;
}

const char *weir_nack_reason_str(uint8_t reason) {
    switch (reason) {
        case WEIR_NACK_BAD_MAGIC:         return "BadMagic";
        case WEIR_NACK_VERSION_MISMATCH:  return "VersionMismatch";
        case WEIR_NACK_BAD_HEADER_CRC:    return "BadHeaderCrc";
        case WEIR_NACK_PAYLOAD_TOO_LARGE: return "PayloadTooLarge";
        case WEIR_NACK_BAD_PAYLOAD_CRC:   return "BadPayloadCrc";
        case WEIR_NACK_INTERNAL_ERROR:    return "InternalError";
        case WEIR_NACK_EMPTY_PAYLOAD:     return "EmptyPayload";
        case WEIR_NACK_UNKNOWN_MESSAGE:   return "UnknownMessage";
        case WEIR_NACK_RESERVED_FLAGS:    return "ReservedFlagsSet";
        default:                          return "Reserved/Unknown";
    }
}

const char *weir_msg_type_str(uint8_t mt) {
    switch (mt) {
        case WEIR_MSG_PUSH:                 return "Push";
        case WEIR_MSG_ACK:                  return "Ack";
        case WEIR_MSG_NACK:                 return "Nack";
        case WEIR_MSG_HEALTHCHECK:          return "HealthCheck";
        case WEIR_MSG_HEALTHCHECK_RESPONSE: return "HealthCheckResponse";
        default:                            return "Unknown";
    }
}

const char *weir_result_str(weir_result r) {
    switch (r) {
        case WEIR_OK:                   return "ok";
        case WEIR_ERR_BUF_TOO_SMALL:    return "output buffer too small";
        case WEIR_ERR_PAYLOAD_TOO_LARGE:return "payload exceeds hard cap";
        case WEIR_ERR_EMPTY_PAYLOAD:    return "empty Push payload";
        case WEIR_ERR_BAD_MAGIC:        return "bad response magic";
        case WEIR_ERR_BAD_VERSION:      return "response version mismatch";
        case WEIR_ERR_BAD_HEADER_CRC:   return "bad response header CRC";
        case WEIR_ERR_RESP_TOO_LARGE:   return "response payload too large (desync)";
        case WEIR_ERR_SHORT_READ:       return "short read (peer closed)";
        case WEIR_ERR_IO:               return "io error";
        case WEIR_ERR_RESERVED_FLAGS:   return "reserved flags set";
        default:                        return "unknown";
    }
}

/*
 * ── Batch extension (PushBatch 0x08 / AckBatch 0x09) ─────────────────────────
 *
 * Written from docs/wire_protocol.md and checked against
 * docs/conformance/wire_v1_batch_vectors.json. Nothing here is ported from the
 * Rust reference: an independent implementation is the only thing that catches
 * an under-specified format, and this one has a bug class no checksum detects
 * -- a bitmap written with the opposite bit order is a well-formed frame, valid
 * CRCs and correct length, that reports failures as successes.
 *
 * Bit i lives in byte i / 8 at mask 1 << (i % 8): LSB-first.
 */

weir_result weir_encode_batch_body(const weir_batch_record *records, size_t count,
                                   uint8_t *out, size_t out_cap, size_t *out_len) {
    size_t need = WEIR_BATCH_HEADER_LEN;
    for (size_t i = 0; i < count; i++) {
        need += 4 + records[i].len;
    }
    if (out_cap < need) return WEIR_ERR_BUF_TOO_SMALL;

    size_t p = 0;
    out[p++] = WEIR_BATCH_VERSION;
    out[p++] = (uint8_t)(count & 0xFF);
    out[p++] = (uint8_t)((count >> 8) & 0xFF);
    for (size_t i = 0; i < count; i++) {
        uint32_t n = (uint32_t)records[i].len;
        out[p++] = (uint8_t)(n & 0xFF);
        out[p++] = (uint8_t)((n >> 8) & 0xFF);
        out[p++] = (uint8_t)((n >> 16) & 0xFF);
        out[p++] = (uint8_t)((n >> 24) & 0xFF);
        if (records[i].len) memcpy(out + p, records[i].data, records[i].len);
        p += records[i].len;
    }
    *out_len = p;
    return WEIR_OK;
}

weir_result weir_decode_batch_body(const uint8_t *body, size_t body_len,
                                   size_t max_records, size_t max_record_len,
                                   weir_batch_record *out, size_t out_cap,
                                   size_t *out_count) {
    if (body_len < WEIR_BATCH_HEADER_LEN) return WEIR_ERR_BATCH_TRUNCATED;
    if (body[0] != WEIR_BATCH_VERSION)    return WEIR_ERR_BATCH_BAD_VERSION;

    size_t declared = (size_t)body[1] | ((size_t)body[2] << 8);
    if (declared == 0) return WEIR_ERR_BATCH_EMPTY;
    size_t cap = max_records < (size_t)WEIR_MAX_BATCH_RECORDS
               ? max_records : (size_t)WEIR_MAX_BATCH_RECORDS;
    if (declared > cap) return WEIR_ERR_BATCH_TOO_MANY;
    /* Only now is `declared` a number this client chose the bound for, so only
     * now may it be compared against the caller's array. */
    if (declared > out_cap) return WEIR_ERR_BUF_TOO_SMALL;

    size_t record_cap = max_record_len < (size_t)WEIR_MAX_PAYLOAD_HARD_CAP
                      ? max_record_len : (size_t)WEIR_MAX_PAYLOAD_HARD_CAP;
    size_t found = 0, cursor = WEIR_BATCH_HEADER_LEN;
    while (cursor < body_len) {
        if (cursor + 4 > body_len) return WEIR_ERR_BATCH_TRUNC_RECORD;
        size_t n = (size_t)body[cursor]
                 | ((size_t)body[cursor + 1] << 8)
                 | ((size_t)body[cursor + 2] << 16)
                 | ((size_t)body[cursor + 3] << 24);
        cursor += 4;
        if (n == 0)           return WEIR_ERR_BATCH_EMPTY_RECORD;
        if (n > record_cap)   return WEIR_ERR_BATCH_RECORD_TOO_LARGE;
        if (cursor + n > body_len) return WEIR_ERR_BATCH_TRUNC_RECORD;
        /* Stop before overrunning the declared count, so a body carrying more
         * records than it declares is a mismatch rather than a silent drop. */
        if (found == declared) return WEIR_ERR_BATCH_LENGTH_MISMATCH;
        out[found].data = body + cursor;
        out[found].len  = n;
        found++;
        cursor += n;
    }
    if (found != declared || cursor != body_len) return WEIR_ERR_BATCH_LENGTH_MISMATCH;
    *out_count = found;
    return WEIR_OK;
}

weir_result weir_encode_ack_batch(const uint8_t *accepted, size_t count,
                                  uint8_t *out, size_t out_cap, size_t *out_len) {
    size_t need = (size_t)WEIR_ACK_BATCH_HEADER_LEN + (count + 7) / 8;
    if (out_cap < need) return WEIR_ERR_BUF_TOO_SMALL;
    memset(out, 0, need);
    out[0] = WEIR_ACK_BATCH_VERSION;
    out[1] = (uint8_t)(count & 0xFF);
    out[2] = (uint8_t)((count >> 8) & 0xFF);
    for (size_t i = 0; i < count; i++) {
        if (accepted[i]) {
            out[WEIR_ACK_BATCH_HEADER_LEN + i / 8] |= (uint8_t)(1u << (i % 8));
        }
    }
    *out_len = need;
    return WEIR_OK;
}

weir_result weir_decode_ack_batch(const uint8_t *payload, size_t payload_len,
                                  size_t expected,
                                  uint8_t *out, size_t out_cap) {
    if (payload_len < WEIR_ACK_BATCH_HEADER_LEN) return WEIR_ERR_BATCH_TRUNCATED;
    if (payload[0] != WEIR_ACK_BATCH_VERSION)    return WEIR_ERR_BATCH_BAD_VERSION;

    size_t declared = (size_t)payload[1] | ((size_t)payload[2] << 8);
    if (declared == 0)        return WEIR_ERR_BATCH_EMPTY;
    if (declared != expected) return WEIR_ERR_BATCH_LENGTH_MISMATCH;
    if (payload_len != (size_t)WEIR_ACK_BATCH_HEADER_LEN + (declared + 7) / 8) {
        return WEIR_ERR_BATCH_LENGTH_MISMATCH;
    }
    if (declared > out_cap) return WEIR_ERR_BUF_TOO_SMALL;

    /* Padding bits must be zero. Ignoring them would make popcount == N -- the
     * obvious way to ask "did the whole batch succeed" -- silently wrong. */
    size_t used = declared % 8;
    if (used != 0) {
        uint8_t mask = (uint8_t)~(uint8_t)((1u << used) - 1u);
        if (payload[payload_len - 1] & mask) return WEIR_ERR_BATCH_PADDING_NOT_ZERO;
    }

    const uint8_t *bits = payload + WEIR_ACK_BATCH_HEADER_LEN;
    for (size_t i = 0; i < declared; i++) {
        out[i] = (bits[i / 8] & (uint8_t)(1u << (i % 8))) ? 1u : 0u;
    }
    return WEIR_OK;
}
