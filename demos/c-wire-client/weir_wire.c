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

weir_result weir_encode_push(weir_durability dur,
                             const uint8_t *payload, size_t payload_len,
                             uint8_t *out, size_t out_cap, size_t *out_len) {
    if (payload_len == 0) return WEIR_ERR_EMPTY_PAYLOAD;
    if (payload_len > WEIR_MAX_PAYLOAD_HARD_CAP) return WEIR_ERR_PAYLOAD_TOO_LARGE;

    size_t total = WEIR_HEADER_LEN + payload_len + WEIR_CRC_LEN;
    if (out_cap < total) return WEIR_ERR_BUF_TOO_SMALL;

    write_header(out, WEIR_MSG_PUSH, dur, (uint32_t)payload_len);
    memcpy(out + WEIR_HEADER_LEN, payload, payload_len);
    put_u32_le(out + WEIR_HEADER_LEN + payload_len,
               weir_crc32(payload, payload_len));

    if (out_len) *out_len = total;
    return WEIR_OK;
}

weir_result weir_encode_healthcheck(uint8_t *out, size_t out_cap,
                                    size_t *out_len) {
    size_t total = WEIR_HEADER_LEN + 0 + WEIR_CRC_LEN;
    if (out_cap < total) return WEIR_ERR_BUF_TOO_SMALL;
    /* Durability must still be a valid byte even though it is unused; the
     * daemon validates the whole header before dispatching. Use Sync. */
    write_header(out, WEIR_MSG_HEALTHCHECK, WEIR_DUR_SYNC, 0);
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
    if (plen > WEIR_MAX_RESPONSE_PAYLOAD) return WEIR_ERR_RESP_TOO_LARGE;

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
