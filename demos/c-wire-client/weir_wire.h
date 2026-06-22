/*
 * weir_wire.h — a dependency-free C implementation of the weir v1 wire
 * protocol, built purely from docs/wire_protocol.md + the conformance
 * vectors. No weir crate is linked; this is a clean polyglot client.
 *
 * Scope: header/payload framing, CRC-32 (ISO-3309), Push/HealthCheck
 * encode, and Ack/Nack/HealthCheckResponse decode. Suitable for an
 * embedded telemetry producer talking to weir-server over a Unix socket.
 *
 * Wire v1, frozen. See docs/wire_protocol.md "Frame layout".
 */
#ifndef WEIR_WIRE_H
#define WEIR_WIRE_H

#include <stddef.h>
#include <stdint.h>

#define WEIR_WIRE_VERSION   1
#define WEIR_HEADER_LEN     16
#define WEIR_CRC_LEN        4
#define WEIR_MAGIC_0 'W'
#define WEIR_MAGIC_1 'E'
#define WEIR_MAGIC_2 'I'
#define WEIR_MAGIC_3 'R'

/* MAX_PAYLOAD_HARD_CAP — absolute ceiling across all code paths (16 MiB). */
#define WEIR_MAX_PAYLOAD_HARD_CAP  (16u * 1024u * 1024u)

/* Response payloads are <= 2 bytes; never allocate an attacker-chosen size. */
#define WEIR_MAX_RESPONSE_PAYLOAD  2

/* MessageType bytes (docs: "Message types"). */
typedef enum {
    WEIR_MSG_PUSH                 = 0x01, /* client -> daemon */
    WEIR_MSG_ACK                  = 0x02, /* daemon -> client */
    WEIR_MSG_NACK                 = 0x03, /* daemon -> client */
    WEIR_MSG_HEALTHCHECK          = 0x04, /* client -> daemon */
    WEIR_MSG_HEALTHCHECK_RESPONSE = 0x05  /* daemon -> client */
} weir_msg_type;

/* Durability tiers (docs: "Durability tiers"). */
typedef enum {
    WEIR_DUR_SYNC     = 0x01,
    WEIR_DUR_BATCHED  = 0x02,
    WEIR_DUR_BUFFERED = 0x03
} weir_durability;

/* NackReason bytes (docs: "Nack payload format"). */
typedef enum {
    WEIR_NACK_BAD_MAGIC         = 0x01,
    WEIR_NACK_VERSION_MISMATCH  = 0x02,
    WEIR_NACK_BAD_HEADER_CRC    = 0x03,
    WEIR_NACK_PAYLOAD_TOO_LARGE = 0x04,
    WEIR_NACK_BAD_PAYLOAD_CRC   = 0x05,
    WEIR_NACK_INTERNAL_ERROR    = 0x06,
    WEIR_NACK_EMPTY_PAYLOAD     = 0x07,
    WEIR_NACK_UNKNOWN_MESSAGE   = 0x08,
    WEIR_NACK_RESERVED_FLAGS    = 0x09
    /* 0x0A..0xFF reserved; surface the raw byte. */
} weir_nack_reason;

/* Result codes for the codec / IO helpers. */
typedef enum {
    WEIR_OK = 0,
    WEIR_ERR_BUF_TOO_SMALL    = -1, /* output buffer cannot hold the frame   */
    WEIR_ERR_PAYLOAD_TOO_LARGE= -2, /* payload exceeds the hard cap          */
    WEIR_ERR_EMPTY_PAYLOAD    = -3, /* zero-length Push (rejected by daemon) */
    WEIR_ERR_BAD_MAGIC        = -4,
    WEIR_ERR_BAD_VERSION      = -5,
    WEIR_ERR_BAD_HEADER_CRC   = -6,
    WEIR_ERR_RESP_TOO_LARGE   = -7, /* response declares > 2-byte payload    */
    WEIR_ERR_SHORT_READ       = -8, /* peer closed / truncated response      */
    WEIR_ERR_IO               = -9, /* errno set                             */
    WEIR_ERR_RESERVED_FLAGS   = -10
} weir_result;

/* CRC-32 / ISO-3309 (zlib / crc32fast). poly 0x04C11DB7, refin/refout,
 * init 0xFFFFFFFF, xorout 0xFFFFFFFF. */
uint32_t weir_crc32(const uint8_t *data, size_t len);

/*
 * Encode a Push frame into out (capacity out_cap).
 * payload must be non-empty (a zero-length Push is rejected by the daemon).
 * On success writes the full frame and sets *out_len to its size.
 */
weir_result weir_encode_push(weir_durability dur,
                             const uint8_t *payload, size_t payload_len,
                             uint8_t *out, size_t out_cap, size_t *out_len);

/* Encode a zero-payload HealthCheck frame (the correct no-payload probe). */
weir_result weir_encode_healthcheck(uint8_t *out, size_t out_cap,
                                    size_t *out_len);

/* A decoded response header. */
typedef struct {
    uint8_t  version;
    uint8_t  message_type;
    uint8_t  durability;   /* filler on responses; ignore per spec */
    uint8_t  flags;
    uint32_t payload_len;
} weir_resp_header;

/*
 * Validate + parse a 16-byte response header: checks magic, version, and
 * header CRC, then caps payload_len at WEIR_MAX_RESPONSE_PAYLOAD.
 */
weir_result weir_decode_resp_header(const uint8_t hdr[WEIR_HEADER_LEN],
                                    weir_resp_header *out);

/* Human-readable names for diagnostics. */
const char *weir_nack_reason_str(uint8_t reason);
const char *weir_msg_type_str(uint8_t mt);
const char *weir_result_str(weir_result r);

#endif /* WEIR_WIRE_H */
