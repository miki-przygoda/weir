/*
 * weir_conn.h — blocking POSIX Unix-socket transport for a weir producer.
 * Frames the stream the way the spec mandates: read the 16-byte header,
 * take payload_len, then read exactly payload_len + 4 more bytes.
 */
#ifndef WEIR_CONN_H
#define WEIR_CONN_H

#include "weir_wire.h"

/* A received response, fully framed and CRC-validated. */
typedef struct {
    weir_resp_header hdr;
    uint8_t  payload[WEIR_MAX_RESPONSE_PAYLOAD];
    size_t   payload_len;
    int      is_nack;        /* convenience: hdr.message_type == Nack */
    uint8_t  nack_reason;    /* valid iff is_nack; payload[0] or 0 */
} weir_response;

/*
 * A received AckTracked, kept SEPARATE from weir_response on purpose.
 *
 * Widening weir_response's payload buffer to 298 would grow every response
 * struct in every caller by 296 bytes, and grow weir_recv_response's stack
 * tail with it, for a frame most programs never receive. A program that never
 * pushes tracked should pay nothing for this feature, so the wider read lives
 * only on this function's stack.
 */
typedef struct {
    weir_resp_header hdr;
    weir_coordinate  coord;      /* valid iff !is_nack and type == AckTracked */
    int      is_nack;
    uint8_t  nack_reason;        /* valid iff is_nack */
} weir_tracked_response;

/* Connect to an AF_UNIX SOCK_STREAM daemon. Returns fd >= 0 or -1 (errno). */
int weir_connect(const char *socket_path);

/* Write all bytes (handles partial writes). 0 on success, WEIR_ERR_* on fail. */
weir_result weir_send_all(int fd, const uint8_t *buf, size_t len);

/*
 * Read exactly one framed response: header, then payload, then payload CRC.
 * Validates header magic/version/CRC, caps payload, and verifies payload CRC.
 * Returns WEIR_OK and fills *resp, or a WEIR_ERR_* code.
 */
weir_result weir_recv_response(int fd, weir_response *resp);

/*
 * Read exactly one AckTracked (or a Nack), decoding the coordinate.
 *
 * Returns WEIR_ERR_TRACKED_UNSUPPORTED when the daemon answers
 * Nack(UnknownMessage) -- it predates PushTracked (0x06). Do not retry on this
 * connection: the daemon closes it. Returns WEIR_ERR_TRACKED_BARE_ACK when the
 * daemon answers a bare Ack, which is an error rather than a success: the
 * request determines the response shape, so an Ack means the peer did not
 * understand the question and the coordinate does not exist.
 */
weir_result weir_recv_tracked_response(int fd, weir_tracked_response *resp);

/* Send one PushTracked and read its reply. */
weir_result weir_push_tracked(int fd, const uint8_t *payload, size_t payload_len,
                              weir_durability dur, weir_tracked_response *resp);

#endif /* WEIR_CONN_H */
