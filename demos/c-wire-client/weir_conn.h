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

#endif /* WEIR_CONN_H */
