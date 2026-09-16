/*
 * weir_conn.c — POSIX Unix-socket transport. Stdlib + POSIX only.
 */
#include "weir_conn.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

int weir_connect(const char *socket_path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    /* Leave room for the NUL terminator. */
    if (strlen(socket_path) >= sizeof(addr.sun_path)) {
        close(fd);
        errno = ENAMETOOLONG;
        return -1;
    }
    strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        int e = errno;
        close(fd);
        errno = e;
        return -1;
    }
    return fd;
}

weir_result weir_send_all(int fd, const uint8_t *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return WEIR_ERR_IO;
        }
        if (n == 0) return WEIR_ERR_IO;
        off += (size_t)n;
    }
    return WEIR_OK;
}

/* Read exactly len bytes, looping over partial reads. */
static weir_result read_exact(int fd, uint8_t *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = read(fd, buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return WEIR_ERR_IO;
        }
        if (n == 0) return WEIR_ERR_SHORT_READ; /* peer closed */
        off += (size_t)n;
    }
    return WEIR_OK;
}

weir_result weir_recv_response(int fd, weir_response *resp) {
    uint8_t hdr[WEIR_HEADER_LEN];
    weir_result r = read_exact(fd, hdr, WEIR_HEADER_LEN);
    if (r != WEIR_OK) return r;

    r = weir_decode_resp_header(hdr, &resp->hdr);
    if (r != WEIR_OK) return r;

    /* Frame the rest: exactly payload_len + 4 CRC bytes. */
    uint8_t tail[WEIR_MAX_RESPONSE_PAYLOAD + WEIR_CRC_LEN];
    size_t tail_len = resp->hdr.payload_len + WEIR_CRC_LEN;
    /* This buffer is sized for the 2-byte cap, and weir_decode_resp_header no
     * longer enforces that cap -- it enforces the cap for the DECLARED TYPE,
     * which is 298 for AckTracked and 259 for AckBatch. Without this check a
     * conformant daemon's AckTracked overflows `tail` by 296 bytes, and it does
     * not take a hostile peer: this client's own batch codec has no receiver of
     * its own, so a program using it reads its AckBatch through this function.
     *
     * weir_recv_tracked_response has carried the identical guard since it was
     * written; this one was left behind when the cap became per-type.
     * Responses wider than this frame belong to the callers that allocate for
     * them. */
    if (tail_len > sizeof tail) return WEIR_ERR_RESP_TOO_LARGE;
    r = read_exact(fd, tail, tail_len);
    if (r != WEIR_OK) return r;

    /* Verify payload CRC. */
    uint32_t want =
        (uint32_t)tail[resp->hdr.payload_len]
      | ((uint32_t)tail[resp->hdr.payload_len + 1] << 8)
      | ((uint32_t)tail[resp->hdr.payload_len + 2] << 16)
      | ((uint32_t)tail[resp->hdr.payload_len + 3] << 24);
    uint32_t got = weir_crc32(tail, resp->hdr.payload_len);
    if (want != got) return WEIR_ERR_BAD_PAYLOAD_CRC;

    memcpy(resp->payload, tail, resp->hdr.payload_len);
    resp->payload_len = resp->hdr.payload_len;
    resp->is_nack = (resp->hdr.message_type == WEIR_MSG_NACK);
    resp->nack_reason = (resp->is_nack && resp->payload_len >= 1)
                          ? resp->payload[0] : 0;
    return WEIR_OK;
}

weir_result weir_recv_tracked_response(int fd, weir_tracked_response *resp) {
    uint8_t hdr[WEIR_HEADER_LEN];
    weir_result r = read_exact(fd, hdr, WEIR_HEADER_LEN);
    if (r != WEIR_OK) return r;

    r = weir_decode_resp_header(hdr, &resp->hdr);
    if (r != WEIR_OK) return r;

    /* The wider read lives HERE, on this function's stack, so a program that
     * never pushes tracked pays nothing for it. */
    uint8_t tail[WEIR_MAX_TRACKED_ACK_PAYLOAD + WEIR_CRC_LEN];
    size_t tail_len = resp->hdr.payload_len + WEIR_CRC_LEN;
    if (tail_len > sizeof tail) return WEIR_ERR_RESP_TOO_LARGE;
    r = read_exact(fd, tail, tail_len);
    if (r != WEIR_OK) return r;

    uint32_t want =
        (uint32_t)tail[resp->hdr.payload_len]
      | ((uint32_t)tail[resp->hdr.payload_len + 1] << 8)
      | ((uint32_t)tail[resp->hdr.payload_len + 2] << 16)
      | ((uint32_t)tail[resp->hdr.payload_len + 3] << 24);
    uint32_t got = weir_crc32(tail, resp->hdr.payload_len);
    if (want != got) return WEIR_ERR_BAD_PAYLOAD_CRC;

    resp->is_nack = (resp->hdr.message_type == WEIR_MSG_NACK);
    resp->nack_reason = (resp->is_nack && resp->hdr.payload_len >= 1) ? tail[0] : 0;

    if (resp->is_nack) {
        /* UnknownMessage here means the daemon predates PushTracked. The wire
         * cannot distinguish that from any other unknown type; only the caller
         * knows which question it asked, which is why this is decided here and
         * not in the decoder. */
        return resp->nack_reason == WEIR_NACK_UNKNOWN_MESSAGE
             ? WEIR_ERR_TRACKED_UNSUPPORTED
             : WEIR_OK;
    }
    if (resp->hdr.message_type == WEIR_MSG_ACK) {
        return WEIR_ERR_TRACKED_BARE_ACK;
    }
    if (resp->hdr.message_type != WEIR_MSG_ACK_TRACKED) {
        return WEIR_ERR_BAD_MAGIC; /* not a reply shape we asked for */
    }
    return weir_decode_coordinate(tail, resp->hdr.payload_len, &resp->coord);
}

weir_result weir_push_tracked(int fd, const uint8_t *payload, size_t payload_len,
                              weir_durability dur, weir_tracked_response *resp) {
    uint8_t frame[WEIR_HEADER_LEN + 4096 + WEIR_CRC_LEN];
    size_t frame_len = 0;
    weir_result r = weir_encode_push_tracked(payload, payload_len, dur,
                                             frame, sizeof frame, &frame_len);
    if (r != WEIR_OK) return r;
    r = weir_send_all(fd, frame, frame_len);
    if (r != WEIR_OK) return r;
    return weir_recv_tracked_response(fd, resp);
}
