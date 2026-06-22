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
    r = read_exact(fd, tail, tail_len);
    if (r != WEIR_OK) return r;

    /* Verify payload CRC. */
    uint32_t want =
        (uint32_t)tail[resp->hdr.payload_len]
      | ((uint32_t)tail[resp->hdr.payload_len + 1] << 8)
      | ((uint32_t)tail[resp->hdr.payload_len + 2] << 16)
      | ((uint32_t)tail[resp->hdr.payload_len + 3] << 24);
    uint32_t got = weir_crc32(tail, resp->hdr.payload_len);
    if (want != got) return WEIR_ERR_BAD_HEADER_CRC; /* reuse: bad CRC */

    memcpy(resp->payload, tail, resp->hdr.payload_len);
    resp->payload_len = resp->hdr.payload_len;
    resp->is_nack = (resp->hdr.message_type == WEIR_MSG_NACK);
    resp->nack_reason = (resp->is_nack && resp->payload_len >= 1)
                          ? resp->payload[0] : 0;
    return WEIR_OK;
}
