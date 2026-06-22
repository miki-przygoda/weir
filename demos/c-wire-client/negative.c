/*
 * negative.c — live error-path coverage against a running daemon.
 *
 * Crafts intentionally malformed frames and confirms (a) the daemon Nacks
 * with the spec-mandated reason, and (b) our client decodes that Nack.
 * Each malformed frame is sent on a FRESH connection because the daemon
 * closes the connection after a permanent error (per the spec table).
 *
 * Usage: negative <socket_path>
 */
#include "weir_conn.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_pass = 0, g_fail = 0;

/* Send a raw byte buffer on a fresh connection, expect a Nack(reason). */
static void expect_nack(const char *sock, const char *label,
                        const uint8_t *frame, size_t flen,
                        uint8_t want_reason) {
    int fd = weir_connect(sock);
    if (fd < 0) { printf("  FAIL [%s] connect: %s\n", label, strerror(errno)); g_fail++; return; }

    if (weir_send_all(fd, frame, flen) != WEIR_OK) {
        printf("  FAIL [%s] send\n", label); g_fail++; close(fd); return;
    }
    weir_response resp;
    weir_result r = weir_recv_response(fd, &resp);
    close(fd);
    if (r != WEIR_OK) {
        printf("  FAIL [%s] recv: %s\n", label, weir_result_str(r)); g_fail++; return;
    }
    if (!resp.is_nack) {
        printf("  FAIL [%s] expected Nack, got %s\n", label,
               weir_msg_type_str(resp.hdr.message_type)); g_fail++; return;
    }
    if (resp.nack_reason != want_reason) {
        printf("  FAIL [%s] Nack reason 0x%02x (%s), wanted 0x%02x (%s)\n",
               label, resp.nack_reason, weir_nack_reason_str(resp.nack_reason),
               want_reason, weir_nack_reason_str(want_reason)); g_fail++; return;
    }
    printf("  PASS [%s] -> Nack %s (0x%02x)\n",
           label, weir_nack_reason_str(resp.nack_reason), resp.nack_reason);
    g_pass++;
}

static void put_u32_le(uint8_t *p, uint32_t v) {
    p[0]=v&0xFF; p[1]=(v>>8)&0xFF; p[2]=(v>>16)&0xFF; p[3]=(v>>24)&0xFF;
}

int main(int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "usage: %s <socket_path>\n", argv[0]); return 2; }
    const char *sock = argv[1];

    printf("weir C client — live negative-path coverage\n\n");

    /* 1. Empty Push -> EmptyPayload (0x07). Build by hand: the encoder
     *    refuses to build a zero-length Push, so we craft the bytes. */
    {
        uint8_t f[WEIR_HEADER_LEN + 4];
        f[0]='W';f[1]='E';f[2]='I';f[3]='R';
        f[4]=WEIR_WIRE_VERSION; f[5]=WEIR_MSG_PUSH; f[6]=WEIR_DUR_SYNC; f[7]=0;
        put_u32_le(f+8, 0);
        put_u32_le(f+12, weir_crc32(f, 12));
        put_u32_le(f+16, weir_crc32(NULL, 0)); /* empty-payload CRC = 0 */
        expect_nack(sock, "empty Push", f, sizeof(f), WEIR_NACK_EMPTY_PAYLOAD);
    }

    /* 2. Bad payload CRC -> BadPayloadCrc (0x05). Valid header, wrong CRC. */
    {
        const uint8_t payload[] = {0xDE, 0xAD};
        uint8_t f[WEIR_HEADER_LEN + 2 + 4];
        f[0]='W';f[1]='E';f[2]='I';f[3]='R';
        f[4]=WEIR_WIRE_VERSION; f[5]=WEIR_MSG_PUSH; f[6]=WEIR_DUR_SYNC; f[7]=0;
        put_u32_le(f+8, 2);
        put_u32_le(f+12, weir_crc32(f, 12));
        memcpy(f+16, payload, 2);
        put_u32_le(f+18, weir_crc32(payload, 2) ^ 0xFFFFFFFFu); /* corrupt */
        expect_nack(sock, "bad payload CRC", f, sizeof(f), WEIR_NACK_BAD_PAYLOAD_CRC);
    }

    /* 3. Bad magic -> BadMagic (0x01). */
    {
        uint8_t f[WEIR_HEADER_LEN + 1 + 4];
        f[0]='X';f[1]='X';f[2]='X';f[3]='X';
        f[4]=WEIR_WIRE_VERSION; f[5]=WEIR_MSG_PUSH; f[6]=WEIR_DUR_SYNC; f[7]=0;
        put_u32_le(f+8, 1);
        put_u32_le(f+12, weir_crc32(f, 12));
        f[16]=0x41;
        put_u32_le(f+17, weir_crc32(f+16, 1));
        expect_nack(sock, "bad magic", f, sizeof(f), WEIR_NACK_BAD_MAGIC);
    }

    /* 4. Reserved flags set -> ReservedFlagsSet (0x09). */
    {
        const uint8_t payload[] = {0x42};
        uint8_t f[WEIR_HEADER_LEN + 1 + 4];
        f[0]='W';f[1]='E';f[2]='I';f[3]='R';
        f[4]=WEIR_WIRE_VERSION; f[5]=WEIR_MSG_PUSH; f[6]=WEIR_DUR_SYNC; f[7]=0x01; /* flag */
        put_u32_le(f+8, 1);
        put_u32_le(f+12, weir_crc32(f, 12));
        f[16]=payload[0];
        put_u32_le(f+17, weir_crc32(payload, 1));
        expect_nack(sock, "reserved flags", f, sizeof(f), WEIR_NACK_RESERVED_FLAGS);
    }

    /* 5. Client sends a daemon->client type (Ack) -> UnknownMessage (0x08). */
    {
        uint8_t f[WEIR_HEADER_LEN + 1 + 4];
        f[0]='W';f[1]='E';f[2]='I';f[3]='R';
        f[4]=WEIR_WIRE_VERSION; f[5]=WEIR_MSG_ACK; f[6]=WEIR_DUR_SYNC; f[7]=0;
        put_u32_le(f+8, 1);
        put_u32_le(f+12, weir_crc32(f, 12));
        f[16]=0x00;
        put_u32_le(f+17, weir_crc32(f+16, 1));
        expect_nack(sock, "client sends Ack", f, sizeof(f), WEIR_NACK_UNKNOWN_MESSAGE);
    }

    printf("\nnegative checks passed: %d  failed: %d\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
