/*
 * telemetry.c — an embedded-systems telemetry producer for weir.
 *
 * Simulates a fleet of sensor nodes emitting periodic readings (a tiny
 * fixed-width binary record: the kind of compact payload an MCU would
 * actually push) and streams them to weir-server over a Unix socket.
 *
 * It first issues a HealthCheck (the correct no-payload liveness probe),
 * then Pushes N records at a chosen durability tier, handling Ack/Nack
 * and connection-close per the spec.
 *
 * Usage:
 *   telemetry <socket_path> [count] [durability: sync|batched|buffered]
 *
 * Payload format (16 bytes, little-endian — a deliberately MCU-friendly
 * fixed record, not JSON):
 *   u32 node_id
 *   u32 seq
 *   i16 temp_centi_c     (centidegrees C, e.g. 2350 == 23.50 C)
 *   u16 humidity_basis   (0.01% units, e.g. 4512 == 45.12%)
 *   u32 uptime_ms
 */
#include "weir_conn.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void put_u32_le(uint8_t *p, uint32_t v) {
    p[0] = v & 0xFF; p[1] = (v >> 8) & 0xFF;
    p[2] = (v >> 16) & 0xFF; p[3] = (v >> 24) & 0xFF;
}
static void put_u16_le(uint8_t *p, uint16_t v) {
    p[0] = v & 0xFF; p[1] = (v >> 8) & 0xFF;
}

#define TELEM_RECORD_LEN 16

static void build_record(uint8_t *rec, uint32_t node_id, uint32_t seq) {
    /* Deterministic pseudo-sensor values so a demo run is reproducible. */
    int16_t  temp = (int16_t)(2000 + (int)((seq * 37u) % 1500));   /* 20.00..34.99 C */
    uint16_t hum  = (uint16_t)(3000 + ((seq * 53u) % 4000));        /* 30.00..69.99 % */
    uint32_t up   = seq * 250u;                                     /* 250 ms cadence */
    put_u32_le(rec + 0, node_id);
    put_u32_le(rec + 4, seq);
    put_u16_le(rec + 8, (uint16_t)temp);
    put_u16_le(rec + 10, hum);
    put_u32_le(rec + 12, up);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr,
            "usage: %s <socket_path> [count] [sync|batched|buffered]\n",
            argv[0]);
        return 2;
    }
    const char *sock = argv[1];
    int count = (argc >= 3) ? atoi(argv[2]) : 8;
    if (count <= 0) count = 8;

    weir_durability dur = WEIR_DUR_SYNC;
    const char *dur_name = "Sync";
    if (argc >= 4) {
        if (strcmp(argv[3], "batched") == 0)      { dur = WEIR_DUR_BATCHED;  dur_name = "Batched"; }
        else if (strcmp(argv[3], "buffered") == 0){ dur = WEIR_DUR_BUFFERED; dur_name = "Buffered"; }
        else if (strcmp(argv[3], "sync") == 0)    { dur = WEIR_DUR_SYNC;     dur_name = "Sync"; }
        else { fprintf(stderr, "unknown durability '%s'\n", argv[3]); return 2; }
    }

    int fd = weir_connect(sock);
    if (fd < 0) {
        fprintf(stderr, "connect(%s): %s\n", sock, strerror(errno));
        return 1;
    }
    printf("connected to weir daemon at %s\n", sock);

    /* --- liveness probe: HealthCheck (the correct no-payload frame) --- */
    {
        uint8_t frame[64];
        size_t flen = 0;
        weir_result r = weir_encode_healthcheck(frame, sizeof(frame), &flen);
        if (r != WEIR_OK) { fprintf(stderr, "encode hc: %s\n", weir_result_str(r)); close(fd); return 1; }
        if (weir_send_all(fd, frame, flen) != WEIR_OK) {
            fprintf(stderr, "send hc: %s\n", strerror(errno)); close(fd); return 1;
        }
        weir_response resp;
        r = weir_recv_response(fd, &resp);
        if (r != WEIR_OK) { fprintf(stderr, "recv hc: %s\n", weir_result_str(r)); close(fd); return 1; }
        printf("healthcheck -> %s\n", weir_msg_type_str(resp.hdr.message_type));
        if (resp.hdr.message_type != WEIR_MSG_HEALTHCHECK_RESPONSE) {
            fprintf(stderr, "unexpected healthcheck reply\n"); close(fd); return 1;
        }
    }

    /* --- stream telemetry records --- */
    printf("streaming %d records at %s durability...\n", count, dur_name);
    const uint32_t node_id = 0xA1B2C3D4u;
    int acked = 0, nacked = 0;
    for (int i = 0; i < count; i++) {
        uint8_t rec[TELEM_RECORD_LEN];
        build_record(rec, node_id, (uint32_t)i);

        uint8_t frame[64];
        size_t flen = 0;
        weir_result r = weir_encode_push(dur, rec, sizeof(rec),
                                         frame, sizeof(frame), &flen);
        if (r != WEIR_OK) {
            fprintf(stderr, "encode push #%d: %s\n", i, weir_result_str(r));
            break;
        }
        if (weir_send_all(fd, frame, flen) != WEIR_OK) {
            fprintf(stderr, "send push #%d: %s\n", i, strerror(errno));
            break;
        }

        weir_response resp;
        r = weir_recv_response(fd, &resp);
        if (r == WEIR_ERR_SHORT_READ) {
            /* Server closed mid-stream: this Push's outcome is UNKNOWN. */
            fprintf(stderr,
                "record #%d: connection closed — outcome UNKNOWN, retry on fresh conn\n", i);
            break;
        }
        if (r != WEIR_OK) {
            fprintf(stderr, "recv push #%d: %s\n", i, weir_result_str(r));
            break;
        }

        if (resp.is_nack) {
            nacked++;
            int transient = (resp.nack_reason == WEIR_NACK_INTERNAL_ERROR);
            fprintf(stderr, "record #%d: NACK %s (0x%02x)%s\n",
                    i, weir_nack_reason_str(resp.nack_reason), resp.nack_reason,
                    transient ? " [transient — connection kept open, retry]"
                              : " [permanent — connection closed]");
            if (!transient) break; /* connection is closed for permanent errors */
        } else if (resp.hdr.message_type == WEIR_MSG_ACK) {
            acked++;
        } else {
            fprintf(stderr, "record #%d: unexpected reply %s\n",
                    i, weir_msg_type_str(resp.hdr.message_type));
            break;
        }
    }

    close(fd);
    printf("done: %d acked, %d nacked (of %d attempted)\n", acked, nacked, count);
    return (acked == count) ? 0 : 1;
}
