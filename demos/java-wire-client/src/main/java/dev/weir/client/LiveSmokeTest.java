package dev.weir.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Exercises happy path AND rejection paths against a LIVE weir daemon, to prove
 * the client reads real Nack frames correctly (not just synthetic vectors).
 *
 * <p>Each negative case opens a FRESH connection because the daemon closes the
 * connection after a permanent protocol error.
 *
 * <p>Usage: {@code java ... LiveSmokeTest <socket-path>}
 */
public final class LiveSmokeTest {

    private static int checks = 0;
    private static int ok = 0;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: LiveSmokeTest <socket-path>");
            System.exit(2);
        }
        Path socket = Path.of(args[0]);
        System.out.println("=== weir live smoke test (Java) ===");

        // 1. HealthCheck.
        try (WeirClient c = WeirClient.connect(socket)) {
            Frame r = c.healthCheck();
            assertThat("healthcheck -> HealthCheckResponse",
                r.messageType == Wire.MessageType.HEALTH_CHECK_RESPONSE);
            assertThat("healthcheck response empty payload", r.payload.length == 0);
        }

        // 2. Happy push at each durability tier.
        for (Wire.Durability d : Wire.Durability.values()) {
            try (WeirClient c = WeirClient.connect(socket)) {
                Frame ack = c.push(("ping-" + d).getBytes(StandardCharsets.UTF_8), d);
                assertThat("push@" + d + " -> Ack", ack.messageType == Wire.MessageType.ACK);
                assertThat("push@" + d + " ack empty payload", ack.payload.length == 0);
            }
        }

        // 3. Reserved flags set -> Nack(ReservedFlagsSet), connection closed.
        try (WeirClient c = WeirClient.connect(socket)) {
            byte[] frame = rawFrameWithFlags("x".getBytes(StandardCharsets.UTF_8), 0x01);
            Frame resp = c.sendRawAndRead(frame);
            assertThat("reserved-flags -> Nack",
                resp.messageType == Wire.MessageType.NACK);
            int reason = resp.payload[0] & 0xFF;
            assertThat("reserved-flags -> ReservedFlagsSet(0x09)",
                Wire.NackReason.fromByte(reason) == Wire.NackReason.RESERVED_FLAGS_SET);
        } catch (ProtocolException | IOException e) {
            assertThat("reserved-flags path raised but unexpectedly: " + e.getMessage(), false);
        }

        // 4. Oversized payload -> Nack(PayloadTooLarge). Use a frame that declares
        //    a payload_len above the daemon's effective cap. We build a raw frame
        //    whose declared payload_len exceeds the default 16 MiB to force it,
        //    but keep the actual bytes small — the daemon rejects on the header's
        //    declared length BEFORE reading the payload (decode step 5).
        try (WeirClient c = WeirClient.connect(socket)) {
            byte[] frame = oversizeDeclaredFrame();
            Frame resp = c.sendRawAndRead(frame);
            assertThat("oversize -> Nack", resp.messageType == Wire.MessageType.NACK);
            int reason = resp.payload[0] & 0xFF;
            assertThat("oversize -> PayloadTooLarge(0x04)",
                Wire.NackReason.fromByte(reason) == Wire.NackReason.PAYLOAD_TOO_LARGE);
        }

        // 5. Empty Push guarded client-side (would be Nack(EmptyPayload) on the wire).
        try (WeirClient c = WeirClient.connect(socket)) {
            boolean threw = false;
            try {
                c.push(new byte[0], Wire.Durability.SYNC);
            } catch (ProtocolException pe) {
                threw = true;
            }
            assertThat("empty Push guarded client-side", threw);
        }

        System.out.println();
        System.out.printf("Live smoke test: %d/%d checks passed.%n", ok, checks);
        if (ok != checks) {
            System.exit(1);
        }
        System.out.println("LIVE SMOKE TEST PASSED");
    }

    /** Builds a Push frame but stamps a nonzero reserved flags byte (bypassing Frame.encode's guard). */
    private static byte[] rawFrameWithFlags(byte[] payload, int flags) {
        // Hand-assemble so we can violate the flags==0 invariant on purpose.
        int total = Wire.HEADER_LEN + payload.length + Wire.CRC_LEN;
        byte[] b = new byte[total];
        System.arraycopy(Wire.MAGIC, 0, b, 0, 4);
        b[4] = (byte) Wire.WIRE_VERSION;
        b[5] = Wire.MessageType.PUSH.code;
        b[6] = Wire.Durability.SYNC.code;
        b[7] = (byte) (flags & 0xFF);
        putIntLE(b, 8, payload.length);
        putIntLE(b, 12, (int) Frame.crc32(b, 0, 12));
        System.arraycopy(payload, 0, b, 16, payload.length);
        putIntLE(b, 16 + payload.length, (int) Frame.crc32(payload, 0, payload.length));
        return b;
    }

    /**
     * Builds a 16-byte header (only) that declares payload_len = HARD_CAP + 1.
     * The daemon caps before allocation/payload read, so it never reads further.
     * This matches the conformance vector {@code reject_payload_too_large}.
     */
    private static byte[] oversizeDeclaredFrame() {
        byte[] b = new byte[Wire.HEADER_LEN];
        System.arraycopy(Wire.MAGIC, 0, b, 0, 4);
        b[4] = (byte) Wire.WIRE_VERSION;
        b[5] = Wire.MessageType.PUSH.code;
        b[6] = Wire.Durability.SYNC.code;
        b[7] = 0;
        putIntLE(b, 8, Wire.MAX_PAYLOAD_HARD_CAP + 1);
        putIntLE(b, 12, (int) Frame.crc32(b, 0, 12));
        return b;
    }

    private static void putIntLE(byte[] b, int off, int v) {
        b[off] = (byte) (v & 0xFF);
        b[off + 1] = (byte) ((v >>> 8) & 0xFF);
        b[off + 2] = (byte) ((v >>> 16) & 0xFF);
        b[off + 3] = (byte) ((v >>> 24) & 0xFF);
    }

    private static void assertThat(String label, boolean cond) {
        checks++;
        if (cond) {
            ok++;
            System.out.println("  PASS  " + label);
        } else {
            System.out.println("  FAIL  " + label);
        }
    }
}
