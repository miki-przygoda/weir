package dev.weir.client;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Validates this Java codec against the canonical wire conformance vectors
 * ({@code docs/conformance/wire_v1_vectors.json}, 29 vectors).
 *
 * <p>For each vector:
 * <ul>
 *   <li>{@code decode == "ok"} → {@link Frame#decode} must succeed and the
 *       decoded message_type / durability / flags / payload must match.</li>
 *   <li>otherwise → {@link Frame#decode} must throw with a matching
 *       {@link Frame.DecodeError} tag.</li>
 * </ul>
 *
 * <p>Also performs an <b>encode round-trip</b> on every {@code ok} vector: it
 * re-encodes the decoded frame and asserts the bytes are byte-identical to the
 * vector hex. This catches CRC / endianness bugs in the encoder, not just the
 * decoder. (Daemon→client frames — Ack / Nack / HealthCheckResponse — are
 * decode-only; the client never encodes those, so their round-trip is skipped.)
 *
 * <p>Usage: {@code java ... ConformanceRunner <path-to-vectors.json>}
 */
public final class ConformanceRunner {

    public static void main(String[] args) throws Exception {
        // Vectors path: arg > WEIR_VECTORS env > the canonical file relative to
        // the repo (this demo lives at demos/java-wire-client/). No vendored copy.
        String vectorsArg = args.length >= 1
            ? args[0]
            : System.getenv().getOrDefault(
                "WEIR_CONFORMANCE_VECTORS", "../../docs/conformance/wire_v1_vectors.json");
        Path vectorsPath = Path.of(vectorsArg);
        String json = Files.readString(vectorsPath);

        @SuppressWarnings("unchecked")
        Map<String, Object> root = (Map<String, Object>) MiniJson.parse(json);
        @SuppressWarnings("unchecked")
        List<Object> vectors = (List<Object>) root.get("vectors");

        int total = 0;
        int passed = 0;
        int roundTrips = 0;
        StringBuilder failures = new StringBuilder();

        for (Object vo : vectors) {
            @SuppressWarnings("unchecked")
            Map<String, Object> v = (Map<String, Object>) vo;
            String name = (String) v.get("name");
            String hex = (String) v.get("hex");
            String expectDecode = (String) v.get("decode");
            byte[] bytes = Hex.decode(hex);
            total++;

            try {
                if ("ok".equals(expectDecode)) {
                    Frame f = Frame.decode(bytes);
                    String expMt = (String) v.get("message_type");
                    String expDur = (String) v.get("durability");
                    long expFlags = ((Number) v.get("flags")).longValue();
                    String expPayloadHex = (String) v.get("payload_hex");

                    check(name, "message_type", expMt, toSpecName(f.messageType));
                    check(name, "durability", expDur, toSpecName(f.durability));
                    check(name, "flags", String.valueOf(expFlags), String.valueOf(f.flags));
                    check(name, "payload", expPayloadHex == null ? "" : expPayloadHex,
                          Hex.encode(f.payload));

                    // Encode round-trip for client-emittable frames only.
                    if (isClientEmittable(f.messageType)) {
                        byte[] reencoded = f.encode();
                        if (!Hex.encode(reencoded).equals(hex)) {
                            throw new AssertionError(
                                "encode round-trip mismatch:\n  vector:    " + hex
                                + "\n  re-encoded: " + Hex.encode(reencoded));
                        }
                        roundTrips++;
                    }
                    passed++;
                    System.out.printf("  PASS  %-28s decode=ok%s%n", name,
                        isClientEmittable(f.messageType) ? " (+round-trip)" : "");
                } else {
                    // Expect a rejection with a matching tag.
                    try {
                        Frame.decode(bytes);
                        throw new AssertionError(
                            "expected rejection " + expectDecode + " but decode succeeded");
                    } catch (ProtocolException pe) {
                        if (pe.decodeError == null) {
                            throw new AssertionError(
                                "rejected, but with no decode tag: " + pe.getMessage());
                        }
                        String got = pe.decodeError.vectorName;
                        if (!got.equals(expectDecode)) {
                            throw new AssertionError(
                                "expected " + expectDecode + " but got " + got
                                + " (" + pe.getMessage() + ")");
                        }
                        passed++;
                        System.out.printf("  PASS  %-28s decode=%s%n", name, expectDecode);
                    }
                }
            } catch (Throwable t) {
                failures.append("  FAIL  ").append(name).append("  ").append(t.getMessage())
                        .append('\n');
                System.out.printf("  FAIL  %-28s %s%n", name, t.getMessage());
            }
        }

        System.out.println();
        System.out.printf("Conformance: %d/%d vectors passed (%d encode round-trips verified)%n",
            passed, total, roundTrips);
        if (passed != total) {
            System.out.println(failures);
            System.exit(1);
        }
        System.out.println("ALL CONFORMANCE VECTORS PASSED");
    }

    private static boolean isClientEmittable(Wire.MessageType mt) {
        return mt == Wire.MessageType.PUSH || mt == Wire.MessageType.HEALTH_CHECK;
    }

    private static String toSpecName(Wire.MessageType mt) {
        switch (mt) {
            case PUSH: return "Push";
            case ACK: return "Ack";
            case NACK: return "Nack";
            case HEALTH_CHECK: return "HealthCheck";
            case HEALTH_CHECK_RESPONSE: return "HealthCheckResponse";
            default: return mt.name();
        }
    }

    private static String toSpecName(Wire.Durability d) {
        switch (d) {
            case SYNC: return "Sync";
            case BATCHED: return "Batched";
            case BUFFERED: return "Buffered";
            default: return d.name();
        }
    }

    private static void check(String vector, String field, String expected, String actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError(
                field + " mismatch: expected '" + expected + "' got '" + actual + "'");
        }
    }
}
