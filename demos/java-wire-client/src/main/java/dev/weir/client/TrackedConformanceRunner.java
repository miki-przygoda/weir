package dev.weir.client;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Runs this client's coordinate codec against wire_v1_tracked_vectors.json.
 *
 * <p>The frozen wire_v1_vectors.json is deliberately untouched by this
 * extension: PushTracked/AckTracked are additive message-type bytes within wire
 * v1, so a client that ignores them stays fully conformant. This runner is for
 * one that does not.
 *
 * <p>Run: {@code java -cp out dev.weir.client.TrackedConformanceRunner <vectors.json>}
 */
public final class TrackedConformanceRunner {

    private static int passed;
    private static int failed;

    private TrackedConformanceRunner() {
    }

    private static void check(String name, boolean cond, String detail) {
        if (cond) {
            passed++;
        } else {
            failed++;
            System.out.printf("  FAIL  %-42s %s%n", name, detail);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: TrackedConformanceRunner <wire_v1_tracked_vectors.json>");
            System.exit(2);
        }
        String text = Files.readString(Path.of(args[0]));
        @SuppressWarnings("unchecked")
        Map<String, Object> doc = (Map<String, Object>) MiniJson.parse(text);

        // ---- Frames ----
        @SuppressWarnings("unchecked")
        List<Object> frames = (List<Object>) doc.get("frame_vectors");
        for (Object o : frames) {
            @SuppressWarnings("unchecked")
            Map<String, Object> v = (Map<String, Object>) o;
            String name = (String) v.get("name");
            byte[] raw = Hex.decode((String) v.get("hex"));
            try {
                Frame f = Frame.decode(raw);
                check(name + ": message_type",
                      typeName(f.messageType).equals(v.get("message_type")),
                      "got " + typeName(f.messageType) + ", want " + v.get("message_type"));
                check(name + ": payload",
                      Hex.encode(f.payload).equals(v.get("payload_hex")),
                      "payload bytes differ");
                // A client-emittable frame must re-encode byte-for-byte, or the
                // encoder and decoder disagree about a layout one is guessing at.
                if (f.messageType == Wire.MessageType.PUSH_TRACKED) {
                    check(name + ": re-encode round-trips",
                          Hex.encode(f.encode()).equals(v.get("hex")), "bytes differ");
                }
            } catch (ProtocolException e) {
                check(name, false, "unexpected rejection: " + e.getMessage());
            }
        }

        // ---- Coordinates ----
        @SuppressWarnings("unchecked")
        List<Object> coords = (List<Object>) doc.get("coordinate_vectors");
        for (Object o : coords) {
            @SuppressWarnings("unchecked")
            Map<String, Object> v = (Map<String, Object>) o;
            String name = (String) v.get("name");
            String want = (String) v.get("decode");
            byte[] raw = Hex.decode((String) v.get("hex"));
            String got;
            RecordCoordinate c = null;
            try {
                c = RecordCoordinate.decode(raw);
                got = "ok";
            } catch (RecordCoordinate.CoordinateException e) {
                got = e.tag.specName;
            }
            check(name + ": verdict", got.equals(want), "got " + got + ", want " + want);
            if ("ok".equals(want) && c != null) {
                check(name + ": segment", c.segment().equals(v.get("segment")), "segment differs");
                // The index is a u64 in a signed long: compare the UNSIGNED
                // rendering, or u64::MAX reads as -1.
                check(name + ": index",
                      c.indexUnsigned().equals(unsignedText(v.get("index"))),
                      "got " + c.indexUnsigned() + ", want " + unsignedText(v.get("index")));
                check(name + ": record_id",
                      c.recordIdHex().equals(v.get("record_id_hex")), "record_id differs");
            }
        }

        // ---- u64::MAX must survive MiniJson ----
        // This is the blocker that had to be fixed before any of the above
        // could run: readNumber used Long.parseLong, which throws on u64::MAX.
        boolean sawMax = false;
        for (Object o : coords) {
            @SuppressWarnings("unchecked")
            Map<String, Object> v = (Map<String, Object>) o;
            if (unsignedText(v.get("index")).equals("18446744073709551615")) {
                sawMax = true;
            }
        }
        check("a vector carries u64::MAX as its index", sawMax,
              "the precision guard no longer guards anything");

        // ---- The response cap, which is what tracked push changes ----
        check("the cap is widened for AckTracked",
              Wire.maxResponsePayload(0x07) == Wire.MAX_TRACKED_ACK_PAYLOAD,
              "got " + Wire.maxResponsePayload(0x07));
        // Swept over ALL 256 type bytes, not over a list of the types that
        // exist today. The list this replaced held 0x01-0x06 and 0xFF, so when
        // AckBatch (0x09) was given a wider cap the check still passed while
        // the property it named had quietly become false.
        StringBuilder wrongCaps = new StringBuilder();
        for (int mt = 0; mt <= 0xFF; mt++) {
            int want = Wire.MAX_RESPONSE_PAYLOAD;
            if (mt == (Wire.MessageType.ACK_TRACKED.code & 0xFF)) {
                want = Wire.MAX_TRACKED_ACK_PAYLOAD;
            } else if (mt == (Wire.MessageType.ACK_BATCH.code & 0xFF)) {
                want = Wire.MAX_ACK_BATCH_PAYLOAD;
            }
            if (Wire.maxResponsePayload(mt) != want) {
                wrongCaps.append(String.format(" 0x%02x:%d!=%d",
                        mt, Wire.maxResponsePayload(mt), want));
            }
        }
        check("the cap is widened for exactly the types that need it",
              wrongCaps.length() == 0, wrongCaps.toString());

        // ---- A record holding a byte[] needs value semantics ----
        // The compiler-generated equals/hashCode use array IDENTITY, so two
        // coordinates with equal bytes would compare unequal. Assert the
        // overrides actually took.
        RecordCoordinate a = new RecordCoordinate("shard_00/x", 7L, new byte[] {1, 2, 3});
        RecordCoordinate b = new RecordCoordinate("shard_00/x", 7L, new byte[] {1, 2, 3});
        check("equal coordinates compare equal", a.equals(b), "array identity semantics leaked");
        check("equal coordinates hash equally", a.hashCode() == b.hashCode(), "hash differs");
        check("toString renders the index unsigned and the id as hex",
              a.toString().contains("index=7") && a.toString().contains("010203"),
              a.toString());
        RecordCoordinate big = new RecordCoordinate("s", -1L, new byte[32]);
        check("a u64::MAX index renders unsigned",
              big.indexUnsigned().equals("18446744073709551615"),
              "got " + big.indexUnsigned());

        // ---- Additivity, made executable ----
        for (Object o : frames) {
            @SuppressWarnings("unchecked")
            Map<String, Object> v = (Map<String, Object>) o;
            int mt = Hex.decode((String) v.get("hex"))[5] & 0xFF;
            check(v.get("name") + ": type outside the v1-only table", mt > 0x05,
                  "a v1-only decoder would have accepted this as a known type");
        }

        int total = passed + failed;
        System.out.printf("%nTracked conformance: %d/%d checks passed%s%n",
            passed, total, failed == 0 ? " — all good" : ", " + failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    /**
     * Renders a parsed JSON integer as the UNSIGNED value it represents.
     *
     * <p>This exists because the first version of this runner got it wrong, in
     * the exact way the client itself could have. MiniJson parses u64::MAX with
     * {@code Long.parseUnsignedLong}, which keeps the bits — and those bits are
     * {@code -1L} in a signed long, so {@code String.valueOf} prints "-1". Java
     * has no unsigned primitive; every comparison and every rendering of a u64
     * has to go through {@code Long.toUnsignedString} or
     * {@code Long.compareUnsigned}, on both sides.
     */
    private static String unsignedText(Object n) {
        if (n instanceof Long l) {
            return Long.toUnsignedString(l);
        }
        // BigInteger, for a value genuinely outside u64 — no weir field is that
        // wide, so this branch means the document is not ours.
        return String.valueOf(n);
    }

    private static String typeName(Wire.MessageType mt) {
        switch (mt) {
            case PUSH: return "Push";
            case ACK: return "Ack";
            case NACK: return "Nack";
            case HEALTH_CHECK: return "HealthCheck";
            case HEALTH_CHECK_RESPONSE: return "HealthCheckResponse";
            case PUSH_TRACKED: return "PushTracked";
            case ACK_TRACKED: return "AckTracked";
            default: return mt.name();
        }
    }
}
