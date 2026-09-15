package dev.weir.client;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runs this client's batch codec against wire_v1_batch_vectors.json.
 *
 * <p>The frozen wire_v1_vectors.json is deliberately untouched by this
 * extension: PushBatch/AckBatch are additive message-type bytes within wire v1,
 * so a client that ignores them stays fully conformant. This runner is for one
 * that does not.
 *
 * <p>Why an independent implementation matters more here than anywhere else in
 * the protocol: a bitmap written with the opposite bit order is a WELL-FORMED
 * frame — header CRC valid, payload CRC valid, payload_len correct — that
 * reports failures as successes. No checksum, length check or cap detects it;
 * only an asymmetric vector at N &gt; 8 does.
 *
 * <p>Run: {@code java -cp out dev.weir.client.BatchConformanceRunner <vectors.json>}
 */
public final class BatchConformanceRunner {

    private static int passed;
    private static int failed;

    private BatchConformanceRunner() {
    }

    private static void check(String name, boolean cond, String detail) {
        if (cond) {
            passed++;
        } else {
            failed++;
            System.out.printf("  FAIL  %-46s %s%n", name, detail);
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: BatchConformanceRunner <wire_v1_batch_vectors.json>");
            System.exit(2);
        }
        String text = Files.readString(Path.of(args[0]));
        Map<String, Object> doc = (Map<String, Object>) MiniJson.parse(text);

        // If the vectors were generated against different constants than this
        // client holds, every byte comparison below is meaningless.
        check("hard cap matches the vectors",
              asInt(doc.get("max_batch_records_hard_cap")) == Wire.MAX_BATCH_RECORDS_HARD_CAP,
              doc.get("max_batch_records_hard_cap") + " != " + Wire.MAX_BATCH_RECORDS_HARD_CAP);
        check("ack payload bound matches the vectors",
              asInt(doc.get("max_ack_batch_payload_len")) == Wire.MAX_ACK_BATCH_PAYLOAD,
              doc.get("max_ack_batch_payload_len") + " != " + Wire.MAX_ACK_BATCH_PAYLOAD);
        // The reason the hard cap is 2048: the reply stays inside the bound
        // this client already had, so batching adds no new allocation maximum.
        check("AckBatch introduces no new largest response",
              Wire.MAX_ACK_BATCH_PAYLOAD <= Wire.MAX_TRACKED_ACK_PAYLOAD,
              Wire.MAX_ACK_BATCH_PAYLOAD + " > " + Wire.MAX_TRACKED_ACK_PAYLOAD);

        // ---- Frames ----
        List<Object> frames = (List<Object>) doc.get("frame_vectors");
        for (Object o : frames) {
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
                if (f.messageType == Wire.MessageType.PUSH_BATCH) {
                    check(name + ": re-encode round-trips",
                          Hex.encode(f.encode()).equals(v.get("hex")), "bytes differ");
                }
                // Additivity, made executable: a decoder knowing only 0x01-0x05
                // must reject this as an unknown type rather than misparse it.
                check(name + ": type byte is outside the v1-only table",
                      (raw[5] & 0xFF) > 0x05, "byte " + (raw[5] & 0xFF));
                if ("AckBatch".equals(v.get("message_type"))) {
                    int n = f.payload.length;
                    check(name + ": exceeds the default response cap",
                          n > Wire.MAX_RESPONSE_PAYLOAD,
                          n + " bytes does not exercise the widening");
                    check(name + ": fits this client's AckBatch cap",
                          n <= Wire.maxResponsePayload(0x09),
                          n + " > " + Wire.maxResponsePayload(0x09));
                }
            } catch (ProtocolException e) {
                check(name, false, "unexpected rejection: " + e.getMessage());
            }
        }

        // ---- PushBatch bodies ----
        int okSeen = 0;
        int rejectSeen = 0;
        List<Object> bodies = (List<Object>) doc.get("body_vectors");
        for (Object o : bodies) {
            Map<String, Object> v = (Map<String, Object>) o;
            String name = (String) v.get("name");
            String want = (String) v.get("decode");
            byte[] raw = Hex.decode((String) v.get("hex"));
            String got;
            List<byte[]> records = null;
            try {
                records = Batch.decodeBody(raw, Wire.MAX_BATCH_RECORDS_HARD_CAP,
                                           Wire.MAX_PAYLOAD_HARD_CAP);
                got = "ok";
            } catch (Batch.BatchException e) {
                got = e.tag.specName;
            }
            check(name + ": verdict", got.equals(want), "got " + got + ", want " + want);
            if (!"ok".equals(want)) {
                rejectSeen++;
                continue;
            }
            if (records == null) {
                continue;
            }
            List<Object> wantHex = (List<Object>) v.get("records_hex");
            List<String> gotHex = new ArrayList<>();
            for (byte[] r : records) {
                gotHex.add(Hex.encode(r));
            }
            check(name + ": records", gotHex.equals(wantHex),
                  gotHex + " != " + wantHex);
            check(name + ": re-encode is byte-identical",
                  Hex.encode(Batch.encodeBody(records)).equals(v.get("hex")), "bytes differ");
            okSeen++;
        }
        check("body vectors cover both outcomes", okSeen > 0 && rejectSeen > 0,
              okSeen + " ok, " + rejectSeen + " rejected");

        // ---- AckBatch payloads ----
        okSeen = 0;
        rejectSeen = 0;
        List<Object> acks = (List<Object>) doc.get("ack_vectors");
        for (Object o : acks) {
            Map<String, Object> v = (Map<String, Object>) o;
            String name = (String) v.get("name");
            String want = (String) v.get("decode");
            int expected = asInt(v.get("expected"));
            byte[] raw = Hex.decode((String) v.get("hex"));
            String got;
            boolean[] accepted = null;
            try {
                accepted = Batch.decodeAck(raw, expected);
                got = "ok";
            } catch (Batch.BatchException e) {
                got = e.tag.specName;
            }
            check(name + ": verdict", got.equals(want), "got " + got + ", want " + want);
            if (!"ok".equals(want)) {
                rejectSeen++;
                continue;
            }
            if (accepted == null) {
                continue;
            }
            boolean[] wantBits = new boolean[expected];
            List<Object> list = (List<Object>) v.get("accepted");
            if (list != null) {
                for (int i = 0; i < expected; i++) {
                    wantBits[i] = Boolean.TRUE.equals(list.get(i));
                }
            } else {
                boolean all = Boolean.TRUE.equals(v.get("accepted_all"));
                java.util.Arrays.fill(wantBits, all);
            }
            check(name + ": verdicts", java.util.Arrays.equals(accepted, wantBits),
                  "if this is the N=9 asymmetric vector, this client's bitmap "
                  + "bit order is inverted");
            check(name + ": re-encode is byte-identical",
                  Hex.encode(Batch.encodeAck(accepted)).equals(v.get("hex")), "bytes differ");
            okSeen++;
        }
        check("ack vectors cover both outcomes", okSeen > 0 && rejectSeen > 0,
              okSeen + " ok, " + rejectSeen + " rejected");

        // ---- The bitmap convention, against a hand-written literal ----
        //
        // Every other assertion here compares this client to the vectors. This
        // one compares it to bytes spelled out in the source, so the convention
        // is visible without running anything.
        byte[] literal = Batch.encodeAck(
                new boolean[] {true, false, true, false, false, false, false, false, true});
        check("the bitmap is LSB-first",
              Hex.encode(literal).equals("0109000501"),
              Hex.encode(literal) + " — MSB-first would be 010900a080");

        int total = passed + failed;
        System.out.printf("%nBatch conformance: %d/%d checks passed%s%n",
                passed, total, failed == 0 ? " — all good" : ", " + failed + " FAILED");
        System.exit(failed == 0 ? 0 : 1);
    }

    private static int asInt(Object o) {
        return o instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(o));
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
            case PUSH_BATCH: return "PushBatch";
            case ACK_BATCH: return "AckBatch";
            default: return mt.name();
        }
    }
}
