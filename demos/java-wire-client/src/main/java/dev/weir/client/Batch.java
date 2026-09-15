package dev.weir.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * The batch extension: {@code PushBatch} (0x08) and {@code AckBatch} (0x09) —
 * N records in one round trip, answered once.
 *
 * <p>Implemented from the spec and checked against
 * {@code docs/conformance/wire_v1_batch_vectors.json}. Nothing here is ported
 * from the Rust reference: an independent implementation is the only thing that
 * catches an under-specified format, and this one has a bug class no checksum
 * detects — a bitmap written with the opposite bit order is a well-formed frame,
 * valid CRCs and correct length, that reports failures as successes.
 *
 * <p><b>Bit i lives in byte i / 8 at mask {@code 1 << (i % 8)}: LSB-first.</b>
 *
 * <p>A set bit means the record is durable at the requested tier and inherits
 * weir's crown invariant. A CLEAR bit is the weak statement: not durable as of
 * this reply, retry it, and expect that it may nonetheless have been written.
 */
public final class Batch {

    private Batch() {}

    /** Rejection tags, matching the conformance vector names. */
    public enum ErrorTag {
        TRUNCATED("Truncated"),
        UNSUPPORTED_VERSION("UnsupportedVersion"),
        EMPTY_BATCH("EmptyBatch"),
        TOO_MANY_RECORDS("TooManyRecords"),
        EMPTY_RECORD("EmptyRecord"),
        RECORD_TOO_LARGE("RecordTooLarge"),
        TRUNCATED_RECORD("TruncatedRecord"),
        LENGTH_MISMATCH("LengthMismatch"),
        PADDING_NOT_ZERO("PaddingNotZero");

        public final String specName;

        ErrorTag(String specName) {
            this.specName = specName;
        }
    }

    /** Thrown by the decoders; {@code tag} matches the vector names. */
    public static final class BatchException extends Exception {
        private static final long serialVersionUID = 1L;
        public final ErrorTag tag;

        BatchException(ErrorTag tag, String detail) {
            super(detail == null ? tag.specName : tag.specName + ": " + detail);
            this.tag = tag;
        }
    }

    /** A PushBatch body: version, u16 count, then u32-length-prefixed records. */
    public static byte[] encodeBody(List<byte[]> records) {
        int n = Wire.BATCH_HEADER_LEN;
        for (byte[] r : records) {
            n += 4 + r.length;
        }
        ByteBuffer bb = ByteBuffer.allocate(n).order(ByteOrder.LITTLE_ENDIAN);
        bb.put(Wire.BATCH_VERSION);
        bb.putShort((short) records.size());
        for (byte[] r : records) {
            bb.putInt(r.length);
            bb.put(r);
        }
        return bb.array();
    }

    /**
     * Parse a PushBatch body.
     *
     * <p>The check ORDER is part of the contract. The declared count is
     * validated against the cap BEFORE anything is sized by it, and a record's
     * declared length is checked against the cap BEFORE it is added to the
     * cursor. The frame's payload CRC has already passed by this point and
     * proves nothing here: a hostile peer computes a perfectly valid CRC over a
     * body declaring 65,535 records in three bytes.
     */
    public static List<byte[]> decodeBody(byte[] body, int maxRecords, int maxRecordLen)
            throws BatchException {
        if (body.length < Wire.BATCH_HEADER_LEN) {
            throw new BatchException(ErrorTag.TRUNCATED,
                    body.length + " < " + Wire.BATCH_HEADER_LEN);
        }
        if (body[0] != Wire.BATCH_VERSION) {
            throw new BatchException(ErrorTag.UNSUPPORTED_VERSION, "version " + (body[0] & 0xFF));
        }
        ByteBuffer bb = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
        // Short.toUnsignedInt, not getShort(): a count above 32,767 arrives
        // negative in Java's signed short and would slip past every cap below.
        int declared = Short.toUnsignedInt(bb.getShort(1));
        if (declared == 0) {
            throw new BatchException(ErrorTag.EMPTY_BATCH, null);
        }
        int cap = Math.min(maxRecords, Wire.MAX_BATCH_RECORDS_HARD_CAP);
        if (declared > cap) {
            throw new BatchException(ErrorTag.TOO_MANY_RECORDS, declared + " > " + cap);
        }

        int recordCap = Math.min(maxRecordLen, Wire.MAX_PAYLOAD_HARD_CAP);
        List<byte[]> records = new ArrayList<>(declared);
        int cursor = Wire.BATCH_HEADER_LEN;
        while (cursor < body.length) {
            if (cursor + 4 > body.length) {
                throw new BatchException(ErrorTag.TRUNCATED_RECORD, "record " + records.size());
            }
            // Integer.toUnsignedLong: a u32 length at or above 2^31 is negative
            // as an int, and every bounds check that follows would pass.
            long n = Integer.toUnsignedLong(bb.getInt(cursor));
            cursor += 4;
            if (n == 0) {
                throw new BatchException(ErrorTag.EMPTY_RECORD, "record " + records.size());
            }
            if (n > recordCap) {
                throw new BatchException(ErrorTag.RECORD_TOO_LARGE,
                        "record " + records.size() + ": " + n + " > " + recordCap);
            }
            if (cursor + n > body.length) {
                throw new BatchException(ErrorTag.TRUNCATED_RECORD, "record " + records.size());
            }
            // Stop before overrunning the declared count, so a body carrying
            // more records than it declares is a mismatch, not a silent drop.
            if (records.size() == declared) {
                throw new BatchException(ErrorTag.LENGTH_MISMATCH,
                        "more than the declared " + declared);
            }
            byte[] r = new byte[(int) n];
            System.arraycopy(body, cursor, r, 0, (int) n);
            records.add(r);
            cursor += (int) n;
        }
        if (records.size() != declared || cursor != body.length) {
            throw new BatchException(ErrorTag.LENGTH_MISMATCH,
                    "declared " + declared + ", found " + records.size());
        }
        return records;
    }

    /**
     * An AckBatch payload from per-record outcomes.
     * Padding bits in the final byte are left zero, which the decoder requires.
     */
    public static byte[] encodeAck(boolean[] accepted) {
        byte[] out = new byte[Wire.ACK_BATCH_HEADER_LEN + (accepted.length + 7) / 8];
        out[0] = Wire.ACK_BATCH_VERSION;
        out[1] = (byte) (accepted.length & 0xFF);
        out[2] = (byte) ((accepted.length >>> 8) & 0xFF);
        for (int i = 0; i < accepted.length; i++) {
            if (accepted[i]) {
                out[Wire.ACK_BATCH_HEADER_LEN + i / 8] |= (byte) (1 << (i % 8));
            }
        }
        return out;
    }

    /**
     * Parse an AckBatch payload into per-record outcomes.
     *
     * <p>{@code expected} is the count this client sent. A bitmap is the first
     * weir response whose meaning depends on client-held state — an Ack says
     * "your last record" and an AckTracked carries its own coordinate, but a
     * bitmap is meaningless without knowing which batch it answers.
     * {@code ceil(N/8)} is not injective (N of 1017 through 1024 all give 131
     * bytes), so the echoed count is the only thing that can catch a desync.
     */
    public static boolean[] decodeAck(byte[] payload, int expected) throws BatchException {
        if (payload.length < Wire.ACK_BATCH_HEADER_LEN) {
            throw new BatchException(ErrorTag.TRUNCATED,
                    payload.length + " < " + Wire.ACK_BATCH_HEADER_LEN);
        }
        if (payload[0] != Wire.ACK_BATCH_VERSION) {
            throw new BatchException(ErrorTag.UNSUPPORTED_VERSION, "version " + (payload[0] & 0xFF));
        }
        int declared = Short.toUnsignedInt(
                ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN).getShort(1));
        if (declared == 0) {
            throw new BatchException(ErrorTag.EMPTY_BATCH, null);
        }
        if (declared != expected) {
            throw new BatchException(ErrorTag.LENGTH_MISMATCH,
                    "reply answers " + declared + ", we sent " + expected);
        }
        int want = Wire.ACK_BATCH_HEADER_LEN + (declared + 7) / 8;
        if (payload.length != want) {
            throw new BatchException(ErrorTag.LENGTH_MISMATCH,
                    payload.length + " bytes, need " + want);
        }
        // Padding bits must be zero. Ignoring them would make popcount == N —
        // the obvious way to ask "did the whole batch succeed" — silently wrong.
        int used = declared % 8;
        if (used != 0) {
            int last = payload[payload.length - 1] & 0xFF;
            if ((last & ~((1 << used) - 1) & 0xFF) != 0) {
                throw new BatchException(ErrorTag.PADDING_NOT_ZERO,
                        "final byte 0x" + Integer.toHexString(last));
            }
        }
        boolean[] out = new boolean[declared];
        for (int i = 0; i < declared; i++) {
            out[i] = (payload[Wire.ACK_BATCH_HEADER_LEN + i / 8] & (1 << (i % 8))) != 0;
        }
        return out;
    }
}
