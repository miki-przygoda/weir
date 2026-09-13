package dev.weir.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;

/**
 * Where a tracked record landed in the buffer — the {@code AckTracked} payload.
 *
 * <p>An <em>address</em>, not a sequence. Other producers interleave in the same
 * segment, so one producer's indices have holes by construction; the spec is
 * explicit that this does not provide gap-free numbering. {@code segment} is an
 * opaque, stable address rather than a filesystem path, and {@code recordId} is
 * the same digest the daemon hands a sink as its per-record idempotency key,
 * which is what lets a producer correlate what it sent with what arrived.
 *
 * <p><b>{@code index} is a u64 in a signed long.</b> Java has no unsigned
 * primitive, so a segment index above {@code Long.MAX_VALUE} arrives negative.
 * Never compare or print it directly — use {@link #indexUnsigned()} and
 * {@link Long#compareUnsigned}. The conformance vectors exercise {@code u64::MAX}
 * precisely so this cannot be forgotten.
 */
public record RecordCoordinate(String segment, long index, byte[] recordId) {

    /** Rejection tags, matching the conformance vector names. */
    public enum ErrorTag {
        TRUNCATED("Truncated"),
        UNSUPPORTED_VERSION("UnsupportedVersion"),
        SEGMENT_TOO_LONG("SegmentTooLong"),
        LENGTH_MISMATCH("LengthMismatch"),
        SEGMENT_NOT_UTF8("SegmentNotUtf8");

        public final String specName;

        ErrorTag(String specName) {
            this.specName = specName;
        }
    }

    /** Thrown by {@link #decode}; {@code tag} matches the vector names. */
    public static final class CoordinateException extends Exception {
        private static final long serialVersionUID = 1L;
        public final ErrorTag tag;

        CoordinateException(ErrorTag tag, String detail) {
            super(detail == null ? tag.specName : tag.specName + ": " + detail);
            this.tag = tag;
        }
    }

    /**
     * Decodes exactly one coordinate.
     *
     * <p>The version byte leads so the layout can grow inside wire v1, which only
     * works if a reader meeting an unknown version <em>rejects</em> rather than
     * parsing a prefix it has never seen. The buffer must be exactly one
     * coordinate for the same reason: a trailing byte means the two ends
     * disagree about the layout, and quietly using the prefix is how that
     * disagreement becomes a wrong address.
     */
    public static RecordCoordinate decode(byte[] buf) throws CoordinateException {
        if (buf.length < Wire.COORDINATE_FIXED_LEN) {
            throw new CoordinateException(ErrorTag.TRUNCATED,
                    buf.length + " < " + Wire.COORDINATE_FIXED_LEN);
        }
        if (buf[0] != Wire.COORDINATE_VERSION) {
            throw new CoordinateException(ErrorTag.UNSUPPORTED_VERSION,
                    "version " + (buf[0] & 0xFF));
        }

        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        long idx = bb.getLong(1);
        byte[] rid = Arrays.copyOfRange(buf, 9, 41);
        int segLen = Short.toUnsignedInt(bb.getShort(41));

        if (segLen > Wire.MAX_SEGMENT_NAME_LEN) {
            throw new CoordinateException(ErrorTag.SEGMENT_TOO_LONG,
                    segLen + " > " + Wire.MAX_SEGMENT_NAME_LEN);
        }
        int want = Wire.COORDINATE_FIXED_LEN + segLen;
        if (buf.length < want) {
            throw new CoordinateException(ErrorTag.TRUNCATED, buf.length + " < " + want);
        }
        if (buf.length != want) {
            throw new CoordinateException(ErrorTag.LENGTH_MISMATCH,
                    (buf.length - want) + " trailing byte(s)");
        }

        // new String(bytes, UTF_8) SUBSTITUTES U+FFFD for invalid sequences
        // rather than throwing, so it cannot detect a non-UTF-8 segment. A
        // decoder with CodingErrorAction.REPORT is what actually rejects.
        CharsetDecoder dec = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        String seg;
        try {
            CharBuffer cb = dec.decode(ByteBuffer.wrap(buf, Wire.COORDINATE_FIXED_LEN, segLen));
            seg = cb.toString();
        } catch (CharacterCodingException e) {
            throw new CoordinateException(ErrorTag.SEGMENT_NOT_UTF8, e.getMessage());
        }

        return new RecordCoordinate(seg, idx, rid);
    }

    /** The index rendered as the unsigned 64-bit value it actually is. */
    public String indexUnsigned() {
        return Long.toUnsignedString(index);
    }

    /** {@code recordId} as lowercase hex — the form the HTTP sink's Idempotency-Key uses. */
    public String recordIdHex() {
        return HexFormat.of().formatHex(recordId);
    }

    // A record containing a byte[] gets array IDENTITY semantics from the
    // compiler-generated members: two coordinates with equal bytes compare
    // unequal, hash differently, and print as "[B@1b6d3586". All three are
    // wrong for a value type, so all three are overridden.

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordCoordinate other)) {
            return false;
        }
        return index == other.index
                && segment.equals(other.segment)
                && Arrays.equals(recordId, other.recordId);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * segment.hashCode() + Long.hashCode(index)) + Arrays.hashCode(recordId);
    }

    @Override
    public String toString() {
        return "RecordCoordinate[segment=" + segment
                + ", index=" + indexUnsigned()
                + ", recordId=" + recordIdHex() + "]";
    }
}
