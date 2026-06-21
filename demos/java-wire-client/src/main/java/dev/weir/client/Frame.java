package dev.weir.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * Encode/decode for a single weir v1 wire frame.
 *
 * <p>This is the executable definition of the framing contract for this client:
 * one buffer, exactly one frame. The CRC32 used is {@link java.util.zip.CRC32}
 * (IEEE / ISO-3309), which the spec confirms is the correct variant (NOT
 * CRC-32C).
 *
 * <p>No third-party dependencies: only {@code java.nio} and {@code java.util.zip}.
 */
public final class Frame {

    public final Wire.MessageType messageType;
    public final Wire.Durability durability;
    public final int flags;
    public final byte[] payload;

    public Frame(Wire.MessageType messageType, Wire.Durability durability, int flags, byte[] payload) {
        this.messageType = messageType;
        this.durability = durability;
        this.flags = flags;
        this.payload = payload;
    }

    /** Convenience constructor for frames with zero flags. */
    public Frame(Wire.MessageType messageType, Wire.Durability durability, byte[] payload) {
        this(messageType, durability, 0, payload);
    }

    /** Computes CRC-32 (IEEE / ISO-3309) over a byte range, returned as an unsigned int. */
    public static long crc32(byte[] data, int off, int len) {
        CRC32 crc = new CRC32();
        crc.update(data, off, len);
        return crc.getValue(); // already masked to 32 bits
    }

    /**
     * Encodes this frame to its on-the-wire byte form:
     * {@code HEADER_LEN + payload.length + CRC_LEN} bytes.
     *
     * @throws ProtocolException if the payload exceeds the hard cap, or flags is nonzero
     */
    public byte[] encode() {
        if (payload.length > Wire.MAX_PAYLOAD_HARD_CAP) {
            throw new ProtocolException(
                "payload_len " + payload.length + " exceeds MAX_PAYLOAD_HARD_CAP "
                + Wire.MAX_PAYLOAD_HARD_CAP);
        }
        if ((flags & 0xFF) != 0) {
            // Defensive: the spec says reserved flags must be zero on write; the
            // daemon would Nack(ReservedFlagsSet) and close. Fail fast locally.
            throw new ProtocolException("reserved flags byte must be zero on write, got 0x"
                + Integer.toHexString(flags & 0xFF));
        }

        int total = Wire.HEADER_LEN + payload.length + Wire.CRC_LEN;
        ByteBuffer buf = ByteBuffer.allocate(total).order(ByteOrder.LITTLE_ENDIAN);

        buf.put(Wire.MAGIC);                     // [0..4)
        buf.put((byte) Wire.WIRE_VERSION);       // [4]
        buf.put(messageType.code);               // [5]
        buf.put(durability.code);                // [6]
        buf.put((byte) (flags & 0xFF));          // [7]
        buf.putInt(payload.length);              // [8..12) LE

        // header CRC over bytes [0..12)
        long headerCrc = crc32(buf.array(), 0, 12);
        buf.putInt((int) headerCrc);             // [12..16) LE

        buf.put(payload);                        // [16..16+n)

        long payloadCrc = crc32(payload, 0, payload.length);
        buf.putInt((int) payloadCrc);            // [16+n..+4) LE

        return buf.array();
    }

    /**
     * Decodes exactly one frame from {@code data}. The buffer must be exactly
     * one frame: {@code HEADER_LEN + payload_len + CRC_LEN} bytes.
     *
     * <p>This mirrors the reference codec's one-buffer-one-frame contract and
     * the server-side decode order (magic → version → header CRC → fields →
     * payload cap → payload → payload CRC).
     *
     * @throws ProtocolException with a {@link DecodeError} tag on any violation
     */
    public static Frame decode(byte[] data) {
        // 1. Magic. A buffer shorter than the full header that *starts* with
        //    valid magic is TruncatedFrame, not BadMagic — so check length first.
        if (data.length < Wire.HEADER_LEN) {
            // Distinguish "not even a header" from "bad magic": if the leading
            // bytes that ARE present diverge from MAGIC, it's BadMagic; otherwise
            // it's a truncated header. The spec: "a complete header is required
            // before any field is interpreted" => short-but-magic-prefix is truncation.
            int cmp = Math.min(data.length, Wire.MAGIC.length);
            for (int i = 0; i < cmp; i++) {
                if (data[i] != Wire.MAGIC[i]) {
                    throw new ProtocolException(DecodeError.BAD_MAGIC, "bad magic prefix");
                }
            }
            throw new ProtocolException(DecodeError.TRUNCATED_FRAME,
                "buffer shorter than 16-byte header (" + data.length + " bytes)");
        }
        for (int i = 0; i < Wire.MAGIC.length; i++) {
            if (data[i] != Wire.MAGIC[i]) {
                throw new ProtocolException(DecodeError.BAD_MAGIC,
                    "first four bytes are not \"WEIR\"");
            }
        }

        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // 2. Version (checked before header CRC, per spec).
        int version = data[4] & 0xFF;
        if (version != Wire.WIRE_VERSION) {
            throw new ProtocolException(DecodeError.VERSION_MISMATCH,
                "frame version " + version + " != WIRE_VERSION " + Wire.WIRE_VERSION);
        }

        // 3. Header CRC over [0..12).
        long computedHeaderCrc = crc32(data, 0, 12);
        long storedHeaderCrc = Integer.toUnsignedLong(buf.getInt(12));
        if (computedHeaderCrc != storedHeaderCrc) {
            throw new ProtocolException(DecodeError.HEADER_CRC_MISMATCH,
                String.format("header CRC mismatch: computed 0x%08x stored 0x%08x",
                    computedHeaderCrc, storedHeaderCrc));
        }

        // 4. Header field parsing (only after header CRC passes).
        int flags = data[7] & 0xFF;
        if (flags != 0) {
            throw new ProtocolException(DecodeError.RESERVED_FLAGS_SET,
                "reserved flags byte is 0x" + Integer.toHexString(flags));
        }
        Wire.MessageType mt = Wire.MessageType.fromByte(data[5]);
        if (mt == null) {
            throw new ProtocolException(DecodeError.UNKNOWN_MESSAGE_TYPE,
                "unknown message_type 0x" + Integer.toHexString(data[5] & 0xFF));
        }
        Wire.Durability dur = Wire.Durability.fromByte(data[6]);
        if (dur == null) {
            throw new ProtocolException(DecodeError.UNKNOWN_DURABILITY,
                "unknown durability 0x" + Integer.toHexString(data[6] & 0xFF));
        }

        // 5. Payload length cap (checked before any allocation, before frame-length check).
        long payloadLen = Integer.toUnsignedLong(buf.getInt(8));
        if (payloadLen > Wire.MAX_PAYLOAD_HARD_CAP) {
            throw new ProtocolException(DecodeError.PAYLOAD_TOO_LARGE,
                "payload_len " + payloadLen + " exceeds hard cap " + Wire.MAX_PAYLOAD_HARD_CAP);
        }

        // 6. One-frame contract: buffer must be exactly HEADER + payload + CRC.
        long expectedTotal = (long) Wire.HEADER_LEN + payloadLen + Wire.CRC_LEN;
        if (data.length < expectedTotal) {
            throw new ProtocolException(DecodeError.TRUNCATED_FRAME,
                "buffer " + data.length + " bytes < expected frame " + expectedTotal);
        }
        if (data.length > expectedTotal) {
            throw new ProtocolException(DecodeError.TRAILING_BYTES,
                (data.length - expectedTotal) + " trailing bytes after one frame");
        }

        // 7. Payload + payload CRC.
        int n = (int) payloadLen;
        byte[] payload = Arrays.copyOfRange(data, Wire.HEADER_LEN, Wire.HEADER_LEN + n);
        long computedPayloadCrc = crc32(payload, 0, n);
        long storedPayloadCrc = Integer.toUnsignedLong(buf.getInt(Wire.HEADER_LEN + n));
        if (computedPayloadCrc != storedPayloadCrc) {
            throw new ProtocolException(DecodeError.PAYLOAD_CRC_MISMATCH,
                String.format("payload CRC mismatch: computed 0x%08x stored 0x%08x",
                    computedPayloadCrc, storedPayloadCrc));
        }

        return new Frame(mt, dur, flags, payload);
    }

    @Override
    public String toString() {
        return "Frame{" + messageType + ", " + durability + ", flags=" + flags
            + ", payload=" + payload.length + "B}";
    }

    /** Decoder verdict tags, mirroring the conformance vectors' {@code decode} values. */
    public enum DecodeError {
        BAD_MAGIC("BadMagic"),
        VERSION_MISMATCH("VersionMismatch"),
        UNKNOWN_MESSAGE_TYPE("UnknownMessageType"),
        UNKNOWN_DURABILITY("UnknownDurability"),
        HEADER_CRC_MISMATCH("HeaderCrcMismatch"),
        RESERVED_FLAGS_SET("ReservedFlagsSet"),
        PAYLOAD_TOO_LARGE("PayloadTooLarge"),
        TRUNCATED_FRAME("TruncatedFrame"),
        PAYLOAD_CRC_MISMATCH("PayloadCrcMismatch"),
        TRAILING_BYTES("TrailingBytes");

        /** The exact string used in {@code wire_v1_vectors.json}'s {@code decode} field. */
        public final String vectorName;

        DecodeError(String vectorName) {
            this.vectorName = vectorName;
        }
    }
}
