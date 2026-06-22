package dev.weir.client;

/**
 * weir wire protocol v1 constants and enums.
 *
 * <p>Single source of truth for the framing layout described in
 * {@code docs/wire_protocol.md}. These values are frozen by the spec; this
 * class deliberately re-declares them rather than depending on any weir crate,
 * so the client is a standalone, polyglot implementation of the wire.
 *
 * <p>Frame layout (16-byte header + payload + 4-byte payload CRC32):
 * <pre>
 *  off size field
 *   0   4   magic = "WEIR"
 *   4   1   version (=1)
 *   5   1   message_type
 *   6   1   durability
 *   7   1   flags (reserved; must be 0 on write)
 *   8   4   payload_len (u32 LE)
 *  12   4   header_crc32 (LE, over bytes [0..12])
 *  16   n   payload
 *  16+n 4   payload_crc32 (LE, over the n payload bytes)
 * </pre>
 */
public final class Wire {
    private Wire() {}

    /** Magic prefix identifying a weir frame. */
    public static final byte[] MAGIC = {'W', 'E', 'I', 'R'};

    /** Wire protocol version this client speaks. Negotiation is strict-equality. */
    public static final int WIRE_VERSION = 1;

    /** Fixed header length in bytes. */
    public static final int HEADER_LEN = 16;

    /** Length of each CRC32 field in bytes. */
    public static final int CRC_LEN = 4;

    /** Absolute payload ceiling across all code paths (16 MiB). */
    public static final int MAX_PAYLOAD_HARD_CAP = 16 * 1024 * 1024;

    /**
     * Maximum payload length we will allocate for a *response*. Per the spec's
     * producer checklist, every weir response payload is at most 2 bytes
     * (Ack/HCR = 0, Nack = 1, VersionMismatch = 2). A larger declared length on
     * a response is a desync or a non-weir peer.
     */
    public static final int MAX_RESPONSE_PAYLOAD = 2;

    /** Message type bytes (see spec "Message types" table). */
    public enum MessageType {
        PUSH((byte) 0x01),
        ACK((byte) 0x02),
        NACK((byte) 0x03),
        HEALTH_CHECK((byte) 0x04),
        HEALTH_CHECK_RESPONSE((byte) 0x05);

        public final byte code;

        MessageType(byte code) {
            this.code = code;
        }

        /** Returns the variant for a byte, or {@code null} if unrecognised. */
        public static MessageType fromByte(int b) {
            for (MessageType m : values()) {
                if ((m.code & 0xFF) == (b & 0xFF)) {
                    return m;
                }
            }
            return null;
        }
    }

    /** Durability tier bytes (see spec "Durability tiers" table). */
    public enum Durability {
        SYNC((byte) 0x01),
        BATCHED((byte) 0x02),
        BUFFERED((byte) 0x03);

        public final byte code;

        Durability(byte code) {
            this.code = code;
        }

        public static Durability fromByte(int b) {
            for (Durability d : values()) {
                if ((d.code & 0xFF) == (b & 0xFF)) {
                    return d;
                }
            }
            return null;
        }
    }

    /**
     * Nack reason bytes (see spec "Nack payload format" table). Reasons
     * 0x0A–0xFF are reserved; an unrecognised reason is surfaced as its raw
     * byte rather than mapped here.
     */
    public enum NackReason {
        BAD_MAGIC((byte) 0x01),
        VERSION_MISMATCH((byte) 0x02),
        BAD_HEADER_CRC((byte) 0x03),
        PAYLOAD_TOO_LARGE((byte) 0x04),
        BAD_PAYLOAD_CRC((byte) 0x05),
        INTERNAL_ERROR((byte) 0x06),
        EMPTY_PAYLOAD((byte) 0x07),
        UNKNOWN_MESSAGE((byte) 0x08),
        RESERVED_FLAGS_SET((byte) 0x09);

        public final byte code;

        NackReason(byte code) {
            this.code = code;
        }

        public static NackReason fromByte(int b) {
            for (NackReason r : values()) {
                if ((r.code & 0xFF) == (b & 0xFF)) {
                    return r;
                }
            }
            return null;
        }
    }
}
