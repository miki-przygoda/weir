package dev.weir.client;

/**
 * A wire-protocol-level error: a malformed frame, a CRC mismatch, a Nack
 * outcome, or any other deviation from the v1 contract.
 *
 * <p>Carries an optional {@link Frame.DecodeError} tag (for decode failures)
 * and an optional {@link Wire.NackReason} (when the failure was a daemon Nack).
 */
public final class ProtocolException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Set when this exception is a decode-side verdict; otherwise {@code null}. */
    public final Frame.DecodeError decodeError;

    /** Set when this exception represents a daemon Nack; otherwise {@code null}. */
    public final Wire.NackReason nackReason;

    /** Raw Nack reason byte, present even when {@link #nackReason} is null (reserved range). */
    public final Integer rawNackByte;

    public ProtocolException(String message) {
        super(message);
        this.decodeError = null;
        this.nackReason = null;
        this.rawNackByte = null;
    }

    public ProtocolException(Frame.DecodeError decodeError, String message) {
        super(decodeError.vectorName + ": " + message);
        this.decodeError = decodeError;
        this.nackReason = null;
        this.rawNackByte = null;
    }

    private ProtocolException(Wire.NackReason reason, Integer rawByte, String message) {
        super(message);
        this.decodeError = null;
        this.nackReason = reason;
        this.rawNackByte = rawByte;
    }

    /** Builds a Nack-outcome exception. {@code reason} may be null for a reserved byte. */
    public static ProtocolException nack(Wire.NackReason reason, int rawByte) {
        String label = reason != null ? reason.name()
            : ("reserved reason byte 0x" + Integer.toHexString(rawByte & 0xFF));
        return new ProtocolException(reason, rawByte, "daemon Nack: " + label);
    }

    /**
     * Whether a producer may retry the same record (possibly on a fresh
     * connection). Mirrors the spec's open/closed connection table.
     *
     * <p>{@code InternalError} is transient (connection stays open, durable
     * outcome unknown → retry). All other Nack reasons are permanent protocol
     * errors (the daemon closes the connection; retrying the identical frame
     * will not succeed). A reserved reason byte is treated as non-retryable
     * (surface it, don't assume).
     */
    public boolean isRetryable() {
        return nackReason == Wire.NackReason.INTERNAL_ERROR;
    }
}
