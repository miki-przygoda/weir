package dev.weir.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;

/**
 * A synchronous weir producer over an {@code AF_UNIX SOCK_STREAM} socket.
 *
 * <p>Uses {@link java.net.UnixDomainSocketAddress} + {@link SocketChannel}
 * (JDK 21+) — pure stdlib, no JNI, no third-party dependency. The wire framing
 * is implemented directly from {@code docs/wire_protocol.md}; there is no
 * dependency on any weir crate.
 *
 * <p>This client does its own stream framing (read the 16-byte header, take
 * {@code payload_len}, read exactly {@code payload_len + 4} more bytes), as the
 * spec mandates — it never hands a multi-frame buffer to {@link Frame#decode}.
 *
 * <p>Not thread-safe: one connection is a serial request/response stream.
 */
public final class WeirClient implements AutoCloseable {

    private final SocketChannel channel;
    private final InputStream in;
    private final OutputStream out;

    private WeirClient(SocketChannel channel) {
        this.channel = channel;
        this.in = Channels.newInputStream(channel);
        this.out = Channels.newOutputStream(channel);
    }

    /** Connects to the daemon listening at {@code socketPath}. */
    public static WeirClient connect(Path socketPath) throws IOException {
        UnixDomainSocketAddress addr = UnixDomainSocketAddress.of(socketPath);
        SocketChannel ch = SocketChannel.open(StandardProtocolFamily.UNIX);
        ch.connect(addr);
        return new WeirClient(ch);
    }

    /**
     * Pushes one record and waits for the Ack/Nack.
     *
     * @return the decoded Ack frame on success
     * @throws ProtocolException if the daemon Nacks (the exception carries the reason)
     * @throws IOException       on connection close / I/O failure
     */
    public Frame push(byte[] payload, Wire.Durability durability) throws IOException {
        if (payload.length == 0) {
            // The daemon would Nack(EmptyPayload) and close. Fail fast and point
            // the caller at HealthCheck, exactly as the spec's checklist says.
            throw new ProtocolException(
                "a zero-length Push is rejected with Nack(EmptyPayload); "
                + "use healthCheck() to probe liveness without a payload");
        }
        Frame request = new Frame(Wire.MessageType.PUSH, durability, payload);
        writeFrame(request);
        Frame response = readResponse();
        return interpret(response);
    }

    /**
     * Pushes a record and returns where it landed.
     *
     * <p>Identical to {@link #push} in every other respect — same tiers, same
     * caps, same Nack reasons — but answered with an {@code AckTracked} carrying
     * the coordinate. A daemon predating the type answers
     * {@code Nack(UnknownMessage)} and closes;
     * {@link ProtocolException#meansNoTrackedSupport()} names that case. Open a
     * new connection and use {@link #push}.
     */
    public RecordCoordinate pushTracked(byte[] payload, Wire.Durability durability)
            throws IOException {
        if (payload.length == 0) {
            throw new ProtocolException(
                "a zero-length PushTracked is rejected with Nack(EmptyPayload); "
                + "use healthCheck() to probe liveness without a payload");
        }
        writeFrame(new Frame(Wire.MessageType.PUSH_TRACKED, durability, payload));
        Frame response = readResponse();
        return interpretTracked(response);
    }

    private RecordCoordinate interpretTracked(Frame response) {
        switch (response.messageType) {
            case ACK_TRACKED:
                try {
                    return RecordCoordinate.decode(response.payload);
                } catch (RecordCoordinate.CoordinateException e) {
                    throw new ProtocolException(
                        "AckTracked payload is not a coordinate: " + e.getMessage());
                }
            case NACK:
                if (response.payload.length < 1) {
                    throw new ProtocolException("Nack frame carried an empty payload");
                }
                int raw = response.payload[0] & 0xFF;
                throw ProtocolException.nack(Wire.NackReason.fromByte(raw), raw);
            case ACK:
                // A bare Ack is an error, not a success. The request determines
                // the response shape, so an Ack here means the peer did not
                // understand what was asked -- returning success would report a
                // coordinate that does not exist.
                throw new ProtocolException(
                    "daemon answered a PushTracked with a bare Ack; the record may be "
                    + "durable but its coordinate is unknown");
            default:
                throw new ProtocolException(
                    "unexpected response message_type for a PushTracked: "
                    + response.messageType);
        }
    }

    /** Sends a HealthCheck and returns the HealthCheckResponse. */
    public Frame healthCheck() throws IOException {
        Frame request = new Frame(Wire.MessageType.HEALTH_CHECK, Wire.Durability.DURABLE, new byte[0]);
        writeFrame(request);
        Frame response = readResponse();
        if (response.messageType != Wire.MessageType.HEALTH_CHECK_RESPONSE) {
            throw new ProtocolException(
                "expected HealthCheckResponse, got " + response.messageType);
        }
        return response;
    }

    /**
     * Lets callers send a raw, possibly-malformed frame for negative testing
     * (e.g. to confirm the daemon Nacks a reserved-flags frame). Returns the
     * decoded response frame; does not interpret Ack/Nack.
     */
    public Frame sendRawAndRead(byte[] frameBytes) throws IOException {
        out.write(frameBytes);
        out.flush();
        return readResponse();
    }

    private void writeFrame(Frame f) throws IOException {
        out.write(f.encode());
        out.flush();
    }

    /** Maps a decoded response frame to success/failure. */
    private Frame interpret(Frame response) {
        switch (response.messageType) {
            case ACK:
                return response;
            case NACK:
                if (response.payload.length < 1) {
                    throw new ProtocolException("Nack frame carried an empty payload");
                }
                int raw = response.payload[0] & 0xFF;
                throw ProtocolException.nack(Wire.NackReason.fromByte(raw), raw);
            default:
                throw new ProtocolException(
                    "unexpected response message_type for a Push: " + response.messageType);
        }
    }

    /**
     * Reads exactly one response frame off the wire, doing its own framing.
     * Caps the declared response payload before allocating, per the spec
     * checklist, by the type the header declares: AckTracked carries a
     * coordinate of up to 298 bytes, every other response at most two.
     */
    private Frame readResponse() throws IOException {
        byte[] header = readExactly(Wire.HEADER_LEN);

        // Validate magic/version before trusting payload_len.
        for (int i = 0; i < Wire.MAGIC.length; i++) {
            if (header[i] != Wire.MAGIC[i]) {
                throw new ProtocolException(Frame.DecodeError.BAD_MAGIC,
                    "response did not start with WEIR magic");
            }
        }
        int version = header[4] & 0xFF;
        if (version != Wire.WIRE_VERSION) {
            throw new ProtocolException(Frame.DecodeError.VERSION_MISMATCH,
                "response wire version " + version);
        }

        ByteBuffer hb = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);

        // Verify the header CRC BEFORE choosing a cap from the type byte. The
        // cap is now per-type, so an unverified type byte could widen the bound
        // a flipped bit later fails anyway -- bounded at 296 bytes, but the
        // reference client decodes the full header first and this should match.
        long headerCrc = Frame.crc32(header, 0, 12);
        if (headerCrc != Integer.toUnsignedLong(hb.getInt(12))) {
            throw new ProtocolException(Frame.DecodeError.HEADER_CRC_MISMATCH,
                "response header CRC mismatch");
        }

        long payloadLen = Integer.toUnsignedLong(hb.getInt(8));
        int cap = Wire.maxResponsePayload(header[5] & 0xFF);
        if (payloadLen > cap) {
            throw new ProtocolException(
                "response declared payload_len " + payloadLen
                + " > " + cap + " for message_type 0x"
                + Integer.toHexString(header[5] & 0xFF)
                + " (desync or non-weir peer)");
        }

        byte[] rest = readExactly((int) payloadLen + Wire.CRC_LEN);

        // Reassemble the exact one-frame buffer and run it through the full decoder,
        // which validates the header CRC, fields, and payload CRC.
        byte[] full = new byte[Wire.HEADER_LEN + rest.length];
        System.arraycopy(header, 0, full, 0, Wire.HEADER_LEN);
        System.arraycopy(rest, 0, full, Wire.HEADER_LEN, rest.length);
        return Frame.decode(full);
    }

    /** Reads exactly {@code n} bytes or throws on premature close. */
    private byte[] readExactly(int n) throws IOException {
        byte[] buf = new byte[n];
        int got = 0;
        while (got < n) {
            int r = in.read(buf, got, n - got);
            if (r < 0) {
                throw new IOException(
                    "connection closed by daemon after " + got + " of " + n
                    + " expected bytes (in-flight outcome unknown; retry on a fresh connection)");
            }
            got += r;
        }
        return buf;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
