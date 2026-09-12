package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// Client is a synchronous weir producer over a Unix socket.
type Client struct {
	conn net.Conn
}

// Dial connects to the daemon's Unix socket. There is no in-band handshake
// (wire_protocol.md "Socket setup").
func Dial(socketPath string) (*Client, error) {
	c, err := net.DialTimeout("unix", socketPath, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", socketPath, err)
	}
	return &Client{conn: c}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// ErrTrackedUnsupported reports a daemon that predates PushTracked (0x06).
//
// The wire cannot distinguish that from any other unknown message type -- both
// are Nack(UnknownMessage) -- so only the caller knows which question it asked.
// Wrapped so callers use errors.Is. Do not retry on this connection: the daemon
// closes it after the Nack. Open a new one and use Push.
var ErrTrackedUnsupported = errors.New("daemon does not support PushTracked (0x06)")

// Response is a decoded daemon reply.
type Response struct {
	Frame      Frame
	IsAck      bool
	IsNack     bool
	NackReason NackReason
	// DaemonWireVersion is set only for a VersionMismatch Nack (2-byte payload).
	DaemonWireVersion uint8
	HasDaemonVersion  bool
	// Coordinate is set only on an AckTracked, i.e. only in reply to a
	// PushTracked. IsAckTracked says whether it is meaningful.
	IsAckTracked bool
	Coordinate   RecordCoordinate
}

// readFrame reads exactly one framed response from the wire: a 16-byte header,
// then payload_len + 4 bytes (wire_protocol.md "Framing is the reader's
// responsibility"). It verifies the response header before consuming payload.
func (c *Client) readFrame() (Frame, error) {
	hdr := make([]byte, HeaderLen)
	if _, err := io.ReadFull(c.conn, hdr); err != nil {
		return Frame{}, fmt.Errorf("read header: %w", err)
	}
	// Verify magic / version / header CRC before trusting payload_len.
	if hdr[0] != magic[0] || hdr[1] != magic[1] || hdr[2] != magic[2] || hdr[3] != magic[3] {
		return Frame{}, ErrBadMagic
	}
	if hdr[4] != WireVersion {
		return Frame{}, ErrVersionMismatch
	}
	if crc(hdr[0:12]) != binary.LittleEndian.Uint32(hdr[12:16]) {
		return Frame{}, ErrHeaderCrcMismatch
	}
	plen := binary.LittleEndian.Uint32(hdr[8:12])
	// Bound the RESPONSE by the type the header declares: AckTracked carries a
	// coordinate of up to 298 bytes, every other response at most two. See
	// maxResponsePayload -- widening the bound for one frame type is not the
	// same as removing it.
	if int(plen) > maxResponsePayload(MessageType(hdr[5])) {
		return Frame{}, ErrPayloadTooLarge
	}
	rest := make([]byte, int(plen)+4)
	if _, err := io.ReadFull(c.conn, rest); err != nil {
		return Frame{}, fmt.Errorf("read payload+crc: %w", err)
	}
	// Reassemble into a single-frame buffer and reuse the strict decoder.
	full := make([]byte, 0, HeaderLen+int(plen)+4)
	full = append(full, hdr...)
	full = append(full, rest...)
	return DecodeFrame(full)
}

// readResponse reads a frame and classifies it as Ack / Nack.
func (c *Client) readResponse() (Response, error) {
	f, err := c.readFrame()
	if err != nil {
		return Response{}, err
	}
	r := Response{Frame: f}
	switch f.MessageType {
	case MsgAck:
		r.IsAck = true
	case MsgNack:
		r.IsNack = true
		if len(f.Payload) < 1 {
			return r, fmt.Errorf("Nack frame had empty payload (expected >=1 reason byte)")
		}
		r.NackReason = NackReason(f.Payload[0])
		if r.NackReason == NackVersionMismatch && len(f.Payload) >= 2 {
			r.DaemonWireVersion = f.Payload[1]
			r.HasDaemonVersion = true
		}
	case MsgAckTracked:
		coord, cerr := DecodeCoordinate(f.Payload)
		if cerr != nil {
			return r, fmt.Errorf("AckTracked payload is not a coordinate: %w", cerr)
		}
		r.IsAckTracked = true
		r.Coordinate = coord
	case MsgHealthCheckResponse:
		// fine; caller decides
	default:
		return r, fmt.Errorf("unexpected response message_type %s", f.MessageType)
	}
	return r, nil
}

// Push writes a pre-encoded frame and reads exactly one response.
// PushTracked pushes a record and returns where it landed.
//
// Identical to a plain Push in every other respect -- same tiers, same caps,
// same Nack reasons -- but answered with an AckTracked carrying the coordinate.
//
// A bare Ack in reply is an error, not a success: the request determines the
// response shape, so an Ack here means the peer did not understand what was
// asked, and treating it as success would report a coordinate that does not
// exist. A daemon predating the type answers Nack(UnknownMessage), which is
// returned wrapped in ErrTrackedUnsupported -- open a new connection and use
// Push, because the daemon closes this one.
func (c *Client) PushTracked(payload []byte, d Durability) (RecordCoordinate, error) {
	resp, err := c.PushRaw(EncodePushTracked(payload, d))
	if err != nil {
		return RecordCoordinate{}, err
	}
	switch {
	case resp.IsAckTracked:
		return resp.Coordinate, nil
	case resp.IsNack && resp.NackReason == NackUnknownMessage:
		return RecordCoordinate{}, fmt.Errorf("%w: %s", ErrTrackedUnsupported, resp.NackReason)
	case resp.IsNack:
		return RecordCoordinate{}, fmt.Errorf("Nack: %s", resp.NackReason)
	case resp.IsAck:
		return RecordCoordinate{}, errors.New(
			"daemon answered a PushTracked with a bare Ack; the record may be durable " +
				"but its coordinate is unknown")
	}
	return RecordCoordinate{}, fmt.Errorf("unexpected response %s", resp.Frame.MessageType)
}

func (c *Client) PushRaw(frame []byte) (Response, error) {
	if _, err := c.conn.Write(frame); err != nil {
		return Response{}, fmt.Errorf("write frame: %w", err)
	}
	return c.readResponse()
}

// SetReadDeadline lets edge-case tests detect a silent close / no-response.
func (c *Client) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}
