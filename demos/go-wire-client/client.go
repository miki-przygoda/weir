package main

import (
	"encoding/binary"
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

// Response is a decoded daemon reply.
type Response struct {
	Frame      Frame
	IsAck      bool
	IsNack     bool
	NackReason NackReason
	// DaemonWireVersion is set only for a VersionMismatch Nack (2-byte payload).
	DaemonWireVersion uint8
	HasDaemonVersion  bool
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
	if plen > MaxPayloadHardCap {
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
	case MsgHealthCheckResponse:
		// fine; caller decides
	default:
		return r, fmt.Errorf("unexpected response message_type %s", f.MessageType)
	}
	return r, nil
}

// Push writes a pre-encoded frame and reads exactly one response.
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
