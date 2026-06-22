// Package main implements a weir v1 wire-protocol producer in pure Go,
// built from docs/wire_protocol.md + docs/conformance/wire_v1_vectors.json
// with NO dependency on the Rust weir-client. Stdlib only.
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// ---- Wire constants (from wire_protocol.md) ----

const (
	WireVersion       = 1
	HeaderLen         = 16
	MaxPayloadHardCap = 16 * 1024 * 1024 // 16 MiB
)

var magic = [4]byte{'W', 'E', 'I', 'R'} // 0x57 0x45 0x49 0x52

// MessageType byte (wire_protocol.md "Message types").
type MessageType uint8

const (
	MsgPush                MessageType = 0x01
	MsgAck                 MessageType = 0x02
	MsgNack                MessageType = 0x03
	MsgHealthCheck         MessageType = 0x04
	MsgHealthCheckResponse MessageType = 0x05
)

func (m MessageType) String() string {
	switch m {
	case MsgPush:
		return "Push"
	case MsgAck:
		return "Ack"
	case MsgNack:
		return "Nack"
	case MsgHealthCheck:
		return "HealthCheck"
	case MsgHealthCheckResponse:
		return "HealthCheckResponse"
	default:
		return fmt.Sprintf("Unknown(0x%02x)", uint8(m))
	}
}

// Durability tier byte (wire_protocol.md "Durability tiers").
type Durability uint8

const (
	Sync     Durability = 0x01
	Batched  Durability = 0x02
	Buffered Durability = 0x03
)

func (d Durability) String() string {
	switch d {
	case Sync:
		return "Sync"
	case Batched:
		return "Batched"
	case Buffered:
		return "Buffered"
	default:
		return fmt.Sprintf("Unknown(0x%02x)", uint8(d))
	}
}

// NackReason byte (wire_protocol.md "Nack payload format").
type NackReason uint8

const (
	NackBadMagic        NackReason = 0x01
	NackVersionMismatch NackReason = 0x02
	NackBadHeaderCrc    NackReason = 0x03
	NackPayloadTooLarge NackReason = 0x04
	NackBadPayloadCrc   NackReason = 0x05
	NackInternalError   NackReason = 0x06
	NackEmptyPayload    NackReason = 0x07
	NackUnknownMessage  NackReason = 0x08
	NackReservedFlags   NackReason = 0x09
)

func (r NackReason) String() string {
	switch r {
	case NackBadMagic:
		return "BadMagic"
	case NackVersionMismatch:
		return "VersionMismatch"
	case NackBadHeaderCrc:
		return "BadHeaderCrc"
	case NackPayloadTooLarge:
		return "PayloadTooLarge"
	case NackBadPayloadCrc:
		return "BadPayloadCrc"
	case NackInternalError:
		return "InternalError"
	case NackEmptyPayload:
		return "EmptyPayload"
	case NackUnknownMessage:
		return "UnknownMessage"
	case NackReservedFlags:
		return "ReservedFlagsSet"
	default:
		// 0x0A..0xFF reserved: surface the raw byte (spec line 77-79).
		return fmt.Sprintf("Reserved(0x%02x)", uint8(r))
	}
}

// Permanent reports whether the daemon closes the connection after this Nack.
// (wire_protocol.md "When the server closes the connection" table.)
func (r NackReason) Permanent() bool {
	switch r {
	case NackInternalError:
		return false // transient; connection kept open
	default:
		return true
	}
}

// crc is the IEEE / ISO-3309 CRC-32 (zlib/PNG/Ethernet), per wire_protocol.md.
// hash/crc32.IEEETable is exactly this variant.
func crc(b []byte) uint32 {
	return crc32.Checksum(b, crc32.IEEETable)
}

// Frame is a decoded weir frame.
type Frame struct {
	Version     uint8
	MessageType MessageType
	Durability  Durability
	Flags       uint8
	Payload     []byte
}

// EncodeFrame builds a complete on-the-wire frame. It does NOT enforce
// semantic rules (empty payload, reserved flags, etc.) so the conformance
// tests can craft deliberately-bad frames; encoding is purely mechanical.
func EncodeFrame(f Frame) []byte {
	plen := len(f.Payload)
	buf := make([]byte, HeaderLen+plen+4)
	copy(buf[0:4], magic[:])
	buf[4] = f.Version
	buf[5] = byte(f.MessageType)
	buf[6] = byte(f.Durability)
	buf[7] = f.Flags
	binary.LittleEndian.PutUint32(buf[8:12], uint32(plen))
	binary.LittleEndian.PutUint32(buf[12:16], crc(buf[0:12]))
	copy(buf[16:16+plen], f.Payload)
	binary.LittleEndian.PutUint32(buf[16+plen:], crc(f.Payload))
	return buf
}

// EncodePush is the common case: a Push of the given payload at a tier.
func EncodePush(payload []byte, d Durability) []byte {
	return EncodeFrame(Frame{
		Version:     WireVersion,
		MessageType: MsgPush,
		Durability:  d,
		Flags:       0,
		Payload:     payload,
	})
}

// EncodeHealthCheck builds a zero-payload HealthCheck (Sync filler by convention).
func EncodeHealthCheck() []byte {
	return EncodeFrame(Frame{
		Version:     WireVersion,
		MessageType: MsgHealthCheck,
		Durability:  Sync,
		Flags:       0,
		Payload:     nil,
	})
}

// ---- Decode errors, mapped to the conformance.md rejection tags ----

var (
	ErrBadMagic           = errors.New("BadMagic")
	ErrVersionMismatch    = errors.New("VersionMismatch")
	ErrUnknownMessageType = errors.New("UnknownMessageType")
	ErrUnknownDurability  = errors.New("UnknownDurability")
	ErrHeaderCrcMismatch  = errors.New("HeaderCrcMismatch")
	ErrPayloadCrcMismatch = errors.New("PayloadCrcMismatch")
	ErrTruncatedFrame     = errors.New("TruncatedFrame")
	ErrPayloadTooLarge    = errors.New("PayloadTooLarge")
	ErrReservedFlagsSet   = errors.New("ReservedFlagsSet")
	ErrTrailingBytes      = errors.New("TrailingBytes")
)

// DecodeFrame decodes a buffer that MUST be exactly one frame, mirroring the
// weir-core reference codec (Envelope::decode) semantics described in
// wire_protocol.md "Reference codec: one buffer, one frame". The decode order
// follows the mandatory server-side order (magic, version, header CRC, fields,
// payload cap, payload, payload CRC).
func DecodeFrame(buf []byte) (Frame, error) {
	if len(buf) < HeaderLen {
		// A buffer that starts with valid magic but is < 16 bytes is
		// TruncatedFrame, not BadMagic (spec line 108). But we can't even
		// know the magic without the bytes; per the conformance vector
		// reject_truncated_header, a short buffer is TruncatedFrame.
		return Frame{}, ErrTruncatedFrame
	}
	// 1. Magic
	if buf[0] != magic[0] || buf[1] != magic[1] || buf[2] != magic[2] || buf[3] != magic[3] {
		return Frame{}, ErrBadMagic
	}
	// 2. Version (before header CRC, per decode order step 2)
	if buf[4] != WireVersion {
		return Frame{}, ErrVersionMismatch
	}
	// 3. Header CRC over [0..12]
	wantHdrCrc := binary.LittleEndian.Uint32(buf[12:16])
	if crc(buf[0:12]) != wantHdrCrc {
		return Frame{}, ErrHeaderCrcMismatch
	}
	// 4. Header field parsing
	mt := MessageType(buf[5])
	switch mt {
	case MsgPush, MsgAck, MsgNack, MsgHealthCheck, MsgHealthCheckResponse:
	default:
		return Frame{}, ErrUnknownMessageType
	}
	d := Durability(buf[6])
	switch d {
	case Sync, Batched, Buffered:
	default:
		return Frame{}, ErrUnknownDurability
	}
	if buf[7] != 0 {
		return Frame{}, ErrReservedFlagsSet
	}
	// 5. Payload length cap (before allocation / frame-length check)
	plen := binary.LittleEndian.Uint32(buf[8:12])
	if plen > MaxPayloadHardCap {
		return Frame{}, ErrPayloadTooLarge
	}
	// 6. Frame-length check: buffer must be EXACTLY one frame.
	want := HeaderLen + int(plen) + 4
	if len(buf) < want {
		return Frame{}, ErrTruncatedFrame
	}
	if len(buf) > want {
		return Frame{}, ErrTrailingBytes
	}
	// 7. Payload CRC
	payload := buf[HeaderLen : HeaderLen+int(plen)]
	wantPayCrc := binary.LittleEndian.Uint32(buf[HeaderLen+int(plen):])
	if crc(payload) != wantPayCrc {
		return Frame{}, ErrPayloadCrcMismatch
	}
	out := make([]byte, len(payload))
	copy(out, payload)
	return Frame{
		Version:     buf[4],
		MessageType: mt,
		Durability:  d,
		Flags:       buf[7],
		Payload:     out,
	}, nil
}
