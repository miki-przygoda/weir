package main

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// PushBatch / AckBatch: N records in one round trip, answered once.
//
// Implemented from docs/wire_protocol.md and checked against
// docs/conformance/wire_v1_batch_vectors.json. Nothing here is ported from the
// Rust reference -- an independent implementation is the only thing that can
// catch an under-specified format, and this one has a bug class no checksum
// detects: a bitmap written with the opposite bit order is a well-formed frame,
// valid CRCs and correct length, that reports failures as successes.
//
// Bit i lives in byte i/8 at mask 1 << (i%8): LSB-first.

const (
	BatchVersion      = 1
	AckBatchVersion   = 1
	BatchHeaderLen    = 1 + 2
	AckBatchHeaderLen = 1 + 2
)

// Rejection reasons, named to match the conformance vectors so a failure here
// points at the vector that pins it.
var (
	ErrBatchTruncated          = errors.New("Truncated")
	ErrBatchUnsupportedVersion = errors.New("UnsupportedVersion")
	ErrBatchEmpty              = errors.New("EmptyBatch")
	ErrBatchTooManyRecords     = errors.New("TooManyRecords")
	ErrBatchEmptyRecord        = errors.New("EmptyRecord")
	ErrBatchRecordTooLarge     = errors.New("RecordTooLarge")
	ErrBatchTruncatedRecord    = errors.New("TruncatedRecord")
	ErrBatchLengthMismatch     = errors.New("LengthMismatch")
	ErrBatchPaddingNotZero     = errors.New("PaddingNotZero")
)

// EncodeBatchBody builds a PushBatch body: version, u16 count, then each record
// as a u32 little-endian length followed by its bytes.
func EncodeBatchBody(records [][]byte) []byte {
	n := BatchHeaderLen
	for _, r := range records {
		n += 4 + len(r)
	}
	out := make([]byte, 0, n)
	out = append(out, BatchVersion)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(records)))
	for _, r := range records {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(r)))
		out = append(out, r...)
	}
	return out
}

// DecodeBatchBody parses a PushBatch body.
//
// The check ORDER is part of the contract. The declared count is validated
// against the cap BEFORE the slice is sized by it, and a record's declared
// length is checked against the cap BEFORE it is added to the cursor. The
// frame's payload CRC has already passed at this point and proves nothing here:
// a hostile peer computes a perfectly valid CRC over a body declaring 65,535
// records in three bytes.
func DecodeBatchBody(body []byte, maxRecords, maxRecordLen int) ([][]byte, error) {
	if len(body) < BatchHeaderLen {
		return nil, ErrBatchTruncated
	}
	if body[0] != BatchVersion {
		return nil, ErrBatchUnsupportedVersion
	}
	declared := int(binary.LittleEndian.Uint16(body[1:3]))
	if declared == 0 {
		return nil, ErrBatchEmpty
	}
	cap := maxRecords
	if MaxBatchRecordsHardCap < cap {
		cap = MaxBatchRecordsHardCap
	}
	if declared > cap {
		return nil, ErrBatchTooManyRecords
	}

	recordCap := maxRecordLen
	if MaxPayloadHardCap < recordCap {
		recordCap = MaxPayloadHardCap
	}
	out := make([][]byte, 0, declared)
	cursor := BatchHeaderLen
	for cursor < len(body) {
		if cursor+4 > len(body) {
			return nil, ErrBatchTruncatedRecord
		}
		n := int(binary.LittleEndian.Uint32(body[cursor : cursor+4]))
		cursor += 4
		if n == 0 {
			return nil, ErrBatchEmptyRecord
		}
		if n > recordCap {
			return nil, ErrBatchRecordTooLarge
		}
		if cursor+n > len(body) {
			return nil, ErrBatchTruncatedRecord
		}
		// Stop before overrunning the declared count, so a body carrying more
		// records than it declares is a mismatch rather than a silent drop.
		if len(out) == declared {
			return nil, ErrBatchLengthMismatch
		}
		out = append(out, body[cursor:cursor+n])
		cursor += n
	}
	if len(out) != declared || cursor != len(body) {
		return nil, ErrBatchLengthMismatch
	}
	return out, nil
}

// EncodeAckBatch builds an AckBatch payload from per-record outcomes.
// Padding bits in the final byte are left zero, which the decoder requires.
func EncodeAckBatch(accepted []bool) []byte {
	out := make([]byte, AckBatchHeaderLen+(len(accepted)+7)/8)
	out[0] = AckBatchVersion
	binary.LittleEndian.PutUint16(out[1:3], uint16(len(accepted)))
	for i, ok := range accepted {
		if ok {
			out[AckBatchHeaderLen+i/8] |= 1 << (i % 8)
		}
	}
	return out
}

// DecodeAckBatch parses an AckBatch payload into per-record outcomes.
//
// `expected` is the count this client sent. A bitmap is the first weir response
// whose meaning depends on client-held state -- an Ack says "your last record"
// and an AckTracked carries its own coordinate, but a bitmap is meaningless
// without knowing which batch it answers. ceil(N/8) is not injective (N of 1017
// through 1024 all give 131 bytes), so the echoed count is the only thing that
// can catch a desync.
//
// A set bit means the record is durable at the requested tier and inherits
// weir's crown invariant. A CLEAR bit is the weak statement: not durable as of
// this reply, retry it, and expect it may nonetheless have been written.
func DecodeAckBatch(payload []byte, expected int) ([]bool, error) {
	if len(payload) < AckBatchHeaderLen {
		return nil, ErrBatchTruncated
	}
	if payload[0] != AckBatchVersion {
		return nil, ErrBatchUnsupportedVersion
	}
	declared := int(binary.LittleEndian.Uint16(payload[1:3]))
	if declared == 0 {
		return nil, ErrBatchEmpty
	}
	if declared != expected {
		return nil, ErrBatchLengthMismatch
	}
	if len(payload) != AckBatchHeaderLen+(declared+7)/8 {
		return nil, ErrBatchLengthMismatch
	}
	// Padding bits must be zero. Ignoring them would make popcount == N -- the
	// obvious way to ask "did the whole batch succeed" -- silently wrong.
	if used := declared % 8; used != 0 {
		if payload[len(payload)-1]&^byte((1<<used)-1) != 0 {
			return nil, ErrBatchPaddingNotZero
		}
	}
	bits := payload[AckBatchHeaderLen:]
	out := make([]bool, declared)
	for i := range out {
		out[i] = bits[i/8]&(1<<(i%8)) != 0
	}
	return out, nil
}

// BatchOutcome reports which records of a batch were accepted.
type BatchOutcome struct{ Accepted []bool }

// AllAccepted reports whether every record is durable.
func (b BatchOutcome) AllAccepted() bool {
	for _, ok := range b.Accepted {
		if !ok {
			return false
		}
	}
	return true
}

// RejectedIndices lists the records to retry.
func (b BatchOutcome) RejectedIndices() []int {
	var out []int
	for i, ok := range b.Accepted {
		if !ok {
			out = append(out, i)
		}
	}
	return out
}

func (b BatchOutcome) String() string {
	return fmt.Sprintf("BatchOutcome{%d records, %d rejected}",
		len(b.Accepted), len(b.RejectedIndices()))
}
