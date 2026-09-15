package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Run MY batch codec against the batch-extension conformance vectors.
//
// The frozen wire_v1_vectors.json is untouched by this extension: PushBatch and
// AckBatch are additive message-type bytes within wire v1, so a client that
// ignores them stays fully conformant. This file exercises the separate
// wire_v1_batch_vectors.json for a client that does implement them.

type batchDoc struct {
	MaxBatchRecordsHardCap int `json:"max_batch_records_hard_cap"`
	MaxAckBatchPayloadLen  int `json:"max_ack_batch_payload_len"`
	FrameVectors           []struct {
		Name        string `json:"name"`
		Hex         string `json:"hex"`
		Decode      string `json:"decode"`
		MessageType string `json:"message_type"`
		PayloadHex  string `json:"payload_hex"`
	} `json:"frame_vectors"`
	BodyVectors []struct {
		Name       string   `json:"name"`
		Hex        string   `json:"hex"`
		Decode     string   `json:"decode"`
		RecordsHex []string `json:"records_hex"`
	} `json:"body_vectors"`
	AckVectors []struct {
		Name        string `json:"name"`
		Hex         string `json:"hex"`
		Decode      string `json:"decode"`
		Expected    int    `json:"expected"`
		Accepted    []bool `json:"accepted"`
		AcceptedAll *bool  `json:"accepted_all"`
	} `json:"ack_vectors"`
}

func loadBatch(t *testing.T) batchDoc {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "conformance", "wire_v1_batch_vectors.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc batchDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return doc
}

func TestBatchVectorsPinTheConstants(t *testing.T) {
	doc := loadBatch(t)
	if doc.MaxBatchRecordsHardCap != MaxBatchRecordsHardCap {
		t.Fatalf("hard cap: vectors say %d, this client says %d",
			doc.MaxBatchRecordsHardCap, MaxBatchRecordsHardCap)
	}
	if doc.MaxAckBatchPayloadLen != MaxAckBatchPayload {
		t.Fatalf("ack payload bound: vectors say %d, this client says %d",
			doc.MaxAckBatchPayloadLen, MaxAckBatchPayload)
	}
	// The reason the hard cap is 2048: the reply stays inside the bound this
	// client already had for AckTracked, so batching adds no new allocation
	// maximum. If the cap is raised past that, this fails.
	if MaxAckBatchPayload > MaxTrackedAckPayload {
		t.Fatalf("AckBatch (%d) now exceeds AckTracked (%d) -- batching has "+
			"introduced a new largest response", MaxAckBatchPayload, MaxTrackedAckPayload)
	}
}

func TestBatchFrameVectors(t *testing.T) {
	doc := loadBatch(t)
	if len(doc.FrameVectors) == 0 {
		t.Fatal("no frame vectors")
	}
	for _, v := range doc.FrameVectors {
		raw, err := hex.DecodeString(v.Hex)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", v.Name, err)
		}
		f, err := DecodeFrame(raw)
		if err != nil {
			t.Errorf("%s: decode: %v", v.Name, err)
			continue
		}
		if got := f.MessageType.String(); got != v.MessageType {
			t.Errorf("%s: message_type %s, want %s", v.Name, got, v.MessageType)
		}
		if got := hex.EncodeToString(f.Payload); got != v.PayloadHex {
			t.Errorf("%s: payload %s, want %s", v.Name, got, v.PayloadHex)
		}
		// The response cap must admit this payload. An AckBatch at the hard cap
		// is 259 bytes; a client still using the 2-byte default would refuse
		// its own daemon's reply.
		if v.MessageType == "AckBatch" {
			if n := len(f.Payload); n > maxResponsePayload(MsgAckBatch) {
				t.Errorf("%s: payload %d exceeds this client's AckBatch cap %d",
					v.Name, n, maxResponsePayload(MsgAckBatch))
			} else if n <= MaxResponsePayload {
				t.Errorf("%s: payload %d does not exceed the default cap %d, so "+
					"the widening is untested by this vector", v.Name, n, MaxResponsePayload)
			}
		}
	}
}

func TestBatchBodyVectors(t *testing.T) {
	doc := loadBatch(t)
	okSeen, rejectSeen := 0, 0
	for _, v := range doc.BodyVectors {
		raw, err := hex.DecodeString(v.Hex)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", v.Name, err)
		}
		records, err := DecodeBatchBody(raw, MaxBatchRecordsHardCap, MaxPayloadHardCap)
		if v.Decode == "ok" {
			if err != nil {
				t.Errorf("%s: expected ok, got %v", v.Name, err)
				continue
			}
			if len(records) != len(v.RecordsHex) {
				t.Errorf("%s: %d records, want %d", v.Name, len(records), len(v.RecordsHex))
				continue
			}
			for i, r := range records {
				if got := hex.EncodeToString(r); got != v.RecordsHex[i] {
					t.Errorf("%s: record %d = %s, want %s", v.Name, i, got, v.RecordsHex[i])
				}
			}
			if got := hex.EncodeToString(EncodeBatchBody(records)); got != v.Hex {
				t.Errorf("%s: re-encode\n got %s\n want %s", v.Name, got, v.Hex)
			}
			okSeen++
			continue
		}
		if err == nil {
			t.Errorf("%s: expected rejection %q, but it decoded", v.Name, v.Decode)
			continue
		}
		if err.Error() != v.Decode {
			t.Errorf("%s: rejected as %q, want %q", v.Name, err, v.Decode)
		}
		rejectSeen++
	}
	if okSeen == 0 || rejectSeen == 0 {
		t.Fatalf("body vectors must cover both outcomes (%d ok, %d rejected)", okSeen, rejectSeen)
	}
}

func TestBatchAckVectors(t *testing.T) {
	doc := loadBatch(t)
	okSeen, rejectSeen := 0, 0
	for _, v := range doc.AckVectors {
		raw, err := hex.DecodeString(v.Hex)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", v.Name, err)
		}
		accepted, err := DecodeAckBatch(raw, v.Expected)
		if v.Decode == "ok" {
			if err != nil {
				t.Errorf("%s: expected ok, got %v", v.Name, err)
				continue
			}
			want := v.Accepted
			if want == nil {
				if v.AcceptedAll == nil {
					t.Fatalf("%s: give exactly one of accepted / accepted_all", v.Name)
				}
				want = make([]bool, v.Expected)
				for i := range want {
					want[i] = *v.AcceptedAll
				}
			}
			if len(accepted) != len(want) {
				t.Errorf("%s: %d verdicts, want %d", v.Name, len(accepted), len(want))
				continue
			}
			for i := range want {
				if accepted[i] != want[i] {
					t.Errorf("%s: verdicts disagree at %d -- if this is the N=9 "+
						"asymmetric vector, this client's bitmap bit order is inverted",
						v.Name, i)
					break
				}
			}
			if got := hex.EncodeToString(EncodeAckBatch(accepted)); got != v.Hex {
				t.Errorf("%s: re-encode\n got %s\n want %s", v.Name, got, v.Hex)
			}
			okSeen++
			continue
		}
		if err == nil {
			t.Errorf("%s: expected rejection %q, but it decoded", v.Name, v.Decode)
			continue
		}
		if err.Error() != v.Decode {
			t.Errorf("%s: rejected as %q, want %q", v.Name, err, v.Decode)
		}
		rejectSeen++
	}
	if okSeen == 0 || rejectSeen == 0 {
		t.Fatalf("ack vectors must cover both outcomes (%d ok, %d rejected)", okSeen, rejectSeen)
	}
}

// The bitmap convention against a hand-written literal, so a reader can see it
// without running the vectors. Records 0 and 2 accepted put bits in the LOW end
// of byte 0; MSB-first would give a0 80.
func TestBitmapIsLSBFirst(t *testing.T) {
	accepted := []bool{true, false, true, false, false, false, false, false, true}
	got := EncodeAckBatch(accepted)
	want := []byte{AckBatchVersion, 9, 0, 0b0000_0101, 0b0000_0001}
	if hex.EncodeToString(got) != hex.EncodeToString(want) {
		t.Fatalf("bitmap = %x, want %x (MSB-first would be a080)", got, want)
	}
	back, err := DecodeAckBatch(got, 9)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	for i := range accepted {
		if back[i] != accepted[i] {
			t.Fatalf("round trip differs at %d", i)
		}
	}
}

// An AckBatch is refused when it answers a batch of a different size, because a
// bitmap carries no other way to detect a desync.
func TestAckBatchCountMustMatchTheRequest(t *testing.T) {
	payload := EncodeAckBatch([]bool{true, true, true})
	if _, err := DecodeAckBatch(payload, 4); !errors.Is(err, ErrBatchLengthMismatch) {
		t.Fatalf("expected LengthMismatch for a count disagreement, got %v", err)
	}
	if _, err := DecodeAckBatch(payload, 3); err != nil {
		t.Fatalf("the matching count must decode: %v", err)
	}
}
