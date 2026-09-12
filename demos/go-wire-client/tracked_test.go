package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// Run MY codec against the tracked-extension conformance vectors.
//
// The frozen wire_v1_vectors.json is deliberately untouched by this extension:
// PushTracked/AckTracked are additive message-type bytes within wire v1, so a
// client that ignores them stays fully conformant (docs/conformance.md). This
// file exercises the separate wire_v1_tracked_vectors.json for a client that
// does implement them.

type trackedDoc struct {
	MaxTrackedAckPayloadLen int `json:"max_tracked_ack_payload_len"`
	FrameVectors            []struct {
		Name        string `json:"name"`
		Hex         string `json:"hex"`
		Decode      string `json:"decode"`
		MessageType string `json:"message_type"`
		PayloadHex  string `json:"payload_hex"`
	} `json:"frame_vectors"`
	// Index MUST be uint64. Decoding into map[string]any or a float64 field
	// silently loses precision on coordinate_max_segment, whose index is
	// u64::MAX -- it comes back as 1.8446744073709552e+19.
	CoordinateVectors []struct {
		Name        string `json:"name"`
		Hex         string `json:"hex"`
		Decode      string `json:"decode"`
		Segment     string `json:"segment"`
		Index       uint64 `json:"index"`
		RecordIDHex string `json:"record_id_hex"`
	} `json:"coordinate_vectors"`
}

func loadTracked(t *testing.T) trackedDoc {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "conformance", "wire_v1_tracked_vectors.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var doc trackedDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	if len(doc.FrameVectors) == 0 || len(doc.CoordinateVectors) == 0 {
		t.Fatal("vector file parsed but is empty")
	}
	return doc
}

func TestTrackedFrameVectors(t *testing.T) {
	doc := loadTracked(t)
	for _, v := range doc.FrameVectors {
		raw, err := hex.DecodeString(v.Hex)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", v.Name, err)
		}
		f, err := DecodeFrame(raw)
		if v.Decode != "ok" {
			if err == nil {
				t.Errorf("%s: expected rejection %q, decoded fine", v.Name, v.Decode)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: expected ok, got %v", v.Name, err)
			continue
		}
		if got := f.MessageType.String(); got != v.MessageType {
			t.Errorf("%s: message_type = %s, want %s", v.Name, got, v.MessageType)
		}
		if got := hex.EncodeToString(f.Payload); got != v.PayloadHex {
			t.Errorf("%s: payload differs", v.Name)
		}
		// Re-encoding must reproduce the vector byte-for-byte, or the encoder
		// and decoder disagree about a layout one of them is guessing at.
		if again := EncodeFrame(f); hex.EncodeToString(again) != v.Hex {
			t.Errorf("%s: re-encode does not round-trip", v.Name)
		}
	}
}

func TestTrackedCoordinateVectors(t *testing.T) {
	doc := loadTracked(t)
	for _, v := range doc.CoordinateVectors {
		raw, err := hex.DecodeString(v.Hex)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", v.Name, err)
		}
		c, err := DecodeCoordinate(raw)

		if v.Decode == "ok" {
			if err != nil {
				t.Errorf("%s: expected ok, got %v", v.Name, err)
				continue
			}
			if c.Segment != v.Segment {
				t.Errorf("%s: segment = %q, want %q", v.Name, c.Segment, v.Segment)
			}
			if c.Index != v.Index {
				t.Errorf("%s: index = %d, want %d", v.Name, c.Index, v.Index)
			}
			if c.RecordIDHex() != v.RecordIDHex {
				t.Errorf("%s: record_id differs", v.Name)
			}
			continue
		}

		// Rejections are matched by the sentinel's name, which is the vector
		// tag -- the same convention the frame errors already use.
		if err == nil {
			t.Errorf("%s: expected rejection %q, decoded fine", v.Name, v.Decode)
			continue
		}
		if err.Error() != v.Decode {
			t.Errorf("%s: rejected as %q, want %q", v.Name, err.Error(), v.Decode)
		}
	}
}

// u64::MAX must survive the JSON decode. Go's encoding/json hands an untyped
// number back as float64, which silently rounds it; the typed struct above is
// what prevents that, and this asserts the prevention rather than assuming it.
func TestTrackedIndexSurvivesU64Max(t *testing.T) {
	doc := loadTracked(t)
	var seen bool
	for _, v := range doc.CoordinateVectors {
		if v.Index == ^uint64(0) {
			seen = true
		}
	}
	if !seen {
		t.Fatal("no vector carries u64::MAX as its index; this test no longer " +
			"guards the precision hazard it was written for")
	}

	var loose map[string]any
	raw, _ := os.ReadFile(filepath.Join("..", "..", "docs", "conformance", "wire_v1_tracked_vectors.json"))
	_ = json.Unmarshal(raw, &loose)
	vecs := loose["coordinate_vectors"].([]any)
	for _, e := range vecs {
		m := e.(map[string]any)
		if m["name"] != "coordinate_max_segment" {
			continue
		}
		// Demonstrates the hazard rather than asserting it from memory: the
		// same field through an untyped decode arrives as a float64, which
		// cannot represent u64::MAX. Converting that float back to uint64 is an
		// out-of-range conversion and implementation-defined, so compare the
		// decimal text instead -- that is well-defined and shows the loss.
		f, ok := m["index"].(float64)
		if !ok {
			t.Fatalf("untyped decode gave %T, expected float64; this guard no "+
				"longer demonstrates the hazard it was written for", m["index"])
		}
		const exact = "18446744073709551615"
		if got := strconv.FormatFloat(f, 'f', -1, 64); got == exact {
			t.Errorf("expected an untyped decode to lose precision on u64::MAX, "+
				"but it produced %s exactly; decode into typed structs anyway", got)
		} else {
			t.Logf("untyped decode of u64::MAX yields %s, not %s "+
				"-- which is why CoordinateVectors.Index is a uint64 field", got, exact)
		}
	}
}

// The response cap is what tracked push actually changes on the read path.
func TestTrackedResponseCapIsWidenedForAckTrackedOnly(t *testing.T) {
	if got := maxResponsePayload(MsgAckTracked); got != MaxTrackedAckPayload {
		t.Errorf("AckTracked cap = %d, want %d", got, MaxTrackedAckPayload)
	}
	for _, mt := range []MessageType{
		MsgPush, MsgAck, MsgNack, MsgHealthCheck, MsgHealthCheckResponse,
		MsgPushTracked, MessageType(0xff),
	} {
		if got := maxResponsePayload(mt); got != MaxResponsePayload {
			t.Errorf("%s unlocked the wider cap (%d); only AckTracked may", mt, got)
		}
	}

	doc := loadTracked(t)
	for _, v := range doc.FrameVectors {
		if v.MessageType != "AckTracked" {
			continue
		}
		n := len(v.PayloadHex) / 2
		if n <= MaxResponsePayload {
			t.Errorf("%s payload is %d bytes, which does not exercise the widened cap",
				v.Name, n)
		}
		if n > MaxTrackedAckPayload {
			t.Errorf("%s payload is %d bytes, over the tracked cap %d",
				v.Name, n, MaxTrackedAckPayload)
		}
	}
}

// decodeFrameV1Only is a decoder that knows only 0x01-0x05, kept here rather
// than in the production API because it exists solely to make the additivity
// claim executable: a v1-only reader must REJECT a tracked frame as an unknown
// message type, not misparse it. That is the whole basis for calling the
// extension additive.
func decodeFrameV1Only(buf []byte) error {
	if len(buf) < HeaderLen {
		return errors.New("TruncatedFrame")
	}
	switch MessageType(buf[5]) {
	case MsgPush, MsgAck, MsgNack, MsgHealthCheck, MsgHealthCheckResponse:
		return nil
	default:
		return ErrUnknownMessageType
	}
}

func TestTrackedFramesAreRejectedByAV1OnlyDecoder(t *testing.T) {
	doc := loadTracked(t)
	for _, v := range doc.FrameVectors {
		raw, _ := hex.DecodeString(v.Hex)
		if err := decodeFrameV1Only(raw); !errors.Is(err, ErrUnknownMessageType) {
			t.Errorf("%s: a v1-only decoder accepted a tracked frame (%v); the "+
				"extension is only additive because it does not", v.Name, err)
		}
	}
}

// Live: a tracked push against a running daemon. Skipped unless WEIR_SOCKET
// points at one, matching the convention in probe_test.go.
func TestLiveTrackedPush(t *testing.T) {
	c := dialOrSkip(t)
	defer c.Close()

	first, err := c.PushTracked([]byte("go tracked record"), Durable)
	if err != nil {
		t.Fatalf("PushTracked: %v", err)
	}
	if first.Index == 0 {
		t.Error("index is 1-based; 0 means the coordinate was not populated")
	}
	if first.Segment == "" {
		t.Error("segment address is empty")
	}
	t.Logf("index=%d segment=%s record_id=%s", first.Index, first.Segment, first.RecordIDHex()[:16])

	second, err := c.PushTracked([]byte("another"), Durable)
	if err != nil {
		t.Fatalf("second PushTracked: %v", err)
	}
	// Within one segment on one connection the index advances. It is NOT a
	// per-producer sequence -- other producers interleave, so holes are normal
	// -- but two consecutive pushes from the only producer must not go backwards.
	if second.Segment == first.Segment && second.Index <= first.Index {
		t.Errorf("index did not advance within a segment: %d then %d",
			first.Index, second.Index)
	}

	// A coordinate is an address, not a durability upgrade: Buffered gets one too.
	buffered, err := c.PushTracked([]byte("buffered"), Buffered)
	if err != nil {
		t.Fatalf("Buffered PushTracked: %v", err)
	}
	if buffered.Index == 0 {
		t.Error("a Buffered record must still get a coordinate")
	}

	// The additive property: an untracked push still works on the same
	// connection, after tracked ones.
	resp, err := c.PushRaw(EncodePush([]byte("untracked"), Durable))
	if err != nil || !resp.IsAck {
		t.Errorf("plain Push after tracked pushes failed: %v (ack=%v)", err, resp.IsAck)
	}
}
