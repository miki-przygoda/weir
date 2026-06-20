package main

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type vector struct {
	Name        string `json:"name"`
	Notes       string `json:"notes"`
	Hex         string `json:"hex"`
	Decode      string `json:"decode"`
	MessageType string `json:"message_type"`
	Durability  string `json:"durability"`
	Flags       int    `json:"flags"`
	PayloadHex  string `json:"payload_hex"`
}

type vectorFile struct {
	WireVersion       int      `json:"wire_version"`
	MaxPayloadHardCap int      `json:"max_payload_hard_cap"`
	Vectors           []vector `json:"vectors"`
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	// Canonical vectors live in the weir repo at docs/conformance/wire_v1_vectors.json.
	// From this demo dir (demos/go-wire-client) that's two levels up; override with
	// WEIR_CONFORMANCE_VECTORS. No vendored copy — the docs file is the source of truth.
	path := os.Getenv("WEIR_CONFORMANCE_VECTORS")
	if path == "" {
		path = filepath.Join("..", "..", "docs", "conformance", "wire_v1_vectors.json")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var vf vectorFile
	if err := json.Unmarshal(b, &vf); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	return vf
}

// TestConformanceDecode runs every vector through DecodeFrame and asserts the
// outcome matches the JSON `decode` field. For "ok" vectors it also asserts the
// decoded header/payload fields and that re-encoding round-trips to the same hex.
func TestConformanceDecode(t *testing.T) {
	vf := loadVectors(t)
	if vf.WireVersion != WireVersion {
		t.Fatalf("vector file wire_version %d != client WireVersion %d", vf.WireVersion, WireVersion)
	}
	if vf.MaxPayloadHardCap != MaxPayloadHardCap {
		t.Fatalf("vector file cap %d != client cap %d", vf.MaxPayloadHardCap, MaxPayloadHardCap)
	}
	pass := 0
	for _, v := range vf.Vectors {
		buf, err := hex.DecodeString(v.Hex)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", v.Name, err)
		}
		f, derr := DecodeFrame(buf)
		if v.Decode == "ok" {
			if derr != nil {
				t.Errorf("%s: expected ok, got error %v", v.Name, derr)
				continue
			}
			if f.MessageType.String() != v.MessageType {
				t.Errorf("%s: message_type = %s, want %s", v.Name, f.MessageType, v.MessageType)
			}
			if f.Durability.String() != v.Durability {
				t.Errorf("%s: durability = %s, want %s", v.Name, f.Durability, v.Durability)
			}
			if int(f.Flags) != v.Flags {
				t.Errorf("%s: flags = %d, want %d", v.Name, f.Flags, v.Flags)
			}
			if hex.EncodeToString(f.Payload) != v.PayloadHex {
				t.Errorf("%s: payload = %s, want %s", v.Name, hex.EncodeToString(f.Payload), v.PayloadHex)
			}
			// Encode/decode are inverses: re-encoding must reproduce the bytes.
			re := EncodeFrame(f)
			if hex.EncodeToString(re) != v.Hex {
				t.Errorf("%s: re-encode = %s, want %s", v.Name, hex.EncodeToString(re), v.Hex)
			}
		} else {
			if derr == nil {
				t.Errorf("%s: expected rejection %q, got ok", v.Name, v.Decode)
				continue
			}
			if derr.Error() != v.Decode {
				t.Errorf("%s: rejection = %q, want %q", v.Name, derr.Error(), v.Decode)
			}
		}
		pass++
	}
	t.Logf("%d/%d vectors processed", pass, len(vf.Vectors))
}
