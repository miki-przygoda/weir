package main

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

// These tests probe corners the docs are quieter about, against the LIVE
// daemon. They are skipped unless WEIR_SOCKET points at a running daemon.
func liveSocket(t *testing.T) string {
	t.Helper()
	s := os.Getenv("WEIR_SOCKET")
	if s == "" {
		t.Skip("WEIR_SOCKET not set; skipping live probe")
	}
	return s
}

func dialOrSkip(t *testing.T) *Client {
	t.Helper()
	c, err := Dial(liveSocket(t))
	if err != nil {
		t.Skipf("dial failed (daemon down?): %v", err)
	}
	return c
}

// Probe: a buffer that starts with valid magic but is shorter than 16 bytes.
// Spec line 108 says this is TruncatedFrame, NOT BadMagic. But on a live
// stream the daemon BLOCKS waiting for the rest of the header — it can't know
// the frame is "truncated" because more bytes might arrive. We verify the
// daemon does NOT respond (it waits), distinguishing live framing from the
// one-shot reference decoder.
func TestProbe_ShortHeaderBlocks(t *testing.T) {
	c := dialOrSkip(t)
	defer c.Close()
	// 8 bytes: "WEIR" + version + push + sync + flags. No len/crc/payload.
	partial := []byte{'W', 'E', 'I', 'R', WireVersion, byte(MsgPush), byte(Sync), 0}
	if _, err := c.conn.Write(partial); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	one := make([]byte, 1)
	_, err := c.conn.Read(one)
	// Expect a timeout (daemon waiting for more header bytes), NOT an EOF/Nack.
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Logf("OK: daemon waits for full header on a partial write (no premature Nack)")
		return
	}
	if errors.Is(err, io.EOF) {
		t.Fatalf("daemon closed on a partial 8-byte header; expected it to wait")
	}
	t.Fatalf("unexpected read result on partial header: err=%v bytes=%v", err, one)
}

// Probe: the daemon's documented "lenient HealthCheck with non-empty payload".
// wire_protocol.md lines 316-323 claim a HealthCheck with a non-empty,
// CRC-valid payload is still answered with a HealthCheckResponse. Verify it.
func TestProbe_HealthCheckWithPayloadIsLenient(t *testing.T) {
	c := dialOrSkip(t)
	defer c.Close()
	frame := EncodeFrame(Frame{
		Version:     WireVersion,
		MessageType: MsgHealthCheck,
		Durability:  Sync,
		Flags:       0,
		Payload:     []byte("not-empty"),
	})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := c.PushRaw(frame)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.Frame.MessageType != MsgHealthCheckResponse {
		t.Fatalf("non-empty HealthCheck got %s; doc claims HealthCheckResponse (lenient)", resp.Frame.MessageType)
	}
	t.Logf("OK: non-empty HealthCheck answered with HealthCheckResponse, as documented")
}

// Probe: trailing bytes on a live stream. The one-shot reference decoder
// rejects a frame + extra bytes as TrailingBytes (G18). But the LIVE daemon
// frames the stream itself: a valid frame followed by extra bytes should be
// Acked (frame 1), then the extra bytes are interpreted as the start of frame
// 2. We send one valid Push + 4 garbage bytes and expect an Ack for frame 1.
func TestProbe_TrailingBytesOnLiveStreamAreNextFrame(t *testing.T) {
	c := dialOrSkip(t)
	defer c.Close()
	frame := EncodePush([]byte("hi"), Sync)
	frame = append(frame, 0xde, 0xad, 0xbe, 0xef) // 4 trailing bytes
	if _, err := c.conn.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := c.readResponse()
	if err != nil {
		t.Fatalf("read frame-1 response: %v", err)
	}
	if !resp.IsAck {
		t.Fatalf("expected Ack for frame 1, got %s", resp.Frame.MessageType)
	}
	t.Logf("OK: live daemon frames the stream; frame 1 Acked, trailing bytes start frame 2 (no TrailingBytes error on the wire)")
	// The 4 trailing bytes (0xdeadbeef) are now interpreted as the start of a
	// new frame's magic -> BadMagic on a follow-up. Drain it to confirm.
	_ = c.SetReadDeadline(time.Now().Add(1 * time.Second))
	r2, err2 := c.readResponse()
	if err2 == nil && r2.IsNack {
		t.Logf("follow-up: trailing bytes parsed as new frame -> Nack:%s", r2.NackReason)
	} else {
		t.Logf("follow-up read: err=%v (daemon may be waiting for more bytes to complete the second 'frame')", err2)
	}
}

// Probe: does the daemon's response durability filler match the doc claim that
// it is "always 0x01 (Sync)" (worked example) vs the conformance vector
// ack_nonsync_durability_filler which shows a Buffered filler is also valid?
// We push at Buffered and inspect the Ack's durability byte.
func TestProbe_AckDurabilityFiller(t *testing.T) {
	c := dialOrSkip(t)
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := c.PushRaw(EncodePush([]byte("z"), Buffered))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !resp.IsAck {
		t.Fatalf("expected Ack, got %s", resp.Frame.MessageType)
	}
	t.Logf("Ack durability filler byte = %s (0x%02x) for a Buffered push",
		resp.Frame.Durability, byte(resp.Frame.Durability))
	// wire_protocol.md line 272-275 says the response durability is ALWAYS
	// 0x01 (Sync). If the daemon echoes Buffered instead, that's a doc/impl gap.
	if resp.Frame.Durability != Sync {
		t.Errorf("DOC GAP: worked-example says response durability is always Sync(0x01); got %s", resp.Frame.Durability)
	}
}

// Probe: NackInternalError is documented as transient (connection kept open).
// We cannot easily force the daemon into InternalError from a well-formed
// client, so we only assert our client correctly classifies the *reason byte's*
// Permanent() mapping (unit-level), and log that we could not provoke it live.
func TestProbe_InternalErrorIsTransientMapping(t *testing.T) {
	if NackInternalError.Permanent() {
		t.Fatalf("client maps InternalError as permanent; doc says transient (keep conn open)")
	}
	t.Logf("OK: client maps InternalError as transient; could not provoke a live InternalError from a well-formed client (would need queue saturation / sink failure)")
}

// Probe: confirm the daemon validates the FULL header before dispatching on
// message_type. A HealthCheck (0x04) carrying an out-of-range durability (0x00)
// must be rejected with UnknownMessage and the conn closed
// (wire_protocol.md lines 305-311), NOT answered with a HealthCheckResponse.
func TestProbe_HealthCheckBadDurabilityRejected(t *testing.T) {
	c := dialOrSkip(t)
	defer c.Close()
	frame := EncodeFrame(Frame{
		Version:     WireVersion,
		MessageType: MsgHealthCheck,
		Durability:  Durability(0x00), // out of range
		Flags:       0,
		Payload:     nil,
	})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := c.PushRaw(frame)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !resp.IsNack || resp.NackReason != NackUnknownMessage {
		t.Fatalf("HealthCheck w/ durability 0x00: want Nack:UnknownMessage, got %s/%s",
			resp.Frame.MessageType, resp.NackReason)
	}
	t.Logf("OK: HealthCheck with bad durability byte rejected as UnknownMessage, as documented")
}

// helper to suppress unused import if probes are all skipped
var _ = binary.LittleEndian

// Probe: the cap is a config knob (--max-payload-bytes) AND a hard cap.
// The conformance vector tests cap+1 against the HARD cap. But a live daemon
// may run a LOWER --max-payload-bytes. A polyglot client that hardcodes
// 16 MiB (from the spec) will send frames the daemon rejects at a smaller
// boundary. We confirm the running daemon (default cap) accepts a payload at
// a reasonable size and document the discoverability gap (no wire-level way to
// learn the daemon's *effective* cap; only the hard cap is in the vectors).
func TestProbe_EffectiveCapNotDiscoverableOnWire(t *testing.T) {
	c := dialOrSkip(t)
	defer c.Close()
	// 1 MiB payload — well under default 16 MiB, should Ack.
	big := make([]byte, 1<<20)
	for i := range big {
		big[i] = byte(i)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := c.PushRaw(EncodePush(big, Buffered))
	if err != nil {
		t.Fatalf("1MiB push: %v", err)
	}
	if !resp.IsAck {
		t.Fatalf("1MiB push got %s/%s; expected Ack under default cap", resp.Frame.MessageType, resp.NackReason)
	}
	t.Logf("OK: 1MiB push Acked. NOTE: there is no wire-level query for the daemon's EFFECTIVE --max-payload-bytes; a client only knows the 16 MiB HARD cap from the spec. A daemon run with a lower cap will Nack:PayloadTooLarge at a boundary the client cannot discover except by trial.")
}

// Probe: against a daemon run with --max-payload-bytes 1024, a 2KiB payload
// (well under the 16 MiB HARD cap a spec-following client would enforce) is
// rejected with PayloadTooLarge. Demonstrates the effective-cap gap concretely.
// Requires WEIR_SOCKET_SMALLCAP -> daemon with --max-payload-bytes 1024.
func TestProbe_SmallCapDaemonRejectsUnderHardCap(t *testing.T) {
	s := os.Getenv("WEIR_SOCKET_SMALLCAP")
	if s == "" {
		t.Skip("WEIR_SOCKET_SMALLCAP not set")
	}
	c, err := Dial(s)
	if err != nil {
		t.Skipf("dial small-cap daemon: %v", err)
	}
	defer c.Close()
	payload := make([]byte, 2048) // 2 KiB: under 16 MiB hard cap, over 1 KiB effective cap
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := c.PushRaw(EncodePush(payload, Buffered))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !resp.IsNack || resp.NackReason != NackPayloadTooLarge {
		t.Fatalf("2KiB push to 1KiB-cap daemon: want Nack:PayloadTooLarge, got %s/%s",
			resp.Frame.MessageType, resp.NackReason)
	}
	t.Logf("CONFIRMED effective-cap gap: a 2KiB payload (legal under the 16 MiB HARD cap the spec checklist tells clients to enforce) is Nack:PayloadTooLarge against a daemon run with --max-payload-bytes=1024. The client had no wire-level way to learn this boundary in advance.")
}
