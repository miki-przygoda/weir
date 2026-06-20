package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// This is an adversarial live harness: it crafts frames that should trigger
// each Nack reason and edge case, sends them to a live daemon, and checks the
// observed Nack reason + connection-close behavior against wire_protocol.md.

var socketPath = flag.String("socket", "weir.sock", "weir daemon Unix socket")

type caseResult struct {
	name      string
	want      string // expected outcome description
	got       string // observed
	ok        bool
	closeWant bool // does spec say the daemon closes the conn after this?
	closeGot  bool // did we observe a close (next read = EOF)?
}

func main() {
	flag.Parse()
	var results []caseResult

	add := func(r caseResult) {
		results = append(results, r)
		status := "PASS"
		if !r.ok {
			status = "FAIL"
		}
		fmt.Printf("[%s] %-28s want=%-22s got=%-22s close want=%v got=%v\n",
			status, r.name, r.want, r.got, r.closeWant, r.closeGot)
	}

	// --- Happy path: a valid Push at each durability tier ---
	for _, d := range []Durability{Sync, Batched, Buffered} {
		add(runOnce("push_"+d.String(), EncodePush([]byte("hello-"+d.String()), d),
			"Ack", false))
	}

	// --- HealthCheck ---
	add(runHealthCheck())

	// --- EmptyPayload (0x07): zero-length Push payload, closes conn ---
	add(runOnce("empty_payload", EncodePush(nil, Sync), "Nack:EmptyPayload", true))

	// --- ReservedFlagsSet (0x09): flags byte nonzero, closes conn ---
	flagsFrame := EncodeFrame(Frame{Version: WireVersion, MessageType: MsgPush, Durability: Sync, Flags: 0x01, Payload: []byte("x")})
	add(runOnce("reserved_flags", flagsFrame, "Nack:ReservedFlagsSet", true))

	// --- BadMagic (0x01): corrupt first 4 bytes, closes conn ---
	bad := EncodePush([]byte("data"), Sync)
	bad[0] = 'X'
	bad[1] = 'X' // recompute header CRC so we isolate magic, not CRC
	bad[2] = 'X'
	bad[3] = 'X'
	binary.LittleEndian.PutUint32(bad[12:16], crc(bad[0:12]))
	add(runOnce("bad_magic", bad, "Nack:BadMagic", true))

	// --- VersionMismatch (0x02): version byte != 1, carries daemon version ---
	ver := EncodePush([]byte("data"), Sync)
	ver[4] = 0x02
	binary.LittleEndian.PutUint32(ver[12:16], crc(ver[0:12]))
	add(runVersionMismatch("version_mismatch", ver))

	// --- BadHeaderCrc (0x03): corrupt the header CRC field, closes conn ---
	hc := EncodePush([]byte("data"), Sync)
	binary.LittleEndian.PutUint32(hc[12:16], crc(hc[0:12])^0xFFFFFFFF)
	add(runOnce("bad_header_crc", hc, "Nack:BadHeaderCrc", true))

	// --- BadPayloadCrc (0x05): corrupt trailing payload CRC, closes conn ---
	pc := EncodePush([]byte("data"), Sync)
	pc[len(pc)-1] ^= 0xFF
	add(runOnce("bad_payload_crc", pc, "Nack:BadPayloadCrc", true))

	// --- UnknownMessage (0x08) via unknown message_type, closes conn ---
	umt := EncodeFrame(Frame{Version: WireVersion, MessageType: MessageType(0x7F), Durability: Sync, Flags: 0, Payload: []byte("x")})
	add(runOnce("unknown_message_type", umt, "Nack:UnknownMessage", true))

	// --- UnknownMessage (0x08) via unknown durability, closes conn ---
	udur := EncodeFrame(Frame{Version: WireVersion, MessageType: MsgPush, Durability: Durability(0xFF), Flags: 0, Payload: []byte("x")})
	add(runOnce("unknown_durability", udur, "Nack:UnknownMessage", true))

	// --- UnknownMessage (0x08): client SENDS a daemon->client type (Ack) ---
	sendAck := EncodeFrame(Frame{Version: WireVersion, MessageType: MsgAck, Durability: Sync, Flags: 0, Payload: nil})
	add(runOnce("client_sends_ack", sendAck, "Nack:UnknownMessage", true))

	// --- PayloadTooLarge (0x04): header declares len > hard cap, closes conn.
	// Send ONLY a header (no payload) declaring an over-cap length: the daemon
	// must reject before reading payload bytes (decode order step 5). ---
	add(runPayloadTooLarge())

	// --- Pipelining: two valid Pushes back-to-back, two Acks in order ---
	add(runPipeline())

	// --- Summary ---
	fmt.Println()
	passed, failed := 0, 0
	for _, r := range results {
		if r.ok {
			passed++
		} else {
			failed++
		}
	}
	fmt.Printf("SUMMARY: %d passed, %d failed, %d total\n", passed, failed, len(results))
	if failed > 0 {
		os.Exit(1)
	}
}

// nextReadIsClose reports whether the daemon closed the connection: after a
// permanent-error Nack the daemon closes, so a follow-up read returns EOF.
func nextReadIsClose(c *Client) bool {
	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	one := make([]byte, 1)
	_, err := c.conn.Read(one)
	return errors.Is(err, io.EOF)
}

func runOnce(name string, frame []byte, want string, closeWant bool) caseResult {
	r := caseResult{name: name, want: want, closeWant: closeWant}
	c, err := Dial(*socketPath)
	if err != nil {
		r.got = "dial-err:" + err.Error()
		return r
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := c.PushRaw(frame)
	if err != nil {
		r.got = "io-err:" + err.Error()
		return r
	}
	switch {
	case resp.IsAck:
		r.got = "Ack"
	case resp.IsNack:
		r.got = "Nack:" + resp.NackReason.String()
	default:
		r.got = resp.Frame.MessageType.String()
	}
	r.closeGot = nextReadIsClose(c)
	r.ok = (r.got == want) && (r.closeGot == r.closeWant)
	return r
}

func runHealthCheck() caseResult {
	r := caseResult{name: "healthcheck", want: "HealthCheckResponse", closeWant: false}
	c, err := Dial(*socketPath)
	if err != nil {
		r.got = "dial-err:" + err.Error()
		return r
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := c.PushRaw(EncodeHealthCheck())
	if err != nil {
		r.got = "io-err:" + err.Error()
		return r
	}
	r.got = resp.Frame.MessageType.String()
	r.closeGot = nextReadIsClose(c)
	r.ok = (r.got == "HealthCheckResponse") && !r.closeGot
	return r
}

func runVersionMismatch(name string, frame []byte) caseResult {
	r := caseResult{name: name, want: "Nack:VersionMismatch", closeWant: true}
	c, err := Dial(*socketPath)
	if err != nil {
		r.got = "dial-err:" + err.Error()
		return r
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := c.PushRaw(frame)
	if err != nil {
		r.got = "io-err:" + err.Error()
		return r
	}
	if resp.IsNack && resp.NackReason == NackVersionMismatch {
		if resp.HasDaemonVersion {
			r.got = fmt.Sprintf("Nack:VersionMismatch(daemon=v%d)", resp.DaemonWireVersion)
			r.want = fmt.Sprintf("Nack:VersionMismatch(daemon=v%d)", WireVersion)
		} else {
			r.got = "Nack:VersionMismatch(NO daemon version byte!)"
		}
	} else if resp.IsNack {
		r.got = "Nack:" + resp.NackReason.String()
	} else {
		r.got = resp.Frame.MessageType.String()
	}
	r.closeGot = nextReadIsClose(c)
	r.ok = resp.IsNack && resp.NackReason == NackVersionMismatch &&
		resp.HasDaemonVersion && resp.DaemonWireVersion == WireVersion && r.closeGot
	return r
}

// runPayloadTooLarge sends only a header declaring an over-cap payload_len and
// expects Nack(PayloadTooLarge) + close, WITHOUT sending the (huge) payload.
func runPayloadTooLarge() caseResult {
	r := caseResult{name: "payload_too_large", want: "Nack:PayloadTooLarge", closeWant: true}
	c, err := Dial(*socketPath)
	if err != nil {
		r.got = "dial-err:" + err.Error()
		return r
	}
	defer c.Close()
	hdr := make([]byte, HeaderLen)
	copy(hdr[0:4], magic[:])
	hdr[4] = WireVersion
	hdr[5] = byte(MsgPush)
	hdr[6] = byte(Sync)
	hdr[7] = 0
	binary.LittleEndian.PutUint32(hdr[8:12], MaxPayloadHardCap+1)
	binary.LittleEndian.PutUint32(hdr[12:16], crc(hdr[0:12]))
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := c.PushRaw(hdr)
	if err != nil {
		r.got = "io-err:" + err.Error()
		return r
	}
	if resp.IsNack {
		r.got = "Nack:" + resp.NackReason.String()
	} else {
		r.got = resp.Frame.MessageType.String()
	}
	r.closeGot = nextReadIsClose(c)
	r.ok = resp.IsNack && resp.NackReason == NackPayloadTooLarge && r.closeGot
	return r
}

// runPipeline writes two valid Pushes back-to-back, then reads two Acks in
// order, exercising the "pipelining at the kernel level" guarantee.
func runPipeline() caseResult {
	r := caseResult{name: "pipeline_two_push", want: "Ack,Ack", closeWant: false}
	conn, err := net.DialTimeout("unix", *socketPath, 5*time.Second)
	if err != nil {
		r.got = "dial-err:" + err.Error()
		return r
	}
	defer conn.Close()
	c := &Client{conn: conn}
	var w bytes.Buffer
	w.Write(EncodePush([]byte("p1"), Sync))
	w.Write(EncodePush([]byte("p2"), Sync))
	if _, err := conn.Write(w.Bytes()); err != nil {
		r.got = "write-err:" + err.Error()
		return r
	}
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	r1, err := c.readResponse()
	if err != nil {
		r.got = "read1-err:" + err.Error()
		return r
	}
	r2, err := c.readResponse()
	if err != nil {
		r.got = "read2-err:" + err.Error()
		return r
	}
	got := ""
	if r1.IsAck {
		got += "Ack"
	} else {
		got += r1.Frame.MessageType.String()
	}
	got += ","
	if r2.IsAck {
		got += "Ack"
	} else {
		got += r2.Frame.MessageType.String()
	}
	r.got = got
	r.ok = r1.IsAck && r2.IsAck
	return r
}
