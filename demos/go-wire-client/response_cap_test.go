package main

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// A peer's declared payload_len must not choose this client's allocation.
//
// This client used to bound a RESPONSE against MaxPayloadHardCap (16 MiB) --
// the cap on a record being SENT -- so a desynced or hostile daemon could
// declare 16 MiB in a 16-byte header and make the client allocate it before a
// single body byte arrived. The published checklist
// (docs/wire_protocol.md, "Response sizes") says a client must cap the
// declared length by the message type; every response this client can receive
// carries at most two bytes.
//
// The test asserts the rejection AND the absence of the allocation, because
// returning an error after having already allocated 16 MiB would still be the
// bug. Against the pre-fix code the allocation assertion is what fails first:
// the read blocks on a body that never comes and the deadline fires.
func TestResponsePayloadLenCannotChooseOurAllocation(t *testing.T) {
	const declared = 8 * 1024 * 1024 // 8 MiB, well under the 16 MiB send cap

	// Not t.TempDir(): on macOS its path exceeds the ~104-byte sun_path limit
	// and bind(2) fails with EINVAL. A short /tmp dir keeps the test portable.
	dir, err := os.MkdirTemp("/tmp", "wgocap")
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	// A well-formed Nack header declaring `declared` bytes, and no body ever.
	hdr := make([]byte, HeaderLen)
	copy(hdr, magic[:])
	hdr[4] = WireVersion
	hdr[5] = byte(MsgNack)
	hdr[6] = byte(Durable)
	binary.LittleEndian.PutUint32(hdr[8:12], declared)
	binary.LittleEndian.PutUint32(hdr[12:16], crc32.ChecksumIEEE(hdr[0:12]))

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1024)
		_, _ = conn.Read(buf)
		_, _ = conn.Write(hdr)
		// Hold the connection open so a client that trusts the length has
		// something to block on rather than an EOF that masks the bug.
		time.Sleep(3 * time.Second)
	}()

	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	start := time.Now()
	_, err = c.PushRaw(EncodePush([]byte("x"), Durable))
	elapsed := time.Since(start)
	runtime.ReadMemStats(&m1)

	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("want ErrPayloadTooLarge for a %d-byte declared response, got %v", declared, err)
	}
	if allocated := m1.TotalAlloc - m0.TotalAlloc; allocated > 64*1024 {
		t.Fatalf("rejected the response but allocated %d bytes doing it; the cap "+
			"must be checked before the body read, not after", allocated)
	}
	if elapsed > time.Second {
		t.Fatalf("took %v to reject; it should not have waited for a body it "+
			"was never going to accept", elapsed)
	}
}
