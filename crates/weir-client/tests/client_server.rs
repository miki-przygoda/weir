//! Integration tests: WeirClient against a minimal in-process mock server.
//!
//! Each test spins up a `std::thread` that listens on a temporary Unix socket,
//! parses one or more frames, and replies with Ack / Nack /
//! HealthCheckResponse. The client then connects with `WeirClient` and the
//! test asserts the outcome.
//!
//! The mock server does not run the full weir-server pipeline — it only
//! understands the wire protocol, which is sufficient to test WeirClient in
//! isolation.

#[cfg(unix)]
mod tests {
    use std::{
        io::{Read, Write},
        os::unix::net::{UnixListener, UnixStream},
        path::PathBuf,
        thread,
        time::Duration,
    };

    use weir_client::{ClientError, WeirClient};
    use weir_core::{Durability, HEADER_LEN, Header, MessageType, NackReason};

    // ── Mock server helpers ───────────────────────────────────────────────────

    /// Returns a unique socket path in the system temp directory.
    fn tmp_socket(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("weir_test_{}_{}.sock", tag, std::process::id()))
    }

    /// Removes the socket file when dropped, even if the test panics.
    struct SocketGuard(PathBuf);
    impl Drop for SocketGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// Reads one complete frame (header + payload + payload CRC) from `stream`.
    /// Returns `(message_type, payload)`. Returns `None` on clean EOF.
    fn read_frame(stream: &mut UnixStream) -> Option<(MessageType, Vec<u8>)> {
        let mut header_buf = [0u8; HEADER_LEN];
        match stream.read_exact(&mut header_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(e) => panic!("mock server: read header: {e}"),
        }

        let header = Header::decode(&header_buf).expect("mock server: decode header");
        let payload_len = header.payload_len as usize;

        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            stream
                .read_exact(&mut payload)
                .expect("mock server: read payload");
        }

        let mut crc_buf = [0u8; 4];
        stream
            .read_exact(&mut crc_buf)
            .expect("mock server: read CRC");

        Some((header.message_type, payload))
    }

    /// Writes an Ack frame (empty payload) to `stream`.
    fn write_ack(stream: &mut UnixStream) {
        let header = Header::new(MessageType::Ack, Durability::Sync, 0, 0).encode();
        let crc = crc32fast::hash(&[]).to_le_bytes();
        stream.write_all(&header).unwrap();
        stream.write_all(&crc).unwrap();
    }

    /// Writes a Nack frame with `reason` to `stream`.
    fn write_nack(stream: &mut UnixStream, reason: NackReason) {
        let payload = [reason as u8];
        let header =
            Header::new(MessageType::Nack, Durability::Sync, 0, payload.len() as u32).encode();
        let crc = crc32fast::hash(&payload).to_le_bytes();
        stream.write_all(&header).unwrap();
        stream.write_all(&payload).unwrap();
        stream.write_all(&crc).unwrap();
    }

    /// Writes a HealthCheckResponse frame to `stream`.
    fn write_health_check_response(stream: &mut UnixStream) {
        let header = Header::new(MessageType::HealthCheckResponse, Durability::Sync, 0, 0).encode();
        let crc = crc32fast::hash(&[]).to_le_bytes();
        stream.write_all(&header).unwrap();
        stream.write_all(&crc).unwrap();
    }

    /// Spawns a mock server thread that accepts exactly one connection and
    /// calls `handler` with the accepted stream. Returns the bound socket path
    /// and a guard that deletes the socket file on drop.
    fn spawn_mock_server<F>(tag: &str, handler: F) -> (PathBuf, SocketGuard)
    where
        F: FnOnce(UnixStream) + Send + 'static,
    {
        let path = tmp_socket(tag);
        let guard = SocketGuard(path.clone());
        let listener = UnixListener::bind(&path).expect("bind mock socket");

        thread::spawn(move || {
            let (stream, _) = listener.accept().expect("mock server: accept");
            handler(stream);
        });

        // Small sleep so the listener is ready before the client connects.
        thread::sleep(Duration::from_millis(5));

        (path, guard)
    }

    // ── Push tests ────────────────────────────────────────────────────────────

    #[test]
    fn push_returns_ok_when_server_acks() {
        let (path, _guard) = spawn_mock_server("push_ack", |mut stream| {
            if let Some((MessageType::Push, _)) = read_frame(&mut stream) {
                write_ack(&mut stream);
            }
        });

        let mut client = WeirClient::connect(&path).unwrap();
        assert!(client.push(b"hello", Durability::Sync).is_ok());
    }

    #[test]
    fn push_returns_nack_error_when_server_nacks() {
        let (path, _guard) = spawn_mock_server("push_nack", |mut stream| {
            if let Some((MessageType::Push, _)) = read_frame(&mut stream) {
                write_nack(&mut stream, NackReason::InternalError);
            }
        });

        let mut client = WeirClient::connect(&path).unwrap();
        let err = client.push(b"hello", Durability::Sync).unwrap_err();
        assert!(
            matches!(err, ClientError::Nack(NackReason::InternalError)),
            "expected Nack(InternalError), got {err:?}"
        );
    }

    #[test]
    fn push_payload_too_large_nack() {
        let (path, _guard) = spawn_mock_server("push_toolarge", |mut stream| {
            if let Some((MessageType::Push, _)) = read_frame(&mut stream) {
                write_nack(&mut stream, NackReason::PayloadTooLarge);
            }
        });

        let mut client = WeirClient::connect(&path).unwrap();
        let err = client
            .push(b"oversized-in-mock", Durability::Buffered)
            .unwrap_err();
        assert!(matches!(
            err,
            ClientError::Nack(NackReason::PayloadTooLarge)
        ));
    }

    #[test]
    fn all_durability_tiers_accepted() {
        for durability in [Durability::Sync, Durability::Batched, Durability::Buffered] {
            let tag = format!("durability_{durability:?}");
            let (path, _guard) = spawn_mock_server(&tag, |mut stream| {
                while let Some((MessageType::Push, _)) = read_frame(&mut stream) {
                    write_ack(&mut stream);
                }
            });

            let mut client = WeirClient::connect(&path).unwrap();
            assert!(
                client.push(b"payload", durability).is_ok(),
                "push with {durability:?} should succeed"
            );
        }
    }

    #[test]
    fn multiple_sequential_pushes_on_one_connection() {
        let (path, _guard) = spawn_mock_server("multi_push", |mut stream| {
            while let Some((MessageType::Push, _)) = read_frame(&mut stream) {
                write_ack(&mut stream);
            }
        });

        let mut client = WeirClient::connect(&path).unwrap();
        for i in 0..10u32 {
            let payload = format!("record-{i:04}");
            assert!(
                client.push(payload.as_bytes(), Durability::Batched).is_ok(),
                "push #{i} should succeed"
            );
        }
    }

    #[test]
    fn push_carries_correct_payload_to_server() {
        let expected = b"exact-payload-content".to_vec();
        let expected_clone = expected.clone();

        let (path, _guard) = spawn_mock_server("payload_check", move |mut stream| {
            if let Some((MessageType::Push, payload)) = read_frame(&mut stream) {
                assert_eq!(payload, expected_clone, "server received wrong payload");
                write_ack(&mut stream);
            }
        });

        let mut client = WeirClient::connect(&path).unwrap();
        client.push(&expected, Durability::Sync).unwrap();
    }

    // ── Health check tests ────────────────────────────────────────────────────

    #[test]
    fn health_check_returns_ok_when_server_responds() {
        let (path, _guard) = spawn_mock_server("health_ok", |mut stream| {
            if let Some((MessageType::HealthCheck, _)) = read_frame(&mut stream) {
                write_health_check_response(&mut stream);
            }
        });

        let mut client = WeirClient::connect(&path).unwrap();
        assert!(client.health_check().is_ok());
    }

    #[test]
    fn health_check_returns_nack_error_when_server_nacks() {
        let (path, _guard) = spawn_mock_server("health_nack", |mut stream| {
            if let Some((MessageType::HealthCheck, _)) = read_frame(&mut stream) {
                write_nack(&mut stream, NackReason::InternalError);
            }
        });

        let mut client = WeirClient::connect(&path).unwrap();
        let err = client.health_check().unwrap_err();
        assert!(matches!(err, ClientError::Nack(NackReason::InternalError)));
    }

    // ── Mixed request sequence ────────────────────────────────────────────────

    #[test]
    fn push_then_health_check_on_same_connection() {
        let (path, _guard) = spawn_mock_server("mixed", |mut stream| {
            // First frame: Push → Ack
            if let Some((MessageType::Push, _)) = read_frame(&mut stream) {
                write_ack(&mut stream);
            }
            // Second frame: HealthCheck → HealthCheckResponse
            if let Some((MessageType::HealthCheck, _)) = read_frame(&mut stream) {
                write_health_check_response(&mut stream);
            }
        });

        let mut client = WeirClient::connect(&path).unwrap();
        client.push(b"first record", Durability::Buffered).unwrap();
        client.health_check().unwrap();
    }

    // ── Error path: server closes connection ──────────────────────────────────

    #[test]
    fn push_returns_io_error_when_server_closes_without_reply() {
        let (path, _guard) = spawn_mock_server("server_close", |mut stream| {
            // Read the frame but close the stream without replying.
            read_frame(&mut stream);
            drop(stream);
        });

        let mut client = WeirClient::connect(&path).unwrap();
        let err = client.push(b"hello", Durability::Sync).unwrap_err();
        assert!(
            matches!(err, ClientError::Io(_)),
            "expected Io error on unexpected close, got {err:?}"
        );
    }
}
