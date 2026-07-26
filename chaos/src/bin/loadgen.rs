//! The load generator — sustained producer pool and the outcome ledger.
//!
//! Runs unprivileged and OUTSIDE the fault zone. The suite kills the daemon,
//! never this process, so an in-memory ledger correctly models what the
//! producer *was told* — which is exactly what the durability claim is about.
//!
//! The ledger is flushed to disk continuously so a multi-day run survives an
//! operator mistake or an OOM.

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use weir_chaos::{LedgerEntry, Outcome, encode_record};
use weir_client::{ClientError, WeirClient};
use weir_core::Durability;

/// Read/write timeout on the producer's socket.
///
/// Without it `--duration-secs` is not a deadline at all: the loop checks the
/// clock only between iterations, so a daemon that accepts a connection and
/// then stops responding without closing it blocks `push` forever. That is
/// worse here than a plain hang — Task 8 asserts this process is *alive* before
/// each fault, and a wedged generator is alive while producing nothing, so load
/// would silently stop without tripping the guard. Generous relative to a
/// Sync-tier ack (hundreds of microseconds) even under injected slow-disk
/// faults.
const PUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// Maps a client error to a ledger outcome.
///
/// The distinction that matters: a `Nack` is weir *explicitly refusing* the
/// record, and invariant I2 holds it to that — a nacked record must never
/// appear downstream. Everything else means the response never arrived, so the
/// record's fate is genuinely indeterminate and the verifier constrains
/// nothing about it.
fn classify(err: &ClientError) -> Outcome {
    match err {
        ClientError::Nack(reason) => Outcome::Nacked(format!("{reason:?}")),
        ClientError::VersionMismatch { daemon_version } => {
            Outcome::Nacked(format!("VersionMismatch({daemon_version})"))
        }
        ClientError::UnknownNack(b) => Outcome::Nacked(format!("UnknownNack({b:#04x})")),
        // Local pre-send rejections are harness bugs, not daemon behaviour, but
        // they are still explicit refusals: no bytes reached the daemon.
        ClientError::PayloadTooLarge { .. } => Outcome::Nacked("LocalPayloadTooLarge".into()),
        ClientError::EmptyPayload => Outcome::Nacked("LocalEmptyPayload".into()),
        ClientError::NoDefaultDurability => Outcome::Nacked("NoDefaultDurability".into()),
        // Io / Protocol / any future variant: no answer arrived.
        _ => Outcome::Unknown,
    }
}

fn tier_from_char(c: char) -> Option<Durability> {
    match c {
        'S' => Some(Durability::Sync),
        'B' => Some(Durability::Batched),
        'U' => Some(Durability::Buffered),
        _ => None,
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

struct Args {
    socket: String,
    ledger: String,
    run_id: u64,
    threads: usize,
    record_size: usize,
    tier: char,
    duration_secs: u64,
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().collect();
    let get = |name: &str, default: &str| -> String {
        argv.iter()
            .position(|a| a == name)
            .and_then(|i| argv.get(i + 1))
            .cloned()
            .unwrap_or_else(|| default.to_string())
    };
    Args {
        socket: get("--socket", "/tmp/weir/run/weir.sock"),
        ledger: get("--ledger", "./ledger.log"),
        run_id: get("--run-id", "1").parse().expect("--run-id must be u64"),
        threads: get("--threads", "4")
            .parse()
            .expect("--threads must be usize"),
        record_size: get("--record-size", "256").parse().expect("--record-size"),
        tier: get("--tier", "S").chars().next().unwrap_or('S'),
        duration_secs: get("--duration-secs", "60")
            .parse()
            .expect("--duration-secs"),
    }
}

fn main() {
    let args = parse_args();
    let durability = tier_from_char(args.tier).expect("--tier must be S, B, or U");

    let ledger = Arc::new(Mutex::new(
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&args.ledger)
            .expect("open ledger"),
    ));
    let seq = Arc::new(AtomicU64::new(0));
    // Set when any thread cannot durably record an outcome. Every thread
    // watches it, so one ledger failure winds the whole generator down rather
    // than leaving the run producing records nobody is keeping track of.
    let fatal = Arc::new(AtomicBool::new(false));

    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let mut handles = Vec::new();

    for _ in 0..args.threads {
        let ledger = Arc::clone(&ledger);
        let seq = Arc::clone(&seq);
        let fatal = Arc::clone(&fatal);
        let socket = args.socket.clone();
        let run_id = args.run_id;
        let record_size = args.record_size;
        let tier = args.tier;

        handles.push(std::thread::spawn(move || {
            let mut client: Option<WeirClient> = None;
            let mut pending: Vec<LedgerEntry> = Vec::with_capacity(256);

            while !fatal.load(Ordering::Relaxed) && Instant::now() < deadline {
                // Reconnect as needed. The daemon is killed repeatedly by
                // design, so a dead connection is expected, not exceptional.
                if client.is_none() {
                    match WeirClient::connect(&socket) {
                        Ok(c) => {
                            // Bound every push. See PUSH_TIMEOUT.
                            let _ = c.set_read_timeout(Some(PUSH_TIMEOUT));
                            let _ = c.set_write_timeout(Some(PUSH_TIMEOUT));
                            client = Some(c);
                        }
                        Err(_) => {
                            std::thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                    }
                }

                let my_seq = seq.fetch_add(1, Ordering::Relaxed);
                let payload = encode_record(run_id, my_seq, record_size);
                let t0 = Instant::now();
                let t_micros = now_micros();

                let c = client.as_mut().expect("just ensured connected");
                let outcome = match c.push(&payload, durability) {
                    Ok(()) => Outcome::Acked,
                    Err(e) => {
                        let o = classify(&e);
                        if !e.is_recoverable() {
                            client = None;
                        }
                        o
                    }
                };

                pending.push(LedgerEntry {
                    seq: my_seq,
                    tier,
                    outcome,
                    t_micros,
                    rtt_micros: t0.elapsed().as_micros() as u64,
                });

                if pending.len() >= 256 && !flush(&ledger, &mut pending, &fatal) {
                    break;
                }
            }
            flush(&ledger, &mut pending, &fatal);
        }));
    }

    // A panicking thread must not be silent. It would drop its buffered ledger
    // entries — possibly including genuinely acked records — WITHOUT setting
    // `fatal`, so the run would report success while the oracle had quietly
    // lost evidence. That is the one failure mode that conceals a real
    // durability violation rather than inventing one.
    let mut panicked = 0usize;
    for h in handles {
        if h.join().is_err() {
            panicked += 1;
        }
    }

    let pushed = seq.load(Ordering::Relaxed);
    if fatal.load(Ordering::Relaxed) {
        eprintln!("loadgen: ABORTED after {pushed} records — ledger write failed");
        std::process::exit(1);
    }
    if panicked > 0 {
        eprintln!(
            "loadgen: ABORTED after {pushed} records — {panicked} producer thread(s) \
             panicked; their unflushed ledger entries are lost, so this run's \
             verification cannot be trusted"
        );
        std::process::exit(1);
    }
    eprintln!("loadgen: finished, {pushed} records pushed");
}

/// Serialises entries, one line each. Split from the I/O so the formatting and
/// the failure path are independently testable.
fn serialise(entries: &[LedgerEntry]) -> String {
    let mut buf = String::with_capacity(entries.len() * 48);
    for e in entries {
        buf.push_str(&e.to_line());
        buf.push('\n');
    }
    buf
}

/// Serialises entries to a writer. Generic over `Write` so the error path is
/// testable with a deliberately-failing writer.
///
/// `flush` no longer calls this directly (it goes through `LedgerSink`), so
/// this exists purely as test-facing coverage of the write-error path; it is
/// `#[cfg(test)]`-only to avoid tripping `dead_code` in the plain bin build.
#[cfg(test)]
fn write_entries<W: Write>(w: &mut W, entries: &[LedgerEntry]) -> std::io::Result<()> {
    w.write_all(serialise(entries).as_bytes())
}

/// The ledger's durable-append surface.
///
/// A trait rather than a concrete `File` so that [`flush`] — the function that
/// decides whether the whole run is still trustworthy — is testable without a
/// real filesystem. Leaving it untestable would mean the harness's own
/// fail-loud path is the one piece of it with no coverage.
trait LedgerSink {
    fn append(&mut self, bytes: &[u8]) -> std::io::Result<()>;
    fn sync(&mut self) -> std::io::Result<()>;
}

impl LedgerSink for std::fs::File {
    fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.write_all(bytes)
    }
    fn sync(&mut self) -> std::io::Result<()> {
        self.sync_data()
    }
}

/// Durably appends `pending` to the ledger. Returns false if the write failed,
/// having set `fatal`.
///
/// A ledger write failure is not recoverable and must not be swallowed: the
/// ledger is the oracle, so losing an entry either invents a durability
/// violation or hides a real one. `pending` is cleared only on success, so a
/// transient caller could retry without losing entries.
fn flush<S: LedgerSink>(
    ledger: &Arc<Mutex<S>>,
    pending: &mut Vec<LedgerEntry>,
    fatal: &Arc<AtomicBool>,
) -> bool {
    if pending.is_empty() {
        return true;
    }
    let mut sink = ledger.lock().expect("ledger mutex");
    let bytes = serialise(pending);
    let result = sink.append(bytes.as_bytes()).and_then(|()| sink.sync());
    match result {
        Ok(()) => {
            pending.clear();
            true
        }
        Err(e) => {
            eprintln!(
                "loadgen: FATAL — could not durably record {} ledger entries: {e}",
                pending.len()
            );
            fatal.store(true, Ordering::Relaxed);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_client_errors_into_ledger_outcomes() {
        use weir_client::ClientError;
        use weir_core::NackReason;

        // A Nack is an explicit refusal — weir said no, and I2 will hold it to that.
        assert!(matches!(
            classify(&ClientError::Nack(NackReason::PayloadTooLarge)),
            weir_chaos::Outcome::Nacked(_)
        ));

        // An I/O error means the response never arrived: legitimately indeterminate.
        let io = ClientError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "x"));
        assert_eq!(classify(&io), weir_chaos::Outcome::Unknown);

        // A protocol violation is equally indeterminate from the producer's view.
        assert_eq!(
            classify(&ClientError::Protocol("bad".into())),
            weir_chaos::Outcome::Unknown
        );
    }

    #[test]
    fn tier_chars_map_to_durability() {
        use weir_core::Durability;
        assert_eq!(tier_from_char('S'), Some(Durability::Sync));
        assert_eq!(tier_from_char('B'), Some(Durability::Batched));
        assert_eq!(tier_from_char('U'), Some(Durability::Buffered));
        assert_eq!(tier_from_char('x'), None);
    }

    /// A writer that always fails, to prove ledger write errors propagate.
    struct FailingWriter;

    impl std::io::Write for FailingWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk gone"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn write_entries_propagates_io_errors_instead_of_swallowing_them() {
        // The ledger IS the oracle. A silently-dropped ledger write loses the
        // record of what weir acked, which either invents a violation or hides
        // one. It must be loud.
        let entries = vec![weir_chaos::LedgerEntry {
            seq: 1,
            tier: 'S',
            outcome: weir_chaos::Outcome::Acked,
            t_micros: 1,
            rtt_micros: 2,
        }];
        let err = write_entries(&mut FailingWriter, &entries).expect_err("must propagate");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
    }

    #[test]
    fn write_entries_emits_one_newline_terminated_line_per_entry() {
        let entries = vec![
            weir_chaos::LedgerEntry {
                seq: 1,
                tier: 'S',
                outcome: weir_chaos::Outcome::Acked,
                t_micros: 10,
                rtt_micros: 20,
            },
            weir_chaos::LedgerEntry {
                seq: 2,
                tier: 'U',
                outcome: weir_chaos::Outcome::Unknown,
                t_micros: 11,
                rtt_micros: 21,
            },
        ];
        let mut buf: Vec<u8> = Vec::new();
        write_entries(&mut buf, &entries).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert_eq!(text, "1 S 10 20 ACK\n2 U 11 21 UNK\n");
    }

    /// A ledger sink that can be made to fail on append, on sync, or neither.
    #[derive(Default)]
    struct FlakySink {
        fail_append: bool,
        fail_sync: bool,
        written: Vec<u8>,
        synced: usize,
    }

    impl LedgerSink for FlakySink {
        fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
            if self.fail_append {
                return Err(std::io::Error::other("append failed"));
            }
            self.written.extend_from_slice(bytes);
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            if self.fail_sync {
                return Err(std::io::Error::other("sync failed"));
            }
            self.synced += 1;
            Ok(())
        }
    }

    fn sample_entry(seq: u64) -> weir_chaos::LedgerEntry {
        weir_chaos::LedgerEntry {
            seq,
            tier: 'S',
            outcome: weir_chaos::Outcome::Acked,
            t_micros: 1,
            rtt_micros: 2,
        }
    }

    #[test]
    fn flush_clears_pending_and_syncs_on_success() {
        let sink = Arc::new(Mutex::new(FlakySink::default()));
        let fatal = Arc::new(AtomicBool::new(false));
        let mut pending = vec![sample_entry(1), sample_entry(2)];
        assert!(flush(&sink, &mut pending, &fatal));
        assert!(pending.is_empty(), "pending must be cleared on success");
        assert!(!fatal.load(Ordering::Relaxed));
        let s = sink.lock().unwrap();
        assert_eq!(s.synced, 1, "durability needs a sync, not just a write");
        assert_eq!(
            String::from_utf8(s.written.clone()).unwrap(),
            "1 S 1 2 ACK\n2 S 1 2 ACK\n"
        );
    }

    #[test]
    fn flush_sets_fatal_and_retains_pending_when_append_fails() {
        let sink = Arc::new(Mutex::new(FlakySink {
            fail_append: true,
            ..Default::default()
        }));
        let fatal = Arc::new(AtomicBool::new(false));
        let mut pending = vec![sample_entry(1)];
        assert!(!flush(&sink, &mut pending, &fatal));
        assert!(
            fatal.load(Ordering::Relaxed),
            "a ledger entry that cannot be recorded must be fatal, never swallowed"
        );
        assert_eq!(
            pending.len(),
            1,
            "pending must survive a failed flush rather than vanish"
        );
    }

    #[test]
    fn flush_sets_fatal_when_only_the_sync_fails() {
        // Written but not durable is still a lost entry: this harness kills the
        // machine's processes by design, and an unsynced ledger tail goes with
        // them.
        let sink = Arc::new(Mutex::new(FlakySink {
            fail_sync: true,
            ..Default::default()
        }));
        let fatal = Arc::new(AtomicBool::new(false));
        let mut pending = vec![sample_entry(1)];
        assert!(!flush(&sink, &mut pending, &fatal));
        assert!(fatal.load(Ordering::Relaxed));
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn flush_on_empty_pending_is_a_no_op() {
        let sink = Arc::new(Mutex::new(FlakySink {
            fail_append: true,
            ..Default::default()
        }));
        let fatal = Arc::new(AtomicBool::new(false));
        let mut pending: Vec<weir_chaos::LedgerEntry> = Vec::new();
        assert!(
            flush(&sink, &mut pending, &fatal),
            "nothing to write is not a failure"
        );
        assert!(!fatal.load(Ordering::Relaxed));
    }

    #[test]
    fn classify_maps_every_refusal_shape_to_nacked() {
        // The Nacked/Unknown split is load-bearing: invariant I2 later asserts a
        // nacked record never appears downstream, so a refusal misfiled as
        // Unknown silently weakens that check.
        use weir_client::ClientError;
        let refusals = [
            ClientError::VersionMismatch { daemon_version: 2 },
            ClientError::UnknownNack(0x0a),
            ClientError::PayloadTooLarge { len: 1, limit: 0 },
            ClientError::EmptyPayload,
            ClientError::NoDefaultDurability,
        ];
        for e in refusals {
            assert!(
                matches!(classify(&e), weir_chaos::Outcome::Nacked(_)),
                "{e:?} is an explicit refusal and must classify as Nacked"
            );
        }
    }
}
