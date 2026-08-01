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
use weir_core::{Durability, NackReason};

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

/// Records buffered per thread before a flush is forced.
///
/// Named because the verifier derives its frontier slack from
/// `threads * LEDGER_FLUSH_THRESHOLD` — the bound on how far a still-buffering
/// thread can lag the delivery log.
const LEDGER_FLUSH_THRESHOLD: usize = 256;

/// Longest a ledger entry may sit unflushed.
///
/// Without a time bound the ledger only flushes at the threshold above, while
/// the recorder fsyncs each delivery ~ms after its ack — so the delivery log
/// runs thousands of records ahead and every one looks like a delivery with no
/// provenance. Spec §3.1 asked for this; it was never implemented.
const LEDGER_FLUSH_INTERVAL: Duration = Duration::from_millis(200);

/// Set by the SIGTERM/SIGINT handler. Producer threads break out of their loop
/// and run their existing final `flush` instead of dying mid-buffer.
///
/// WHY THIS EXISTS, and which way the error runs — the direction matters and is
/// easy to get backwards. With no handler installed, SIGTERM's default
/// disposition kills this process instantly and up to
/// `threads * LEDGER_FLUSH_THRESHOLD` (8 × 256 = 2048) buffered ledger entries
/// are discarded, because the final `flush` never runs. The first real run
/// showed it plainly: `loadgen.log` was **0 bytes**, the process having died
/// before it could print anything at all — including its own
/// `FATAL — could not durably record N ledger entries` message.
///
/// - Losing **ledger** entries is UNDER-CHECKING. A record absent from the
///   ledger is simply not held to I1: the oracle never learns weir acked it, so
///   it cannot accuse weir of losing it. Safe, but blind.
/// - Losing **delivery** entries would be CONCEALING, which is worse: an acked
///   record would look undelivered and the oracle would fabricate an I1
///   violation against a weir that did nothing wrong. That log belongs to the
///   recorder, not to this process, and it fsyncs before its 200.
///
/// So the old behaviour was safe. It is nonetheless what made `frontier_slack=0`
/// unsound in the orchestrator's final verification pass: that pass rests on
/// "the producer is stopped and both logs are complete", and a truncated ledger
/// tail makes the second half of that claim false.
static STOP: AtomicBool = AtomicBool::new(false);

/// Signal numbers, written out rather than pulled from a crate.
///
/// Neither `libc` nor `signal_hook` is a dependency of this crate today, and
/// adding one for two integers and a single `extern "C"` declaration is a poor
/// trade in a harness whose credibility rests on nobody having to take an
/// unreviewed dependency's word for anything. `SIGINT = 2` and `SIGTERM = 15`
/// are fixed by every ABI this can run on (Linux and macOS alike).
#[cfg(unix)]
const SIGINT: i32 = 2;
#[cfg(unix)]
const SIGTERM: i32 = 15;

/// `SIG_ERR` — `signal(2)`'s failure return, `(sighandler_t) -1`.
#[cfg(unix)]
const SIG_ERR: usize = usize::MAX;

#[cfg(unix)]
unsafe extern "C" {
    /// `signal(2)`, declared directly. See [`SIGTERM`] for why not via a crate.
    ///
    /// glibc, musl and macOS all give this BSD semantics — the handler stays
    /// installed after the first delivery — which is more than is needed here,
    /// since one delivery is enough to latch the flag.
    fn signal(signum: i32, handler: usize) -> usize;
}

/// Latches [`STOP`]. The only thing it does: a signal handler may call only
/// async-signal-safe code, and a relaxed store to a lock-free atomic is about
/// the whole of what qualifies.
#[cfg(unix)]
extern "C" fn note_stop_signal(_signum: i32) {
    STOP.store(true, Ordering::Relaxed);
}

/// Installs the stop handler for SIGTERM and SIGINT.
#[cfg(unix)]
fn install_stop_handlers() {
    for sig in [SIGINT, SIGTERM] {
        // SAFETY: `note_stop_signal` is an `extern "C" fn(i32)` — exactly the
        // shape `signal(2)` expects — and it touches nothing but one atomic.
        // Cast via `*const ()`: a direct `fn as usize` trips
        // `function_casts_as_integer`.
        let prev = unsafe { signal(sig, note_stop_signal as *const () as usize) };
        if prev == SIG_ERR {
            // Fail LOUD. A handler that silently failed to install reverts to
            // the old behaviour — killed mid-buffer, ledger tail gone — and the
            // orchestrator's final pass would then trust a ledger it must not.
            eprintln!(
                "loadgen: WARNING — could not install a handler for signal {sig}. A \
                 stop signal will kill this process outright and discard its buffered \
                 ledger entries; the run's final verification pass must not be trusted."
            );
        }
    }
}

/// No-op off Unix. The harness is Linux-only (see `orchestrator/run.py`); this
/// exists so the crate still compiles under a non-Unix `cargo check`.
#[cfg(not(unix))]
fn install_stop_handlers() {}

/// Whether a producer thread should push another record.
///
/// Pulled out of the loop header so the stop-flag path is testable without
/// spawning the pool and signalling it: a thread that ignores [`STOP`] is
/// precisely the bug this fixes, and it would otherwise be observable only in a
/// live privileged run.
fn keep_producing(fatal: &AtomicBool, deadline: Instant) -> bool {
    !fatal.load(Ordering::Relaxed) && !STOP.load(Ordering::Relaxed) && Instant::now() < deadline
}

/// Maps a client error to a ledger outcome.
///
/// The distinction that matters: a `Nack` is weir *explicitly refusing* the
/// record, and invariant I2 holds it to that — a nacked record must never
/// appear downstream. Everything else means the response never arrived, so the
/// record's fate is genuinely indeterminate and the verifier constrains
/// nothing about it.
fn classify(err: &ClientError) -> Outcome {
    match err {
        // INDETERMINATE, not a refusal. weir emits InternalError when a flusher
        // returned a non-durable outcome or its ack sender was dropped, so the
        // record may already be in the segment and recovery may legitimately
        // replay it. Holding it to I2 ("a nacked record is never delivered")
        // would be stricter than weir's own contract and would manufacture a P0
        // finding out of correct behaviour.
        ClientError::Nack(NackReason::InternalError) => Outcome::Unknown,
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

    // BEFORE any thread starts, so there is no window in which a stop signal
    // still has its default (fatal) disposition while records are in flight.
    install_stop_handlers();

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
            let mut pending: Vec<LedgerEntry> = Vec::with_capacity(LEDGER_FLUSH_THRESHOLD);
            let mut last_flush = Instant::now();

            while keep_producing(&fatal, deadline) {
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

                let due = pending.len() >= LEDGER_FLUSH_THRESHOLD
                    || last_flush.elapsed() >= LEDGER_FLUSH_INTERVAL;
                if due {
                    if !flush(&ledger, &mut pending, &fatal) {
                        break;
                    }
                    last_flush = Instant::now();
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
    // Exit 0 either way: a signalled stop is a CLEAN stop now, because every
    // thread ran its final `flush` on the way out. The orchestrator keys its
    // "the ledger is complete, `frontier_slack=0` is sound" decision off this
    // exit status, so exiting non-zero here would needlessly downgrade the one
    // verification pass that can afford zero slack.
    if STOP.load(Ordering::Relaxed) {
        eprintln!("loadgen: stopped by signal after {pushed} records pushed; ledger flushed");
    } else {
        eprintln!("loadgen: finished, {pushed} records pushed");
    }
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

    /// Serialises the tests that mutate the process-global [`STOP`]. Without it
    /// they race each other under cargo's parallel test threads and the failure
    /// looks like a flake rather than the shared-state bug it is.
    static STOP_TEST_LOCK: Mutex<()> = Mutex::new(());

    /// Takes the lock and leaves [`STOP`] cleared, whatever the previous test
    /// did with it (including panicking part-way through).
    fn stop_flag_guard() -> std::sync::MutexGuard<'static, ()> {
        let guard = STOP_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        STOP.store(false, Ordering::Relaxed);
        guard
    }

    #[test]
    fn the_stop_flag_ends_the_producer_loop() {
        let _guard = stop_flag_guard();
        let fatal = AtomicBool::new(false);
        let deadline = Instant::now() + Duration::from_secs(3600);

        assert!(
            keep_producing(&fatal, deadline),
            "a healthy thread inside its deadline must keep producing"
        );
        STOP.store(true, Ordering::Relaxed);
        assert!(
            !keep_producing(&fatal, deadline),
            "the stop flag must break the loop so the final flush runs — without \
             this the process dies mid-buffer and up to threads*256 ledger \
             entries go with it"
        );
        STOP.store(false, Ordering::Relaxed);
    }

    #[test]
    fn a_past_deadline_and_a_fatal_ledger_error_each_end_the_loop() {
        let _guard = stop_flag_guard();
        let fatal = AtomicBool::new(false);
        assert!(
            !keep_producing(&fatal, Instant::now() - Duration::from_secs(1)),
            "--duration-secs must still bound the run"
        );
        let fatal = AtomicBool::new(true);
        assert!(
            !keep_producing(&fatal, Instant::now() + Duration::from_secs(3600)),
            "a ledger write failure must still wind the generator down"
        );
    }

    /// Sends SIGTERM to this very process. If the handler is NOT installed the
    /// default disposition applies and the test binary dies outright — which is
    /// exactly the bug, and cargo reports it as loudly as a failed assert.
    #[cfg(unix)]
    #[test]
    fn a_term_signal_latches_the_flag_instead_of_killing_the_process() {
        unsafe extern "C" {
            fn raise(sig: i32) -> i32;
        }

        let _guard = stop_flag_guard();
        install_stop_handlers();
        assert!(!STOP.load(Ordering::Relaxed));

        // SAFETY: `raise` sends the signal to the calling thread; the handler
        // installed above is process-wide and touches nothing but one atomic.
        assert_eq!(unsafe { raise(SIGTERM) }, 0, "raise(SIGTERM) failed");

        assert!(
            STOP.load(Ordering::Relaxed),
            "SIGTERM must set the stop flag, not terminate the producer — the \
             whole point is that the final ledger flush gets to run"
        );
        STOP.store(false, Ordering::Relaxed);
    }

    #[test]
    fn classifies_client_errors_into_ledger_outcomes() {
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
    fn nack_internal_error_is_indeterminate_not_a_refusal() {
        // weir emits InternalError when a flusher returned a non-durable
        // outcome or its ack sender was dropped — the bytes may already be in
        // the segment, so recovery may legitimately replay and deliver them.
        // Holding this to I2 ("a nacked record is never delivered") would be
        // stricter than weir's own contract and would manufacture a P0 finding
        // out of correct behaviour.
        assert_eq!(
            classify(&ClientError::Nack(NackReason::InternalError)),
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
