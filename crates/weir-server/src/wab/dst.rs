//! Deterministic Simulation Testing (DST) harness for the WAB flusher.
//!
//! Drives the **real** flusher code paths ([`super::flusher_thread`],
//! [`super::flush_batch`], [`super::run_with_panic_supervision`]) against the
//! injectable seams from this subsystem — [`BlockingClock`] (virtual time) and
//! [`SegmentStore`] (a fault-injecting segment backend) — so durability faults
//! are reproducible from a single `u64` seed:
//!
//! ```ignore
//! Sim::new(seed)
//!     .fault(Fault::FsyncReturns { nth: 1 })
//!     .scenario(Scenario::SyncFlush { records: 4 })
//!     .run();
//! ```
//!
//! [`Fault`]/[`Scenario`]/[`SimSpec`] are `serde` enums, so a failing seed
//! serialises straight into `tests/dst_seeds/*.json` as a pinned regression.
//! Invariants are checked in-harness ([`assert_invariant`]); a violation panics
//! with the invariant name, the seed, and a one-line `WEIR_DST_SEED=…` repro.
//!
//! The whole module compiles only under `cargo test` or `--features dst`
//! ([`super`] gates it), so release binaries carry zero sim code. The
//! module-level `dead_code` allow covers the non-test `--features dst` build,
//! where the harness has no consumer yet (a future sweep binary would be one).

#![allow(dead_code)]

use std::{
    cell::RefCell,
    collections::HashSet,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    },
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::clock::BlockingClock;
use super::segment::{FsSegmentStore, SegmentHandle, SegmentStore, ShardWriter, WabSegment};
use crate::metrics::Metrics;
use crate::models::{Batch, WorkUnit};
use weir_core::{Durability, Payload};

// ── Deterministic RNG ───────────────────────────────────────────────────────

/// splitmix64 — a tiny, dependency-free, fully deterministic PRNG. The seed
/// drives every random choice in a run (record payloads today; scheduler order
/// once the cooperative executor lands), so a seed reproduces a run exactly.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A seed-derived payload whose first 8 bytes are `index` (big-endian), so
    /// every record in a run is unique — the durability [`Ledger`] keys on the
    /// payload bytes, and the recovery checks compare payloads directly.
    fn unique_payload(&mut self, index: u64) -> Vec<u8> {
        let mut payload = index.to_be_bytes().to_vec();
        let tail = (self.next_u64() % 56) as usize;
        payload.extend((0..tail).map(|_| (self.next_u64() & 0xFF) as u8));
        payload
    }
}

// ── Durability ledger ───────────────────────────────────────────────────────

/// Records which payloads have actually reached stable storage. A
/// [`SimSegmentHandle`] accumulates the payloads written to it and moves them
/// here the instant a real `fsync` (or seal) succeeds; a segment dropped before
/// its fsync (a mid-batch write error) never marks its records durable. The
/// oracle then checks the core durability invariant: **no record is acked
/// `true` unless it is in this set.**
#[derive(Clone, Default)]
struct Ledger {
    durable: Arc<Mutex<HashSet<Vec<u8>>>>,
}

impl Ledger {
    fn mark_durable(&self, payloads: impl IntoIterator<Item = Vec<u8>>) {
        self.durable.lock().unwrap().extend(payloads);
    }

    fn is_durable(&self, payload: &[u8]) -> bool {
        self.durable.lock().unwrap().contains(payload)
    }
}

// ── Virtual clock ───────────────────────────────────────────────────────────

/// A [`BlockingClock`] that never blocks the real thread: `sleep` advances a
/// virtual-nanosecond counter instantly, collapsing the panic-supervisor's
/// ~550 ms backoff to microseconds. `recv_timeout` still performs a real
/// bounded wait (the Phase-1 flusher runs on a real OS thread — there is no
/// cooperative scheduler yet) but advances virtual time by the timeout so the
/// accounting stays consistent.
#[derive(Clone, Default)]
pub struct SimClock {
    virtual_nanos: Arc<AtomicU64>,
}

impl SimClock {
    pub fn new() -> Self {
        SimClock::default()
    }

    /// Total virtual time the clock has advanced (sum of every `sleep` +
    /// `recv_timeout` timeout).
    pub fn virtual_elapsed(&self) -> Duration {
        Duration::from_nanos(self.virtual_nanos.load(SeqCst))
    }
}

impl BlockingClock for SimClock {
    fn recv_timeout<T>(&self, rx: &Receiver<T>, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.virtual_nanos
            .fetch_add(timeout.as_nanos() as u64, SeqCst);
        rx.recv_timeout(timeout)
    }

    fn sleep(&self, dur: Duration) {
        self.virtual_nanos.fetch_add(dur.as_nanos() as u64, SeqCst);
    }
}

// ── Faults ──────────────────────────────────────────────────────────────────

/// A single injectable fault. `serde` so a failing seed's fault list pins into
/// `tests/dst_seeds/*.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    /// The `nth` `fdatasync` on the shard (1-based) returns `EIO` instead of
    /// syncing — models a disk that fails the durability barrier.
    FsyncReturns { nth: u64 },
    /// The `nth` `write_record` on the shard (1-based) returns a short-write
    /// error — models a torn write (the writev that only partially lands on
    /// ENOSPC). `ShardWriter` drops the segment; the prior records written to it
    /// are now in an un-fsynced, dropped segment.
    ShortWriteOn { nth: u64 },
    /// Every `seal()` fails outright (its sentinel/footer write or fsync hits
    /// ENOSPC) — the segment is left un-sealed at its `.wab` path. Records
    /// already covered by an earlier group fsync stay durable; recovery must
    /// still recover them from the un-sealed file.
    SealFails,
    /// The next `seal()` finalises (sentinel + footer + fsync) but its rename
    /// never lands — models a crash between `sync_all` and `rename`.
    RenameFails,
}

/// Shared, interior-mutable fault state. One instance is held by the
/// [`SimSegmentStore`] and every [`SimSegmentHandle`] it creates (and by the
/// harness, to read back what fired), so fault counters span the whole run.
struct SimFaults {
    /// 1-based index of the fsync that must fail (`None` = never).
    fsync_fail_on: Option<u64>,
    /// Count of fsyncs seen so far (across all handles on the shard).
    fsync_calls: AtomicU64,
    /// Set once an injected fsync actually fired — read by invariant checks.
    fsync_failed: AtomicBool,
    /// 1-based index of the `write_record` that must tear (`None` = never).
    write_fail_on: Option<u64>,
    /// Count of `write_record` calls seen so far (across all handles).
    write_calls: AtomicU64,
    /// Every seal fails outright (ENOSPC at seal).
    seal_fails: AtomicBool,
    /// The next seal's rename fails.
    rename_fails: AtomicBool,
}

impl SimFaults {
    fn from_faults(faults: &[Fault]) -> Arc<Self> {
        let mut fsync_fail_on = None;
        let mut write_fail_on = None;
        let mut seal_fails = false;
        let mut rename_fails = false;
        for fault in faults {
            match fault {
                Fault::FsyncReturns { nth } => fsync_fail_on = Some(*nth),
                Fault::ShortWriteOn { nth } => write_fail_on = Some(*nth),
                Fault::SealFails => seal_fails = true,
                Fault::RenameFails => rename_fails = true,
            }
        }
        Arc::new(SimFaults {
            fsync_fail_on,
            fsync_calls: AtomicU64::new(0),
            fsync_failed: AtomicBool::new(false),
            write_fail_on,
            write_calls: AtomicU64::new(0),
            seal_fails: AtomicBool::new(seal_fails),
            rename_fails: AtomicBool::new(rename_fails),
        })
    }

    fn any_fsync_failed(&self) -> bool {
        self.fsync_failed.load(SeqCst)
    }
}

// ── Fault-injecting segment store ───────────────────────────────────────────

/// A [`SegmentStore`] that writes real files to a real temp directory but
/// injects [`Fault`]s at the fsync / seal boundary. Recovery and the segment
/// reader run unmodified against the resulting on-disk state.
pub struct SimSegmentStore {
    faults: Arc<SimFaults>,
    ledger: Ledger,
}

impl SimSegmentStore {
    fn new(faults: Arc<SimFaults>, ledger: Ledger) -> Self {
        SimSegmentStore { faults, ledger }
    }
}

impl SegmentStore for SimSegmentStore {
    fn create(&self, path: &Path, shard_id: u16) -> io::Result<Box<dyn SegmentHandle>> {
        let inner = WabSegment::create(path, shard_id)?;
        Ok(Box::new(SimSegmentHandle {
            inner,
            faults: Arc::clone(&self.faults),
            ledger: self.ledger.clone(),
            pending: RefCell::new(Vec::new()),
        }))
    }

    fn segment_counters(&self, dir: &Path) -> io::Result<Vec<u64>> {
        // The sim writes real files, so the production scan is exact here.
        FsSegmentStore.segment_counters(dir)
    }
}

struct SimSegmentHandle {
    inner: WabSegment,
    faults: Arc<SimFaults>,
    ledger: Ledger,
    /// Payloads written to this segment but not yet fsynced. On a successful
    /// fsync/seal they move to the [`Ledger`]; if the segment is dropped first
    /// (a mid-batch write error drops it from `ShardWriter`), they never do —
    /// which is exactly the data that a crash would lose.
    pending: RefCell<Vec<Vec<u8>>>,
}

impl SegmentHandle for SimSegmentHandle {
    fn write_record(&mut self, payload: &[u8]) -> io::Result<()> {
        let nth = self.faults.write_calls.fetch_add(1, SeqCst) + 1;
        if self.faults.write_fail_on == Some(nth) {
            // Torn write: the underlying writev returned short (ENOSPC). The real
            // WabSegment poisons and `ShardWriter` drops the segment. We model
            // the failed write without touching `inner` — ShardWriter drops the
            // segment on *any* write error, and the prior records' durability
            // fate is exactly what we're testing.
            return Err(io::Error::other("DST: injected torn (short) write"));
        }
        self.inner.write_record(payload)?;
        self.pending.borrow_mut().push(payload.to_vec());
        Ok(())
    }

    fn fsync(&self) -> io::Result<()> {
        let nth = self.faults.fsync_calls.fetch_add(1, SeqCst) + 1;
        if self.faults.fsync_fail_on == Some(nth) {
            self.faults.fsync_failed.store(true, SeqCst);
            return Err(io::Error::other("DST: injected EIO on fdatasync"));
        }
        self.inner.fsync()?;
        // The barrier succeeded — everything written so far is now durable.
        self.ledger
            .mark_durable(self.pending.borrow_mut().drain(..));
        Ok(())
    }

    fn should_rotate(&self, max_bytes: u64) -> bool {
        self.inner.should_rotate(max_bytes)
    }

    fn seal(self: Box<Self>) -> io::Result<PathBuf> {
        let SimSegmentHandle {
            inner,
            faults,
            ledger,
            pending,
        } = *self;
        if faults.seal_fails.load(SeqCst) {
            // ENOSPC at seal: the sentinel/footer write or fsync fails, leaving
            // the segment un-sealed at its `.wab` path. We do NOT mark `pending`
            // durable — the seal's own fsync didn't complete. Records covered by
            // an earlier group fsync are already in the ledger from that fsync;
            // records that were never group-fsynced (e.g. a failed rotation
            // seal) correctly stay non-durable and get Nacked by the caller.
            return Err(io::Error::other("DST: injected ENOSPC at seal"));
        }
        if faults.rename_fails.load(SeqCst) {
            // Durably finalise (sentinel + footer + fsync) but never rename:
            // the segment is left fully formed at its `.wab` path, exactly as a
            // crash between sync_all and rename would leave it. Recovery
            // re-seals it via the sentinel branch in `recover_segment`. The data
            // IS synced, so its records are durable despite the failed rename.
            inner.finalize_to_disk()?;
            ledger.mark_durable(pending.into_inner());
            return Err(io::Error::other(
                "DST: injected crash between fsync and rename",
            ));
        }
        let sealed = inner.seal()?;
        ledger.mark_durable(pending.into_inner());
        Ok(sealed)
    }
}

// ── Scenarios + spec ────────────────────────────────────────────────────────

/// What the harness drives. `serde` for seed pinning.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Scenario {
    /// Flush `records` Sync work units through the real flusher under the
    /// configured faults; collect each record's ack outcome.
    SyncFlush { records: usize },
    /// Write `records`, seal (rename injected to fail = crash), then run
    /// recovery; collect the recovered payloads.
    CrashBeforeRename { records: usize },
    /// Flush `records` (group-fsynced + acked), then fail the shutdown seal and
    /// run recovery; assert every acked record is still recoverable.
    SealFailsAtShutdown { records: usize },
    /// Drive the panic supervisor under [`SimClock`]; assert it caps out in
    /// virtual (instantly-advanced) time.
    PanicSupervisor,
}

/// A fully specified, reproducible run. Serialises to a `tests/dst_seeds/*.json`
/// regression entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SimSpec {
    pub seed: u64,
    pub scenario: Scenario,
    #[serde(default)]
    pub faults: Vec<Fault>,
}

impl SimSpec {
    /// Execute the run. Checks all relevant invariants in-harness (panicking
    /// with a seed repro on violation) and returns the observed outcome.
    pub fn run(&self) -> SimReport {
        match &self.scenario {
            Scenario::SyncFlush { records } => run_sync_flush(self.seed, &self.faults, *records),
            Scenario::CrashBeforeRename { records } => {
                run_crash_before_rename(self.seed, &self.faults, *records)
            }
            Scenario::SealFailsAtShutdown { records } => {
                run_seal_fails_at_shutdown(self.seed, &self.faults, *records)
            }
            Scenario::PanicSupervisor => run_panic_supervisor(self.seed),
        }
    }
}

/// Builder surface — see the module docs.
pub struct Sim {
    seed: u64,
    faults: Vec<Fault>,
    scenario: Option<Scenario>,
}

impl Sim {
    pub fn new(seed: u64) -> Self {
        Sim {
            seed,
            faults: Vec::new(),
            scenario: None,
        }
    }

    pub fn fault(mut self, fault: Fault) -> Self {
        self.faults.push(fault);
        self
    }

    pub fn scenario(mut self, scenario: Scenario) -> Self {
        self.scenario = Some(scenario);
        self
    }

    pub fn run(self) -> SimReport {
        let scenario = self.scenario.expect("Sim::run requires a scenario");
        SimSpec {
            seed: self.seed,
            scenario,
            faults: self.faults,
        }
        .run()
    }
}

/// Everything a test might assert about a completed run. Fields are populated
/// per scenario; the rest stay at their `Default`.
#[derive(Default)]
pub struct SimReport {
    pub seed: u64,
    /// `SyncFlush`: per-record ack outcomes (`true` = durable ack).
    pub acks: Vec<bool>,
    /// `SyncFlush`: whether an injected fsync actually fired.
    pub fsync_failed: bool,
    /// `CrashBeforeRename`: payloads written before the crash.
    pub written: Vec<Vec<u8>>,
    /// `CrashBeforeRename`: payloads recovered afterwards.
    pub recovered: Vec<Vec<u8>>,
    /// `PanicSupervisor`: total panics recorded.
    pub flusher_panics: u64,
    /// `PanicSupervisor`: virtual time the sim clock advanced (the collapsed
    /// backoff).
    pub virtual_elapsed: Duration,
    /// `PanicSupervisor`: real wall-clock spent (should be ~0, proving collapse).
    pub real_elapsed: Duration,
}

// ── Invariant oracle ────────────────────────────────────────────────────────

/// Panics with the invariant name, the seed, and a one-line repro when `holds`
/// is false. This is the DST oracle: every scenario funnels its durability
/// checks through here so a failure always carries a reproducer.
fn assert_invariant(seed: u64, name: &str, holds: bool) {
    assert!(
        holds,
        "DST invariant `{name}` VIOLATED — seed {seed:#018x}\n      \
         reproduce: WEIR_DST_SEED={seed:#018x} cargo test -p weir-server --features dst dst::"
    );
}

// ── Scenario drivers ────────────────────────────────────────────────────────

/// A throwaway WAB root with one pre-created shard dir + metrics.
struct SimEnv {
    wab_dir: PathBuf,
    metrics: Arc<Metrics>,
}

static SIM_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

impl SimEnv {
    fn new(label: &str) -> Self {
        let n = SIM_DIR_COUNTER.fetch_add(1, SeqCst);
        let wab_dir =
            std::env::temp_dir().join(format!("weir_dst_{label}_{}_{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&wab_dir);
        super::create_dir_private(wab_dir.clone()).expect("create wab_dir");
        super::create_dir_private(wab_dir.join("shard_00")).expect("create shard_00");
        SimEnv {
            wab_dir,
            metrics: Arc::new(Metrics::new().0),
        }
    }

    fn shard_dir(&self, shard: usize) -> PathBuf {
        self.wab_dir.join(format!("shard_{shard:02}"))
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(&self.wab_dir);
    }
}

fn make_unit(payload: Vec<u8>, durability: Durability, ack_tx: oneshot::Sender<bool>) -> WorkUnit {
    WorkUnit {
        shard_id: 0,
        payload: Payload::from(payload),
        durability,
        ack_tx,
        #[cfg(feature = "bench-trace")]
        enqueued_at: Instant::now(),
    }
}

fn make_batch(records: Vec<WorkUnit>) -> Batch {
    Batch {
        shard_id: 0,
        records,
        #[cfg(feature = "bench-trace")]
        flushed_at: Instant::now(),
    }
}

/// Reads every `.wab.sealed` segment in `shard_dir` (counter order) and returns
/// the concatenated record payloads.
fn read_sealed_payloads(shard_dir: &Path) -> Vec<Vec<u8>> {
    let mut sealed: Vec<PathBuf> = std::fs::read_dir(shard_dir)
        .expect("read shard dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".wab.sealed"))
        .collect();
    sealed.sort();

    let mut out = Vec::new();
    for path in sealed {
        let reader = super::SegmentReader::open(&path).expect("open sealed segment");
        for record in reader {
            out.push(record.expect("read record").to_vec());
        }
    }
    out
}

/// The observable result of driving the real flusher over a batch of Sync
/// records: what was sent, how each was acked, the durability ledger, and the
/// (not-yet-cleaned-up) environment so a caller can run recovery against it.
struct FlushOutcome {
    payloads: Vec<Vec<u8>>,
    acks: Vec<bool>,
    ledger: Ledger,
    fsync_failed: bool,
    env: SimEnv,
}

/// Sends `records` unique Sync units in one batch through the **real**
/// [`super::flusher_thread`] under `faults`, joins the flusher, and collects the
/// per-record acks. Performs no invariant checks and leaves `env` un-cleaned so
/// the caller can recover from the on-disk state.
fn drive_sync_flusher(seed: u64, faults: &[Fault], records: usize) -> FlushOutcome {
    let env = SimEnv::new("sync_flush");
    let sim_faults = SimFaults::from_faults(faults);
    let ledger = Ledger::default();
    let store: Arc<dyn SegmentStore> = Arc::new(SimSegmentStore::new(
        Arc::clone(&sim_faults),
        ledger.clone(),
    ));
    let clock = SimClock::new();

    let (work_tx, work_rx) = crossbeam_channel::bounded::<Batch>(records.max(1) * 4);
    let (drain_tx, _drain_rx) = crossbeam_channel::unbounded::<PathBuf>();
    let coalesce_hint = Arc::new(AtomicU64::new(200));

    let mut rng = SplitMix64::new(seed);
    let mut payloads = Vec::with_capacity(records);
    let mut ack_rxs = Vec::with_capacity(records);
    let mut units = Vec::with_capacity(records);
    for i in 0..records {
        let payload = rng.unique_payload(i as u64);
        let (ack_tx, ack_rx) = oneshot::channel();
        ack_rxs.push(ack_rx);
        units.push(make_unit(payload.clone(), Durability::Sync, ack_tx));
        payloads.push(payload);
    }
    work_tx.send(make_batch(units)).expect("send batch");
    drop(work_tx); // flusher drains the one batch then exits

    let shard_dir = env.shard_dir(0);
    let metrics = Arc::clone(&env.metrics);
    let handle = std::thread::spawn(move || {
        super::flusher_thread(
            0,
            shard_dir,
            work_rx,
            drain_tx,
            records.max(1),
            Duration::from_millis(5),
            64 * 1024 * 1024,
            None,
            metrics,
            coalesce_hint,
            &clock,
            store,
        );
    });
    handle.join().expect("flusher thread join");

    let acks: Vec<bool> = ack_rxs
        .into_iter()
        .map(|mut rx| {
            rx.try_recv().unwrap_or_else(|e| {
                panic!("DST: Sync record left without an ack (seed {seed:#018x}): {e}")
            })
        })
        .collect();

    FlushOutcome {
        payloads,
        acks,
        ledger,
        fsync_failed: sim_faults.any_fsync_failed(),
        env,
    }
}

/// I1 — durability causality: a record acked `true` must actually be on stable
/// storage (in the ledger). Subsumes the simple "EIO ⇒ all Nacked" check and
/// catches mid-batch false-ack windows.
fn assert_acked_records_durable(seed: u64, out: &FlushOutcome) {
    for (payload, &acked) in out.payloads.iter().zip(&out.acks) {
        if acked {
            assert_invariant(
                seed,
                "i1_acked_true_is_durable",
                out.ledger.is_durable(payload),
            );
        }
    }
}

/// Scenario 1 — `EIO` on `fdatasync` (G-WAB-1). Sends `records` Sync units
/// through the real flusher thread under the fault schedule; asserts that a
/// failed durability barrier produces no `true` ack.
fn run_sync_flush(seed: u64, faults: &[Fault], records: usize) -> SimReport {
    let out = drive_sync_flusher(seed, faults, records);
    assert_acked_records_durable(seed, &out);
    out.env.cleanup();
    SimReport {
        seed,
        acks: out.acks,
        fsync_failed: out.fsync_failed,
        ..Default::default()
    }
}

/// Scenario (Phase 2) — `ENOSPC` at the shutdown seal. The records are written
/// and group-fsynced (acked `true` + durable), then the flusher's shutdown seal
/// fails, leaving an un-sealed `.wab`. Recovery must still recover every acked
/// record: a durable ack survives a failed seal.
fn run_seal_fails_at_shutdown(seed: u64, faults: &[Fault], records: usize) -> SimReport {
    let out = drive_sync_flusher(seed, faults, records);
    // The group fsync succeeded, so the records are durable and acked despite
    // the later seal failure.
    assert_acked_records_durable(seed, &out);

    // The shutdown seal failed → the segment is an un-sealed `.wab`. Recover it.
    super::recovery::recover_open_segments(&out.env.wab_dir, &out.env.metrics).expect("recovery");
    let recovered = read_sealed_payloads(&out.env.shard_dir(0));

    // I3 — durable-ack survivability: every record acked `true` must be
    // recoverable after the failed seal (no acked record is lost).
    for (payload, &acked) in out.payloads.iter().zip(&out.acks) {
        if acked {
            assert_invariant(
                seed,
                "i3_acked_record_recoverable",
                recovered.contains(payload),
            );
        }
    }

    out.env.cleanup();
    SimReport {
        seed,
        acks: out.acks,
        fsync_failed: out.fsync_failed,
        written: out.payloads,
        recovered,
        ..Default::default()
    }
}

/// Scenario 2 — crash between `sync_all` and `rename` in `seal()` (G-WAB-3).
/// Writes `records`, seals (rename injected to fail), then recovers; asserts
/// every synced record comes back intact and in order.
fn run_crash_before_rename(seed: u64, faults: &[Fault], records: usize) -> SimReport {
    let env = SimEnv::new("crash_rename");
    let sim_faults = SimFaults::from_faults(faults);
    let ledger = Ledger::default();
    let store: Arc<dyn SegmentStore> =
        Arc::new(SimSegmentStore::new(Arc::clone(&sim_faults), ledger));
    let shard_dir = env.shard_dir(0);

    let mut writer = ShardWriter::new_with_store(
        0,
        shard_dir.clone(),
        64 * 1024 * 1024,
        Arc::clone(&env.metrics),
        store,
    );

    let mut rng = SplitMix64::new(seed);
    let mut written = Vec::with_capacity(records);
    for i in 0..records {
        let payload = rng.unique_payload(i as u64);
        writer.write_record(&payload).expect("write_record");
        written.push(payload);
    }

    // The crash: seal finalises + fsyncs but the rename never lands.
    let seal_result = writer.seal_current();
    assert_invariant(
        seed,
        "i_seal_rename_fault_fired",
        seal_result.is_err() && sim_faults.rename_fails.load(SeqCst),
    );
    drop(writer);

    super::recovery::recover_open_segments(&env.wab_dir, &env.metrics).expect("recovery");
    let recovered = read_sealed_payloads(&shard_dir);

    // I2 — no lost / torn record: recovery returns exactly what was synced.
    assert_invariant(seed, "i2_no_lost_record_after_crash", recovered == written);

    env.cleanup();
    SimReport {
        seed,
        written,
        recovered,
        ..Default::default()
    }
}

/// Scenario 3 — panic-supervisor backoff under [`SimClock`]. The supervisor's
/// ~550 ms real backoff collapses to virtual time; asserts it still caps out.
fn run_panic_supervisor(seed: u64) -> SimReport {
    let metrics = Arc::new(Metrics::new().0);
    let clock = SimClock::new();

    let real_start = Instant::now();
    super::run_with_panic_supervision(0, Arc::clone(&metrics), &clock, || {
        || panic!("DST: persistent flusher panic")
    });
    let real_elapsed = real_start.elapsed();

    let flusher_panics = metrics.wab_flusher_panics.get();
    // I — the supervisor terminates by capping out (initial attempt + every respawn).
    assert_invariant(
        seed,
        "i_supervisor_caps_out",
        flusher_panics == u64::from(super::MAX_FLUSHER_RESPAWNS + 1),
    );

    SimReport {
        seed,
        flusher_panics,
        virtual_elapsed: clock.virtual_elapsed(),
        real_elapsed,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario 1: an `EIO` on the group fdatasync must Nack every Sync record
    /// in the batch — no producer is told its record is durable when it isn't.
    #[test]
    fn eio_on_fdatasync_nacks_every_sync_record() {
        let report = Sim::new(0x5EED_0001)
            .fault(Fault::FsyncReturns { nth: 1 })
            .scenario(Scenario::SyncFlush { records: 4 })
            .run();
        assert!(report.fsync_failed, "the injected fsync should have fired");
        assert_eq!(report.acks.len(), 4);
        assert!(
            report.acks.iter().all(|&ok| !ok),
            "every Sync record must be Nacked when its fsync fails, got {:?}",
            report.acks
        );
    }

    /// Sanity: with no fault, the same flush acks every record `true`.
    #[test]
    fn sync_flush_without_fault_acks_all() {
        let report = Sim::new(0x5EED_0002)
            .scenario(Scenario::SyncFlush { records: 4 })
            .run();
        assert!(!report.fsync_failed);
        assert!(report.acks.iter().all(|&ok| ok), "acks: {:?}", report.acks);
    }

    /// Scenario 2: a crash between sync_all and rename leaves a fully-formed
    /// `.wab`; recovery must replay every synced record, intact and in order.
    #[test]
    fn crash_between_sync_and_rename_recovers_all_records() {
        let report = Sim::new(0x5EED_0003)
            .fault(Fault::RenameFails)
            .scenario(Scenario::CrashBeforeRename { records: 6 })
            .run();
        assert_eq!(report.written.len(), 6);
        assert_eq!(
            report.recovered, report.written,
            "recovery must return exactly the synced records"
        );
    }

    /// Scenario 3: under the sim clock the panic supervisor caps out in virtual
    /// time — same panic count as the real-clock test, but ~0 wall-clock.
    #[test]
    fn panic_supervisor_caps_out_in_virtual_time() {
        let report = Sim::new(0x5EED_0004)
            .scenario(Scenario::PanicSupervisor)
            .run();
        assert_eq!(
            report.flusher_panics,
            u64::from(super::super::MAX_FLUSHER_RESPAWNS + 1)
        );
        // The real-clock test budgets ~550 ms of backoff; the sim collapses it.
        assert!(
            report.real_elapsed < Duration::from_millis(100),
            "sim clock should collapse the backoff, took {:?}",
            report.real_elapsed
        );
        // …while the virtual clock still advanced the full linear backoff.
        assert_eq!(
            report.virtual_elapsed,
            Duration::from_millis(
                (1..=u64::from(super::super::MAX_FLUSHER_RESPAWNS))
                    .map(|a| 10 * a)
                    .sum()
            ),
            "virtual time should equal the summed backoff"
        );
    }

    /// Seed reproducibility: the same seed yields byte-identical payloads, so a
    /// failing seed reproduces exactly. Two runs of the same spec must agree.
    #[test]
    fn same_seed_reproduces_identical_run() {
        let spec = SimSpec {
            seed: 0x1234_5678_9ABC_DEF0,
            scenario: Scenario::CrashBeforeRename { records: 8 },
            faults: vec![Fault::RenameFails],
        };
        let a = spec.run();
        let b = spec.run();
        assert_eq!(a.written, b.written, "same seed must produce same payloads");
        assert_eq!(a.recovered, b.recovered);
        assert_eq!(a.written, a.recovered);
    }

    /// Scenario (Phase 2): a torn write on the 3rd record of a 4-record Sync
    /// batch drops the active segment mid-flush. The two records already written
    /// to that segment were never fsynced, so they MUST be Nacked, not falsely
    /// acked off a later segment's fsync (I1, checked inside `run`). The torn
    /// record is Nacked; the 4th record lands in a fresh segment and is durably
    /// acked. This pins the fix for the mid-batch false-ack window.
    #[test]
    fn torn_write_midbatch_does_not_falsely_ack_prior_records() {
        let report = Sim::new(0x5EED_0005)
            .fault(Fault::ShortWriteOn { nth: 3 })
            .scenario(Scenario::SyncFlush { records: 4 })
            .run();
        assert_eq!(
            report.acks,
            vec![false, false, false, true],
            "records 0-1 (dropped segment) + record 2 (torn) are Nacked; \
             record 3 (fresh segment, fsynced) is durably acked"
        );
    }

    /// Scenario (Phase 2): the shutdown seal hits ENOSPC after the records were
    /// already group-fsynced + acked. The acks stay `true` (the data is durable)
    /// and recovery recovers every one of them from the un-sealed `.wab` — a
    /// failed seal must not lose an acked record.
    #[test]
    fn enospc_at_shutdown_seal_recovers_every_acked_record() {
        let report = Sim::new(0x5EED_0006)
            .fault(Fault::SealFails)
            .scenario(Scenario::SealFailsAtShutdown { records: 5 })
            .run();
        assert_eq!(report.acks, vec![true; 5], "the group fsync succeeded");
        assert_eq!(
            report.recovered, report.written,
            "recovery recovers exactly the acked records from the un-sealed segment"
        );
    }

    /// Replays every pinned regression seed in `tests/dst_seeds/`. A spec whose
    /// invariants once failed lives here forever; `run()` re-checks them and
    /// panics with the seed repro on any regression. New failing seeds are
    /// pinned by dropping their serialised `SimSpec` JSON into that directory.
    #[test]
    fn replay_pinned_regression_seeds() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/dst_seeds");
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("tests/dst_seeds unreadable ({}): {e}", dir.display()));
        let mut replayed = 0;
        for entry in entries {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let json = std::fs::read_to_string(&path).unwrap();
            let spec: SimSpec = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("malformed DST seed {}: {e}", path.display()));
            spec.run();
            replayed += 1;
        }
        assert!(
            replayed > 0,
            "no pinned DST seeds found in {}",
            dir.display()
        );
    }

    /// Random-seed sweep across the two fault scenarios. Every run checks its
    /// invariants in-harness, so any seed that breaks one fails here with a
    /// pin-able repro. The count is `WEIR_DST_SWEEP` (small by default so PR
    /// runs stay fast; the CI `dst` job cranks it up).
    #[test]
    fn sweep_random_seeds() {
        let n: u64 = std::env::var("WEIR_DST_SWEEP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16);
        let mut rng = SplitMix64::new(0xA11C_E5EE_D000);
        for _ in 0..n {
            let seed = rng.next_u64();
            let records = 1 + (seed % 8) as usize;
            Sim::new(seed)
                .fault(Fault::FsyncReturns { nth: 1 })
                .scenario(Scenario::SyncFlush { records })
                .run();
            Sim::new(seed)
                .fault(Fault::RenameFails)
                .scenario(Scenario::CrashBeforeRename { records })
                .run();
        }
    }
}
