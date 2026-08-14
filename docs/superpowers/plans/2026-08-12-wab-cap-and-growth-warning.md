# WAB size cap and growth warning — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop weir acking records into an unbounded WAB — add an opt-in size
cap that Nacks over the limit, and warn when the WAB is growing while the sink
is down.

**Architecture:** A background task already computes WAB bytes every 5 s for a
gauge. It additionally stores the value in a shared `AtomicU64`, which the
connection handler loads before accepting a push. No new I/O, no new scan, and
an atomic load on the ingest path.

**Tech Stack:** Rust 2024, tokio, `prometheus-client`.

## Global Constraints

- **Source spec:** `docs/superpowers/specs/2026-08-12-backpressure-and-quarantine-design.md`,
  §3 and §4. §5 is a **separate plan** (quarantine tooling) — do not implement it here.
- **Branch:** `v2/main-line`.
- **Over the cap, Nack `NackReason::InternalError` (0x06)** — NOT a new byte.
  This is verified-deliberate: `weir-client/src/unix.rs:108,113` maps
  `Nack(InternalError) => true` but `UnknownNack(_) => false`, so a new byte
  would make every existing client reconnect at the worst possible moment.
- **`wab_max_bytes` defaults to `0` (disabled).** No existing deployment
  changes behaviour on upgrade.
- **The cap is a SOFT high-water mark.** The observed value is up to 5 s stale.
  Docs must say so in those words.
- **The gate** for every "run the tests" step:
  ```bash
  cargo test --workspace --exclude weir-server
  cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
  cargo test -p weir-server --bins -- --test-threads=1
  ```
  (The `--test-threads=1` is W3, the process-global umask issue. A fix is
  specced in `docs/superpowers/specs/2026-08-10-project-hygiene-design.md`; if
  that has landed, plain `cargo test` works instead.)

---

### Task 1: Config knob `wab_max_bytes`

**Files:**
- Modify: `crates/weir-server/src/config/mod.rs` (raw field, resolved field, validation, struct literal)
- Modify: `crates/weir-server/src/config/cli.rs`
- Modify: `crates/weir-server/src/config/env.rs`
- Modify: `crates/weir-server/src/config/file.rs` (struct field **and** `BASE_SERVER_KEYS`)

**Interfaces:**
- Consumes: nothing.
- Produces: `Config::wab_max_bytes: u64`. Task 3 reads it; Task 5 documents it.

Follow the `wab_segment_max_age_secs` precedent exactly — it is the closest
analogue (a `u64` WAB knob with `0 = disabled`).

- [ ] **Step 1: Write the failing tests**

Append to the `mod tests` block in `crates/weir-server/src/config/mod.rs`:

```rust
    #[test]
    fn wab_max_bytes_defaults_to_disabled() {
        let dir = tmp_dir("cap_default");
        let c = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap();
        assert_eq!(c.wab_max_bytes, 0, "default must preserve existing behaviour");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn wab_max_bytes_accepts_a_value_at_or_above_one_segment() {
        let dir = tmp_dir("cap_ok");
        let c = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                wab_segment_max_bytes: Some(1024 * 1024),
                wab_max_bytes: Some(64 * 1024 * 1024),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap();
        assert_eq!(c.wab_max_bytes, 64 * 1024 * 1024);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn wab_max_bytes_below_one_segment_is_rejected() {
        // A cap under the rotation threshold would reject before a single
        // segment could fill — a configuration error, not a policy.
        let dir = tmp_dir("cap_too_small");
        let err = Config::from_layers(
            PartialConfig {
                wab_dir: Some(dir.clone()),
                wab_segment_max_bytes: Some(64 * 1024 * 1024),
                wab_max_bytes: Some(1024),
                ..PartialConfig::empty()
            },
            PartialConfig::empty(),
            PartialConfig::empty(),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("wab_max_bytes"), "{msg}");
        assert!(msg.contains("wab_segment_max_bytes"), "{msg}");
        fs::remove_dir_all(dir).ok();
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p weir-server --bins wab_max_bytes -- --test-threads=1`
Expected: FAIL to compile — `PartialConfig has no field named wab_max_bytes`.

- [ ] **Step 3: Add the raw and resolved fields**

In `crates/weir-server/src/config/mod.rs`, add to `PartialConfig` beside
`wab_segment_max_age_secs`:

```rust
    pub wab_max_bytes: Option<u64>,
```

and to `Config` beside `wab_segment_max_age_secs`:

```rust
    /// Soft upper bound on live WAB bytes on disk. `0` (default) disables it.
    ///
    /// When exceeded, pushes are Nacked with `NackReason::InternalError` rather
    /// than acked into a WAB that cannot be drained. This closes the case where
    /// a dead or slow drain lets the disk fill while producers keep receiving
    /// successful acks.
    ///
    /// **This is a SOFT high-water mark.** The value it is checked against is
    /// refreshed every 5 seconds, so the WAB can overshoot by up to 5 seconds of
    /// peak ingest. Leave at least that much headroom below actual free space.
    pub wab_max_bytes: u64,
```

- [ ] **Step 4: Add the merge and validation**

In `Config::from_layers`, immediately after the `wab_segment_max_bytes` line:

```rust
        // 0 = disabled. Otherwise the cap must admit at least one full segment,
        // or ingest would be rejected before a single segment could fill.
        let wab_max_bytes = merge!(wab_max_bytes).unwrap_or(0);
        if wab_max_bytes != 0 && wab_max_bytes < wab_segment_max_bytes {
            return Err(ConfigError::InvalidValue {
                field: "wab_max_bytes",
                reason: format!(
                    "wab_max_bytes ({wab_max_bytes}) must be 0 (disabled) or at least \
                     wab_segment_max_bytes ({wab_segment_max_bytes})"
                ),
            });
        }
```

and add `wab_max_bytes,` to the `Ok(Config { … })` struct literal, beside
`wab_segment_max_age_secs`.

- [ ] **Step 5: Plumb CLI, env and TOML**

`config/cli.rs`, beside the other `wab_` knobs:

```rust
        wab_max_bytes: pargs
            .opt_value_from_str("--wab-max-bytes")
            .map_err(pico_err)?,
```

`config/env.rs`:

```rust
        wab_max_bytes: env_parse("WEIR_WAB_MAX_BYTES")?,
```

`config/file.rs` — the struct field:

```rust
    wab_max_bytes: Option<u64>,
```

the `BASE_SERVER_KEYS` entry (**required**, or a TOML file setting it is
rejected as an unknown key):

```rust
    "wab_max_bytes",
```

and the build:

```rust
            wab_max_bytes: s.wab_max_bytes,
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p weir-server --bins wab_max_bytes -- --test-threads=1`
Expected: PASS — 3 passed.

- [ ] **Step 7: Commit**

```bash
cargo fmt
git add crates/weir-server/src/config
git commit -m "feat(config): wab_max_bytes, a soft cap on live WAB bytes

Opt-in (0 = disabled, matching the wab_segment_max_age_secs convention), so no
existing deployment changes behaviour on upgrade. Validated to be either 0 or
at least wab_segment_max_bytes: a cap below the rotation threshold would reject
ingest before a single segment could fill, which is a misconfiguration rather
than a policy.

Nothing enforces it yet — that lands with the ingest check.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: The shared byte counter and the cap-rejection metric

**Files:**
- Modify: `crates/weir-server/src/metrics/mod.rs`
- Modify: `crates/weir-server/src/main.rs` (the 5 s gauge task, ~line 566)
- Modify: `crates/weir-server/tests/system.rs` (the metric guard list)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `Metrics::wab_cap_rejections: Counter<u64, AtomicU64>` (exposed as
    `weir_wab_cap_rejections_total`).
  - An `Arc<AtomicU64>` created in `main`, updated by the 5 s task, holding the
    most recent `compute_wab_bytes_on_disk` value. Task 3 reads it.

- [ ] **Step 1: Register the counter**

In `crates/weir-server/src/metrics/mod.rs`, add the field beside
`recovery_quarantine_copy_failed`:

```rust
    /// Pushes rejected because live WAB bytes exceeded `wab_max_bytes`.
    pub wab_cap_rejections: Counter<u64, AtomicU64>,
```

register it beside the other `reg!` calls:

```rust
        let wab_cap_rejections = reg!(
            Counter::<u64, AtomicU64>::default(),
            "weir_wab_cap_rejections",
            "Pushes Nacked because live WAB bytes exceeded wab_max_bytes. These \
             surface to clients as NackReason::InternalError (the same byte as \
             queue saturation), so this counter is how the two are told apart"
        );
```

and add `wab_cap_rejections,` to the constructor's struct literal.

- [ ] **Step 2: Add it to the metric guard list**

`crates/weir-server/tests/system.rs` has `metrics_all_families_registered`,
which fails when a registered family is missing from its expected list. Add,
beside the other `weir_wab_` entries:

```rust
        "weir_wab_cap_rejections",
```

- [ ] **Step 3: Verify the guard passes**

Run: `cargo test -p weir-server --test system metrics_all_families_registered`
Expected: PASS. If it fails with a count mismatch, the name in the guard list
does not match the `reg!` name — they must agree exactly.

- [ ] **Step 4: Publish the byte count into a shared atomic**

In `crates/weir-server/src/main.rs`, the existing 5 s task reads:

```rust
        let wab_dir_bg = config.wab_dir.clone();
        let metrics_w = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let wab_dir = wab_dir_bg.clone();
                let bytes = tokio::task::spawn_blocking(move || {
                    compute_wab_bytes_on_disk(&wab_dir)
                })
                .await
                .unwrap_or(0);
                metrics_w.wab_bytes_on_disk.set(bytes as f64);
            }
        });
```

Create the atomic **before** the task and clone it in, then store into it
alongside the gauge:

```rust
        // Shared with the connection handlers so the ingest path can consult the
        // WAB size without doing any I/O of its own — this task already walks
        // the directory for the gauge, so the value is free.
        let wab_bytes_now = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let wab_dir_bg = config.wab_dir.clone();
        let metrics_w = Arc::clone(&metrics);
        let wab_bytes_bg = Arc::clone(&wab_bytes_now);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let wab_dir = wab_dir_bg.clone();
                let bytes = tokio::task::spawn_blocking(move || {
                    compute_wab_bytes_on_disk(&wab_dir)
                })
                .await
                .unwrap_or(0);
                metrics_w.wab_bytes_on_disk.set(bytes as f64);
                wab_bytes_bg.store(bytes, std::sync::atomic::Ordering::Relaxed);
            }
        });
```

`wab_bytes_now` must be declared where `socket_config` (`main.rs:710`) can also
see it — hoist it above the runtime block if the borrow checker requires.

- [ ] **Step 5: Verify it compiles and nothing regressed**

```bash
cargo fmt
cargo check -p weir-server --all-targets
cargo test -p weir-server --bins -- --test-threads=1
```
Expected: 0 errors; the bin suite passes unchanged (nothing consumes the atomic
yet).

- [ ] **Step 6: Commit**

```bash
git add crates/weir-server/src/metrics crates/weir-server/src/main.rs crates/weir-server/tests/system.rs
git commit -m "feat(metrics): weir_wab_cap_rejections + publish WAB bytes to a shared atomic

The 5s task that already walks the WAB directory for weir_wab_bytes_on_disk now
also stores the value in an AtomicU64, so the ingest path can consult WAB size
with an atomic load and no I/O of its own.

The counter exists because cap rejections surface to clients as
NackReason::InternalError — the same byte as queue saturation — so metrics are
where the two are told apart. That is the price of not minting a new wire byte
that would make every existing client reconnect.

Nothing reads the atomic yet.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Enforce the cap on the ingest path

**Files:**
- Modify: `crates/weir-server/src/socket/mod.rs` (`SocketConfig`, `conn_cfg_template`)
- Modify: `crates/weir-server/src/socket/connection.rs` (`ConnectionConfig`, the `MessageType::Push` arm)
- Modify: `crates/weir-server/src/main.rs` (`SocketConfig` construction, ~line 710)

**Interfaces:**
- Consumes: `Config::wab_max_bytes` (Task 1), the `Arc<AtomicU64>` and
  `Metrics::wab_cap_rejections` (Task 2).
- Produces: nothing for later tasks.

**Read before editing.** The check must happen **after** the whole frame is
read and CRC-verified, in the `MessageType::Push` arm, and **before**
`records_accepted` is incremented. Nacking mid-frame would leave unread payload
bytes that the client mis-reads as a later reply — the client poisons its
connection on exactly that (`unix.rs:299-307`). And a rejected record must not
count as accepted.

Hysteresis: reject while over `wab_max_bytes`, resume once below
`wab_max_bytes × 9 / 10`. Track the "currently rejecting" state in a shared
`AtomicBool` next to the byte counter so all connections agree.

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `crates/weir-server/src/socket/connection.rs`:

```rust
    #[tokio::test]
    async fn push_over_the_wab_cap_nacks_internal_error_and_keeps_the_connection() {
        // The cap Nack must be InternalError specifically: the client's
        // is_recoverable() maps that to `true`, so producers back off instead of
        // tearing down and reconnecting at the worst possible moment.
        let bytes = Arc::new(AtomicU64::new(5_000));
        let rejecting = Arc::new(AtomicBool::new(false));
        let cfg = ConnectionConfig {
            wab_max_bytes: 1_000,
            wab_bytes_now: Arc::clone(&bytes),
            wab_cap_rejecting: Arc::clone(&rejecting),
            ..test_cfg()
        };
        let metrics = Arc::new(Metrics::new().0);
        let mut client = spawn_handler_with(cfg, Arc::clone(&metrics)).await;

        client.write_all(&push_frame(b"hello")).await.unwrap();
        let (msg_type, payload) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Nack);
        assert_eq!(
            payload[0],
            NackReason::InternalError as u8,
            "cap rejection must reuse InternalError so clients stay connected"
        );
        assert_eq!(metrics.wab_cap_rejections.get(), 1);

        // The connection must still be usable — send a second frame and get a
        // reply rather than a closed socket.
        client.write_all(&push_frame(b"again")).await.unwrap();
        let (msg_type2, _) = read_response(&mut client).await;
        assert_eq!(
            msg_type2,
            MessageType::Nack,
            "connection must remain open after a cap Nack"
        );
    }

    #[tokio::test]
    async fn push_under_the_wab_cap_is_accepted() {
        let bytes = Arc::new(AtomicU64::new(100));
        let rejecting = Arc::new(AtomicBool::new(false));
        let cfg = ConnectionConfig {
            wab_max_bytes: 1_000,
            wab_bytes_now: Arc::clone(&bytes),
            wab_cap_rejecting: Arc::clone(&rejecting),
            ..test_cfg()
        };
        let metrics = Arc::new(Metrics::new().0);
        let mut client = spawn_handler_acking_with(cfg, Arc::clone(&metrics), Some(true)).await;
        client.write_all(&push_frame(b"hello")).await.unwrap();
        let (msg_type, _) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Ack);
        assert_eq!(metrics.wab_cap_rejections.get(), 0);
    }

    #[tokio::test]
    async fn cap_disabled_never_rejects() {
        let bytes = Arc::new(AtomicU64::new(u64::MAX));
        let rejecting = Arc::new(AtomicBool::new(false));
        let cfg = ConnectionConfig {
            wab_max_bytes: 0, // disabled
            wab_bytes_now: Arc::clone(&bytes),
            wab_cap_rejecting: Arc::clone(&rejecting),
            ..test_cfg()
        };
        let metrics = Arc::new(Metrics::new().0);
        let mut client = spawn_handler_acking_with(cfg, Arc::clone(&metrics), Some(true)).await;
        client.write_all(&push_frame(b"hello")).await.unwrap();
        let (msg_type, _) = read_response(&mut client).await;
        assert_eq!(msg_type, MessageType::Ack, "cap 0 must disable the check entirely");
    }

    #[tokio::test]
    async fn cap_hysteresis_holds_until_the_low_water_mark() {
        // Once rejecting, stay rejecting until bytes fall below cap * 0.9 —
        // otherwise ingest flaps on and off at the boundary.
        let bytes = Arc::new(AtomicU64::new(1_500));
        let rejecting = Arc::new(AtomicBool::new(false));
        let cfg = ConnectionConfig {
            wab_max_bytes: 1_000,
            wab_bytes_now: Arc::clone(&bytes),
            wab_cap_rejecting: Arc::clone(&rejecting),
            ..test_cfg()
        };
        let metrics = Arc::new(Metrics::new().0);
        let mut client = spawn_handler_acking_with(cfg, Arc::clone(&metrics), Some(true)).await;

        client.write_all(&push_frame(b"a")).await.unwrap();
        assert_eq!(read_response(&mut client).await.0, MessageType::Nack);

        // Just under the cap but above the low-water mark: still rejecting.
        bytes.store(950, Ordering::Relaxed);
        client.write_all(&push_frame(b"b")).await.unwrap();
        assert_eq!(
            read_response(&mut client).await.0,
            MessageType::Nack,
            "must not resume until below cap * 0.9"
        );

        // Below the low-water mark: accepting again.
        bytes.store(800, Ordering::Relaxed);
        client.write_all(&push_frame(b"c")).await.unwrap();
        assert_eq!(read_response(&mut client).await.0, MessageType::Ack);
    }
```

`spawn_handler_with` / `spawn_handler_acking_with` are variants of the module's
existing `spawn_handler_acking` that take an explicit `ConnectionConfig` and
`Arc<Metrics>`. Read `spawn_handler_acking` first and add the variants beside
it rather than duplicating its body.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p weir-server --bins wab_cap -- --test-threads=1`
Expected: FAIL to compile — `ConnectionConfig has no field wab_max_bytes`.

- [ ] **Step 3: Extend the config structs**

`crates/weir-server/src/socket/connection.rs`, on `ConnectionConfig`:

```rust
    /// Soft cap on live WAB bytes; `0` disables the check. See
    /// `Config::wab_max_bytes`.
    pub wab_max_bytes: u64,
    /// Most recent WAB byte count, refreshed every 5 s by the gauge task in
    /// `main`. Read with `Ordering::Relaxed` — a stale read costs at most one
    /// extra accepted record, and the cap is documented as a soft bound.
    pub wab_bytes_now: Arc<AtomicU64>,
    /// Whether the cap is currently rejecting. Shared across connections so all
    /// of them agree, and so the low-water mark is honoured globally rather
    /// than per-connection.
    pub wab_cap_rejecting: Arc<AtomicBool>,
```

`crates/weir-server/src/socket/mod.rs`, the same three fields on `SocketConfig`,
and pass them through in `conn_cfg_template`:

```rust
    let conn_cfg_template = ConnectionConfig {
        max_payload_bytes: effective_cap,
        read_timeout: Duration::from_secs(config.connection_read_timeout_secs),
        ack_timeout: crate::socket::connection::ACK_TIMEOUT,
        shard_id: 0, // overridden per connection below
        wab_max_bytes: config.wab_max_bytes,
        wab_bytes_now: Arc::clone(&config.wab_bytes_now),
        wab_cap_rejecting: Arc::clone(&config.wab_cap_rejecting),
    };
```

`crates/weir-server/src/main.rs`, in the `SocketConfig { … }` literal:

```rust
                wab_max_bytes: config.wab_max_bytes,
                wab_bytes_now: Arc::clone(&wab_bytes_now),
                wab_cap_rejecting: Arc::new(std::sync::atomic::AtomicBool::new(false)),
```

- [ ] **Step 4: Implement the check**

In `connection.rs`, in the `MessageType::Push` arm, **before** the
`records_accepted` increment:

```rust
            MessageType::Push => {
                let tv = durability_to_tier(header.durability());

                // WAB cap. Checked here — after the frame is fully read and CRC
                // verified, before the record counts as accepted. Nacking any
                // earlier would leave unread payload bytes in the stream that the
                // client mis-reads as a later reply, and it poisons its connection
                // on exactly that.
                //
                // All three durability tiers are rejected: Buffered still writes
                // to the WAB, it just acks earlier.
                if over_wab_cap(&config) {
                    send_nack(
                        stream.get_mut(),
                        WireNack::InternalError,
                        &[],
                        config.read_timeout,
                    )
                    .await?;
                    metrics.wab_cap_rejections.inc();
                    metrics
                        .records_nack
                        .get_or_create(&NackLabel {
                            tier: tv,
                            reason: MetricNack::internal_error,
                        })
                        .inc();
                    continue;
                }

                metrics
                    .records_accepted
                    .get_or_create(&TierLabel { tier: tv.clone() })
                    .inc();
                // … existing handle_push call unchanged
```

Use `continue` if the handler body is a loop over frames; if it is not, use
whatever the neighbouring `BadPayloadCrc` rejection uses (`return Ok(())`) —
match the existing control flow rather than inventing one.

Add the helper beside the other free functions in `connection.rs`:

```rust
/// Whether the WAB cap is currently rejecting pushes.
///
/// Hysteresis: once rejecting, keep rejecting until bytes fall below
/// `cap * 9 / 10`, so ingest does not flap on and off at the boundary. `cap == 0`
/// disables the check entirely.
fn over_wab_cap(config: &ConnectionConfig) -> bool {
    if config.wab_max_bytes == 0 {
        return false;
    }
    let bytes = config.wab_bytes_now.load(Ordering::Relaxed);
    let rejecting = config.wab_cap_rejecting.load(Ordering::Relaxed);
    let low_water = config.wab_max_bytes / 10 * 9;
    let now_rejecting = if rejecting {
        bytes >= low_water
    } else {
        bytes >= config.wab_max_bytes
    };
    if now_rejecting != rejecting {
        config
            .wab_cap_rejecting
            .store(now_rejecting, Ordering::Relaxed);
    }
    now_rejecting
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p weir-server --bins wab_cap -- --test-threads=1`
Expected: PASS — 4 passed.

- [ ] **Step 6: Prove the client actually stays usable**

The unit test above asserts the daemon keeps the connection open. Add a system
test that asserts it through a **real `WeirClient`**, because the whole
justification for reusing `InternalError` is the client's reaction to it:

```rust
#[test]
fn wab_cap_nack_is_recoverable_for_a_real_client() {
    // wab_max_bytes just above one segment, and a segment size small enough that
    // a short burst crosses it. The point is not the exact trip count — it is
    // that when the Nack arrives, the client reports it as recoverable and the
    // same connection keeps working.
    let srv = weir_server!("cap_recoverable")
        .extra_config("wab_segment_max_bytes = 65536\nwab_max_bytes = 65536\nsink_type = \"noop\"")
        .start();
    let mut client = srv.client();

    let payload = vec![b'x'; 4096];
    let mut saw_nack = false;
    for _ in 0..200 {
        match client.push(&payload, Durability::Sync) {
            Ok(()) => {}
            Err(e) => {
                assert!(
                    e.is_recoverable(),
                    "a cap Nack must be recoverable, got {e:?}"
                );
                saw_nack = true;
                break;
            }
        }
    }
    assert!(saw_nack, "expected the cap to trip within 200 pushes");
    assert!(!client.is_poisoned(), "the connection must still be usable");
}
```

Run: `cargo test -p weir-server --test system wab_cap_nack_is_recoverable`
Expected: PASS. If the cap never trips, the drain is keeping up — lower
`wab_max_bytes` or raise the burst, but do **not** delete the assertion.

- [ ] **Step 7: Run the full gate**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --all-targets --no-default-features -- -D warnings
cargo test --workspace --exclude weir-server
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
cargo test -p weir-server --bins -- --test-threads=1
```
Expected: all exit 0.

- [ ] **Step 8: Commit**

```bash
git add crates/weir-server/src
git commit -m "feat(socket): enforce wab_max_bytes on the ingest path

Over the cap, a push is Nacked with NackReason::InternalError instead of being
acked into a WAB that cannot be drained. That closes the case where a dead or
slow drain lets the disk fill while producers keep getting successful acks.

InternalError rather than a new byte, deliberately: the client maps
Nack(InternalError) to recoverable but UnknownNack to non-recoverable, so a new
byte would make every existing client tear down and reconnect exactly when the
daemon is already under strain. A system test asserts recoverability through a
real WeirClient, since that reaction is the entire justification.

The check sits after the frame is fully read and CRC-verified and before the
record counts as accepted: Nacking earlier would leave unread payload bytes
that the client mis-reads as a later reply, and it poisons its connection on
exactly that. All three tiers are rejected — Buffered still writes to the WAB.

Hysteresis at cap * 0.9, shared across connections so the low-water mark is
global rather than per-connection.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: The growth warning

**Files:**
- Modify: `crates/weir-server/src/main.rs` (the 5 s task)

**Interfaces:**
- Consumes: `Config::wab_max_bytes` (Task 1), the shared atomic (Task 2).
- Produces: nothing.

Warns only when **all three** hold: the cap is unset, the samples show sustained
growth, and the sink is not `Healthy` **or** the drain is not `Draining`.
Condition 3 is what makes it trustworthy — sustained growth under a healthy
drain is a fast producer, and warning on that trains operators to ignore it.

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `crates/weir-server/src/main.rs`:

```rust
    #[test]
    fn growth_warning_fires_only_when_all_three_conditions_hold() {
        // Growing + unhealthy + no cap => warn.
        assert!(should_warn_wab_growth(
            &[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200],
            0,
            false,
        ));
        // Growing + HEALTHY => no warning. This is the condition that keeps the
        // message trustworthy: a fast producer must not trigger it.
        assert!(!should_warn_wab_growth(
            &[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200],
            0,
            true,
        ));
        // Growing + unhealthy but a cap IS set => no warning; the cap handles it.
        assert!(!should_warn_wab_growth(
            &[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200],
            64 * 1024 * 1024,
            false,
        ));
        // Flat or shrinking => no warning.
        assert!(!should_warn_wab_growth(
            &[500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500],
            0,
            false,
        ));
        assert!(!should_warn_wab_growth(
            &[1200, 1100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100],
            0,
            false,
        ));
        // Not enough samples yet => no warning.
        assert!(!should_warn_wab_growth(&[100, 200, 300], 0, false));
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p weir-server --bins growth_warning -- --test-threads=1`
Expected: FAIL — `cannot find function should_warn_wab_growth`.

- [ ] **Step 3: Implement the predicate**

Add to `crates/weir-server/src/main.rs`, beside `compute_wab_bytes_on_disk`:

```rust
/// Number of 5 s samples the growth warning considers (60 s).
const WAB_GROWTH_WINDOW: usize = 12;

/// Whether to warn that the WAB is growing unbounded.
///
/// All three must hold:
/// 1. no cap is set — with one set, the cap handles it and the warning is noise;
/// 2. the window is full and shows net growth with no decrease;
/// 3. the sink is unhealthy or the drain is not draining.
///
/// Condition 3 is the one that matters. Sustained growth under a healthy drain
/// is just a fast producer; warning on that teaches operators to ignore the
/// message, which defeats the purpose.
fn should_warn_wab_growth(samples: &[u64], wab_max_bytes: u64, drain_healthy: bool) -> bool {
    if wab_max_bytes != 0 || drain_healthy || samples.len() < WAB_GROWTH_WINDOW {
        return false;
    }
    let window = &samples[samples.len() - WAB_GROWTH_WINDOW..];
    let monotonic = window.windows(2).all(|w| w[1] >= w[0]);
    monotonic && window[window.len() - 1] > window[0]
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p weir-server --bins growth_warning -- --test-threads=1`
Expected: PASS.

- [ ] **Step 5: Wire it into the 5 s task**

In the task from Task 2 Step 4, keep a rolling sample buffer, read the health
gauges back, and warn at most once per 5 minutes:

```rust
            let mut samples: Vec<u64> = Vec::with_capacity(WAB_GROWTH_WINDOW + 1);
            let mut last_warned: Option<std::time::Instant> = None;
            loop {
                interval.tick().await;
                // … existing compute + gauge + atomic store …

                samples.push(bytes);
                if samples.len() > WAB_GROWTH_WINDOW {
                    samples.remove(0);
                }
                // `sink_health` and `drain_state` are Gauge families, so the
                // current value reads back without new plumbing.
                let drain_healthy = metrics_w
                    .sink_health
                    .get_or_create(&crate::metrics::SinkHealthLabel {
                        state: crate::metrics::SinkHealthValue::healthy,
                    })
                    .get()
                    > 0.0;
                let due = last_warned
                    .map(|t| t.elapsed() >= std::time::Duration::from_secs(300))
                    .unwrap_or(true);
                if due && should_warn_wab_growth(&samples, cap_for_warning, drain_healthy) {
                    warn!(
                        wab_bytes = bytes,
                        growth_bytes = bytes.saturating_sub(samples[0]),
                        window_secs = WAB_GROWTH_WINDOW * 5,
                        "WAB is growing while the sink is not healthy and no wab_max_bytes \
                         is set — producers are still being acked into an unbounded buffer. \
                         Set wab_max_bytes to bound it."
                    );
                    last_warned = Some(std::time::Instant::now());
                }
            }
```

`cap_for_warning` is `config.wab_max_bytes` captured before the task is spawned.
The exact `SinkHealthLabel` / `SinkHealthValue` variant names are in
`crates/weir-server/src/metrics/mod.rs` — read them rather than assuming; if
`sink_health` is keyed differently, adapt the read and keep the semantics
("healthy" ⇒ no warning).

- [ ] **Step 6: Run the gate and commit**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test -p weir-server --bins -- --test-threads=1
```

```bash
git add crates/weir-server/src/main.rs
git commit -m "feat(wab): warn when the WAB grows unbounded with an unhealthy sink

wab_max_bytes defaults to off, so protection only reaches operators who already
know to ask. Rather than pick a default cap that could Nack a deployment
legitimately buffering through a long outage — which is weir's job — the daemon
warns at the moment the hole is actually opening.

Three conditions, and the third is what makes it trustworthy: no cap set,
sustained growth over 60s, AND the sink unhealthy or the drain not draining.
Sustained growth under a healthy drain is just a fast producer; warning on that
teaches operators to ignore the message. Rate-limited to once per 5 minutes.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Documentation

**Files:**
- Modify: `docs/operations/configuration.md`
- Modify: `docs/monitoring.md`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: everything. Produces nothing.

- [ ] **Step 1: Document the knob**

Add a `#### \`wab_max_bytes\`` section to `docs/operations/configuration.md`,
beside the other `wab_` knobs, matching their format (Type / Default / Range /
CLI / Env / TOML, then prose). It must state:

- what it does, and that **all three durability tiers** are rejected;
- that over the cap, clients receive `NackReason::InternalError`, the same
  reason as queue saturation, and that `weir_wab_cap_rejections_total` is how
  the two are distinguished;
- **in these words, that it is a soft high-water mark**: the value is refreshed
  every 5 seconds, so the WAB can overshoot by up to 5 seconds of peak ingest,
  and operators should leave at least that much headroom below actual free
  space;
- that `0` disables it and is the default;
- that it must be `0` or at least `wab_segment_max_bytes`.

If the config-doc drift guard from the project-hygiene plan has landed, it will
fail until this section exists — that is the guard working.

- [ ] **Step 2: Document the metric**

Add `weir_wab_cap_rejections_total` to the metric table in
`docs/monitoring.md`, beside the other `weir_wab_` entries, noting that it is
the only way to distinguish cap rejections from queue-saturation
`InternalError` Nacks.

- [ ] **Step 3: CHANGELOG**

Add under `## [Unreleased]`:

```markdown
- **`wab_max_bytes` — a soft cap on live WAB bytes, off by default.** Until now
  the WAB was unbounded: when the drain gave up, its own log said *"delivery is
  stopped and the WAB will accumulate on disk until restart"* — while producers
  kept receiving successful acks. Every acked record really was on disk, so this
  was never a false ack, but it was its nearest neighbour: a disk filling behind
  a green light.

  Over the cap, pushes are Nacked with `NackReason::InternalError` — the
  existing byte, not a new one. That is deliberate: the client treats
  `InternalError` as recoverable but an unknown Nack reason as fatal, so a new
  byte would make every client built before it tear down and reconnect precisely
  when the daemon is already under strain. The cost is that cap rejections share
  a reason byte with queue saturation; `weir_wab_cap_rejections_total`
  distinguishes them.

  **It is a soft high-water mark.** The value is refreshed every 5 seconds, so
  the WAB can overshoot by up to 5 seconds of peak ingest — leave headroom.

  Because it defaults to off, the daemon also **warns when the WAB is growing
  while the sink is unhealthy and no cap is set**, rate-limited to once every 5
  minutes. That warning deliberately does not fire under a healthy drain, where
  sustained growth is just a fast producer.
```

- [ ] **Step 4: Full gate and commit**

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --exclude weir-server
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
cargo test -p weir-server --bins -- --test-threads=1
cargo deny check advisories bans licenses sources
```

```bash
git add docs CHANGELOG.md
git commit -m "docs: wab_max_bytes, the cap-rejection metric, and the soft-bound caveat

States the soft-bound limitation in the words the spec requires: the value is
refreshed every 5s, so the WAB can overshoot by up to 5s of peak ingest, and an
operator who sets the cap at their exact free space will still fill the disk.
A caveat carried by docs is weaker than one enforced by code, so it is stated
plainly rather than buried.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Self-review notes

**Spec coverage.** §3.1 shared atomic → Task 2. §3.2 `InternalError` → Task 3,
with the real-client assertion in Step 6. §3.3 frame fully consumed → Task 3
Step 4 comment and placement. §3.4 soft cap → Task 5 Step 1, stated verbatim.
§3.5 all tiers / low-water / counter / default → Tasks 1–3. §4 warning → Task 4.
§6 config → Task 1. §7 testing → distributed across Tasks 1, 3, 4.

**Not covered here, deliberately.** Spec §5 (quarantine tooling,
`RecoveryReader`, `weir_quarantine_bytes_on_disk`,
`weir_recovery_segments_failed_total`) is a separate plan — different crates,
no shared code.

## As built — where the shipped code deviates from the tasks above

Completed at `7c65837` (range `2d2735e..7c65837`). Three of the code blocks
above were **rejected during review and must not be copied from this document**;
they are left in place as the record of what was planned.

**1. Task 2 Step 4 prescribes a fail-open defect.** The plan's task body
contains `.await.unwrap_or(0)`. A join error therefore reports the WAB as 0
bytes, which does not just lose a sample — it *releases the cap entirely* at the
moment the scan is failing. Shipped code extracts `apply_wab_scan`
(`main.rs:133`), which takes the `JoinResult` rather than an `Option<u64>` and,
on a join error, keeps the last known value and contributes no sample. Taking
the `JoinResult` is load-bearing: the defect is a call-site substitution, so a
helper taking `Option<u64>` cannot guard against it.

**2. Task 1 Step 4's validation was too weak to be safe.** Planned:
`wab_max_bytes >= wab_segment_max_bytes`. That admits a legal cap that wedges
ingest permanently under a perfectly healthy sink, because the hysteresis resume
threshold has to clear the bytes that *cannot* be drained away — one un-sealable
active segment **per shard** — and an active segment only seals when a write
crosses the size threshold, which over the cap never happens. Shipped
(`config/mod.rs:580-600`): reject unless `wab_max_bytes / 10 * 9 >
wab_segment_max_bytes * shard_count`, with the error message computing the exact
minimum. The single test named in Step 1 became six, including
`wab_max_bytes_sized_for_one_shard_is_rejected_when_there_are_more`.

**3. Task 4 Step 3's predicate was too strict.** Planned: pairwise monotonic
(`w[1] >= w[0]` across the window), which a single 1-byte dip anywhere in 60 s
defeats. Shipped (`main.rs:184-187`): `last > first && all(s >= first)` — net
growth with no sample below the window's start. Step 5's `drain_healthy` read is
also not a bare `sink_health` lookup; it composes `sink_health` **and**
`drain_state` in a separately unit-tested helper, because a swapped `&&`/`||`
there silently inverts which failures the warning covers.

**Also shipped under this plan but absent from it.** Review found the new
terminal `drain_state{stopped}` was invisible to every consumer weir ships:
no alert rule, dashboards summing it to 0 and rendering a green "Draining", and
a readiness probe reporting ready. `11f03a9` fixed the alert rules, the five
generated dashboards plus the hand-maintained one, the readiness probe (ordered
before the sink-health check, which `stopped` invalidates), and the
`monitoring.md` runbook.

---

**Known rough edges for the implementer.**
- Task 3 Step 1 names `spawn_handler_with` / `spawn_handler_acking_with`, which
  do not exist yet. Read the module's existing `spawn_handler_acking` and add
  variants beside it; do not build a parallel harness.
- Task 3 Step 4's `continue` assumes the handler loops over frames. Check the
  neighbouring `BadPayloadCrc` rejection and match whatever control flow it
  uses.
- Task 4 Step 5 reads `sink_health` back through a label whose exact variant
  names must be checked in `metrics/mod.rs`. The semantics are fixed ("healthy
  ⇒ no warning"); the label spelling is not.
- Task 3 Step 6's system test depends on the cap tripping within 200 pushes. If
  the drain keeps up, tune the config — do not weaken the assertion.
