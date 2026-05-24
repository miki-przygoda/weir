/// Current wire protocol version. Strict equality is enforced on decode — no forward compatibility.
/// Incrementing this value is a breaking change: all clients must be updated before the daemon.
pub const WIRE_VERSION: u8 = 1;

/// Absolute ceiling on payload size across all callers: socket layer, WAB recovery, and drain.
/// Defined here so the compiler enforces a single source of truth at the crate boundary.
/// Any site that needs a payload size cap must import this constant, not define its own.
/// 16 MiB is large enough for any reasonable record and small enough to bound memory pressure
/// during recovery (worst case: one corrupt segment with max-size records).
pub const MAX_PAYLOAD_HARD_CAP: usize = 16 * 1024 * 1024;
