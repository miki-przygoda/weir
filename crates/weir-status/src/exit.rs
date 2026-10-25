//! The one exit-code table for `weir-ctl status`, shared with
//! `deploy/systemd/weir-readiness.sh` (spec §12, §14). The script maps these
//! codes 1:1; the parity rule is `code ∈ {0, 3} ⇔ readiness().ok ⇔ /readyz 200`.

use crate::{Condition, Severity};

/// How `weir-ctl status` exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusExit {
    /// 0: ready, and no critical condition is active.
    Ready,
    /// 1: not ready: exactly when `/readyz` would answer 503.
    NotReady,
    /// 2: no daemon answered and there are no offline facts, or the admin
    /// socket failed a client-side identity check (spec §14, README Q9).
    Unreachable,
    /// 3: ready, but a critical condition that does not gate readiness is
    /// active (any `affects`), e.g. `shard_offline` or `drain_stopped`.
    ReadyWithCritical,
}

impl StatusExit {
    /// The process exit status: PR 1's shared constants, so the readiness
    /// script, `weir-ctl status` and `exit_code` cannot disagree.
    pub const fn code(self) -> u8 {
        match self {
            StatusExit::Ready => crate::EXIT_READY as u8,
            StatusExit::NotReady => crate::EXIT_NOT_READY as u8,
            StatusExit::Unreachable => crate::EXIT_UNREACHABLE as u8,
            StatusExit::ReadyWithCritical => crate::EXIT_CRITICAL as u8,
        }
    }

    /// The table itself. Not-ready wins over critical, so exit 1 is exactly
    /// `ready == false`.
    pub const fn from_parts(ready: bool, any_critical: bool) -> StatusExit {
        match (ready, any_critical) {
            (false, _) => StatusExit::NotReady,
            (true, true) => StatusExit::ReadyWithCritical,
            (true, false) => StatusExit::Ready,
        }
    }

    /// The table over an evaluated condition list (`ready` is
    /// `readiness(&evidence).ok`, or a daemon document's `verdict.ready`).
    pub fn from_verdict(ready: bool, conditions: &[Condition]) -> StatusExit {
        StatusExit::from_parts(
            ready,
            conditions.iter().any(|c| c.severity == Severity::Critical),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_are_the_documented_table() {
        // spec §14 / §12: one table, shared with deploy/systemd/weir-readiness.sh.
        assert_eq!(StatusExit::Ready.code(), 0);
        assert_eq!(StatusExit::NotReady.code(), 1);
        assert_eq!(StatusExit::Unreachable.code(), 2);
        assert_eq!(StatusExit::ReadyWithCritical.code(), 3);
    }

    #[test]
    fn from_parts_is_ready_first_then_critical() {
        assert_eq!(StatusExit::from_parts(true, false), StatusExit::Ready);
        assert_eq!(
            StatusExit::from_parts(true, true),
            StatusExit::ReadyWithCritical
        );
        // Not ready wins over critical: exit 1 exactly when /readyz is 503.
        assert_eq!(StatusExit::from_parts(false, false), StatusExit::NotReady);
        assert_eq!(StatusExit::from_parts(false, true), StatusExit::NotReady);
    }

    #[test]
    fn exit_in_0_or_3_iff_ready() {
        for ready in [false, true] {
            for crit in [false, true] {
                let code = StatusExit::from_parts(ready, crit).code();
                assert_eq!(
                    code == 0 || code == 3,
                    ready,
                    "ready={ready} critical={crit}"
                );
                assert_ne!(code, 2, "2 is reserved for unreachable / identity mismatch");
            }
        }
    }
}
