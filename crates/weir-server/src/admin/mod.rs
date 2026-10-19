//! The admin Unix socket (Foundations spec §5.9, §11): a JSON-lines control
//! channel for the daemon's own uid and root, separate from the producer
//! socket and never on the producer-shared directory.

// Unreachable from `main` until Task 6.14 wires it in. Unconditional, so it
// also covers the test target that `--all-targets` lints; Task 6.14 deletes
// this line and clippy must then pass without it.
#![allow(dead_code, reason = "wired into main by Task 6.14")]

pub(crate) mod identity;
