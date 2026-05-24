// Items are incrementally wired into main() across steps 01–08. Until step-08
// completes the wiring, unused-item warnings are noise rather than signal.
#![allow(dead_code)]

mod models;
mod queue;
#[cfg(unix)]
mod socket;
mod wab;
mod worker;

fn main() {}
