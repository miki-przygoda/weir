//! The blocking-time seam for the WAB flusher.
//!
//! [`BlockingClock`] abstracts the two ways the flusher blocks on wall-clock
//! time — waiting for the next batch (`recv_timeout`) and the panic-supervisor
//! backoff (`sleep`) — so the deterministic-simulation harness can substitute a
//! virtual clock that advances instantly and reproducibly. Production uses
//! [`RealClock`], which delegates straight to crossbeam / `std::thread`.
//!
//! It is taken as a generic bound (`C: BlockingClock`), never a trait object:
//! `recv_timeout` is generic over the channel message type, which makes the
//! trait object-unsafe — and a zero-sized `RealClock` monomorphises back to the
//! exact code the flusher ran before the seam existed.

use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError};

/// The flusher's wall-clock dependencies. See the module docs.
pub(crate) trait BlockingClock {
    /// Block for at most `timeout`, returning the next value from `rx`.
    /// Mirrors [`crossbeam_channel::Receiver::recv_timeout`].
    fn recv_timeout<T>(&self, rx: &Receiver<T>, timeout: Duration) -> Result<T, RecvTimeoutError>;

    /// Block the current thread for `dur`. Mirrors [`std::thread::sleep`].
    fn sleep(&self, dur: Duration);
}

/// Production [`BlockingClock`]: real crossbeam waits and real thread sleeps.
/// Zero-sized, so the flusher monomorphises to its pre-seam form.
#[derive(Clone, Copy, Default)]
pub(crate) struct RealClock;

impl BlockingClock for RealClock {
    #[inline]
    fn recv_timeout<T>(&self, rx: &Receiver<T>, timeout: Duration) -> Result<T, RecvTimeoutError> {
        rx.recv_timeout(timeout)
    }

    #[inline]
    fn sleep(&self, dur: Duration) {
        std::thread::sleep(dur);
    }
}
