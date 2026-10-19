//! The blocking-time seam for the WAB flusher.
//!
//! [`BlockingClock`] abstracts the three ways the flusher blocks on wall-clock
//! time — waiting for the next batch (`recv_timeout`), handing a sealed
//! segment to the drain (`send_timeout`) and the panic-supervisor backoff
//! (`sleep`) — and its one time source, the hand-off's shutdown budget
//! (`now`), so the deterministic-simulation harness can substitute a virtual
//! clock that advances instantly and reproducibly. Production uses
//! [`RealClock`], which delegates straight to crossbeam / `std::thread` /
//! `std::time::Instant`.
//!
//! It is taken as a generic bound (`C: BlockingClock`), never a trait object:
//! `recv_timeout` is generic over the channel message type, which makes the
//! trait object-unsafe — and a zero-sized `RealClock` monomorphises back to the
//! exact code the flusher ran before the seam existed.

use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, SendTimeoutError, Sender};

/// The flusher's wall-clock dependencies. See the module docs.
pub(crate) trait BlockingClock {
    /// Block for at most `timeout`, returning the next value from `rx`.
    /// Mirrors [`crossbeam_channel::Receiver::recv_timeout`].
    fn recv_timeout<T>(&self, rx: &Receiver<T>, timeout: Duration) -> Result<T, RecvTimeoutError>;

    /// Block for at most `timeout` trying to send `value` on `tx`. On a timeout
    /// or a closed channel the value comes back inside the error, so the caller
    /// still owns it. Mirrors [`crossbeam_channel::Sender::send_timeout`]. The
    /// flusher's hand-off to the drain is the only caller (`wab::handoff`).
    #[cfg_attr(not(test), allow(dead_code))] // Task 3.4 routes the flusher's sends here
    fn send_timeout<T>(
        &self,
        tx: &Sender<T>,
        value: T,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<T>>;

    /// Block the current thread for `dur`. Mirrors [`std::thread::sleep`].
    fn sleep(&self, dur: Duration);

    /// Monotonic time since an arbitrary epoch. Only differences are
    /// meaningful. The hand-off measures its shutdown budget with it, so the
    /// DST clocks can make that budget virtual and deterministic.
    #[cfg_attr(not(test), allow(dead_code))] // Task 3.4 routes the flusher's sends here
    fn now(&self) -> Duration;
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
    fn send_timeout<T>(
        &self,
        tx: &Sender<T>,
        value: T,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<T>> {
        tx.send_timeout(value, timeout)
    }

    #[inline]
    fn sleep(&self, dur: Duration) {
        std::thread::sleep(dur);
    }

    /// Elapsed time since a process-wide epoch, fixed on the first call.
    fn now(&self) -> Duration {
        static EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        EPOCH.get_or_init(std::time::Instant::now).elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::SendTimeoutError;
    use std::time::Instant;

    #[test]
    fn real_send_timeout_hands_the_value_back_after_the_timeout_on_a_full_channel() {
        let (tx, _rx) = crossbeam_channel::bounded::<u32>(1);
        tx.send(1).unwrap();
        let t0 = Instant::now();
        match RealClock.send_timeout(&tx, 2, Duration::from_millis(100)) {
            Err(SendTimeoutError::Timeout(v)) => {
                assert_eq!(v, 2, "the unsent value must come back")
            }
            other => panic!("expected Timeout(2), got {other:?}"),
        }
        let waited = t0.elapsed();
        assert!(
            waited >= Duration::from_millis(90) && waited < Duration::from_millis(600),
            "a full channel must wait out the timeout, waited {waited:?}"
        );
    }

    #[test]
    fn real_send_timeout_reports_a_closed_channel_at_once() {
        let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
        drop(rx);
        let t0 = Instant::now();
        assert!(matches!(
            RealClock.send_timeout(&tx, 7, Duration::from_secs(5)),
            Err(SendTimeoutError::Disconnected(7))
        ));
        assert!(t0.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn real_send_timeout_delivers_when_there_is_room() {
        let (tx, rx) = crossbeam_channel::bounded::<u32>(1);
        RealClock
            .send_timeout(&tx, 3, Duration::from_millis(100))
            .unwrap();
        assert_eq!(rx.try_recv().unwrap(), 3);
    }

    #[test]
    fn real_now_is_monotonic_and_follows_wall_time() {
        let a = RealClock.now();
        std::thread::sleep(Duration::from_millis(50));
        let b = RealClock.now();
        assert!(
            b >= a + Duration::from_millis(45),
            "now() must advance with real time: {a:?} -> {b:?}"
        );
        assert!(b - a < Duration::from_millis(1000), "{a:?} -> {b:?}");
    }
}
