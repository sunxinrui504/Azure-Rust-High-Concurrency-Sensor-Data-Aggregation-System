use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BufferStats {
    pub capacity: usize,
    pub len: usize,
    pub high_watermark: usize,
    pub total_pushed: u64,
    pub total_popped: u64,
    pub push_wait_count: u64,
    pub pop_wait_count: u64,
    pub push_wait_ns_total: u128,
    pub pop_wait_ns_total: u128,
    pub is_shutdown: bool,
}

impl BufferStats {
    pub fn fill_ratio(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.len as f64 / self.capacity as f64
        }
    }

    pub fn is_high_pressure(&self, watermark: f64) -> bool {
        self.fill_ratio() >= watermark.clamp(0.0, 1.0)
    }

    pub fn push_wait_count_delta_since(&self, earlier: &Self) -> u64 {
        self.push_wait_count.saturating_sub(earlier.push_wait_count)
    }

    pub fn pop_wait_count_delta_since(&self, earlier: &Self) -> u64 {
        self.pop_wait_count.saturating_sub(earlier.pop_wait_count)
    }

    pub fn push_wait_ns_delta_since(&self, earlier: &Self) -> u128 {
        self.push_wait_ns_total
            .saturating_sub(earlier.push_wait_ns_total)
    }

    pub fn pop_wait_ns_delta_since(&self, earlier: &Self) -> u128 {
        self.pop_wait_ns_total
            .saturating_sub(earlier.pop_wait_ns_total)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushError {
    Shutdown,
}

struct Inner<T> {
    q: VecDeque<T>,
    is_shutdown: bool,
    high_watermark: usize,
    total_pushed: u64,
    total_popped: u64,
    push_wait_count: u64,
    pop_wait_count: u64,
    push_wait_ns_total: u128,
    pop_wait_ns_total: u128,
}

/// A bounded, blocking, thread-safe queue for producer-consumer pipelines.
///
/// - `push()` blocks when full (backpressure).
/// - `pop_blocking()` blocks when empty.
/// - `pop_timeout()` blocks up to a duration.
/// - `shutdown()` wakes all waiters; subsequent pops return `None` when empty.
pub struct SensorBufferManager<T> {
    capacity: usize,
    inner: Mutex<Inner<T>>,
    not_empty: Condvar,
    not_full: Condvar,
}

impl<T> SensorBufferManager<T> {
    pub fn new(capacity: usize) -> Arc<Self> {
        assert!(capacity > 0, "capacity must be > 0");
        Arc::new(Self {
            capacity,
            inner: Mutex::new(Inner {
                q: VecDeque::with_capacity(capacity.min(1024)),
                is_shutdown: false,
                high_watermark: 0,
                total_pushed: 0,
                total_popped: 0,
                push_wait_count: 0,
                pop_wait_count: 0,
                push_wait_ns_total: 0,
                pop_wait_ns_total: 0,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        })
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        let g = self.inner.lock().unwrap();
        g.q.len()
    }

    /// Returns a snapshot of internal metrics (best-effort).
    pub fn stats(&self) -> BufferStats {
        let g = self.inner.lock().unwrap();
        BufferStats {
            capacity: self.capacity,
            len: g.q.len(),
            high_watermark: g.high_watermark,
            total_pushed: g.total_pushed,
            total_popped: g.total_popped,
            push_wait_count: g.push_wait_count,
            pop_wait_count: g.pop_wait_count,
            push_wait_ns_total: g.push_wait_ns_total,
            pop_wait_ns_total: g.pop_wait_ns_total,
            is_shutdown: g.is_shutdown,
        }
    }

    /// Signals all waiters to exit. After shutdown:
    /// - `push()` returns `Err(PushError::Shutdown)`
    /// - `pop_*()` returns `None` once the queue becomes empty
    pub fn shutdown(&self) {
        let mut g = self.inner.lock().unwrap();
        g.is_shutdown = true;
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }

    pub fn is_shutdown(&self) -> bool {
        self.inner.lock().unwrap().is_shutdown
    }

    /// Non-blocking push.
    /// Returns `Ok(())` on success, `Err(PushError::Shutdown)` if shutdown,
    /// or `Err(())` if full.
    pub fn try_push(&self, item: T) -> Result<(), Result<PushError, T>> {
        let mut g = self.inner.lock().unwrap();
        if g.is_shutdown {
            return Err(Ok(PushError::Shutdown));
        }
        if g.q.len() >= self.capacity {
            return Err(Err(item));
        }
        g.q.push_back(item);
        g.total_pushed += 1;
        g.high_watermark = g.high_watermark.max(g.q.len());
        drop(g);
        self.not_empty.notify_one();
        Ok(())
    }

    /// Blocking push with backpressure when full.
    pub fn push(&self, item: T) -> Result<(), PushError> {
        let mut g = self.inner.lock().unwrap();
        if g.is_shutdown {
            return Err(PushError::Shutdown);
        }

        while g.q.len() >= self.capacity && !g.is_shutdown {
            g.push_wait_count += 1;
            let start = Instant::now();
            g = self.not_full.wait(g).unwrap();
            g.push_wait_ns_total += start.elapsed().as_nanos();
        }

        if g.is_shutdown {
            return Err(PushError::Shutdown);
        }

        g.q.push_back(item);
        g.total_pushed += 1;
        g.high_watermark = g.high_watermark.max(g.q.len());
        drop(g);
        self.not_empty.notify_one();
        Ok(())
    }

    /// Non-blocking pop.
    pub fn try_pop(&self) -> Option<T> {
        let mut g = self.inner.lock().unwrap();
        let out = g.q.pop_front();
        if out.is_some() {
            g.total_popped += 1;
            drop(g);
            self.not_full.notify_one();
        }
        out
    }

    /// Blocking pop. Returns `None` only when shutdown and the queue is empty.
    pub fn pop_blocking(&self) -> Option<T> {
        let mut g = self.inner.lock().unwrap();

        while g.q.is_empty() && !g.is_shutdown {
            g.pop_wait_count += 1;
            let start = Instant::now();
            g = self.not_empty.wait(g).unwrap();
            g.pop_wait_ns_total += start.elapsed().as_nanos();
        }

        let out = g.q.pop_front();
        if out.is_some() {
            g.total_popped += 1;
            drop(g);
            self.not_full.notify_one();
            return out;
        }

        // Empty: if shutdown -> None; if not shutdown, loop would have waited.
        None
    }

    /// Pop with timeout. Returns `None` on timeout, or when shutdown and empty.
    pub fn pop_timeout(&self, timeout: Duration) -> Option<T> {
        let mut g = self.inner.lock().unwrap();
        let mut remaining = timeout;

        while g.q.is_empty() && !g.is_shutdown {
            if remaining.is_zero() {
                return None;
            }
            g.pop_wait_count += 1;
            let start = Instant::now();
            let (gg, wait_res) = self.not_empty.wait_timeout(g, remaining).unwrap();
            let waited = start.elapsed();
            let waited_ns = waited.as_nanos();
            g = gg;
            g.pop_wait_ns_total += waited_ns;

            if wait_res.timed_out() {
                return None;
            }
            remaining = remaining.saturating_sub(waited);
        }

        let out = g.q.pop_front();
        if out.is_some() {
            g.total_popped += 1;
            drop(g);
            self.not_full.notify_one();
        }
        out
    }

    /// Helper: consider buffer under backpressure at a given fraction.
    /// Example: `is_under_backpressure(0.8)` means len/capacity >= 0.8.
    pub fn is_under_backpressure(&self, watermark: f64) -> bool {
        let s = self.stats();
        if s.capacity == 0 {
            return false;
        }
        (s.len as f64) >= (watermark.clamp(0.0, 1.0) * s.capacity as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn pop_timeout_times_out_when_empty() {
        let b = SensorBufferManager::<u32>::new(4);
        let start = Instant::now();
        let got = b.pop_timeout(Duration::from_millis(50));
        let elapsed = start.elapsed();
        assert!(got.is_none());
        assert!(elapsed >= Duration::from_millis(40));
    }

    #[test]
    fn push_blocks_when_full_until_pop() {
        let b = SensorBufferManager::<u32>::new(1);
        b.push(1).unwrap();

        let b2 = Arc::clone(&b);
        let handle = thread::spawn(move || {
            // must block until main pops
            b2.push(2).unwrap();
        });

        thread::sleep(Duration::from_millis(50));
        assert_eq!(b.try_pop(), Some(1));
        handle.join().unwrap();
        assert_eq!(b.try_pop(), Some(2));
    }

    #[test]
    fn shutdown_wakes_blocking_pop() {
        let b = SensorBufferManager::<u32>::new(4);
        let b2 = Arc::clone(&b);
        let handle = thread::spawn(move || b2.pop_blocking());

        thread::sleep(Duration::from_millis(50));
        b.shutdown();
        let got = handle.join().unwrap();
        assert!(got.is_none());
    }
}

