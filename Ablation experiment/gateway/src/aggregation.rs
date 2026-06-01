use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime},
};

use crate::{
    buffer::SensorBufferManager,
    types::{AggregatedFrame, AnomalyRecord, ReadingEnvelope, SensorStats},
};

#[derive(Debug, Clone)]
pub struct AggregationConfig {
    pub window: Duration,
    pub workers: usize,
    /// Bounded inbox capacity per worker.
    pub worker_inbox_capacity: usize,
    /// How long the dispatcher waits when popping from the source buffer.
    pub source_pop_timeout: Duration,
    /// How long the coordinator waits for each worker report after rotation.
    pub report_timeout: Duration,
    /// Output channel capacity for aggregated frames.
    pub output_capacity: usize,
    /// Z-score threshold for anomaly detection.
    pub anomaly_z_threshold: f64,
    /// Minimum samples required before performing anomaly detection.
    pub anomaly_min_samples: u64,
}

impl Default for AggregationConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(1),
            workers: 4,
            worker_inbox_capacity: 8192,
            source_pop_timeout: Duration::from_millis(10),
            report_timeout: Duration::from_millis(200),
            output_capacity: 64,
            anomaly_z_threshold: 3.0,
            anomaly_min_samples: 20,
        }
    }
}

#[derive(Debug)]
enum WorkerMsg {
    Data(ReadingEnvelope),
    Rotate,
    Shutdown,
}

#[derive(Debug)]
struct WindowReport {
    bucket: HashMap<String, SensorStats>,
    anomalies: Vec<AnomalyRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowCollectError {
    Timeout,
    Disconnected,
}

fn hash_to_worker(sensor_id: &str, workers: usize) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    sensor_id.hash(&mut h);
    (h.finish() as usize) % workers
}

fn reduce_window_reports(
    report_rx: &Receiver<WindowReport>,
    expected_reports: usize,
    timeout: Duration,
) -> Result<(HashMap<String, SensorStats>, Vec<AnomalyRecord>), WindowCollectError> {
    let mut merged: HashMap<String, SensorStats> = HashMap::new();
    let mut anomalies: Vec<AnomalyRecord> = Vec::new();

    for _ in 0..expected_reports {
        match report_rx.recv_timeout(timeout) {
            Ok(rep) => {
                for (sid, stats) in rep.bucket {
                    let entry = merged.entry(sid).or_default();
                    *entry = entry.merge(stats);
                }
                anomalies.extend(rep.anomalies);
            }
            Err(RecvTimeoutError::Timeout) => return Err(WindowCollectError::Timeout),
            Err(RecvTimeoutError::Disconnected) => return Err(WindowCollectError::Disconnected),
        }
    }

    Ok((merged, anomalies))
}

struct AggregationWorker {
    inbox: Receiver<WorkerMsg>,
    report_tx: SyncSender<WindowReport>,
    local_stats: HashMap<String, SensorStats>,
    anomalies: Vec<AnomalyRecord>,
    anomaly_z_threshold: f64,
    anomaly_min_samples: u64,
}

impl AggregationWorker {
    fn run(mut self, shutdown: Arc<AtomicBool>) {
        while !shutdown.load(Ordering::Relaxed) {
            let msg = match self.inbox.recv() {
                Ok(m) => m,
                Err(_) => break,
            };

            match msg {
                WorkerMsg::Data(r) => {
                    let sid = r.sensor_id.clone();
                    let entry = self.local_stats.entry(sid.clone()).or_default();
                    entry.update(r.value);

                    if entry.count >= self.anomaly_min_samples {
                        let std = entry.stddev_sample();
                        if std.is_finite() && std > 0.0 {
                            let z = ((r.value - entry.mean).abs()) / std;
                            if z > self.anomaly_z_threshold {
                                self.anomalies.push(AnomalyRecord {
                                    sensor_id: sid,
                                    ts: r.ts,
                                    value: r.value,
                                    z,
                                });
                            }
                        }
                    }
                }
                WorkerMsg::Rotate => {
                    // O(1) swap-on-rotate: hand off current bucket to coordinator.
                    let bucket = std::mem::take(&mut self.local_stats);
                    let anomalies = std::mem::take(&mut self.anomalies);
                    // Best-effort: if coordinator is gone, we stop.
                    if self
                        .report_tx
                        .send(WindowReport {
                            bucket,
                            anomalies,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
                WorkerMsg::Shutdown => break,
            }
        }
    }
}

/// MapReduce-style aggregation engine:
/// - Dispatcher routes readings to workers by `hash(sensor_id) % workers`
/// - Workers update thread-local maps (no shared locks)
/// - On window rotation, workers swap buckets and send reports to the coordinator
/// - Coordinator reduces (merges) per-sensor stats via Chan/Welford merge
pub struct AggregationEngine {
    shutdown: Arc<AtomicBool>,
    dispatcher: JoinHandle<()>,
    workers: Vec<JoinHandle<()>>,
    frames_rx: mpsc::Receiver<AggregatedFrame>,
}

impl AggregationEngine {
    pub fn start(
        cfg: AggregationConfig,
        source: Arc<SensorBufferManager<ReadingEnvelope>>,
    ) -> Self {
        assert!(cfg.workers > 0, "workers must be > 0");
        assert!(cfg.worker_inbox_capacity > 0, "worker_inbox_capacity must be > 0");
        assert!(cfg.output_capacity > 0, "output_capacity must be > 0");

        let shutdown = Arc::new(AtomicBool::new(false));

        // Reports from workers to coordinator (bounded to avoid unbounded memory).
        let (report_tx, report_rx) = mpsc::sync_channel::<WindowReport>(cfg.workers * 2);

        // Output frames from coordinator to downstream storage/web.
        let (frames_tx, frames_rx) = mpsc::sync_channel::<AggregatedFrame>(cfg.output_capacity);

        // Create workers.
        let mut worker_txs = Vec::with_capacity(cfg.workers);
        let mut workers = Vec::with_capacity(cfg.workers);
        for _ in 0..cfg.workers {
            let (tx, rx) = mpsc::sync_channel::<WorkerMsg>(cfg.worker_inbox_capacity);
            worker_txs.push(tx);
            let shutdown_clone = Arc::clone(&shutdown);
            let report_tx_clone = report_tx.clone();
            let anomaly_z_threshold = cfg.anomaly_z_threshold;
            let anomaly_min_samples = cfg.anomaly_min_samples;
            let handle = thread::spawn(move || {
                AggregationWorker {
                    inbox: rx,
                    report_tx: report_tx_clone,
                    local_stats: HashMap::new(),
                    anomalies: Vec::new(),
                    anomaly_z_threshold,
                    anomaly_min_samples,
                }
                .run(shutdown_clone);
            });
            workers.push(handle);
        }
        drop(report_tx); // dispatcher/coordinator only uses report_rx now

        // Dispatcher + coordinator combined (single monotonic clock source).
        let shutdown_clone = Arc::clone(&shutdown);
        let dispatcher = thread::spawn(move || {
            let mut window_id: u64 = 0;
            let mut window_start_instant = Instant::now();
            let mut window_start_wall = SystemTime::now();
            let mut saw_data_in_window = false;

            let emit_window = |window_id: u64,
                               window_start_wall: SystemTime,
                               window_end_wall: SystemTime,
                               shutdown_flag: &Arc<AtomicBool>,
                               worker_txs: &Vec<SyncSender<WorkerMsg>>,
                               report_rx: &Receiver<WindowReport>,
                               frames_tx: &SyncSender<AggregatedFrame>|
             -> bool {
                for tx in worker_txs {
                    if tx.send(WorkerMsg::Rotate).is_err() {
                        shutdown_flag.store(true, Ordering::Relaxed);
                        return false;
                    }
                }

                let (merged, anomalies) = match reduce_window_reports(report_rx, cfg.workers, cfg.report_timeout) {
                    Ok(parts) => parts,
                    Err(WindowCollectError::Timeout | WindowCollectError::Disconnected) => {
                        shutdown_flag.store(true, Ordering::Relaxed);
                        return false;
                    }
                };

                let frame = AggregatedFrame {
                    window_id,
                    window_start: window_start_wall,
                    window_end: window_end_wall,
                    per_sensor: merged,
                    anomalies,
                };

                if frames_tx.send(frame).is_err() {
                    shutdown_flag.store(true, Ordering::Relaxed);
                    return false;
                }

                true
            };

            loop {
                if shutdown_clone.load(Ordering::Relaxed) && !(source.is_shutdown() && source.len() > 0) {
                    break;
                }

                let source_drained = source.is_shutdown() && source.len() == 0;
                if source_drained {
                    if saw_data_in_window
                        && !emit_window(
                            window_id,
                            window_start_wall,
                            SystemTime::now(),
                            &shutdown_clone,
                            &worker_txs,
                            &report_rx,
                            &frames_tx,
                        )
                    {
                        break;
                    }
                    break;
                }

                if let Some(reading) = source.pop_timeout(cfg.source_pop_timeout) {
                    let idx = hash_to_worker(&reading.sensor_id, cfg.workers);
                    if worker_txs[idx].send(WorkerMsg::Data(reading)).is_err() {
                        shutdown_clone.store(true, Ordering::Relaxed);
                        break;
                    }
                    saw_data_in_window = true;
                }

                if window_start_instant.elapsed() >= cfg.window {
                    if !emit_window(
                        window_id,
                        window_start_wall,
                        SystemTime::now(),
                        &shutdown_clone,
                        &worker_txs,
                        &report_rx,
                        &frames_tx,
                    ) {
                        break;
                    }

                    window_id += 1;
                    window_start_instant = Instant::now();
                    window_start_wall = SystemTime::now();
                    saw_data_in_window = false;
                }
            }

            // Shutdown workers
            for tx in &worker_txs {
                let _ = tx.send(WorkerMsg::Shutdown);
            }
        });

        Self {
            shutdown,
            dispatcher,
            workers,
            frames_rx,
        }
    }

    /// Receive one aggregated frame, blocking up to `timeout`.
    pub fn recv_frame_timeout(&self, timeout: Duration) -> Option<AggregatedFrame> {
        self.frames_rx.recv_timeout(timeout).ok()
    }

    /// Signal shutdown and join all internal threads.
    pub fn shutdown(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = self.dispatcher.join();
        for h in self.workers {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::SensorBufferManager;
    use std::sync::mpsc;

    #[test]
    fn merges_stats_across_workers() {
        let buf = SensorBufferManager::new(1024);
        let cfg = AggregationConfig {
            window: Duration::from_millis(200),
            workers: 2,
            worker_inbox_capacity: 1024,
            source_pop_timeout: Duration::from_millis(5),
            report_timeout: Duration::from_millis(100),
            output_capacity: 8,
            anomaly_z_threshold: 3.0,
            anomaly_min_samples: 5,
        };
        let engine = AggregationEngine::start(cfg, Arc::clone(&buf));

        // Two sensors, should deterministically route by hash.
        let now = SystemTime::now();
        for _ in 0..10 {
            buf.push(ReadingEnvelope::new("s-1", now, 1.0)).unwrap();
            buf.push(ReadingEnvelope::new("s-2", now, 2.0)).unwrap();
        }

        let frame = engine
            .recv_frame_timeout(Duration::from_secs(2))
            .expect("expected a frame");

        assert_eq!(frame.per_sensor.get("s-1").unwrap().count, 10);
        assert_eq!(frame.per_sensor.get("s-2").unwrap().count, 10);
        assert!(frame.anomalies.is_empty());

        engine.shutdown();
    }

    #[test]
    fn report_collection_times_out_without_emitting_partial_results() {
        let (tx, rx) = mpsc::sync_channel(2);
        let mut bucket = HashMap::new();
        let mut stats = SensorStats::default();
        stats.update(1.0);
        bucket.insert("sensor-a".to_string(), stats);
        tx.send(WindowReport {
            bucket,
            anomalies: Vec::new(),
        })
        .unwrap();

        let err = reduce_window_reports(&rx, 2, Duration::from_millis(10)).unwrap_err();
        assert_eq!(err, WindowCollectError::Timeout);
    }
}

