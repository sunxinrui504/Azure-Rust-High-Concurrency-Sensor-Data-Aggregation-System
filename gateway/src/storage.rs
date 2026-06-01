use std::{
    fs::{self, File},
    io::{Error, ErrorKind, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::AggregatedFrame;

#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Base directory for all outputs (e.g. `data/`).
    pub dir: PathBuf,
    /// Flush policy: flush JSONL writer every N frames (0 = never except rotation/shutdown).
    pub flush_every_n_frames: u64,
    /// If true, call `sync_all()` on shutdown (durability).
    pub sync_on_shutdown: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data"),
            flush_every_n_frames: 50,
            sync_on_shutdown: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    ChannelClosed,
    WorkerFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageMetrics {
    pub persisted_frames: u64,
    pub last_persist_us: u64,
    pub max_persist_us: u64,
    pub last_window_id: Option<u64>,
    pub current_hour_file: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug)]
struct PersistRequest {
    frame: AggregatedFrame,
    stats_snapshot: Option<Value>,
    ack: SyncSender<Result<(), StorageError>>,
}

#[derive(Debug)]
enum StorageMsg {
    Persist(PersistRequest),
    Shutdown,
}

#[derive(Clone)]
pub struct StorageHandle {
    tx: SyncSender<StorageMsg>,
    shutdown: Arc<AtomicBool>,
    join: Arc<MutexJoin<JoinHandle<()>>>,
    metrics: Arc<Mutex<StorageMetrics>>,
}

// A tiny wrapper so we can store JoinHandle behind Arc without exposing a mutex in API.
struct MutexJoin<T>(std::sync::Mutex<Option<T>>);
impl<T> MutexJoin<T> {
    fn new(v: T) -> Self {
        Self(std::sync::Mutex::new(Some(v)))
    }
    fn take(&self) -> Option<T> {
        self.0.lock().unwrap().take()
    }
}

impl StorageHandle {
    pub fn start(cfg: StorageConfig, capacity: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel::<StorageMsg>(capacity.max(1));
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);
        let metrics = Arc::new(Mutex::new(StorageMetrics::default()));
        let metrics_clone = Arc::clone(&metrics);
        let join = thread::spawn(move || storage_worker_main(cfg, rx, shutdown_clone, metrics_clone));

        Self {
            tx,
            shutdown,
            join: Arc::new(MutexJoin::new(join)),
            metrics,
        }
    }

    pub fn persist_frame(
        &self,
        frame: AggregatedFrame,
        stats_snapshot: Option<Value>,
    ) -> Result<(), StorageError> {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        self.tx
            .send(StorageMsg::Persist(PersistRequest {
                frame,
                stats_snapshot,
                ack: ack_tx,
            }))
            .map_err(|_| StorageError::ChannelClosed)?;
        ack_rx.recv().unwrap_or(Err(StorageError::WorkerFailed))
    }

    pub fn send_frame(&self, frame: AggregatedFrame) -> Result<(), StorageError> {
        self.persist_frame(frame, None)
    }

    pub fn metrics(&self) -> StorageMetrics {
        self.metrics.lock().unwrap().clone()
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = self.tx.send(StorageMsg::Shutdown);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

fn ensure_dirs(base: &Path) -> std::io::Result<(PathBuf, PathBuf)> {
    let frames_dir = base.join("frames");
    fs::create_dir_all(&frames_dir)?;
    Ok((base.to_path_buf(), frames_dir))
}

fn hour_key_from_system_time(t: SystemTime) -> (i64, u32, u32, u32) {
    // Use local time formatting would require chrono/time crates; keep it simple and stable:
    // derive hour key from Unix seconds (UTC), rounded down to hour.
    let secs = t
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs() as i64;
    let hour = secs / 3600;
    // For a human-readable name we still need y/m/d/h; we instead encode as `hour-<unix_hour>`.
    (hour, 0, 0, 0)
}

fn hour_filename(unix_hour: i64) -> String {
    format!("hour-{}.jsonl", unix_hour)
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.flush()?;
        // We intentionally do not fsync tmp here to keep performance reasonable.
    }
    fs::rename(tmp, path)?;
    Ok(())
}

fn atomic_write_json<T: Serialize>(path: &Path, v: &T) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(v).unwrap_or_else(|_| b"{}".to_vec());
    atomic_write_bytes(path, &bytes)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuntimeSnapshot {
    latest: AggregatedFrame,
    stats: Value,
}

struct HourlyWriter {
    frames_dir: PathBuf,
    current_unix_hour: Option<i64>,
    current_path: Option<PathBuf>,
    current_lines: Vec<String>,
}

impl HourlyWriter {
    fn new(frames_dir: PathBuf) -> Self {
        Self {
            frames_dir,
            current_unix_hour: None,
            current_path: None,
            current_lines: Vec::new(),
        }
    }

    fn rotate_if_needed(&mut self, unix_hour: i64) -> std::io::Result<()> {
        if self.current_unix_hour == Some(unix_hour) && self.current_path.is_some() {
            return Ok(());
        }

        let path = self.frames_dir.join(hour_filename(unix_hour));
        let existing = match fs::read_to_string(&path) {
            Ok(content) => content
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(ToOwned::to_owned)
                .collect(),
            Err(err) if err.kind() == ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(err),
        };

        self.current_unix_hour = Some(unix_hour);
        self.current_path = Some(path);
        self.current_lines = existing;
        Ok(())
    }

    fn append_jsonl(&mut self, unix_hour: i64, frame: &AggregatedFrame) -> std::io::Result<()> {
        self.rotate_if_needed(unix_hour)?;
        let line = serde_json::to_string(frame)
            .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
        self.current_lines.push(line);
        self.flush()
    }

    fn current_hour_file(&self) -> Option<String> {
        self.current_unix_hour.map(hour_filename)
    }

    fn flush(&self) -> std::io::Result<()> {
        let Some(path) = &self.current_path else {
            return Ok(());
        };
        let mut bytes = self.current_lines.join("\n").into_bytes();
        if !bytes.is_empty() {
            bytes.push(b'\n');
        }
        atomic_write_bytes(path, &bytes)
    }

    fn sync_all(&self) {
        if let Some(path) = &self.current_path {
            if let Ok(file) = File::open(path) {
                let _ = file.sync_all();
            }
        }
    }
}

fn persist_runtime_artifacts(
    base_dir: &Path,
    hourly: &mut HourlyWriter,
    frame: &AggregatedFrame,
    stats_snapshot: Option<&Value>,
) -> std::io::Result<()> {
    let latest_snapshot_path = base_dir.join("latest.json");
    let latest_index_path = base_dir.join("latest");
    let stats_snapshot_path = base_dir.join("stats.json");
    let runtime_snapshot_path = base_dir.join("snapshot.json");

    let (unix_hour, _, _, _) = hour_key_from_system_time(frame.window_end);
    hourly.append_jsonl(unix_hour, frame)?;

    atomic_write_json(&latest_snapshot_path, frame)?;

    if let Some(stats) = stats_snapshot {
        atomic_write_json(&stats_snapshot_path, stats)?;
        atomic_write_json(
            &runtime_snapshot_path,
            &RuntimeSnapshot {
                latest: frame.clone(),
                stats: stats.clone(),
            },
        )?;
    }

    let idx = serde_json::json!({
        "window_id": frame.window_id,
        "window_end_unix": frame.window_end.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs()),
        "hour_file": hourly.current_hour_file(),
    });
    atomic_write_json(&latest_index_path, &idx)?;
    Ok(())
}

fn ack_result(ack: SyncSender<Result<(), StorageError>>, result: Result<(), StorageError>) {
    let _ = ack.send(result);
}

fn sync_snapshot_files(base_dir: &Path) {
    for name in ["latest.json", "latest", "stats.json", "snapshot.json"] {
        let path = base_dir.join(name);
        if let Ok(file) = File::open(path) {
            let _ = file.sync_all();
        }
    }
}

fn persist_with_metrics(
    base_dir: &Path,
    hourly: &mut HourlyWriter,
    frame: &AggregatedFrame,
    stats_snapshot: Option<&Value>,
    shared_metrics: &Mutex<StorageMetrics>,
) -> Result<(), StorageError> {
    let start = Instant::now();
    let persist_result = persist_runtime_artifacts(base_dir, hourly, frame, stats_snapshot);
    let mut metrics = shared_metrics.lock().unwrap().clone();
    match persist_result {
        Ok(()) => {
            let persist_us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            metrics.persisted_frames += 1;
            metrics.last_persist_us = persist_us;
            metrics.max_persist_us = metrics.max_persist_us.max(persist_us);
            metrics.last_window_id = Some(frame.window_id);
            metrics.current_hour_file = hourly.current_hour_file();
            metrics.last_error = None;
            *shared_metrics.lock().unwrap() = metrics;
            Ok(())
        }
        Err(err) => {
            eprintln!("storage: persistence failed: {err}");
            metrics.last_error = Some(err.to_string());
            *shared_metrics.lock().unwrap() = metrics;
            Err(StorageError::WorkerFailed)
        }
    }
}

fn storage_worker_main(
    cfg: StorageConfig,
    rx: Receiver<StorageMsg>,
    shutdown: Arc<AtomicBool>,
    metrics: Arc<Mutex<StorageMetrics>>,
) {
    let (base_dir, frames_dir) = match ensure_dirs(&cfg.dir) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("storage: failed to create dirs: {e}");
            return;
        }
    };
    let mut hourly = HourlyWriter::new(frames_dir);

    // Keep processing until shutdown + channel drained.
    while !shutdown.load(Ordering::Relaxed) {
        let msg = match rx.recv() {
            Ok(m) => m,
            Err(_) => break,
        };

        match msg {
            StorageMsg::Persist(req) => ack_result(
                req.ack,
                persist_with_metrics(
                    &base_dir,
                    &mut hourly,
                    &req.frame,
                    req.stats_snapshot.as_ref(),
                    &metrics,
                ),
            ),
            StorageMsg::Shutdown => break,
        }
    }

    // Drain remaining frames after shutdown request (best-effort).
    while let Ok(msg) = rx.try_recv() {
        if let StorageMsg::Persist(req) = msg {
            ack_result(
                req.ack,
                persist_with_metrics(
                    &base_dir,
                    &mut hourly,
                    &req.frame,
                    req.stats_snapshot.as_ref(),
                    &metrics,
                ),
            );
        }
    }

    if cfg.sync_on_shutdown {
        hourly.sync_all();
        sync_snapshot_files(&base_dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        std::env::temp_dir().join(format!("project-azure-{name}-{nanos}"))
    }

    fn sample_frame(window_id: u64, window_end: SystemTime) -> AggregatedFrame {
        AggregatedFrame {
            window_id,
            window_start: window_end - Duration::from_secs(1),
            window_end,
            per_sensor: Default::default(),
            anomalies: Vec::new(),
        }
    }

    #[test]
    fn persist_frame_writes_latest_stats_and_snapshot() {
        let dir = temp_dir("storage-persist");
        let storage = StorageHandle::start(
            StorageConfig {
                dir: dir.clone(),
                ..StorageConfig::default()
            },
            2,
        );

        let frame = sample_frame(7, SystemTime::now());
        let stats = serde_json::json!({
            "window_id": frame.window_id,
            "sensors": 0,
            "anomalies": 0
        });

        storage
            .persist_frame(frame.clone(), Some(stats.clone()))
            .expect("persist should succeed");
        let metrics = storage.metrics();
        assert_eq!(metrics.persisted_frames, 1);
        assert_eq!(metrics.last_window_id, Some(frame.window_id));
        assert!(metrics.last_persist_us > 0);
        storage.shutdown();

        let latest: Value =
            serde_json::from_slice(&fs::read(dir.join("latest.json")).expect("latest exists")).unwrap();
        let latest_window = latest.get("window_id").and_then(Value::as_u64);
        assert_eq!(latest_window, Some(frame.window_id));

        let written_stats: Value =
            serde_json::from_slice(&fs::read(dir.join("stats.json")).expect("stats exists")).unwrap();
        assert_eq!(written_stats.get("window_id").and_then(Value::as_u64), Some(frame.window_id));

        let snapshot: Value =
            serde_json::from_slice(&fs::read(dir.join("snapshot.json")).expect("snapshot exists")).unwrap();
        assert_eq!(
            snapshot
                .get("latest")
                .and_then(|v| v.get("window_id"))
                .and_then(Value::as_u64),
            Some(frame.window_id)
        );
        assert_eq!(
            snapshot
                .get("stats")
                .and_then(|v| v.get("window_id"))
                .and_then(Value::as_u64),
            Some(frame.window_id)
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn hourly_log_is_atomically_rewritten_with_all_frames() {
        let dir = temp_dir("storage-hourly");
        let storage = StorageHandle::start(
            StorageConfig {
                dir: dir.clone(),
                ..StorageConfig::default()
            },
            2,
        );

        let window_end = SystemTime::now();
        storage
            .persist_frame(sample_frame(1, window_end), None)
            .expect("first persist should succeed");
        storage
            .persist_frame(sample_frame(2, window_end + Duration::from_secs(1)), None)
            .expect("second persist should succeed");
        let metrics = storage.metrics();
        assert_eq!(metrics.persisted_frames, 2);
        assert_eq!(metrics.current_hour_file, Some(hour_filename(hour_key_from_system_time(window_end).0)));
        storage.shutdown();

        let unix_hour = hour_key_from_system_time(window_end).0;
        let log = fs::read_to_string(dir.join("frames").join(hour_filename(unix_hour))).unwrap();
        assert_eq!(log.lines().count(), 2);
        assert!(log.contains("\"window_id\":1"));
        assert!(log.contains("\"window_id\":2"));

        let _ = fs::remove_dir_all(dir);
    }
}

