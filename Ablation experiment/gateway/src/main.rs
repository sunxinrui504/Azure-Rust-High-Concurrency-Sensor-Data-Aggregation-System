use std::env;

mod ablation {
    use std::sync::atomic::AtomicUsize;

    
    pub static SENSOR_OVERFLOW_EVENTS: AtomicUsize = AtomicUsize::new(0);
    pub static SENSOR_LOST_READINGS: AtomicUsize = AtomicUsize::new(0);
    
    pub fn record_overflow_event() {
        SENSOR_OVERFLOW_EVENTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// 记录丢失的读数数量
    pub fn add_lost_readings(count: usize) {
        SENSOR_LOST_READINGS.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }


    pub fn get_overflow_threshold(sensor_id: &str) -> usize {
        0
    }
}

use sensor_sim::{
    accelerometer::Accelerometer,
    force_sensor::ForceSensor,
    thermometer::Thermometer,
    traits::Sensor,
};

use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use gateway::{
    aggregation::{AggregationConfig, AggregationEngine},
    buffer::SensorBufferManager,
    governor::{AdaptivePollingConfig, AdaptivePollingGovernor, GovernorOverride, GovernorStats},
    storage::{StorageConfig, StorageHandle, StorageMetrics},
    types::{AggregatedFrame, ReadingEnvelope, SensorStats},
};

// 导入Web Server依赖
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Json, Response},
    routing::get,
    Router,
};
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tracing::{error, info};
use tracing_subscriber;

const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

// 教授要求的传感器文件写入
fn write_sensor_reading_to_file(sensor_id: &str, reading: &Value) -> std::io::Result<()> {
    let data_dir = PathBuf::from("data");
    fs::create_dir_all(&data_dir)?;
    
    let file_path = data_dir.join(format!("{}.txt", sensor_id));
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)?;
    
    writeln!(file, "{}", serde_json::to_string(reading)?)?;
    Ok(())
}

// 教授要求的传感器文件读取
fn read_sensor_file(sensor_id: &str) -> std::io::Result<Vec<Value>> {
    let file_path = PathBuf::from("data").join(format!("{}.txt", sensor_id));
    if !file_path.exists() {
        return Ok(Vec::new());
    }
    
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut readings = Vec::new();
    
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(&line) {
            Ok(value) => readings.push(value),
            Err(e) => eprintln!("Failed to parse line from {}: {}", sensor_id, e),
        }
    }
    
    Ok(readings)
}

fn magnitude3(x: f32, y: f32, z: f32) -> f64 {
    let (x, y, z) = (x as f64, y as f64, z as f64);
    (x * x + y * y + z * z).sqrt()
}

fn update_max(a: &AtomicUsize, v: usize) {
    let mut cur = a.load(Ordering::Relaxed);
    while v > cur {
        match a.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => cur = next,
        }
    }
}

fn elapsed_ms_since(now: SystemTime, earlier: SystemTime) -> u64 {
    now.duration_since(earlier)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
enum ReaderControlMode {
    Automatic,
    ClampToNominal,
    ForceFastRecovery,
    ShutdownDrain,
}

#[derive(Debug, Clone)]
struct ReaderCommand {
    mode: ReaderControlMode,
}

#[derive(Debug, Clone)]
struct ReaderStatus {
    sensor_id: String,
    observed_at: SystemTime,
    last_successful_read: SystemTime,
    available: usize,
    total_reads: u64,
    is_draining: bool,
    governor: GovernorStats,
    buffer_fill_ratio: f64,
    push_wait_count: u64,
    pop_wait_count: u64,
}

#[derive(Debug, Clone, Serialize)]
struct SchedulerReaderSnapshot {
    last_available: usize,
    total_reads: u64,
    consecutive_warning_hits: u32,
    consecutive_critical_hits: u32,
    last_command: ReaderControlMode,
    last_governor_mode: gateway::governor::GovernorMode,
    last_governor_sleep_us: u64,
    last_buffer_fill_ratio: f64,
    read_idle_ms: u64,
    last_push_wait_count: u64,
    last_pop_wait_count: u64,
    reader_stale_ms: u64,
    stalled: bool,
    draining: bool,
}

impl Default for SchedulerReaderSnapshot {
    fn default() -> Self {
        Self {
            last_available: 0,
            total_reads: 0,
            consecutive_warning_hits: 0,
            consecutive_critical_hits: 0,
            last_command: ReaderControlMode::Automatic,
            last_governor_mode: gateway::governor::GovernorMode::Tracking,
            last_governor_sleep_us: 0,
            last_buffer_fill_ratio: 0.0,
            read_idle_ms: 0,
            last_push_wait_count: 0,
            last_pop_wait_count: 0,
            reader_stale_ms: 0,
            stalled: false,
            draining: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
struct SchedulerStats {
    poll_interval_ms: u64,
    warning_threshold: usize,
    critical_threshold: usize,
    warning_consecutive_limit: u32,
    stale_reader_ms: u64,
    readers: BTreeMap<String, SchedulerReaderSnapshot>,
}

#[derive(Debug, Clone, Copy)]
struct SchedulerConfig {
    poll_interval: Duration,
    warning_threshold: usize,
    critical_threshold: usize,
    warning_consecutive_limit: u32,
    stale_reader_timeout: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(50),
            warning_threshold: 16,
            critical_threshold: 24,
            warning_consecutive_limit: 3,
            stale_reader_timeout: Duration::from_millis(500),
        }
    }
}

fn spawn_scheduler(
    stop: Arc<AtomicBool>,
    status_rx: Receiver<ReaderStatus>,
    command_txs: HashMap<String, Sender<ReaderCommand>>,
    shared_stats: Arc<Mutex<SchedulerStats>>,
    cfg: SchedulerConfig,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let ids: Vec<String> = command_txs.keys().cloned().collect();
        let mut latest_status = HashMap::<String, ReaderStatus>::new();
        let mut warning_hits = HashMap::<String, u32>::new();
        let mut critical_hits = HashMap::<String, u32>::new();
        let mut last_command = HashMap::<String, ReaderControlMode>::new();

        while !stop.load(Ordering::Relaxed) {
            while let Ok(status) = status_rx.try_recv() {
                latest_status.insert(status.sensor_id.clone(), status);
            }

            let now = SystemTime::now();
            let mut snapshot = SchedulerStats {
                poll_interval_ms: cfg.poll_interval.as_millis() as u64,
                warning_threshold: cfg.warning_threshold,
                critical_threshold: cfg.critical_threshold,
                warning_consecutive_limit: cfg.warning_consecutive_limit,
                stale_reader_ms: cfg.stale_reader_timeout.as_millis() as u64,
                readers: BTreeMap::new(),
            };

            for id in &ids {
                let Some(status) = latest_status.get(id) else {
                    snapshot
                        .readers
                        .insert(id.clone(), SchedulerReaderSnapshot::default());
                    continue;
                };

                let warn = if status.available >= cfg.warning_threshold {
                    warning_hits.get(id).copied().unwrap_or(0).saturating_add(1)
                } else {
                    0
                };
                warning_hits.insert(id.clone(), warn);

                let crit = if status.available >= cfg.critical_threshold {
                    critical_hits.get(id).copied().unwrap_or(0).saturating_add(1)
                } else {
                    0
                };
                critical_hits.insert(id.clone(), crit);

                let reader_stale_ms = elapsed_ms_since(now, status.observed_at);
                let stalled = reader_stale_ms >= cfg.stale_reader_timeout.as_millis() as u64;
                let desired = if stalled || crit > 0 {
                    ReaderControlMode::ForceFastRecovery
                } else if warn >= cfg.warning_consecutive_limit || status.buffer_fill_ratio >= 0.20 {
                    ReaderControlMode::ClampToNominal
                } else {
                    ReaderControlMode::Automatic
                };

                if last_command.get(id).copied() != Some(desired) {
                    if let Some(tx) = command_txs.get(id) {
                        let _ = tx.send(ReaderCommand { mode: desired });
                    }
                    last_command.insert(id.clone(), desired);
                }

                snapshot.readers.insert(
                    id.clone(),
                    SchedulerReaderSnapshot {
                        last_available: status.available,
                        total_reads: status.total_reads,
                        consecutive_warning_hits: warn,
                        consecutive_critical_hits: crit,
                        last_command: desired,
                        last_governor_mode: status.governor.mode,
                        last_governor_sleep_us: status.governor.current_sleep_us,
                        last_buffer_fill_ratio: status.buffer_fill_ratio,
                        read_idle_ms: elapsed_ms_since(now, status.last_successful_read),
                        last_push_wait_count: status.push_wait_count,
                        last_pop_wait_count: status.pop_wait_count,
                        reader_stale_ms,
                        stalled,
                        draining: status.is_draining,
                    },
                );
            }

            *shared_stats.lock().unwrap() = snapshot;
            thread::sleep(cfg.poll_interval);
        }

        for tx in command_txs.values() {
            let _ = tx.send(ReaderCommand {
                mode: ReaderControlMode::ShutdownDrain,
            });
        }

        while let Ok(status) = status_rx.try_recv() {
            latest_status.insert(status.sensor_id.clone(), status);
        }

        let now = SystemTime::now();
        let mut snapshot = SchedulerStats {
            poll_interval_ms: cfg.poll_interval.as_millis() as u64,
            warning_threshold: cfg.warning_threshold,
            critical_threshold: cfg.critical_threshold,
            warning_consecutive_limit: cfg.warning_consecutive_limit,
            stale_reader_ms: cfg.stale_reader_timeout.as_millis() as u64,
            readers: BTreeMap::new(),
        };
        for id in ids {
            let mut reader = SchedulerReaderSnapshot::default();
            if let Some(status) = latest_status.get(&id) {
                reader.last_available = status.available;
                reader.total_reads = status.total_reads;
                reader.consecutive_warning_hits = warning_hits.get(&id).copied().unwrap_or(0);
                reader.consecutive_critical_hits = critical_hits.get(&id).copied().unwrap_or(0);
                reader.last_governor_mode = status.governor.mode;
                reader.last_governor_sleep_us = status.governor.current_sleep_us;
                reader.last_buffer_fill_ratio = status.buffer_fill_ratio;
                reader.read_idle_ms = elapsed_ms_since(now, status.last_successful_read);
                reader.last_push_wait_count = status.push_wait_count;
                reader.last_pop_wait_count = status.pop_wait_count;
                reader.reader_stale_ms = elapsed_ms_since(now, status.observed_at);
                reader.draining = true;
            }
            reader.last_command = ReaderControlMode::ShutdownDrain;
            snapshot.readers.insert(id, reader);
        }
        *shared_stats.lock().unwrap() = snapshot;
    })
}

fn spawn_reader<S: Sensor + Send + 'static>(
    mut sensor: S,
    value_fn: fn(S::SensorReading) -> f64,
    buf: Arc<SensorBufferManager<ReadingEnvelope>>,
    stop: Arc<AtomicBool>,
    max_avail: Arc<AtomicUsize>,
    governor_cfg: AdaptivePollingConfig,
    governor_stats: Arc<Mutex<GovernorStats>>,
    command_rx: Receiver<ReaderCommand>,
    status_tx: Sender<ReaderStatus>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let sid = sensor.id();
        let mut governor = AdaptivePollingGovernor::new(governor_cfg);
        let mut total_reads: u64 = 0;
        let mut last_successful_read = SystemTime::now();
        let mut control_mode = ReaderControlMode::Automatic;
        let mut governor_override = GovernorOverride::None;
        let mut draining = false;
        let mut drain_deadline = None::<Instant>;
        let mut sensor_stopped = false;

        loop {
            while let Ok(cmd) = command_rx.try_recv() {
                control_mode = cmd.mode;
                match cmd.mode {
                    ReaderControlMode::Automatic => governor_override = GovernorOverride::None,
                    ReaderControlMode::ClampToNominal => {
                        governor_override =
                            GovernorOverride::ClampMaxSleep(governor_cfg.nominal_sleep_us.max(governor_cfg.min_sleep_us));
                    }
                    ReaderControlMode::ForceFastRecovery => {
                        governor_override = GovernorOverride::ForceFastRecovery;
                    }
                    ReaderControlMode::ShutdownDrain => {
                        stop.store(true, Ordering::Relaxed);
                        draining = true;
                        governor_override = GovernorOverride::ForceFastRecovery;
                        if !sensor_stopped {
                            sensor.stop();
                            sensor_stopped = true;
                        }
                        drain_deadline.get_or_insert_with(|| Instant::now() + DRAIN_TIMEOUT);
                    }
                }
            }

            if stop.load(Ordering::Relaxed) && !draining {
                draining = true;
                control_mode = ReaderControlMode::ShutdownDrain;
                governor_override = GovernorOverride::ForceFastRecovery;
                if !sensor_stopped {
                    sensor.stop();
                    sensor_stopped = true;
                }
                drain_deadline.get_or_insert_with(|| Instant::now() + DRAIN_TIMEOUT);
            }

            let available_before = sensor.available();
            let sensor_id = sensor.id();
            let overflow_threshold = ablation::get_overflow_threshold(&sensor_id);
            if available_before >= overflow_threshold {
                ablation::record_overflow_event();
                let lost = available_before.saturating_sub(overflow_threshold);
                if lost > 0 {
                    ablation::add_lost_readings(lost);
                    println!(
                        "Sensor overflow: {} (available={}, lost approx {})",
                        sensor_id, available_before, lost
                    );
                } else {
                    println!("Sensor overflow: {} (available={})", sensor_id, available_before);
                }
            }

            update_max(&max_avail, available_before);
            if total_reads % 1000 == 0 {
                println!("[{}] avail_before={}, buffer_len={}", sid, available_before, buf.len());
            }

            while let Some(r) = sensor.read() {
                total_reads += 1;
                last_successful_read = SystemTime::now();
                
                // 先计算 value，避免所有权问题
                let sensor_value = value_fn(r);
                
                // 教授要求：写入传感器数据文件
                let reading_value = serde_json::json!({
                    "sensor_id": sid.clone(),
                    "timestamp": SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs_f64(),
                    "value": sensor_value,  // 使用保存的值
                });
                
                // 写入文件（满足教授要求）
                if let Err(e) = write_sensor_reading_to_file(&sid, &reading_value) {
                    eprintln!("Failed to write sensor {} data to file: {}", sid, e);
                }
                
                // 原有功能：推送到缓冲区
                if buf
                    .push(ReadingEnvelope::new(
                        sid.clone(),
                        SystemTime::now(),
                        sensor_value,  // 使用保存的值
                    ))
                    .is_err()
                {
                    stop.store(true, Ordering::Relaxed);
                    draining = true;
                    control_mode = ReaderControlMode::ShutdownDrain;
                    governor_override = GovernorOverride::ForceFastRecovery;
                    if !sensor_stopped {
                        sensor.stop();
                        sensor_stopped = true;
                    }
                    drain_deadline.get_or_insert_with(|| Instant::now() + DRAIN_TIMEOUT);
                    break;
                }
            }
            thread::sleep(Duration::from_micros(10));

            let available_after = sensor.available();
            update_max(&max_avail, available_after);
            let bstats = buf.stats();
            let fill_ratio = bstats.fill_ratio();
            let sleep_for = governor.update_with_override(available_after, &bstats, governor_override);
            let stats = governor.stats();
            *governor_stats.lock().unwrap() = stats.clone();
            let _ = status_tx.send(ReaderStatus {
                sensor_id: sid.clone(),
                observed_at: SystemTime::now(),
                last_successful_read,
                available: available_after,
                total_reads,
                is_draining: draining,
                governor: stats,
                buffer_fill_ratio: fill_ratio,
                push_wait_count: bstats.push_wait_count,
                pop_wait_count: bstats.pop_wait_count,
            });

            if draining {
                let timed_out = drain_deadline
                    .map(|deadline| Instant::now() >= deadline)
                    .unwrap_or(false);
                if available_after == 0 || timed_out {
                    break;
                }
            }

            if !sleep_for.is_zero() {
                thread::sleep(sleep_for);
            } else {
                thread::yield_now();
            }
        }

        if !sensor_stopped {
            sensor.stop();
        }

        let final_available = sensor.available();
        let stats = governor.stats();
        *governor_stats.lock().unwrap() = stats.clone();
        let _ = status_tx.send(ReaderStatus {
            sensor_id: sid,
            observed_at: SystemTime::now(),
            last_successful_read,
            available: final_available,
            total_reads,
            is_draining: true,
            governor: stats,
            buffer_fill_ratio: buf.stats().fill_ratio(),
            push_wait_count: buf.stats().push_wait_count,
            pop_wait_count: buf.stats().pop_wait_count,
        });
        let _ = control_mode;
    })
}

async fn wait_for_shutdown_signal(stop: Arc<AtomicBool>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        
        let mut terminate = signal(SignalKind::terminate()).expect("sigterm handler");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    }
    
    stop.store(true, Ordering::Relaxed);
}

fn build_stats_snapshot(
    frame: &AggregatedFrame,
    buffer: &SensorBufferManager<ReadingEnvelope>,
    max_t1: &AtomicUsize,
    max_t2: &AtomicUsize,
    max_a1: &AtomicUsize,
    max_a2: &AtomicUsize,
    max_f1: &AtomicUsize,
    gov_t1: &Mutex<GovernorStats>,
    gov_t2: &Mutex<GovernorStats>,
    gov_a1: &Mutex<GovernorStats>,
    gov_a2: &Mutex<GovernorStats>,
    gov_f1: &Mutex<GovernorStats>,
    scheduler_stats: &Mutex<SchedulerStats>,
    storage_metrics: &StorageMetrics,
) -> serde_json::Value {
    let bstats = buffer.stats();
    serde_json::json!({
        "window_id": frame.window_id,
        "sensors": frame.per_sensor.len(),
        "anomalies": frame.anomalies.len(),
        "buffer": bstats,
        "sensor_max_available": {
            "thermo-1": max_t1.load(Ordering::Relaxed),
            "thermo-2": max_t2.load(Ordering::Relaxed),
            "accel-1": max_a1.load(Ordering::Relaxed),
            "accel-2": max_a2.load(Ordering::Relaxed),
            "force-1": max_f1.load(Ordering::Relaxed),
        },
        "governor": {
            "thermo-1": gov_t1.lock().unwrap().clone(),
            "thermo-2": gov_t2.lock().unwrap().clone(),
            "accel-1": gov_a1.lock().unwrap().clone(),
            "accel-2": gov_a2.lock().unwrap().clone(),
            "force-1": gov_f1.lock().unwrap().clone(),
        },
        "scheduler": scheduler_stats.lock().unwrap().clone(),
        "storage": storage_metrics,
    })
}

// ================ Web Server 相关定义 ================

/// Web Server 应用状态
#[derive(Clone)]
struct AppState {
    data_dir: PathBuf,
    stats: Arc<RwLock<Option<WebStats>>>,
}

/// Web Server 统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebStats {
    total_frames: usize,
    latest_window_id: u64,
    latest_window_end: SystemTime,
    sensor_count: usize,
    anomaly_count: usize,
    last_updated: SystemTime,
}

/// 错误定义
#[derive(thiserror::Error, Debug)]
enum WebAppError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("File not found: {0}")]
    FileNotFound(String),
    
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
    
    #[error("Internal server error")]
    Internal,
}

impl IntoResponse for WebAppError {
    fn into_response(self) -> axum::response::Response {
        let status = match self {
            WebAppError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            WebAppError::Json(_) => StatusCode::BAD_REQUEST,
            WebAppError::FileNotFound(_) => StatusCode::NOT_FOUND,
            WebAppError::InvalidParameter(_) => StatusCode::BAD_REQUEST,
            WebAppError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        
        let body = format!("Error: {}", self);
        (status, body).into_response()
    }
}

/// 读取传感器原始数据（教授要求的文件格式）
async fn read_raw_sensor_data(_data_dir: &PathBuf) -> Result<HashMap<String, Vec<Value>>, WebAppError> {
    let sensors = ["thermo-1", "thermo-2", "accel-1", "accel-2", "force-1"];
    let mut all_data = HashMap::new();
    
    for sensor in sensors {
        match read_sensor_file(sensor) {
            Ok(readings) => {
                all_data.insert(sensor.to_string(), readings);
            }
            Err(e) => {
                eprintln!("Warning: Failed to read sensor {} data: {}", sensor, e);
                all_data.insert(sensor.to_string(), Vec::new());
            }
        }
    }
    
    Ok(all_data)
}

/// 从传感器原始数据实时聚合（演示教授要求的功能）
fn aggregate_from_raw_data(sensor_data: &HashMap<String, Vec<Value>>) -> AggregatedFrame {
    let mut per_sensor = HashMap::new();
    let anomalies = Vec::new();
    
    for (sensor_id, readings) in sensor_data {
        let mut stats = SensorStats::default();
        
        for reading in readings {
            if let Some(value) = reading.get("value").and_then(Value::as_f64) {
                stats.update(value);
            }
        }
        
        per_sensor.insert(sensor_id.clone(), stats);
    }
    
    AggregatedFrame {
        window_id: 1, // 演示用
        window_start: SystemTime::now() - Duration::from_secs(1),
        window_end: SystemTime::now(),
        per_sensor,
        anomalies,
    }
}

/// 根路径处理器 - 返回HTML仪表盘
async fn root_handler() -> Result<Html<String>, WebAppError> {
    let html = r#"
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Project Azure - Sensor Dashboard</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                color: #333;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                padding: 2rem;
            }
            header {
                text-align: center;
                margin-bottom: 2rem;
                color: white;
            }
            h1 { 
                font-size: 2.5rem; 
                margin-bottom: 0.5rem;
                text-shadow: 0 2px 4px rgba(0,0,0,0.2);
            }
            .subtitle {
                font-size: 1.1rem;
                opacity: 0.9;
            }
            .dashboard {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 1.5rem;
                margin-top: 2rem;
            }
            .card {
                background: white;
                border-radius: 12px;
                padding: 1.5rem;
                box-shadow: 0 10px 20px rgba(0,0,0,0.1);
                transition: transform 0.3s ease;
            }
            .card:hover {
                transform: translateY(-5px);
            }
            .card h2 {
                color: #4f46e5;
                margin-bottom: 1rem;
                font-size: 1.3rem;
            }
            .stats-grid {
                display: grid;
                grid-template-columns: repeat(2, 1fr);
                gap: 1rem;
            }
            .stat-item {
                padding: 0.8rem;
                background: #f8fafc;
                border-radius: 8px;
                border-left: 4px solid #4f46e5;
            }
            .stat-label {
                font-size: 0.9rem;
                color: #64748b;
                display: block;
            }
            .stat-value {
                font-size: 1.2rem;
                font-weight: bold;
                color: #1e293b;
            }
            .api-endpoints {
                margin-top: 2rem;
                background: rgba(255,255,255,0.1);
                padding: 1.5rem;
                border-radius: 10px;
                color: white;
            }
            .api-endpoints h3 {
                margin-bottom: 1rem;
            }
            .endpoint {
                background: rgba(255,255,255,0.2);
                padding: 0.8rem;
                border-radius: 6px;
                margin: 0.5rem 0;
                font-family: monospace;
                word-break: break-all;
            }
            .sensor-files {
                background: rgba(255,255,255,0.1);
                padding: 1.5rem;
                border-radius: 10px;
                color: white;
                margin-top: 1rem;
            }
            footer {
                text-align: center;
                margin-top: 3rem;
                color: rgba(255,255,255,0.7);
                font-size: 0.9rem;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>Project Azure</h1>
                <p class="subtitle">Distributed Sensor Data Aggregation Platform - Real-time Dashboard</p>
                <p style="color: #c7d2fe; font-size: 0.9rem;">✓ Adapted for file-based IPC (Professor's requirement)</p>
            </header>
            
            <div class="dashboard">
                <div class="card">
                    <h2>📊 System Overview</h2>
                    <div id="system-stats" class="stats-grid">
                        <div class="stat-item">
                            <span class="stat-label">Total Frames</span>
                            <span id="total-frames" class="stat-value">Loading...</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">Latest Window ID</span>
                            <span id="latest-window" class="stat-value">Loading...</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">Sensors</span>
                            <span id="sensor-count" class="stat-value">Loading...</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">Anomalies</span>
                            <span id="anomaly-count" class="stat-value">Loading...</span>
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <h2>🔍 Latest Anomalies</h2>
                    <div id="anomalies-list">
                        <p>No anomalies detected</p>
                    </div>
                </div>
                
                <div class="card">
                    <h2>📡 Sensor Status</h2>
                    <div id="sensor-status">
                        <p>Loading sensor data...</p>
                    </div>
                </div>
            </div>
            
            <div class="api-endpoints">
                <h3>🛠️ Available API Endpoints</h3>
                <div class="endpoint">GET /api/frames/latest - Latest aggregated frames</div>
                <div class="endpoint">GET /api/frames/window/{id} - Frame by window ID</div>
                <div class="endpoint">GET /api/frames/sensor/{id} - Frames for specific sensor</div>
                <div class="endpoint">GET /api/frames/range?start=...&end=... - Frames in time range</div>
                <div class="endpoint">GET /api/stats - System statistics</div>
                <div class="endpoint">GET /api/raw-sensor/{id} - Raw sensor data from file (Professor's requirement)</div>
                <div class="endpoint">GET /api/health - Health check</div>
            </div>
            
            <div class="sensor-files">
                <h3>📁 Sensor Data Files (File-based IPC)</h3>
                <p>Sensor data is written to files in <code>data/</code> directory:</p>
                <ul id="sensor-files-list" style="list-style: none; padding: 0;">
                    <li>thermo-1.txt ✓</li>
                    <li>thermo-2.txt ✓</li>
                    <li>accel-1.txt ✓</li>
                    <li>accel-2.txt ✓</li>
                    <li>force-1.txt ✓</li>
                </ul>
            </div>
            
            <footer>
                <p>© 2026 COMP2432 - Operating Systems | Hong Kong Polytechnic University</p>
                <p>Project Azure | Web Server v1.0 | File-based IPC Enabled</p>
            </footer>
        </div>
        
        <script>
            // 获取统计信息
            async function loadStats() {
                try {
                    const response = await fetch('/api/stats', { cache: 'no-store' });
                    const data = await response.json();
                    
                    document.getElementById('total-frames').textContent = data.total_frames;
                    document.getElementById('latest-window').textContent = data.latest_window_id;
                    document.getElementById('sensor-count').textContent = data.sensor_count;
                    document.getElementById('anomaly-count').textContent = data.anomaly_count;
                    
                } catch (error) {
                    console.error('Failed to load stats:', error);
                }
            }
            
            // 获取最新异常
            async function loadAnomalies() {
                try {
                    const response = await fetch('/api/frames/latest?limit=5', { cache: 'no-store' });
                    const frames = await response.json();
                    
                    const anomalies = frames.flatMap(frame => 
                        (frame.anomalies || []).map(anomaly => ({
                            ...anomaly,
                            window_id: frame.window_id
                        }))
                    );
                    
                    const container = document.getElementById('anomalies-list');
                    if (anomalies.length > 0) {
                        container.innerHTML = anomalies.slice(0, 5).map(a => 
                            `<div style="margin-bottom: 0.5rem; padding: 0.5rem; background: #fef2f2; border-radius: 4px; border-left: 3px solid #dc2626;">
                                <strong>Window ${a.window_id}</strong>: ${a.message || 'Unknown anomaly'}
                                <small style="display: block; color: #666;">Sensor: ${a.sensor_id}</small>
                            </div>`
                        ).join('');
                    } else {
                        container.innerHTML = '<p style="color: #666;">✅ No anomalies detected in recent data</p>';
                    }
                    
                } catch (error) {
                    console.error('Failed to load anomalies:', error);
                }
            }
            
            async function loadSensorStatus() {
                const el = document.getElementById('sensor-status');
                try {
                    const response = await fetch('/api/frames/latest', { cache: 'no-store' });
                    const frames = await response.json();
                    if (!frames.length) {
                        el.innerHTML = '<p>No aggregated frames yet — wait for the first 1s window after starting the gateway.</p>';
                        return;
                    }
                    const latest = frames[0];
                    const keys = latest.per_sensor ? Object.keys(latest.per_sensor) : [];
                    if (keys.length === 0) {
                        el.innerHTML = '<p>No per-sensor keys in the latest frame.</p>';
                    } else {
                        el.innerHTML = '<ul style="margin:0;padding-left:1.2rem;">' +
                            keys.map(k => `<li><strong>${k}</strong></li>`).join('') + '</ul>';
                    }
                } catch (error) {
                    el.innerHTML = '<p>Could not load sensor list (see console).</p>';
                    console.error('Failed to load sensor status:', error);
                }
            }
            
            // 检查传感器文件
            async function checkSensorFiles() {
                const sensors = ['thermo-1', 'thermo-2', 'accel-1', 'accel-2', 'force-1'];
                const listEl = document.getElementById('sensor-files-list');
                
                for (const sensor of sensors) {
                    try {
                        const response = await fetch(`/api/raw-sensor/${sensor}?limit=1`);
                        if (response.ok) {
                            const data = await response.json();
                            const li = listEl.querySelector(`li:contains(${sensor}.txt)`);
                            if (li) {
                                li.innerHTML = `${sensor}.txt ✓ (${data.data.length} readings)`;
                            }
                        }
                    } catch (error) {
                        console.error(`Failed to check sensor ${sensor}:`, error);
                    }
                }
            }
            
            // 页面加载时初始化
            document.addEventListener('DOMContentLoaded', () => {
                loadStats();
                loadAnomalies();
                loadSensorStatus();
                checkSensorFiles();
                
                // 每10秒刷新数据
                setInterval(() => {
                    loadStats();
                    loadAnomalies();
                    loadSensorStatus();
                }, 10000);
                
                // 每30秒检查传感器文件
                setInterval(checkSensorFiles, 30000);
            });
        </script>
    </body>
    </html>
    "#;
    
    Ok(Html(html.to_string()))
}

/// 获取最新帧（禁用浏览器缓存，避免刷新仍显示旧列表）
async fn get_latest_frames(State(state): State<AppState>) -> Result<Response, WebAppError> {
    let frames = load_latest_frames(&state.data_dir, 20).await?;
    let mut res = Json(frames).into_response();
    res.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    Ok(res)
}

/// 按窗口ID获取帧
async fn get_frame_by_window_id(
    State(state): State<AppState>,
    Path(window_id): Path<u64>,
) -> Result<Json<Option<AggregatedFrame>>, WebAppError> {
    let frame = find_frame_by_window_id(&state.data_dir, window_id).await?;
    Ok(Json(frame))
}

/// 按传感器ID获取帧
async fn get_frames_by_sensor(
    State(state): State<AppState>,
    Path(sensor_id): Path<String>,
) -> Result<Json<Vec<AggregatedFrame>>, WebAppError> {
    let frames = find_frames_by_sensor(&state.data_dir, &sensor_id).await?;
    Ok(Json(frames))
}

/// 获取时间范围内的帧
async fn get_frames_in_range(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Vec<AggregatedFrame>>, WebAppError> {
    let start = params.get("start").and_then(|s| s.parse::<u64>().ok());
    let end = params.get("end").and_then(|s| s.parse::<u64>().ok());
    let limit = params.get("limit").and_then(|s| s.parse::<usize>().ok()).unwrap_or(100);
    
    let frames = find_frames_in_range(&state.data_dir, start, end, limit).await?;
    Ok(Json(frames))
}

/// 获取原始传感器数据（教授要求的文件格式）
async fn get_raw_sensor_data(
    Path(sensor_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, WebAppError> {
    let limit = params.get("limit").and_then(|s| s.parse::<usize>().ok()).unwrap_or(100);
    
    match read_sensor_file(&sensor_id) {
        Ok(mut data) => {
            if data.len() > limit {
                data.truncate(limit);
            }
            Ok(Json(serde_json::json!({
                "sensor_id": sensor_id,
                "count": data.len(),
                "data": data
            })))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(Json(serde_json::json!({
                "sensor_id": sensor_id,
                "error": "Sensor data file not found",
                "count": 0,
                "data": []
            })))
        }
        Err(e) => Err(WebAppError::Io(e)),
    }
}

/// 从传感器文件实时聚合（演示功能）
async fn get_realtime_aggregated(State(state): State<AppState>) -> Result<Json<AggregatedFrame>, WebAppError> {
    let sensor_data = read_raw_sensor_data(&state.data_dir).await?;
    let aggregated = aggregate_from_raw_data(&sensor_data);
    Ok(Json(aggregated))
}

/// 获取系统统计信息（每次请求重算，避免首次写入后永久返回旧缓存）
async fn get_stats(State(state): State<AppState>) -> Result<Response, WebAppError> {
    let computed_stats: WebStats = compute_stats(&state.data_dir).await?;
    *state.stats.write().await = Some(computed_stats.clone());
    let mut res = Json(computed_stats).into_response();
    res.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    Ok(res)
}

/// 健康检查端点
async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

// ================ 文件读取辅助函数 ================

/// 安全读取JSONL文件
async fn read_jsonl_file<P: AsRef<std::path::Path>>(path: P) -> Result<Vec<AggregatedFrame>, WebAppError> {
    let content = tokio::fs::read_to_string(path).await?;
    let mut frames = Vec::new();
    
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let frame: AggregatedFrame = serde_json::from_str(line)?;
        frames.push(frame);
    }
    
    Ok(frames)
}

/// Collect `*.jsonl` in one directory (non-recursive). Storage writes under `data/frames/hour-*.jsonl`.
fn collect_jsonl_in_dir(dir: &std::path::Path, files: &mut Vec<PathBuf>) -> Result<(), WebAppError> {
    use std::fs;
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "jsonl")
            && !path.to_string_lossy().contains(".tmp")
        {
            files.push(path);
        }
    }
    Ok(())
}

/// 获取所有数据文件（`data/*.jsonl` 以及 `data/frames/*.jsonl`）
fn get_data_files(data_dir: &std::path::Path) -> Result<Vec<PathBuf>, WebAppError> {
    let mut files = Vec::new();
    collect_jsonl_in_dir(data_dir, &mut files)?;
    collect_jsonl_in_dir(&data_dir.join("frames"), &mut files)?;
    files.sort();
    files.reverse();
    Ok(files)
}

/// 加载最新帧
async fn load_latest_frames(data_dir: &std::path::Path, limit: usize) -> Result<Vec<AggregatedFrame>, WebAppError> {
    let files = get_data_files(data_dir)?;
    let mut all_frames = Vec::new();
    
    for file in files {
        if all_frames.len() >= limit {
            break;
        }
        
        let frames: Vec<AggregatedFrame> = read_jsonl_file(&file).await?;
        all_frames.extend(frames);
        
        if all_frames.len() > limit {
            all_frames.truncate(limit);
        }
    }
    
    Ok(all_frames)
}

/// 按窗口ID查找帧
async fn find_frame_by_window_id(data_dir: &std::path::Path, window_id: u64) -> Result<Option<AggregatedFrame>, WebAppError> {
    let files = get_data_files(data_dir)?;
    
    for file in files {
        let frames: Vec<AggregatedFrame> = read_jsonl_file(&file).await?;
        for frame in frames {
            if frame.window_id == window_id {
                return Ok(Some(frame));
            }
        }
    }
    
    Ok(None)
}

/// 按传感器ID查找帧
async fn find_frames_by_sensor(data_dir: &std::path::Path, sensor_id: &str) -> Result<Vec<AggregatedFrame>, WebAppError> {
    let files = get_data_files(data_dir)?;
    let mut result = Vec::new();
    
    for file in files {
        let frames: Vec<AggregatedFrame> = read_jsonl_file(&file).await?;
        for frame in frames {
            if frame.per_sensor.contains_key(sensor_id) {
                result.push(frame);
            }
        }
    }
    
    Ok(result)
}

/// 查找时间范围内的帧
async fn find_frames_in_range(
    data_dir: &std::path::Path,
    start: Option<u64>,
    end: Option<u64>,
    limit: usize,
) -> Result<Vec<AggregatedFrame>, WebAppError> {
    let files = get_data_files(data_dir)?;
    let mut result = Vec::new();
    
    for file in files {
        if result.len() >= limit {
            break;
        }
        
        let frames: Vec<AggregatedFrame> = read_jsonl_file(&file).await?;
        for frame in frames {
            let window_start = frame.window_start
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            
            // 检查时间范围
            let in_range = match (start, end) {
                (Some(s), Some(e)) => window_start >= s && window_start <= e,
                (Some(s), None) => window_start >= s,
                (None, Some(e)) => window_start <= e,
                (None, None) => true,
            };
            
            if in_range {
                result.push(frame);
                if result.len() >= limit {
                    break;
                }
            }
        }
    }
    
    Ok(result)
}

/// 计算统计信息
async fn compute_stats(data_dir: &std::path::Path) -> Result<WebStats, WebAppError> {
    use std::collections::HashSet;
    
    let files = get_data_files(data_dir)?;
    let mut total_frames = 0;
    let mut sensor_set: HashSet<String> = HashSet::new();
    let mut anomaly_count = 0;
    let mut latest_window_id = 0;
    let mut latest_window_end = std::time::UNIX_EPOCH;
    
    // 读取每个文件统计
    for file in files.iter().take(10) { // 限制最近10个文件以提高性能
        let frames: Vec<AggregatedFrame> = read_jsonl_file(file).await?;
        total_frames += frames.len();
        
        for frame in frames {
            if frame.window_id > latest_window_id {
                latest_window_id = frame.window_id;
                latest_window_end = frame.window_end;
            }
            
            for sensor_id in frame.per_sensor.keys() {
                sensor_set.insert(sensor_id.clone());
            }
            
            anomaly_count += frame.anomalies.len();
        }
    }
    
    Ok(WebStats {
        total_frames,
        latest_window_id,
        latest_window_end,
        sensor_count: sensor_set.len(),
        anomaly_count,
        last_updated: SystemTime::now(),
    })
}

/// 启动Web Server
async fn start_web_server(data_dir: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    // 初始化状态
    let state = AppState {
        data_dir: data_dir.clone(),
        stats: Arc::new(RwLock::new(None)),
    };

    // 启动后台任务更新统计缓存
    let stats_updater_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            match compute_stats(&stats_updater_state.data_dir).await {
                Ok(s) => *stats_updater_state.stats.write().await = Some(s),
                Err(e) => error!("Failed to update stats cache: {}", e),
            }
        }
    });

    // 构建路由
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/api/frames/latest", get(get_latest_frames))
        .route("/api/frames/window/:window_id", get(get_frame_by_window_id))
        .route("/api/frames/sensor/:sensor_id", get(get_frames_by_sensor))
        .route("/api/frames/range", get(get_frames_in_range))
        .route("/api/raw-sensor/:sensor_id", get(get_raw_sensor_data))
        .route("/api/realtime-aggregated", get(get_realtime_aggregated))
        .route("/api/stats", get(get_stats))
        .route("/api/health", get(health_check))
        .layer(CorsLayer::permissive())
        .with_state(state);

    // 启动服务器
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    
    info!("🚀 Web Server listening on http://{}", addr);
    info!("✅ File-based IPC enabled: Sensor data written to data/*.txt");
    info!("📊 Try:");
    info!("  - http://{}/ (Dashboard)", addr);
    info!("  - http://{}/api/frames/latest (Latest frames)", addr);
    info!("  - http://{}/api/raw-sensor/thermo-1 (Raw sensor data)", addr);
    info!("  - http://{}/api/stats (Statistics)", addr);
    
    axum::serve(listener, app).await?;
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let sensor_rate = env::var("SENSOR_RATE")
        .unwrap_or_else(|_| "200".to_string())
        .parse::<u32>()
        .unwrap_or(200);                 // 默认200次/秒
    let buffer_capacity = env::var("BUFFER_CAPACITY")
        .unwrap_or_else(|_| "1024".to_string())
        .parse::<usize>()
        .unwrap_or(1024);                // 默认1024
    let agg_workers = env::var("AGG_WORKERS")
        .unwrap_or_else(|_| "2".to_string())
        .parse::<usize>()
        .unwrap_or(2);                   // 默认2个聚合worker
    let governor_min_sleep_us = env::var("GOV_MIN_SLEEP_US")
        .unwrap_or_else(|_| "10".to_string())
        .parse::<u64>()
        .unwrap_or(10);                  // 默认10微秒
    let governor_max_sleep_us = env::var("GOV_MAX_SLEEP_US")
        .unwrap_or_else(|_| "500".to_string())
        .parse::<u64>()
        .unwrap_or(500);

    let start_time = Instant::now();
    // 确保数据目录存在
    let data_dir = PathBuf::from("data");
    fs::create_dir_all(&data_dir)?;
    
    info!("Starting Project Azure Gateway with File-based IPC");
    info!("Data directory: {}", data_dir.display());
    
    // 初始化传感器
    let mut thermo_1 = Thermometer::new("thermo-1".to_string(), sensor_rate);
    let mut thermo_2 = Thermometer::new("thermo-2".to_string(), sensor_rate);
    let mut accel_1 = Accelerometer::new("accel-1".to_string(), sensor_rate);
    let mut accel_2 = Accelerometer::new("accel-2".to_string(), sensor_rate);
    let mut force_1 = ForceSensor::new("force-1".to_string(), sensor_rate);

    thermo_1.start();
    thermo_2.start();
    accel_1.start();
    accel_2.start();
    force_1.start();

    let buffer = SensorBufferManager::<ReadingEnvelope>::new(buffer_capacity);
    let stop = Arc::new(AtomicBool::new(false));
    let mut governor_cfg = AdaptivePollingConfig::default();
    governor_cfg.min_sleep_us = governor_min_sleep_us;
    governor_cfg.max_sleep_us = governor_max_sleep_us;
    governor_cfg.nominal_sleep_us = governor_min_sleep_us;
    let scheduler_cfg = SchedulerConfig::default();

    let max_t1 = Arc::new(AtomicUsize::new(0));
    let max_t2 = Arc::new(AtomicUsize::new(0));
    let max_a1 = Arc::new(AtomicUsize::new(0));
    let max_a2 = Arc::new(AtomicUsize::new(0));
    let max_f1 = Arc::new(AtomicUsize::new(0));

    let gov_t1 = Arc::new(Mutex::new(AdaptivePollingGovernor::new(governor_cfg).stats()));
    let gov_t2 = Arc::new(Mutex::new(AdaptivePollingGovernor::new(governor_cfg).stats()));
    let gov_a1 = Arc::new(Mutex::new(AdaptivePollingGovernor::new(governor_cfg).stats()));
    let gov_a2 = Arc::new(Mutex::new(AdaptivePollingGovernor::new(governor_cfg).stats()));
    let gov_f1 = Arc::new(Mutex::new(AdaptivePollingGovernor::new(governor_cfg).stats()));
    let scheduler_stats = Arc::new(Mutex::new(SchedulerStats::default()));

    let (status_tx, status_rx) = mpsc::channel::<ReaderStatus>();
    let (cmd_t1_tx, cmd_t1_rx) = mpsc::channel::<ReaderCommand>();
    let (cmd_t2_tx, cmd_t2_rx) = mpsc::channel::<ReaderCommand>();
    let (cmd_a1_tx, cmd_a1_rx) = mpsc::channel::<ReaderCommand>();
    let (cmd_a2_tx, cmd_a2_rx) = mpsc::channel::<ReaderCommand>();
    let (cmd_f1_tx, cmd_f1_rx) = mpsc::channel::<ReaderCommand>();

    let mut command_txs = HashMap::new();
    command_txs.insert("thermo-1".to_string(), cmd_t1_tx);
    command_txs.insert("thermo-2".to_string(), cmd_t2_tx);
    command_txs.insert("accel-1".to_string(), cmd_a1_tx);
    command_txs.insert("accel-2".to_string(), cmd_a2_tx);
    command_txs.insert("force-1".to_string(), cmd_f1_tx);

    // 启动传感器读取线程
    info!("Starting sensor reader threads with file-based IPC...");
    let t1 = spawn_reader(
        thermo_1,
        |r| r.temperature_celsius as f64,
        Arc::clone(&buffer),
        Arc::clone(&stop),
        Arc::clone(&max_t1),
        governor_cfg,
        Arc::clone(&gov_t1),
        cmd_t1_rx,
        status_tx.clone(),
    );
    let t2 = spawn_reader(
        thermo_2,
        |r| r.temperature_celsius as f64,
        Arc::clone(&buffer),
        Arc::clone(&stop),
        Arc::clone(&max_t2),
        governor_cfg,
        Arc::clone(&gov_t2),
        cmd_t2_rx,
        status_tx.clone(),
    );
    let a1 = spawn_reader(
        accel_1,
        |r| magnitude3(r.acceleration_x, r.acceleration_y, r.acceleration_z),
        Arc::clone(&buffer),
        Arc::clone(&stop),
        Arc::clone(&max_a1),
        governor_cfg,
        Arc::clone(&gov_a1),
        cmd_a1_rx,
        status_tx.clone(),
    );
    let a2 = spawn_reader(
        accel_2,
        |r| magnitude3(r.acceleration_x, r.acceleration_y, r.acceleration_z),
        Arc::clone(&buffer),
        Arc::clone(&stop),
        Arc::clone(&max_a2),
        governor_cfg,
        Arc::clone(&gov_a2),
        cmd_a2_rx,
        status_tx.clone(),
    );
    let f1 = spawn_reader(
        force_1,
        |r| magnitude3(r.force_x, r.force_y, r.force_z),
        Arc::clone(&buffer),
        Arc::clone(&stop),
        Arc::clone(&max_f1),
        governor_cfg,
        Arc::clone(&gov_f1),
        cmd_f1_rx,
        status_tx.clone(),
    );
    drop(status_tx);

    // 启动调度器
    let scheduler = spawn_scheduler(
        Arc::clone(&stop),
        status_rx,
        command_txs,
        Arc::clone(&scheduler_stats),
        scheduler_cfg,
    );

    // 启动聚合引擎
    info!("Starting aggregation engine...");
    let cfg = AggregationConfig {
        window: Duration::from_secs(1),
        workers: agg_workers,
        ..Default::default()
    };
    let engine = AggregationEngine::start(cfg, Arc::clone(&buffer));
    let storage = Arc::new(StorageHandle::start(StorageConfig::default(), 256));

    // 启动Web Server
    info!("Starting Web Server dashboard...");
    let web_server_handle = tokio::spawn(async move {
        if let Err(e) = start_web_server(data_dir).await {
            eprintln!("Web Server error: {}", e);
        }
    });

    // 启动数据消费者线程
    let stats_buffer = Arc::clone(&buffer);
    let stop_for_consumer = Arc::clone(&stop);
    let scheduler_stats_for_consumer = Arc::clone(&scheduler_stats);
    let storage_clone = Arc::clone(&storage);
    let consumer = thread::spawn(move || {
        let mut shutdown_idle_rounds = 0u8;

        loop {
            if let Some(frame) = engine.recv_frame_timeout(Duration::from_millis(250)) {
                shutdown_idle_rounds = 0;
                let storage_metrics = storage_clone.metrics();
                let stats = build_stats_snapshot(
                    &frame,
                    &stats_buffer,
                    &max_t1,
                    &max_t2,
                    &max_a1,
                    &max_a2,
                    &max_f1,
                    &gov_t1,
                    &gov_t2,
                    &gov_a1,
                    &gov_a2,
                    &gov_f1,
                    &scheduler_stats_for_consumer,
                    &storage_metrics,
                );
                if let Err(err) = storage_clone.persist_frame(frame.clone(), Some(stats)) {
                    eprintln!("storage: failed to persist window {}: {err:?}", frame.window_id);
                    stop_for_consumer.store(true, Ordering::Relaxed);
                    stats_buffer.shutdown();
                    break;
                }
                println!(
                    "[window {}] sensors={} anomalies={}",
                    frame.window_id,
                    frame.per_sensor.len(),
                    frame.anomalies.len()
                );
                continue;
            }

            if stop_for_consumer.load(Ordering::Relaxed) && stats_buffer.is_shutdown() {
                shutdown_idle_rounds = shutdown_idle_rounds.saturating_add(1);
                if shutdown_idle_rounds >= 4 {
                    break;
                }
            }
        }

        engine.shutdown();
        storage_clone.shutdown();
    });

    // 等待关闭信号
    info!("All components started. Press Ctrl+C to stop.");
    info!("Dashboard: http://127.0.0.1:3000/");
    wait_for_shutdown_signal(Arc::clone(&stop)).await;
    
    // 等待所有线程结束
    info!("Shutting down...");
    let _ = scheduler.join();
    let _ = t1.join();
    let _ = t2.join();
    let _ = a1.join();
    let _ = a2.join();
    let _ = f1.join();
    buffer.shutdown();
    let _ = consumer.join();

    web_server_handle.abort();
    let _ = web_server_handle.await;

    info!("Shutdown complete.");


    // ===================== 消融实验性能报告 =====================
    let end_time = Instant::now();
    let elapsed_time = end_time.duration_since(start_time).as_secs_f64();
    let buffer_stats = buffer.stats();
    let _storage_metrics = storage.metrics();

// 核心指标计算
    let total_pushed = buffer_stats.total_pushed;
    let total_popped = buffer_stats.total_popped;
    let sensor_lost = ablation::SENSOR_LOST_READINGS.load(Ordering::Relaxed) as u64;
    let total_generated = total_pushed + sensor_lost;

// 1. 数据丢失率 (%) - 综合传感器丢失和缓冲区残留
    let data_loss_rate = if total_generated > 0 {
        let lost_total = sensor_lost + (total_pushed - total_popped);
        (lost_total as f64 / total_generated as f64) * 100.0
    } else {
        0.0
    };

// 2. 吞吐量 TPS (使用推入缓冲区的数据量)
    let throughput = if elapsed_time > 0.0 {
        total_pushed as f64 / elapsed_time
    } else {
        0.0
    };

// 3. 平均延迟 (ms) - 保持原有定义（阻塞等待时间）
    let avg_push_wait_ms = if buffer_stats.push_wait_count > 0 {
        (buffer_stats.push_wait_ns_total as f64 / buffer_stats.push_wait_count as f64) / 1_000_000.0
    } else {
        0.0
    };

// 4. 线程冲突数（锁竞争代理指标）
    let thread_conflicts = buffer_stats.push_wait_count;

// 5. 传感器溢出事件次数（原溢出计数）
    let sensor_overflow_events = ablation::SENSOR_OVERFLOW_EVENTS.load(Ordering::Relaxed);
// 传感器丢失读数总数（新增）
    let sensor_lost_readings = sensor_lost;

// 输出格式化报告
    println!("\n==================== Performance indicators of ablation experiments ====================");
    println!("Data loss rate          : {:.4}%", data_loss_rate);
    println!("Throughput              : {:.2} TPS", throughput);
    println!("Average latency         : {:.4} ms", avg_push_wait_ms);
    println!("Number of thread conflicts : {}", thread_conflicts);
    println!("Sensor overflow events  : {}", sensor_overflow_events);
    println!("Sensor lost readings    : {}", sensor_lost_readings);
    println!("=========================================================\n");

// 写入持久化文件
    let report = format!(
        "Data loss rate (%): {:.4}\nThroughput (TPS): {:.2}\nAverage latency (ms): {:.4}\nThread Conflicts: {}\nSensor overflow events: {}\nSensor lost readings: {}\n",
        data_loss_rate, throughput, avg_push_wait_ms, thread_conflicts, sensor_overflow_events, sensor_lost_readings
    );
    let _ = std::fs::write("ablation_metrics.txt", report);


    Ok(())
}