//! Micro-benchmarks for the student buffer + optional adaptive reader sleep vs static sleep.
//! Writes JSON lines under `data/bench/` for `docs/make_figures.py`.
//!
//! Examples:
//!   cargo run --release -p gateway --bin azure_bench -- sweep-pareto
//!   cargo run --release -p gateway --bin azure_bench -- sweep-load
//!   cargo run --release -p gateway --bin azure_bench -- sweep-governor
//!   cargo run --release -p gateway --bin azure_bench -- gov-trace
//!   cargo run --release -p gateway --bin azure_bench -- zero-loss

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use gateway::buffer::SensorBufferManager;
use gateway::governor::{
    AdaptivePollingConfig, AdaptivePollingGovernor, GovernorMode, GovernorOverride,
};
use gateway::types::ReadingEnvelope;
use serde::Serialize;

const RING_CAP: usize = 128;

#[derive(Clone, Serialize)]
struct BenchRow {
    capacity: usize,
    reader_sleep_us: u64,
    consumer_work_us: u64,
    sensor_interval_us: u64,
    adaptive: bool,
    duration_ms: u64,
    sensors: usize,
    sensor_overwrites: u64,
    buffer_high_watermark: usize,
    push_wait_count: u64,
    pop_wait_count: u64,
    p95_lag_us: u64,
    throughput_rps_total: f64,
    /// Offered load ≈ sensors / (interval_us × 10⁻⁶) readings/s.
    offered_rps_total: f64,
    /// ρ̂ ≈ offered_rps × consumer_work_us × 10⁻⁶ for one serial consumer.
    rho_hat: f64,
    governor_mode_switches: u64,
    governor_economy_ticks: u64,
    governor_fast_recovery_ticks: u64,
    /// Process user+sys CPU seconds over this run (Unix getrusage; 0 on non-Unix).
    process_cpu_secs: f64,
}

#[derive(Clone, Serialize)]
struct GovTracePoint {
    t_ms: u64,
    reader_id: u8,
    sleep_us: u64,
    mode: String,
    fill_ratio: f64,
    sensor_avail: usize,
}

#[derive(Serialize)]
struct ZeroLossReport {
    run_duration_s: u64,
    buffer_capacity: usize,
    reader_sleep_us: u64,
    sensors: usize,
    sensor_overwrites: u64,
    buffer_high_watermark: usize,
    push_wait_count: u64,
    pop_wait_count: u64,
    p95_lag_us: u64,
    note: &'static str,
}

struct RunParams {
    capacity: usize,
    reader_sleep_us: u64,
    consumer_work_us: u64,
    sensor_interval_us: u64,
    adaptive: bool,
    duration: Duration,
    sensors: usize,
}

#[cfg(unix)]
fn process_cpu_user_sys_secs() -> f64 {
    use libc::{getrusage, rusage, RUSAGE_SELF};
    let mut ru: rusage = unsafe { std::mem::zeroed() };
    if unsafe { getrusage(RUSAGE_SELF, &mut ru as *mut _) } != 0 {
        return 0.0;
    }
    let u = ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 * 1e-6;
    let s = ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 * 1e-6;
    u + s
}

#[cfg(not(unix))]
fn process_cpu_user_sys_secs() -> f64 {
    0.0
}

fn p95_us(mut v: Vec<u64>) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    let i = ((v.len() as f64 * 0.95).floor() as usize).min(v.len() - 1);
    v[i]
}

fn run_micro(p: RunParams) -> BenchRow {
    let cpu_t0 = process_cpu_user_sys_secs();
    let buf = SensorBufferManager::<ReadingEnvelope>::new(p.capacity);
    let stop = Arc::new(AtomicBool::new(false));
    let overwrites = Arc::new(AtomicU64::new(0));
    let lags = Arc::new(Mutex::new(Vec::<u64>::with_capacity(200_000)));

    let mut avails = Vec::new();
    let mut gov_ticks = Vec::new();

    for _ in 0..p.sensors {
        avails.push(Arc::new(AtomicUsize::new(0)));
        gov_ticks.push(Arc::new(Mutex::new((0u64, 0u64, 0u64)))); // economy, fr, switches (approx)
    }

    let gov_cfg = AdaptivePollingConfig::default();

    let mut handles = vec![];

    for i in 0..p.sensors {
        let avail = avails[i].clone();
        let ov = overwrites.clone();
        let st = stop.clone();
        let iv = i;
        handles.push(thread::spawn(move || {
            let mut seq = 0u64;
            while !st.load(Ordering::Relaxed) {
                let a = avail.load(Ordering::Relaxed);
                if a < RING_CAP {
                    avail.fetch_add(1, Ordering::Relaxed);
                } else {
                    ov.fetch_add(1, Ordering::Relaxed);
                }
                seq = seq.wrapping_add(1);
                thread::sleep(Duration::from_micros(p.sensor_interval_us.max(1)));
            }
            let _ = seq;
            let _ = iv;
        }));
    }

    for i in 0..p.sensors {
        let buf = buf.clone();
        let avail = avails[i].clone();
        let st = stop.clone();
        let ticks = gov_ticks[i].clone();
        let sid = format!("s{}", i + 1);
        let adaptive = p.adaptive;
        let static_us = p.reader_sleep_us;
        let gc = gov_cfg;

        handles.push(thread::spawn(move || {
            let mut gov = AdaptivePollingGovernor::new(gc);
            loop {
                if st.load(Ordering::Relaxed) && avail.load(Ordering::Relaxed) == 0 {
                    break;
                }
                let av = avail.load(Ordering::Relaxed);
                let bst = buf.stats();
                let sleep_dur = if adaptive {
                    let d = gov.update_with_override(av, &bst, GovernorOverride::None);
                    let s = gov.stats();
                    let mut t = ticks.lock().unwrap();
                    if s.mode == GovernorMode::Economy {
                        t.0 += 1;
                    }
                    if s.mode == GovernorMode::FastRecovery {
                        t.1 += 1;
                    }
                    t.2 = s.mode_switches;
                    d
                } else {
                    Duration::from_micros(static_us)
                };
                thread::sleep(sleep_dur);

                while avail.load(Ordering::Relaxed) > 0 {
                    let cur = avail.load(Ordering::Relaxed);
                    if cur == 0 {
                        break;
                    }
                    if avail
                        .compare_exchange_weak(cur, cur - 1, Ordering::AcqRel, Ordering::Relaxed)
                        .is_err()
                    {
                        continue;
                    }
                    let env = ReadingEnvelope::new(&sid, SystemTime::now(), cur as f64);
                    if buf.push(env).is_err() {
                        break;
                    }
                }
            }
        }));
    }

    let buf_c = buf.clone();
    let st_c = stop.clone();
    let lags_c = lags.clone();
    let work = p.consumer_work_us;
    handles.push(thread::spawn(move || {
        while !st_c.load(Ordering::Relaxed) || buf_c.len() > 0 {
            if let Some(e) = buf_c.pop_timeout(Duration::from_millis(2)) {
                let lag = SystemTime::now()
                    .duration_since(e.ts)
                    .map(|d| d.as_micros().min(u128::from(u64::MAX)) as u64)
                    .unwrap_or(0);
                {
                    let mut g = lags_c.lock().unwrap();
                    if g.len() < 500_000 {
                        g.push(lag);
                    }
                }
                if work > 0 {
                    thread::sleep(Duration::from_micros(work));
                }
            }
        }
    }));

    thread::sleep(p.duration);
    stop.store(true, Ordering::SeqCst);

    for h in handles {
        let _ = h.join();
    }

    let bs = buf.stats();
    let lag_sample = lags.lock().unwrap().clone();
    let p95 = p95_us(lag_sample);
    let elapsed = p.duration.as_secs_f64().max(1e-6);
    let popped = bs.total_popped as f64;
    let offered = (p.sensors as f64) / (p.sensor_interval_us.max(1) as f64 / 1e6);
    let rho_hat = offered * (p.consumer_work_us as f64) * 1e-6;
    let cpu_t1 = process_cpu_user_sys_secs();
    let process_cpu_secs = (cpu_t1 - cpu_t0).max(0.0);

    let (mut eco, mut fr, mut sw) = (0u64, 0u64, 0u64);
    for t in &gov_ticks {
        let x = t.lock().unwrap();
        eco += x.0;
        fr += x.1;
        sw = sw.max(x.2);
    }

    BenchRow {
        capacity: p.capacity,
        reader_sleep_us: p.reader_sleep_us,
        consumer_work_us: p.consumer_work_us,
        sensor_interval_us: p.sensor_interval_us,
        adaptive: p.adaptive,
        duration_ms: p.duration.as_millis() as u64,
        sensors: p.sensors,
        sensor_overwrites: overwrites.load(Ordering::Relaxed),
        buffer_high_watermark: bs.high_watermark,
        push_wait_count: bs.push_wait_count,
        pop_wait_count: bs.pop_wait_count,
        p95_lag_us: p95,
        throughput_rps_total: popped / elapsed,
        offered_rps_total: offered,
        rho_hat,
        governor_mode_switches: sw,
        governor_economy_ticks: eco,
        governor_fast_recovery_ticks: fr,
        process_cpu_secs,
    }
}

fn project_root() -> std::path::PathBuf {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.pop();
    d
}

fn default_bench_dir() -> std::path::PathBuf {
    project_root().join("data").join("bench")
}

fn write_line(path: &std::path::Path, row: &BenchRow) {
    if let Some(p) = path.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open bench out");
    writeln!(f, "{}", serde_json::to_string(row).unwrap()).unwrap();
}

fn sweep_pareto(out: &std::path::Path, duration_ms: u64) {
    let _ = std::fs::remove_file(out);
    // Smaller default grid (~18 runs); widen arrays locally if you need denser sweeps.
    let caps = [256usize, 4096, 65536];
    let sleeps = [0u64, 50, 100, 200];
    let works = [0u64, 100, 200];
    // ~20k rps per sensor → 50 µs
    let interval = 50u64;
    for &cap in &caps {
        for &rs in &sleeps {
            for &cw in &works {
                let row = run_micro(RunParams {
                    capacity: cap,
                    reader_sleep_us: rs,
                    consumer_work_us: cw,
                    sensor_interval_us: interval,
                    adaptive: false,
                    duration: Duration::from_millis(duration_ms),
                    sensors: 2,
                });
                write_line(out, &row);
                eprintln!(
                    "pareto cap={cap} sleep_us={rs} work_us={cw} p95={} ov={}",
                    row.p95_lag_us, row.sensor_overwrites
                );
            }
        }
    }
}

fn sweep_load(out: &std::path::Path, duration_ms: u64) {
    let _ = std::fs::remove_file(out);
    let cap = 256usize;
    let rs = 0u64;
    // interval in µs → offered rate; 200 µs = 5k/s per sensor, 25 µs = 40k/s per sensor
    let intervals = [200u64, 100, 66, 50, 40, 33, 25];
    for &work in &[0u64, 200] {
        for &iv in &intervals {
            let row = run_micro(RunParams {
                capacity: cap,
                reader_sleep_us: rs,
                consumer_work_us: work,
                sensor_interval_us: iv,
                adaptive: false,
                duration: Duration::from_millis(duration_ms),
                sensors: 2,
            });
            let thr = row.throughput_rps_total;
            let ov = row.sensor_overwrites;
            let line = serde_json::json!({
                "tag": if work == 0 { "stable_consumer" } else { "slow_consumer_200us" },
                "row": row,
            });
            if let Some(p) = out.parent() {
                let _ = std::fs::create_dir_all(p);
            }
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(out)
                .unwrap();
            writeln!(f, "{}", line).unwrap();
            eprintln!(
                "load interval={iv} work={work} thr={thr:.0} ov={ov}",
            );
        }
    }
}

fn sweep_governor(out: &std::path::Path, duration_ms: u64) {
    let _ = std::fs::remove_file(out);
    // (capacity, consumer_work_us); static policy uses fixed 100 µs sleep vs adaptive governor.
    let grid: &[(usize, u64)] = &[
        (256, 0),
        (256, 200),
        (4096, 0),
        (4096, 200),
        (65536, 200),
    ];
    let interval = 50u64;
    let static_nominal_us = 100u64;
    for &(cap, cw) in grid {
        for adaptive in [false, true] {
            let row = run_micro(RunParams {
                capacity: cap,
                reader_sleep_us: static_nominal_us,
                consumer_work_us: cw,
                sensor_interval_us: interval,
                adaptive,
                duration: Duration::from_millis(duration_ms),
                sensors: 2,
            });
            let line = serde_json::json!({
                "policy": if adaptive { "adaptive" } else { "static" },
                "static_nominal_us": static_nominal_us,
                "row": row,
            });
            use std::io::Write;
            if let Some(p) = out.parent() {
                let _ = std::fs::create_dir_all(p);
            }
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(out)
                .unwrap();
            writeln!(f, "{}", line).unwrap();
        }
    }
}

fn gov_trace(out: &std::path::Path, duration_ms: u64) {
    let buf = SensorBufferManager::<ReadingEnvelope>::new(1024);
    let stop = Arc::new(AtomicBool::new(false));
    let avail = Arc::new(AtomicUsize::new(0));
    let trace = Arc::new(Mutex::new(Vec::<GovTracePoint>::new()));
    let over = Arc::new(AtomicU64::new(0));

    let st = stop.clone();
    let av = avail.clone();
    let ov = over.clone();
    let sensor_h = thread::spawn(move || {
        while !st.load(Ordering::Relaxed) {
            let a = av.load(Ordering::Relaxed);
            if a < RING_CAP {
                av.fetch_add(1, Ordering::Relaxed);
            } else {
                ov.fetch_add(1, Ordering::Relaxed);
            }
            thread::sleep(Duration::from_micros(80));
        }
    });

    let buf_r = buf.clone();
    let av_r = avail.clone();
    let st_r = stop.clone();
    let tr = trace.clone();
    let reader_h = thread::spawn(move || {
        let mut gov = AdaptivePollingGovernor::new(AdaptivePollingConfig::default());
        let t0 = Instant::now();
        loop {
            if st_r.load(Ordering::Relaxed) && av_r.load(Ordering::Relaxed) == 0 {
                break;
            }
            let sensor_avail = av_r.load(Ordering::Relaxed);
            let bst = buf_r.stats();
            let d = gov.update_with_override(sensor_avail, &bst, GovernorOverride::None);
            let s = gov.stats();
            {
                let mut g = tr.lock().unwrap();
                g.push(GovTracePoint {
                    t_ms: t0.elapsed().as_millis() as u64,
                    reader_id: 1,
                    sleep_us: s.current_sleep_us,
                    mode: format!("{:?}", s.mode),
                    fill_ratio: s.last_fill_ratio,
                    sensor_avail,
                });
            }
            thread::sleep(d);
            while av_r.load(Ordering::Relaxed) > 0 {
                let cur = av_r.load(Ordering::Relaxed);
                if cur == 0 {
                    break;
                }
                if av_r
                    .compare_exchange_weak(cur, cur - 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_err()
                {
                    continue;
                }
                let _ = buf_r.push(ReadingEnvelope::new("trace-1", SystemTime::now(), 1.0));
            }
        }
    });

    let buf_c = buf.clone();
    let st_c = stop.clone();
    let cons_h = thread::spawn(move || {
        while !st_c.load(Ordering::Relaxed) || buf_c.len() > 0 {
            if let Some(_) = buf_c.pop_timeout(Duration::from_millis(2)) {
                thread::sleep(Duration::from_micros(30));
            }
        }
    });

    thread::sleep(Duration::from_millis(duration_ms));
    stop.store(true, Ordering::SeqCst);
    let _ = sensor_h.join();
    let _ = reader_h.join();
    let _ = cons_h.join();

    let points = trace.lock().unwrap().clone();
    if let Some(p) = out.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    std::fs::write(out, serde_json::to_string_pretty(&points).unwrap()).unwrap();
    eprintln!("wrote {} points to {}", points.len(), out.display());
}

fn zero_loss_report(out: &std::path::Path, duration_ms: u64) {
    let row = run_micro(RunParams {
        capacity: 65536,
        reader_sleep_us: 2000,
        consumer_work_us: 0,
        sensor_interval_us: 500,
        adaptive: false,
        duration: Duration::from_millis(duration_ms),
        sensors: 2,
    });
    let rep = ZeroLossReport {
        run_duration_s: (duration_ms + 999) / 1000,
        buffer_capacity: row.capacity,
        reader_sleep_us: row.reader_sleep_us,
        sensors: row.sensors,
        sensor_overwrites: row.sensor_overwrites,
        buffer_high_watermark: row.buffer_high_watermark,
        push_wait_count: row.push_wait_count,
        pop_wait_count: row.pop_wait_count,
        p95_lag_us: row.p95_lag_us,
        note: "Synthetic micro-bench (2 logical sensors); use full gateway for course demo.",
    };
    if let Some(p) = out.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    std::fs::write(out, serde_json::to_string_pretty(&rep).unwrap()).unwrap();
}

fn main() {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| "help".into());
    let bench_dir = default_bench_dir();
    let duration_ms: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1200);

    match cmd.as_str() {
        "sweep-pareto" => {
            let p = args
                .next()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| bench_dir.join("pareto_sweep.jsonl"));
            sweep_pareto(&p, duration_ms);
        }
        "sweep-load" => {
            let p = args
                .next()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| bench_dir.join("load_sweep.jsonl"));
            sweep_load(&p, duration_ms);
        }
        "sweep-governor" => {
            let p = args
                .next()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| bench_dir.join("governor_pareto_sweep.jsonl"));
            sweep_governor(&p, duration_ms);
        }
        "gov-trace" => {
            let p = args
                .next()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| bench_dir.join("governor_trace.json"));
            gov_trace(&p, duration_ms.max(800));
        }
        "zero-loss" => {
            let p = args
                .next()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| bench_dir.join("zero_loss_report.json"));
            zero_loss_report(&p, duration_ms.max(2000));
        }
        _ => {
            eprintln!(
                "usage: azure_bench sweep-pareto|sweep-load|sweep-governor|gov-trace|zero-loss [duration_ms] [out_path]"
            );
        }
    }
}
