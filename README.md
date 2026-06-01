# COMP2432 Project Azure

**Author:** XU Mingyue  

PolyU COMP2432 coursework: a multi-threaded Rust pipeline that drains simulated sensors into a bounded queue, aggregates readings in time windows, writes JSON snapshots to disk, and serves them over HTTP.

## Crates

| Crate | Role |
|--------|------|
| `gateway` | Binary: reader threads, `SensorBufferManager`, scheduler, aggregation engine, storage worker, Tokio signal handling. |
| `dashboard` | HTTP app (Hotaru): `/`, `/latest`, `/stats`, `/sensor/<id>`. |
| `os_lib` | Internal queue primitives used by `sensor_sim`; application code talks to sensors through the public trait only. |

## Requirements

- Rust **stable**, recent enough for **edition 2024** (check with `rustc --version`; if `cargo build` complains, run `rustup update`).

## Build and test

```bash
cargo build --release
cargo test
```

## Run

From the repository root:

```bash
cargo run -p gateway --release
```

- Stops cleanly on **Ctrl+C** or **SIGTERM** (readers and workers are joined in order; the buffer is shut down so blocking consumers wake up).
- Writes under `data/` (e.g. hourly JSONL under `data/frames/`, `snapshot.json` / `latest.json` / `stats.json` when the storage worker publishes). Paths listed in `.gitignore` are runtime-only and are not meant to be committed.

Dashboard listens on **`http://127.0.0.1:3000`**.

## Course alignment

- **Buffer:** `Mutex` + `Condvar` bounded queue; blocking push/pop, try variants, timeout pop, stats, shutdown.
- **Readers:** one thread per sensor, drain-first polling on `Sensor::read()`.
- **Aggregation:** windowed stats, multiple workers, merge at window boundaries; fail-closed if a window cannot be completed.
- **Storage:** dedicated writer thread; atomic publication (write-then-rename) so readers do not see partial files.

## License / use

Submitted as coursework; reuse only within what your instructor allows.
