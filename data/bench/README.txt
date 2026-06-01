Project Azure benchmark artifacts (versioned for report reproducibility).

Regenerate micro-benches (800 ms per grid point; adjust duration as needed):
  cargo run --release -p gateway --bin azure_bench -- sweep-pareto 800 data/bench/pareto_sweep.jsonl
  cargo run --release -p gateway --bin azure_bench -- sweep-load 800 data/bench/load_sweep.jsonl
  cargo run --release -p gateway --bin azure_bench -- sweep-governor 800 data/bench/governor_pareto_sweep.jsonl
  cargo run --release -p gateway --bin azure_bench -- zero-loss 30000 data/bench/zero_loss_report.json

Integrated gateway snapshot summary (manual protocol) is recorded in integrated_zero_loss_32s.json.

Figures: committed under docs/figures/; regenerate with:
  pip install -r docs/requirements-figures.txt && python3 docs/make_figures.py
