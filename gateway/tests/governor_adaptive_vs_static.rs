//! Scenarios where adaptive polling is **observably different** from a static sleep policy.
//!
//! End-to-end benchmarks often show similar loss/latency when the consumer is saturated:
//! both policies still wake often enough that the bottleneck is downstream. These tests
//! fix the telemetry fed into [`AdaptivePollingGovernor`] so differences are deterministic.

use gateway::buffer::BufferStats;
use gateway::governor::{AdaptivePollingConfig, AdaptivePollingGovernor, GovernorMode};
use std::time::Duration;

fn buffer(
    capacity: usize,
    len: usize,
    push_wait_ns_total: u128,
    pop_wait_ns_total: u128,
) -> BufferStats {
    BufferStats {
        capacity,
        len,
        high_watermark: len,
        total_pushed: 0,
        total_popped: 0,
        push_wait_count: 0,
        pop_wait_count: 0,
        push_wait_ns_total,
        pop_wait_ns_total,
        is_shutdown: false,
    }
}

/// Baseline: same sleep every tick, regardless of queue or sensor (what “static” means here).
fn static_sleep_us(fixed_us: u64, steps: usize) -> u128 {
    u128::from(fixed_us) * steps as u128
}

#[test]
fn adaptive_sum_sleep_exceeds_static_under_slack_pop_bias() {
    let cfg = AdaptivePollingConfig::default();
    let fixed = cfg.nominal_sleep_us;
    let cap = 10_000usize;
    let steps = 40usize;

    let mut gov = AdaptivePollingGovernor::new(cfg);
    let mut adaptive_total_us: u128 = 0;
    for i in 0..steps {
        let pop_ns = (i as u128 + 1) * 1_000_000u128;
        let d = gov.update(0, &buffer(cap, 0, 0, pop_ns));
        adaptive_total_us += d.as_micros();
    }

    let static_total_us = static_sleep_us(fixed, steps);
    assert_eq!(gov.stats().mode, GovernorMode::Economy);
    assert!(
        adaptive_total_us > static_total_us,
        "under repeated slack + pop-biased waits, adaptive should lengthen sleep vs nominal static; adaptive={adaptive_total_us} static={static_total_us}"
    );
}

#[test]
fn adaptive_drops_to_min_sleep_on_push_pressure_static_stays_flat() {
    let cfg = AdaptivePollingConfig::default();
    let fixed = cfg.nominal_sleep_us;
    let cap = 2048usize;

    let mut gov = AdaptivePollingGovernor::new(cfg);
    gov.update(0, &buffer(cap, 0, 0, 0));
    let d = gov.update(0, &buffer(cap, 0, 999_999, 0));
    assert_eq!(d.as_micros(), gov.cfg().min_sleep_us as u128);

    let static_always = Duration::from_micros(fixed);
    assert_ne!(
        d.as_micros(),
        static_always.as_micros(),
        "static policy at nominal would not react to push-wait pressure"
    );
}

#[test]
fn phased_workload_produces_multiple_governor_modes() {
    let mut gov = AdaptivePollingGovernor::new(AdaptivePollingConfig::default());
    let cap = 1000usize;

    for step in 1u32..=8 {
        let push_ns = u128::from(step) * 40_000u128;
        gov.update(0, &buffer(cap, 15, push_ns, 0));
    }
    let push_floor = 8 * 40_000u128;
    for i in 0..35 {
        let pop_ns = (i as u128 + 1) * 1_000_000u128;
        gov.update(0, &buffer(cap, 1, push_floor, pop_ns));
    }

    let s = gov.stats();
    assert!(
        s.fast_recovery_entries >= 1 && s.economy_entries >= 1,
        "expected both FastRecovery and Economy in a phased trace, got FR={} Econ={}",
        s.fast_recovery_entries,
        s.economy_entries
    );
    assert!(
        s.mode_switches >= 2,
        "mode switches should be visible for a dynamic policy; got {}",
        s.mode_switches
    );
}

#[test]
fn sensor_backlog_crossing_hard_threshold_switches_to_fast_recovery() {
    let cfg = AdaptivePollingConfig::default();
    let hard = cfg.hard_sensor_threshold;
    let mut gov = AdaptivePollingGovernor::new(cfg);
    gov.update(hard.saturating_sub(1).max(0), &buffer(4096, 0, 0, 0));
    assert_ne!(gov.stats().mode, GovernorMode::FastRecovery);

    gov.update(hard, &buffer(4096, 0, 0, 0));
    assert_eq!(gov.stats().mode, GovernorMode::FastRecovery);
}
