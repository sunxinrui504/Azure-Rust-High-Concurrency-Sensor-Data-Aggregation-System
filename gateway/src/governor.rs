use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::buffer::BufferStats;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GovernorOverride {
    None,
    ClampMaxSleep(u64),
    ForceFastRecovery,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum GovernorMode {
    FastRecovery,
    Tracking,
    Economy,
    FailSafe,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AdaptivePollingConfig {
    pub min_sleep_us: u64,
    pub nominal_sleep_us: u64,
    pub max_sleep_us: u64,
    pub target_fill_ratio: f64,
    pub high_fill_ratio: f64,
    pub low_fill_ratio: f64,
    pub soft_sensor_threshold: usize,
    pub hard_sensor_threshold: usize,
    pub step_up_us: u64,
    pub step_down_us: u64,
    pub kp_us_per_fill: f64,
    pub ki_us_per_fill: f64,
    pub integral_limit: f64,
    pub ewma_alpha: f64,
}

impl Default for AdaptivePollingConfig {
    fn default() -> Self {
        Self {
            min_sleep_us: 25,
            nominal_sleep_us: 100,
            max_sleep_us: 1_000,
            target_fill_ratio: 0.03,
            high_fill_ratio: 0.25,
            low_fill_ratio: 0.01,
            soft_sensor_threshold: 8,
            hard_sensor_threshold: 24,
            step_up_us: 25,
            step_down_us: 75,
            kp_us_per_fill: 2_000.0,
            ki_us_per_fill: 80.0,
            integral_limit: 1.0,
            ewma_alpha: 0.2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovernorStats {
    pub mode: GovernorMode,
    pub current_sleep_us: u64,
    pub updates: u64,
    pub mode_switches: u64,
    pub fast_recovery_entries: u64,
    pub economy_entries: u64,
    pub tracking_entries: u64,
    pub fail_safe_entries: u64,
    pub last_fill_ratio: f64,
    pub smoothed_fill_ratio: f64,
    pub last_sensor_available: usize,
    pub last_push_wait_delta_ns: u128,
    pub last_pop_wait_delta_ns: u128,
}

pub struct AdaptivePollingGovernor {
    cfg: AdaptivePollingConfig,
    mode: GovernorMode,
    current_sleep_us: u64,
    integral_error: f64,
    prev_push_wait_ns_total: u128,
    prev_pop_wait_ns_total: u128,
    updates: u64,
    mode_switches: u64,
    fast_recovery_entries: u64,
    economy_entries: u64,
    tracking_entries: u64,
    fail_safe_entries: u64,
    last_fill_ratio: f64,
    smoothed_fill_ratio: f64,
    last_sensor_available: usize,
    last_push_wait_delta_ns: u128,
    last_pop_wait_delta_ns: u128,
}

impl AdaptivePollingGovernor {
    pub fn new(mut cfg: AdaptivePollingConfig) -> Self {
        if cfg.min_sleep_us == 0
            || cfg.nominal_sleep_us < cfg.min_sleep_us
            || cfg.max_sleep_us < cfg.nominal_sleep_us
            || !(0.0..=1.0).contains(&cfg.low_fill_ratio)
            || !(0.0..=1.0).contains(&cfg.target_fill_ratio)
            || !(0.0..=1.0).contains(&cfg.high_fill_ratio)
            || !(0.0..=1.0).contains(&cfg.ewma_alpha)
            || cfg.low_fill_ratio > cfg.target_fill_ratio
            || cfg.target_fill_ratio > cfg.high_fill_ratio
            || cfg.soft_sensor_threshold > cfg.hard_sensor_threshold
        {
            cfg = AdaptivePollingConfig::default();
        }

        Self {
            current_sleep_us: cfg.nominal_sleep_us,
            cfg,
            mode: GovernorMode::Tracking,
            integral_error: 0.0,
            prev_push_wait_ns_total: 0,
            prev_pop_wait_ns_total: 0,
            updates: 0,
            mode_switches: 0,
            fast_recovery_entries: 0,
            economy_entries: 0,
            tracking_entries: 1,
            fail_safe_entries: 0,
            last_fill_ratio: 0.0,
            smoothed_fill_ratio: 0.0,
            last_sensor_available: 0,
            last_push_wait_delta_ns: 0,
            last_pop_wait_delta_ns: 0,
        }
    }

    pub fn cfg(&self) -> AdaptivePollingConfig {
        self.cfg
    }

    fn set_mode(&mut self, next: GovernorMode) {
        if self.mode != next {
            self.mode_switches += 1;
            self.mode = next;
            match next {
                GovernorMode::FastRecovery => self.fast_recovery_entries += 1,
                GovernorMode::Tracking => self.tracking_entries += 1,
                GovernorMode::Economy => self.economy_entries += 1,
                GovernorMode::FailSafe => self.fail_safe_entries += 1,
            }
        }
    }

    pub fn update(&mut self, sensor_available: usize, stats: &BufferStats) -> Duration {
        self.update_with_override(sensor_available, stats, GovernorOverride::None)
    }

    pub fn update_with_override(
        &mut self,
        sensor_available: usize,
        stats: &BufferStats,
        override_mode: GovernorOverride,
    ) -> Duration {
        self.updates += 1;

        let fill_ratio = if stats.capacity == 0 {
            self.set_mode(GovernorMode::FailSafe);
            self.current_sleep_us = 1_000;
            self.last_fill_ratio = 0.0;
            self.smoothed_fill_ratio = 0.0;
            self.last_sensor_available = sensor_available;
            self.last_push_wait_delta_ns = 0;
            self.last_pop_wait_delta_ns = 0;
            return Duration::from_micros(self.current_sleep_us);
        } else {
            stats.len as f64 / stats.capacity as f64
        };

        let push_wait_delta_ns = stats
            .push_wait_ns_total
            .saturating_sub(self.prev_push_wait_ns_total);
        let pop_wait_delta_ns = stats
            .pop_wait_ns_total
            .saturating_sub(self.prev_pop_wait_ns_total);
        self.prev_push_wait_ns_total = stats.push_wait_ns_total;
        self.prev_pop_wait_ns_total = stats.pop_wait_ns_total;

        self.last_fill_ratio = fill_ratio;
        self.smoothed_fill_ratio =
            (1.0 - self.cfg.ewma_alpha) * self.smoothed_fill_ratio + self.cfg.ewma_alpha * fill_ratio;
        self.last_sensor_available = sensor_available;
        self.last_push_wait_delta_ns = push_wait_delta_ns;
        self.last_pop_wait_delta_ns = pop_wait_delta_ns;

        let emergency = matches!(override_mode, GovernorOverride::ForceFastRecovery)
            || sensor_available >= self.cfg.hard_sensor_threshold
            || self.smoothed_fill_ratio >= self.cfg.high_fill_ratio
            || push_wait_delta_ns > 0;

        if emergency {
            self.set_mode(GovernorMode::FastRecovery);
            self.integral_error = 0.0;
            self.current_sleep_us = self.cfg.min_sleep_us;
            return Duration::from_micros(self.current_sleep_us);
        }

        let error = self.cfg.target_fill_ratio - self.smoothed_fill_ratio;
        self.integral_error =
            (self.integral_error + error).clamp(-self.cfg.integral_limit, self.cfg.integral_limit);

        let control_delta = self.cfg.kp_us_per_fill * error + self.cfg.ki_us_per_fill * self.integral_error;
        let mut proposed = (self.cfg.nominal_sleep_us as f64 + control_delta)
            .clamp(self.cfg.min_sleep_us as f64, self.cfg.max_sleep_us as f64)
            as u64;

        let economy_allowed = !matches!(
            override_mode,
            GovernorOverride::ClampMaxSleep(_) | GovernorOverride::ForceFastRecovery
        );

        let can_enter_economy = economy_allowed
            && self.smoothed_fill_ratio <= self.cfg.low_fill_ratio
            && sensor_available <= self.cfg.soft_sensor_threshold / 2
            && pop_wait_delta_ns > push_wait_delta_ns;

        let should_stay_economy = economy_allowed
            && self.mode == GovernorMode::Economy
            && self.smoothed_fill_ratio <= self.cfg.target_fill_ratio
            && sensor_available <= self.cfg.soft_sensor_threshold
            && push_wait_delta_ns == 0;

        if can_enter_economy || should_stay_economy {
            self.set_mode(GovernorMode::Economy);
            proposed = proposed.saturating_add(self.cfg.step_up_us).min(self.cfg.max_sleep_us);
        } else {
            self.set_mode(GovernorMode::Tracking);
            if self.smoothed_fill_ratio > self.cfg.target_fill_ratio
                || sensor_available >= self.cfg.soft_sensor_threshold
            {
                proposed = proposed.saturating_sub(self.cfg.step_down_us).max(self.cfg.min_sleep_us);
            } else {
                proposed = proposed.min(self.cfg.nominal_sleep_us.saturating_add(self.cfg.step_up_us));
            }
        }

        if let GovernorOverride::ClampMaxSleep(max_sleep_us) = override_mode {
            proposed = proposed.min(max_sleep_us.max(self.cfg.min_sleep_us));
        }

        if proposed == 0 {
            self.set_mode(GovernorMode::FailSafe);
            self.current_sleep_us = 1_000;
        } else {
            self.current_sleep_us = proposed;
        }

        Duration::from_micros(self.current_sleep_us)
    }

    pub fn stats(&self) -> GovernorStats {
        GovernorStats {
            mode: self.mode,
            current_sleep_us: self.current_sleep_us,
            updates: self.updates,
            mode_switches: self.mode_switches,
            fast_recovery_entries: self.fast_recovery_entries,
            economy_entries: self.economy_entries,
            tracking_entries: self.tracking_entries,
            fail_safe_entries: self.fail_safe_entries,
            last_fill_ratio: self.last_fill_ratio,
            smoothed_fill_ratio: self.smoothed_fill_ratio,
            last_sensor_available: self.last_sensor_available,
            last_push_wait_delta_ns: self.last_push_wait_delta_ns,
            last_pop_wait_delta_ns: self.last_pop_wait_delta_ns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferStats;

    fn stats(
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

    /// Fixed sleep ignores queue and sensor telemetry; adaptive does not.
    #[test]
    fn push_wait_activity_forces_fast_recovery_not_fixed_sleep() {
        let mut g = AdaptivePollingGovernor::new(AdaptivePollingConfig::default());
        let fixed_us = g.cfg().nominal_sleep_us;
        g.update(0, &stats(1024, 0, 0, 0));
        let d = g.update(0, &stats(1024, 0, 5000, 0));
        assert_eq!(g.stats().mode, GovernorMode::FastRecovery);
        assert_eq!(d.as_micros(), g.cfg().min_sleep_us as u128);
        assert_ne!(d.as_micros(), fixed_us as u128);
    }

    #[test]
    fn high_buffer_fill_triggers_fast_recovery() {
        let mut g = AdaptivePollingGovernor::new(AdaptivePollingConfig::default());
        let cap = 1000usize;
        let len = (cap as f64 * (g.cfg().high_fill_ratio + 0.05)) as usize;
        for _ in 0..30 {
            g.update(0, &stats(cap, len, 0, 0));
        }
        assert_eq!(g.stats().mode, GovernorMode::FastRecovery);
        assert_eq!(g.stats().current_sleep_us, g.cfg().min_sleep_us);
    }

    #[test]
    fn sensor_backlog_at_hard_threshold_triggers_fast_recovery() {
        let mut g = AdaptivePollingGovernor::new(AdaptivePollingConfig::default());
        let hard = g.cfg().hard_sensor_threshold;
        g.update(hard, &stats(4096, 0, 0, 0));
        assert_eq!(g.stats().mode, GovernorMode::FastRecovery);
    }

    #[test]
    fn economy_relaxes_sleep_when_consumer_runs_ahead_of_producers() {
        let mut g = AdaptivePollingGovernor::new(AdaptivePollingConfig::default());
        let cap = 10_000usize;
        let nominal = g.cfg().nominal_sleep_us;
        for i in 0..50 {
            let pop_ns = (i as u128 + 1) * 1_000_000u128;
            g.update(0, &stats(cap, 0, 0, pop_ns));
        }
        assert_eq!(g.stats().mode, GovernorMode::Economy);
        assert!(
            g.stats().current_sleep_us > nominal,
            "economy should grow sleep above nominal; got {}",
            g.stats().current_sleep_us
        );
        assert!(g.stats().current_sleep_us <= g.cfg().max_sleep_us);
    }

    #[test]
    fn force_fast_recovery_override_matches_emergency_sleep() {
        let mut g = AdaptivePollingGovernor::new(AdaptivePollingConfig::default());
        g.update_with_override(
            0,
            &stats(1000, 0, 0, 0),
            GovernorOverride::ForceFastRecovery,
        );
        assert_eq!(g.stats().mode, GovernorMode::FastRecovery);
        assert_eq!(g.stats().current_sleep_us, g.cfg().min_sleep_us);
    }

    #[test]
    fn clamp_max_sleep_caps_proposed_duration() {
        let mut cfg = AdaptivePollingConfig::default();
        cfg.max_sleep_us = 10_000;
        let mut g = AdaptivePollingGovernor::new(cfg);
        for i in 0..30 {
            let pop_ns = (i as u128 + 1) * 1_000_000u128;
            g.update_with_override(
                0,
                &stats(10_000, 0, 0, pop_ns),
                GovernorOverride::ClampMaxSleep(80),
            );
        }
        assert!(g.stats().current_sleep_us <= 80);
    }

    #[test]
    fn burst_then_idle_produces_distinct_modes_and_sleep_range() {
        let mut g = AdaptivePollingGovernor::new(AdaptivePollingConfig::default());
        let cap = 1000usize;
        for step in 1u32..=6 {
            let push_ns = u128::from(step) * 50_000u128;
            g.update(0, &stats(cap, 20, push_ns, 0));
        }
        assert!(g.stats().fast_recovery_entries >= 1);
        let min_seen = g.stats().current_sleep_us;
        let push_floor = 6 * 50_000u128;
        for i in 0..55 {
            let pop_ns = (i as u128 + 1) * 1_000_000u128;
            g.update(0, &stats(cap, 1, push_floor, pop_ns));
        }
        assert!(g.stats().economy_entries >= 1);
        assert!(g.stats().current_sleep_us > min_seen);
        assert!(g.stats().mode_switches >= 1);
    }
}

