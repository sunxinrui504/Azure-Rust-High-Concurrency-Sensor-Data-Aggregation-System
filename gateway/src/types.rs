use std::{collections::HashMap, time::SystemTime};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadingEnvelope {
    pub sensor_id: String,
    pub ts: SystemTime,
    /// Scalar value used for aggregation (see project design notes).
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyRecord {
    pub sensor_id: String,
    pub ts: SystemTime,
    pub value: f64,
    pub z: f64,
}

impl ReadingEnvelope {
    pub fn new(sensor_id: impl Into<String>, ts: SystemTime, value: f64) -> Self {
        Self {
            sensor_id: sensor_id.into(),
            ts,
            value,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SensorStats {
    pub count: u64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub m2: f64,
}

impl Default for SensorStats {
    fn default() -> Self {
        Self {
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl SensorStats {
    pub fn update(&mut self, x: f64) {
        if self.count == 0 {
            self.count = 1;
            self.min = x;
            self.max = x;
            self.mean = x;
            self.m2 = 0.0;
            return;
        }

        self.count += 1;
        if x < self.min {
            self.min = x;
        }
        if x > self.max {
            self.max = x;
        }

        // Welford update
        let delta = x - self.mean;
        self.mean += delta / (self.count as f64);
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    /// Merge two Welford states using Chan's parallel algorithm.
    pub fn merge(self, other: Self) -> Self {
        if self.count == 0 {
            return other;
        }
        if other.count == 0 {
            return self;
        }

        let n_a = self.count as f64;
        let n_b = other.count as f64;
        let n = n_a + n_b;
        let delta = other.mean - self.mean;

        let mean = self.mean + delta * (n_b / n);
        let m2 = self.m2 + other.m2 + delta * delta * (n_a * n_b / n);

        Self {
            count: (n as u64),
            min: self.min.min(other.min),
            max: self.max.max(other.max),
            mean,
            m2,
        }
    }

    pub fn variance_sample(&self) -> f64 {
        if self.count <= 1 {
            0.0
        } else {
            self.m2 / ((self.count - 1) as f64)
        }
    }

    pub fn stddev_sample(&self) -> f64 {
        self.variance_sample().sqrt()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedFrame {
    pub window_id: u64,
    pub window_start: SystemTime,
    pub window_end: SystemTime,
    pub per_sensor: HashMap<String, SensorStats>,
    pub anomalies: Vec<AnomalyRecord>,
}

