use serde_json::Value;

pub(crate) fn parse_u64_opt(s: Option<String>) -> Option<u64> {
    s.and_then(|v| v.parse::<u64>().ok())
}

pub(crate) fn unix_secs_from_systemtime_value(v: &Value) -> Option<u64> {
    v.get("secs_since_epoch")?.as_u64()
}

pub(crate) fn stddev_from_stats(stats: &Value) -> Option<f64> {
    let count = stats.get("count")?.as_u64()?;
    if count <= 1 {
        return Some(0.0);
    }
    let m2 = stats.get("m2")?.as_f64()?;
    Some((m2 / ((count - 1) as f64)).sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_u64_opt_parses_digits() {
        assert_eq!(parse_u64_opt(Some("42".into())), Some(42));
        assert_eq!(parse_u64_opt(Some("x".into())), None);
        assert_eq!(parse_u64_opt(None), None);
    }

    #[test]
    fn unix_secs_from_systemtime_value_reads_field() {
        let v = json!({"secs_since_epoch": 1_700_000_000u64});
        assert_eq!(unix_secs_from_systemtime_value(&v), Some(1_700_000_000));
        assert_eq!(unix_secs_from_systemtime_value(&json!({})), None);
    }

    #[test]
    fn stddev_sample_at_most_one_count_returns_zero_or_none() {
        assert_eq!(stddev_from_stats(&json!({"count": 1, "m2": 10.0})), Some(0.0));
        assert_eq!(stddev_from_stats(&json!({"count": 0, "m2": 1.0})), Some(0.0));
        assert_eq!(stddev_from_stats(&json!({"m2": 1.0})), None);
    }

    #[test]
    fn stddev_sample_two_observations() {
        let stats = json!({"count": 2u64, "m2": 1.0});
        let s = stddev_from_stats(&stats).unwrap();
        assert!((s - 1.0).abs() < 1e-9);
    }
}
