use hotaru::prelude::*;
use hotaru::http::*;
use std::fs;
use serde_json::Value;

mod helpers;

pub static APP: SApp = Lazy::new(|| {
    App::new()
        .binding("127.0.0.1:3000")
        .build()
});

fn json_file_response(path: &str) -> HttpResponse {
    let resolved = crate::resource::locate_resource(path).unwrap_or_else(|| path.into());
    match fs::read(&resolved) {
        Ok(bytes) => response_templates::normal_response(StatusCode::OK, bytes)
            .content_type(HttpContentType::ApplicationJson()),
        Err(_) => response_templates::normal_response(StatusCode::NOT_FOUND, b"{\"error\":\"not found\"}")
            .content_type(HttpContentType::ApplicationJson()),
    }
}

fn json_bytes_response(status: StatusCode, bytes: Vec<u8>) -> HttpResponse {
    response_templates::normal_response(status, bytes).content_type(HttpContentType::ApplicationJson())
}

fn json_value_response(status: StatusCode, v: Value) -> HttpResponse {
    match serde_json::to_vec(&v) {
        Ok(bytes) => json_bytes_response(status, bytes),
        Err(_) => json_bytes_response(StatusCode::INTERNAL_SERVER_ERROR, br#"{"error":"serialize"}"#.to_vec()),
    }
}

fn read_json_value(path: &str) -> Result<Value, ()> {
    let resolved = crate::resource::locate_resource(path).unwrap_or_else(|| path.into());
    let bytes = fs::read(&resolved).map_err(|_| ())?;
    serde_json::from_slice(&bytes).map_err(|_| ())
}

fn read_runtime_snapshot_field(key: &str) -> Result<Value, ()> {
    let snapshot = read_json_value("data/snapshot.json")?;
    snapshot.get(key).cloned().ok_or(())
}

endpoint! {
    APP.url("/"),
    pub index<HTTP> {
        html_response(br#"<!doctype html>
<html>
  <head><meta charset="utf-8"><title>Project Azure</title></head>
  <body>
    <h1>Project Azure Dashboard</h1>
    <ul>
      <li><a href="/latest">/latest</a></li>
      <li><a href="/stats">/stats</a></li>
      <li>/sensor/&lt;id&gt;:
        <a href="/sensor/thermo-1">thermo-1</a>,
        <a href="/sensor/thermo-2">thermo-2</a>,
        <a href="/sensor/accel-1">accel-1</a>,
        <a href="/sensor/accel-2">accel-2</a>,
        <a href="/sensor/force-1">force-1</a>
      </li>
    </ul>
  </body>
</html>"#)
    }
} 

endpoint! {
    APP.url("/latest"),
    pub latest<HTTP> {
        match read_runtime_snapshot_field("latest") {
            Ok(v) => json_value_response(StatusCode::OK, v),
            Err(_) => json_file_response("data/latest.json"),
        }
    }
}

endpoint! {
    APP.url("/stats"),
    pub stats<HTTP> {
        match read_runtime_snapshot_field("stats") {
            Ok(v) => json_value_response(StatusCode::OK, v),
            Err(_) => json_file_response("data/stats.json"),
        }
    }
}

endpoint! {
    APP.url("/sensor/<id>"),
    pub sensor<HTTP> {
        let sensor_id = req.param("id").unwrap_or_else(|| "".to_string());
        if sensor_id.is_empty() {
            return json_value_response(StatusCode::BAD_REQUEST, serde_json::json!({"error":"missing sensor id"}));
        }

        let since = helpers::parse_u64_opt(req.query("since")); // unix secs, optional
        let until = helpers::parse_u64_opt(req.query("until")); // unix secs, optional

        // If time range is provided, scan hourly logs and return an array.
        if since.is_some() || until.is_some() {
            let since_s = since.unwrap_or(0);
            let until_s = until.unwrap_or(u64::MAX);
            if since_s > until_s {
                return json_value_response(StatusCode::BAD_REQUEST, serde_json::json!({"error":"since > until"}));
            }

            let start_hour = (since_s / 3600) as i64;
            let end_hour = (until_s / 3600) as i64;
            // guard against huge ranges
            if end_hour.saturating_sub(start_hour) > 24 * 14 {
                return json_value_response(StatusCode::BAD_REQUEST, serde_json::json!({"error":"range too large (max 14 days)"}));
            }

            let mut out: Vec<Value> = Vec::new();
            for h in start_hour..=end_hour {
                let rel = format!("data/frames/hour-{}.jsonl", h);
                let resolved = crate::resource::locate_resource(&rel).unwrap_or_else(|| rel.clone().into());
                let Ok(content) = fs::read_to_string(&resolved) else { continue; };
                for line in content.lines() {
                    let Ok(frame) = serde_json::from_str::<Value>(line) else { continue; };

                    let end_secs = frame
                        .get("window_end")
                        .and_then(helpers::unix_secs_from_systemtime_value)
                        .unwrap_or(0);
                    if end_secs < since_s || end_secs > until_s {
                        continue;
                    }

                    let Some(per_sensor) = frame.get("per_sensor") else { continue; };
                    let Some(stats) = per_sensor.get(&sensor_id) else { continue; };

                    let stddev = helpers::stddev_from_stats(stats);
                    let anomalies = frame
                        .get("anomalies")
                        .and_then(|a| a.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter(|x| x.get("sensor_id").and_then(|s| s.as_str()) == Some(sensor_id.as_str()))
                                .cloned()
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();

                    out.push(serde_json::json!({
                        "window_id": frame.get("window_id"),
                        "window_start": frame.get("window_start"),
                        "window_end": frame.get("window_end"),
                        "sensor_id": sensor_id,
                        "stats": stats,
                        "stddev_sample": stddev,
                        "anomalies": anomalies,
                    }));
                }
            }

            return json_value_response(StatusCode::OK, serde_json::json!({
                "sensor_id": sensor_id,
                "since": since,
                "until": until,
                "frames": out,
            }));
        }

        // Otherwise, return latest frame filtered to this sensor.
        let Ok(latest) = read_runtime_snapshot_field("latest").or_else(|_| read_json_value("data/latest.json")) else {
            return json_value_response(StatusCode::NOT_FOUND, serde_json::json!({"error":"latest not found"}));
        };
        let per_sensor = latest.get("per_sensor");
        let stats = per_sensor.and_then(|m| m.get(&sensor_id));
        let Some(stats) = stats else {
            return json_value_response(StatusCode::NOT_FOUND, serde_json::json!({"error":"sensor not found in latest"}));
        };

        let stddev = helpers::stddev_from_stats(stats);
        let anomalies = latest
            .get("anomalies")
            .and_then(|a| a.as_array())
            .map(|arr| {
                arr.iter()
                    .filter(|x| x.get("sensor_id").and_then(|s| s.as_str()) == Some(sensor_id.as_str()))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        json_value_response(StatusCode::OK, serde_json::json!({
            "window_id": latest.get("window_id"),
            "window_start": latest.get("window_start"),
            "window_end": latest.get("window_end"),
            "sensor_id": sensor_id,
            "stats": stats,
            "stddev_sample": stddev,
            "anomalies": anomalies,
        }))
    }
}

pub mod resource; 
