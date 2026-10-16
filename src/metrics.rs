//! Prometheus metrics at GET /metrics, in the plain text exposition format
//! -- written out by hand rather than pulled in from a crate, since it's a
//! few counters and one histogram.
//!
//! Counters live here and are bumped where things happen. Gauges (the
//! line, the fleet, open sockets) are read fresh on every scrape from the
//! same places /health reads them, so they can never drift.
//!
//! Every response is counted by route and status
//! (canalis_http_responses_total), which already covers 429s (line full),
//! 422s (schema gate), 401s (refused registrations) and 503s (draining).
//! canalis_http_request_duration_seconds is time to the response's
//! headers: for a waiting-room stream or a WebSocket that's when the
//! caller was let in, not how long they stayed.

use crate::{AppState, assign};
use axum::{
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const BUCKETS: [f64; 11] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

const COUNTERS: [(&str, &str); 8] = [
    ("canalis_http_responses_total", "Responses sent, by route and status."),
    ("canalis_line_rejected_total", "Jobs turned away because the line was at a limit, by limit."),
    ("canalis_reroutes_total", "Requests and sessions re-routed because their instance was draining or unreachable."),
    ("canalis_scale_signals_total", "canalis_scale_up webhooks sent."),
    ("canalis_line_forwarded_total", "Requests passed on to the Canalis node holding their tenant's line."),
    ("canalis_shed_total", "Requests answered 503 because CANALIS_MAX_IN_FLIGHT requests were already in progress."),
    ("canalis_auth_rejected_total", "Requests refused for missing or invalid credentials, or because the provider's keys couldn't be fetched."),
    ("canalis_websocket_wait_timeouts_total", "WebSocket sessions closed with 1013 after waiting too long for an instance."),
];

#[derive(Default)]
struct Histogram {
    buckets: [u64; BUCKETS.len()],
    sum: f64,
    count: u64,
}

#[derive(Default)]
pub struct Metrics {
    // Keyed by the full series, e.g. `canalis_reroutes_total` or
    // `canalis_line_rejected_total{reason="max_jobs"}`.
    counters: Mutex<BTreeMap<String, u64>>,
    durations: Mutex<BTreeMap<String, Histogram>>,
}

impl Metrics {
    pub fn inc(&self, name: &str, labels: &[(&str, &str)]) {
        *self.counters.lock().unwrap_or_else(|e| e.into_inner()).entry(series(name, labels)).or_default() += 1;
    }

    fn observe(&self, route: &str, status: u16, took: Duration) {
        self.inc("canalis_http_responses_total", &[("route", route), ("status", &status.to_string())]);
        let seconds = took.as_secs_f64();
        let mut durations = self.durations.lock().unwrap_or_else(|e| e.into_inner());
        let h = durations.entry(route.to_string()).or_default();
        for (i, bound) in BUCKETS.iter().enumerate() {
            if seconds <= *bound {
                h.buckets[i] += 1;
            }
        }
        h.sum += seconds;
        h.count += 1;
    }

    fn write_counters(&self, out: &mut String) {
        let counters = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        for (name, help) in COUNTERS {
            let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} counter");
            for (key, value) in counters.iter().filter(|(k, _)| k.split('{').next() == Some(name)) {
                let _ = writeln!(out, "{key} {value}");
            }
        }
        let name = "canalis_http_request_duration_seconds";
        let _ = writeln!(out, "# HELP {name} Time until the response's headers were sent, by route.\n# TYPE {name} histogram");
        for (route, h) in self.durations.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            for (bound, n) in BUCKETS.iter().zip(h.buckets) {
                let _ = writeln!(out, "{} {n}", series(&format!("{name}_bucket"), &[("route", route), ("le", &bound.to_string())]));
            }
            let _ = writeln!(out, "{} {}", series(&format!("{name}_bucket"), &[("route", route), ("le", "+Inf")]), h.count);
            let _ = writeln!(out, "{} {}", series(&format!("{name}_sum"), &[("route", route)]), h.sum);
            let _ = writeln!(out, "{} {}", series(&format!("{name}_count"), &[("route", route)]), h.count);
        }
    }
}

fn series(name: &str, labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    let labels: Vec<String> = labels.iter().map(|(k, v)| format!("{k}=\"{}\"", escape(v))).collect();
    format!("{name}{{{}}}", labels.join(","))
}

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

fn gauge(out: &mut String, name: &str, help: &str, samples: &[(Vec<(&str, &str)>, f64)]) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge");
    for (labels, value) in samples {
        let _ = writeln!(out, "{} {value}", series(name, labels));
    }
}

/// Route layer: counts and times every response by its matched route, so
/// /jobs/{idempotent_key} is one series, not one per key.
pub async fn track(State(state): State<AppState>, route: Option<MatchedPath>, request: Request, next: Next) -> Response {
    let route = route.map(|r| r.as_str().to_string()).unwrap_or_else(|| "unmatched".into());
    let started = Instant::now();
    let response = next.run(request).await;
    state.metrics.observe(&route, response.status().as_u16(), started.elapsed());
    response
}

pub async fn handler(State(state): State<AppState>) -> Response {
    let mut out = String::new();
    state.metrics.write_counters(&mut out);

    let line = state.line.clone();
    let (jobs, oldest, leased) = tokio::task::spawn_blocking(move || {
        (line.len().unwrap_or(0), line.oldest_wait_seconds().ok().flatten(), line.leased().unwrap_or(0))
    })
    .await
    .unwrap_or((0, None, 0));
    gauge(&mut out, "canalis_line_jobs", "Jobs waiting in this node's line.", &[(vec![], jobs as f64)]);
    gauge(&mut out, "canalis_line_in_flight", "Jobs taken out of line and not yet handed off.", &[(vec![], leased as f64)]);
    gauge(&mut out, "canalis_line_oldest_wait_seconds", "How long the oldest job in line has waited (0 when empty).", &[(vec![], oldest.unwrap_or(0) as f64)]);
    let waiters = state.waiters.lock().map(|w| w.len()).unwrap_or(0);
    gauge(&mut out, "canalis_line_streams", "Callers holding a waiting-room stream open.", &[(vec![], waiters as f64)]);
    gauge(&mut out, "canalis_websocket_open", "WebSocket sessions open through this node.", &[(vec![], state.sockets.open() as f64)]);
    gauge(&mut out, "canalis_websocket_waiting", "WebSocket sessions waiting for an instance with room.", &[(vec![], state.ws_room.len() as f64)]);
    gauge(&mut out, "canalis_in_flight", "Proxied requests in progress (see CANALIS_MAX_IN_FLIGHT).", &[(vec![], state.shedder.in_flight() as f64)]);
    gauge(&mut out, "canalis_draining", "1 while this node is shutting down.", &[(vec![], if state.lifecycle.is_draining() { 1.0 } else { 0.0 })]);

    // Instances come from Valkey; if it can't be read, say so rather than
    // reporting an empty fleet.
    let instances = assign::instances(&mut state.valkey.clone(), &state.aqueduct).await;
    gauge(&mut out, "canalis_valkey_up", "1 if this scrape could read the instance table from Valkey.", &[(vec![], if instances.is_ok() { 1.0 } else { 0.0 })]);
    if let Ok(instances) = instances {
        let accepting = instances.iter().filter(|(m, f)| m.is_active() && f.is_none_or(|f| f < state.aqueduct.queue_at_percent)).count();
        gauge(&mut out, "canalis_instances_accepting", "Live instances that would take a new user now.", &[(vec![], accepting as f64)]);
        let state_of = |m: &assign::Member| m.state.clone().unwrap_or_else(|| "active".into());
        let states: Vec<String> = instances.iter().map(|(m, _)| state_of(m)).collect();
        let pools: Vec<String> = instances
            .iter()
            .map(|(m, _)| if m.pools.is_empty() { assign::DEFAULT_POOL.to_string() } else { m.pools.join(",") })
            .collect();
        let labels = |i: usize| {
            vec![("instance", instances[i].0.id.as_str()), ("state", states[i].as_str()), ("pools", pools[i].as_str())]
        };
        let each = |f: &dyn Fn(usize) -> Option<f64>| -> Vec<(Vec<(&str, &str)>, f64)> {
            (0..instances.len()).filter_map(|i| Some((labels(i), f(i)?))).collect()
        };
        gauge(&mut out, "canalis_instance_up", "Each registered instance, labelled with its state.", &each(&|_| Some(1.0)));
        gauge(&mut out, "canalis_instance_fullness_percent", "Fullness the instance last reported (X-Aqueduct-Fullness).", &each(&|i| instances[i].1.map(f64::from)));
        gauge(&mut out, "canalis_instance_active_users", "Users the instance reported as active.", &each(&|i| Some(instances[i].0.active_users as f64)));
        gauge(&mut out, "canalis_instance_user_capacity", "Users the instance said it can hold.", &each(&|i| Some(instances[i].0.capacity as f64)));
    }

    ([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], out).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_render_with_escaped_labels() {
        let m = Metrics::default();
        m.inc("canalis_line_rejected_total", &[("reason", "max_jobs")]);
        m.inc("canalis_line_rejected_total", &[("reason", "max_jobs")]);
        m.inc("canalis_reroutes_total", &[]);
        m.observe("/jobs/{idempotent_key}", 200, Duration::from_millis(30));
        let mut out = String::new();
        m.write_counters(&mut out);
        assert!(out.contains("canalis_line_rejected_total{reason=\"max_jobs\"} 2\n"), "{out}");
        assert!(out.contains("canalis_reroutes_total 1\n"));
        assert!(out.contains("canalis_http_responses_total{route=\"/jobs/{idempotent_key}\",status=\"200\"} 1\n"));
        assert!(out.contains("canalis_http_request_duration_seconds_bucket{route=\"/jobs/{idempotent_key}\",le=\"0.025\"} 0\n"));
        assert!(out.contains("canalis_http_request_duration_seconds_bucket{route=\"/jobs/{idempotent_key}\",le=\"0.05\"} 1\n"));
        assert!(out.contains("canalis_http_request_duration_seconds_count{route=\"/jobs/{idempotent_key}\"} 1\n"));
        assert_eq!(series("x", &[("a", "say \"hi\"\n")]), "x{a=\"say \\\"hi\\\"\\n\"}");
    }
}
