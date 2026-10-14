//! The scale-up signal: tells whatever adds instances that the line is
//! backing up.
//!
//! The line exists for the gap between "more capacity is needed" and "more
//! capacity is running" -- it should never be how a fleet that isn't
//! growing copes. So when a job has waited at least CANALIS_SCALE_AFTER_SECONDS
//! for an instance, Canalis POSTs to CANALIS_SCALE_WEBHOOK_URL (at most once
//! per CANALIS_SCALE_WEBHOOK_INTERVAL_SECONDS), and whatever starts
//! instances -- a script against Fly's Machines API, a Kubernetes
//! controller -- decides what to do. Canalis doesn't start infrastructure
//! itself. The same numbers are in GET /health for anything that polls
//! instead.
//!
//! ```json
//! {"event":"canalis_scale_up","line_jobs":42,"oldest_wait_seconds":17,
//!  "instances":{"total":3,"accepting":0},"at":1792000000}
//! ```
//!
//! Every Canalis node sends its own; a receiver behind several should
//! treat them as the same signal.

use crate::assign::Fleet;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub struct ScaleSignal {
    url: Option<String>,
    after: Duration,
    every: Duration,
    http: reqwest::Client,
    last_sent: Mutex<Option<Instant>>,
}

impl ScaleSignal {
    pub fn new(url: Option<String>, after: Duration, every: Duration) -> Self {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap_or_default();
        Self { url, after, every, http, last_sent: Mutex::default() }
    }

    pub fn from_env() -> Self {
        Self::new(
            std::env::var("CANALIS_SCALE_WEBHOOK_URL").ok().filter(|u| !u.trim().is_empty()),
            Duration::from_secs(crate::env_u64("CANALIS_SCALE_AFTER_SECONDS", 10)),
            Duration::from_secs(crate::env_u64("CANALIS_SCALE_WEBHOOK_INTERVAL_SECONDS", 60)),
        )
    }

    /// Sends the signal if the line has backed up long enough and it
    /// wasn't sent too recently. Returns whether it was sent.
    pub async fn check(&self, line_jobs: u64, oldest_wait_seconds: Option<u64>, fleet: Fleet) -> bool {
        let Some(url) = &self.url else { return false };
        let Some(oldest) = oldest_wait_seconds else { return false };
        if line_jobs == 0 || Duration::from_secs(oldest) < self.after {
            return false;
        }
        {
            let mut last = self.last_sent.lock().unwrap_or_else(|e| e.into_inner());
            if last.is_some_and(|at| at.elapsed() < self.every) {
                return false;
            }
            *last = Some(Instant::now());
        }
        let payload = serde_json::json!({
            "event": "canalis_scale_up",
            "line_jobs": line_jobs,
            "oldest_wait_seconds": oldest,
            "instances": fleet,
            "at": SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or_default(),
        });
        match self.http.post(url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!("scale-up signal sent: {line_jobs} jobs waiting, oldest {oldest}s");
                true
            }
            Ok(resp) => {
                tracing::warn!("scale-up webhook answered {}", resp.status());
                true
            }
            Err(err) => {
                tracing::warn!("scale-up webhook failed: {err}");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Json, Router};
    use std::sync::Arc;

    async fn receiver() -> (String, Arc<Mutex<Vec<serde_json::Value>>>) {
        let got = Arc::new(Mutex::new(Vec::new()));
        let seen = got.clone();
        let app = Router::new().route(
            "/scale",
            post(move |Json(body): Json<serde_json::Value>| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(body);
                    "ok"
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/scale"), got)
    }

    #[tokio::test]
    async fn a_backed_up_line_sends_the_signal_once_per_interval() {
        let (url, got) = receiver().await;
        let signal = ScaleSignal::new(Some(url), Duration::from_secs(10), Duration::from_secs(60));
        let fleet = Fleet { total: 3, accepting: 0 };

        assert!(!signal.check(5, Some(9), fleet).await, "not waiting long enough yet");
        assert!(signal.check(5, Some(12), fleet).await);
        assert!(!signal.check(6, Some(20), fleet).await, "already sent within the interval");

        let got = got.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["event"], "canalis_scale_up");
        assert_eq!(got[0]["line_jobs"], 5);
        assert_eq!(got[0]["oldest_wait_seconds"], 12);
        assert_eq!(got[0]["instances"]["total"], 3);
        assert_eq!(got[0]["instances"]["accepting"], 0);
    }

    #[tokio::test]
    async fn no_url_or_an_empty_line_sends_nothing() {
        let (url, got) = receiver().await;
        let fleet = Fleet::default();
        assert!(!ScaleSignal::new(None, Duration::ZERO, Duration::ZERO).check(5, Some(99), fleet).await);
        assert!(!ScaleSignal::new(Some(url), Duration::ZERO, Duration::ZERO).check(0, None, fleet).await);
        assert!(got.lock().unwrap().is_empty());
    }
}
