//! Graceful shutdown, mirroring Aquifer's (shutdown.go, lifecycle.go).
//!
//! On SIGTERM (or Ctrl-C) Canalis drains instead of dying mid-request:
//!
//! 1. New work is refused with 503 + Retry-After + `X-Aqueduct-Node-State:
//!    draining` -- the same answer a draining Aquifer gives, so a gateway
//!    or load balancer in front of several Canalis nodes retries another
//!    one. `GET /ready` turns 503. Registrations and `GET /jobs/{key}`
//!    keep working.
//! 2. Every open WebSocket session gets Aquifer's own `server_draining`
//!    status, then a 1012 close after CANALIS_WS_DRAIN_GRACE_SECONDS; the
//!    client reconnects through the gateway and its transcript replays.
//! 3. Every caller waiting in the line gets `canalis_draining` and its
//!    stream closes. The job stays in the line, on disk, and this node
//!    drains it after it restarts.
//! 4. Nothing more is taken out of the line; handoffs already under way
//!    finish. Then the HTTP server stops, after letting in-flight requests
//!    complete.
//!
//! Whatever is still running after CANALIS_SHUTDOWN_TIMEOUT_SECONDS is
//! cut off, and the process exits.

use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct ShutdownConfig {
    /// Everything has to be done by then; the process exits regardless.
    pub timeout: Duration,
    /// How long to keep answering (with 503s) before closing anything, so
    /// whatever routes traffic here notices and stops.
    pub quiesce: Duration,
    /// How long an open WebSocket session gets between `server_draining`
    /// and its 1012 close.
    pub ws_grace: Duration,
}

impl ShutdownConfig {
    pub fn from_env() -> Self {
        Self {
            timeout: Duration::from_secs(crate::env_u64("CANALIS_SHUTDOWN_TIMEOUT_SECONDS", 30)),
            quiesce: Duration::from_millis(crate::env_u64("CANALIS_SHUTDOWN_QUIESCE_MS", 500)),
            ws_grace: Duration::from_secs(crate::env_u64("CANALIS_WS_DRAIN_GRACE_SECONDS", 5)),
        }
    }
}

pub struct Lifecycle {
    draining: AtomicBool,
    signal: watch::Sender<bool>,
    pub config: ShutdownConfig,
}

impl Lifecycle {
    pub fn new(config: ShutdownConfig) -> Self {
        Self { draining: AtomicBool::new(false), signal: watch::Sender::new(false), config }
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Starts draining. True the first time, false if already draining.
    pub fn begin(&self) -> bool {
        let first = !self.draining.swap(true, Ordering::SeqCst);
        if first {
            self.signal.send_replace(true);
        }
        first
    }

    /// Resolves once draining has begun (immediately if it already has).
    pub async fn drained(&self) {
        let mut rx = self.signal.subscribe();
        let _ = rx.wait_for(|draining| *draining).await;
    }
}

/// What new work gets while this node drains: the same shape a draining
/// Aquifer answers with, so whatever is in front retries elsewhere.
pub fn draining_response() -> Response {
    let mut response = (
        StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(serde_json::json!({ "error": "canalis is draining; retry through the gateway" })),
    )
        .into_response();
    let headers = response.headers_mut();
    headers.insert("Retry-After", HeaderValue::from(1u32));
    headers.insert("X-Aqueduct-Node-State", HeaderValue::from_static("draining"));
    response
}

/// SIGTERM or Ctrl-C, whichever comes first.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn begin_happens_once_and_wakes_everyone_waiting_for_it() {
        let lifecycle = std::sync::Arc::new(Lifecycle::new(ShutdownConfig {
            timeout: Duration::from_secs(1),
            quiesce: Duration::ZERO,
            ws_grace: Duration::ZERO,
        }));
        let waiter = {
            let lifecycle = lifecycle.clone();
            tokio::spawn(async move { lifecycle.drained().await })
        };
        assert!(!lifecycle.is_draining());
        assert!(lifecycle.begin());
        assert!(!lifecycle.begin(), "already draining");
        tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(1), lifecycle.drained()).await.unwrap();
    }

    #[test]
    fn the_draining_answer_matches_a_draining_aquifers() {
        let response = draining_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
        assert_eq!(response.headers()["x-aqueduct-node-state"], "draining");
    }
}
