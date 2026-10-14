//! `GET /websocket`: WebSocket sessions, routed to an instance and piped.
//!
//! Aquifer's and Spillway's own `/websocket` (an `aqueduct.v1` session over
//! a Valkey transcript) already does the hard parts: ordering, replay from
//! a cursor, reconnecting to the backend, pacing. The transcript lives in
//! shared Valkey, so a client that reconnects to *any* instance replays
//! what it missed. Canalis's job here is the one a gateway has: pick the
//! instance, then move frames both ways without reading them.
//!
//! - Which instance: the same shared assignment table as HTTP (assign.rs),
//!   for WebSocket traffic -- only instances with `role: ws` or no role,
//!   not past CANALIS_QUEUE_AT_PERCENT for a new session. The user is the
//!   gateway's `X-Aqueduct-User-ID` header (the trusted gateway already
//!   injects `X-Aqueduct-Upstream-URL`), else the `session_id` query
//!   parameter.
//! - No instance has room: 503 with Retry-After at the handshake. (A
//!   waiting room for sockets is future work; for HTTP, see the line.)
//! - CANALIS_WS_MAX_CONNECTIONS caps sockets open through this Canalis node.
//! - The instance's handshake answer is relayed if it refuses (a 409
//!   replay gap, a 400, ...). A 503 because it's draining marks it draining
//!   and tries one other instance.
//! - An instance shutting down sends `server_draining` and closes with
//!   1012; that passes straight through, and the client's reconnect comes
//!   back through Canalis to an instance that isn't draining.

use crate::{assign, AppState};
use axum::extract::ws::{self, WebSocket, WebSocketUpgrade};
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

/// Sockets open through this Canalis node right now.
#[derive(Default)]
pub struct Sockets {
    open: AtomicUsize,
    max: usize,
}

impl Sockets {
    pub fn from_env() -> Self {
        Self { open: AtomicUsize::new(0), max: crate::env_u64("CANALIS_WS_MAX_CONNECTIONS", 10_000) as usize }
    }

    #[cfg(test)]
    pub fn with_max(max: usize) -> Self {
        Self { open: AtomicUsize::new(0), max }
    }

    pub fn open(&self) -> usize {
        self.open.load(Ordering::SeqCst)
    }

    fn try_open(self: &Arc<Self>) -> Option<OpenSocket> {
        let previous = self.open.fetch_add(1, Ordering::SeqCst);
        if previous >= self.max {
            self.open.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(OpenSocket(self.clone()))
    }
}

/// Holds one slot in `Sockets` until dropped.
struct OpenSocket(Arc<Sockets>);

impl Drop for OpenSocket {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::SeqCst);
    }
}

// Request headers that describe this connection or this handshake, not the
// session: never passed to the instance. Everything else the gateway sent
// (Authorization, X-Aqueduct-Upstream-URL, Sec-WebSocket-Protocol, ...) is.
const NOT_FORWARDED: [&str; 12] = [
    "host",
    "connection",
    "upgrade",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "sec-websocket-key",
    "sec-websocket-version",
    "sec-websocket-extensions",
    "content-length",
];

fn error(status: StatusCode, message: &str) -> Response {
    (status, axum::Json(serde_json::json!({ "error": message }))).into_response()
}

pub async fn handler(
    State(state): State<AppState>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if state.lifecycle.is_draining() {
        return crate::lifecycle::draining_response();
    }
    let Some(slot) = state.sockets.try_open() else {
        let mut response = error(StatusCode::SERVICE_UNAVAILABLE, "too many WebSocket connections on this Canalis node");
        response.headers_mut().insert("Retry-After", HeaderValue::from(1u32));
        return response;
    };
    let query = query.unwrap_or_default();
    let Some(user) = routing_key(&headers, &query) else {
        return error(StatusCode::BAD_REQUEST, "need an X-Aqueduct-User-ID header or a session_id query parameter");
    };

    let mut tried_draining = None;
    for _ in 0..2 {
        let mut valkey = state.valkey.clone();
        let node = match assign::assign(&mut valkey, &state.aqueduct, &user, assign::Traffic::WebSocket).await {
            Ok(assign::AssignOutcome::Assigned(node)) if Some(&node.id) != tried_draining.as_ref() => node,
            Ok(_) => {
                let mut response = error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "no WebSocket instance has room right now; retry through the gateway",
                );
                let retry = state.line.limits().retry_after_seconds;
                response.headers_mut().insert("Retry-After", HeaderValue::from(retry));
                return response;
            }
            Err(err) => {
                tracing::error!("assign failed for a WebSocket session: {err}");
                return error(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
            }
        };

        match connect(&node, &query, &headers).await {
            Ok((upstream, protocol)) => {
                let mut upgrade = upgrade;
                if let Some(protocol) = protocol {
                    upgrade = upgrade.protocols([protocol]);
                }
                return upgrade
                    .on_upgrade(move |client| pipe(state, client, upstream, node, user, slot))
                    .into_response();
            }
            Err(Refused::Draining) => {
                let mut valkey = state.valkey.clone();
                let _ = assign::mark_draining(&mut valkey, &state.aqueduct, &node.id).await;
                let _ = assign::release_ws_user(&mut valkey, &state.aqueduct, &user, &node.id).await;
                tracing::info!("{} is draining; trying another instance for a WebSocket session", node.id);
                tried_draining = Some(node.id);
            }
            Err(Refused::Answered(response)) => return response,
            Err(Refused::Unreachable(err)) => {
                tracing::error!("failed to reach {} for a WebSocket session: {err}", node.id);
                return error(StatusCode::BAD_GATEWAY, "failed to reach the assigned instance");
            }
        }
    }
    let mut response = error(StatusCode::SERVICE_UNAVAILABLE, "the assigned instances are draining; retry through the gateway");
    response.headers_mut().insert("Retry-After", HeaderValue::from(1u32));
    response
}

// The gateway's X-Aqueduct-User-ID, else the session id.
fn routing_key(headers: &HeaderMap, query: &str) -> Option<String> {
    if let Some(user) = headers.get("X-Aqueduct-User-ID").and_then(|v| v.to_str().ok()).map(str::trim) {
        if !user.is_empty() {
            return Some(user.to_string());
        }
    }
    reqwest::Url::parse(&format!("http://query/?{query}"))
        .ok()?
        .query_pairs()
        .find(|(k, _)| k == "session_id")
        .map(|(_, v)| v.into_owned())
        .filter(|v| !v.is_empty())
}

type Upstream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

enum Refused {
    Draining,
    Answered(Response),
    Unreachable(String),
}

// Opens the instance's /websocket with the client's query and end-to-end
// headers, before accepting the client's upgrade, so a refusal can still
// be answered as plain HTTP. Returns the subprotocol the instance chose.
async fn connect(node: &assign::Node, query: &str, headers: &HeaderMap) -> Result<(Upstream, Option<String>), Refused> {
    let base = node
        .base_url
        .strip_prefix("https://")
        .map(|rest| format!("wss://{rest}"))
        .or_else(|| node.base_url.strip_prefix("http://").map(|rest| format!("ws://{rest}")))
        .unwrap_or_else(|| node.base_url.clone());
    let url = if query.is_empty() { format!("{base}/websocket") } else { format!("{base}/websocket?{query}") };

    let mut request = url.into_client_request().map_err(|e| Refused::Unreachable(e.to_string()))?;
    for (name, value) in headers {
        if !NOT_FORWARDED.contains(&name.as_str()) {
            request.headers_mut().append(name.clone(), value.clone());
        }
    }

    match tokio_tungstenite::connect_async(request).await {
        Ok((upstream, response)) => {
            let protocol = response
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            Ok((upstream, protocol))
        }
        Err(tungstenite::Error::Http(refusal)) => {
            if crate::node_is_draining(refusal.status().as_u16(), refusal.headers()) {
                return Err(Refused::Draining);
            }
            let (parts, body) = refusal.into_parts();
            let mut response = Response::builder()
                .status(parts.status)
                .body(axum::body::Body::from(body.unwrap_or_default()))
                .unwrap_or_else(|_| error(StatusCode::BAD_GATEWAY, "invalid instance response"));
            crate::relay_headers(&parts.headers, response.headers_mut());
            Err(Refused::Answered(response))
        }
        Err(err) => Err(Refused::Unreachable(err.to_string())),
    }
}

// Moves frames both ways until either side closes, renewing the session's
// assignment while it's open so it can't idle out from under a long-lived
// socket. If this Canalis node starts draining, the client gets Aquifer's
// own `server_draining` status, then a 1012 close after the grace period,
// and reconnects through the gateway.
async fn pipe(state: AppState, client: WebSocket, upstream: Upstream, node: assign::Node, user: String, _slot: OpenSocket) {
    let (client_tx, mut client_rx) = client.split();
    let (upstream_tx, mut upstream_rx) = upstream.split();
    let client_tx = Arc::new(tokio::sync::Mutex::new(client_tx));
    let upstream_tx = Arc::new(tokio::sync::Mutex::new(upstream_tx));

    let to_upstream = {
        let upstream_tx = upstream_tx.clone();
        async move {
            while let Some(Ok(message)) = client_rx.next().await {
                let closing = matches!(message, ws::Message::Close(_));
                if upstream_tx.lock().await.send(to_tungstenite(message)).await.is_err() || closing {
                    break;
                }
            }
        }
    };
    let to_client = {
        let client_tx = client_tx.clone();
        async move {
            while let Some(Ok(message)) = upstream_rx.next().await {
                let Some(message) = to_axum(message) else { continue };
                let closing = matches!(message, ws::Message::Close(_));
                if client_tx.lock().await.send(message).await.is_err() || closing {
                    break;
                }
            }
        }
    };
    let renew = async {
        let every = (state.aqueduct.assignment_ttl / 3).max(Duration::from_secs(1));
        let mut last = Instant::now();
        loop {
            tokio::time::sleep(every.saturating_sub(last.elapsed())).await;
            let _ = assign::assign(&mut state.valkey.clone(), &state.aqueduct, &user, assign::Traffic::WebSocket).await;
            last = Instant::now();
        }
    };
    let canalis_draining = {
        let client_tx = client_tx.clone();
        let lifecycle = state.lifecycle.clone();
        async move {
            lifecycle.drained().await;
            let grace = lifecycle.config.ws_grace;
            let notice = serde_json::json!({
                "type": "status",
                "state": "server_draining",
                "reason": "reconnect through the gateway",
                "retry_after_ms": grace.as_millis() as u64,
            });
            let _ = client_tx.lock().await.send(ws::Message::Text(notice.to_string().into())).await;
            tokio::time::sleep(grace).await;
            let close = ws::CloseFrame { code: 1012, reason: "canalis is draining; reconnect through the gateway".into() };
            let _ = client_tx.lock().await.send(ws::Message::Close(Some(close))).await;
        }
    };

    tokio::select! {
        _ = to_upstream => {}
        _ = to_client => {}
        _ = renew => {}
        _ = canalis_draining => {}
    }
    let _ = client_tx.lock().await.close().await;
    let _ = upstream_tx.lock().await.close().await;
    tracing::debug!("WebSocket session for {user} on {} closed", node.id);
}

fn to_tungstenite(message: ws::Message) -> tungstenite::Message {
    match message {
        ws::Message::Text(text) => tungstenite::Message::text(text.as_str()),
        ws::Message::Binary(bytes) => tungstenite::Message::Binary(bytes),
        ws::Message::Ping(bytes) => tungstenite::Message::Ping(bytes),
        ws::Message::Pong(bytes) => tungstenite::Message::Pong(bytes),
        ws::Message::Close(frame) => tungstenite::Message::Close(frame.map(|f| tungstenite::protocol::CloseFrame {
            code: f.code.into(),
            reason: f.reason.as_str().into(),
        })),
    }
}

fn to_axum(message: tungstenite::Message) -> Option<ws::Message> {
    Some(match message {
        tungstenite::Message::Text(text) => ws::Message::Text(text.as_str().into()),
        tungstenite::Message::Binary(bytes) => ws::Message::Binary(bytes),
        tungstenite::Message::Ping(bytes) => ws::Message::Ping(bytes),
        tungstenite::Message::Pong(bytes) => ws::Message::Pong(bytes),
        tungstenite::Message::Close(frame) => ws::Message::Close(frame.map(|f| ws::CloseFrame {
            code: f.code.into(),
            reason: f.reason.as_str().into(),
        })),
        tungstenite::Message::Frame(_) => return None,
    })
}
