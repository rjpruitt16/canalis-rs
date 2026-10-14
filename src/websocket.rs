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
//! - No instance has room: the session waits in Canalis's WebSocket
//!   waiting room, oldest first, hearing `{"type":"status","state":
//!   "canalis_waiting","position":N}` as the line moves (Canalis-prefixed,
//!   so it's never mistaken for an instance's own `waiting`), then
//!   `canalis_assigned` once an instance takes it. While anyone is waiting,
//!   new sessions join the back rather than taking freed room first. Past
//!   CANALIS_WS_MAX_WAITING the handshake gets 503 + Retry-After; past
//!   CANALIS_WS_WAIT_TIMEOUT_SECONDS the session closes with 1013.
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
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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

/// Sessions waiting for an instance with room, oldest first.
pub struct WaitingRoom {
    queue: Mutex<VecDeque<WaitingSession>>,
    next_id: AtomicU64,
    max_waiting: usize,
    timeout: Duration,
}

struct WaitingSession {
    id: u64,
    user: String,
    ready: tokio::sync::oneshot::Sender<assign::Node>,
}

impl WaitingRoom {
    pub fn new(max_waiting: usize, timeout: Duration) -> Self {
        Self { queue: Mutex::default(), next_id: AtomicU64::new(1), max_waiting, timeout }
    }

    pub fn from_env() -> Self {
        Self::new(
            crate::env_u64("CANALIS_WS_MAX_WAITING", 1_000) as usize,
            Duration::from_secs(crate::env_u64("CANALIS_WS_WAIT_TIMEOUT_SECONDS", 60)),
        )
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<WaitingSession>> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    // Joins the back (or, after a draining instance turned it away, the
    // front) of the line; None when the room is full.
    fn join(&self, user: &str, front: bool) -> Option<(u64, tokio::sync::oneshot::Receiver<assign::Node>)> {
        let mut queue = self.lock();
        if !front && queue.len() >= self.max_waiting {
            return None;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (ready, rx) = tokio::sync::oneshot::channel();
        let session = WaitingSession { id, user: user.to_string(), ready };
        if front { queue.push_front(session) } else { queue.push_back(session) }
        Some((id, rx))
    }

    // 1-based place in line, or None once it has left.
    fn position(&self, id: u64) -> Option<usize> {
        self.lock().iter().position(|w| w.id == id).map(|i| i + 1)
    }

    fn leave(&self, id: u64) {
        self.lock().retain(|w| w.id != id);
    }
}

/// Hands instances with room to waiting sessions, oldest first. Stops at
/// the first one that still can't be placed, so nobody behind it jumps
/// ahead. Runs on a short interval.
pub async fn hand_out(state: &AppState) {
    loop {
        let front = {
            let mut queue = state.ws_room.lock();
            while queue.front().is_some_and(|w| w.ready.is_closed()) {
                queue.pop_front();
            }
            queue.front().map(|w| (w.id, w.user.clone()))
        };
        let Some((id, user)) = front else { return };
        let mut valkey = state.valkey.clone();
        let Ok(assign::AssignOutcome::Assigned(node)) =
            assign::assign(&mut valkey, &state.aqueduct, &user, assign::Traffic::WebSocket).await
        else {
            return;
        };
        let session = {
            let mut queue = state.ws_room.lock();
            match queue.front() {
                Some(w) if w.id == id => queue.pop_front(),
                _ => None,
            }
        };
        let handed = session.is_some_and(|s| s.ready.send(node.clone()).is_ok());
        if !handed {
            let _ = assign::release_ws_user(&mut valkey, &state.aqueduct, &user, &node.id).await;
        }
    }
}

fn status(state: &str, extra: serde_json::Value) -> ws::Message {
    let mut message = serde_json::json!({ "type": "status", "state": state });
    if let (Some(m), Some(extra)) = (message.as_object_mut(), extra.as_object()) {
        m.extend(extra.clone());
    }
    ws::Message::Text(message.to_string().into())
}

async fn close(client: &mut WebSocket, code: u16, reason: &str) {
    let _ = client.send(ws::Message::Close(Some(ws::CloseFrame { code, reason: reason.into() }))).await;
}

// Everything a waiting session needs to connect once it's handed an
// instance.
struct Pending {
    user: String,
    query: String,
    headers: HeaderMap,
}

// A session in the waiting room: keeps the client informed of its place,
// connects it to the instance it's handed, and gives up cleanly on
// timeout, a client that leaves, or this node draining.
async fn wait_for_instance(
    state: AppState,
    mut client: WebSocket,
    mut id: u64,
    mut ready: tokio::sync::oneshot::Receiver<assign::Node>,
    pending: Pending,
    slot: OpenSocket,
) {
    let deadline = tokio::time::sleep(state.ws_room.timeout);
    tokio::pin!(deadline);
    let mut told = None;
    loop {
        let position = state.ws_room.position(id);
        if position.is_some() && position != told {
            let _ = client.send(status("canalis_waiting", serde_json::json!({ "position": position }))).await;
            told = position;
        }
        let node = tokio::select! {
            node = &mut ready => match node {
                Ok(node) => node,
                Err(_) => return,
            },
            message = client.recv() => match message {
                None | Some(Err(_)) | Some(Ok(ws::Message::Close(_))) => {
                    state.ws_room.leave(id);
                    return;
                }
                // Nothing goes anywhere before there's an instance to send
                // it to.
                Some(Ok(_)) => continue,
            },
            _ = tokio::time::sleep(Duration::from_millis(500)) => continue,
            _ = &mut deadline => {
                state.ws_room.leave(id);
                state.metrics.inc("canalis_websocket_wait_timeouts_total", &[]);
                close(&mut client, 1013, "no WebSocket instance had room in time; retry through the gateway").await;
                return;
            }
            _ = state.lifecycle.drained() => {
                state.ws_room.leave(id);
                let _ = client.send(status("server_draining", serde_json::json!({ "reason": "reconnect through the gateway", "retry_after_ms": 1000 }))).await;
                close(&mut client, 1012, "canalis is draining; reconnect through the gateway").await;
                return;
            }
        };

        match connect(&node, &pending.query, &pending.headers).await {
            Ok((upstream, _)) => {
                let _ = client.send(status("canalis_assigned", serde_json::json!({}))).await;
                pipe(state, client, upstream, node, pending.user, slot).await;
                return;
            }
            Err(Refused::Draining) => {
                let mut valkey = state.valkey.clone();
                let _ = assign::mark_draining(&mut valkey, &state.aqueduct, &node.id).await;
                let _ = assign::release_ws_user(&mut valkey, &state.aqueduct, &pending.user, &node.id).await;
                // Back to the front: it already waited its turn.
                let Some((new_id, new_ready)) = state.ws_room.join(&pending.user, true) else { return };
                (id, ready) = (new_id, new_ready);
                told = None;
            }
            Err(Refused::Answered(response)) => {
                let reason = format!("the instance refused the session ({})", response.status());
                close(&mut client, 1011, &reason).await;
                return;
            }
            // Nothing was established, so it's safe to try another
            // instance: take this one out of rotation (its next ping puts
            // it back) and wait at the front again.
            Err(Refused::Unreachable(err)) => {
                tracing::warn!("{} is unreachable for a waiting WebSocket session; removed it until it registers again: {err}", node.id);
                let _ = assign::remove_instance(&mut state.valkey.clone(), &state.aqueduct, &node.id).await;
                state.metrics.inc("canalis_reroutes_total", &[]);
                let Some((new_id, new_ready)) = state.ws_room.join(&pending.user, true) else { return };
                (id, ready) = (new_id, new_ready);
                told = None;
            }
        }
    }
}

// The subprotocols the client offered, so a session accepted into the
// waiting room (before any instance is chosen) agrees on one.
fn offered_protocols(headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all("Sec-WebSocket-Protocol")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
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

    // An instance that turned this session away (draining or unreachable)
    // isn't offered it again.
    let mut skip = None;
    for _ in 0..2 {
        // While anyone is already waiting, a new session waits behind them
        // instead of taking room that frees up first.
        let outcome = if state.ws_room.is_empty() {
            assign::assign(&mut state.valkey.clone(), &state.aqueduct, &user, assign::Traffic::WebSocket).await
        } else {
            Ok(assign::AssignOutcome::PoolExhausted)
        };
        let node = match outcome {
            Ok(assign::AssignOutcome::Assigned(node)) if Some(&node.id) != skip.as_ref() => node,
            Ok(_) => {
                if let Some((id, ready)) = state.ws_room.join(&user, false) {
                    let pending = Pending { user, query, headers: headers.clone() };
                    return upgrade
                        .protocols(offered_protocols(&headers))
                        .on_upgrade(move |client| wait_for_instance(state, client, id, ready, pending, slot))
                        .into_response();
                }
                let mut response = error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "no WebSocket instance has room and the waiting room is full; retry through the gateway",
                );
                let retry = state.line.limits().retry_after_seconds;
                response.headers_mut().insert("Retry-After", HeaderValue::from(retry));
                return response;
            }
            Err(err) => {
                tracing::error!("assign failed for a WebSocket session: {err}");
                let mut response = error(StatusCode::SERVICE_UNAVAILABLE, "assignment store unavailable; retry through the gateway");
                response.headers_mut().insert("Retry-After", HeaderValue::from(1u32));
                return response;
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
                state.metrics.inc("canalis_reroutes_total", &[]);
                skip = Some(node.id);
            }
            Err(Refused::Answered(response)) => return response,
            Err(Refused::Unreachable(err)) => {
                tracing::warn!("{} is unreachable for a WebSocket session; removed it until it registers again: {err}", node.id);
                let _ = assign::remove_instance(&mut state.valkey.clone(), &state.aqueduct, &node.id).await;
                state.metrics.inc("canalis_reroutes_total", &[]);
                skip = Some(node.id);
            }
        }
    }
    let mut response = error(StatusCode::SERVICE_UNAVAILABLE, "the assigned instances are draining or unreachable; retry through the gateway");
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
