mod assign;
mod l8;
mod line;
mod schemas;

use axum::{
    body::Body,
    extract::{ConnectInfo, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use axum::body::Bytes;
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use std::time::{Duration, Instant};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ConnectionManager and reqwest::Client both derive Clone (each wraps an
// Arc internally), so it's safe to hand a copy of the whole state to
// every request handler without an Arc<Mutex<>> wrapper of our own --
// unlike URLWorker's plain struct fields in Aquifer, which do need an
// external mutex because they aren't safe to share on their own.
pub type Valkey = redis::aio::ConnectionManager;

#[derive(Clone)]
pub struct AppState {
    valkey: Valkey,
    http: reqwest::Client,
    // This node's own durable waiting line -- see line.rs. Valkey still
    // holds assignment state; queued work never goes there.
    line: Arc<line::Line>,
    // Tenants whose backlog a drain task is already working through, so a
    // second drain (a sweep, another registration) doesn't start a parallel
    // one against the same instance and blow past its concurrency.
    draining: Arc<Mutex<HashSet<String>>>,
    // Callers on this node currently waiting in line with an open stream,
    // keyed by (user_id, idempotent_key). The drain that dispatches their
    // job takes their stream out of here and splices the node's own
    // response into it.
    waiters: Arc<Mutex<HashMap<(String, String), Waiter>>>,
    next_waiter_id: Arc<AtomicU64>,
    // Where assignment state lives (Aquifer's shared aqueduct: keys) and
    // the policy around it -- see assign.rs.
    aqueduct: Arc<assign::Aqueduct>,
    // L8 v0.2 request schemas, checked before a job goes anywhere -- see
    // schemas.rs. Off unless CANALIS_L8_SCHEMA_VALIDATION=true.
    schemas: Arc<schemas::SchemaGate>,
    // L8 receiver side: Canalis's own key, and which node keys may register
    // (see l8.rs).
    l8: Arc<l8::L8>,
}

// One caller waiting in line. `seq` is the job's place in the line's
// global order (u64::MAX until the enqueue that sets it commits).
struct Waiter {
    id: u64,
    seq: u64,
    tx: mpsc::Sender<Bytes>,
    last_ahead: Option<u64>,
}

// Waiting-room events are prefixed `canalis_` on purpose: they describe
// Canalis's own line, not the node's. Once a job is handed to a node, that
// node's own events (`queued`, `dispatching`, `completed`, ...) follow on
// the same stream -- it may have a queue of its own to go through.
fn sse_event(name: &str, data: serde_json::Value) -> Bytes {
    Bytes::from(format!("event: {name}\ndata: {data}\n\n"))
}

// Sends every waiting caller their new place in line, if it changed, and
// forgets callers who hung up. One pass over the line for everyone
// (Line::ranks), not one count per caller.
async fn waiting_room_tick(state: &AppState) -> Result<(), BoxError> {
    let seqs: HashSet<u64> = {
        let mut waiters = state.waiters.lock().map_err(|e| e.to_string())?;
        waiters.retain(|_, w| !w.tx.is_closed());
        waiters.values().map(|w| w.seq).filter(|&seq| seq != u64::MAX).collect()
    };
    if seqs.is_empty() {
        return Ok(());
    }
    let ranks = on_line(&state.line, move |l| l.ranks(&seqs)).await?;
    let mut waiters = state.waiters.lock().map_err(|e| e.to_string())?;
    for waiter in waiters.values_mut() {
        let Some(&ahead) = ranks.get(&waiter.seq) else { continue };
        if waiter.last_ahead != Some(ahead)
            && waiter
                .tx
                .try_send(sse_event("canalis_position", serde_json::json!({ "ahead": ahead })))
                .is_ok()
        {
            waiter.last_ahead = Some(ahead);
        }
    }
    Ok(())
}

// redb is a blocking store; every call goes through spawn_blocking so a
// commit's fsync never stalls the async executor.
async fn on_line<T: Send + 'static>(
    line: &Arc<line::Line>,
    f: impl FnOnce(&line::Line) -> Result<T, line::LineError> + Send + 'static,
) -> Result<T, line::LineError> {
    let line = line.clone();
    tokio::task::spawn_blocking(move || f(&line))
        .await
        .map_err(|err| line::LineError::Storage(err.to_string()))?
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let valkey_url =
        std::env::var("CANALIS_VALKEY_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let client = redis::Client::open(valkey_url).expect("invalid Valkey connection URL");
    let valkey: Valkey = redis::aio::ConnectionManager::new(client)
        .await
        .expect("failed to connect to Valkey");

    let line_path =
        std::env::var("CANALIS_LINE_PATH").unwrap_or_else(|_| "canalis-line.redb".into());
    let line = line::Line::open(&line_path, line::LineLimits::from_env())
        .unwrap_or_else(|err| panic!("failed to open the line at {line_path}: {err}"));

    match line.len() {
        Ok(0) => {}
        Ok(waiting) => tracing::info!("line at {line_path} still holds {waiting} jobs from before the restart"),
        Err(err) => tracing::error!("failed to read the line's length: {err}"),
    }

    let state = AppState {
        valkey,
        http: reqwest::Client::new(),
        line: Arc::new(line),
        draining: Arc::default(),
        waiters: Arc::default(),
        next_waiter_id: Arc::default(),
        aqueduct: Arc::new(assign::Aqueduct::from_env()),
        schemas: Arc::new(schemas::SchemaGate::from_env()),
        l8: Arc::new(l8::L8::from_env().unwrap_or_else(|err| panic!("L8 setup failed: {err}"))),
    };

    let position_every =
        Duration::from_millis(env_u64("CANALIS_POSITION_INTERVAL_MS", 1_000).max(50));
    let room_state = state.clone();
    tokio::spawn(async move {
        loop {
            if let Err(err) = waiting_room_tick(&room_state).await {
                tracing::error!("waiting room update failed: {err}");
            }
            tokio::time::sleep(position_every).await;
        }
    });

    // Runs once immediately -- that's crash recovery: whatever was in line
    // when this process last died gets drained as soon as instances are
    // free -- and then periodically, so a line held by this node still
    // drains when the instance that freed up registered with a different
    // Canalis node.
    let sweep_every = Duration::from_secs(env_u64("CANALIS_LINE_SWEEP_SECONDS", 5).max(1));
    let sweeper_state = state.clone();
    tokio::spawn(async move {
        loop {
            if let Err(err) = sweep_once(sweeper_state.clone()).await {
                tracing::error!("line sweep failed: {err}");
            }
            tokio::time::sleep(sweep_every).await;
        }
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/valkey-check", get(valkey_check))
        .route("/register", post(register))
        .route("/.well-known/l8", get(l8_well_known))
        .route("/l8/challenge", post(l8_challenge))
        .route("/proxy", post(proxy))
        .route("/jobs", post(jobs))
        .route("/jobs/{idempotent_key}", get(job_status))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("failed to bind to 0.0.0.0:8080");

    tracing::info!("canalis-rs listening on {}", listener.local_addr().unwrap());

    // into_make_service_with_connect_info::<SocketAddr> is what makes the
    // ConnectInfo<SocketAddr> extractor available in handlers below --
    // without it, register()'s signature wouldn't compile (there'd be
    // nothing supplying that extractor).
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .expect("server error");
}

async fn health() -> &'static str {
    "ok"
}

// Real round trip against Valkey: SET a key, then GET it back, so this
// actually proves connectivity rather than just proving the client
// constructed without erroring.
async fn valkey_check(State(mut state): State<AppState>) -> String {
    let _: () = state
        .valkey
        .set("canalis:check", "hello from canalis-rs")
        .await
        .expect("SET failed");

    let value: String = state.valkey.get("canalis:check").await.expect("GET failed");

    value
}

// The instance only reports its port, not a full address -- ConnectInfo
// below gives us the real source IP directly from the connection itself,
// which is more trustworthy than anything the instance could claim about
// its own reachable address.
//
// `state` is Aquifer's lifecycle state: `active` on normal heartbeats,
// `draining` as soon as it gets SIGTERM, a best-effort `offline` once its
// accepted work is done (see Aquifer's REGISTRATION.md). Missing means
// `active` -- Spillway's ping doesn't send it yet.
//
// The optional fields (see Aquifer's and Spillway's REGISTRATION.md):
// `reserved_for` dedicates the instance to one user, `role` says which
// traffic it's for (`ws` instances aren't given HTTP traffic), `drain_state`
// is drain mode's own state (`unassigned` = free for the next tenant), and
// `fullness` is 0-100, the same number as X-Aqueduct-Fullness.
#[derive(Deserialize, Default)]
struct RegisterRequest {
    port: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    reserved_for: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    drain_state: Option<String>,
    #[serde(default)]
    fullness: Option<u8>,
}

// Reads the raw body first: an L8 signature covers the exact bytes sent,
// so it's checked before the JSON is parsed. A signed ping that doesn't
// verify is refused; an unsigned one is refused only with
// CANALIS_REQUIRE_SIGNED_REGISTRATION (see l8.rs).
async fn register(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let refuse = |why: String| {
        tracing::warn!("refused registration from {}: {why}", remote.ip());
        json_response(StatusCode::UNAUTHORIZED, serde_json::json!({ "error": why }))
    };
    match state.l8.verify(&headers, &body) {
        l8::Verdict::Valid { .. } => {}
        l8::Verdict::Invalid(why) => return refuse(why),
        l8::Verdict::Unsigned if state.l8.require_signed => {
            return refuse("registration must be L8-signed by a trusted node key".into());
        }
        l8::Verdict::Unsigned => {}
    }
    let payload: RegisterRequest = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(err) => {
            return json_response(StatusCode::BAD_REQUEST, serde_json::json!({ "error": format!("invalid registration: {err}") }));
        }
    };

    let address = format!("{}:{}", remote.ip(), payload.port);
    match handle_registration(state, &address, &payload).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(err) => {
            tracing::error!("failed to register instance {address}: {err}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn l8_well_known(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(state.l8.meta())
}

async fn l8_challenge(State(state): State<AppState>, Json(req): Json<l8::ChallengeRequest>) -> Response {
    match state.l8.handle_challenge(&req) {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => {
            tracing::warn!("refused L8 challenge from key {}: {err}", req.sender_public_key);
            json_response(StatusCode::FORBIDDEN, serde_json::json!({ "error": err }))
        }
    }
}

// A registration ping becomes the instance's record in the shared
// assignment table, the same record an Aquifer in Valkey cluster mode
// writes for itself; its id is the address Canalis reaches it at. Records
// expire on their own if pings stop (three missed intervals), with no
// explicit deregistration needed.
async fn handle_registration(
    mut state: AppState,
    address: &str,
    payload: &RegisterRequest,
) -> Result<(), BoxError> {
    let aq = state.aqueduct.clone();
    let node_state = payload.state.as_deref().unwrap_or("active");

    if node_state == "offline" {
        assign::remove_instance(&mut state.valkey, &aq, address).await?;
        tracing::info!("instance {address} is offline; removed it and released its users");
        return Ok(());
    }

    let previous = assign::member(&mut state.valkey, &aq, address).await?;
    let member = assign::Member {
        id: address.to_string(),
        address: format!("http://{address}"),
        state: Some(node_state.to_string()),
        // A drain-mode instance serves one tenant at a time; anything else
        // takes CANALIS_DEFAULT_USER_CAPACITY (0 here means "the default").
        capacity: if payload.drain_state.is_some() { 1 } else { 0 },
        reported_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or_default(),
        reserved_for: payload.reserved_for.clone().filter(|s| !s.is_empty()),
        role: payload.role.clone().filter(|s| !s.is_empty()),
        drain_state: payload.drain_state.clone(),
        ..Default::default()
    };
    assign::heartbeat(&mut state.valkey, &aq, &member).await?;
    if let Some(fullness) = payload.fullness {
        assign::record_fullness(&mut state.valkey, &aq, address, fullness.min(100)).await?;
    }

    // A drain-mode instance that just finished its tenant: release whoever
    // is still assigned to it, so it can take the next one.
    let was_free = previous.as_ref().and_then(|p| p.drain_state.as_deref()) == Some("unassigned");
    let became_free = payload.drain_state.as_deref() == Some("unassigned") && !was_free;
    if became_free {
        let released = assign::release_all(&mut state.valkey, &aq, address).await?;
        tracing::info!("drain-mode instance {address} is free again; released {released} users");
    }

    if node_state == "draining" {
        // Its users move on with their next request (the claim script
        // skips draining instances); it finishes what it already has.
        tracing::info!("instance {address} is draining; no new users");
        return Ok(());
    }

    let newly_available = previous.as_ref().is_none_or(|p| !p.is_active()) || became_free;
    tracing::info!("registered instance {address}");
    if newly_available {
        // Spawned, not awaited: registration shouldn't block on a full
        // job dispatch completing. If the line is empty, this returns
        // immediately and does nothing.
        tokio::spawn(async move {
            if let Err(err) = try_drain_one(state).await {
                tracing::error!("failed while attempting to drain a pending job: {err}");
            }
        });
    }
    Ok(())
}

// X-Aqueduct-Fullness (0-100) from any instance response, so Canalis's
// picture of how full an instance is stays at most one response old.
fn fullness_header(headers: &reqwest::header::HeaderMap) -> Option<u8> {
    headers
        .get("X-Aqueduct-Fullness")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u16>().ok())
        .map(|v| v.min(100) as u8)
}

fn remember_fullness(state_valkey: &Valkey, aq: &Arc<assign::Aqueduct>, node_id: &str, headers: &reqwest::header::HeaderMap) {
    if let Some(fullness) = fullness_header(headers) {
        let (mut valkey, aq, id) = (state_valkey.clone(), aq.clone(), node_id.to_string());
        tokio::spawn(async move {
            if let Err(err) = assign::record_fullness(&mut valkey, &aq, &id, fullness).await {
                tracing::warn!("failed to record fullness for {id}: {err}");
            }
        });
    }
}

// Aquifer answers 503 with X-Aqueduct-Node-State: draining (Spillway the
// same, lowercase) once it's shutting down, even before its next
// registration ping says so.
fn node_is_draining(status: u16, headers: &reqwest::header::HeaderMap) -> bool {
    status == 503
        && ["X-Aqueduct-Node-State", "X-Aquifer-Node-State"].iter().any(|name| {
            headers
                .get(*name)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.eq_ignore_ascii_case("draining"))
        })
}

// Default pacing before anything's been learned about the target
// instance's own tolerance -- mirrors Aquifer's own account_queue.go
// defaults (RateConfig{RPS: 2.0, MaxConcurrent: 1}) exactly: start
// conservative (fully sequential), then adjust from whatever the
// instance reports back on each dispatch.
const DEFAULT_DRAIN_RPS: f64 = 2.0;
const DEFAULT_DRAIN_MAX_CONCURRENT: usize = 1;
const MIN_DRAIN_RPS: f64 = 0.5;
// A misbehaving or malicious instance advertising an absurd concurrency
// ceiling shouldn't be able to make Canalis fire an unbounded number of
// requests at once -- this caps how far X-Canalis-Max-Concurrent can
// push things up, independent of whatever the instance claims.
const MAX_DRAIN_CONCURRENCY_CEILING: usize = 50;

// Checked in this order: X-Canalis-<name> first (the purpose-built
// instance-to-Canalis signal), falling back to X-Aqueduct-<name> then
// X-Aquifer-<name> -- Aquifer's own existing pacing-header names, in
// case an instance's simplest path to supporting this is mirroring its
// already-computed internal rps/maxConc back out under the header names
// it already produces, rather than adding a Canalis-specific one.
// Mirrors Aquifer's own dual-namespace pacingHeader() lookup, extended
// by one more fallback layer.
const PACING_HEADER_PREFIXES: [&str; 3] = ["X-Canalis-", "X-Aqueduct-", "X-Aquifer-"];

fn pacing_header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    PACING_HEADER_PREFIXES.iter().find_map(|prefix| {
        headers
            .get(format!("{prefix}{name}").as_str())
            .and_then(|v| v.to_str().ok())
    })
}

// The instance-reported pacing signal for further drain dispatches,
// read off each dispatched job's own response -- see
// PACING_HEADER_PREFIXES for the header names checked. This closes the
// gap DESIGN.md's Open Questions section flagged (Canalis had no way to
// learn an instance's own concurrency capacity): X-Aqueduct-*/X-Aquifer-*
// are the *backend* telling an Aquifer instance how hard it'll tolerate
// being hit; X-Canalis-* is the *instance* telling Canalis the same
// thing about itself, one layer further out. Not yet emitted by
// Aquifer/Spillway under any of these names -- until an
// instance actually sends one, every drain stays at the conservative
// sequential default above.
struct PacingSignal {
    max_concurrent: Option<usize>,
    rps: Option<f64>,
}

impl PacingSignal {
    fn from_headers(headers: &reqwest::header::HeaderMap) -> Self {
        let max_concurrent = pacing_header(headers, "Max-Concurrent")
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0);
        let rps = pacing_header(headers, "Rps")
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|&v| v > 0.0);
        PacingSignal { max_concurrent, rps }
    }

    fn none() -> Self {
        PacingSignal { max_concurrent: None, rps: None }
    }
}

// Marks a tenant as being drained for as long as it's alive, so the
// marker can't leak if a drain task errors out or panics partway.
struct DrainGuard {
    draining: Arc<Mutex<HashSet<String>>>,
    user_id: String,
}

impl Drop for DrainGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.draining.lock() {
            set.remove(&self.user_id);
        }
    }
}

// Takes the longest-waiting job out of line and resolves an instance for
// its tenant. If nothing's free after all, the job goes back in its
// original place (not to the back of the line) and this returns None.
async fn claim_next(
    state: &mut AppState,
) -> Result<Option<(line::QueuedJob, assign::Node, DrainGuard)>, BoxError> {
    let skip = state.draining.lock().map_err(|e| e.to_string())?.clone();
    let Some(queued) = on_line(&state.line, move |l| l.pop_oldest(&skip)).await? else {
        return Ok(None);
    };

    let user_id = queued.job.user_id.clone();
    let newly_marked = state.draining.lock().map_err(|e| e.to_string())?.insert(user_id.clone());
    if !newly_marked {
        // A drain for this tenant started between the snapshot above and
        // now; it'll reach this job itself.
        on_line(&state.line, move |l| l.requeue(&queued)).await?;
        return Ok(None);
    }
    let guard = DrainGuard { draining: state.draining.clone(), user_id: user_id.clone() };

    match assign::assign(&mut state.valkey, &state.aqueduct, &user_id).await {
        Ok(assign::AssignOutcome::Assigned(node)) => Ok(Some((queued, node, guard))),
        Ok(assign::AssignOutcome::PoolExhausted) => {
            on_line(&state.line, move |l| l.requeue(&queued)).await?;
            Ok(None)
        }
        Err(err) => {
            on_line(&state.line, move |l| l.requeue(&queued)).await?;
            Err(err.into())
        }
    }
}

// Drains one tenant's whole backlog against one freed instance, waiting
// for it to finish. Assignment is sticky per user_id, so there's no
// reason a single freed instance should only ever clear one of their jobs
// before waiting for another registration to come along. Returns whether
// anything was drained.
async fn try_drain_one(mut state: AppState) -> Result<bool, BoxError> {
    let Some((first, node, guard)) = claim_next(&mut state).await? else {
        return Ok(false);
    };
    drain_tenant_backlog(state, first, node, guard).await?;
    Ok(true)
}

// Starts a drain for every tenant in line that can get an instance right
// now, without waiting for them to finish, and drops expired results.
// Returns how many drains were started.
async fn sweep_once(mut state: AppState) -> Result<usize, BoxError> {
    on_line(&state.line, |l| l.expire_results()).await?;

    let mut started = 0;
    while let Some((first, node, guard)) = claim_next(&mut state).await? {
        let drain_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = drain_tenant_backlog(drain_state, first, node, guard).await {
                tracing::error!("drain failed: {err}");
            }
        });
        started += 1;
    }
    Ok(started)
}

// Drains user_id's entire backlog against one sticky assignment,
// dispatching concurrently up to a live concurrency ceiling that
// adjusts as X-Canalis-Max-Concurrent/X-Canalis-Rps come back from each
// dispatch -- a direct port of Aquifer's own account_queue.go run()
// loop (pull work, dispatch up to maxConc concurrently, paced at 1/rps
// between dispatch *starts*, adjust both from each completion's
// response headers) with two differences: work comes from this node's
// line (line.rs) instead of an in-memory channel, and there's no jitter (Aquifer's
// jitter exists to keep many *different* domains' queues from
// resonating against a shared backend; one tenant's own drain has no
// sibling queue to desynchronize from, so the complexity isn't worth it
// here).
//
// Headless throughout -- no live connection to relay to, so each job's
// outcome gets cached for whichever still-open request (or future
// polling endpoint) is waiting on it. Always dispatches to the assigned
// instance's own /jobs, not /proxy: these jobs were already queued once,
// so there's no reason to attempt a synchronous direct attempt again --
// /jobs already gives a uniform "wait for the full SSE lifecycle to
// finish, then read whatever it settled on" shape, which is exactly
// what's needed here.
async fn drain_tenant_backlog(
    mut state: AppState,
    first: line::QueuedJob,
    node: assign::Node,
    _guard: DrainGuard,
) -> Result<(), BoxError> {
    let user_id = first.job.user_id.clone();
    let mut pending = Some(first);
    let mut max_concurrent = DEFAULT_DRAIN_MAX_CONCURRENT;
    let mut rps = DEFAULT_DRAIN_RPS;
    let mut last_dispatch_at: Option<Instant> = None;
    let mut in_flight: tokio::task::JoinSet<DispatchOutcome> = tokio::task::JoinSet::new();
    // Set once the instance says it's draining: stop taking more jobs out
    // of line, let what's in flight finish, then hand the tenant back.
    let mut node_draining = false;
    // Every request through Canalis renews the user's assignment; a long
    // drain does too, so it can't idle out from under the drain.
    let mut last_renewed = Instant::now();

    loop {
        while !node_draining && in_flight.len() < max_concurrent {
            let job = match pending.take() {
                Some(job) => job,
                None => {
                    let user_id = user_id.clone();
                    match on_line(&state.line, move |l| l.pop_for(&user_id)).await? {
                        Some(queued) => queued,
                        None => break,
                    }
                }
            };

            if let Some(prev) = last_dispatch_at {
                let interval = Duration::from_secs_f64(1.0 / rps.max(MIN_DRAIN_RPS));
                let elapsed = prev.elapsed();
                if elapsed < interval {
                    tokio::time::sleep(interval - elapsed).await;
                }
            }
            last_dispatch_at = Some(Instant::now());
            if last_renewed.elapsed() > state.aqueduct.assignment_ttl / 3 {
                let _ = assign::assign(&mut state.valkey, &state.aqueduct, &user_id).await;
                last_renewed = Instant::now();
            }

            in_flight.spawn(dispatch_drained_job(
                state.clone(),
                node.clone(),
                job,
            ));
        }

        let Some(joined) = in_flight.join_next().await else {
            // Nothing in flight, and the top-up loop above just found
            // nothing left to pop -- the backlog is genuinely empty.
            break;
        };

        let outcome = joined.map_err(|err| format!("drain dispatch task panicked: {err}"))?;
        if let Some(job) = outcome.back_in_line {
            node_draining = true;
            on_line(&state.line, move |l| l.requeue(&job)).await?;
        }
        let signal = outcome.pacing;

        if let Some(mc) = signal.max_concurrent {
            max_concurrent = mc.min(MAX_DRAIN_CONCURRENCY_CEILING);
        }
        if let Some(new_rps) = signal.rps {
            rps = new_rps.max(MIN_DRAIN_RPS);
        }
    }

    if node_draining {
        // The rest of this tenant's line stays where it is; marking the
        // node draining and releasing the assignment means the next claim
        // finds them a different node.
        assign::mark_draining(&mut state.valkey, &state.aqueduct, &node.id).await?;
        assign::release_user(&mut state.valkey, &state.aqueduct, &user_id, &node.id).await?;
        tracing::info!("{} is draining; {user_id}'s remaining jobs stay in line for another node", node.id);
    }
    Ok(())
}

// What one drained dispatch hands back to drain_tenant_backlog: the
// instance's pacing signal, plus the job itself if the instance turned out
// to be draining and it has to go back in line instead of being finished.
struct DispatchOutcome {
    pacing: PacingSignal,
    back_in_line: Option<line::QueuedJob>,
}

// One job's worth of the drain, run as its own task so up to
// max_concurrent of these can be in flight together. Every job's outcome
// is cached for GET /jobs/{key}; if its caller is still waiting on this
// node, the node's response is also streamed to them live, after a
// `canalis_assigned` event -- byte for byte when the node streams SSE (as
// Aquifer's /jobs does), or as one `canalis_result` event otherwise. A
// caching failure is only logged: with several of these in flight at once
// there's no single caller left to hand a Result to.
async fn dispatch_drained_job(
    state: AppState,
    node: assign::Node,
    queued: line::QueuedJob,
) -> DispatchOutcome {
    let (line, waiters, http) = (&state.line, &state.waiters, &state.http);
    let addr = &node.id;
    let job = &queued.job;
    // Same path the caller used: a /proxy caller gets the node's /proxy
    // answer (the upstream's own response, or its queue stream), a /jobs
    // caller gets the job accepted on the node.
    let target_url = format!("{}{}", node.base_url, queued.path);
    let follows_job_stream = queued.path == "/jobs";
    let response = http.post(&target_url).json(job).send().await;

    let mut resp = match response {
        Ok(resp) if node_is_draining(resp.status().as_u16(), resp.headers()) => {
            // Back in line; the caller (if any) stays registered and keeps
            // getting position updates until another node takes the job.
            return DispatchOutcome { pacing: PacingSignal::none(), back_in_line: Some(queued) };
        }
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!("failed to dispatch drained job to {addr}: {err}");
            let cached = line::CachedResult {
                status: 502,
                content_type: "application/json".into(),
                body: serde_json::json!({ "error": format!("failed to reach assigned instance: {err}") })
                    .to_string(),
                headers: Vec::new(),
            };
            let waiter = take_waiter(waiters, job);
            finish_drained_job(line, job, cached, waiter, false).await;
            return DispatchOutcome { pacing: PacingSignal::none(), back_in_line: None };
        }
    };

    let pacing = PacingSignal::from_headers(resp.headers());
    remember_fullness(&state.valkey, &state.aqueduct, &node.id, resp.headers());
    observe_schema_hash(&state, &job.url, resp.headers());
    let stored_headers = headers_to_store(resp.headers());
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let streams = content_type.starts_with("text/event-stream");

    let mut waiter = take_waiter(waiters, job);
    // A /jobs answer is the node's acknowledgment (201 + job_id); the
    // caller learns about the assignment once that id is known, below.
    if !follows_job_stream {
        if let Some(tx) = &waiter {
            if tx.send(sse_event("canalis_assigned", serde_json::json!({}))).await.is_err() {
                waiter = None;
            }
        }
    }

    // Read the node's response chunk by chunk: each one goes to the waiting
    // caller as it arrives (live, not buffered) and into the cached copy.
    let mut body = Vec::new();
    let mut read_failed = None;
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                body.extend_from_slice(&chunk);
                if streams {
                    if let Some(tx) = &waiter {
                        if tx.send(chunk).await.is_err() {
                            // Caller hung up; keep reading so the result
                            // is still cached for GET /jobs/{key}.
                            waiter = None;
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                read_failed = Some(err);
                break;
            }
        }
    }

    let mut already_streamed = streams && read_failed.is_none();
    let cached = match read_failed {
        None => line::CachedResult {
            status,
            content_type,
            body: String::from_utf8_lossy(&body).into_owned(),
            headers: stored_headers,
        },
        Some(err) => {
            tracing::error!("failed to read drained response body from {addr}: {err}");
            line::CachedResult {
                status: 502,
                content_type: "application/json".into(),
                body: serde_json::json!({ "error": "failed to read upstream response" }).to_string(),
                headers: Vec::new(),
            }
        }
    };

    // A /jobs caller still on the line stays on it through the node's own
    // job stream (`queued`, `position`, `dispatching`, `completed`), same
    // as calling GET /jobs/{id}/stream on Aquifer directly -- joining late
    // is safe, the node replays the states already passed. What gets
    // cached is the acknowledgment; the outcome itself is the node's
    // (webhook, or its own GET /jobs/{id}).
    if follows_job_stream {
        let job_id = (cached.status == 201)
            .then(|| serde_json::from_str::<serde_json::Value>(&cached.body).ok())
            .flatten()
            .and_then(|v| v.get("job_id").and_then(|id| id.as_str()).map(str::to_string));
        if let Some(tx) = waiter.take() {
            let assigned = serde_json::json!({ "job_id": job_id });
            if tx.send(sse_event("canalis_assigned", assigned)).await.is_ok() {
                already_streamed = match &job_id {
                    Some(id) => follow_job_stream(http, &node.base_url, id, &tx).await,
                    None => false,
                };
                waiter = Some(tx);
            }
        }
    }

    finish_drained_job(line, job, cached, waiter, already_streamed).await;
    DispatchOutcome { pacing, back_in_line: None }
}

// Splices the node's GET /jobs/{id}/stream into a waiting caller's stream.
// Returns whether it got through it; on failure the caller gets the
// acknowledgment as a canalis_result instead.
async fn follow_job_stream(
    http: &reqwest::Client,
    base_url: &str,
    job_id: &str,
    tx: &mpsc::Sender<Bytes>,
) -> bool {
    let url = format!("{base_url}/jobs/{job_id}/stream");
    let mut resp = match http.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => resp,
        Ok(resp) => {
            tracing::warn!("{url} answered {}; not following it", resp.status());
            return false;
        }
        Err(err) => {
            tracing::warn!("failed to open {url}: {err}");
            return false;
        }
    };
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if tx.send(chunk).await.is_err() {
                    return true; // caller hung up; nothing left to tell them
                }
            }
            Ok(None) => return true,
            Err(err) => {
                tracing::warn!("{url} broke off: {err}");
                return true; // part of it already went out; don't repeat it as a result
            }
        }
    }
}

fn take_waiter(
    waiters: &Mutex<HashMap<(String, String), Waiter>>,
    job: &JobRequest,
) -> Option<mpsc::Sender<Bytes>> {
    waiters
        .lock()
        .ok()?
        .remove(&(job.user_id.clone(), job.idempotent_key.clone()))
        .map(|w| w.tx)
}

// Caches a drained job's outcome, then -- unless its stream already
// carried it -- tells a still-waiting caller with a `canalis_result` event.
// Cache first: the caller's stream ends when its sender is dropped here,
// and a caller who checks GET /jobs/{key} right after must find the result.
async fn finish_drained_job(
    line: &Arc<line::Line>,
    job: &JobRequest,
    cached: line::CachedResult,
    waiter: Option<mpsc::Sender<Bytes>>,
    already_streamed: bool,
) {
    let event = (waiter.is_some() && !already_streamed).then(|| {
        sse_event(
            "canalis_result",
            serde_json::json!({
                "status": cached.status,
                "content_type": cached.content_type,
                "headers": cached.headers.iter().cloned().collect::<HashMap<_, _>>(),
                "body": cached.body,
            }),
        )
    });
    let (user_id, key) = (job.user_id.clone(), job.idempotent_key.clone());
    if let Err(err) = on_line(line, move |l| l.store_result(&user_id, &key, &cached)).await {
        tracing::error!("failed to cache drained result for {}: {err}", job.idempotent_key);
    }
    if let (Some(tx), Some(event)) = (waiter, event) {
        let _ = tx.send(event).await;
    }
}

// Mirrors Aquifer's own POST /proxy and POST /jobs request body exactly,
// so a caller already speaking Aquifer's API can point at Canalis with no
// translation layer -- the whole point of the assignment-then-passthrough
// design (see DESIGN.md). Serialize too: this struct gets forwarded
// as-is to whichever instance Canalis assigns, not rebuilt field by field.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub(crate) struct JobRequest {
    pub(crate) user_id: String,
    pub(crate) idempotent_key: String,
    pub(crate) url: String,
    pub(crate) method: String,
    #[serde(default)]
    pub(crate) headers: HashMap<String, String>,
    #[serde(default)]
    pub(crate) body: Option<String>,
    #[serde(default)]
    pub(crate) webhook_url: Option<String>,
}

async fn proxy(State(state): State<AppState>, Json(job): Json<JobRequest>) -> Response {
    forward(state, job, "/proxy").await
}

async fn jobs(State(state): State<AppState>, Json(job): Json<JobRequest>) -> Response {
    forward(state, job, "/jobs").await
}

// All the retry/timeout knobs forward() needs, as explicit values rather
// than env-var lookups inside the retry logic itself -- env vars are
// process-global mutable state, and tests running in parallel (Rust's
// default) mutating the same env var to get a short test-only timeout
// would leak into whichever other test happened to be running
// concurrently. Passing these as plain arguments sidesteps that
// entirely: production reads them once, via RetryConfig::from_env(), and
// tests construct short values directly, no shared mutable state at all.
struct RetryConfig {
    // How long a caller's stream stays open in line before Canalis ends it
    // with `canalis_timeout`. The job itself stays in line either way.
    pool_wait_timeout: Duration,
    // A few quick retries against the *same* assigned instance, for a
    // momentary blip (a restart mid-request, a brief network hiccup) --
    // mirrors Aquifer's own account_queue.go retry pattern. Deliberately
    // not used for pool exhaustion: that's a different problem with a
    // different fix (see pool_wait_timeout above).
    instance_retry_attempts: u32,
    instance_retry_base_delay: Duration,
}

impl RetryConfig {
    // Env-var overridable, same convention Aquifer/Spillway
    // already use for their own idle/drain timers -- production should
    // leave these at the defaults.
    fn from_env() -> Self {
        Self {
            pool_wait_timeout: Duration::from_millis(env_u64(
                "CANALIS_POOL_WAIT_TIMEOUT_MS",
                30_000,
            )),
            instance_retry_attempts: 3,
            instance_retry_base_delay: Duration::from_millis(env_u64(
                "CANALIS_INSTANCE_RETRY_BASE_DELAY_MS",
                200,
            )),
        }
    }
}

pub(crate) fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

async fn forward(state: AppState, job: JobRequest, target_path: &str) -> Response {
    forward_with_config(state, job, target_path, &RetryConfig::from_env()).await
}

// Puts the job in line and turns the caller's connection into the waiting
// room's stream. The caller is registered as a waiter *before* the job is
// enqueued, so a drain that picks the job up immediately still finds
// someone to stream to.
async fn wait_in_line(state: AppState, job: JobRequest, path: &str, timeout: Duration) -> Response {
    let key = (job.user_id.clone(), job.idempotent_key.clone());
    let id = state.next_waiter_id.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = mpsc::channel::<Bytes>(64);
    {
        // Scoped so the guard is provably gone before any await below.
        let Ok(mut waiters) = state.waiters.lock() else {
            return json_response(StatusCode::INTERNAL_SERVER_ERROR, serde_json::json!({ "error": "internal error" }));
        };
        // A second connection for the same job takes over; the earlier
        // one's stream ends when its sender is dropped here.
        waiters.insert(key.clone(), Waiter { id, seq: u64::MAX, tx: tx.clone(), last_ahead: None });
    }

    let unregister = |state: &AppState| {
        if let Ok(mut waiters) = state.waiters.lock() {
            if waiters.get(&key).is_some_and(|w| w.id == id) {
                waiters.remove(&key);
            }
        }
    };

    let (queued_job, path) = (job.clone(), path.to_string());
    let (ahead, seq) = match on_line(&state.line, move |l| l.enqueue(&queued_job, &path)).await {
        Ok(line::Enqueued::Queued { ahead, seq } | line::Enqueued::AlreadyQueued { ahead, seq }) => (ahead, seq),
        Ok(line::Enqueued::AlreadyDone(cached)) => {
            unregister(&state);
            return cached_result_response(cached);
        }
        Err(line::LineError::Full { reason, limit }) => {
            unregister(&state);
            return line_full_response(reason, limit, state.line.limits().retry_after_seconds);
        }
        Err(err) => {
            unregister(&state);
            tracing::error!("failed to durably enqueue job for {}: {err}", job.user_id);
            return json_response(StatusCode::INTERNAL_SERVER_ERROR, serde_json::json!({ "error": "internal error" }));
        }
    };
    tracing::info!("no instance free for {}; in line with {ahead} ahead", job.user_id);

    if let Ok(mut waiters) = state.waiters.lock() {
        if let Some(waiter) = waiters.get_mut(&key).filter(|w| w.id == id) {
            waiter.seq = seq;
            waiter.last_ahead = Some(ahead);
        }
    }
    let status_url = format!("/jobs/{}?user_id={}", job.idempotent_key, job.user_id);
    let _ = tx.try_send(sse_event(
        "canalis_queued",
        serde_json::json!({ "reason": "no_instance_free", "ahead": ahead, "status": status_url }),
    ));

    // Only this task and the drain hold senders; once both are gone the
    // stream ends. If the wait outlasts the timeout and the job is still
    // ours to report on, say so and let go -- the job stays in line.
    let timeout_state = state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(timeout).await;
        let still_waiting = timeout_state
            .waiters
            .lock()
            .ok()
            .and_then(|mut waiters| {
                if waiters.get(&key).is_some_and(|w| w.id == id) { waiters.remove(&key) } else { None }
            });
        if let Some(waiter) = still_waiting {
            let event = sse_event(
                "canalis_timeout",
                serde_json::json!({ "note": "still in line; this stream is closing", "status": status_url }),
            );
            let _ = waiter.tx.send(event).await;
        }
    });
    drop(tx);

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok::<_, std::io::Error>);
    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "text/event-stream")
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| json_response(StatusCode::INTERNAL_SERVER_ERROR, serde_json::json!({ "error": "internal error" })))
}

// Marks a response as "the assigned instance said it's draining", so
// forward_with_config can re-route instead of relaying it.
#[derive(Clone)]
struct NodeDraining(String);

async fn forward_with_config(
    state: AppState,
    job: JobRequest,
    target_path: &str,
    config: &RetryConfig,
) -> Response {
    // Before anything else: a body the upstream's published schema rejects
    // never takes a spot in the line or a user slot on an instance.
    if let Err(mismatch) = state.schemas.check(&job.url, &job.method, job.body.as_deref().unwrap_or("")).await {
        return schema_mismatch_response(mismatch);
    }

    let mut valkey = state.valkey.clone();
    let user_id = job.user_id.clone();
    let first = forward_attempt(state.clone(), job.clone(), target_path, config).await;
    let Some(NodeDraining(node_id)) = first.extensions().get::<NodeDraining>().cloned() else {
        return first;
    };

    // The assigned instance is shutting down: mark it so, hand this user
    // back, and try once more -- a different instance, or the line. Once,
    // not in a loop: a second draining answer is relayed as-is.
    if let Err(err) = assign::mark_draining(&mut valkey, &state.aqueduct, &node_id).await {
        tracing::warn!("failed to mark {node_id} draining: {err}");
    }
    if let Err(err) = assign::release_user(&mut valkey, &state.aqueduct, &user_id, &node_id).await {
        tracing::error!("failed to release {user_id} from draining instance {node_id}: {err}");
        return first;
    }
    tracing::info!("{node_id} is draining; re-routing {user_id}");
    forward_attempt(state, job, target_path, config).await
}

// Resolves the assignment, then forwards the same job body to the
// assigned instance's own target_path ("/proxy" or "/jobs"). A plain
// response gets its status, content-type, and body relayed verbatim; an
// SSE response (Aquifer's fallback-to-queue path, and always the case for
// /jobs) is relayed live.
//
// Two distinct fallback behaviors, not one: a dead *assigned* instance
// gets a few quick retries against the instance itself (a momentary
// blip), then a clean error. Pool exhaustion is different: the job goes in
// this node's line (line.rs -- on local disk, survives a Canalis crash,
// and bounded: a full line answers 429 instead of growing), and the
// caller's connection becomes the waiting room's SSE stream, the same way
// Aquifer's own /proxy turns into a stream when it has to queue:
// `canalis_queued`, `canalis_position` as the line moves, then
// `canalis_assigned` and the node's own response spliced in live once a
// drain hands the job over -- or `canalis_timeout` if that takes longer
// than CANALIS_POOL_WAIT_TIMEOUT_MS (the job stays in line; a webhook_url
// still fires, and GET /jobs/{key} reports it).
async fn forward_attempt(
    mut state: AppState,
    job: JobRequest,
    target_path: &str,
    config: &RetryConfig,
) -> Response {
    let node = match assign::assign(&mut state.valkey, &state.aqueduct, &job.user_id).await {
        Ok(assign::AssignOutcome::Assigned(node)) => node,
        Ok(assign::AssignOutcome::PoolExhausted) => {
            return wait_in_line(state, job, target_path, config.pool_wait_timeout).await;
        }
        Err(err) => {
            tracing::error!("assign failed: {err}");
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({ "error": "internal error" }),
            );
        }
    };

    let addr = &node.id;
    let target_url = format!("{}{target_path}", node.base_url);
    let mut last_err = None;
    let mut upstream = None;
    for attempt in 0..config.instance_retry_attempts {
        if attempt > 0 {
            tokio::time::sleep(config.instance_retry_base_delay * 2u32.pow(attempt - 1)).await;
        }
        match state.http.post(&target_url).json(&job).send().await {
            Ok(resp) => {
                upstream = Some(resp);
                break;
            }
            Err(err) => {
                tracing::warn!(
                    "attempt {}/{} failed to reach {addr}: {err}",
                    attempt + 1,
                    config.instance_retry_attempts
                );
                last_err = Some(err);
            }
        }
    }

    let upstream = match upstream {
        Some(resp) => resp,
        None => {
            let err = last_err.expect("loop always sets last_err when upstream stays None");
            tracing::error!(
                "gave up reaching assigned instance {addr} after {} attempts: {err}",
                config.instance_retry_attempts
            );
            return json_response(
                StatusCode::BAD_GATEWAY,
                serde_json::json!({ "error": format!("failed to reach assigned instance: {err}") }),
            );
        }
    };

    remember_fullness(&state.valkey, &state.aqueduct, &node.id, upstream.headers());
    observe_schema_hash(&state, &job.url, upstream.headers());
    if node_is_draining(upstream.status().as_u16(), upstream.headers()) {
        let mut response = json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({ "error": "assigned instance is draining" }),
        );
        response
            .headers_mut()
            .insert(axum::http::header::RETRY_AFTER, axum::http::HeaderValue::from(1u32));
        response.extensions_mut().insert(NodeDraining(node.id.clone()));
        return response;
    }

    let streams = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/event-stream"));

    let status = StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let upstream_headers = upstream.headers().clone();

    if streams {
        // Live relay, not buffered: bytes_stream() yields chunks as they
        // arrive from the assigned instance, and Body::from_stream feeds
        // them to our own caller as they arrive too -- reqwest and axum
        // share the same Stream/Bytes abstractions, so this is close to
        // handing one stream directly to the other, not a rewrite.
        //
        // Once this starts, the response status/headers are already
        // committed to our caller -- if the upstream connection drops
        // mid-stream, there's no changing course to an error response
        // anymore. The stream just ends, which is exactly how SSE
        // clients (EventSource) are already built to handle an
        // unexpected close: reconnect, not treat it as a different kind
        // of failure needing special handling here.
        let stream = upstream.bytes_stream();
        let mut response = Response::builder()
            .status(status)
            .body(Body::from_stream(stream))
            .unwrap_or_else(|_| {
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .expect("a hardcoded empty error response should always build")
            });
        relay_headers(&upstream_headers, response.headers_mut());
        return response;
    }

    let body = match upstream.bytes().await {
        Ok(b) => b,
        Err(err) => {
            tracing::error!("failed to read {addr}'s response body: {err}");
            return json_response(
                StatusCode::BAD_GATEWAY,
                serde_json::json!({ "error": "failed to read upstream response" }),
            );
        }
    };

    let mut response = Response::builder().status(status).body(Body::from(body)).unwrap_or_else(|_| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .expect("a hardcoded empty error response should always build")
    });
    relay_headers(&upstream_headers, response.headers_mut());
    response
}

// Hop-by-hop headers (RFC 9110 section 7.6.1) describe one connection,
// not the response, so a proxy must not pass them on. Content-Length is
// left to the body Canalis actually sends.
const NOT_RELAYED: [&str; 10] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
];

// The instance's response headers that should reach the caller as-is:
// Retry-After on a 429, the X-Aqueduct-* load and pacing headers,
// X-Aqueduct-Node-State, the content type -- everything except hop-by-hop
// headers and anything the instance's Connection header names as
// connection-specific.
fn relayed_headers(
    from: &reqwest::header::HeaderMap,
) -> impl Iterator<Item = (&reqwest::header::HeaderName, &reqwest::header::HeaderValue)> {
    let named_by_connection: Vec<String> = from
        .get_all(reqwest::header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    from.iter().filter(move |(name, _)| {
        let name = name.as_str();
        !NOT_RELAYED.contains(&name) && !named_by_connection.iter().any(|n| n == name)
    })
}

fn relay_headers(from: &reqwest::header::HeaderMap, to: &mut axum::http::HeaderMap) {
    for (name, value) in relayed_headers(from) {
        to.append(name.clone(), value.clone());
    }
}

// The same headers, kept with a drained job's stored result (the content
// type has its own field there).
fn headers_to_store(from: &reqwest::header::HeaderMap) -> Vec<(String, String)> {
    relayed_headers(from)
        .filter(|(name, _)| *name != reqwest::header::CONTENT_TYPE)
        .filter_map(|(name, value)| Some((name.as_str().to_string(), value.to_str().ok()?.to_string())))
        .collect()
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response {
    (status, Json(body)).into_response()
}

// Same body as Aquifer's own schema rejection (writeSchemaMismatch), so a
// client handling one handles both.
fn schema_mismatch_response(mismatch: schemas::Mismatch) -> Response {
    json_response(
        StatusCode::UNPROCESSABLE_ENTITY,
        serde_json::json!({
            "error": format!("request body does not match the schema the upstream advertises for {}", mismatch.route),
            "schema_route": mismatch.route,
            "schema_hash": mismatch.schema_hash,
            "schema_errors": mismatch.detail,
        }),
    )
}

// An upstream's X-Aqueduct-Schema-Hash, relayed by the instance, tells the
// schema gate when the upstream's contract changed.
fn observe_schema_hash(state: &AppState, job_url: &str, headers: &reqwest::header::HeaderMap) {
    if let Some(hash) = headers.get("X-Aqueduct-Schema-Hash").and_then(|v| v.to_str().ok()) {
        state.schemas.observe_hash(job_url, hash.trim());
    }
}

// Same body shape as Aquifer's own admission rejections
// ({"error", "limit_reason", "limit"}), so a client handling one handles
// both.
fn line_full_response(reason: &'static str, limit: u64, retry_after_seconds: u64) -> Response {
    let mut response = json_response(
        StatusCode::TOO_MANY_REQUESTS,
        serde_json::json!({
            "error": "line full: no instance is free and the waiting line is at its limit",
            "limit_reason": reason,
            "limit": limit,
        }),
    );
    response.headers_mut().insert(
        axum::http::header::RETRY_AFTER,
        axum::http::HeaderValue::from(retry_after_seconds),
    );
    response
}

#[derive(Deserialize)]
struct StatusQuery {
    user_id: String,
}

// Where a queued job stands, on *this* Canalis node: its finished result
// (same response the waiting connection would have gotten), its place in
// line (202), or 404. The line is per-node, so this has to be asked of the
// node that queued it.
async fn job_status(
    State(state): State<AppState>,
    Path(idempotent_key): Path<String>,
    Query(query): Query<StatusQuery>,
) -> Response {
    let user_id = query.user_id;
    match on_line(&state.line, move |l| l.lookup(&user_id, &idempotent_key)).await {
        Ok(line::Lookup::Done(cached)) => cached_result_response(cached),
        Ok(line::Lookup::Queued { ahead }) => json_response(
            StatusCode::ACCEPTED,
            serde_json::json!({ "status": "queued", "ahead": ahead }),
        ),
        Ok(line::Lookup::Unknown) => json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({ "error": "no queued job or recent result for that key" }),
        ),
        Err(err) => {
            tracing::error!("failed to look up job status: {err}");
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({ "error": "internal error" }),
            )
        }
    }
}

// A drained job's cached outcome is the *complete* text a live viewer
// would have seen (the full SSE event history, or a plain body) -- it's
// replayed back all at once here rather than live, since the job already
// finished by the time anyone's asking. No information is lost, only the
// incremental delivery is, which is an honest, deliberate simplification
// for a result that's already history.
fn cached_result_response(cached: line::CachedResult) -> Response {
    let status = StatusCode::from_u16(cached.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut builder = Response::builder().status(status);
    if !cached.content_type.is_empty() {
        builder = builder.header(axum::http::header::CONTENT_TYPE, cached.content_type);
    }
    for (name, value) in &cached.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    builder.body(Body::from(cached.body)).unwrap_or_else(|_| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .expect("a hardcoded empty error response should always build")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use axum::extract::State as AxumState;
    use axum::routing::post;
    use http_body_util::BodyExt;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    async fn test_valkey() -> Valkey {
        let client = redis::Client::open("redis://127.0.0.1:6379").expect("valid Valkey URL");
        redis::aio::ConnectionManager::new(client)
            .await
            .expect("connect to a real local Valkey on 127.0.0.1:6379 -- required for this test")
    }

    // Clock nanoseconds alone collide on back-to-back calls (macOS's clock
    // only moves in microseconds), and two jobs with the same idempotent
    // key are rightly deduplicated by the line -- assign.rs's version adds
    // a counter.
    fn unique(prefix: &str) -> String {
        assign::tests::unique(prefix)
    }

    // Every test gets its own line file, so queued jobs can never leak from
    // one test (or one failed run) into another. Keep the TempDir alive for
    // as long as the state is in use -- dropping it deletes the file.
    async fn test_state_with(limits: line::LineLimits) -> (AppState, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let line = line::Line::open(dir.path().join("line.redb"), limits).unwrap();
        let state = AppState {
            valkey: test_valkey().await,
            http: reqwest::Client::new(),
            line: Arc::new(line),
            draining: Arc::default(),
            waiters: Arc::default(),
            next_waiter_id: Arc::default(),
            aqueduct: Arc::new(assign::tests::test_aqueduct()),
            schemas: Arc::new(schemas::SchemaGate::new(false)),
            l8: Arc::new(l8::L8::new(l8::tests::new_key(), None, false)),
        };
        (state, dir)
    }

    async fn test_state() -> (AppState, tempfile::TempDir) {
        test_state_with(line::LineLimits::default()).await
    }

    // Each test's state has its own aqueduct: prefix (see
    // assign::tests::test_aqueduct); this deletes everything under it.
    async fn cleanup(state: &AppState) {
        assign::tests::cleanup(&mut state.valkey.clone(), &state.aqueduct).await;
    }

    async fn register_node(state: &AppState, addr: &str) {
        assign::heartbeat(&mut state.valkey.clone(), &state.aqueduct, &assign::tests::node(addr))
            .await
            .unwrap();
    }

    // A real mock server (not Canalis), answering POST /proxy with a real
    // SSE stream: one chunk immediately, a second after `delay`. If
    // forward() secretly buffered the whole upstream response before
    // relaying it, both chunks would arrive at our test client together,
    // near t=delay, not one near t=0 and the other near t=delay.
    async fn start_delayed_sse_mock(delay: Duration) -> String {
        async fn handler(AxumState(delay): AxumState<Duration>) -> Response {
            let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(2);
            tokio::spawn(async move {
                let _ = tx
                    .send(Ok(Bytes::from_static(b"event: queued\ndata: {}\n\n")))
                    .await;
                tokio::time::sleep(delay).await;
                let _ = tx
                    .send(Ok(Bytes::from_static(b"event: completed\ndata: {}\n\n")))
                    .await;
            });
            Response::builder()
                .status(200)
                .header(axum::http::header::CONTENT_TYPE, "text/event-stream")
                .header("X-Aqueduct-Queue-Depth", "3")
                .body(Body::from_stream(ReceiverStream::new(rx)))
                .expect("a hardcoded streaming response should always build")
        }

        let app = Router::new()
            .route("/proxy", post(handler))
            .route("/jobs", post(handler))
            .with_state(delay);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr.to_string()
    }

    #[tokio::test]
    async fn sse_response_is_relayed_live_not_buffered() {

        let delay = Duration::from_millis(400);
        let mock_addr = start_delayed_sse_mock(delay).await;

        let (state, _dir) = test_state().await;
        let user_id = unique("user-sse-timing");
        register_node(&state, &mock_addr).await;
        let cleanup_state = state.clone();

        let response = forward(state, job_for(&user_id), "/proxy").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-aqueduct-queue-depth"], "3", "streamed responses relay headers too");

        let mut body = response.into_body();
        let start = Instant::now();
        let mut arrivals = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.expect("streamed frame should not error");
            if frame.is_data() {
                arrivals.push(start.elapsed());
            }
        }
        cleanup(&cleanup_state).await;

        assert_eq!(arrivals.len(), 2, "expected exactly two data chunks");
        let gap = arrivals[1] - arrivals[0];
        // Loose bound on purpose: "roughly delay, not roughly zero" is the
        // actual claim under test.
        assert!(
            gap > delay / 2,
            "expected a gap close to {delay:?} between chunks (proving live relay, not buffering), got {gap:?}"
        );
    }

    fn short_retry_config() -> RetryConfig {
        RetryConfig {
            pool_wait_timeout: Duration::from_millis(500),
            instance_retry_attempts: 3,
            instance_retry_base_delay: Duration::from_millis(20),
        }
    }

    // More headroom for the "eventually succeeds" test: a background task
    // has to register, drain, and complete a real HTTP round-trip before
    // the deadline, which was flaky at 500ms under test-parallelism load.
    fn generous_wait_retry_config() -> RetryConfig {
        RetryConfig {
            pool_wait_timeout: Duration::from_secs(3),
            instance_retry_attempts: 3,
            instance_retry_base_delay: Duration::from_millis(20),
        }
    }

    fn job_for(user_id: &str) -> JobRequest {
        JobRequest {
            user_id: user_id.to_string(),
            idempotent_key: unique("k"),
            url: "https://example.com".into(),
            method: "GET".into(),
            headers: HashMap::new(),
            body: None,
            webhook_url: None,
        }
    }

    async fn start_plain_ok_mock() -> String {
        async fn handler() -> Response {
            Response::builder()
                .status(200)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"status":"completed"}"#))
                .expect("a hardcoded response should always build")
        }
        let app = Router::new()
            .route("/proxy", post(handler))
            .route("/jobs", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr.to_string()
    }

    async fn read_all(response: Response) -> String {
        let body = response.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8_lossy(&body).into_owned()
    }

    // The order the waiting room's events (and the node's) arrived in.
    fn event_names(stream: &str) -> Vec<&str> {
        stream.lines().filter_map(|l| l.strip_prefix("event: ")).collect()
    }

    // Answers the way an overloaded Aquifer does, plus the headers a proxy
    // must not pass on.
    async fn start_rejecting_mock() -> String {
        async fn handler() -> Response {
            Response::builder()
                .status(429)
                .header("Retry-After", "7")
                .header("X-Aqueduct-Rps", "2.00")
                .header("X-Aqueduct-Fullness", "93")
                .header("Connection", "close, X-Hop-Only")
                .header("X-Hop-Only", "secret")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"error":"admission rejected"}"#))
                .expect("a hardcoded response should always build")
        }
        let app = Router::new().route("/proxy", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr.to_string()
    }

    async fn start_schema_upstream() -> String {
        async fn well_known() -> Json<serde_json::Value> {
            Json(serde_json::json!({
                "protocol_version": "0.2",
                "public_key": "AAAA",
                "challenge_endpoint": "/l8/challenge",
                "schema_hash": "sha256:v1",
                "request_schemas": { "POST /v1/chat": { "type": "object", "required": ["model"] } }
            }))
        }
        let app = Router::new().route("/.well-known/l8", get(well_known));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn a_job_that_breaks_its_upstreams_schema_is_blocked_before_the_line() {
        let (mut state, _dir) = test_state().await;
        state.schemas = Arc::new(schemas::SchemaGate::new(true));
        let upstream = start_schema_upstream().await;
        let mut job = job_for("acme");
        job.url = format!("{upstream}/v1/chat");
        job.method = "POST".into();
        job.body = Some(r#"{"messages": []}"#.into());

        // No instances at all: without the gate this job would wait in line.
        let response = forward_with_config(state.clone(), job, "/proxy", &short_retry_config()).await;
        let status = response.status();
        let body: serde_json::Value = serde_json::from_str(&read_all(response).await).unwrap();
        cleanup(&state).await;

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["schema_route"], "POST /v1/chat");
        assert_eq!(body["schema_hash"], "sha256:v1");
        assert!(body["schema_errors"].as_str().unwrap().contains("model"));
        assert_eq!(state.line.len().unwrap(), 0, "a rejected job never takes a spot in line");
    }

    #[tokio::test]
    async fn an_instances_own_headers_reach_the_caller() {
        let (state, _dir) = test_state().await;
        let addr = start_rejecting_mock().await;
        register_node(&state, &addr).await;

        let response = forward_with_config(state.clone(), job_for("acme"), "/proxy", &short_retry_config()).await;
        cleanup(&state).await;

        let headers = response.headers();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(headers["retry-after"], "7", "the instance's own Retry-After reaches the caller");
        assert_eq!(headers["x-aqueduct-rps"], "2.00");
        assert_eq!(headers["x-aqueduct-fullness"], "93");
        assert_eq!(headers["content-type"], "application/json");
        assert!(headers.get("connection").is_none(), "hop-by-hop headers stay behind");
        assert!(headers.get("x-hop-only").is_none(), "so does anything Connection names");
        assert_eq!(headers.get_all("content-type").iter().count(), 1, "relayed once, not twice");
    }

    #[tokio::test]
    async fn a_waiting_caller_also_gets_the_instances_headers() {
        let (state, _dir) = test_state().await;
        let addr = start_rejecting_mock().await;
        let job = job_for("acme");
        let key = job.idempotent_key.clone();

        let task_state = state.clone();
        let task_addr = addr.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            register_node(&task_state, &task_addr).await;
            try_drain_one(task_state).await.unwrap();
        });
        let stream = read_all(forward_with_config(state.clone(), job, "/proxy", &generous_wait_retry_config()).await).await;
        let later = job_status(State(state.clone()), Path(key), Query(StatusQuery { user_id: "acme".into() })).await;
        cleanup(&state).await;

        assert!(stream.contains(r#""retry-after":"7""#), "canalis_result carries the instance's headers: {stream}");
        assert!(!stream.contains("x-hop-only"), "but not hop-by-hop ones");
        assert_eq!(later.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(later.headers()["retry-after"], "7", "and so does the stored result");
    }

    #[tokio::test]
    async fn waiting_in_line_streams_queued_then_the_nodes_answer() {
        let (state, _dir) = test_state().await;
        let cleanup_state = state.clone();
        let user_id = unique("user-pool-wait");

        // Nothing in the pool yet -- forward() has to put the job in line.
        // Registering alone doesn't drain; the real register() handler
        // calls try_drain_one when an instance becomes available, so mirror
        // that here.
        let mock_addr = start_plain_ok_mock().await;
        let task_addr = mock_addr.clone();
        let task_state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            register_node(&task_state, &task_addr).await;
            try_drain_one(task_state).await.unwrap();
        });

        let response =
            forward_with_config(state, job_for(&user_id), "/proxy", &generous_wait_retry_config()).await;
        let status = response.status();
        let content_type = response.headers()[axum::http::header::CONTENT_TYPE].clone();
        let stream = read_all(response).await;
        cleanup(&cleanup_state).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type, "text/event-stream");
        assert_eq!(event_names(&stream), ["canalis_queued", "canalis_assigned", "canalis_result"]);
        assert!(stream.contains(r#"\"status\":\"completed\""#), "the node's answer rides in canalis_result: {stream}");
    }

    #[tokio::test]
    async fn a_nodes_own_stream_is_spliced_in_live_after_canalis_assigned() {
        let (state, _dir) = test_state().await;
        let cleanup_state = state.clone();
        let user_id = unique("user-splice");
        let delay = Duration::from_millis(400);
        let mock_addr = start_delayed_sse_mock(delay).await;

        let task_addr = mock_addr.clone();
        let task_state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            register_node(&task_state, &task_addr).await;
            try_drain_one(task_state).await.unwrap();
        });

        let response =
            forward_with_config(state, job_for(&user_id), "/proxy", &generous_wait_retry_config()).await;
        let mut body = response.into_body();
        let start = Instant::now();
        let mut stream = String::new();
        let mut node_chunk_times = Vec::new();
        while let Some(frame) = body.frame().await {
            if let Ok(data) = frame.unwrap().into_data() {
                let text = String::from_utf8_lossy(&data).into_owned();
                if text.starts_with("event: queued") || text.starts_with("event: completed") {
                    node_chunk_times.push(start.elapsed());
                }
                stream.push_str(&text);
            }
        }
        cleanup(&cleanup_state).await;

        // Canalis's line first, then the node's own events: two different
        // queues, and the names say which is which.
        assert_eq!(event_names(&stream), ["canalis_queued", "canalis_assigned", "queued", "completed"]);
        assert_eq!(node_chunk_times.len(), 2);
        let gap = node_chunk_times[1] - node_chunk_times[0];
        assert!(gap > delay / 2, "the node's stream must be relayed live, not buffered; gap {gap:?}");
    }

    // Answers like Aquifer: POST /jobs -> 201 {"job_id"}, and the job's
    // own lifecycle at GET /jobs/{id}/stream.
    async fn start_aquifer_like_jobs_mock() -> String {
        async fn accept() -> Response {
            Response::builder()
                .status(201)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"job_id":"node-job-1","status":"queued"}"#))
                .expect("a hardcoded response should always build")
        }
        async fn stream(Path(id): Path<String>) -> Response {
            let events = format!(
                "event: queued\ndata: {{\"job_id\":\"{id}\"}}\n\nevent: completed\ndata: {{\"job_id\":\"{id}\",\"response_status\":200}}\n\n"
            );
            Response::builder()
                .status(200)
                .header(axum::http::header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from(events))
                .expect("a hardcoded response should always build")
        }
        let app = Router::new()
            .route("/jobs", post(accept))
            .route("/jobs/{id}/stream", get(stream));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr.to_string()
    }

    #[tokio::test]
    async fn a_jobs_caller_stays_on_the_line_through_the_nodes_job_stream() {
        let (state, _dir) = test_state().await;
        let cleanup_state = state.clone();
        let user_id = unique("user-jobs-follow");
        let mock_addr = start_aquifer_like_jobs_mock().await;

        let task_addr = mock_addr.clone();
        let task_state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            register_node(&task_state, &task_addr).await;
            try_drain_one(task_state).await.unwrap();
        });

        let job = job_for(&user_id);
        let key = job.idempotent_key.clone();
        let line = state.line.clone();
        let response = forward_with_config(state, job, "/jobs", &generous_wait_retry_config()).await;
        let stream = read_all(response).await;
        cleanup(&cleanup_state).await;

        assert_eq!(event_names(&stream), ["canalis_queued", "canalis_assigned", "queued", "completed"]);
        assert!(stream.contains(r#""job_id":"node-job-1""#), "canalis_assigned names the node's job: {stream}");
        let cached = line.get_result(&user_id, &key).unwrap().unwrap();
        assert_eq!(cached.status, 201, "what's cached is the node's acknowledgment");
    }

    #[tokio::test]
    async fn waiting_callers_hear_when_the_line_moves() {
        let (state, _dir) = test_state().await;
        let ahead_of_you = job_for(&unique("ahead-of-you"));
        state.line.enqueue(&ahead_of_you, "/jobs").unwrap();

        // This test's own aqueduct: prefix has no instances at all.
        let response =
            forward_with_config(state.clone(), job_for(&unique("user-moving")), "/proxy", &short_retry_config())
                .await;

        // The job in front leaves the line; the next tick tells the caller.
        state.line.pop_for(&ahead_of_you.user_id).unwrap();
        waiting_room_tick(&state).await.unwrap();

        let stream = read_all(response).await;
        assert_eq!(event_names(&stream), ["canalis_queued", "canalis_position", "canalis_timeout"]);
        assert!(stream.contains(r#""ahead":1"#) && stream.contains(r#""ahead":0"#), "{stream}");
    }

    #[tokio::test]
    async fn a_wait_that_runs_out_closes_the_stream_but_keeps_the_job_in_line() {
        let (state, _dir) = test_state().await;
        let line = state.line.clone();
        let user_id = unique("user-pool-timeout");
        let job = job_for(&user_id);
        let key = job.idempotent_key.clone();

        let start = Instant::now();
        let response = forward_with_config(state, job, "/proxy", &short_retry_config()).await;
        let stream = read_all(response).await;
        let elapsed = start.elapsed();

        assert_eq!(event_names(&stream), ["canalis_queued", "canalis_timeout"]);
        assert!(stream.contains(&format!("/jobs/{key}?user_id={user_id}")), "points at the status endpoint");
        assert!(
            elapsed < Duration::from_secs(2),
            "expected the stream to close near the configured 500ms wait, took {elapsed:?}"
        );
        assert!(
            matches!(line.lookup(&user_id, &key).unwrap(), line::Lookup::Queued { ahead: 0 }),
            "closing the stream must not drop the job from the line"
        );
    }

    #[tokio::test]
    async fn full_line_blocks_with_429_and_retry_after() {
        let limits = line::LineLimits { max_jobs: 1, retry_after_seconds: 7, ..Default::default() };
        let (state, _dir) = test_state_with(limits).await;
        state.line.enqueue(&job_for(&unique("already-waiting")), "/jobs").unwrap();

        let response =
            forward_with_config(state, job_for(&unique("user-full")), "/proxy", &short_retry_config())
                .await;

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get("retry-after").unwrap(), "7");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["limit_reason"], "line_jobs");
        assert_eq!(body["limit"], 1);
    }

    #[tokio::test]
    async fn dead_assigned_instance_retries_then_gives_a_clean_error_not_a_hang() {
        let (state, _dir) = test_state().await;
        let cleanup_state = state.clone();
        let user_id = unique("user-dead-instance");

        // Nothing listens on port 1: an immediate connection-refused.
        let dead_addr = "127.0.0.1:1".to_string();
        register_node(&state, &dead_addr).await;

        let start = Instant::now();
        let response =
            forward_with_config(state, job_for(&user_id), "/proxy", &short_retry_config()).await;
        let elapsed = start.elapsed();
        let status = response.status();
        cleanup(&cleanup_state).await;

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(
            elapsed < Duration::from_secs(2),
            "expected the instance retries to give up quickly, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn one_registration_drains_a_tenants_whole_backlog_not_just_one_job() {
        let (state, _dir) = test_state().await;
        let cleanup_state = state.clone();
        let user_id = unique("user-deep-backlog");
        let mock_addr = start_plain_ok_mock().await;

        let jobs: Vec<JobRequest> = (0..3).map(|_| job_for(&user_id)).collect();
        for job in &jobs {
            state.line.enqueue(job, "/jobs").unwrap();
        }

        register_node(&state, &mock_addr).await;
        let line = state.line.clone();
        assert!(try_drain_one(state).await.unwrap());
        cleanup(&cleanup_state).await;

        for (i, job) in jobs.iter().enumerate() {
            let cached = line
                .get_result(&user_id, &job.idempotent_key)
                .unwrap()
                .unwrap_or_else(|| panic!("job {i} in the backlog was never drained"));
            assert_eq!(cached.status, 200, "job {i} should have been dispatched and completed");
        }
        assert_eq!(line.len().unwrap(), 0);
    }

    #[tokio::test]
    async fn no_free_instance_puts_the_job_back_in_its_place() {
        let (state, _dir) = test_state().await;
        let first = job_for(&unique("first-in-line"));
        let second = job_for(&unique("second-in-line"));
        state.line.enqueue(&first, "/jobs").unwrap();
        state.line.enqueue(&second, "/jobs").unwrap();

        // Nothing registered for these tenants, so the drain can't hand the
        // job anywhere. It must go back in front, not behind `second`.
        let line = state.line.clone();
        assert!(!try_drain_one(state).await.unwrap());
        let next = line.pop_oldest(&HashSet::new()).unwrap().unwrap();
        assert_eq!(next.job.user_id, first.user_id);
    }

    // Crash recovery end to end: a job queued before a restart (a fresh
    // Line opened on the same file) is drained by the startup sweep once
    // an instance is free, with no new request needed to trigger it.
    #[tokio::test]
    async fn the_sweep_drains_jobs_left_in_line_by_a_previous_process() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("line.redb");
        let user_id = unique("user-after-restart");
        let job = job_for(&user_id);
        {
            let before = line::Line::open(&path, line::LineLimits::default()).unwrap();
            before.enqueue(&job, "/jobs").unwrap();
        }

        let line = Arc::new(line::Line::open(&path, line::LineLimits::default()).unwrap());
        let state = AppState {
            valkey: test_valkey().await,
            http: reqwest::Client::new(),
            line: line.clone(),
            draining: Arc::default(),
            waiters: Arc::default(),
            next_waiter_id: Arc::default(),
            aqueduct: Arc::new(assign::tests::test_aqueduct()),
            schemas: Arc::new(schemas::SchemaGate::new(false)),
            l8: Arc::new(l8::L8::new(l8::tests::new_key(), None, false)),
        };
        let cleanup_state = state.clone();
        let mock_addr = start_plain_ok_mock().await;
        register_node(&state, &mock_addr).await;

        let started = sweep_once(state).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut result = None;
        while result.is_none() && Instant::now() < deadline {
            result = line.get_result(&user_id, &job.idempotent_key).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cleanup(&cleanup_state).await;

        assert_eq!(started, 1);
        assert_eq!(result.expect("the restarted line was never drained").status, 200);
    }

    #[tokio::test]
    async fn job_status_reports_place_in_line_then_the_result() {
        let (state, _dir) = test_state().await;
        let user_id = unique("user-status");
        let job = job_for(&user_id);
        state.line.enqueue(&job_for(&unique("ahead-of-you")), "/jobs").unwrap();
        state.line.enqueue(&job, "/jobs").unwrap();

        let ask = |state: AppState| {
            let (key, user_id) = (job.idempotent_key.clone(), user_id.clone());
            async move { job_status(State(state), Path(key), Query(StatusQuery { user_id })).await }
        };

        let queued = ask(state.clone()).await;
        assert_eq!(queued.status(), StatusCode::ACCEPTED);
        let body = queued.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&body).unwrap()["ahead"], 1);

        state.line.pop_for(&user_id).unwrap();
        let result = line::CachedResult { status: 201, body: "made".into(), ..Default::default() };
        state.line.store_result(&user_id, &job.idempotent_key, &result).unwrap();
        assert_eq!(ask(state.clone()).await.status(), StatusCode::CREATED);
    }

    // Answers everything the way a shutting-down Aquifer does.
    async fn start_draining_mock() -> String {
        async fn handler() -> Response {
            Response::builder()
                .status(503)
                .header("X-Aqueduct-Node-State", "draining")
                .header("Retry-After", "1")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"error":"aquifer is draining"}"#))
                .expect("a hardcoded response should always build")
        }
        let app = Router::new()
            .route("/proxy", post(handler))
            .route("/jobs", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr.to_string()
    }

    fn ping(state: &str) -> RegisterRequest {
        RegisterRequest { port: "0".into(), state: Some(state.into()), ..Default::default() }
    }

    // Which instance user_id is assigned to right now, read straight from
    // the shared table (Aquifer's own key shape).
    async fn assigned_to(state: &AppState, user_id: &str) -> Option<String> {
        let key = format!("{}assignment:{}", state.aqueduct.prefix, assign::scope_hash(user_id));
        state.valkey.clone().get(key).await.unwrap()
    }

    async fn claim(state: &AppState, user_id: &str) -> Option<String> {
        match assign::assign(&mut state.valkey.clone(), &state.aqueduct, user_id).await.unwrap() {
            assign::AssignOutcome::Assigned(node) => Some(node.id),
            assign::AssignOutcome::PoolExhausted => None,
        }
    }

    async fn ping_via_handler(state: &AppState, headers: axum::http::HeaderMap, body: &'static [u8]) -> StatusCode {
        let remote: SocketAddr = "127.0.0.1:50000".parse().unwrap();
        register(State(state.clone()), ConnectInfo(remote), headers, Bytes::from_static(body)).await.status()
    }

    async fn is_registered(state: &AppState, id: &str) -> bool {
        assign::member(&mut state.valkey.clone(), &state.aqueduct, id).await.unwrap().is_some()
    }

    #[tokio::test]
    async fn with_signing_required_only_a_trusted_nodes_signed_ping_registers() {
        let (mut state, _dir) = test_state().await;
        let node = l8::tests::new_key();
        let trusted = std::collections::HashSet::from([node.verifying_key().to_bytes()]);
        state.l8 = Arc::new(l8::L8::new(l8::tests::new_key(), Some(trusted), true));
        let body: &'static [u8] = br#"{"port":"7001","state":"active"}"#;

        let unsigned = ping_via_handler(&state, axum::http::HeaderMap::new(), body).await;
        let stranger = ping_via_handler(&state, l8::tests::signed_headers(&l8::tests::new_key(), "d1", body), body).await;
        let registered_before = is_registered(&state, "127.0.0.1:7001").await;
        let signed = ping_via_handler(&state, l8::tests::signed_headers(&node, "d2", body), body).await;
        let registered_after = is_registered(&state, "127.0.0.1:7001").await;
        cleanup(&state).await;

        assert_eq!(unsigned, StatusCode::UNAUTHORIZED);
        assert_eq!(stranger, StatusCode::UNAUTHORIZED);
        assert!(!registered_before, "a refused ping must leave no record");
        assert_eq!(signed, StatusCode::OK);
        assert!(registered_after);
    }

    #[tokio::test]
    async fn without_signing_required_unsigned_pings_still_work_but_a_bad_signature_never_does() {
        let (state, _dir) = test_state().await;
        let body: &'static [u8] = br#"{"port":"7002"}"#;

        let unsigned = ping_via_handler(&state, axum::http::HeaderMap::new(), body).await;
        let forged = ping_via_handler(&state, l8::tests::signed_headers(&l8::tests::new_key(), "d1", body), body).await;
        cleanup(&state).await;

        assert_eq!(unsigned, StatusCode::OK, "existing deployments keep working");
        assert_eq!(forged, StatusCode::UNAUTHORIZED, "signed but unverifiable is refused either way");
    }

    #[tokio::test]
    async fn a_draining_ping_moves_users_off_and_an_active_ping_brings_the_node_back() {
        let (state, _dir) = test_state().await;
        let addr = unique("node");

        handle_registration(state.clone(), &addr, &ping("active")).await.unwrap();
        let before = claim(&state, "on-it").await;

        handle_registration(state.clone(), &addr, &ping("draining")).await.unwrap();
        let while_draining = claim(&state, "on-it").await;
        let newcomer = claim(&state, "newcomer").await;

        handle_registration(state.clone(), &addr, &ping("active")).await.unwrap();
        let after_restart = claim(&state, "on-it").await;

        handle_registration(state.clone(), &addr, &ping("offline")).await.unwrap();
        let after_offline = claim(&state, "on-it").await;
        cleanup(&state).await;

        assert_eq!(before.as_deref(), Some(addr.as_str()));
        assert_eq!(while_draining, None, "a draining node's users move off it");
        assert_eq!(newcomer, None, "and it takes no one new");
        assert_eq!(after_restart.as_deref(), Some(addr.as_str()), "back to active, usable again");
        assert_eq!(after_offline, None, "offline: gone");
    }

    #[tokio::test]
    async fn a_reported_fullness_past_the_threshold_keeps_new_users_off() {
        let (state, _dir) = test_state().await;
        let addr = unique("node");

        let full = RegisterRequest { fullness: Some(90), ..ping("active") };
        handle_registration(state.clone(), &addr, &full).await.unwrap();
        let while_full = claim(&state, "newcomer").await;

        let roomy = RegisterRequest { fullness: Some(10), ..ping("active") };
        handle_registration(state.clone(), &addr, &roomy).await.unwrap();
        let with_room = claim(&state, "newcomer").await;
        cleanup(&state).await;

        assert_eq!(while_full, None, "90% is past CANALIS_QUEUE_AT_PERCENT's 80");
        assert_eq!(with_room.as_deref(), Some(addr.as_str()));
    }

    #[tokio::test]
    async fn a_drain_mode_node_takes_one_tenant_and_is_freed_by_unassigned() {
        let (state, _dir) = test_state().await;
        let addr = unique("drain-node");

        let busy = RegisterRequest { drain_state: Some("active".into()), ..ping("active") };
        handle_registration(state.clone(), &addr, &busy).await.unwrap();
        let first = claim(&state, "tenant-1").await;
        let second_while_busy = claim(&state, "tenant-2").await;

        let free = RegisterRequest { drain_state: Some("unassigned".into()), ..ping("active") };
        handle_registration(state.clone(), &addr, &free).await.unwrap();
        let second_once_free = claim(&state, "tenant-2").await;
        cleanup(&state).await;

        assert_eq!(first.as_deref(), Some(addr.as_str()));
        assert_eq!(second_while_busy, None, "one tenant at a time");
        assert_eq!(second_once_free.as_deref(), Some(addr.as_str()));
    }

    #[tokio::test]
    async fn a_reserved_ping_dedicates_the_node_to_one_user() {
        let (state, _dir) = test_state().await;
        let addr = unique("vip-node");

        let reserved = RegisterRequest { reserved_for: Some("vip".into()), ..ping("active") };
        handle_registration(state.clone(), &addr, &reserved).await.unwrap();
        let stranger = claim(&state, "stranger").await;
        let owner = claim(&state, "vip").await;
        cleanup(&state).await;

        assert_eq!(stranger, None);
        assert_eq!(owner.as_deref(), Some(addr.as_str()));
    }

    async fn start_full_ok_mock(fullness: &'static str) -> String {
        async fn handler(AxumState(fullness): AxumState<&'static str>) -> Response {
            Response::builder()
                .status(200)
                .header("X-Aqueduct-Fullness", fullness)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"status":"completed"}"#))
                .expect("a hardcoded response should always build")
        }
        let app = Router::new().route("/proxy", post(handler)).with_state(fullness);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr.to_string()
    }

    #[tokio::test]
    async fn fullness_on_a_response_keeps_new_users_off_that_node() {
        let (state, _dir) = test_state().await;
        let addr = start_full_ok_mock("95").await;
        register_node(&state, &addr).await;

        let response = forward_with_config(state.clone(), job_for("first"), "/proxy", &short_retry_config()).await;
        assert_eq!(response.status(), StatusCode::OK);

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut newcomer = claim(&state, "newcomer").await;
        while newcomer.is_some() && Instant::now() < deadline {
            // The reading is recorded in the background; give it a moment.
            assign::release_user(&mut state.valkey.clone(), &state.aqueduct, "newcomer", &addr).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            newcomer = claim(&state, "newcomer").await;
        }
        cleanup(&state).await;

        assert_eq!(newcomer, None, "the node said it's 95% full");
    }

    #[tokio::test]
    async fn a_request_hitting_a_draining_node_is_rerouted_not_failed() {
        let (state, _dir) = test_state().await;
        let user_id = unique("user-rerouted");
        let draining_addr = start_draining_mock().await;
        let ok_addr = start_plain_ok_mock().await;

        // Assigned to the node that's shutting down (its draining ping
        // hasn't arrived yet), then a healthy node comes along.
        register_node(&state, &draining_addr).await;
        assert_eq!(claim(&state, &user_id).await.as_deref(), Some(draining_addr.as_str()));
        register_node(&state, &ok_addr).await;

        let response = forward_with_config(state.clone(), job_for(&user_id), "/proxy", &short_retry_config()).await;
        let status = response.status();
        let now_on = assigned_to(&state, &user_id).await;
        cleanup(&state).await;

        assert_eq!(status, StatusCode::OK, "the caller should get the healthy node's answer");
        assert_eq!(now_on.as_deref(), Some(ok_addr.as_str()));
    }

    #[tokio::test]
    async fn draining_the_line_into_a_draining_node_keeps_the_jobs_in_line() {
        let (state, _dir) = test_state().await;
        let user_id = unique("user-line-vs-draining");
        let draining_addr = start_draining_mock().await;

        let jobs: Vec<JobRequest> = (0..2).map(|_| job_for(&user_id)).collect();
        for job in &jobs {
            state.line.enqueue(job, "/jobs").unwrap();
        }
        register_node(&state, &draining_addr).await;

        let line = state.line.clone();
        assert!(try_drain_one(state.clone()).await.unwrap());
        let assignment = assigned_to(&state, &user_id).await;
        let next_claim = claim(&state, &user_id).await;
        cleanup(&state).await;

        assert_eq!(line.len().unwrap(), 2, "both jobs must still be waiting, not lost");
        for job in &jobs {
            assert!(
                line.get_result(&user_id, &job.idempotent_key).unwrap().is_none(),
                "a draining node's 503 must not be stored as the job's result"
            );
        }
        let first = line.pop_oldest(&HashSet::new()).unwrap().unwrap();
        assert_eq!(first.job.idempotent_key, jobs[0].idempotent_key, "and still in their original order");
        assert!(assignment.is_none(), "the tenant must be free to get a different node");
        assert_eq!(next_claim, None, "and the draining node isn't offered again");
    }

    // Every response takes `delay`, and advertises the given
    // max_concurrent/rps under `<header_prefix>Max-Concurrent` /
    // `<header_prefix>Rps` -- lets a test prove whether
    // drain_tenant_backlog actually acted on those headers by measuring
    // wall-clock time.
    async fn start_delayed_pacing_mock(
        delay: Duration,
        header_prefix: &'static str,
        max_concurrent: &'static str,
        rps: &'static str,
    ) -> String {
        async fn handler(
            AxumState((delay, header_prefix, max_concurrent, rps)): AxumState<(
                Duration,
                &'static str,
                &'static str,
                &'static str,
            )>,
        ) -> Response {
            tokio::time::sleep(delay).await;
            Response::builder()
                .status(200)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .header(format!("{header_prefix}Max-Concurrent"), max_concurrent)
                .header(format!("{header_prefix}Rps"), rps)
                .body(Body::from(r#"{"status":"completed"}"#))
                .expect("a hardcoded response should always build")
        }
        let app = Router::new()
            .route("/jobs", post(handler))
            .with_state((delay, header_prefix, max_concurrent, rps));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr.to_string()
    }

    async fn assert_pacing_header_drives_concurrency(header_prefix: &'static str, user_id_prefix: &str) {
        let (state, _dir) = test_state().await;
        let cleanup_state = state.clone();
        let user_id = unique(user_id_prefix);

        // Every response advertises room for 4 concurrent, at 100rps. The
        // *first* job still goes out alone against the conservative
        // default, so: job 1 pays the 300ms delay by itself, then jobs 2-4
        // pay it together. Fully serial would take ~1200ms; header-driven
        // concurrency lands close to ~600ms.
        let delay = Duration::from_millis(300);
        let mock_addr = start_delayed_pacing_mock(delay, header_prefix, "4", "100").await;

        for _ in 0..4 {
            state.line.enqueue(&job_for(&user_id), "/jobs").unwrap();
        }
        register_node(&state, &mock_addr).await;

        let start = Instant::now();
        try_drain_one(state).await.unwrap();
        let elapsed = start.elapsed();
        cleanup(&cleanup_state).await;

        assert!(
            elapsed < Duration::from_millis(900),
            "expected {header_prefix}-driven concurrency to clear 4 jobs in ~2 waves of {delay:?}, not 4 sequential waves; took {elapsed:?}"
        );
        assert!(
            elapsed >= delay,
            "expected at least one full delay's worth of real waiting (job 1 always goes out alone), took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn x_canalis_headers_raise_concurrency_and_pacing_mid_drain() {
        assert_pacing_header_drives_concurrency("X-Canalis-", "user-pacing-canalis").await;
    }

    // Proves pacing_header's fallback chain, not just its preferred name.
    #[tokio::test]
    async fn x_aqueduct_headers_are_accepted_as_a_fallback_pacing_signal() {
        assert_pacing_header_drives_concurrency("X-Aqueduct-", "user-pacing-aqueduct").await;
    }
}
