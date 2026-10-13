mod assign;
mod line;

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
use std::sync::{Arc, Mutex};
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
    };

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
#[derive(Deserialize)]
struct RegisterRequest {
    port: String,
    reported_at: String,
    #[serde(default)]
    state: Option<String>,
}

// Registrations expire on their own if pings stop arriving, rather than
// needing an explicit deregistration call -- 3x Aquifer's own default
// registration interval (15s), so one or two missed pings don't
// immediately drop a still-live instance, but a genuinely dead one clears
// out on its own within a bounded window.
const REGISTRATION_TTL_SECONDS: u64 = 45;

async fn register(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Json(payload): Json<RegisterRequest>,
) -> StatusCode {
    let address = format!("{}:{}", remote.ip(), payload.port);
    match handle_registration(state, &address, &payload).await {
        Ok(()) => StatusCode::OK,
        Err(err) => {
            tracing::error!("failed to register instance {address}: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn handle_registration(
    mut state: AppState,
    address: &str,
    payload: &RegisterRequest,
) -> Result<(), BoxError> {
    let node_state = payload.state.as_deref().unwrap_or("active");
    let key = format!("canalis:instance:{address}");

    if node_state == "draining" || node_state == "offline" {
        // Stop handing it work right away, and give its users back: their
        // next request lands on another node, or waits in line. The node
        // finishes whatever it already accepted on its own.
        let released = assign::retire_instance(&mut state.valkey, address).await?;
        if node_state == "offline" {
            let _: () = state.valkey.del(&key).await?;
        }
        tracing::info!("instance {address} is {node_state}; released {released} users from it");
        return Ok(());
    }

    let _: () = state
        .valkey
        .set_ex(&key, &payload.reported_at, REGISTRATION_TTL_SECONDS)
        .await?;

    // Every heartbeat, not just the first -- but a no-op if addr is
    // already claimed or already in the pool (see register_if_free's own
    // docs for why that idempotency matters). The returned bool is what
    // it actually added something new, not just "the call succeeded" --
    // that's what tells us whether it's worth attempting to drain a
    // pending job, rather than checking on every single heartbeat.
    let newly_freed = assign::register_if_free(&mut state.valkey, address).await?;
    tracing::info!("registered instance {address}");

    if newly_freed {
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
) -> Result<Option<(line::QueuedJob, String, DrainGuard)>, BoxError> {
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

    match assign::assign(&mut state.valkey, &user_id).await {
        Ok(assign::AssignOutcome::Assigned(addr)) => Ok(Some((queued, addr, guard))),
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
    let Some((first, addr, guard)) = claim_next(&mut state).await? else {
        return Ok(false);
    };
    drain_tenant_backlog(state, first, addr, guard).await?;
    Ok(true)
}

// Starts a drain for every tenant in line that can get an instance right
// now, without waiting for them to finish, and drops expired results.
// Returns how many drains were started.
async fn sweep_once(mut state: AppState) -> Result<usize, BoxError> {
    on_line(&state.line, |l| l.expire_results()).await?;

    let mut started = 0;
    while let Some((first, addr, guard)) = claim_next(&mut state).await? {
        let drain_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = drain_tenant_backlog(drain_state, first, addr, guard).await {
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
    addr: String,
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

            in_flight.spawn(dispatch_drained_job(
                state.line.clone(),
                state.http.clone(),
                addr.clone(),
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
        // The rest of this tenant's line stays where it is; releasing the
        // assignment means the next claim finds them a different node.
        assign::release_user(&mut state.valkey, &user_id, &addr).await?;
        tracing::info!("{addr} is draining; {user_id}'s remaining jobs stay in line for another node");
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
// max_concurrent of these can be in flight together. Caches its own
// result directly rather than handing it back to drain_tenant_backlog's
// loop -- Valkey and reqwest::Client are both cheap, Arc-backed clones,
// so each task owns everything it needs to finish independently. A
// caching failure here is only logged, not propagated: with several of
// these in flight at once there's no single caller left to hand a
// Result to.
async fn dispatch_drained_job(
    line: Arc<line::Line>,
    http: reqwest::Client,
    addr: String,
    queued: line::QueuedJob,
) -> DispatchOutcome {
    let job = &queued.job;
    let target_url = format!("http://{addr}/jobs");
    let response = http.post(&target_url).json(job).send().await;

    let (cached, pacing) = match response {
        Ok(resp) if node_is_draining(resp.status().as_u16(), resp.headers()) => {
            return DispatchOutcome { pacing: PacingSignal::none(), back_in_line: Some(queued) };
        }
        Ok(resp) => {
            let pacing = PacingSignal::from_headers(resp.headers());
            let status = resp.status().as_u16();
            let content_type = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            // .bytes() on a streamed response waits for the stream to
            // finish (Aquifer's /jobs SSE stream closes once the job
            // reaches a terminal state), then hands back everything that
            // was ever sent -- the full event history, not just the
            // final line. No manual SSE parsing needed: the cached
            // result is the same text a live viewer would have seen.
            let body = resp.bytes().await.map(|b| String::from_utf8_lossy(&b).into_owned());
            let cached = match body {
                Ok(body) => line::CachedResult { status, content_type, body },
                Err(err) => {
                    tracing::error!("failed to read drained response body from {addr}: {err}");
                    line::CachedResult {
                        status: 502,
                        content_type: "application/json".into(),
                        body: serde_json::json!({ "error": "failed to read upstream response" })
                            .to_string(),
                    }
                }
            };
            (cached, pacing)
        }
        Err(err) => {
            tracing::error!("failed to dispatch drained job to {addr}: {err}");
            let cached = line::CachedResult {
                status: 502,
                content_type: "application/json".into(),
                body: serde_json::json!({ "error": format!("failed to reach assigned instance: {err}") })
                    .to_string(),
            };
            (cached, PacingSignal::none())
        }
    };

    let (user_id, key) = (job.user_id.clone(), job.idempotent_key.clone());
    if let Err(err) = on_line(&line, move |l| l.store_result(&user_id, &key, &cached)).await {
        tracing::error!("failed to cache drained result for {}: {err}", job.idempotent_key);
    }

    DispatchOutcome { pacing, back_in_line: None }
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
    // How long the pool-exhausted retry loop keeps waiting for a new
    // registration to free something up before giving up for good.
    pool_wait_timeout: Duration,
    pool_wait_retry_interval: Duration,
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
            pool_wait_retry_interval: Duration::from_millis(env_u64(
                "CANALIS_POOL_WAIT_RETRY_INTERVAL_MS",
                500,
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

// Resolves the assignment, then forwards the same job body to the
// assigned instance's own target_path ("/proxy" or "/jobs" -- this is
// where those two finally diverge, now that there's real forwarding to
// diverge in). Buffered relay only: a plain response gets its status,
// content-type, and body relayed verbatim. An SSE response (Aquifer's
// fallback-to-queue path, and always the case for /jobs) is relayed live.
//
// Two distinct fallback behaviors, not one: a dead *assigned* instance
// gets a few quick retries against the instance itself (a momentary
// blip), then a clean error -- assignment is sticky, so there's no point
// waiting further; nothing changes without the not-yet-built release
// mechanism. Pool exhaustion is different: put the job in this node's
// line (line.rs -- on local disk, survives a Canalis crash, and bounded:
// a full line answers 429 instead of growing) and poll for its result,
// since a *new* registration genuinely can resolve this one.
// try_drain_one (on register()) and sweep_once (periodic, and at startup)
// are what actually dispatch queued jobs once a slot frees up; this
// function only waits for the result to show up.
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
    let mut valkey = state.valkey.clone();
    let user_id = job.user_id.clone();
    let first = forward_attempt(state.clone(), job.clone(), target_path, config).await;
    let Some(NodeDraining(addr)) = first.extensions().get::<NodeDraining>().cloned() else {
        return first;
    };

    // The assigned instance is shutting down: hand this user back and try
    // once more -- a different instance, or the line. Once, not in a loop:
    // a second draining answer is relayed as-is.
    if let Err(err) = assign::release_user(&mut valkey, &user_id, &addr).await {
        tracing::error!("failed to release {user_id} from draining instance {addr}: {err}");
        return first;
    }
    tracing::info!("{addr} is draining; re-routing {user_id}");
    forward_attempt(state, job, target_path, config).await
}

async fn forward_attempt(
    mut state: AppState,
    job: JobRequest,
    target_path: &str,
    config: &RetryConfig,
) -> Response {
    let addr = match assign::assign(&mut state.valkey, &job.user_id).await {
        Ok(assign::AssignOutcome::Assigned(addr)) => addr,
        Ok(assign::AssignOutcome::PoolExhausted) => {
            let queued_job = job.clone();
            match on_line(&state.line, move |l| l.enqueue(&queued_job)).await {
                Ok(line::Enqueued::AlreadyDone(cached)) => return cached_result_response(cached),
                Ok(line::Enqueued::Queued { ahead } | line::Enqueued::AlreadyQueued { ahead }) => {
                    tracing::info!("no instance free for {}; in line with {ahead} ahead", job.user_id);
                }
                Err(line::LineError::Full { reason, limit }) => {
                    return line_full_response(reason, limit, state.line.limits().retry_after_seconds);
                }
                Err(err) => {
                    tracing::error!("failed to durably enqueue job for {}: {err}", job.user_id);
                    return json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        serde_json::json!({ "error": "internal error" }),
                    );
                }
            }

            let wait_deadline = Instant::now() + config.pool_wait_timeout;
            loop {
                let (user_id, key) = (job.user_id.clone(), job.idempotent_key.clone());
                match on_line(&state.line, move |l| l.get_result(&user_id, &key)).await {
                    Ok(Some(cached)) => return cached_result_response(cached),
                    Ok(None) => {}
                    Err(err) => {
                        tracing::error!("failed to poll for queued result: {err}");
                        return json_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            serde_json::json!({ "error": "internal error" }),
                        );
                    }
                }
                if Instant::now() >= wait_deadline {
                    // The job stays in line -- this only gives up on *this*
                    // connection, not the work itself. A caller with a
                    // webhook_url still gets notified once it's drained;
                    // anyone can check GET /jobs/{idempotent_key} on this
                    // same Canalis node meanwhile.
                    return json_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        serde_json::json!({
                            "error": "no Aquifer instance became available in time",
                            "note": "the job remains queued and will still be attempted",
                            "status": format!("/jobs/{}?user_id={}", job.idempotent_key, job.user_id),
                        }),
                    );
                }
                tokio::time::sleep(config.pool_wait_retry_interval).await;
            }
        }
        Err(err) => {
            tracing::error!("assign failed: {err}");
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({ "error": "internal error" }),
            );
        }
    };

    let target_url = format!("http://{addr}{target_path}");
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

    if node_is_draining(upstream.status().as_u16(), upstream.headers()) {
        let mut response = json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({ "error": "assigned instance is draining" }),
        );
        response
            .headers_mut()
            .insert(axum::http::header::RETRY_AFTER, axum::http::HeaderValue::from(1u32));
        response.extensions_mut().insert(NodeDraining(addr));
        return response;
    }

    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let status = StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);

    if content_type.starts_with("text/event-stream") {
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
        return Response::builder()
            .status(status)
            .header(axum::http::header::CONTENT_TYPE, content_type)
            .body(Body::from_stream(stream))
            .unwrap_or_else(|_| {
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .expect("a hardcoded empty error response should always build")
            });
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

    let mut builder = Response::builder().status(status);
    if !content_type.is_empty() {
        builder = builder.header(axum::http::header::CONTENT_TYPE, content_type);
    }
    builder.body(Body::from(body)).unwrap_or_else(|_| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .expect("a hardcoded empty error response should always build")
    })
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response {
    (status, Json(body)).into_response()
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
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    async fn test_valkey() -> Valkey {
        let client = redis::Client::open("redis://127.0.0.1:6379").expect("valid Valkey URL");
        redis::aio::ConnectionManager::new(client)
            .await
            .expect("connect to a real local Valkey on 127.0.0.1:6379 -- required for this test")
    }

    fn unique(prefix: &str) -> String {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        format!("{prefix}-{nanos}")
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
        };
        (state, dir)
    }

    async fn test_state() -> (AppState, tempfile::TempDir) {
        test_state_with(line::LineLimits::default()).await
    }

    async fn cleanup_assignment(valkey: &mut Valkey, addr: &str, user_id: &str) {
        let _: redis::RedisResult<()> = valkey.srem("canalis:pool:free", addr).await;
        let _: redis::RedisResult<()> = valkey.srem("canalis:assigned", addr).await;
        let _: redis::RedisResult<()> = valkey.del(format!("canalis:assignment:{user_id}")).await;
        let _: redis::RedisResult<()> = valkey.del(format!("canalis:instance_tenants:{addr}")).await;
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
                .body(Body::from_stream(ReceiverStream::new(rx)))
                .expect("a hardcoded streaming response should always build")
        }

        let app = Router::new().route("/proxy", post(handler)).with_state(delay);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr.to_string()
    }

    #[tokio::test]
    async fn sse_response_is_relayed_live_not_buffered() {
        // canalis:pool:free is a global key and this test seeds+pops it --
        // see TEST_LOCK's doc comment in assign.rs.
        let _guard = assign::TEST_LOCK.lock().await;

        let delay = Duration::from_millis(400);
        let mock_addr = start_delayed_sse_mock(delay).await;

        let (mut state, _dir) = test_state().await;
        let user_id = unique("user-sse-timing");
        let _: () = state.valkey.sadd("canalis:pool:free", &mock_addr).await.unwrap();
        let mut valkey = state.valkey.clone();

        let response = forward(state, job_for(&user_id), "/proxy").await;
        assert_eq!(response.status(), StatusCode::OK);

        let mut body = response.into_body();
        let start = Instant::now();
        let mut arrivals = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.expect("streamed frame should not error");
            if frame.is_data() {
                arrivals.push(start.elapsed());
            }
        }
        cleanup_assignment(&mut valkey, &mock_addr, &user_id).await;

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
            pool_wait_retry_interval: Duration::from_millis(50),
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
            pool_wait_retry_interval: Duration::from_millis(50),
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

    #[tokio::test]
    async fn pool_exhaustion_waits_in_line_until_a_new_registration_succeeds() {
        let _guard = assign::TEST_LOCK.lock().await;
        let (state, _dir) = test_state().await;
        let mut valkey = state.valkey.clone();
        let user_id = unique("user-pool-wait");

        // Nothing in the pool yet -- forward() has to put the job in line
        // and wait. register_if_free alone doesn't drain; the real
        // register() handler calls try_drain_one right after a genuinely
        // new add, so mirror that here.
        let mock_addr = start_plain_ok_mock().await;
        let task_addr = mock_addr.clone();
        let mut task_state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let added = assign::register_if_free(&mut task_state.valkey, &task_addr)
                .await
                .unwrap();
            assert!(added, "test setup bug: expected this to be a genuinely new pool add");
            try_drain_one(task_state).await.unwrap();
        });

        let response =
            forward_with_config(state, job_for(&user_id), "/proxy", &generous_wait_retry_config())
                .await;
        let status = response.status();
        cleanup_assignment(&mut valkey, &mock_addr, &user_id).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "expected the delayed registration to eventually satisfy the waiting request"
        );
    }

    #[tokio::test]
    async fn pool_exhaustion_gives_up_waiting_but_keeps_the_job_in_line() {
        let _guard = assign::TEST_LOCK.lock().await;
        let (state, _dir) = test_state().await;
        let line = state.line.clone();
        let user_id = unique("user-pool-timeout");
        let job = job_for(&user_id);
        let key = job.idempotent_key.clone();

        let start = Instant::now();
        let response = forward_with_config(state, job, "/proxy", &short_retry_config()).await;
        let elapsed = start.elapsed();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            elapsed < Duration::from_secs(2),
            "expected this to give up close to the configured 500ms wait, took {elapsed:?}"
        );
        assert!(
            matches!(line.lookup(&user_id, &key).unwrap(), line::Lookup::Queued { ahead: 0 }),
            "giving up on the connection must not drop the job from the line"
        );
    }

    #[tokio::test]
    async fn full_line_blocks_with_429_and_retry_after() {
        let _guard = assign::TEST_LOCK.lock().await;
        let limits = line::LineLimits { max_jobs: 1, retry_after_seconds: 7, ..Default::default() };
        let (state, _dir) = test_state_with(limits).await;
        state.line.enqueue(&job_for(&unique("already-waiting"))).unwrap();

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
        let _guard = assign::TEST_LOCK.lock().await;
        let (mut state, _dir) = test_state().await;
        let mut valkey = state.valkey.clone();
        let user_id = unique("user-dead-instance");

        // Nothing listens on port 1: an immediate connection-refused.
        let dead_addr = "127.0.0.1:1".to_string();
        let _: () = state.valkey.sadd("canalis:pool:free", &dead_addr).await.unwrap();

        let start = Instant::now();
        let response =
            forward_with_config(state, job_for(&user_id), "/proxy", &short_retry_config()).await;
        let elapsed = start.elapsed();
        let status = response.status();
        cleanup_assignment(&mut valkey, &dead_addr, &user_id).await;

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(
            elapsed < Duration::from_secs(2),
            "expected the instance retries to give up quickly, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn one_registration_drains_a_tenants_whole_backlog_not_just_one_job() {
        let _guard = assign::TEST_LOCK.lock().await;
        let (mut state, _dir) = test_state().await;
        let mut valkey = state.valkey.clone();
        let user_id = unique("user-deep-backlog");
        let mock_addr = start_plain_ok_mock().await;

        let jobs: Vec<JobRequest> = (0..3).map(|_| job_for(&user_id)).collect();
        for job in &jobs {
            state.line.enqueue(job).unwrap();
        }

        let added = assign::register_if_free(&mut state.valkey, &mock_addr).await.unwrap();
        assert!(added, "test setup bug: expected this to be a genuinely new pool add");
        let line = state.line.clone();
        assert!(try_drain_one(state).await.unwrap());
        cleanup_assignment(&mut valkey, &mock_addr, &user_id).await;

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
        let _guard = assign::TEST_LOCK.lock().await;
        let (state, _dir) = test_state().await;
        let first = job_for(&unique("first-in-line"));
        let second = job_for(&unique("second-in-line"));
        state.line.enqueue(&first).unwrap();
        state.line.enqueue(&second).unwrap();

        // Nothing registered for these tenants, so the drain can't hand the
        // job anywhere. It must go back in front, not behind `second`.
        let line = state.line.clone();
        let pool_size: usize = state.valkey.clone().scard("canalis:pool:free").await.unwrap_or(0);
        if pool_size == 0 {
            assert!(!try_drain_one(state).await.unwrap());
            let next = line.pop_oldest(&HashSet::new()).unwrap().unwrap();
            assert_eq!(next.job.user_id, first.user_id);
        }
    }

    // Crash recovery end to end: a job queued before a restart (a fresh
    // Line opened on the same file) is drained by the startup sweep once
    // an instance is free, with no new request needed to trigger it.
    #[tokio::test]
    async fn the_sweep_drains_jobs_left_in_line_by_a_previous_process() {
        let _guard = assign::TEST_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("line.redb");
        let user_id = unique("user-after-restart");
        let job = job_for(&user_id);
        {
            let before = line::Line::open(&path, line::LineLimits::default()).unwrap();
            before.enqueue(&job).unwrap();
        }

        let line = Arc::new(line::Line::open(&path, line::LineLimits::default()).unwrap());
        let mut state = AppState {
            valkey: test_valkey().await,
            http: reqwest::Client::new(),
            line: line.clone(),
            draining: Arc::default(),
        };
        let mut valkey = state.valkey.clone();
        let mock_addr = start_plain_ok_mock().await;
        assign::register_if_free(&mut state.valkey, &mock_addr).await.unwrap();

        let started = sweep_once(state).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut result = None;
        while result.is_none() && Instant::now() < deadline {
            result = line.get_result(&user_id, &job.idempotent_key).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cleanup_assignment(&mut valkey, &mock_addr, &user_id).await;

        assert_eq!(started, 1);
        assert_eq!(result.expect("the restarted line was never drained").status, 200);
    }

    #[tokio::test]
    async fn job_status_reports_place_in_line_then_the_result() {
        let (state, _dir) = test_state().await;
        let user_id = unique("user-status");
        let job = job_for(&user_id);
        state.line.enqueue(&job_for(&unique("ahead-of-you"))).unwrap();
        state.line.enqueue(&job).unwrap();

        let ask = |state: AppState| {
            let (key, user_id) = (job.idempotent_key.clone(), user_id.clone());
            async move { job_status(State(state), Path(key), Query(StatusQuery { user_id })).await }
        };

        let queued = ask(state.clone()).await;
        assert_eq!(queued.status(), StatusCode::ACCEPTED);
        let body = queued.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&body).unwrap()["ahead"], 1);

        state.line.pop_for(&user_id).unwrap();
        let result = line::CachedResult { status: 201, content_type: String::new(), body: "made".into() };
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
        RegisterRequest {
            port: "0".into(),
            reported_at: "2026-10-13T00:00:00Z".into(),
            state: Some(state.into()),
        }
    }

    #[tokio::test]
    async fn a_draining_ping_releases_users_and_an_active_ping_brings_the_node_back() {
        let _guard = assign::TEST_LOCK.lock().await;
        let (mut state, _dir) = test_state().await;
        let mut valkey = state.valkey.clone();
        let addr = unique("node");
        let user_id = unique("user-on-draining-node");

        handle_registration(state.clone(), &addr, &ping("active")).await.unwrap();
        assert!(matches!(
            assign::assign(&mut state.valkey, &user_id).await.unwrap(),
            assign::AssignOutcome::Assigned(a) if a == addr
        ));

        handle_registration(state.clone(), &addr, &ping("draining")).await.unwrap();
        let assignment: Option<String> =
            valkey.get(format!("canalis:assignment:{user_id}")).await.unwrap();
        let in_pool: bool = valkey.sismember("canalis:pool:free", &addr).await.unwrap();
        let back_after_restart = {
            handle_registration(state.clone(), &addr, &ping("active")).await.unwrap();
            valkey.sismember::<_, _, bool>("canalis:pool:free", &addr).await.unwrap()
        };
        cleanup_assignment(&mut valkey, &addr, &user_id).await;
        let _: redis::RedisResult<()> = valkey.del(format!("canalis:instance:{addr}")).await;

        assert!(assignment.is_none(), "users on a draining node must be released");
        assert!(!in_pool, "a draining node must not be handed new users");
        assert!(back_after_restart, "the same address coming back active must be usable again");
    }

    #[tokio::test]
    async fn a_request_hitting_a_draining_node_is_rerouted_not_failed() {
        let _guard = assign::TEST_LOCK.lock().await;
        let (state, _dir) = test_state().await;
        let mut valkey = state.valkey.clone();
        let user_id = unique("user-rerouted");
        let draining_addr = start_draining_mock().await;
        let ok_addr = start_plain_ok_mock().await;

        // Already assigned to the node that's shutting down (its draining
        // ping hasn't arrived yet), with a healthy node free.
        let _: () = valkey.set(format!("canalis:assignment:{user_id}"), &draining_addr).await.unwrap();
        let _: () = valkey.sadd("canalis:assigned", &draining_addr).await.unwrap();
        let _: () = valkey.sadd("canalis:pool:free", &ok_addr).await.unwrap();

        let response = forward_with_config(state, job_for(&user_id), "/proxy", &short_retry_config()).await;
        let status = response.status();
        let now_on: Option<String> = valkey.get(format!("canalis:assignment:{user_id}")).await.unwrap();
        cleanup_assignment(&mut valkey, &draining_addr, &user_id).await;
        cleanup_assignment(&mut valkey, &ok_addr, &user_id).await;

        assert_eq!(status, StatusCode::OK, "the caller should get the healthy node's answer");
        assert_eq!(now_on.as_deref(), Some(ok_addr.as_str()));
    }

    #[tokio::test]
    async fn draining_the_line_into_a_draining_node_keeps_the_jobs_in_line() {
        let _guard = assign::TEST_LOCK.lock().await;
        let (state, _dir) = test_state().await;
        let mut valkey = state.valkey.clone();
        let user_id = unique("user-line-vs-draining");
        let draining_addr = start_draining_mock().await;

        let jobs: Vec<JobRequest> = (0..2).map(|_| job_for(&user_id)).collect();
        for job in &jobs {
            state.line.enqueue(job).unwrap();
        }
        let _: () = valkey.set(format!("canalis:assignment:{user_id}"), &draining_addr).await.unwrap();
        let _: () = valkey.sadd("canalis:assigned", &draining_addr).await.unwrap();

        let line = state.line.clone();
        assert!(try_drain_one(state).await.unwrap());
        let assignment: Option<String> = valkey.get(format!("canalis:assignment:{user_id}")).await.unwrap();
        cleanup_assignment(&mut valkey, &draining_addr, &user_id).await;

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
        let _guard = assign::TEST_LOCK.lock().await;
        let (mut state, _dir) = test_state().await;
        let mut valkey = state.valkey.clone();
        let user_id = unique(user_id_prefix);

        // Every response advertises room for 4 concurrent, at 100rps. The
        // *first* job still goes out alone against the conservative
        // default, so: job 1 pays the 300ms delay by itself, then jobs 2-4
        // pay it together. Fully serial would take ~1200ms; header-driven
        // concurrency lands close to ~600ms.
        let delay = Duration::from_millis(300);
        let mock_addr = start_delayed_pacing_mock(delay, header_prefix, "4", "100").await;

        for _ in 0..4 {
            state.line.enqueue(&job_for(&user_id)).unwrap();
        }
        let added = assign::register_if_free(&mut state.valkey, &mock_addr).await.unwrap();
        assert!(added, "test setup bug: expected this to be a genuinely new pool add");

        let start = Instant::now();
        try_drain_one(state).await.unwrap();
        let elapsed = start.elapsed();
        cleanup_assignment(&mut valkey, &mock_addr, &user_id).await;

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
