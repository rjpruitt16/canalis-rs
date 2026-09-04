mod assign;
mod account_queue;

use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

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

    let state = AppState {
        valkey,
        http: reqwest::Client::new(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/valkey-check", get(valkey_check))
        .route("/register", post(register))
        .route("/proxy", post(proxy))
        .route("/jobs", post(jobs))
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
#[derive(Deserialize)]
struct RegisterRequest {
    port: String,
    reported_at: String,
}

// Registrations expire on their own if pings stop arriving, rather than
// needing an explicit deregistration call -- 3x Aquifer's own default
// registration interval (15s), so one or two missed pings don't
// immediately drop a still-live instance, but a genuinely dead one clears
// out on its own within a bounded window.
const REGISTRATION_TTL_SECONDS: u64 = 45;

async fn register(
    State(mut state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Json(payload): Json<RegisterRequest>,
) -> StatusCode {
    let address = format!("{}:{}", remote.ip(), payload.port);
    let key = format!("canalis:instance:{address}");

    let result: redis::RedisResult<()> = state
        .valkey
        .set_ex(&key, &payload.reported_at, REGISTRATION_TTL_SECONDS)
        .await;

    if let Err(err) = result {
        tracing::error!("failed to register instance {address}: {err}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    // Every heartbeat, not just the first -- but a no-op if addr is
    // already claimed or already in the pool (see register_if_free's own
    // docs for why that idempotency matters). The returned bool is what
    // it actually added something new, not just "the call succeeded" --
    // that's what tells us whether it's worth attempting to drain a
    // pending job, rather than checking on every single heartbeat.
    let newly_freed = match assign::register_if_free(&mut state.valkey, &address).await {
        Ok(added) => added,
        Err(err) => {
            tracing::error!("failed to add {address} to the free pool: {err}");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    tracing::info!("registered instance {address}");

    if newly_freed {
        // Spawned, not awaited: registration shouldn't block on a full
        // job dispatch completing. If nothing's pending, pop_one just
        // returns None immediately and this does nothing.
        tokio::spawn(async move {
            if let Err(err) = try_drain_one(state).await {
                tracing::error!("failed while attempting to drain a pending job: {err}");
            }
        });
    }

    StatusCode::OK
}

// Pops one pending job (if any) and dispatches it for real, now that a
// slot has freed up -- headless, no live connection to relay to, so the
// outcome gets cached for whichever still-open request (or future
// polling endpoint) is waiting on it. Always dispatches to the assigned
// instance's own /jobs, not /proxy: this job was already queued once, so
// there's no reason to attempt a synchronous direct attempt again --
// /jobs already gives a uniform "wait for the full SSE lifecycle to
// finish, then read whatever it settled on" shape, which is exactly
// what's needed here (no live connection to stream to, only a final
// result worth keeping).
async fn try_drain_one(mut state: AppState) -> redis::RedisResult<()> {
    let Some(job) = account_queue::pop_one(&mut state.valkey).await? else {
        return Ok(());
    };

    let addr = match assign::assign(&mut state.valkey, &job.user_id).await {
        Ok(assign::AssignOutcome::Assigned(addr)) => addr,
        Ok(assign::AssignOutcome::PoolExhausted) => {
            // Someone else's request claimed the freed instance in the
            // gap between register() and this task running. Put the job
            // back for the next registration to try.
            return account_queue::enqueue(&mut state.valkey, &job).await;
        }
        Err(err) => return Err(err),
    };

    let target_url = format!("http://{addr}/jobs");
    let response = state.http.post(&target_url).json(&job).send().await;

    let cached = match response {
        Ok(resp) => {
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
            match body {
                Ok(body) => account_queue::CachedResult { status, content_type, body },
                Err(err) => {
                    tracing::error!("failed to read drained response body from {addr}: {err}");
                    account_queue::CachedResult {
                        status: 502,
                        content_type: "application/json".into(),
                        body: serde_json::json!({ "error": "failed to read upstream response" })
                            .to_string(),
                    }
                }
            }
        }
        Err(err) => {
            tracing::error!("failed to dispatch drained job to {addr}: {err}");
            account_queue::CachedResult {
                status: 502,
                content_type: "application/json".into(),
                body: serde_json::json!({ "error": format!("failed to reach assigned instance: {err}") })
                    .to_string(),
            }
        }
    };

    account_queue::store_result(&mut state.valkey, &job.idempotent_key, &cached).await
}

// Mirrors Aquifer's own POST /proxy and POST /jobs request body exactly,
// so a caller already speaking Aquifer's API can point at Canalis with no
// translation layer -- the whole point of the assignment-then-passthrough
// design (see DESIGN.md). Serialize too: this struct gets forwarded
// as-is to whichever instance Canalis assigns, not rebuilt field by field.
#[derive(Deserialize, Serialize)]
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
    // Env-var overridable, same convention Aquifer/ezthrottle-local
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

fn env_u64(key: &str, default: u64) -> u64 {
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
// mechanism. Pool exhaustion is different: durably enqueue the job (see
// pending.rs -- survives a Canalis crash, unlike the in-memory retry
// loop this replaced) and poll for a cached result, since a *new*
// registration genuinely can resolve this one. try_drain_one (triggered
// from register()) is what actually dispatches a queued job once a slot
// frees up; this function only waits for that result to show up.
async fn forward_with_config(
    mut state: AppState,
    job: JobRequest,
    target_path: &str,
    config: &RetryConfig,
) -> Response {
    let addr = match assign::assign(&mut state.valkey, &job.user_id).await {
        Ok(assign::AssignOutcome::Assigned(addr)) => addr,
        Ok(assign::AssignOutcome::PoolExhausted) => {
            if let Err(err) = account_queue::enqueue(&mut state.valkey, &job).await {
                tracing::error!("failed to durably enqueue job for {}: {err}", job.user_id);
                return json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    serde_json::json!({ "error": "internal error" }),
                );
            }

            let wait_deadline = Instant::now() + config.pool_wait_timeout;
            loop {
                match account_queue::get_result(&mut state.valkey, &job.idempotent_key).await {
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
                    // The job stays durably queued -- this only gives up
                    // on *this* connection, not the work itself. A caller
                    // with a webhook_url still gets notified once it's
                    // eventually drained; one without has no way to learn
                    // the outcome once this connection closes, an
                    // accepted, documented gap until the polling endpoint
                    // (the next slice) exists.
                    return json_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        serde_json::json!({
                            "error": "no Aquifer instance became available in time",
                            "note": "the job remains queued and will still be attempted"
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

// A drained job's cached outcome is the *complete* text a live viewer
// would have seen (the full SSE event history, or a plain body) -- it's
// replayed back all at once here rather than live, since the job already
// finished by the time anyone's asking. No information is lost, only the
// incremental delivery is, which is an honest, deliberate simplification
// for a result that's already history.
fn cached_result_response(cached: account_queue::CachedResult) -> Response {
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

    // A real mock server (not Canalis), answering POST /proxy with a real
    // SSE stream: one chunk immediately, a second after `delay`. This is
    // the automated version of the manual proof already done with a
    // Python mock + timestamped client -- same claim under test: if
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
        // Shared with assign.rs's own tests -- canalis:pool:free is a
        // global key, and this test seeds+pops it, so it needs the same
        // serialization those tests already use against each other. See
        // TEST_LOCK's own doc comment in assign.rs for why this caused a
        // real, confusing failure before it was shared across modules.
        let _guard = assign::TEST_LOCK.lock().await;

        let delay = Duration::from_millis(400);
        let mock_addr = start_delayed_sse_mock(delay).await;

        let mut valkey = test_valkey().await;
        let user_id = unique("user-sse-timing");
        let _: () = valkey.sadd("canalis:pool:free", &mock_addr).await.unwrap();

        let state = AppState {
            valkey,
            http: reqwest::Client::new(),
        };
        let job = JobRequest {
            user_id: user_id.clone(),
            idempotent_key: "k".into(),
            url: "https://example.com".into(),
            method: "GET".into(),
            headers: HashMap::new(),
            body: None,
            webhook_url: None,
        };

        let response = forward(state, job, "/proxy").await;
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

        assert_eq!(arrivals.len(), 2, "expected exactly two data chunks");
        let gap = arrivals[1] - arrivals[0];

        // Loose bound on purpose: "roughly delay, not roughly zero" is
        // the actual claim under test. A live relay should show a gap
        // close to `delay`; a secretly-buffered one would show both
        // chunks arriving together, gap near zero.
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

    // A separate, more generous config for the "eventually succeeds"
    // test specifically -- that one needs real headroom for a background
    // task to register, drain, and complete an actual HTTP round-trip
    // before the deadline, which found to be genuinely flaky at 500ms
    // under real test-parallelism load (not a logic bug: the failure was
    // always a timeout, never a wrong result). short_retry_config's tight
    // bound stays as-is for the *timeout* test, which specifically wants
    // to prove giving-up-quickly, not the other way around.
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
            // Unique, not a fixed literal -- canalis:result:<idempotent_key>
            // is keyed by this alone, not scoped by user_id, so a shared
            // literal here would collide across every test using job_for.
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
    async fn pool_exhaustion_retries_until_a_new_registration_succeeds() {
        let _guard = assign::TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let user_id = unique("user-pool-wait");

        // Deliberately nothing seeded in the pool yet -- forward() has to
        // durably enqueue and wait, with nothing to claim at first.
        let mock_addr = start_plain_ok_mock().await;
        let mock_addr_for_task = mock_addr.clone();
        let state_for_task = AppState {
            valkey: test_valkey().await,
            http: reqwest::Client::new(),
        };
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            // register_if_free alone only adds to the pool -- it does not
            // trigger a drain by itself. The real HTTP register() handler
            // calls try_drain_one right after a genuinely new add; mirror
            // that here rather than calling register_if_free in
            // isolation, which was the actual bug this test caught the
            // first time it was written (it passed a stale positive
            // before this fix, since nothing ever drove the drain).
            let mut state_for_task = state_for_task;
            let added = assign::register_if_free(&mut state_for_task.valkey, &mock_addr_for_task)
                .await
                .unwrap();
            assert!(added, "test setup bug: expected this to be a genuinely new pool add");
            try_drain_one(state_for_task).await.unwrap();
        });

        let job = job_for(&user_id);
        let idempotent_key = job.idempotent_key.clone();
        let state = AppState {
            valkey: valkey.clone(),
            http: reqwest::Client::new(),
        };
        // Generous deadline, not the tight one: this test needs real
        // headroom for a background task to register, drain, and
        // complete an actual HTTP round-trip before giving up -- see
        // generous_wait_retry_config's own doc comment for why 500ms
        // turned out to be genuinely flaky under real test-parallelism
        // load, not a logic bug.
        let response =
            forward_with_config(state, job, "/proxy", &generous_wait_retry_config()).await;
        let status = response.status();

        // Cleanup runs *before* the assert on purpose, unconditionally --
        // an assert! panic aborts the test function immediately, and any
        // cleanup written after it would silently never run on failure.
        // That's exactly what caused a real, confusing bug here: an
        // earlier failed run left an undrained job sitting in
        // canalis:pending_tenants forever, which a *later*, unrelated
        // test run's SPOP could then pick up instead of its own real
        // job, timing out for a reason that had nothing to do with its
        // own logic. Defensive cleanup of pending_tenants/account_queue
        // below covers the case where the drain never actually happened.
        let _: redis::RedisResult<()> = valkey.srem("canalis:pool:free", &mock_addr).await;
        let _: redis::RedisResult<()> = valkey.srem("canalis:assigned", &mock_addr).await;
        let _: redis::RedisResult<()> = valkey
            .del(format!("canalis:assignment:{user_id}"))
            .await;
        let _: redis::RedisResult<()> =
            valkey.del(format!("canalis:result:{idempotent_key}")).await;
        let _: redis::RedisResult<()> = valkey.srem("canalis:pending_tenants", &user_id).await;
        let _: redis::RedisResult<()> = valkey
            .del(format!("canalis:account_queue:{user_id}"))
            .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "expected the delayed registration to eventually satisfy the waiting request"
        );
    }

    #[tokio::test]
    async fn pool_exhaustion_gives_up_after_the_configured_wait() {
        let _guard = assign::TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let user_id = unique("user-pool-timeout");

        // Nothing seeded, nothing ever registered -- this should hit
        // pool_wait_timeout and give up, not hang. The job still gets
        // durably enqueued regardless (giving up only ends this
        // connection's wait, not the queued work itself) -- cleaned up
        // below since nothing will ever drain it in this test.
        let job = job_for(&user_id);
        let idempotent_key = job.idempotent_key.clone();
        let state = AppState {
            valkey: valkey.clone(),
            http: reqwest::Client::new(),
        };
        let start = Instant::now();
        let response = forward_with_config(state, job, "/proxy", &short_retry_config()).await;
        let elapsed = start.elapsed();

        let _: redis::RedisResult<()> =
            valkey.srem("canalis:pending_tenants", &user_id).await;
        let _: redis::RedisResult<()> = valkey
            .del(format!("canalis:account_queue:{user_id}"))
            .await;
        let _: redis::RedisResult<()> =
            valkey.del(format!("canalis:result:{idempotent_key}")).await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            elapsed < Duration::from_secs(2),
            "expected this to give up close to the configured 500ms wait, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn dead_assigned_instance_retries_then_gives_a_clean_error_not_a_hang() {
        let _guard = assign::TEST_LOCK.lock().await;
        let mut valkey = test_valkey().await;
        let user_id = unique("user-dead-instance");

        // 127.0.0.1:1 -- nothing listens on port 1, a real, immediate
        // connection-refused, not a slow timeout. Seeded directly into
        // the pool so assign() hands it back deterministically.
        let dead_addr = "127.0.0.1:1".to_string();
        let _: () = valkey.sadd("canalis:pool:free", &dead_addr).await.unwrap();

        let state = AppState {
            valkey: valkey.clone(),
            http: reqwest::Client::new(),
        };
        let start = Instant::now();
        let response =
            forward_with_config(state, job_for(&user_id), "/proxy", &short_retry_config()).await;
        let elapsed = start.elapsed();

        let status = response.status();

        // Cleanup before the asserts, same reasoning as the other tests
        // in this file -- a panic here shouldn't leave canalis:assigned
        // permanently holding a dead address.
        let _: redis::RedisResult<()> = valkey.srem("canalis:assigned", &dead_addr).await;
        let _: redis::RedisResult<()> = valkey
            .del(format!("canalis:assignment:{user_id}"))
            .await;

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(
            elapsed < Duration::from_secs(2),
            "expected the instance retries to give up quickly, took {elapsed:?}"
        );
    }
}
