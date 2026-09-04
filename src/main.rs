mod assign;

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
    // docs for why that idempotency matters).
    if let Err(err) = assign::register_if_free(&mut state.valkey, &address).await {
        tracing::error!("failed to add {address} to the free pool: {err}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    tracing::info!("registered instance {address}");
    StatusCode::OK
}

// Mirrors Aquifer's own POST /proxy and POST /jobs request body exactly,
// so a caller already speaking Aquifer's API can point at Canalis with no
// translation layer -- the whole point of the assignment-then-passthrough
// design (see DESIGN.md). Serialize too: this struct gets forwarded
// as-is to whichever instance Canalis assigns, not rebuilt field by field.
#[derive(Deserialize, Serialize)]
struct JobRequest {
    user_id: String,
    idempotent_key: String,
    url: String,
    method: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    webhook_url: Option<String>,
}

async fn proxy(State(state): State<AppState>, Json(job): Json<JobRequest>) -> Response {
    forward(state, job, "/proxy").await
}

async fn jobs(State(state): State<AppState>, Json(job): Json<JobRequest>) -> Response {
    forward(state, job, "/jobs").await
}

// Resolves the assignment, then forwards the same job body to the
// assigned instance's own target_path ("/proxy" or "/jobs" -- this is
// where those two finally diverge, now that there's real forwarding to
// diverge in). Buffered relay only: a plain response gets its status,
// content-type, and body relayed verbatim. An SSE response (Aquifer's
// fallback-to-queue path, and always the case for /jobs) is detected but
// not yet relayed -- that's the deliberately separate next slice, so it
// gets the same TODO-stub treatment pool exhaustion already does, not a
// silent wrong behavior like buffering a live stream as if it were a
// normal body.
async fn forward(mut state: AppState, job: JobRequest, target_path: &str) -> Response {
    let addr = match assign::assign(&mut state.valkey, &job.user_id).await {
        Ok(assign::AssignOutcome::Assigned(addr)) => addr,
        Ok(assign::AssignOutcome::PoolExhausted) => {
            return json_response(
                StatusCode::NOT_IMPLEMENTED,
                serde_json::json!({ "todo": "pool exhaustion / waiting room not yet implemented" }),
            );
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
    let upstream = match state.http.post(&target_url).json(&job).send().await {
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!("failed to reach assigned instance {addr}: {err}");
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

    if content_type.starts_with("text/event-stream") {
        return json_response(
            StatusCode::NOT_IMPLEMENTED,
            serde_json::json!({ "todo": "SSE stream relay not yet implemented" }),
        );
    }

    let status = StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);

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
