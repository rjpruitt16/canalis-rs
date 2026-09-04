mod assign;

use axum::{
    extract::{ConnectInfo, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use redis::AsyncCommands;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;

// ConnectionManager derives Clone (it wraps an Arc internally), so it's
// safe to hand a copy to every request handler without an Arc<Mutex<>>
// wrapper of our own -- unlike URLWorker's plain struct fields in Aquifer,
// which do need an external mutex because they aren't safe to share on
// their own.
pub type AppState = redis::aio::ConnectionManager;

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
    let valkey: AppState = redis::aio::ConnectionManager::new(client)
        .await
        .expect("failed to connect to Valkey");

    let app = Router::new()
        .route("/health", get(health))
        .route("/valkey-check", get(valkey_check))
        .route("/register", post(register))
        .route("/proxy", post(proxy))
        .route("/jobs", post(jobs))
        .with_state(valkey);

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
async fn valkey_check(State(mut valkey): State<AppState>) -> String {
    let _: () = valkey
        .set("canalis:check", "hello from canalis-rs")
        .await
        .expect("SET failed");

    let value: String = valkey.get("canalis:check").await.expect("GET failed");

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
    State(mut valkey): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Json(payload): Json<RegisterRequest>,
) -> StatusCode {
    let address = format!("{}:{}", remote.ip(), payload.port);
    let key = format!("canalis:instance:{address}");

    let result: redis::RedisResult<()> = valkey
        .set_ex(&key, &payload.reported_at, REGISTRATION_TTL_SECONDS)
        .await;

    if let Err(err) = result {
        tracing::error!("failed to register instance {address}: {err}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    // Every heartbeat, not just the first -- but a no-op if addr is
    // already claimed or already in the pool (see register_if_free's own
    // docs for why that idempotency matters).
    if let Err(err) = assign::register_if_free(&mut valkey, &address).await {
        tracing::error!("failed to add {address} to the free pool: {err}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    tracing::info!("registered instance {address}");
    StatusCode::OK
}

// Mirrors Aquifer's own POST /proxy and POST /jobs request body exactly,
// so a caller already speaking Aquifer's API can point at Canalis with no
// translation layer -- the whole point of the assignment-then-passthrough
// design (see DESIGN.md).
#[derive(Deserialize)]
struct JobRequest {
    user_id: String,
    #[allow(dead_code)] // not read yet -- forwarding (the next slice) will need this
    idempotent_key: String,
    #[allow(dead_code)]
    url: String,
    #[allow(dead_code)]
    method: String,
    #[allow(dead_code)]
    #[serde(default)]
    headers: HashMap<String, String>,
    #[allow(dead_code)]
    #[serde(default)]
    body: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    webhook_url: Option<String>,
}

async fn proxy(State(valkey): State<AppState>, Json(job): Json<JobRequest>) -> impl IntoResponse {
    assignment_only_response(valkey, job).await
}

async fn jobs(State(valkey): State<AppState>, Json(job): Json<JobRequest>) -> impl IntoResponse {
    assignment_only_response(valkey, job).await
}

// /proxy and /jobs are identical right now, on purpose: neither actually
// forwards anything to the assigned instance yet (that's the next slice,
// which is also where /proxy's direct-attempt-first behavior and /jobs'
// straight-to-queue behavior will actually start to diverge). This only
// proves assignment resolves correctly through the real public API shape.
async fn assignment_only_response(mut valkey: AppState, job: JobRequest) -> impl IntoResponse {
    match assign::assign(&mut valkey, &job.user_id).await {
        Ok(assign::AssignOutcome::Assigned(addr)) => (
            StatusCode::OK,
            Json(serde_json::json!({ "assigned_instance": addr })),
        ),
        Ok(assign::AssignOutcome::PoolExhausted) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({
                "todo": "pool exhaustion / waiting room not yet implemented"
            })),
        ),
        Err(err) => {
            tracing::error!("assign failed: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal error" })),
            )
        }
    }
}
