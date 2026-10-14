//! L8 v0.2 request schemas, checked at the gate.
//!
//! An upstream API can publish, in its `/.well-known/l8`, the JSON Schema
//! each of its routes accepts (`request_schemas`, keyed `"METHOD /path"`).
//! With CANALIS_L8_SCHEMA_VALIDATION=true, Canalis checks a job's body
//! against the schema for the job's own method and URL before doing
//! anything else with it, and answers 422 on a mismatch -- so a request
//! the upstream would reject never takes a spot in Canalis's line or a
//! user slot on an instance. Aquifer runs the same check on its side
//! (AQUIFER_L8_SCHEMA_VALIDATION, l8_schema.go); this mirrors its rules:
//!
//! - schemas are cached per upstream for 10 minutes, a failed fetch for 1;
//! - `request_schemas` without a `schema_hash` are ignored;
//! - external `$ref`s are refused, never fetched: a schema comes from a
//!   remote party and must not become a way to make Canalis fetch URLs;
//! - anything that can't be fetched or compiled isn't checked (L8 stays
//!   opt-in and degrades open);
//! - an `X-Aqueduct-Schema-Hash` response header that differs from the
//!   cached hash drops the cache for that upstream, at most every 10s.
//!
//! Like Aquifer, this fetches `/.well-known/l8` from whatever upstream a job
//! targets, which is why it's off by default and why Canalis belongs behind
//! a gateway that only passes trusted jobs.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

const SCHEMA_TTL: Duration = Duration::from_secs(600);
const NEGATIVE_TTL: Duration = Duration::from_secs(60);
const MIN_REFETCH_AGE: Duration = Duration::from_secs(10);
const META_MAX_BYTES: usize = 1 << 20;
const FETCH_TIMEOUT: Duration = Duration::from_secs(3);

/// A job body that doesn't match its upstream's published schema.
#[derive(Debug, Clone, PartialEq)]
pub struct Mismatch {
    pub route: String,
    pub schema_hash: String,
    pub detail: String,
}

#[derive(Deserialize, Default)]
struct Meta {
    #[serde(default)]
    public_key: String,
    #[serde(default)]
    challenge_endpoint: String,
    #[serde(default)]
    schema_hash: String,
    #[serde(default)]
    request_schemas: HashMap<String, serde_json::Value>,
}

struct Loaded {
    hash: String,
    routes: HashMap<String, jsonschema::Validator>,
    fetched_at: Instant,
    expires_at: Instant,
}

/// Refuses every external `$ref`.
struct NoRetrieval;

impl jsonschema::Retrieve for NoRetrieval {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(format!("external $ref {uri} is not allowed in L8 request_schemas").into())
    }
}

pub struct SchemaGate {
    enabled: bool,
    http: reqwest::Client,
    // One cell per upstream: concurrent jobs for the same upstream wait on
    // one fetch instead of all hitting /.well-known/l8 at once.
    entries: Mutex<HashMap<String, Arc<OnceCell<Arc<Loaded>>>>>,
}

impl SchemaGate {
    pub fn new(enabled: bool) -> Self {
        let http = reqwest::Client::builder()
            .timeout(FETCH_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_default();
        Self { enabled, http, entries: Mutex::default() }
    }

    pub fn from_env() -> Self {
        let enabled = std::env::var("CANALIS_L8_SCHEMA_VALIDATION")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        Self::new(enabled)
    }

    /// Checks `body` against the schema the upstream at `url` publishes for
    /// `method` + the URL's path. `Ok` when checking is off, the upstream
    /// publishes nothing for that route, or its metadata can't be had.
    pub async fn check(&self, url: &str, method: &str, body: &str) -> Result<(), Mismatch> {
        if !self.enabled {
            return Ok(());
        }
        let Some((origin, route)) = origin_and_route(url, method) else {
            return Ok(());
        };
        let loaded = self.get(&origin).await;
        let Some(validator) = loaded.routes.get(&route) else {
            return Ok(());
        };

        let mismatch = |detail: String| Mismatch { route: route.clone(), schema_hash: loaded.hash.clone(), detail };
        let instance: serde_json::Value = match serde_json::from_str(body) {
            Ok(v) => v,
            Err(_) => return Err(mismatch("body is not valid JSON".into())),
        };
        let errors: Vec<String> = validator
            .iter_errors(&instance)
            .map(|e| {
                let at = e.instance_path().to_string();
                if at.is_empty() { e.to_string() } else { format!("{at}: {e}") }
            })
            .collect();
        if errors.is_empty() { Ok(()) } else { Err(mismatch(errors.join("; "))) }
    }

    /// Reacts to an `X-Aqueduct-Schema-Hash` the upstream sent: a hash that
    /// differs from the cached one means the upstream changed its contract,
    /// so the next check refetches. Ignored for a cache younger than 10s,
    /// so an upstream whose header and metadata disagree can't force a
    /// refetch per response.
    pub fn observe_hash(&self, url: &str, hash: &str) {
        if !self.enabled || hash.is_empty() {
            return;
        }
        let Some((origin, _)) = origin_and_route(url, "GET") else { return };
        let Ok(mut entries) = self.entries.lock() else { return };
        let stale = entries
            .get(&origin)
            .and_then(|cell| cell.get())
            .is_some_and(|l| l.hash != hash && l.fetched_at.elapsed() >= MIN_REFETCH_AGE);
        if stale {
            tracing::info!("{origin} advertised schema hash {hash}; refetching its request schemas");
            entries.remove(&origin);
        }
    }

    async fn get(&self, origin: &str) -> Arc<Loaded> {
        let cell = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            match entries.get(origin) {
                Some(cell) if cell.get().is_none_or(|l| Instant::now() < l.expires_at) => cell.clone(),
                _ => {
                    let cell = Arc::new(OnceCell::new());
                    entries.insert(origin.to_string(), cell.clone());
                    cell
                }
            }
        };
        cell.get_or_init(|| async { Arc::new(self.load(origin).await) }).await.clone()
    }

    async fn load(&self, origin: &str) -> Loaded {
        let now = Instant::now();
        let empty = Loaded { hash: String::new(), routes: HashMap::new(), fetched_at: now, expires_at: now + NEGATIVE_TTL };
        let Some(meta) = self.fetch(origin).await else { return empty };
        if meta.request_schemas.is_empty() {
            return empty;
        }
        if meta.schema_hash.is_empty() {
            tracing::warn!("{origin} publishes request_schemas without schema_hash; ignoring them");
            return empty;
        }
        match compile(&meta.request_schemas) {
            Ok(routes) => Loaded { hash: meta.schema_hash, routes, fetched_at: now, expires_at: now + SCHEMA_TTL },
            Err(err) => {
                tracing::warn!("{origin} request_schemas did not compile: {err}");
                empty
            }
        }
    }

    async fn fetch(&self, origin: &str) -> Option<Meta> {
        let mut resp = self.http.get(format!("{origin}/.well-known/l8")).send().await.ok()?;
        if resp.status() != 200 {
            return None;
        }
        let mut body = Vec::new();
        while let Some(chunk) = resp.chunk().await.ok()? {
            body.extend_from_slice(&chunk);
            if body.len() > META_MAX_BYTES {
                tracing::warn!("{origin}/.well-known/l8 is larger than {META_MAX_BYTES} bytes; ignoring it");
                return None;
            }
        }
        let meta: Meta = serde_json::from_slice(&body).ok()?;
        (!meta.public_key.is_empty() && !meta.challenge_endpoint.is_empty()).then_some(meta)
    }
}

fn compile(raw: &HashMap<String, serde_json::Value>) -> Result<HashMap<String, jsonschema::Validator>, String> {
    let mut routes = HashMap::with_capacity(raw.len());
    for (route, schema) in raw {
        let key = normalize_route(route).ok_or_else(|| format!("route {route:?} must look like \"POST /path\""))?;
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .with_retriever(NoRetrieval)
            .build(schema)
            .map_err(|e| format!("{route}: {e}"))?;
        routes.insert(key, validator);
    }
    Ok(routes)
}

// "post  /v1/x " -> "POST /v1/x". Method case-insensitive, path exact.
fn normalize_route(route: &str) -> Option<String> {
    let (method, path) = route.trim().split_once(' ')?;
    let path = path.trim();
    (!method.is_empty() && path.starts_with('/')).then(|| format!("{} {path}", method.to_ascii_uppercase()))
}

// ("https://api.example.com", "POST /v1/x") for a job's URL and method.
fn origin_and_route(url: &str, method: &str) -> Option<(String, String)> {
    let parsed = reqwest::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    let origin = match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    };
    let path = if parsed.path().is_empty() { "/" } else { parsed.path() };
    Some((origin, format!("{} {path}", method.to_ascii_uppercase())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn start_upstream(meta: serde_json::Value, fetches: Arc<AtomicUsize>) -> String {
        let app = Router::new().route(
            "/.well-known/l8",
            get(move || {
                let meta = meta.clone();
                let fetches = fetches.clone();
                async move {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    Json(meta)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn meta(schemas: serde_json::Value, hash: &str) -> serde_json::Value {
        serde_json::json!({
            "protocol_version": "0.2",
            "public_key": "AAAA",
            "challenge_endpoint": "/l8/challenge",
            "capabilities": ["signed_payloads", "request_schemas"],
            "schema_hash": hash,
            "request_schemas": schemas,
        })
    }

    fn chat_schema() -> serde_json::Value {
        serde_json::json!({
            "POST /v1/chat": {
                "type": "object",
                "required": ["model", "messages"],
                "properties": { "model": { "type": "string" } }
            }
        })
    }

    #[tokio::test]
    async fn a_body_that_breaks_the_schema_is_rejected_and_says_why() {
        let base = start_upstream(meta(chat_schema(), "sha256:v1"), Arc::default()).await;
        let gate = SchemaGate::new(true);

        let err = gate.check(&format!("{base}/v1/chat?x=1"), "post", r#"{"model": 7}"#).await.unwrap_err();
        assert_eq!(err.route, "POST /v1/chat", "method case-insensitive, query string ignored");
        assert_eq!(err.schema_hash, "sha256:v1");
        assert!(err.detail.contains("messages"), "names what's missing: {}", err.detail);
        assert!(err.detail.contains("/model"), "and where it's wrong: {}", err.detail);
    }

    #[tokio::test]
    async fn a_matching_body_and_unlisted_routes_pass() {
        let base = start_upstream(meta(chat_schema(), "sha256:v1"), Arc::default()).await;
        let gate = SchemaGate::new(true);

        assert!(gate.check(&format!("{base}/v1/chat"), "POST", r#"{"model":"m","messages":[]}"#).await.is_ok());
        assert!(gate.check(&format!("{base}/v1/other"), "POST", "not json").await.is_ok(), "no schema, no check");
        assert!(gate.check(&format!("{base}/v1/chat"), "GET", "").await.is_ok(), "different method, different route");
    }

    #[tokio::test]
    async fn invalid_json_is_a_mismatch() {
        let base = start_upstream(meta(chat_schema(), "sha256:v1"), Arc::default()).await;
        let err = SchemaGate::new(true).check(&format!("{base}/v1/chat"), "POST", "{nope").await.unwrap_err();
        assert_eq!(err.detail, "body is not valid JSON");
    }

    #[tokio::test]
    async fn off_by_default_and_never_fetches() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let base = start_upstream(meta(chat_schema(), "sha256:v1"), fetches.clone()).await;

        assert!(SchemaGate::new(false).check(&format!("{base}/v1/chat"), "POST", "{}").await.is_ok());
        assert_eq!(fetches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn schemas_are_fetched_once_and_cached() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let base = start_upstream(meta(chat_schema(), "sha256:v1"), fetches.clone()).await;
        let gate = Arc::new(SchemaGate::new(true));

        let mut checks = tokio::task::JoinSet::new();
        for _ in 0..10 {
            let (gate, url) = (gate.clone(), format!("{base}/v1/chat"));
            checks.spawn(async move { gate.check(&url, "POST", "{}").await });
        }
        while let Some(result) = checks.join_next().await {
            assert!(result.unwrap().is_err());
        }
        assert_eq!(fetches.load(Ordering::SeqCst), 1, "ten concurrent checks, one fetch");
    }

    #[tokio::test]
    async fn an_external_ref_is_refused_not_fetched() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let decoy = start_upstream(serde_json::json!({}), fetches.clone()).await;
        let schemas = serde_json::json!({ "POST /v1/chat": { "$ref": format!("{decoy}/.well-known/l8") } });
        let base = start_upstream(meta(schemas, "sha256:v1"), Arc::default()).await;

        let result = SchemaGate::new(true).check(&format!("{base}/v1/chat"), "POST", "{}").await;
        assert!(result.is_ok(), "a schema that can't compile isn't checked");
        assert_eq!(fetches.load(Ordering::SeqCst), 0, "and the $ref target was never contacted");
    }

    #[tokio::test]
    async fn schemas_without_a_hash_are_ignored() {
        let base = start_upstream(meta(chat_schema(), ""), Arc::default()).await;
        assert!(SchemaGate::new(true).check(&format!("{base}/v1/chat"), "POST", "{}").await.is_ok());
    }

    #[tokio::test]
    async fn a_new_schema_hash_drops_the_cache() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let base = start_upstream(meta(chat_schema(), "sha256:v1"), fetches.clone()).await;
        let gate = SchemaGate::new(true);
        let url = format!("{base}/v1/chat");
        let _ = gate.check(&url, "POST", "{}").await;

        gate.observe_hash(&url, "sha256:v2");
        let _ = gate.check(&url, "POST", "{}").await;
        assert_eq!(fetches.load(Ordering::SeqCst), 1, "too soon after the fetch to refetch");

        if let Some(cell) = gate.entries.lock().unwrap().get_mut(&base) {
            // Age the cached entry past the 10s floor.
            let loaded = cell.get().unwrap();
            let aged = Loaded {
                hash: loaded.hash.clone(),
                routes: HashMap::new(),
                fetched_at: Instant::now() - MIN_REFETCH_AGE,
                expires_at: loaded.expires_at,
            };
            *cell = Arc::new(OnceCell::new_with(Some(Arc::new(aged))));
        }
        gate.observe_hash(&url, "sha256:v1");
        assert_eq!(fetches.load(Ordering::SeqCst), 1, "same hash: keep the cache");
        gate.observe_hash(&url, "sha256:v2");
        let _ = gate.check(&url, "POST", "{}").await;
        assert_eq!(fetches.load(Ordering::SeqCst), 2, "a different hash refetches");
    }
}
