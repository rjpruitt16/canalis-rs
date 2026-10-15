//! Who a request is for.
//!
//! CANALIS_AUTH picks how Canalis decides:
//!
//! - `header` (default): it believes the `user_id` / `X-Aqueduct-User-ID`
//!   the caller sends. Only safe when nothing untrusted can reach Canalis.
//! - `upstream-secret`: the same, but only from a gateway that sends
//!   `X-Canalis-Upstream-Secret` equal to CANALIS_UPSTREAM_SECRET. The
//!   gateway does the login.
//! - `jwt`: the caller sends the token its login provider issued
//!   (`Authorization: Bearer ...`; browser WebSockets, which can't set
//!   headers, may use an `access_token` query parameter). Canalis checks
//!   it against the provider's published keys (CANALIS_JWKS_URL) and takes
//!   the tenant from one of its claims, replacing whatever user id the
//!   request named. Clerk, WorkOS, Auth0, Cognito, Supabase and Firebase
//!   all issue these.
//!
//! The check is local: keys are fetched once and cached, refetched when a
//! token names a key id Canalis hasn't seen (the provider rotated keys)
//! and every CANALIS_JWKS_REFRESH_SECONDS. No call to the provider per
//! request.

use jsonwebtoken::{Algorithm, DecodingKey, Validation, jwk::JwkSet};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Algorithms a provider's token may be signed with. HMAC (shared-secret)
/// algorithms are refused: a published key set can't verify them, and
/// accepting them is the classic way to let anyone forge tokens.
const ALLOWED: [Algorithm; 8] = [
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::EdDSA,
];

/// Don't refetch the key set more often than this because of unknown key
/// ids, so a stream of tokens with made-up `kid`s can't hammer the provider.
const MIN_REFETCH: Duration = Duration::from_secs(30);

pub enum Auth {
    Header,
    UpstreamSecret(String),
    Jwt(Box<Jwt>),
}

/// Who a request is for, once checked.
#[derive(Debug, PartialEq)]
pub enum Identity {
    /// Use the user id the request names (header and upstream-secret modes).
    AsSent,
    /// Use this tenant, whatever the request names; and this pool, if the
    /// token says one.
    Verified { tenant: String, pool: Option<String> },
}

#[derive(Debug, PartialEq)]
pub enum Rejection {
    /// No credentials at all (401).
    Missing,
    /// Credentials that don't check out (401).
    Invalid(String),
    /// The provider's keys couldn't be fetched and none are cached (503).
    Unavailable(String),
}

impl Rejection {
    pub fn reason(&self) -> &'static str {
        match self {
            Rejection::Missing => "missing",
            Rejection::Invalid(_) => "invalid",
            Rejection::Unavailable(_) => "keys_unavailable",
        }
    }
}

pub struct Jwt {
    jwks_url: String,
    issuer: String,
    audience: Vec<String>,
    authorized_parties: Vec<String>,
    tenant_claim: String,
    pool_claim: Option<String>,
    refresh_every: Duration,
    pub forward_authorization: bool,
    http: reqwest::Client,
    keys: Mutex<Keys>,
    // One fetch at a time; others wait for it and use what it found.
    fetching: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct Keys {
    by_kid: HashMap<String, (DecodingKey, Option<Algorithm>)>,
    // Keys without a `kid`, tried in turn.
    unnamed: Vec<(DecodingKey, Option<Algorithm>)>,
    fetched_at: Option<Instant>,
}

fn env_list(name: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn env_bool(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

impl Auth {
    pub fn from_env() -> Result<Self, String> {
        match std::env::var("CANALIS_AUTH").unwrap_or_default().trim() {
            "" | "header" => Ok(Auth::Header),
            "upstream-secret" => {
                let secret = env_nonempty("CANALIS_UPSTREAM_SECRET")
                    .ok_or("CANALIS_AUTH=upstream-secret needs CANALIS_UPSTREAM_SECRET")?;
                Ok(Auth::UpstreamSecret(secret))
            }
            "jwt" => {
                let jwks_url = env_nonempty("CANALIS_JWKS_URL").ok_or("CANALIS_AUTH=jwt needs CANALIS_JWKS_URL")?;
                let issuer = env_nonempty("CANALIS_JWT_ISSUER").ok_or("CANALIS_AUTH=jwt needs CANALIS_JWT_ISSUER")?;
                Ok(Auth::Jwt(Box::new(Jwt::new(
                    jwks_url,
                    issuer,
                    env_list("CANALIS_JWT_AUDIENCE"),
                    env_list("CANALIS_JWT_AUTHORIZED_PARTIES"),
                    env_nonempty("CANALIS_JWT_TENANT_CLAIM").unwrap_or_else(|| "sub".into()),
                    env_nonempty("CANALIS_JWT_POOL_CLAIM"),
                    Duration::from_secs(crate::env_u64("CANALIS_JWKS_REFRESH_SECONDS", 600)),
                    env_bool("CANALIS_FORWARD_AUTHORIZATION"),
                ))))
            }
            other => Err(format!("CANALIS_AUTH={other:?}: expected header, upstream-secret, or jwt")),
        }
    }

    /// Whether the caller's Authorization header (or access_token) should
    /// be stripped before a WebSocket handshake goes to an instance.
    pub fn strips_credentials(&self) -> bool {
        matches!(self, Auth::Jwt(jwt) if !jwt.forward_authorization)
    }

    pub fn is_verified(&self) -> bool {
        matches!(self, Auth::Jwt(_))
    }

    /// Checks a request's credentials. `query_token` is an `access_token`
    /// query parameter, accepted only where the caller can't set headers
    /// (WebSocket handshakes).
    pub async fn check(&self, headers: &axum::http::HeaderMap, query_token: Option<&str>) -> Result<Identity, Rejection> {
        match self {
            Auth::Header => Ok(Identity::AsSent),
            Auth::UpstreamSecret(secret) => {
                let sent = headers.get("X-Canalis-Upstream-Secret").ok_or(Rejection::Missing)?;
                if constant_time_eq(sent.as_bytes(), secret.as_bytes()) {
                    Ok(Identity::AsSent)
                } else {
                    Err(Rejection::Invalid("wrong upstream secret".into()))
                }
            }
            Auth::Jwt(jwt) => {
                // An explicit access_token wins: on a WebSocket, an
                // Authorization header may belong to something in between.
                let token = query_token.or_else(|| bearer(headers)).ok_or(Rejection::Missing)?;
                jwt.verify(token).await
            }
        }
    }
}

fn bearer(headers: &axum::http::HeaderMap) -> Option<&str> {
    let value = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?.trim();
    let (scheme, token) = value.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim()).filter(|t| !t.is_empty())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl Jwt {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        jwks_url: String,
        issuer: String,
        audience: Vec<String>,
        authorized_parties: Vec<String>,
        tenant_claim: String,
        pool_claim: Option<String>,
        refresh_every: Duration,
        forward_authorization: bool,
    ) -> Self {
        Self {
            jwks_url,
            issuer,
            audience,
            authorized_parties,
            tenant_claim,
            pool_claim,
            refresh_every,
            forward_authorization,
            http: reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap_or_default(),
            keys: Mutex::default(),
            fetching: tokio::sync::Mutex::new(()),
        }
    }

    async fn verify(&self, token: &str) -> Result<Identity, Rejection> {
        let header = jsonwebtoken::decode_header(token).map_err(|e| Rejection::Invalid(format!("not a JWT: {e}")))?;
        if !ALLOWED.contains(&header.alg) {
            return Err(Rejection::Invalid(format!("{:?} tokens aren't accepted", header.alg)));
        }
        let candidates = self.keys_for(header.kid.as_deref()).await?;
        if candidates.is_empty() {
            return Err(Rejection::Invalid("signed with a key the provider doesn't publish".into()));
        }

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.issuer]);
        if self.audience.is_empty() {
            validation.validate_aud = false;
        } else {
            validation.set_audience(&self.audience);
        }
        validation.set_required_spec_claims(&["exp", "iss"]);
        validation.leeway = 30;

        let mut last_err = None;
        for (key, key_alg) in candidates {
            if key_alg.is_some_and(|alg| alg != header.alg) {
                continue;
            }
            match jsonwebtoken::decode::<serde_json::Value>(token, &key, &validation) {
                Ok(data) => return self.identity(&data.claims),
                Err(err) => last_err = Some(err),
            }
        }
        Err(Rejection::Invalid(last_err.map_or_else(|| "no key matches the token's algorithm".into(), |e| e.to_string())))
    }

    fn identity(&self, claims: &serde_json::Value) -> Result<Identity, Rejection> {
        if !self.authorized_parties.is_empty() {
            let azp = claims.get("azp").and_then(|v| v.as_str()).unwrap_or("");
            if !self.authorized_parties.iter().any(|p| p == azp) {
                return Err(Rejection::Invalid(format!("token was issued for {azp:?}, not an authorized party")));
            }
        }
        let tenant = claim_string(claims, &self.tenant_claim)
            .ok_or_else(|| Rejection::Invalid(format!("token has no {:?} claim", self.tenant_claim)))?;
        let pool = self.pool_claim.as_deref().and_then(|path| claim_string(claims, path));
        Ok(Identity::Verified { tenant, pool })
    }

    // The keys a token with this `kid` could have been signed with. The
    // set is refetched when it's due a refresh, or when the kid is new (the
    // provider rotated keys) -- but not within MIN_REFETCH of the last
    // fetch. If a refetch fails, the cached keys still answer.
    async fn keys_for(&self, kid: Option<&str>) -> Result<Vec<(DecodingKey, Option<Algorithm>)>, Rejection> {
        let (found, stale, recently) = self.cached(kid);
        if !found.is_empty() && !stale {
            return Ok(found);
        }
        if !recently {
            if let Err(err) = self.fetch().await {
                if self.keys.lock().unwrap_or_else(|e| e.into_inner()).fetched_at.is_none() {
                    return Err(Rejection::Unavailable(err));
                }
                tracing::warn!("{err}; using the keys already loaded");
                return Ok(found);
            }
        }
        Ok(self.cached(kid).0)
    }

    // (keys matching kid, whether the set is due a refresh, whether it was
    // fetched within MIN_REFETCH)
    fn cached(&self, kid: Option<&str>) -> (Vec<(DecodingKey, Option<Algorithm>)>, bool, bool) {
        let keys = self.keys.lock().unwrap_or_else(|e| e.into_inner());
        let found = match kid {
            Some(kid) => keys.by_kid.get(kid).cloned().into_iter().collect(),
            None => keys.by_kid.values().cloned().chain(keys.unnamed.iter().cloned()).collect(),
        };
        let age = keys.fetched_at.map(|at| at.elapsed());
        (found, age.is_none_or(|a| a >= self.refresh_every), age.is_some_and(|a| a < MIN_REFETCH))
    }

    async fn fetch(&self) -> Result<(), String> {
        let _one_at_a_time = self.fetching.lock().await;
        if self.keys.lock().unwrap_or_else(|e| e.into_inner()).fetched_at.is_some_and(|at| at.elapsed() < MIN_REFETCH) {
            return Ok(());
        }
        let set: JwkSet = self
            .http
            .get(&self.jwks_url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| format!("fetching {}: {e}", self.jwks_url))?
            .json()
            .await
            .map_err(|e| format!("reading {}: {e}", self.jwks_url))?;
        let mut keys = Keys { fetched_at: Some(Instant::now()), ..Default::default() };
        for jwk in &set.keys {
            let Ok(key) = DecodingKey::from_jwk(jwk) else {
                tracing::warn!("ignoring a key in {} that can't verify signatures", self.jwks_url);
                continue;
            };
            let alg = jwk.common.key_algorithm.and_then(|a| a.to_string().parse::<Algorithm>().ok());
            match &jwk.common.key_id {
                Some(kid) => {
                    keys.by_kid.insert(kid.clone(), (key, alg));
                }
                None => keys.unnamed.push((key, alg)),
            }
        }
        tracing::info!("loaded {} signing keys from {}", keys.by_kid.len() + keys.unnamed.len(), self.jwks_url);
        *self.keys.lock().unwrap_or_else(|e| e.into_inner()) = keys;
        Ok(())
    }
}

/// A claim by dotted path (`sub`, `org_id`, `o.id`), as a string. Numbers
/// are accepted and written out; anything else isn't an identity.
fn claim_string(claims: &serde_json::Value, path: &str) -> Option<String> {
    let mut value = claims;
    for part in path.split('.') {
        value = value.get(part)?;
    }
    match value {
        serde_json::Value::String(s) if !s.is_empty() => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::{Router, routing::get};
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
    use jsonwebtoken::{EncodingKey, Header};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub(crate) const ISSUER: &str = "https://auth.example.test";

    /// A signing key for tests: Ed25519, generated fresh, with its JWK.
    pub(crate) struct TestKey {
        pub kid: String,
        encoding: EncodingKey,
        alg: Algorithm,
        pub jwk: serde_json::Value,
    }

    impl TestKey {
        pub(crate) fn ed25519(kid: &str) -> Self {
            let signing = ed25519_dalek::SigningKey::generate(&mut rand_core::OsRng);
            // PKCS#8 v1 for an Ed25519 private key: a fixed prefix, then the seed.
            let mut der = hex_decode("302e020100300506032b657004220420");
            der.extend_from_slice(signing.as_bytes());
            Self {
                kid: kid.into(),
                encoding: EncodingKey::from_ed_der(&der),
                alg: Algorithm::EdDSA,
                jwk: serde_json::json!({
                    "kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "kid": kid,
                    "x": B64URL.encode(signing.verifying_key().as_bytes()),
                }),
            }
        }

        /// The fixed RSA test key in testdata/ (RS256, what Clerk and
        /// WorkOS sign with).
        pub(crate) fn rsa(kid: &str) -> Self {
            let pem = include_str!("../testdata/jwt-test-rsa.pem");
            let n = include_str!("../testdata/jwt-test-rsa.n").trim();
            Self {
                kid: kid.into(),
                encoding: EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap(),
                alg: Algorithm::RS256,
                jwk: serde_json::json!({ "kty": "RSA", "alg": "RS256", "use": "sig", "kid": kid, "n": n, "e": "AQAB" }),
            }
        }

        pub(crate) fn sign(&self, claims: serde_json::Value) -> String {
            let mut header = Header::new(self.alg);
            header.kid = Some(self.kid.clone());
            jsonwebtoken::encode(&header, &claims, &self.encoding).unwrap()
        }
    }

    fn hex_decode(hex: &str) -> Vec<u8> {
        (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect()
    }

    pub(crate) fn now() -> i64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
    }

    pub(crate) fn claims(sub: &str) -> serde_json::Value {
        serde_json::json!({ "iss": ISSUER, "sub": sub, "exp": now() + 300 })
    }

    /// A JWKS endpoint serving whatever keys are in `keys`, counting
    /// fetches.
    pub(crate) async fn serve_jwks(keys: Arc<Mutex<Vec<serde_json::Value>>>) -> (String, Arc<AtomicUsize>) {
        let fetches = Arc::new(AtomicUsize::new(0));
        let counter = fetches.clone();
        let app = Router::new().route(
            "/jwks.json",
            get(move || {
                let keys = keys.clone();
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    axum::Json(serde_json::json!({ "keys": *keys.lock().unwrap() }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/jwks.json"), fetches)
    }

    pub(crate) fn verifier(url: &str) -> Jwt {
        Jwt::new(url.into(), ISSUER.into(), vec![], vec![], "sub".into(), None, Duration::from_secs(600), false)
    }

    async fn setup(keys: &[&TestKey]) -> (Jwt, Arc<Mutex<Vec<serde_json::Value>>>, Arc<AtomicUsize>) {
        let published = Arc::new(Mutex::new(keys.iter().map(|k| k.jwk.clone()).collect()));
        let (url, fetches) = serve_jwks(published.clone()).await;
        (verifier(&url), published, fetches)
    }

    fn verified(tenant: &str) -> Result<Identity, Rejection> {
        Ok(Identity::Verified { tenant: tenant.into(), pool: None })
    }

    #[tokio::test]
    async fn a_valid_token_names_the_tenant() {
        let key = TestKey::ed25519("k1");
        let (jwt, _, _) = setup(&[&key]).await;
        assert_eq!(jwt.verify(&key.sign(claims("user_123"))).await, verified("user_123"));
    }

    #[tokio::test]
    async fn rs256_tokens_verify_too() {
        let key = TestKey::rsa("rsa1");
        let (jwt, _, _) = setup(&[&key]).await;
        assert_eq!(jwt.verify(&key.sign(claims("user_rsa"))).await, verified("user_rsa"));
    }

    #[tokio::test]
    async fn bad_tokens_are_refused() {
        let key = TestKey::ed25519("k1");
        let stranger = TestKey::ed25519("k1"); // same kid, different key: a forgery
        let (jwt, _, _) = setup(&[&key]).await;
        let mut expired = claims("u");
        expired["exp"] = (now() - 3600).into();
        let mut wrong_issuer = claims("u");
        wrong_issuer["iss"] = "https://evil.example".into();
        let mut no_sub = claims("u");
        no_sub.as_object_mut().unwrap().remove("sub");

        for (what, token) in [
            ("expired", key.sign(expired)),
            ("wrong issuer", key.sign(wrong_issuer)),
            ("forged", stranger.sign(claims("u"))),
            ("no tenant claim", key.sign(no_sub)),
            ("garbage", "not.a.jwt".to_string()),
        ] {
            assert!(matches!(jwt.verify(&token).await, Err(Rejection::Invalid(_))), "{what} should be refused");
        }
    }

    #[tokio::test]
    async fn hmac_tokens_are_refused_whatever_they_say() {
        let key = TestKey::ed25519("k1");
        let (jwt, _, _) = setup(&[&key]).await;
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("k1".into());
        let token = jsonwebtoken::encode(&header, &claims("u"), &EncodingKey::from_secret(b"guess")).unwrap();
        assert!(matches!(jwt.verify(&token).await, Err(Rejection::Invalid(_))));
    }

    #[tokio::test]
    async fn a_rotated_key_is_picked_up_without_a_restart() {
        let old = TestKey::ed25519("old");
        let new = TestKey::ed25519("new");
        let (jwt, published, fetches) = setup(&[&old]).await;
        assert_eq!(jwt.verify(&old.sign(claims("a"))).await, verified("a"));

        published.lock().unwrap().push(new.jwk.clone());
        // Pretend the last fetch was long enough ago to look again.
        jwt.keys.lock().unwrap().fetched_at = Some(Instant::now() - MIN_REFETCH);
        assert_eq!(jwt.verify(&new.sign(claims("b"))).await, verified("b"));
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn made_up_key_ids_dont_hammer_the_provider() {
        let key = TestKey::ed25519("k1");
        let (jwt, _, fetches) = setup(&[&key]).await;
        for i in 0..20 {
            let _ = jwt.verify(&TestKey::ed25519(&format!("bogus-{i}")).sign(claims("u"))).await;
        }
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn audience_authorized_parties_and_nested_claims() {
        let key = TestKey::ed25519("k1");
        let (url, _) = serve_jwks(Arc::new(Mutex::new(vec![key.jwk.clone()]))).await;
        let jwt = Jwt::new(
            url,
            ISSUER.into(),
            vec!["api.example".into()],
            vec!["https://app.example".into()],
            "o.id".into(),
            Some("pool".into()),
            Duration::from_secs(600),
            false,
        );
        let mut good = claims("user_1");
        good["aud"] = "api.example".into();
        good["azp"] = "https://app.example".into();
        good["o"] = serde_json::json!({ "id": "org_9" });
        good["pool"] = "gpu".into();
        assert_eq!(
            jwt.verify(&key.sign(good.clone())).await,
            Ok(Identity::Verified { tenant: "org_9".into(), pool: Some("gpu".into()) })
        );

        let mut wrong_aud = good.clone();
        wrong_aud["aud"] = "other".into();
        let mut wrong_azp = good.clone();
        wrong_azp["azp"] = "https://evil.example".into();
        assert!(matches!(jwt.verify(&key.sign(wrong_aud)).await, Err(Rejection::Invalid(_))));
        assert!(matches!(jwt.verify(&key.sign(wrong_azp)).await, Err(Rejection::Invalid(_))));
    }

    #[tokio::test]
    async fn unreachable_keys_are_a_503_not_a_401() {
        let jwt = verifier("http://127.0.0.1:1/jwks.json");
        let token = TestKey::ed25519("k1").sign(claims("u"));
        assert!(matches!(jwt.verify(&token).await, Err(Rejection::Unavailable(_))));
    }

    #[tokio::test]
    async fn upstream_secret_and_bearer_parsing() {
        let auth = Auth::UpstreamSecret("s3cret".into());
        let mut headers = axum::http::HeaderMap::new();
        assert_eq!(auth.check(&headers, None).await, Err(Rejection::Missing));
        headers.insert("X-Canalis-Upstream-Secret", "nope".parse().unwrap());
        assert!(matches!(auth.check(&headers, None).await, Err(Rejection::Invalid(_))));
        headers.insert("X-Canalis-Upstream-Secret", "s3cret".parse().unwrap());
        assert_eq!(auth.check(&headers, None).await, Ok(Identity::AsSent));

        let mut h = axum::http::HeaderMap::new();
        h.insert("Authorization", "bearer  abc ".parse().unwrap());
        assert_eq!(bearer(&h), Some("abc"));
        h.insert("Authorization", "Basic abc".parse().unwrap());
        assert_eq!(bearer(&h), None);
    }
}
