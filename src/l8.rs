//! L8, receiver side: who is allowed to register an instance.
//!
//! Aquifer and Spillway already sign their registration pings with L8 once
//! the receiver supports it -- they fetch `GET /.well-known/l8`, prove key
//! ownership through `POST /l8/challenge`, then sign every delivery
//! (`X-L8-*` headers over `{delivery_id}.{timestamp}.{base64(sha256(body))}`).
//! A receiver that doesn't publish the endpoint just gets unsigned pings,
//! which is all Canalis got before this. Wire format matches Aquifer's
//! l8.go exactly; see https://rjpruitt16.github.io/l8-protocol/.
//!
//! The handshake proves a node owns its key, not that it's allowed in. That
//! part is the allowlist: CANALIS_L8_TRUSTED_NODE_KEYS, the base64 public
//! keys of the nodes you run. With it set, only those keys can complete the
//! handshake or sign a registration. With CANALIS_REQUIRE_SIGNED_REGISTRATION
//! =true, an unsigned ping is refused too. Both are off by default, so
//! existing deployments keep working; a ping that *is* signed but doesn't
//! verify is refused either way.
//!
//! Canalis's own key comes from L8_PRIVATE_KEY (base64, Go's 64-byte
//! ed25519 private key or a 32-byte seed), else CANALIS_L8_KEY_PATH
//! (default `.l8-key`, generated on first start, same raw format Aquifer
//! writes).

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PROTOCOL_VERSION: &str = "0.2";
/// Same window Aquifer uses: a challenge or delivery more than this far
/// from now (either direction) is refused, and nonces / delivery ids are
/// remembered this long for replay protection.
const WINDOW: Duration = Duration::from_secs(300);

#[derive(Deserialize)]
pub struct ChallengeRequest {
    pub challenge_id: String,
    pub nonce: String,
    pub timestamp: i64,
    pub sender_public_key: String,
    pub signature: String,
}

#[derive(Serialize)]
pub struct ChallengeResponse {
    challenge_id: String,
    nonce: String,
    receiver_signature: String,
    receiver_public_key: String,
}

/// What a registration ping's signature says about it.
#[derive(Debug, PartialEq)]
pub enum Verdict {
    /// No `X-L8-Signature`: the sender hasn't established trust (or
    /// doesn't speak L8).
    Unsigned,
    /// Signed by a key Canalis knows, and the body is what was signed.
    Valid { key: String },
    /// Signed, but not by a trusted key, or tampered with, or replayed.
    Invalid(String),
}

pub struct L8 {
    key: SigningKey,
    public_b64: String,
    /// Allowlisted node keys, if any. None means "any key that completes
    /// the handshake".
    trusted: Option<HashSet<[u8; 32]>>,
    /// Keys that completed the handshake, by key id.
    learned: Mutex<HashMap<String, VerifyingKey>>,
    /// Where learned keys are kept (one base64 public key per line), so a
    /// node that already shook hands with this Canalis is still recognized
    /// after a restart: the node caches its trust and keeps signing
    /// without shaking hands again.
    learned_file: Option<std::path::PathBuf>,
    // Key ids already warned about (signed by a key nobody learned), so
    // that's logged once per key rather than on every ping.
    warned: Mutex<HashSet<String>>,
    seen: Mutex<HashMap<String, Instant>>,
    pub require_signed: bool,
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or_default()
}

pub fn key_id(public: &[u8; 32]) -> String {
    B64.encode(&public[..8])
}

impl L8 {
    pub fn new(key: SigningKey, trusted: Option<HashSet<[u8; 32]>>, require_signed: bool) -> Self {
        let public_b64 = B64.encode(key.verifying_key().as_bytes());
        Self { key, public_b64, trusted, learned: Mutex::default(), learned_file: None, warned: Mutex::default(), seen: Mutex::default(), require_signed }
    }

    /// Keeps learned keys in `path`, loading any already there.
    pub fn with_learned_file(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        let path = path.into();
        if let Ok(text) = std::fs::read_to_string(&path) {
            let mut learned = self.learned.lock().unwrap_or_else(|e| e.into_inner());
            for key in text.lines().filter_map(|l| decode_public_key(l.trim())) {
                learned.insert(key_id(key.as_bytes()), key);
            }
            tracing::info!("[L8] remembered {} node keys from {}", learned.len(), path.display());
        }
        self.learned_file = Some(path);
        self
    }

    fn remember(&self, sender: VerifyingKey) {
        let id = key_id(sender.as_bytes());
        let new = self.learned.lock().unwrap_or_else(|e| e.into_inner()).insert(id, sender).is_none();
        let Some(path) = self.learned_file.as_ref().filter(|_| new) else { return };
        use std::io::Write;
        let saved = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .and_then(|mut f| writeln!(f, "{}", B64.encode(sender.as_bytes())));
        if let Err(err) = saved {
            tracing::warn!("[L8] couldn't save a learned node key to {}: {err}", path.display());
        }
    }

    pub fn from_env() -> Result<Self, String> {
        let key = load_or_generate_key(
            std::env::var("L8_PRIVATE_KEY").ok().filter(|v| !v.trim().is_empty()),
            Path::new(&std::env::var("CANALIS_L8_KEY_PATH").unwrap_or_else(|_| ".l8-key".into())),
        )?;
        let trusted = match std::env::var("CANALIS_L8_TRUSTED_NODE_KEYS") {
            Ok(list) if !list.trim().is_empty() => Some(parse_keys(&list)?),
            _ => None,
        };
        let require_signed = std::env::var("CANALIS_REQUIRE_SIGNED_REGISTRATION")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        let key_path = std::env::var("CANALIS_L8_KEY_PATH").unwrap_or_else(|_| ".l8-key".into());
        let l8 = Self::new(key, trusted, require_signed).with_learned_file(format!("{key_path}.learned"));
        tracing::info!("[L8] public key: {}", l8.public_b64);
        if require_signed && l8.trusted.is_none() {
            tracing::warn!(
                "[L8] CANALIS_REQUIRE_SIGNED_REGISTRATION is on without CANALIS_L8_TRUSTED_NODE_KEYS: \
                 any node that completes the handshake can register"
            );
        }
        Ok(l8)
    }

    /// `GET /.well-known/l8`. Signing only: Canalis doesn't advertise an
    /// encryption key, so senders don't encrypt what they send it.
    pub fn meta(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "service_name": "canalis",
            "public_key": self.public_b64,
            "challenge_endpoint": "/l8/challenge",
            "supported_algorithms": ["ed25519"],
            "capabilities": ["signed_payloads"],
            "spec_url": "https://rjpruitt16.github.io/l8-protocol/spec.json",
        })
    }

    /// `POST /l8/challenge`: the sender proves it owns its key, and Canalis
    /// proves it owns its own. Refuses stale timestamps, reused nonces, and
    /// -- when an allowlist is set -- any key not on it.
    pub fn handle_challenge(&self, req: &ChallengeRequest) -> Result<ChallengeResponse, String> {
        if (now_unix() - req.timestamp).unsigned_abs() > WINDOW.as_secs() {
            return Err("timestamp expired".into());
        }
        let sender = decode_public_key(&req.sender_public_key).ok_or("invalid sender_public_key")?;
        if let Some(trusted) = &self.trusted
            && !trusted.contains(sender.as_bytes()) {
                return Err("sender key is not in CANALIS_L8_TRUSTED_NODE_KEYS".into());
            }
        let signature = decode_signature(&req.signature).ok_or("invalid signature encoding")?;
        let message = format!("{}:{}", req.challenge_id, req.nonce);
        sender.verify(message.as_bytes(), &signature).map_err(|_| "signature verification failed")?;
        if !self.first_sighting(&format!("nonce:{}", req.nonce)) {
            return Err("nonce already used".into());
        }

        self.remember(sender);
        Ok(ChallengeResponse {
            challenge_id: req.challenge_id.clone(),
            nonce: req.nonce.clone(),
            receiver_signature: B64.encode(self.key.sign(message.as_bytes()).to_bytes()),
            receiver_public_key: self.public_b64.clone(),
        })
    }

    /// Checks a signed delivery (a registration ping) against its body.
    pub fn verify(&self, headers: &axum::http::HeaderMap, body: &[u8]) -> Verdict {
        let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).map(str::trim);
        let Some(signature) = header("X-L8-Signature") else {
            return Verdict::Unsigned;
        };
        let (Some(delivery_id), Some(timestamp), Some(id)) =
            (header("X-L8-Delivery-Id"), header("X-L8-Timestamp"), header("X-L8-Key-Id"))
        else {
            return Verdict::Invalid("incomplete X-L8 headers".into());
        };
        let Ok(ts) = timestamp.parse::<i64>() else {
            return Verdict::Invalid("X-L8-Timestamp is not a number".into());
        };
        if (now_unix() - ts).unsigned_abs() > WINDOW.as_secs() {
            return Verdict::Invalid("X-L8-Timestamp is too old or in the future".into());
        }
        let Some(key) = self.key_for(id) else {
            // With no allowlist and signing not required, an unsigned ping
            // is accepted, so one signed by a key this Canalis hasn't
            // learned (a new Canalis, or one whose learned keys were lost)
            // is no less trustworthy: treat it as unsigned rather than lock
            // the node out until it happens to shake hands again.
            if self.trusted.is_none() && !self.require_signed {
                if self.warned.lock().unwrap_or_else(|e| e.into_inner()).insert(id.to_string()) {
                    tracing::warn!("[L8] pings signed by key {id}, which no Canalis node has learned; accepting them as unsigned");
                }
                return Verdict::Unsigned;
            }
            return Verdict::Invalid(format!("key {id} is not trusted"));
        };
        let Some(signature) = decode_signature(signature) else {
            return Verdict::Invalid("X-L8-Signature is not a valid signature".into());
        };
        let message = format!("{delivery_id}.{timestamp}.{}", B64.encode(Sha256::digest(body)));
        if key.verify(message.as_bytes(), &signature).is_err() {
            return Verdict::Invalid("signature does not match the body".into());
        }
        if !self.first_sighting(&format!("delivery:{delivery_id}")) {
            return Verdict::Invalid("delivery replayed".into());
        }
        Verdict::Valid { key: B64.encode(key.as_bytes()) }
    }

    /// The key id of a signed delivery whose key this node hasn't learned
    /// (and has no allowlist to find it in): one another Canalis node may
    /// have learned, worth looking up in Valkey before verifying.
    pub fn unlearned_key_id(&self, headers: &axum::http::HeaderMap) -> Option<String> {
        if self.trusted.is_some() || !headers.contains_key("X-L8-Signature") {
            return None;
        }
        let id = headers.get("X-L8-Key-Id")?.to_str().ok()?.trim().to_string();
        let known = self.learned.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&id);
        (!known).then_some(id)
    }

    /// Adopts a key another Canalis node learned through the handshake
    /// (shared in Valkey). Ignored if it isn't a valid key for that id.
    pub fn adopt(&self, id: &str, public_b64: &str) {
        if let Some(key) = decode_public_key(public_b64).filter(|k| key_id(k.as_bytes()) == id) {
            self.remember(key);
        }
    }

    // An allowlisted key by its id, or -- with no allowlist -- one that
    // completed the handshake.
    fn key_for(&self, id: &str) -> Option<VerifyingKey> {
        match &self.trusted {
            Some(trusted) => trusted
                .iter()
                .find(|k| key_id(k) == id)
                .and_then(|k| VerifyingKey::from_bytes(k).ok()),
            None => self.learned.lock().unwrap_or_else(|e| e.into_inner()).get(id).copied(),
        }
    }

    // True the first time a nonce or delivery id is seen within the window.
    fn first_sighting(&self, token: &str) -> bool {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        seen.retain(|_, at| now.duration_since(*at) < WINDOW);
        seen.insert(token.to_string(), now).is_none()
    }
}

/// A base64 Ed25519 public key's key id, as X-L8-Key-Id carries it.
pub fn key_id_of(public_b64: &str) -> Option<String> {
    decode_public_key(public_b64).map(|k| key_id(k.as_bytes()))
}

fn decode_public_key(b64: &str) -> Option<VerifyingKey> {
    let bytes: [u8; 32] = B64.decode(b64.trim()).ok()?.try_into().ok()?;
    VerifyingKey::from_bytes(&bytes).ok()
}

fn decode_signature(b64: &str) -> Option<Signature> {
    let bytes: [u8; 64] = B64.decode(b64.trim()).ok()?.try_into().ok()?;
    Some(Signature::from_bytes(&bytes))
}

fn parse_keys(list: &str) -> Result<HashSet<[u8; 32]>, String> {
    list.split(',')
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .map(|k| {
            decode_public_key(k)
                .map(|key| key.to_bytes())
                .ok_or_else(|| format!("CANALIS_L8_TRUSTED_NODE_KEYS: {k:?} is not a base64 Ed25519 public key"))
        })
        .collect()
}

// Accepts Go's 64-byte ed25519.PrivateKey (seed + public key, what Aquifer
// writes and reads) or a bare 32-byte seed.
fn signing_key_from_bytes(bytes: &[u8]) -> Option<SigningKey> {
    let seed: [u8; 32] = bytes.get(..32)?.try_into().ok()?;
    let key = SigningKey::from_bytes(&seed);
    match bytes.len() {
        32 => Some(key),
        64 if bytes[32..] == key.verifying_key().to_bytes() => Some(key),
        _ => None,
    }
}

fn load_or_generate_key(env: Option<String>, path: &Path) -> Result<SigningKey, String> {
    if let Some(raw) = env {
        return B64
            .decode(raw.trim())
            .ok()
            .and_then(|b| signing_key_from_bytes(&b))
            .ok_or_else(|| "L8_PRIVATE_KEY is not a base64 Ed25519 private key".into());
    }
    if let Ok(bytes) = std::fs::read(path) {
        return signing_key_from_bytes(&bytes).ok_or_else(|| format!("{} is not an Ed25519 private key", path.display()));
    }
    let key = SigningKey::generate(&mut rand_core::OsRng);
    let mut go_format = key.to_bytes().to_vec();
    go_format.extend_from_slice(key.verifying_key().as_bytes());
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        let _ = std::fs::create_dir_all(dir);
    }
    write_private(path, &go_format).map_err(|e| format!("write {}: {e}", path.display()))?;
    tracing::info!("[L8] generated a new key at {}", path.display());
    Ok(key)
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)?.write_all(bytes)
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::http::HeaderMap;

    pub(crate) fn new_key() -> SigningKey {
        SigningKey::generate(&mut rand_core::OsRng)
    }

    // What Aquifer's EnsureTrust sends.
    pub(crate) fn challenge_from(node: &SigningKey, nonce: &str) -> ChallengeRequest {
        let challenge_id = format!("challenge-{nonce}");
        ChallengeRequest {
            signature: B64.encode(node.sign(format!("{challenge_id}:{nonce}").as_bytes()).to_bytes()),
            challenge_id,
            nonce: nonce.into(),
            timestamp: now_unix(),
            sender_public_key: B64.encode(node.verifying_key().as_bytes()),
        }
    }

    // What Aquifer's signHeaders puts on a delivery.
    pub(crate) fn signed_headers(node: &SigningKey, delivery_id: &str, body: &[u8]) -> HeaderMap {
        let ts = now_unix();
        let message = format!("{delivery_id}.{ts}.{}", B64.encode(Sha256::digest(body)));
        let mut headers = HeaderMap::new();
        headers.insert("X-L8-Delivery-Id", delivery_id.parse().unwrap());
        headers.insert("X-L8-Timestamp", ts.to_string().parse().unwrap());
        headers.insert("X-L8-Key-Id", key_id(node.verifying_key().as_bytes()).parse().unwrap());
        headers.insert("X-L8-Signature", B64.encode(node.sign(message.as_bytes()).to_bytes()).parse().unwrap());
        headers
    }

    fn allowing(node: &SigningKey) -> L8 {
        L8::new(new_key(), Some([node.verifying_key().to_bytes()].into()), true)
    }

    #[test]
    fn a_trusted_node_completes_the_handshake_and_gets_canalis_signature_back() {
        let node = new_key();
        let canalis = allowing(&node);
        let resp = canalis.handle_challenge(&challenge_from(&node, "n1")).unwrap();

        // Exactly what Aquifer checks: the receiver signature over
        // "challenge_id:nonce", by the key /.well-known/l8 advertises.
        let advertised = decode_public_key(canalis.meta()["public_key"].as_str().unwrap()).unwrap();
        let sig = decode_signature(&resp.receiver_signature).unwrap();
        assert!(advertised.verify(b"challenge-n1:n1", &sig).is_ok());
    }

    #[test]
    fn an_unlisted_node_cannot_complete_the_handshake() {
        let canalis = allowing(&new_key());
        let err = canalis.handle_challenge(&challenge_from(&new_key(), "n1")).err().unwrap();
        assert!(err.contains("not in CANALIS_L8_TRUSTED_NODE_KEYS"));
    }

    #[test]
    fn a_reused_nonce_or_stale_challenge_is_refused() {
        let node = new_key();
        let canalis = allowing(&node);
        canalis.handle_challenge(&challenge_from(&node, "n1")).unwrap();
        assert_eq!(canalis.handle_challenge(&challenge_from(&node, "n1")).err().unwrap(), "nonce already used");

        let mut stale = challenge_from(&node, "n2");
        stale.timestamp -= 301;
        assert_eq!(canalis.handle_challenge(&stale).err().unwrap(), "timestamp expired");
    }

    #[test]
    fn a_forged_challenge_signature_is_refused() {
        let node = new_key();
        let canalis = L8::new(new_key(), None, false);
        let mut forged = challenge_from(&node, "n1");
        forged.signature = B64.encode(new_key().sign(b"challenge-n1:n1").to_bytes());
        assert_eq!(canalis.handle_challenge(&forged).err().unwrap(), "signature verification failed");
    }

    #[test]
    fn a_signed_ping_from_a_trusted_node_verifies() {
        let node = new_key();
        let canalis = allowing(&node);
        let body = br#"{"port":"8080","state":"active"}"#;
        assert!(matches!(canalis.verify(&signed_headers(&node, "d1", body), body), Verdict::Valid { .. }));
    }

    #[test]
    fn tampering_replay_and_unknown_keys_are_all_invalid() {
        let node = new_key();
        let canalis = allowing(&node);
        let body = br#"{"port":"8080"}"#;
        let headers = signed_headers(&node, "d1", body);

        assert!(matches!(canalis.verify(&headers, br#"{"port":"6666"}"#), Verdict::Invalid(_)), "body changed");
        assert!(matches!(canalis.verify(&headers, body), Verdict::Valid { .. }));
        assert_eq!(canalis.verify(&headers, body), Verdict::Invalid("delivery replayed".into()));
        let stranger = new_key();
        assert!(matches!(canalis.verify(&signed_headers(&stranger, "d2", body), body), Verdict::Invalid(_)));
    }

    #[test]
    fn without_an_allowlist_only_keys_that_did_the_handshake_verify() {
        let node = new_key();
        let canalis = L8::new(new_key(), None, true);
        let body = b"{}";
        assert!(matches!(canalis.verify(&signed_headers(&node, "d1", body), body), Verdict::Invalid(_)));

        canalis.handle_challenge(&challenge_from(&node, "n1")).unwrap();
        assert!(matches!(canalis.verify(&signed_headers(&node, "d2", body), body), Verdict::Valid { .. }));
    }

    // Signing not required: an unsigned ping would be accepted, so a ping
    // signed by a key this node never learned is accepted as unsigned too,
    // instead of locking the node out.
    #[test]
    fn without_required_signing_an_unknown_key_counts_as_unsigned() {
        let canalis = L8::new(new_key(), None, false);
        let body = b"{}";
        assert_eq!(canalis.verify(&signed_headers(&new_key(), "d1", body), body), Verdict::Unsigned);
        // A known key that doesn't match the body is still invalid.
        let node = new_key();
        canalis.handle_challenge(&challenge_from(&node, "n1")).unwrap();
        assert!(matches!(canalis.verify(&signed_headers(&node, "d2", body), b"tampered"), Verdict::Invalid(_)));
    }

    // A node caches its trust and keeps signing without shaking hands
    // again, so the keys Canalis learned have to outlive a restart.
    #[test]
    fn learned_keys_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(".l8-key.learned");
        let node = new_key();
        let before = L8::new(new_key(), None, true).with_learned_file(&file);
        before.handle_challenge(&challenge_from(&node, "n1")).unwrap();
        before.handle_challenge(&challenge_from(&node, "n2")).unwrap();
        drop(before);
        assert_eq!(std::fs::read_to_string(&file).unwrap().lines().count(), 1, "a key is saved once");

        let after = L8::new(new_key(), None, true).with_learned_file(&file);
        let body = b"{}";
        assert!(matches!(after.verify(&signed_headers(&node, "d1", body), body), Verdict::Valid { .. }));
    }

    #[test]
    fn no_signature_is_unsigned_not_invalid() {
        assert_eq!(L8::new(new_key(), None, false).verify(&HeaderMap::new(), b"{}"), Verdict::Unsigned);
    }

    #[test]
    fn keys_load_in_gos_format_and_as_a_seed_and_persist_when_generated() {
        let key = new_key();
        let mut go = key.to_bytes().to_vec();
        go.extend_from_slice(key.verifying_key().as_bytes());
        let from_go = load_or_generate_key(Some(B64.encode(&go)), Path::new("/nonexistent")).unwrap();
        let from_seed = load_or_generate_key(Some(B64.encode(key.to_bytes())), Path::new("/nonexistent")).unwrap();
        assert_eq!(from_go.verifying_key(), key.verifying_key());
        assert_eq!(from_seed.verifying_key(), key.verifying_key());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keys").join(".l8-key");
        let generated = load_or_generate_key(None, &path).unwrap();
        assert_eq!(std::fs::read(&path).unwrap().len(), 64, "written in Go's 64-byte format");
        let again = load_or_generate_key(None, &path).unwrap();
        assert_eq!(generated.verifying_key(), again.verifying_key(), "the same key on the next start");
    }
}
