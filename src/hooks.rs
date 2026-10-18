//! Webhook intake through plugins.
//!
//! A plugin is a WebAssembly module (built with Extism, so it can be
//! written in TypeScript, Rust, Go, ...) that recognizes one provider's
//! webhooks. Drop `shopify.wasm` into CANALIS_PLUGINS_DIR (default
//! `plugins`) and Canalis serves `POST /hooks/shopify`. The plugin only
//! answers "is this request genuinely from the provider, and what is it?";
//! everything else is Canalis's, the same for every plugin: the event is
//! deduplicated by its id, written to the line before the provider is
//! answered, and delivered to Aquifer fairly per tenant from there.
//!
//! The plugin interface, all JSON:
//!
//! - `describe() -> { name, secrets: [env var names] }`. Every secret it
//!   names must be set in the environment, or Canalis won't start (better
//!   than accepting webhooks it can't verify).
//! - `verify({ method, path, query, headers, body_base64, secrets })`
//!   `-> { ok: { event_id, tenant?, event_type?, target?, headers? } }`
//!   `|  { reject: { status, reason } }`.
//!   `tenant` is whom the event is for, for fairness (default `hook:<name>`);
//!   `target` is where it's delivered (default CANALIS_HOOK_<NAME>_TARGET);
//!   `headers` are passed on with it.
//!
//! Plugins run sandboxed: no network, no filesystem, a time limit
//! (CANALIS_PLUGIN_TIMEOUT_MS, 1000) and a memory limit. For signatures they
//! call Canalis's own crypto, so it's native speed and correct:
//! `hmac_sha256` / `hmac_sha1` (`{ key_base64, message_base64 }` -> base64)
//! and `sha256` (base64 -> base64).

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use extism::{CompiledPlugin, Manifest, PTR, Plugin, PluginBuilder, Pool, UserData, Wasm, host_fn};
use hmac::Mac;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[derive(Deserialize, Serialize)]
struct KeyedMessage {
    key_base64: String,
    message_base64: String,
}

fn hmac_of<M: Mac + hmac::digest::KeyInit>(input: &str) -> Result<String, extism::Error> {
    let req: KeyedMessage = serde_json::from_str(input)?;
    let mut mac = <M as hmac::digest::KeyInit>::new_from_slice(&B64.decode(req.key_base64)?)?;
    mac.update(&B64.decode(req.message_base64)?);
    Ok(B64.encode(mac.finalize().into_bytes()))
}

host_fn!(hmac_sha256(input: String) -> String { hmac_of::<hmac::Hmac<sha2::Sha256>>(&input) });
host_fn!(hmac_sha1(input: String) -> String { hmac_of::<hmac::Hmac<sha1::Sha1>>(&input) });
host_fn!(sha256(input: String) -> String { Ok(B64.encode(sha2::Sha256::digest(B64.decode(input)?))) });

#[derive(Deserialize, Debug)]
struct Description {
    name: String,
    #[serde(default)]
    secrets: Vec<String>,
}

#[derive(Serialize)]
pub struct VerifyInput<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub headers: HashMap<String, String>,
    pub body_base64: String,
    pub secrets: &'a HashMap<String, String>,
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Ok(Verified),
    Reject(Rejected),
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Verified {
    pub event_id: String,
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub event_type: Option<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Rejected {
    pub status: u16,
    pub reason: String,
}

pub struct HookPlugin {
    pub name: String,
    pub secrets: HashMap<String, String>,
    /// CANALIS_HOOK_<NAME>_TARGET, when set.
    pub default_target: Option<String>,
    pool: Pool,
    timeout: Duration,
}

impl HookPlugin {
    /// Runs the plugin's verify (blocking: call it off the async runtime).
    pub fn verify(&self, input: &VerifyInput) -> Result<Verdict, String> {
        let input = serde_json::to_string(input).map_err(|e| e.to_string())?;
        let output = self
            .pool
            .with_plugin(self.timeout, |plugin| plugin.call::<&str, String>("verify", &input))
            .map_err(|e| format!("plugin {} failed: {e}", self.name))?
            .ok_or_else(|| format!("plugin {} is busy", self.name))?;
        serde_json::from_str(&output).map_err(|e| format!("plugin {} returned something that isn't a verdict: {e}", self.name))
    }
}

/// Every plugin found, by name.
#[derive(Default)]
pub struct Hooks {
    plugins: HashMap<String, Arc<HookPlugin>>,
}

fn env_name(plugin: &str) -> String {
    plugin.to_ascii_uppercase().replace('-', "_")
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 40 && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

impl Hooks {
    pub fn get(&self, name: &str) -> Option<Arc<HookPlugin>> {
        self.plugins.get(name).cloned()
    }

    #[cfg(test)]
    pub fn names(&self) -> Vec<&str> {
        self.plugins.keys().map(String::as_str).collect()
    }

    pub fn from_env() -> Result<Self, String> {
        let dir = std::env::var("CANALIS_PLUGINS_DIR").unwrap_or_else(|_| "plugins".into());
        let timeout = Duration::from_millis(crate::env_u64("CANALIS_PLUGIN_TIMEOUT_MS", 1000).max(10));
        Self::load(Path::new(&dir), timeout, |name| std::env::var(name).ok())
    }

    /// Loads every `*.wasm` in `dir` (none if it doesn't exist). `env`
    /// looks up secrets and targets.
    pub fn load(dir: &Path, timeout: Duration, env: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mut hooks = Self::default();
        let Ok(entries) = std::fs::read_dir(dir) else { return Ok(hooks) };
        let mut paths: Vec<_> = entries.filter_map(|e| e.ok().map(|e| e.path())).filter(|p| p.extension().is_some_and(|x| x == "wasm")).collect();
        paths.sort();
        for path in paths {
            let plugin = load_one(&path, timeout, &env)?;
            tracing::info!("plugin {} serving POST /hooks/{}", path.display(), plugin.name);
            if hooks.plugins.insert(plugin.name.clone(), Arc::new(plugin)).is_some() {
                return Err(format!("two plugins in {} are both named the same", dir.display()));
            }
        }
        Ok(hooks)
    }
}

fn load_one(path: &Path, timeout: Duration, env: &impl Fn(&str) -> Option<String>) -> Result<HookPlugin, String> {
    let fail = |e: &dyn std::fmt::Display| format!("plugin {}: {e}", path.display());
    let manifest = Manifest::new([Wasm::file(path)])
        // 64 MiB of plugin memory (pages of 64 KiB).
        .with_memory_max(1024)
        .with_timeout(timeout);
    let builder = PluginBuilder::new(manifest)
        .with_wasi(true)
        .with_function("hmac_sha256", [PTR], [PTR], UserData::<()>::default(), hmac_sha256)
        .with_function("hmac_sha1", [PTR], [PTR], UserData::<()>::default(), hmac_sha1)
        .with_function("sha256", [PTR], [PTR], UserData::<()>::default(), sha256);
    let compiled = Arc::new(CompiledPlugin::new(builder).map_err(|e| fail(&e))?);

    let mut first = Plugin::new_from_compiled(&compiled).map_err(|e| fail(&e))?;
    let described: Description = first
        .call::<&str, String>("describe", "")
        .map_err(|e| fail(&format!("describe failed: {e}")))
        .and_then(|out| serde_json::from_str(&out).map_err(|e| fail(&format!("describe didn't return {{name, secrets}}: {e}"))))?;
    if !valid_name(&described.name) {
        return Err(fail(&format!("name {:?} must be 1-40 of a-z 0-9 -", described.name)));
    }
    if !first.function_exists("verify") {
        return Err(fail(&"it has no verify function"));
    }
    let mut secrets = HashMap::new();
    for secret in &described.secrets {
        let value = env(secret).filter(|v| !v.is_empty()).ok_or_else(|| fail(&format!("needs the secret {secret}, which isn't set")))?;
        secrets.insert(secret.clone(), value);
    }
    let default_target = env(&format!("CANALIS_HOOK_{}_TARGET", env_name(&described.name))).filter(|v| !v.is_empty());
    drop(first);

    let source = compiled.clone();
    let pool = Pool::new(move || Plugin::new_from_compiled(&source));
    Ok(HookPlugin { name: described.name, secrets, default_target, pool, timeout })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn host_crypto_matches_known_answers() {
        let input = serde_json::to_string(&KeyedMessage { key_base64: B64.encode("secret"), message_base64: B64.encode("hello") }).unwrap();
        // printf hello | openssl dgst -sha256 -hmac secret -binary | base64
        assert_eq!(hmac_of::<hmac::Hmac<sha2::Sha256>>(&input).unwrap(), "iKqz7ejTrflNJquQ07r9SiCDBww7zOnAFO4EpEOEfAs=");
        // printf hello | openssl dgst -sha1 -hmac secret -binary | base64
        assert_eq!(hmac_of::<hmac::Hmac<sha1::Sha1>>(&input).unwrap(), "URIFXAX5RPhXVe/FzYlw4ZTp9Fs=");
    }

    #[test]
    fn verdicts_parse_and_names_are_checked() {
        let ok: Verdict = serde_json::from_str(r#"{"ok":{"event_id":"e1","tenant":"shop.example"}}"#).unwrap();
        assert!(matches!(ok, Verdict::Ok(Verified { ref event_id, .. }) if event_id == "e1"));
        let no: Verdict = serde_json::from_str(r#"{"reject":{"status":401,"reason":"bad signature"}}"#).unwrap();
        assert_eq!(no, Verdict::Reject(Rejected { status: 401, reason: "bad signature".into() }));
        assert!(valid_name("shopify") && valid_name("my-crm-2") && !valid_name("Shopify") && !valid_name("a/b") && !valid_name(""));
        assert_eq!(env_name("my-crm"), "MY_CRM");
    }

    #[test]
    fn a_missing_plugins_dir_means_no_hooks() {
        let hooks = Hooks::load(Path::new("/definitely/not/here"), Duration::from_secs(1), |_| None).unwrap();
        assert!(hooks.names().is_empty());
    }
}
