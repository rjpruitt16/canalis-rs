//! Assignment: which instance a user's traffic goes to.
//!
//! Canalis shares Aquifer's own cluster assignment table in Valkey (its
//! `AQUIFER_CLUSTER_PROVIDER=valkey` mode, cluster_valkey.go) instead of
//! keeping a separate one, so Canalis and Aquifer nodes that coordinate
//! through Valkey agree on where every user lives:
//!
//! ```text
//! aqueduct:instances                         set of instance ids
//! aqueduct:instance:<id>                     member JSON, expires without heartbeats
//! aqueduct:instance:<id>:assignments         zset: user-scope hash -> expiry (ms)
//! aqueduct:assignment:<sha256(user_id)>      instance id, expires after idling
//! aqueduct:instance:<id>:fullness            0-100 as last reported (Canalis's own)
//! ```
//!
//! Claims run in the same Lua script Aquifer uses, so concurrent claims
//! from Canalis and Aquifer converge on the first one. Canalis passes one
//! extra flag ("strict"): when every candidate is at capacity, Aquifer
//! falls back to the least-loaded instance because it has nowhere else to
//! put the work; Canalis has its line, so it gets nothing back and queues
//! the user instead.
//!
//! What Canalis adds on top, before the script runs: instances at or above
//! CANALIS_QUEUE_AT_PERCENT fullness aren't offered to new users (a user
//! already on one stays -- the script keeps live assignments sticky), an
//! instance reserved for one user is only offered to that user, and a
//! `ws`-role instance isn't offered HTTP traffic.

use crate::Valkey;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

/// Where assignment state lives and the policy around it.
#[derive(Clone, Debug)]
pub struct Aqueduct {
    /// Key prefix, shared with Aquifer (`AQUIFER_CLUSTER_VALKEY_PREFIX`).
    pub prefix: String,
    /// How long an idle assignment lives before the user is free to land
    /// elsewhere; every request through Canalis renews it.
    pub assignment_ttl: Duration,
    /// How long an instance stays assignable without a heartbeat.
    pub instance_ttl: Duration,
    /// Users an instance takes when it doesn't say (drain-mode instances
    /// always take exactly one).
    pub default_capacity: i64,
    /// At or above this fullness, an instance gets no new users.
    pub queue_at_percent: u8,
}

impl Aqueduct {
    pub fn from_env() -> Self {
        let mut prefix = std::env::var("CANALIS_AQUEDUCT_PREFIX").unwrap_or_else(|_| "aqueduct:".into());
        if !prefix.ends_with(':') {
            prefix.push(':');
        }
        let heartbeat = crate::env_u64("CANALIS_REGISTRATION_INTERVAL_SECONDS", 15);
        Self {
            prefix,
            // Same shape as Aquifer's assignmentTTL: idle window plus two
            // heartbeats of slack.
            assignment_ttl: Duration::from_secs(
                crate::env_u64("CANALIS_ASSIGNMENT_IDLE_SECONDS", 300) + 2 * heartbeat,
            ),
            instance_ttl: Duration::from_secs(3 * heartbeat),
            default_capacity: crate::env_u64("CANALIS_DEFAULT_USER_CAPACITY", 100).max(1) as i64,
            queue_at_percent: crate::env_u64("CANALIS_QUEUE_AT_PERCENT", 80).clamp(1, 100) as u8,
        }
    }

    fn instances_key(&self) -> String {
        format!("{}instances", self.prefix)
    }
    fn instance_key(&self, id: &str) -> String {
        format!("{}instance:{id}", self.prefix)
    }
    fn assignments_key(&self, id: &str) -> String {
        format!("{}instance:{id}:assignments", self.prefix)
    }
    fn fullness_key(&self, id: &str) -> String {
        format!("{}instance:{id}:fullness", self.prefix)
    }
    fn assignment_key(&self, scope_hash: &str) -> String {
        format!("{}assignment:{scope_hash}", self.prefix)
    }
}

/// An instance's record, the same JSON Aquifer's ClusterMember writes.
/// Canalis's extra fields ride along; Aquifer ignores what it doesn't know.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Member {
    pub id: String,
    /// Base URL requests go to, e.g. `http://10.0.0.5:8080`.
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub active_users: i64,
    #[serde(rename = "user_capacity", default, skip_serializing_if = "is_zero")]
    pub capacity: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub reported_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserved_for: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_state: Option<String>,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

impl Member {
    pub fn is_active(&self) -> bool {
        !matches!(self.state.as_deref(), Some("draining") | Some("offline"))
    }
}

/// The instance a user is assigned to.
#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub id: String,
    pub base_url: String,
}

pub enum AssignOutcome {
    Assigned(Node),
    PoolExhausted,
}

/// Which kind of traffic a claim is for. An instance registered with
/// `role: ws` only gets WebSocket sessions and one with `role: http` only
/// gets HTTP jobs; one with no role gets both.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Traffic {
    Http,
    WebSocket,
}

impl Traffic {
    fn accepts(self, role: Option<&str>) -> bool {
        !matches!((self, role), (Traffic::Http, Some("ws")) | (Traffic::WebSocket, Some("http")))
    }
}

/// Same hash Aquifer uses for assignment keys (store.go's hashKey), so
/// Canalis and Aquifer find each other's assignments.
pub fn scope_hash(user_id: &str) -> String {
    hex(&Sha256::digest(user_id.as_bytes()))
}

/// Same ranking as Aquifer's rankedOwners: highest
/// sha256(id + ":" + user_id) first, ties by id -- so for a brand-new user
/// Canalis and Aquifer have the same first choice.
fn rendezvous_score(id: &str, user_id: &str) -> String {
    hex(&Sha256::digest(format!("{id}:{user_id}").as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// Aquifer's assignClusterScopeScript (cluster_valkey.go), plus one optional
// trailing argument: "1" means strict -- when no candidate is under
// capacity, return nothing instead of the least-loaded one. Without it the
// script behaves exactly as Aquifer's does.
const ASSIGN_LUA: &str = r"
local assignment = redis.call('GET', KEYS[1])
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
local ttl = tonumber(ARGV[1])
local scope = ARGV[2]
local count = tonumber(ARGV[3])
local prefix = ARGV[4]

local function active_member(payload)
  if not payload then return nil end
  local ok, member = pcall(cjson.decode, payload)
  if ok and member['state'] ~= 'draining' and member['state'] ~= 'offline' then
    return member
  end
  return nil
end

if assignment then
  local payload = redis.call('GET', prefix .. 'instance:' .. assignment)
  if active_member(payload) then
    redis.call('PEXPIRE', KEYS[1], ttl)
    redis.call('ZADD', prefix .. 'instance:' .. assignment .. ':assignments', now + ttl, scope)
    return payload
  end
  redis.call('DEL', KEYS[1])
  redis.call('ZREM', prefix .. 'instance:' .. assignment .. ':assignments', scope)
end

local selected = 0
local least = 0
local least_ratio = nil
for i = 1, count do
  local payload = redis.call('GET', KEYS[2 + (i - 1) * 2])
  local member = active_member(payload)
  if member then
    local assignments = KEYS[3 + (i - 1) * 2]
    redis.call('ZREMRANGEBYSCORE', assignments, '-inf', now)
    local used = redis.call('ZCARD', assignments)
    local reported = tonumber(member['active_users']) or 0
    if reported > used then used = reported end
    local capacity = tonumber(member['user_capacity']) or tonumber(ARGV[4 + (i - 1) * 2 + 2])
    if capacity < 1 then capacity = 1 end
    if selected == 0 and used < capacity then selected = i end
    local ratio = used / capacity
    if least == 0 or ratio < least_ratio then
      least = i
      least_ratio = ratio
    end
  end
end
if selected == 0 and ARGV[5 + count * 2] ~= '1' then selected = least end
if selected == 0 then return '' end

local id = ARGV[4 + (selected - 1) * 2 + 1]
redis.call('SET', KEYS[1], id, 'PX', ttl)
redis.call('ZADD', KEYS[3 + (selected - 1) * 2], now + ttl, scope)
return redis.call('GET', KEYS[2 + (selected - 1) * 2])
";

// Aquifer's releaseClusterScopeScript: only removes an assignment that
// still points at this instance, never a newer one.
const RELEASE_LUA: &str = r"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[2])
return 1
";

/// Every live instance record. Ids whose record has expired (no
/// heartbeat) are pruned from the set, as Aquifer's Members() does.
pub async fn members(valkey: &mut Valkey, aq: &Aqueduct) -> redis::RedisResult<Vec<Member>> {
    let ids: Vec<String> = valkey.smembers(aq.instances_key()).await?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let keys: Vec<String> = ids.iter().map(|id| aq.instance_key(id)).collect();
    let values: Vec<Option<String>> = valkey.mget(&keys).await?;

    let mut members = Vec::with_capacity(ids.len());
    let mut stale = Vec::new();
    for (id, value) in ids.iter().zip(values) {
        match value.and_then(|v| serde_json::from_str::<Member>(&v).ok()) {
            Some(m) if !m.id.is_empty() && !m.address.is_empty() => members.push(m),
            _ => stale.push(id.clone()),
        }
    }
    if !stale.is_empty() {
        let _: () = valkey.srem(aq.instances_key(), &stale).await?;
        for id in &stale {
            let _: () = valkey.del(aq.assignments_key(id)).await?;
        }
    }
    Ok(members)
}

/// The record last written for one instance, if it's still live.
pub async fn member(valkey: &mut Valkey, aq: &Aqueduct, id: &str) -> redis::RedisResult<Option<Member>> {
    let value: Option<String> = valkey.get(aq.instance_key(id)).await?;
    Ok(value.and_then(|v| serde_json::from_str(&v).ok()))
}

/// Writes an instance's record and keeps it in the set, same as Aquifer's
/// own heartbeat. The record expires on its own if heartbeats stop.
pub async fn heartbeat(valkey: &mut Valkey, aq: &Aqueduct, member: &Member) -> redis::RedisResult<()> {
    let payload = serde_json::to_string(member)
        .map_err(|e| redis::RedisError::from((redis::ErrorKind::Client, "encode member", e.to_string())))?;
    let _: () = redis::pipe()
        .atomic()
        .sadd(aq.instances_key(), &member.id)
        .ignore()
        .set_ex(aq.instance_key(&member.id), payload, aq.instance_ttl.as_secs().max(1))
        .ignore()
        .query_async(valkey)
        .await?;
    Ok(())
}

/// Forgets an instance entirely (it said it's offline).
pub async fn remove_instance(valkey: &mut Valkey, aq: &Aqueduct, id: &str) -> redis::RedisResult<()> {
    let _: () = redis::pipe()
        .atomic()
        .del(aq.instance_key(id))
        .ignore()
        .del(aq.fullness_key(id))
        .ignore()
        .srem(aq.instances_key(), id)
        .ignore()
        .query_async(valkey)
        .await?;
    release_all(valkey, aq, id).await.map(|_| ())
}

/// Marks an instance draining in its own record, because it just answered
/// a request with `503` + `X-Aqueduct-Node-State: draining` and its own
/// draining ping may not have arrived yet. Without this the next claim
/// could pick the same instance again. Its next heartbeat overwrites this
/// with whatever it reports itself.
pub async fn mark_draining(valkey: &mut Valkey, aq: &Aqueduct, id: &str) -> redis::RedisResult<()> {
    if let Some(mut m) = member(valkey, aq, id).await? {
        m.state = Some("draining".into());
        heartbeat(valkey, aq, &m).await?;
    }
    Ok(())
}

/// Remembers how full an instance said it was, from its registration ping
/// or the X-Aqueduct-Fullness header on any response. Short-lived, so a
/// reading that stops being refreshed stops counting.
pub async fn record_fullness(valkey: &mut Valkey, aq: &Aqueduct, id: &str, percent: u8) -> redis::RedisResult<()> {
    valkey.set_ex(aq.fullness_key(id), percent, aq.instance_ttl.as_secs().max(1)).await
}

/// How many instances are live, and how many of those would take a new
/// user right now (active and under CANALIS_QUEUE_AT_PERCENT), for
/// GET /health and the scale-up signal. Capacity isn't counted: a full
/// instance at capacity still shows as accepting until it reports fullness.
#[derive(Clone, Copy, Debug, Default, PartialEq, serde::Serialize)]
pub struct Fleet {
    pub total: usize,
    pub accepting: usize,
}

pub async fn fleet(valkey: &mut Valkey, aq: &Aqueduct) -> redis::RedisResult<Fleet> {
    let all = instances(valkey, aq).await?;
    let active: Vec<&(Member, Option<u8>)> = all.iter().filter(|(m, _)| m.is_active()).collect();
    Ok(Fleet {
        total: active.len(),
        accepting: active.iter().filter(|(_, f)| f.is_none_or(|f| f < aq.queue_at_percent)).count(),
    })
}

/// Every registered instance with the fullness it last reported, for
/// GET /metrics.
pub async fn instances(valkey: &mut Valkey, aq: &Aqueduct) -> redis::RedisResult<Vec<(Member, Option<u8>)>> {
    let all = members(valkey, aq).await?;
    let keys: Vec<String> = all.iter().map(|m| aq.fullness_key(&m.id)).collect();
    let fullness: Vec<Option<u8>> = if keys.is_empty() { Vec::new() } else { valkey.mget(&keys).await? };
    Ok(all.into_iter().zip(fullness).collect())
}

/// Resolves user_id's instance: their live assignment if they have one
/// (renewed), otherwise a claim on the best eligible instance for this kind
/// of traffic, otherwise PoolExhausted -- every eligible instance is full,
/// and the caller queues (HTTP) or refuses (WebSocket).
pub async fn assign(
    valkey: &mut Valkey,
    aq: &Aqueduct,
    user_id: &str,
    traffic: Traffic,
) -> redis::RedisResult<AssignOutcome> {
    let all = members(valkey, aq).await?;
    let fullness_keys: Vec<String> = all.iter().map(|m| aq.fullness_key(&m.id)).collect();
    let fullness: Vec<Option<u8>> = if fullness_keys.is_empty() {
        Vec::new()
    } else {
        valkey.mget(&fullness_keys).await?
    };

    let mut candidates: Vec<&Member> = all
        .iter()
        .zip(fullness.iter())
        .filter(|(m, full)| {
            m.is_active()
                && traffic.accepts(m.role.as_deref())
                && m.reserved_for.as_deref().is_none_or(|owner| owner == user_id)
                && full.is_none_or(|f| f < aq.queue_at_percent)
        })
        .map(|(m, _)| m)
        .collect();
    // An instance reserved for this user first, then Aquifer's ranking.
    candidates.sort_by(|a, b| {
        let mine = |m: &Member| m.reserved_for.as_deref() == Some(user_id);
        mine(b)
            .cmp(&mine(a))
            .then_with(|| rendezvous_score(&b.id, user_id).cmp(&rendezvous_score(&a.id, user_id)))
            .then_with(|| a.id.cmp(&b.id))
    });

    // Runs even with no candidates: a user who already has a live
    // assignment keeps it, full or not. A user's WebSocket sessions are
    // assigned separately from their HTTP jobs (a different scope), since
    // they may need a different kind of instance.
    let hash = match traffic {
        Traffic::Http => scope_hash(user_id),
        Traffic::WebSocket => scope_hash(&format!("ws\x00{user_id}")),
    };
    let assign_script = redis::Script::new(ASSIGN_LUA);
    let mut script = assign_script.prepare_invoke();
    script.key(aq.assignment_key(&hash));
    script
        .arg(aq.assignment_ttl.as_millis() as u64)
        .arg(&hash)
        .arg(candidates.len())
        .arg(&aq.prefix);
    for m in &candidates {
        script.key(aq.instance_key(&m.id)).key(aq.assignments_key(&m.id));
        let capacity = if m.capacity > 0 { m.capacity } else { aq.default_capacity };
        script.arg(&m.id).arg(capacity);
    }
    script.arg("1");

    let payload: String = script.invoke_async(valkey).await?;
    if payload.is_empty() {
        return Ok(AssignOutcome::PoolExhausted);
    }
    let member: Member = serde_json::from_str(&payload)
        .map_err(|e| redis::RedisError::from((redis::ErrorKind::Client, "decode member", e.to_string())))?;
    Ok(AssignOutcome::Assigned(Node { id: member.id, base_url: member.address }))
}

/// Releases user_id from node_id, if that's still where they're assigned.
/// Their next request resolves a fresh assignment, or waits in line.
pub async fn release_user(valkey: &mut Valkey, aq: &Aqueduct, user_id: &str, node_id: &str) -> redis::RedisResult<bool> {
    let hash = scope_hash(user_id);
    release_scope(valkey, aq, &hash, node_id).await
}

/// release_user for a user's WebSocket assignment.
pub async fn release_ws_user(valkey: &mut Valkey, aq: &Aqueduct, user_id: &str, node_id: &str) -> redis::RedisResult<bool> {
    let hash = scope_hash(&format!("ws\x00{user_id}"));
    release_scope(valkey, aq, &hash, node_id).await
}

async fn release_scope(valkey: &mut Valkey, aq: &Aqueduct, hash: &str, node_id: &str) -> redis::RedisResult<bool> {
    let released: i64 = redis::Script::new(RELEASE_LUA)
        .key(aq.assignment_key(hash))
        .key(aq.assignments_key(node_id))
        .arg(node_id)
        .arg(hash)
        .invoke_async(valkey)
        .await?;
    Ok(released > 0)
}

/// Releases every user assigned to node_id -- a drain-mode instance that
/// just reported itself free, or one that went offline. Returns how many.
pub async fn release_all(valkey: &mut Valkey, aq: &Aqueduct, node_id: &str) -> redis::RedisResult<usize> {
    let scopes: Vec<String> = valkey.zrange(aq.assignments_key(node_id), 0, -1).await?;
    let mut released = 0;
    for hash in &scopes {
        if release_scope(valkey, aq, hash, node_id).await? {
            released += 1;
        }
    }
    let _: () = valkey.del(aq.assignments_key(node_id)).await?;
    Ok(released)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    pub(crate) fn unique(prefix: &str) -> String {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        format!("{prefix}-{nanos}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) async fn test_valkey() -> Valkey {
        let client = redis::Client::open("redis://127.0.0.1:6379").expect("valid Valkey URL");
        redis::aio::ConnectionManager::new(client)
            .await
            .expect("connect to a real local Valkey on 127.0.0.1:6379 -- required for these tests")
    }

    /// Its own key prefix per test: nothing a test writes can be seen by
    /// another test, or by a real Aquifer sharing the same Valkey.
    pub(crate) fn test_aqueduct() -> Aqueduct {
        Aqueduct {
            prefix: format!("{}:", unique("test-aqueduct")),
            assignment_ttl: Duration::from_secs(60),
            instance_ttl: Duration::from_secs(60),
            default_capacity: 100,
            queue_at_percent: 80,
        }
    }

    /// Deletes everything under the test's prefix.
    pub(crate) async fn cleanup(valkey: &mut Valkey, aq: &Aqueduct) {
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{}*", aq.prefix))
            .query_async(valkey)
            .await
            .unwrap_or_default();
        if !keys.is_empty() {
            let _: redis::RedisResult<()> = valkey.del(keys).await;
        }
    }

    pub(crate) fn node(id: &str) -> Member {
        Member { id: id.into(), address: format!("http://{id}"), state: Some("active".into()), ..Default::default() }
    }

    async fn assigned(valkey: &mut Valkey, aq: &Aqueduct, user: &str) -> Option<String> {
        match assign(valkey, aq, user, Traffic::Http).await.unwrap() {
            AssignOutcome::Assigned(node) => Some(node.id),
            AssignOutcome::PoolExhausted => None,
        }
    }

    #[test]
    fn scope_hash_matches_aquifers_hash_key() {
        // Lowercase hex SHA-256, what Go's hex.EncodeToString(sha256.Sum256(...)) gives.
        assert_eq!(
            scope_hash("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[tokio::test]
    async fn a_user_lands_on_an_instance_and_stays_there() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        heartbeat(&mut v, &aq, &node("a")).await.unwrap();
        heartbeat(&mut v, &aq, &node("b")).await.unwrap();

        let first = assigned(&mut v, &aq, "acme").await;
        let again = assigned(&mut v, &aq, "acme").await;
        let stored: Option<String> = v.get(aq.assignment_key(&scope_hash("acme"))).await.unwrap();
        cleanup(&mut v, &aq).await;

        assert!(first.is_some());
        assert_eq!(first, again, "assignment is sticky");
        assert_eq!(stored, first, "stored under Aquifer's own key shape");
    }

    #[tokio::test]
    async fn many_users_share_an_instance_up_to_its_capacity_then_queue() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        heartbeat(&mut v, &aq, &Member { capacity: 2, ..node("only") }).await.unwrap();

        let one = assigned(&mut v, &aq, "u1").await;
        let two = assigned(&mut v, &aq, "u2").await;
        let three = assigned(&mut v, &aq, "u3").await;
        cleanup(&mut v, &aq).await;

        assert_eq!(one.as_deref(), Some("only"));
        assert_eq!(two.as_deref(), Some("only"));
        assert_eq!(three, None, "strict: a full fleet means the line, not an overloaded instance");
    }

    #[tokio::test]
    async fn an_instance_past_the_fullness_threshold_gets_no_new_users_but_keeps_its_own() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        heartbeat(&mut v, &aq, &node("busy")).await.unwrap();
        let before = assigned(&mut v, &aq, "already-here").await;

        record_fullness(&mut v, &aq, "busy", 85).await.unwrap();
        let newcomer = assigned(&mut v, &aq, "newcomer").await;
        let existing = assigned(&mut v, &aq, "already-here").await;

        heartbeat(&mut v, &aq, &node("spare")).await.unwrap();
        let newcomer_later = assigned(&mut v, &aq, "newcomer").await;
        cleanup(&mut v, &aq).await;

        assert_eq!(before.as_deref(), Some("busy"));
        assert_eq!(newcomer, None, "85% >= the 80% threshold: queue the newcomer");
        assert_eq!(existing.as_deref(), Some("busy"), "users already there stay");
        assert_eq!(newcomer_later.as_deref(), Some("spare"));
    }

    #[tokio::test]
    async fn fleet_counts_live_instances_and_those_still_taking_new_users() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        heartbeat(&mut v, &aq, &node("roomy")).await.unwrap();
        heartbeat(&mut v, &aq, &node("full")).await.unwrap();
        heartbeat(&mut v, &aq, &Member { state: Some("draining".into()), ..node("leaving") }).await.unwrap();
        record_fullness(&mut v, &aq, "full", 95).await.unwrap();

        let fleet = fleet(&mut v, &aq).await.unwrap();
        cleanup(&mut v, &aq).await;

        assert_eq!(fleet, Fleet { total: 2, accepting: 1 }, "a draining instance doesn't count at all");
    }

    #[tokio::test]
    async fn a_reserved_instance_only_takes_its_own_user() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        heartbeat(&mut v, &aq, &Member { reserved_for: Some("vip".into()), ..node("vip-node") }).await.unwrap();

        let stranger = assigned(&mut v, &aq, "stranger").await;
        let owner = assigned(&mut v, &aq, "vip").await;
        cleanup(&mut v, &aq).await;

        assert_eq!(stranger, None);
        assert_eq!(owner.as_deref(), Some("vip-node"));
    }

    #[tokio::test]
    async fn a_reserved_user_prefers_their_own_instance() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        for id in ["shared-1", "shared-2", "shared-3"] {
            heartbeat(&mut v, &aq, &node(id)).await.unwrap();
        }
        heartbeat(&mut v, &aq, &Member { reserved_for: Some("vip".into()), ..node("vip-node") }).await.unwrap();

        let owner = assigned(&mut v, &aq, "vip").await;
        cleanup(&mut v, &aq).await;

        assert_eq!(owner.as_deref(), Some("vip-node"));
    }

    #[tokio::test]
    async fn ws_instances_are_kept_out_of_http_routing() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        heartbeat(&mut v, &aq, &Member { role: Some("ws".into()), ..node("ws-node") }).await.unwrap();

        let user = assigned(&mut v, &aq, "acme").await;
        cleanup(&mut v, &aq).await;

        assert_eq!(user, None);
    }

    #[tokio::test]
    async fn websocket_sessions_go_to_ws_instances_and_http_jobs_to_http_ones() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        heartbeat(&mut v, &aq, &Member { role: Some("ws".into()), ..node("ws-node") }).await.unwrap();
        heartbeat(&mut v, &aq, &Member { role: Some("http".into()), ..node("http-node") }).await.unwrap();

        let ws = match assign(&mut v, &aq, "acme", Traffic::WebSocket).await.unwrap() {
            AssignOutcome::Assigned(n) => Some(n.id),
            AssignOutcome::PoolExhausted => None,
        };
        let http = assigned(&mut v, &aq, "acme").await;
        cleanup(&mut v, &aq).await;

        assert_eq!(ws.as_deref(), Some("ws-node"), "the same user's socket and jobs can live on different instances");
        assert_eq!(http.as_deref(), Some("http-node"));
    }

    #[tokio::test]
    async fn a_draining_instance_hands_its_users_to_another() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        heartbeat(&mut v, &aq, &node("leaving")).await.unwrap();
        let before = assigned(&mut v, &aq, "acme").await;

        heartbeat(&mut v, &aq, &Member { state: Some("draining".into()), ..node("leaving") }).await.unwrap();
        heartbeat(&mut v, &aq, &node("staying")).await.unwrap();
        let after = assigned(&mut v, &aq, "acme").await;
        cleanup(&mut v, &aq).await;

        assert_eq!(before.as_deref(), Some("leaving"));
        assert_eq!(after.as_deref(), Some("staying"));
    }

    #[tokio::test]
    async fn release_all_frees_a_drain_mode_instance_for_the_next_tenant() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        heartbeat(&mut v, &aq, &Member { capacity: 1, drain_state: Some("active".into()), ..node("drain") }).await.unwrap();
        let first = assigned(&mut v, &aq, "tenant-1").await;
        let blocked = assigned(&mut v, &aq, "tenant-2").await;

        let released = release_all(&mut v, &aq, "drain").await.unwrap();
        let next = assigned(&mut v, &aq, "tenant-2").await;
        cleanup(&mut v, &aq).await;

        assert_eq!(first.as_deref(), Some("drain"));
        assert_eq!(blocked, None, "one tenant at a time");
        assert_eq!(released, 1);
        assert_eq!(next.as_deref(), Some("drain"));
    }

    #[tokio::test]
    async fn release_never_clobbers_a_newer_assignment() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        let _: () = v.set(aq.assignment_key(&scope_hash("acme")), "new-node").await.unwrap();

        let released = release_user(&mut v, &aq, "acme", "old-node").await.unwrap();
        let still: String = v.get(aq.assignment_key(&scope_hash("acme"))).await.unwrap();
        cleanup(&mut v, &aq).await;

        assert!(!released);
        assert_eq!(still, "new-node");
    }

    #[tokio::test]
    async fn an_expired_instance_is_pruned_and_never_offered() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        let _: () = v.sadd(aq.instances_key(), "ghost").await.unwrap();

        let user = assigned(&mut v, &aq, "acme").await;
        let still_listed: bool = v.sismember(aq.instances_key(), "ghost").await.unwrap();
        cleanup(&mut v, &aq).await;

        assert_eq!(user, None);
        assert!(!still_listed);
    }
}
