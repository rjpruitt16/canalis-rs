//! Which Canalis node holds a tenant's line.
//!
//! Each Canalis keeps its line on its own disk. With several Canalis nodes
//! behind a load balancer, a tenant whose requests land on two of them
//! would otherwise wait in two lines, and `GET /jobs/{key}` would only find
//! a job on the node that queued it. So the first node to put a tenant in
//! line claims them in Valkey, and any other node that would queue that
//! tenant forwards the request to the owner instead:
//!
//! ```text
//! <prefix>canalis:node:<node id>        this node's URL, expires without heartbeats
//! <prefix>line-owner:<sha256(tenant)>   the node holding the tenant's line
//! ```
//!
//! Ownership is checked only when a request would be queued, so the usual
//! path (an instance has room) costs nothing extra. The owner renews its
//! claim while the tenant has jobs waiting and releases it when they're
//! done. If the owner stops heartbeating, the next node to queue that
//! tenant takes over; jobs still on the lost node's disk wait there until
//! it comes back, and are gone if it never does.
//!
//! A node with no URL to advertise (CANALIS_ADVERTISE_URL, or FLY_PRIVATE_IP
//! on Fly) never claims tenants, so a single Canalis behaves as before.

use crate::{Valkey, assign};
use redis::AsyncCommands;
use std::time::Duration;

pub struct Ownership {
    /// This node's id, unique among the Canalis nodes sharing Valkey.
    pub node_id: String,
    /// How other nodes reach this one, if it takes part.
    pub advertise: Option<String>,
    pub owner_ttl: Duration,
    pub node_ttl: Duration,
}

/// Where a queued request should go.
#[derive(Debug, PartialEq)]
pub enum LineOwner {
    /// This node: queue it here.
    Here,
    /// Another live node, at this URL: forward it there.
    Elsewhere { node: String, url: String },
}

// Claim the tenant if nobody holds it (or this node already does); else
// say who does. Returns this node's id when it holds the claim.
const CLAIM_LUA: &str = r"
local cur = redis.call('GET', KEYS[1])
if (not cur) or cur == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return ARGV[1]
end
return cur
";

// Take over from a node that's gone, only if it still holds the claim.
const TAKE_OVER_LUA: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[2] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
  return 1
end
return 0
";

// Release only a claim this node still holds.
const RELEASE_LUA: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
";

impl Ownership {
    pub fn from_env() -> Self {
        let node_id = std::env::var("CANALIS_NODE_ID")
            .or_else(|_| std::env::var("FLY_MACHINE_ID"))
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(random_id);
        let advertise = std::env::var("CANALIS_ADVERTISE_URL")
            .ok()
            .map(|v| v.trim().trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty())
            .or_else(|| {
                std::env::var("FLY_PRIVATE_IP")
                    .ok()
                    .and_then(|ip| ip.trim().parse::<std::net::IpAddr>().ok())
                    .map(|ip| format!("http://{}", std::net::SocketAddr::new(ip, 8080)))
            });
        let owner_ttl = Duration::from_secs(crate::env_u64("CANALIS_LINE_OWNER_TTL_SECONDS", 30).max(5));
        Self { node_id, advertise, owner_ttl, node_ttl: Duration::from_secs(15) }
    }

    pub fn enabled(&self) -> bool {
        self.advertise.is_some()
    }

    fn owner_key(aq: &assign::Aqueduct, tenant: &assign::Tenant) -> String {
        format!("{}line-owner:{}", aq.prefix, assign::scope_hash(&format!("line\x00{}", tenant.key())))
    }

    fn node_key(aq: &assign::Aqueduct, node: &str) -> String {
        format!("{}canalis:node:{node}", aq.prefix)
    }

    /// Advertises this node; called every few seconds.
    pub async fn heartbeat(&self, valkey: &mut Valkey, aq: &assign::Aqueduct) -> redis::RedisResult<()> {
        let Some(url) = &self.advertise else { return Ok(()) };
        valkey.set_ex(Self::node_key(aq, &self.node_id), url, self.node_ttl.as_secs()).await
    }

    pub async fn leave(&self, valkey: &mut Valkey, aq: &assign::Aqueduct) -> redis::RedisResult<()> {
        valkey.del(Self::node_key(aq, &self.node_id)).await
    }

    /// Where a request for `tenant` that has to wait in line should go.
    /// Claims the tenant for this node if nobody holds it, and takes over
    /// from an owner that stopped heartbeating.
    pub async fn owner_for_queueing(
        &self,
        valkey: &mut Valkey,
        aq: &assign::Aqueduct,
        tenant: &assign::Tenant,
    ) -> redis::RedisResult<LineOwner> {
        if !self.enabled() {
            return Ok(LineOwner::Here);
        }
        let key = Self::owner_key(aq, tenant);
        let ttl = self.owner_ttl.as_millis() as u64;
        let owner: String =
            redis::Script::new(CLAIM_LUA).key(&key).arg(&self.node_id).arg(ttl).invoke_async(valkey).await?;
        if owner == self.node_id {
            return Ok(LineOwner::Here);
        }
        let url: Option<String> = valkey.get(Self::node_key(aq, &owner)).await?;
        match url {
            Some(url) => Ok(LineOwner::Elsewhere { node: owner, url }),
            None => {
                let took: i64 = redis::Script::new(TAKE_OVER_LUA)
                    .key(&key)
                    .arg(&self.node_id)
                    .arg(&owner)
                    .arg(ttl)
                    .invoke_async(valkey)
                    .await?;
                if took == 1 {
                    tracing::warn!("Canalis node {owner} is gone; took over {}'s line", tenant.user);
                    return Ok(LineOwner::Here);
                }
                // Someone else took over first; ask again who.
                Box::pin(self.owner_for_queueing(valkey, aq, tenant)).await
            }
        }
    }

    /// The live node other than this one holding `tenant`'s line, if any:
    /// where `GET /jobs/{key}` should look for a job this node doesn't have.
    pub async fn other_owner(
        &self,
        valkey: &mut Valkey,
        aq: &assign::Aqueduct,
        tenant: &assign::Tenant,
    ) -> redis::RedisResult<Option<(String, String)>> {
        let owner: Option<String> = valkey.get(Self::owner_key(aq, tenant)).await?;
        let Some(owner) = owner.filter(|o| *o != self.node_id) else { return Ok(None) };
        let url: Option<String> = valkey.get(Self::node_key(aq, &owner)).await?;
        Ok(url.map(|url| (owner, url)))
    }

    /// Keeps this node's claim on tenants it still has jobs for (claiming
    /// any it doesn't hold yet, such as after a restart).
    pub async fn renew(&self, valkey: &mut Valkey, aq: &assign::Aqueduct, tenants: &[assign::Tenant]) -> redis::RedisResult<()> {
        if !self.enabled() {
            return Ok(());
        }
        let ttl = self.owner_ttl.as_millis() as u64;
        for tenant in tenants {
            let owner: String = redis::Script::new(CLAIM_LUA)
                .key(Self::owner_key(aq, tenant))
                .arg(&self.node_id)
                .arg(ttl)
                .invoke_async(valkey)
                .await?;
            if owner != self.node_id {
                tracing::warn!("{} has jobs waiting here but their line is held by Canalis node {owner}", tenant.user);
            }
        }
        Ok(())
    }

    /// Lets go of a tenant whose line here is empty.
    pub async fn release(&self, valkey: &mut Valkey, aq: &assign::Aqueduct, tenant: &assign::Tenant) -> redis::RedisResult<()> {
        if !self.enabled() {
            return Ok(());
        }
        let _: i64 = redis::Script::new(RELEASE_LUA)
            .key(Self::owner_key(aq, tenant))
            .arg(&self.node_id)
            .invoke_async(valkey)
            .await?;
        Ok(())
    }
}

fn random_id() -> String {
    let mut bytes = [0u8; 8];
    rand_core::RngCore::fill_bytes(&mut rand_core::OsRng, &mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::assign::tests::{cleanup, test_aqueduct, test_valkey};

    pub(crate) fn node(id: &str, url: Option<&str>) -> Ownership {
        Ownership {
            node_id: id.into(),
            advertise: url.map(str::to_string),
            owner_ttl: Duration::from_secs(30),
            node_ttl: Duration::from_secs(15),
        }
    }

    #[tokio::test]
    async fn the_first_node_to_queue_a_tenant_holds_them_and_others_forward() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        let (a, b) = (node("a", Some("http://a:8080")), node("b", Some("http://b:8080")));
        a.heartbeat(&mut v, &aq).await.unwrap();
        b.heartbeat(&mut v, &aq).await.unwrap();
        let acme = assign::Tenant::user("acme");

        let at_a = a.owner_for_queueing(&mut v, &aq, &acme).await.unwrap();
        let at_b = b.owner_for_queueing(&mut v, &aq, &acme).await.unwrap();
        let other_tenant_at_b = b.owner_for_queueing(&mut v, &aq, &assign::Tenant::user("other")).await.unwrap();
        let lookup_from_b = b.other_owner(&mut v, &aq, &acme).await.unwrap();

        a.release(&mut v, &aq, &acme).await.unwrap();
        let after_release = b.owner_for_queueing(&mut v, &aq, &acme).await.unwrap();
        cleanup(&mut v, &aq).await;

        assert_eq!(at_a, LineOwner::Here);
        assert_eq!(at_b, LineOwner::Elsewhere { node: "a".into(), url: "http://a:8080".into() });
        assert_eq!(other_tenant_at_b, LineOwner::Here);
        assert_eq!(lookup_from_b, Some(("a".into(), "http://a:8080".into())));
        assert_eq!(after_release, LineOwner::Here, "a released acme, so b takes them");
    }

    #[tokio::test]
    async fn a_gone_owner_is_taken_over() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        let (a, b) = (node("a", Some("http://a:8080")), node("b", Some("http://b:8080")));
        a.heartbeat(&mut v, &aq).await.unwrap();
        let acme = assign::Tenant::user("acme");
        assert_eq!(a.owner_for_queueing(&mut v, &aq, &acme).await.unwrap(), LineOwner::Here);

        a.leave(&mut v, &aq).await.unwrap(); // a's heartbeat is gone
        let at_b = b.owner_for_queueing(&mut v, &aq, &acme).await.unwrap();
        let back_at_a = a.owner_for_queueing(&mut v, &aq, &acme).await;
        cleanup(&mut v, &aq).await;

        assert_eq!(at_b, LineOwner::Here, "b took over");
        // a (no heartbeat any more, b's isn't written either) doesn't see a
        // live owner to forward to, so it takes the claim back: never stuck.
        assert!(back_at_a.is_ok());
    }

    #[tokio::test]
    async fn a_node_without_an_address_never_claims() {
        let mut v = test_valkey().await;
        let aq = test_aqueduct();
        let solo = node("solo", None);
        let acme = assign::Tenant::user("acme");
        assert_eq!(solo.owner_for_queueing(&mut v, &aq, &acme).await.unwrap(), LineOwner::Here);
        let held: Option<String> = v.get(Ownership::owner_key(&aq, &acme)).await.unwrap();
        cleanup(&mut v, &aq).await;
        assert_eq!(held, None);
    }
}
