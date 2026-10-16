//! Limits on what one client or one tenant can take.
//!
//! Volumetric floods belong to the edge (the cloud's load balancer, a CDN).
//! These are the application-level limits Canalis enforces itself. Each is
//! a token bucket; past it, 429 + Retry-After.
//!
//! - **Per tenant, on by default**: 1,000 requests a second, bursts of
//!   2,000 (CANALIS_TENANT_RPS / CANALIS_TENANT_BURST, or `limits.tenant`
//!   in the config file; 0 turns it off). That's cluster-wide: each Canalis
//!   node enforces its share, the limit divided by the live Canalis nodes,
//!   so there's no Valkey round trip per request. Tenants can be given
//!   their own limit in the config file (`limits.tenants`).
//! - **Per client address**: 100 requests a second, bursts of 200, and 200
//!   open waiting-room streams and WebSocket sessions
//!   (CANALIS_PER_IP_RPS / _BURST / _MAX_STREAMS, or `limits.per_ip`). On
//!   once CANALIS_TRUSTED_PROXIES is set, or when set explicitly. Off
//!   otherwise, because behind a load balancer Canalis doesn't know about,
//!   every request seems to come from the balancer's one address.
//!
//! Environment variables override the config file.
//!
//! The client address is the connection's, or, when the connection comes
//! from an address in CANALIS_TRUSTED_PROXIES, the rightmost untrusted
//! address in X-Forwarded-For. Registration, health and metrics are never
//! limited.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct Rate {
    per_second: f64,
    burst: f64,
}

impl Rate {
    #[cfg(test)]
    pub fn new(per_second: f64, burst: f64) -> Self {
        Self { per_second, burst }
    }
}

#[derive(Clone, Copy, Debug)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

/// An address range: an IP and how many leading bits must match.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Cidr {
    addr: IpAddr,
    bits: u8,
}

impl Cidr {
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        let (addr, bits) = match s.split_once('/') {
            Some((addr, bits)) => (addr.parse::<IpAddr>().ok()?.to_canonical(), bits.parse::<u8>().ok()?),
            None => {
                let addr = s.parse::<IpAddr>().ok()?.to_canonical();
                (addr, if addr.is_ipv4() { 32 } else { 128 })
            }
        };
        let max = if addr.is_ipv4() { 32 } else { 128 };
        (bits <= max).then_some(Self { addr, bits })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip.to_canonical()) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = if self.bits == 0 { 0 } else { u32::MAX << (32 - self.bits) };
                u32::from(net) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = if self.bits == 0 { 0 } else { u128::MAX << (128 - self.bits) };
                u128::from(net) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }
}

/// Why a request was turned away, as `limit_reason` and the
/// canalis_limited_total label.
#[derive(Debug, PartialEq)]
pub struct Limited {
    pub reason: &'static str,
    pub retry_after: Duration,
}

pub struct Limits {
    trusted_proxies: Vec<Cidr>,
    ip_rate: Option<Rate>,
    ip_max_streams: usize,
    // Cluster-wide; see per_node.
    tenant_rate: Option<Rate>,
    // By user id; None = no limit.
    tenant_overrides: HashMap<String, Option<Rate>>,
    // Live Canalis nodes sharing the tenant limits (at least 1).
    nodes: std::sync::atomic::AtomicUsize,
    ip_buckets: Mutex<HashMap<IpAddr, Bucket>>,
    tenant_buckets: Mutex<HashMap<String, Bucket>>,
    ip_streams: Mutex<HashMap<IpAddr, usize>>,
}

fn env_f64(name: &str) -> Option<f64> {
    std::env::var(name).ok().and_then(|v| v.trim().parse::<f64>().ok())
}

// A rate from (in order) the environment, the file, or the default; a rate
// of 0 means no limit.
fn rate(env_rps: &str, env_burst: &str, file: Option<(f64, Option<f64>)>, default: Option<(f64, f64)>) -> Option<Rate> {
    let (per_second, burst) = match (env_f64(env_rps), file, default) {
        (Some(rps), _, _) => (rps, env_f64(env_burst)),
        (None, Some((rps, burst)), _) => (rps, env_f64(env_burst).or(burst)),
        (None, None, Some((rps, burst))) => (rps, env_f64(env_burst).or(Some(burst))),
        (None, None, None) => return None,
    };
    (per_second > 0.0).then(|| Rate { per_second, burst: burst.filter(|b| *b >= 1.0).unwrap_or((per_second * 2.0).max(1.0)) })
}

impl Limits {
    pub fn from_config(file: &crate::config::LimitsConfig) -> Result<Self, String> {
        let mut trusted_proxies = Vec::new();
        for entry in std::env::var("CANALIS_TRUSTED_PROXIES").unwrap_or_default().split(',') {
            if entry.trim().is_empty() {
                continue;
            }
            trusted_proxies.push(
                Cidr::parse(entry).ok_or_else(|| format!("CANALIS_TRUSTED_PROXIES: {entry:?} isn't an address or CIDR range"))?,
            );
        }
        let per_ip = file.per_ip.unwrap_or_default();
        // Per-address limits mean something once Canalis can see real client
        // addresses: with trusted proxies listed, or when asked for outright.
        let ip_defaults = !trusted_proxies.is_empty();
        let ip_rate = rate(
            "CANALIS_PER_IP_RPS",
            "CANALIS_PER_IP_BURST",
            per_ip.rps.map(|r| (r, per_ip.burst)),
            ip_defaults.then_some((100.0, 200.0)),
        );
        let ip_max_streams = std::env::var("CANALIS_PER_IP_MAX_STREAMS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .or(per_ip.max_streams)
            .unwrap_or(if ip_defaults { 200 } else { 0 });
        let tenant_rate = rate(
            "CANALIS_TENANT_RPS",
            "CANALIS_TENANT_BURST",
            file.tenant.map(|t| (t.rps, t.burst)),
            Some((1000.0, 2000.0)),
        );
        let mut limits = Self::new(trusted_proxies, ip_rate, ip_max_streams, tenant_rate);
        for (user, r) in &file.tenants {
            let own = (r.rps > 0.0).then(|| Rate { per_second: r.rps, burst: r.burst.filter(|b| *b >= 1.0).unwrap_or((r.rps * 2.0).max(1.0)) });
            limits.tenant_overrides.insert(user.clone(), own);
        }
        tracing::info!(
            "limits: per tenant {:?}, per address {:?} with {} streams, {} tenant overrides, {} trusted proxy ranges",
            limits.tenant_rate,
            limits.ip_rate,
            limits.ip_max_streams,
            limits.tenant_overrides.len(),
            limits.trusted_proxies.len()
        );
        Ok(limits)
    }

    /// How many Canalis nodes share the cluster-wide tenant limits.
    pub fn set_live_nodes(&self, nodes: usize) {
        self.nodes.store(nodes.max(1), std::sync::atomic::Ordering::Relaxed);
    }

    // This node's share of a cluster-wide rate.
    fn per_node(&self, rate: Rate) -> Rate {
        let n = self.nodes.load(std::sync::atomic::Ordering::Relaxed).max(1) as f64;
        Rate { per_second: rate.per_second / n, burst: (rate.burst / n).max(1.0) }
    }

    pub fn new(trusted_proxies: Vec<Cidr>, ip_rate: Option<Rate>, ip_max_streams: usize, tenant_rate: Option<Rate>) -> Self {
        Self {
            trusted_proxies,
            ip_rate,
            ip_max_streams,
            tenant_rate,
            tenant_overrides: HashMap::new(),
            nodes: std::sync::atomic::AtomicUsize::new(1),
            ip_buckets: Mutex::default(),
            tenant_buckets: Mutex::default(),
            ip_streams: Mutex::default(),
        }
    }

    #[cfg(test)]
    pub fn none() -> Self {
        Self::new(Vec::new(), None, 0, None)
    }

    pub fn is_trusted_proxy(&self, ip: IpAddr) -> bool {
        self.trusted_proxies.iter().any(|c| c.contains(ip))
    }

    /// The client's address: the connection's, unless it's a trusted
    /// proxy, in which case the rightmost address in X-Forwarded-For that
    /// isn't one (addresses further left are whatever the client claimed).
    pub fn client_ip(&self, remote: IpAddr, headers: &axum::http::HeaderMap) -> IpAddr {
        let remote = remote.to_canonical();
        if !self.is_trusted_proxy(remote) {
            return remote;
        }
        let chain: Vec<IpAddr> = headers
            .get_all("x-forwarded-for")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .filter_map(|a| a.trim().parse::<IpAddr>().ok())
            .map(|a| a.to_canonical())
            .collect();
        chain.iter().rev().find(|a| !self.is_trusted_proxy(**a)).copied().unwrap_or(remote)
    }

    pub fn check_ip(&self, ip: IpAddr) -> Result<(), Limited> {
        let Some(rate) = self.ip_rate else { return Ok(()) };
        take(&self.ip_buckets, ip, rate).map_err(|retry_after| Limited { reason: "ip_rate", retry_after })
    }

    pub fn check_tenant(&self, tenant: &crate::assign::Tenant) -> Result<(), Limited> {
        let rate = match self.tenant_overrides.get(&tenant.user) {
            Some(own) => *own,
            None => self.tenant_rate,
        };
        let Some(rate) = rate else { return Ok(()) };
        take(&self.tenant_buckets, tenant.key(), self.per_node(rate)).map_err(|retry_after| Limited { reason: "tenant_rate", retry_after })
    }

    /// Opens one of an address's streams, or Limited past
    /// CANALIS_PER_IP_MAX_STREAMS. The stream is counted until the guard
    /// is dropped.
    pub fn open_stream(self: &Arc<Self>, ip: IpAddr) -> Result<StreamGuard, Limited> {
        if self.ip_max_streams > 0 {
            let mut streams = self.ip_streams.lock().unwrap_or_else(|e| e.into_inner());
            let open = streams.entry(ip).or_default();
            if *open >= self.ip_max_streams {
                return Err(Limited { reason: "ip_streams", retry_after: Duration::from_secs(5) });
            }
            *open += 1;
        }
        Ok(StreamGuard { limits: self.clone(), ip })
    }

    /// Forgets full buckets and addresses with no streams.
    pub fn prune(&self) {
        // Untouched for a minute: refilled long since, so forgetting it
        // changes nothing.
        let idle = |b: &Bucket| b.at.elapsed() > Duration::from_secs(60);
        self.ip_buckets.lock().unwrap_or_else(|e| e.into_inner()).retain(|_, b| !idle(b));
        self.tenant_buckets.lock().unwrap_or_else(|e| e.into_inner()).retain(|_, b| !idle(b));
        self.ip_streams.lock().unwrap_or_else(|e| e.into_inner()).retain(|_, n| *n > 0);
    }
}

pub struct StreamGuard {
    limits: Arc<Limits>,
    ip: IpAddr,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if self.limits.ip_max_streams == 0 {
            return;
        }
        let mut streams = self.limits.ip_streams.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(open) = streams.get_mut(&self.ip) {
            *open = open.saturating_sub(1);
        }
    }
}

fn refill(bucket: Bucket, rate: Rate, now: Instant) -> Bucket {
    let tokens = (bucket.tokens + now.duration_since(bucket.at).as_secs_f64() * rate.per_second).min(rate.burst);
    Bucket { tokens, at: now }
}

// Takes one token, or says how long until there is one.
fn take<K: std::hash::Hash + Eq>(buckets: &Mutex<HashMap<K, Bucket>>, key: K, rate: Rate) -> Result<(), Duration> {
    let now = Instant::now();
    let mut buckets = buckets.lock().unwrap_or_else(|e| e.into_inner());
    let bucket = buckets.entry(key).or_insert(Bucket { tokens: rate.burst, at: now });
    *bucket = refill(*bucket, rate, now);
    if bucket.tokens >= 1.0 {
        bucket.tokens -= 1.0;
        Ok(())
    } else {
        Err(Duration::from_secs_f64((1.0 - bucket.tokens) / rate.per_second))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rate(per_second: f64, burst: f64) -> Option<Rate> {
        Some(Rate { per_second, burst })
    }

    #[test]
    fn cidrs_match_v4_v6_and_mapped_addresses() {
        let ten = Cidr::parse("10.0.0.0/8").unwrap();
        assert!(ten.contains("10.1.2.3".parse().unwrap()));
        assert!(ten.contains("::ffff:10.1.2.3".parse().unwrap()));
        assert!(!ten.contains("11.0.0.1".parse().unwrap()));
        let fly = Cidr::parse("fdaa::/16").unwrap();
        assert!(fly.contains("fdaa:2d:d853::2".parse().unwrap()));
        assert!(!fly.contains("fdab::1".parse().unwrap()));
        assert!(Cidr::parse("1.2.3.4").unwrap().contains("1.2.3.4".parse().unwrap()));
        assert!(Cidr::parse("10.0.0.0/33").is_none() && Cidr::parse("nope").is_none());
    }

    #[test]
    fn x_forwarded_for_is_only_believed_from_a_trusted_proxy() {
        let limits = Limits::new(vec![Cidr::parse("10.0.0.0/8").unwrap()], None, 0, None);
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-forwarded-for", "6.6.6.6, 203.0.113.9, 10.0.0.2".parse().unwrap());
        assert_eq!(limits.client_ip("10.0.0.1".parse().unwrap(), &headers), "203.0.113.9".parse::<IpAddr>().unwrap(), "rightmost untrusted");
        assert_eq!(limits.client_ip("198.51.100.7".parse().unwrap(), &headers), "198.51.100.7".parse::<IpAddr>().unwrap(), "not from a proxy: ignored");
        assert_eq!(limits.client_ip("10.0.0.1".parse().unwrap(), &axum::http::HeaderMap::new()), "10.0.0.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn a_token_bucket_allows_its_burst_then_says_when_to_retry() {
        let limits = Limits::new(Vec::new(), rate(1.0, 3.0), 0, rate(1.0, 1.0));
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        for _ in 0..3 {
            assert!(limits.check_ip(ip).is_ok());
        }
        let refused = limits.check_ip(ip).unwrap_err();
        assert_eq!(refused.reason, "ip_rate");
        assert!(refused.retry_after > Duration::ZERO && refused.retry_after <= Duration::from_secs(1));
        assert!(limits.check_ip("203.0.113.10".parse().unwrap()).is_ok(), "another address has its own bucket");
        let acme = crate::assign::Tenant::user("acme");
        assert!(limits.check_tenant(&acme).is_ok());
        assert_eq!(limits.check_tenant(&acme).unwrap_err().reason, "tenant_rate");
    }

    #[test]
    fn stream_slots_come_back_when_the_stream_ends() {
        let limits = Arc::new(Limits::new(Vec::new(), None, 2, None));
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        let a = limits.open_stream(ip).unwrap();
        let _b = limits.open_stream(ip).unwrap();
        assert_eq!(limits.open_stream(ip).err().unwrap().reason, "ip_streams");
        drop(a);
        assert!(limits.open_stream(ip).is_ok());
    }

    #[test]
    fn with_nothing_configured_nothing_is_limited() {
        let limits = Arc::new(Limits::none());
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        for _ in 0..1000 {
            assert!(limits.check_ip(ip).is_ok() && limits.check_tenant(&crate::assign::Tenant::user("t")).is_ok());
        }
        let _many: Vec<_> = (0..1000).map(|_| limits.open_stream(ip).unwrap()).collect();
    }

    #[test]
    fn the_environment_beats_the_file_which_beats_the_default() {
        // Variable names no other test uses, so tests running in parallel
        // can't see each other's.
        let none = super::rate("CANALIS_TEST_UNSET_RPS", "CANALIS_TEST_UNSET_BURST", None, Some((1000.0, 2000.0))).unwrap();
        assert_eq!((none.per_second, none.burst), (1000.0, 2000.0), "the default");
        let file = super::rate("CANALIS_TEST_UNSET_RPS", "CANALIS_TEST_UNSET_BURST", Some((50.0, None)), Some((1000.0, 2000.0))).unwrap();
        assert_eq!((file.per_second, file.burst), (50.0, 100.0), "the file, with burst defaulting to twice the rate");
        unsafe { std::env::set_var("CANALIS_TEST_PRECEDENCE_RPS", "7") };
        let env = super::rate("CANALIS_TEST_PRECEDENCE_RPS", "CANALIS_TEST_UNSET_BURST", Some((50.0, None)), Some((1000.0, 2000.0))).unwrap();
        assert_eq!(env.per_second, 7.0, "the environment");
        unsafe { std::env::set_var("CANALIS_TEST_OFF_RPS", "0") };
        assert!(super::rate("CANALIS_TEST_OFF_RPS", "CANALIS_TEST_UNSET_BURST", Some((50.0, None)), Some((1000.0, 2000.0))).is_none(), "0 turns it off");
        assert!(super::rate("CANALIS_TEST_UNSET_RPS", "CANALIS_TEST_UNSET_BURST", None, None).is_none(), "no default: off");
    }

    #[test]
    fn each_node_enforces_its_share_of_a_tenant_limit() {
        let limits = Limits::new(Vec::new(), None, 0, super::rate("CANALIS_TEST_UNSET_RPS", "CANALIS_TEST_UNSET_BURST", Some((10.0, Some(4.0))), None));
        let t = crate::assign::Tenant::user("acme");
        limits.set_live_nodes(2);
        assert!(limits.check_tenant(&t).is_ok() && limits.check_tenant(&t).is_ok());
        assert!(limits.check_tenant(&t).is_err(), "half of a burst of 4 on each of 2 nodes");
    }

    #[test]
    fn a_tenant_can_have_its_own_limit_or_none() {
        let mut limits = Limits::new(Vec::new(), None, 0, super::rate("CANALIS_TEST_UNSET_RPS", "CANALIS_TEST_UNSET_BURST", Some((1.0, Some(1.0))), None));
        limits.tenant_overrides.insert("big".into(), None);
        limits.tenant_overrides.insert("bad".into(), Some(Rate { per_second: 1.0, burst: 1.0 }));
        let (big, bad, other) = (crate::assign::Tenant::user("big"), crate::assign::Tenant::user("bad"), crate::assign::Tenant::user("other"));
        for _ in 0..100 {
            assert!(limits.check_tenant(&big).is_ok(), "no limit for big");
        }
        assert!(limits.check_tenant(&bad).is_ok() && limits.check_tenant(&bad).is_err());
        assert!(limits.check_tenant(&other).is_ok() && limits.check_tenant(&other).is_err(), "everyone else gets the default");
    }
}
