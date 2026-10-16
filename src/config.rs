//! The optional config file (CANALIS_CONFIG_PATH), read once at startup.
//! Environment variables override what it says. Today it holds limits:
//!
//! ```yaml
//! limits:
//!   tenant: { rps: 1000, burst: 2000 }      # cluster-wide, per tenant
//!   per_ip: { rps: 100, burst: 200, max_streams: 200 }
//!   tenants:                                # by user id; rps 0 = no limit
//!     big-customer: { rps: 5000 }
//!     bad-actor: { rps: 2 }
//! ```

use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    #[serde(default)]
    pub limits: LimitsConfig,
}

#[derive(Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    pub tenant: Option<RateConfig>,
    pub per_ip: Option<PerIpConfig>,
    #[serde(default)]
    pub tenants: HashMap<String, RateConfig>,
}

#[derive(Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RateConfig {
    pub rps: f64,
    pub burst: Option<f64>,
}

#[derive(Deserialize, Clone, Copy, Debug, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PerIpConfig {
    pub rps: Option<f64>,
    pub burst: Option<f64>,
    pub max_streams: Option<usize>,
}

impl FileConfig {
    /// The file at CANALIS_CONFIG_PATH, or an empty config if it's unset.
    /// A file that's set but unreadable or invalid stops startup: running
    /// with limits silently missing would be worse.
    pub fn from_env() -> Result<Self, String> {
        let Ok(path) = std::env::var("CANALIS_CONFIG_PATH") else { return Ok(Self::default()) };
        let path = path.trim();
        if path.is_empty() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(path).map_err(|e| format!("CANALIS_CONFIG_PATH {path}: {e}"))?;
        Self::parse(&text).map_err(|e| format!("CANALIS_CONFIG_PATH {path}: {e}"))
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        serde_yaml::from_str(text).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_file_parses() {
        let config = FileConfig::parse(
            "limits:\n  tenant: { rps: 1000, burst: 2000 }\n  per_ip: { rps: 100, max_streams: 50 }\n  tenants:\n    big: { rps: 5000 }\n    bad: { rps: 2, burst: 2 }\n",
        )
        .unwrap();
        assert_eq!(config.limits.tenant, Some(RateConfig { rps: 1000.0, burst: Some(2000.0) }));
        assert_eq!(config.limits.per_ip.unwrap().max_streams, Some(50));
        assert_eq!(config.limits.tenants["bad"], RateConfig { rps: 2.0, burst: Some(2.0) });
    }

    #[test]
    fn typos_are_errors_not_silently_ignored() {
        assert!(FileConfig::parse("limits:\n  tennant: { rps: 1 }\n").is_err());
        assert!(FileConfig::parse("limits:\n  tenant: { rsp: 1 }\n").is_err());
        assert!(FileConfig::parse("").unwrap().limits.tenant.is_none());
    }
}
