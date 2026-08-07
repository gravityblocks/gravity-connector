use core::fmt;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr,
};

use gravity_types::{LoggingConfig, env_string};
use serde::{Deserialize, Deserializer, de};
use serde_with::{DisplayFromStr, serde_as};
use solana_address::Address;
use url::Url;

#[serde_as]
#[derive(serde::Deserialize)]
pub struct Config {
    #[serde(default, with = "env_string")]
    pub discord_webhook: Option<String>,
    pub instance_id: String,
    /// Validator ledger directory containing `admin.rpc` and
    /// `scheduler_bindings.ipc`.
    pub ledger_path: PathBuf,
    pub connector_agave_core: usize,
    pub connector_network_core: usize,
    pub num_workers: usize,
    pub relay_addrs: Vec<RelayEndpoint>,
    pub client_variant: ClientVariant,
    pub jito: Option<JitoConfig>,
    pub logging: LoggingConfig,
    pub slot_duration_adjustment_ms: i64,
    pub shred_receivers: Vec<SocketAddr>,
    pub shred_retransmit_receivers: Vec<SocketAddr>,
    #[serde(default)]
    pub filter_ofac: bool,
    pub identity_path: PathBuf,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: SocketAddr,
}

const fn default_metrics_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9093)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelayEndpoint {
    url: Url,
}

impl RelayEndpoint {
    pub fn host(&self) -> &str {
        self.url.host_str().expect("validated relay endpoint must have a host")
    }

    pub fn port(&self) -> u16 {
        self.url.port().expect("validated relay endpoint must have a port")
    }

    pub fn ip_addr(&self) -> Option<IpAddr> {
        self.host().trim_start_matches('[').trim_end_matches(']').parse().ok()
    }
}

impl fmt::Display for RelayEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(f)
    }
}

impl FromStr for RelayEndpoint {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // Keep existing configurations working while TCP URLs are rolled out.
        if let Ok(addr) = raw.parse::<SocketAddr>() {
            let url = Url::parse(&format!("tcp://{addr}"))
                .map_err(|err| format!("invalid relay address `{raw}`: {err}"))?;
            return Ok(Self { url });
        }

        let url =
            Url::parse(raw).map_err(|err| format!("invalid relay endpoint `{raw}`: {err}"))?;
        if url.scheme() != "tcp" {
            return Err(format!("relay endpoint `{raw}` must use the `tcp` scheme"));
        }
        if url.host().is_none() {
            return Err(format!("relay endpoint `{raw}` must include a host"));
        }
        if url.port().is_none() {
            return Err(format!("relay endpoint `{raw}` must include an explicit port"));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(format!("relay endpoint `{raw}` must not include credentials"));
        }
        if !url.path().is_empty() || url.query().is_some() || url.fragment().is_some() {
            return Err(format!(
                "relay endpoint `{raw}` must not include a path, query, or fragment"
            ));
        }
        Ok(Self { url })
    }
}

impl<'de> Deserialize<'de> for RelayEndpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?.parse().map_err(de::Error::custom)
    }
}

impl Config {
    pub fn admin_rpc_path(&self) -> PathBuf {
        self.ledger_path.join("admin.rpc")
    }

    pub fn scheduler_bindings_path(&self) -> PathBuf {
        self.ledger_path.join("scheduler_bindings.ipc")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClientVariant {
    Agave,
    Jito,
}

impl ClientVariant {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Agave => "agave",
            Self::Jito => "jito",
        }
    }
}

#[serde_as]
#[derive(serde::Deserialize)]
pub struct JitoConfig {
    #[serde_as(as = "DisplayFromStr")]
    pub vote_account_pubkey: Address,
    pub block_engine_proxy_addr: Option<SocketAddr>,
    pub block_engine_urls: Option<Vec<Url>>,
    pub rpc_url: String,
    #[serde(default, with = "env_string")]
    pub rpc_api_key: Option<String>,
    pub mev_commission_bps: u16,
    #[serde_as(as = "DisplayFromStr")]
    pub merkle_root_upload_authority: Address,
    #[serde_as(as = "DisplayFromStr")]
    pub tip_distribution_program_pubkey: Address,
    #[serde_as(as = "DisplayFromStr")]
    pub tip_payment_program_pubkey: Address,
}
