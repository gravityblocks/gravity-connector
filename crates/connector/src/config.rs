use core::fmt;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr,
};

use gravity_types::{AlertWebhook, LoggingConfig, WebhookUrl, env_string};
use serde::{Deserialize, Deserializer, de};
use serde_with::{DisplayFromStr, serde_as};
use solana_address::Address;
use url::Url;

#[serde_as]
#[derive(serde::Deserialize)]
pub struct Config {
    #[serde(default)]
    pub alert_webhook: Option<AlertWebhook>,
    /// Legacy Discord-only configuration. Prefer `alert_webhook` for new
    /// deployments.
    #[serde(default, with = "env_string")]
    pub discord_webhook: Option<WebhookUrl>,
    pub instance_id: String,
    /// Validator ledger directory containing `admin.rpc` and
    /// `scheduler_bindings.ipc`.
    pub ledger_path: PathBuf,
    pub connector_core: usize,
    pub num_workers: usize,
    pub relay_addrs: Vec<RelayEndpoint>,
    pub client: ClientConfig,
    pub logging: LoggingConfig,
    #[serde(default)]
    pub slot_duration_override_ms: Option<u64>,
    #[serde(default)]
    pub filter_ofac: bool,
    /// Public validator identity that must be active before the connector
    /// starts. When omitted, this is derived from `identity_path` for
    /// backwards compatibility.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub expected_identity: Option<Address>,
    /// Optional file-backed identity source. When omitted, the connector waits
    /// for an identity through its local admin RPC.
    #[serde(default)]
    pub identity_path: Option<PathBuf>,
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
    pub fn take_alert_webhook(&mut self) -> Option<AlertWebhook> {
        self.alert_webhook.take().or_else(|| self.discord_webhook.take().map(AlertWebhook::discord))
    }

    pub fn admin_rpc_path(&self) -> PathBuf {
        self.ledger_path.join("admin.rpc")
    }

    pub fn identity_rpc_path(&self) -> PathBuf {
        self.ledger_path.join("gravity-admin").join("admin.rpc")
    }

    pub fn failsafe_path(&self) -> PathBuf {
        self.ledger_path.join("gravity-admin").join("failsafe.json")
    }

    pub fn scheduler_bindings_path(&self) -> PathBuf {
        self.ledger_path.join("scheduler_bindings.ipc")
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.alert_webhook.is_some() && self.discord_webhook.is_some() {
            return Err("configure only one of alert_webhook or discord_webhook".to_owned());
        }
        if self.identity_path.is_none() && self.expected_identity.is_none() {
            return Err("expected_identity is required when identity_path is omitted".to_owned());
        }
        self.client.validate()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClientConfig {
    Agave(AgaveClientConfig),
    Jito(JitoClientConfig),
}

impl ClientConfig {
    pub const fn variant(&self) -> ClientVariant {
        match self {
            Self::Agave(_) => ClientVariant::Agave,
            Self::Jito(_) => ClientVariant::Jito,
        }
    }

    fn validate(&self) -> Result<(), String> {
        match self {
            Self::Agave(config)
                if config.jito_block_engines.as_ref().is_some_and(Vec::is_empty) =>
            {
                Err("client.agave.jito_block_engines must not be empty when set".to_owned())
            }
            Self::Jito(config) if config.jito_block_engines.as_ref().is_some_and(Vec::is_empty) => {
                Err("client.jito.jito_block_engines must not be empty when set".to_owned())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Default)]
pub struct AgaveClientConfig {
    pub tip_management: Option<TipManagementConfig>,
    pub jito_block_engines: Option<Vec<Url>>,
}

#[serde_as]
#[derive(Default, serde::Deserialize)]
struct RawAgaveClientConfig {
    jito_block_engines: Option<Vec<Url>>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    vote_account_pubkey: Option<Address>,
    rpc_url: Option<String>,
    #[serde(default, with = "env_string")]
    rpc_api_key: Option<String>,
    mev_commission_bps: Option<u16>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    merkle_root_upload_authority: Option<Address>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    tip_distribution_program_pubkey: Option<Address>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    tip_payment_program_pubkey: Option<Address>,
}

impl<'de> Deserialize<'de> for AgaveClientConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawAgaveClientConfig::deserialize(deserializer)?;
        let has_tip_management = raw.jito_block_engines.is_some() ||
            raw.vote_account_pubkey.is_some() ||
            raw.rpc_url.is_some() ||
            raw.rpc_api_key.is_some() ||
            raw.mev_commission_bps.is_some() ||
            raw.merkle_root_upload_authority.is_some() ||
            raw.tip_distribution_program_pubkey.is_some() ||
            raw.tip_payment_program_pubkey.is_some();

        if !has_tip_management {
            return Ok(Self::default());
        }

        let tip_management = TipManagementConfig {
            vote_account_pubkey: raw
                .vote_account_pubkey
                .ok_or_else(|| de::Error::missing_field("vote_account_pubkey"))?,
            rpc_url: raw.rpc_url.ok_or_else(|| de::Error::missing_field("rpc_url"))?,
            rpc_api_key: raw.rpc_api_key,
            mev_commission_bps: raw
                .mev_commission_bps
                .ok_or_else(|| de::Error::missing_field("mev_commission_bps"))?,
            merkle_root_upload_authority: raw
                .merkle_root_upload_authority
                .ok_or_else(|| de::Error::missing_field("merkle_root_upload_authority"))?,
            tip_distribution_program_pubkey: raw
                .tip_distribution_program_pubkey
                .ok_or_else(|| de::Error::missing_field("tip_distribution_program_pubkey"))?,
            tip_payment_program_pubkey: raw
                .tip_payment_program_pubkey
                .ok_or_else(|| de::Error::missing_field("tip_payment_program_pubkey"))?,
        };

        Ok(Self {
            tip_management: Some(tip_management),
            jito_block_engines: raw.jito_block_engines,
        })
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct JitoClientConfig {
    pub block_engine_proxy_addr: SocketAddr,
    pub jito_block_engines: Option<Vec<Url>>,
    #[serde(default)]
    pub shred_receivers: Vec<SocketAddr>,
    #[serde(default)]
    pub shred_retransmit_receivers: Vec<SocketAddr>,
}

#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct TipManagementConfig {
    #[serde_as(as = "DisplayFromStr")]
    pub vote_account_pubkey: Address,
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
