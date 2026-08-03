//! Minimal client for the validator's local admin RPC.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use jsonrpc_core::Result;
use jsonrpc_core_client::{RpcError, transports::ipc};
use jsonrpc_derive::rpc;
use serde::{Deserialize, Serialize};
use solana_address::Address;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tracing::{debug, info, warn};

use crate::StopCodes;

const IDENTITY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const ADMIN_RPC_TIMEOUT: Duration = Duration::from_secs(1);

#[allow(dead_code)]
#[rpc]
pub trait AdminRpc {
    type Metadata;

    #[rpc(meta, name = "contactInfo")]
    fn contact_info(&self, meta: Self::Metadata) -> Result<AdminContactInfo>;

    #[rpc(meta, name = "setShredReceiverAddress")]
    fn set_shred_receiver_address(&self, meta: Self::Metadata, addr: String) -> Result<()>;
}

/// Partial view of Agave's `AdminRpcContactInfo` response.
#[derive(Debug, Deserialize, Serialize)]
pub struct AdminContactInfo {
    id: String,
}

#[derive(Debug, thiserror::Error)]
enum IdentityPollError {
    #[error("admin RPC request timed out after {ADMIN_RPC_TIMEOUT:?}")]
    Timeout,
    #[error("admin RPC request failed: {0}")]
    Rpc(#[from] RpcError),
    #[error("admin RPC returned invalid identity {identity}: {error}")]
    InvalidIdentity { identity: String, error: String },
}

struct IdentityClient {
    admin_rpc_path: PathBuf,
    client: Option<gen_client::Client>,
}

impl IdentityClient {
    fn new(admin_rpc_path: PathBuf) -> Self {
        Self { admin_rpc_path, client: None }
    }

    async fn current_identity(&mut self) -> std::result::Result<Address, IdentityPollError> {
        if let Ok(result) = timeout(ADMIN_RPC_TIMEOUT, self.current_identity_inner()).await {
            result
        } else {
            self.client = None;
            Err(IdentityPollError::Timeout)
        }
    }

    async fn current_identity_inner(&mut self) -> std::result::Result<Address, IdentityPollError> {
        if self.client.is_none() {
            self.client = Some(connect(&self.admin_rpc_path).await?);
        }

        let result = self.client.as_ref().expect("client connected above").contact_info().await;
        let info = match result {
            Ok(info) => info,
            Err(err) => {
                self.client = None;
                return Err(err.into());
            }
        };

        info.id.parse::<Address>().map_err(|err| IdentityPollError::InvalidIdentity {
            identity: info.id,
            error: err.to_string(),
        })
    }
}

async fn connect(admin_rpc_path: &Path) -> std::result::Result<gen_client::Client, RpcError> {
    if !admin_rpc_path.exists() {
        return Err(RpcError::Client(format!("{} does not exist", admin_rpc_path.display())));
    }
    ipc::connect::<_, gen_client::Client>(&admin_rpc_path.display().to_string()).await
}

/// Wait until Agave's live identity matches the configured connector identity.
/// Returns false when a stop signal arrives before a match.
pub async fn wait_for_expected_identity(
    admin_rpc_path: PathBuf,
    expected: Address,
    stop: Arc<AtomicUsize>,
) -> bool {
    let mut client = IdentityClient::new(admin_rpc_path.clone());
    let mut ticker = interval(IDENTITY_POLL_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut failure_streak = 0u64;
    let mut last_mismatch = None;

    loop {
        ticker.tick().await;
        if !StopCodes::running(&stop) {
            return false;
        }

        match client.current_identity().await {
            Ok(actual) if actual == expected => {
                info!(%expected, path = %admin_rpc_path.display(), "validator identity matches config");
                return true;
            }
            Ok(actual) => {
                failure_streak = 0;
                if last_mismatch == Some(actual) {
                    debug!(%expected, %actual, "validator identity still does not match config");
                } else {
                    warn!(%expected, %actual, "validator identity does not match config; waiting");
                    last_mismatch = Some(actual);
                }
            }
            Err(err) => {
                last_mismatch = None;
                if failure_streak == 0 {
                    warn!(?err, path = %admin_rpc_path.display(), "failed to read validator identity; waiting");
                } else {
                    debug!(
                        ?err,
                        streak = failure_streak,
                        "still unable to read validator identity"
                    );
                }
                failure_streak = failure_streak.saturating_add(1);
            }
        }
    }
}

/// Poll Agave's identity while the connector runs and request a graceful stop
/// if it changes away from the configured identity.
pub async fn monitor_identity(admin_rpc_path: PathBuf, expected: Address, stop: Arc<AtomicUsize>) {
    let mut client = IdentityClient::new(admin_rpc_path.clone());
    let mut ticker = interval(IDENTITY_POLL_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut failure_streak = 0u64;

    loop {
        ticker.tick().await;
        if !StopCodes::running(&stop) {
            return;
        }

        match client.current_identity().await {
            Ok(actual) if actual == expected => failure_streak = 0,
            Ok(actual) => {
                warn!(%expected, %actual, "validator identity changed; stopping connector");
                let _ = stop.compare_exchange(
                    StopCodes::CONTINUE as usize,
                    StopCodes::AGAVE_IDENTITY_MISMATCH as usize,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
                return;
            }
            Err(err) => {
                if failure_streak == 0 {
                    warn!(?err, path = %admin_rpc_path.display(), "failed to monitor validator identity");
                } else {
                    debug!(
                        ?err,
                        streak = failure_streak,
                        "still unable to monitor validator identity"
                    );
                }
                failure_streak = failure_streak.saturating_add(1);
            }
        }
    }
}

pub async fn set_shred_receiver_addresses(admin_rpc_path: PathBuf, addresses: Vec<SocketAddr>) {
    let rpc_addresses = addresses.iter().map(SocketAddr::to_string).collect::<Vec<_>>().join(",");
    let result = timeout(ADMIN_RPC_TIMEOUT, async {
        connect(&admin_rpc_path).await?.set_shred_receiver_address(rpc_addresses).await
    })
    .await;
    match result {
        Ok(Ok(())) => info!(?addresses, "updated validator shred receiver addresses"),
        Ok(Err(err)) => warn!(
            ?err,
            ?addresses,
            path = %admin_rpc_path.display(),
            "failed to update validator shred receiver addresses"
        ),
        Err(_) => warn!(
            ?addresses,
            path = %admin_rpc_path.display(),
            "timed out updating validator shred receiver addresses"
        ),
    }
}
