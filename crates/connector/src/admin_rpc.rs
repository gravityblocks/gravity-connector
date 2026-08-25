//! Minimal client for the validator's local admin RPC and the connector's
//! in-memory identity RPC.

use std::{
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{Receiver, RecvTimeoutError, Sender, channel},
    },
    time::Duration,
};
#[cfg(unix)]
use std::{os::unix::fs::FileTypeExt, os::unix::net::UnixStream};

use gravity_types::runtime::background_runtime;
use jsonrpc_core::{IoHandler, Params, Result, Value};
use jsonrpc_core_client::{RpcError, transports::ipc};
use jsonrpc_derive::rpc;
use jsonrpc_ipc_server::{SecurityAttributes, Server, ServerBuilder};
use serde::{Deserialize, Serialize};
use solana_address::Address;
use solana_keypair::{Keypair, read_keypair_file};
use solana_signer::Signer;
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

    #[rpc(meta, name = "setShredRetransmitReceiverAddress")]
    fn set_shred_retransmit_receiver_address(
        &self,
        meta: Self::Metadata,
        addr: String,
    ) -> Result<()>;
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

#[derive(Debug, thiserror::Error)]
enum KeypairPollError {
    #[error("failed to read keypair: {0}")]
    Read(String),
    #[error("keypair identity is {actual}, expected {expected}")]
    UnexpectedIdentity { expected: Address, actual: Address },
}

fn read_expected_keypair(
    identity_path: &Path,
    expected: Address,
) -> std::result::Result<Keypair, KeypairPollError> {
    let keypair =
        read_keypair_file(identity_path).map_err(|err| KeypairPollError::Read(err.to_string()))?;
    let actual = keypair.pubkey();
    if actual != expected {
        return Err(KeypairPollError::UnexpectedIdentity { expected, actual });
    }
    Ok(keypair)
}

struct IdentityClient {
    admin_rpc_path: PathBuf,
    client: Option<gen_client::Client>,
}

/// Owner of the connector's minimal, Agave-compatible identity RPC server.
///
/// Dropping this value closes the server and removes its socket.
pub struct IdentityRpcServer {
    _server: Server,
    path: PathBuf,
    expected: Address,
    identity_rx: Receiver<Keypair>,
}

impl IdentityRpcServer {
    pub fn start(path: PathBuf, expected: Address) -> io::Result<Self> {
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            match fs::create_dir(parent) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists && parent.is_dir() => {}
                Err(err) => return Err(err),
            }
        }
        reject_non_socket_path(&path)?;

        let (identity_tx, identity_rx) = channel();
        let accepted = AtomicBool::new(false);
        let mut io = IoHandler::new();
        io.add_sync_method("setIdentityFromBytes", move |params| {
            set_identity_from_bytes(expected, &identity_tx, &accepted, params)
        });

        let server = ServerBuilder::new(io)
            .set_security_attributes(SecurityAttributes::empty())
            .event_loop_executor(background_runtime().handle().clone())
            .start(&path.display().to_string())?;
        info!(%expected, path = %path.display(), "started identity RPC server");

        Ok(Self { _server: server, path, expected, identity_rx })
    }

    /// Wait for a matching identity to be injected, or for a stop signal.
    pub fn wait_for_identity(&self, stop: &AtomicUsize) -> Option<Keypair> {
        loop {
            if !StopCodes::running(stop) {
                return None;
            }
            match self.identity_rx.recv_timeout(IDENTITY_POLL_INTERVAL) {
                Ok(keypair) => {
                    info!(
                        expected = %self.expected,
                        path = %self.path.display(),
                        "in-memory identity matches config"
                    );
                    return Some(keypair);
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("identity RPC keypair channel disconnected")
                }
            }
        }
    }
}

fn set_identity_from_bytes(
    expected: Address,
    identity_tx: &Sender<Keypair>,
    accepted: &AtomicBool,
    params: Params,
) -> Result<Value> {
    let identity_bytes = parse_identity_params(params)?;
    let keypair = Keypair::try_from(identity_bytes.as_slice());
    let keypair = keypair.map_err(|err| {
        jsonrpc_core::Error::invalid_params(format!(
            "Failed to read identity keypair from provided byte array: {err}"
        ))
    })?;
    let actual = keypair.pubkey();
    if actual != expected {
        return Err(jsonrpc_core::Error::invalid_params(format!(
            "Identity keypair pubkey is {actual}, expected {expected}"
        )));
    }

    if accepted.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_err() {
        debug!(identity = %actual, "in-memory identity already loaded");
        return Ok(Value::Null);
    }
    if identity_tx.send(keypair).is_err() {
        accepted.store(false, Ordering::Relaxed);
        return Err(jsonrpc_core::Error::internal_error());
    }
    info!(identity = %actual, "received in-memory identity");
    Ok(Value::Null)
}

fn parse_identity_params(params: Params) -> Result<Vec<u8>> {
    let Params::Array(mut params) = params else {
        return Err(jsonrpc_core::Error::invalid_params("Expected positional parameters"));
    };
    if !(1..=3).contains(&params.len()) || params.iter().skip(1).any(|value| !value.is_boolean()) {
        return Err(jsonrpc_core::Error::invalid_params(
            "Expected identity bytes followed by up to two booleans",
        ));
    }

    let Value::Array(bytes) = params.remove(0) else {
        return Err(jsonrpc_core::Error::invalid_params("Expected identity byte array"));
    };
    bytes
        .into_iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|byte| u8::try_from(byte).ok())
                .ok_or_else(|| jsonrpc_core::Error::invalid_params("Invalid identity byte array"))
        })
        .collect()
}

fn reject_non_socket_path(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_socket() => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("refusing to replace non-socket path {}", path.display()),
            ));
        }
        Ok(_) => match UnixStream::connect(path) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("identity RPC socket is already in use at {}", path.display()),
                ));
            }
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                ) => {}
            Err(err) => return Err(err),
        },
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    Ok(())
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

/// Wait until the keypair file contains the expected connector identity.
/// Returns `None` when a stop signal arrives before a matching keypair is
/// available.
pub async fn wait_for_expected_keypair(
    identity_path: PathBuf,
    expected: Address,
    stop: Arc<AtomicUsize>,
) -> Option<Keypair> {
    let mut ticker = interval(IDENTITY_POLL_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut failure_streak = 0u64;
    let mut last_mismatch = None;

    loop {
        ticker.tick().await;
        if !StopCodes::running(&stop) {
            return None;
        }

        match read_expected_keypair(&identity_path, expected) {
            Ok(keypair) => {
                info!(%expected, path = %identity_path.display(), "identity keypair matches config");
                return Some(keypair);
            }
            Err(KeypairPollError::UnexpectedIdentity { actual, .. }) => {
                failure_streak = 0;
                if last_mismatch == Some(actual) {
                    debug!(%expected, %actual, "identity keypair still does not match config");
                } else {
                    warn!(%expected, %actual, path = %identity_path.display(), "identity keypair does not match config; waiting");
                    last_mismatch = Some(actual);
                }
            }
            Err(err @ KeypairPollError::Read(_)) => {
                last_mismatch = None;
                if failure_streak == 0 {
                    warn!(?err, path = %identity_path.display(), "failed to read identity keypair; waiting");
                } else {
                    debug!(?err, streak = failure_streak, "still unable to read identity keypair");
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

pub async fn set_shred_retransmit_receiver_addresses(
    admin_rpc_path: PathBuf,
    addresses: Vec<SocketAddr>,
) {
    let rpc_addresses = addresses.iter().map(SocketAddr::to_string).collect::<Vec<_>>().join(",");
    let result = timeout(ADMIN_RPC_TIMEOUT, async {
        connect(&admin_rpc_path).await?.set_shred_retransmit_receiver_address(rpc_addresses).await
    })
    .await;
    match result {
        Ok(Ok(())) => info!(?addresses, "updated validator shred retransmit receiver addresses"),
        Ok(Err(err)) => warn!(
            ?err,
            ?addresses,
            path = %admin_rpc_path.display(),
            "failed to update validator shred retransmit receiver addresses"
        ),
        Err(_) => warn!(
            ?addresses,
            path = %admin_rpc_path.display(),
            "timed out updating validator shred retransmit receiver addresses"
        ),
    }
}
