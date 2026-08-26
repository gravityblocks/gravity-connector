mod admin_rpc;
mod bridge;
pub mod bundle;
mod cache;
mod config;
mod dispatch;
mod domain;
mod messages;
pub mod metrics;
mod network;
mod worker_to_pack;

use std::{
    fs,
    io::ErrorKind,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

pub use admin_rpc::{
    IdentityRpcServer, monitor_identity, set_shred_receiver_addresses,
    set_shred_retransmit_receiver_addresses, wait_for_expected_identity, wait_for_expected_keypair,
};
pub use bridge::ConnectorTile;
pub use bundle::*;
pub use config::{
    AgaveClientConfig, ClientConfig, ClientVariant, Config, JitoClientConfig, RelayEndpoint,
    TipManagementConfig,
};
use flux::{timing::Nanos, utils::directories::local_share_dir};
pub use messages::*;
pub use network::{
    MAX_SHRED_RECEIVER_ADDRESSES, Network, RESERVED_RELAY_SHRED_RECEIVERS, dedup_shred_receivers,
};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec_pretty};
use tracing::error;

pub const APP_NAME: &str = "gravity-connector";

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum FailsafeReason {
    NoScheduled,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Failsafe {
    pub timestamp: Nanos,
    pub reason: FailsafeReason,
}
impl Failsafe {
    const KILL_DURATION: Nanos = Nanos::from_hours(12);

    pub fn write_no_schedule() {
        Self { timestamp: Nanos::now(), reason: FailsafeReason::NoScheduled }.write();
    }
    pub fn is_expired(&self) -> bool {
        self.timestamp.elapsed_saturating() >= Self::KILL_DURATION
    }
    pub fn remove() {
        if let Err(err) = fs::remove_file(Self::path()) {
            if err.kind() != ErrorKind::NotFound {
                error!(?err, "failed removing failsafe");
            }
        }
    }
    pub fn read() -> Option<Self> {
        let file = match fs::read(Self::path()) {
            Ok(file) => file,
            Err(err) => {
                if err.kind() != ErrorKind::NotFound {
                    error!(?err, "Failed reading Failsafe file!");
                }
                return None;
            }
        };
        match from_slice::<Self>(&file) {
            Ok(val) => Some(val),
            Err(err) => {
                error!(?err, "Failed parsing failsafe");
                None
            }
        }
    }
    fn write(self) {
        let serialized = match to_vec_pretty(&self) {
            Ok(ser) => ser,
            Err(err) => {
                error!(?err, "Failed serializing failsafe info!");
                return;
            }
        };
        if let Err(err) = fs::write(Self::path(), serialized) {
            error!(?err, "failed writing failsafe");
        }
    }
    pub fn path() -> PathBuf {
        local_share_dir().join(APP_NAME).join("failsafe.json")
    }
}

#[allow(nonstandard_style)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum StopCodes {
    CONTINUE = 0,
    SIGTERM = signal_hook::consts::SIGTERM as usize,
    SIGINT = signal_hook::consts::SIGINT as usize,
    SIGQUIT = signal_hook::consts::SIGQUIT as usize,
    AGAVE_NO_PROGRESS = 128,
    AGAVE_IDENTITY_MISMATCH = 129,
    UNKNOWN = 255,
}

impl StopCodes {
    pub fn poll(flag: &AtomicUsize) -> Option<Self> {
        match Self::from(flag.load(Ordering::Relaxed)) {
            Self::CONTINUE => None,
            code => Some(code),
        }
    }

    pub fn running(flag: &AtomicUsize) -> bool {
        Self::poll(flag).is_none()
    }
}

impl From<usize> for StopCodes {
    fn from(value: usize) -> Self {
        match value {
            x if x == Self::CONTINUE as usize => Self::CONTINUE,
            x if x == Self::SIGTERM as usize => Self::SIGTERM,
            x if x == Self::SIGINT as usize => Self::SIGINT,
            x if x == Self::SIGQUIT as usize => Self::SIGQUIT,
            x if x == Self::AGAVE_NO_PROGRESS as usize => Self::AGAVE_NO_PROGRESS,
            x if x == Self::AGAVE_IDENTITY_MISMATCH as usize => Self::AGAVE_IDENTITY_MISMATCH,
            _ => Self::UNKNOWN,
        }
    }
}
