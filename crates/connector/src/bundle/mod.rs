//! Jito block engine integration:
//! - subscribe to block engine bundle streams and forward bundles to the
//!   builder, see `receiver`
//! - port of tip management logic from jito-solana: on every new sequencing
//!   slot, crank the tip program to insert the validator as the fee recipient
//!   of tips. This is handled by [`JitoHandler`] using logic ported from
//!   jito-solana in `tip_manager`, `tip_distribution`, and `tip_payment`
//!
//! Notes
//! - a few methods in [`TipManager`] take the `Bank` as param, which we don't
//!   have access to here, so we use an RPC client instead
//! - crank logic is spread across bundle and worker stage in jito solana, but
//!   done once at the beginning of the slot here
//!
//! In an ideal world, Jito bumps to 4.0.0 and keeps doing all the tip
//! management logic in the node

mod proxy;
mod receiver;
mod tip_distribution;
mod tip_manager;
mod tip_payment;

use gravity_protos::block_engine::{BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse};
pub use gravity_types::AuthInterceptor;
use gravity_types::{BlockEngineConnectionError, create_client, make_endpoint};
pub use proxy::{BlockEngineProxyHandle, spawn_block_engine_proxy};
pub use receiver::{BlockEngineReceiverMsg, block_engine_receiver_loop};
use solana_address::Address;
use solana_commitment_config::CommitmentConfig;
use solana_hash::Hash;
use solana_keypair::Keypair;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_transaction::versioned::VersionedTransaction;
pub use tip_manager::{TipDistributionAccountConfig, TipManager, TipManagerConfig};
use tokio::{sync::mpsc, time::timeout};
use tonic::transport::Endpoint;
use tracing::{error, info, warn};
use url::Url;

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BlockBuilderFeeInfo {
    pub block_builder: Address,
    pub block_builder_commission: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct PrevTipConfig {
    pub tip_receiver: Address,
    pub block_builder: Address,
}

#[derive(Debug, Clone, Copy)]
pub struct CrankTrigger {
    pub slot: u64,
    pub prev: Option<PrevTipConfig>,
}

const CONN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const BLOCK_ENGINE_URLS: [&str; 8] = [
    "https://amsterdam.mainnet.block-engine.jito.wtf",
    "https://dublin.mainnet.block-engine.jito.wtf",
    "https://frankfurt.mainnet.block-engine.jito.wtf",
    "https://london.mainnet.block-engine.jito.wtf",
    "https://ny.mainnet.block-engine.jito.wtf",
    "https://singapore.mainnet.block-engine.jito.wtf",
    "https://slc.mainnet.block-engine.jito.wtf",
    "https://tokyo.mainnet.block-engine.jito.wtf",
];

pub fn default_block_engine_urls() -> Vec<Url> {
    BLOCK_ENGINE_URLS.iter().map(|url| url.parse().unwrap()).collect()
}

pub async fn spawn_bundle_loop(
    block_engine_urls: Vec<Url>,
    identity_kp: Keypair,
    tip_manager: TipManager,
    rpc_url: String,
    crank_bundle_tx: mpsc::Sender<VersionedTransaction>,
    mut crank_trigger_rx: mpsc::Receiver<CrankTrigger>,
) {
    let (refresh_tx, mut refresh_rx) = mpsc::channel(1000);
    tokio::spawn(refresh_loop(
        refresh_tx,
        rpc_url.clone(),
        block_engine_urls,
        identity_kp.insecure_clone(),
        tip_manager.clone(),
    ));

    let mut epoch = None;
    let mut last_blockhash = None;
    let mut block_builder_info = None;

    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::processed());

    loop {
        tokio::select! {
            Some(refresh) = refresh_rx.recv() => {
                match refresh {
                    RefreshInfo::Info(info) => block_builder_info = Some(info),
                    RefreshInfo::Epoch(e) => epoch = Some(e),
                    RefreshInfo::Blockhash(hash) => last_blockhash = Some(hash),
                }
            }

            Some(CrankTrigger { slot, prev }) = crank_trigger_rx.recv() => {
                info!(slot, has_prev = prev.is_some(), "creating crank bundle");

                let Some(info) = block_builder_info else {
                    error!("missing block builder info! can't create crank bundle");
                    continue;
                };

                let Some(epoch) = epoch else {
                    error!("missing epoch! can't create crank bundle");
                    continue;
                };

                let Some(last_blockhash) = last_blockhash else {
                    error!("missing blockhash! can't create crank bundle");
                    continue;
                };

                let result = match prev {
                    Some(prev) => tip_manager.crank_bundle_with_prev_config(
                        &identity_kp,
                        &info,
                        epoch,
                        last_blockhash,
                        &prev.tip_receiver,
                        &prev.block_builder,
                    ),
                    None => {
                        tip_manager
                            .get_tip_programs_crank_bundle(&client, &identity_kp, &info, epoch, last_blockhash)
                            .await
                    }
                };

                match result {
                    Ok(tx) => {
                        if crank_bundle_tx.try_send(tx).is_err() {
                            error!("failed sending crank bundle to bundle tile");
                        }
                    }

                    Err(err) => {
                        error!(?err, "failed to create crank bundle");
                    }
                }
            }
        }
    }
}

enum RefreshInfo {
    Info(BlockBuilderFeeInfo),
    Epoch(u64),
    Blockhash(Hash),
}

async fn refresh_loop(
    refresh_tx: mpsc::Sender<RefreshInfo>,
    rpc_url: String,
    block_engine_urls: Vec<Url>,
    identity_kp: Keypair,
    tip_manager: TipManager,
) {
    const MAINTENANCE_TICK: std::time::Duration = std::time::Duration::from_mins(10);
    const FREQUENT_TICK: std::time::Duration = std::time::Duration::from_secs(15);

    let endpoints = block_engine_urls
        .iter()
        .map(|url| (url.to_string(), make_endpoint(url)))
        .collect::<Vec<_>>();

    let mut frequent_tick = tokio::time::interval(FREQUENT_TICK);
    let mut maintenance_tick = tokio::time::interval(MAINTENANCE_TICK);
    let client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::finalized());

    let mut epoch = None;
    let mut last_blockhash = None;
    let mut has_block_builder_info = false;

    loop {
        tokio::select! {
            _ = frequent_tick.tick() => {
                match client.get_latest_blockhash().await {
                    Ok(blockhash) => {
                        if let Err(err) = refresh_tx.try_send(RefreshInfo::Blockhash(blockhash)) {
                            error!(?err, "failed sending blockhash");
                        }
                        last_blockhash = Some(blockhash);
                    },

                    Err(err) => {
                        error!(?err, "failed to fetch blockhash");
                    }
                }

                match client.get_epoch_info().await {
                    Ok(e) => {
                        if let Err(err) = refresh_tx.try_send(RefreshInfo::Epoch(e.epoch)) {
                            error!(?err, "failed sending epoch");
                        }
                        epoch = Some(e.epoch);
                    },

                    Err(err) => {
                        error!(?err, "failed to fetch epoch");
                    }
                }

                if !has_block_builder_info {
                    has_block_builder_info = refresh_block_builder_info(
                        Some(&refresh_tx),
                        &endpoints,
                        &identity_kp,
                        None,
                    ).await;
                }
            }


            _ = maintenance_tick.tick() => {
                info!("refreshing block builder info");
                has_block_builder_info = refresh_block_builder_info(
                    Some(&refresh_tx),
                    &endpoints,
                    &identity_kp,
                    None,
                ).await || has_block_builder_info;

                let Some(epoch) = epoch else {
                    warn!("missing epoch! can't init tip receiver");
                    continue;
                };

                let Some(last_blockhash) = last_blockhash else {
                    warn!("missing blockhash! can't init tip receiver");
                    continue;
                };

                if let Err(err) = tip_manager.check_init_once(&client, &identity_kp, epoch, last_blockhash).await {
                     error!(?err, "failed init tip receiver");
                }
            }
        }
    }
}

pub async fn spawn_block_builder_info_loop(
    block_engine_urls: Vec<Url>,
    identity_kp: Keypair,
    block_engine_proxy: BlockEngineProxyHandle,
) {
    const MAINTENANCE_TICK: std::time::Duration = std::time::Duration::from_mins(10);
    const RETRY_TICK: std::time::Duration = std::time::Duration::from_secs(15);

    let endpoints = block_engine_urls
        .iter()
        .map(|url| (url.to_string(), make_endpoint(url)))
        .collect::<Vec<_>>();
    let mut retry_tick = tokio::time::interval(RETRY_TICK);
    let mut maintenance_tick = tokio::time::interval(MAINTENANCE_TICK);
    let mut has_block_builder_info = false;

    loop {
        tokio::select! {
            _ = retry_tick.tick(), if !has_block_builder_info => {
                has_block_builder_info = refresh_block_builder_info(
                    None,
                    &endpoints,
                    &identity_kp,
                    Some(&block_engine_proxy),
                ).await;
            }

            _ = maintenance_tick.tick() => {
                info!("refreshing block builder info");
                has_block_builder_info = refresh_block_builder_info(
                    None,
                    &endpoints,
                    &identity_kp,
                    Some(&block_engine_proxy),
                ).await || has_block_builder_info;
            }
        }
    }
}

async fn refresh_block_builder_info(
    refresh_tx: Option<&mpsc::Sender<RefreshInfo>>,
    endpoints: &[(String, Endpoint)],
    identity_kp: &Keypair,
    block_engine_proxy: Option<&BlockEngineProxyHandle>,
) -> bool {
    match fetch_any_block_builder_info(endpoints, identity_kp).await {
        Ok(new_info) => {
            info!(?new_info, "new block builder info");
            if let Some(proxy) = block_engine_proxy {
                proxy.set_block_builder_info(new_info);
            }
            if let Some(refresh_tx) = refresh_tx &&
                let Err(err) = refresh_tx.try_send(RefreshInfo::Info(new_info))
            {
                error!(?err, "failed sending builder info");
            }
            true
        }
        Err(err) => {
            error!(?err, "failed to refresh block builder info");
            false
        }
    }
}

async fn fetch_any_block_builder_info(
    endpoints: &[(String, Endpoint)],
    identity_kp: &Keypair,
) -> Result<BlockBuilderFeeInfo, BlockEngineConnectionError> {
    let mut last_err = None;
    for (url, endpoint) in endpoints {
        match fetch_block_builder_info(endpoint, identity_kp).await {
            Ok(info) => return Ok(info),
            Err(err) => {
                warn!(%url, ?err, "failed to fetch block builder info from upstream");
                last_err = Some(err);
            }
        }
    }

    Err(last_err.unwrap_or(BlockEngineConnectionError::InvalidBuilderPubkey(
        "no block engine URLs configured".to_owned(),
    )))
}

async fn fetch_block_builder_info(
    endpoint: &Endpoint,
    identity_kp: &Keypair,
) -> Result<BlockBuilderFeeInfo, BlockEngineConnectionError> {
    let mut client = create_client(endpoint, identity_kp).await?;
    let info: BlockBuilderFeeInfoResponse =
        timeout(CONN_TIMEOUT, client.get_block_builder_fee_info(BlockBuilderFeeInfoRequest {}))
            .await??
            .into_inner();

    let Ok(pubkey) = info.pubkey.parse() else {
        return Err(BlockEngineConnectionError::InvalidBuilderPubkey(info.pubkey));
    };

    let msg =
        BlockBuilderFeeInfo { block_builder: pubkey, block_builder_commission: info.commission };

    Ok(msg)
}
