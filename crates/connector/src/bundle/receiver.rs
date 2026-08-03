//! Receives packet and bundle streams from the Jito block engines and routes
//! them to the external builder or local block-engine proxy.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use flux::{
    timing::{Instant, Nanos},
    utils::ArrayStr,
};
use futures_util::StreamExt;
use gravity_protos::block_engine::{
    SubscribeBundlesRequest, SubscribeBundlesResponse, SubscribePacketsRequest,
    SubscribePacketsResponse,
};
use gravity_types::{BlockEngineConnectionError, JitoClient, create_client, make_endpoint};
use solana_keypair::Keypair;
use tokio::{sync::mpsc, time::timeout};
use tonic::{Request, transport::Endpoint};
use tracing::{error, info, warn};
use url::Url;

use crate::{
    bundle::{BlockEngineProxyHandle, CONN_TIMEOUT},
    metrics::BlockEngineMetrics,
};

const MIN_RECONNECT_FREQ: std::time::Duration = std::time::Duration::from_millis(750);

/// Raw block-engine stream message: (response, received at, source URI).
pub enum BlockEngineReceiverMsg {
    Bundles(SubscribeBundlesResponse, Nanos, ArrayStr<64>),
    Packets(SubscribePacketsResponse, Nanos, ArrayStr<64>),
}

pub async fn bundle_receiver_loop(
    block_engine_url: Url,
    identity_kp: Keypair,
    tx: mpsc::Sender<BlockEngineReceiverMsg>,
    builder_is_connected: Arc<AtomicBool>,
    block_engine_proxy: Option<BlockEngineProxyHandle>,
) {
    let endpoint = make_endpoint(&block_engine_url);
    let url = endpoint.uri().to_string();
    let source_uri = ArrayStr::<64>::from_str_truncate(&url);
    let metrics = BlockEngineMetrics::new(&url);
    loop {
        let client = get_client(&endpoint, &identity_kp).await;
        if let Err(err) = bundle_receiver_stream(
            client,
            &url,
            source_uri,
            &tx,
            &builder_is_connected,
            block_engine_proxy.as_ref(),
            &metrics,
        )
        .await
        {
            warn!(%url, ?err, "bundle receiver stream ended, reconnecting");
        }
        metrics.set_connected(false);
        metrics.record_reconnect();
        tokio::time::sleep(MIN_RECONNECT_FREQ).await;
    }
}

async fn get_client(endpoint: &Endpoint, identity_kp: &Keypair) -> JitoClient {
    let start = Instant::now();
    let mut retry = 0;

    info!(url =% endpoint.uri(), "connecting to block engine");

    loop {
        match create_client(endpoint, identity_kp).await {
            Ok(client) => {
                info!(time =% start.elapsed(), retry, "connected to block engine");
                return client;
            }
            Err(err) => {
                warn!(?err, retry, "failed connecting client");
                retry += 1;
                tokio::time::sleep(MIN_RECONNECT_FREQ).await;
            }
        }
    }
}

async fn bundle_receiver_stream(
    mut client: JitoClient,
    url: &str,
    source_uri: ArrayStr<64>,
    tx: &mpsc::Sender<BlockEngineReceiverMsg>,
    builder_is_connected: &AtomicBool,
    block_engine_proxy: Option<&BlockEngineProxyHandle>,
    metrics: &BlockEngineMetrics,
) -> Result<(), BlockEngineConnectionError> {
    let packets_resp =
        timeout(CONN_TIMEOUT, client.subscribe_packets(Request::new(SubscribePacketsRequest {})))
            .await??;
    let mut packets_stream = packets_resp.into_inner();

    let bundles_resp =
        timeout(CONN_TIMEOUT, client.subscribe_bundles(Request::new(SubscribeBundlesRequest {})))
            .await??;
    let mut bundles_stream = bundles_resp.into_inner();

    info!(%url, "bundle-receiver: subscribed to packet and bundle streams");
    metrics.set_connected(true);

    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut bundle_count: u64 = 0;
    let mut packet_count: u64 = 0;

    loop {
        tokio::select! {
            msg = bundles_stream.next() => {
                match msg {
                    Some(Ok(resp)) => {
                        bundle_count += resp.bundles.len() as u64;
                        if builder_is_connected.load(Ordering::Relaxed) {
                            let msg = BlockEngineReceiverMsg::Bundles(resp, Nanos::now(), source_uri);
                            if tx.try_send(msg).is_err() {
                                error!(%url, "bundle-receiver: channel is full! dropping bundles");
                            }
                        } else if let Some(proxy) = block_engine_proxy {
                            proxy.publish_bundles(resp);
                        }
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        warn!(%url, "bundle-receiver: bundle stream closed by remote");
                        return Ok(());
                    }
                }
            }
            msg = packets_stream.next() => {
                match msg {
                    Some(Ok(resp)) => {
                        packet_count += resp.batch.as_ref().map_or(0, |batch| batch.packets.len()) as u64;
                        if builder_is_connected.load(Ordering::Relaxed) {
                            let msg = BlockEngineReceiverMsg::Packets(resp, Nanos::now(), source_uri);
                            if tx.try_send(msg).is_err() {
                                error!(%url, "bundle-receiver: channel is full! dropping packets");
                            }
                        } else if let Some(proxy) = block_engine_proxy {
                            proxy.publish_packets(resp);
                        }
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        warn!(%url, "bundle-receiver: packet stream closed by remote");
                        return Ok(());
                    }
                }
            }
            _ = tick.tick() => {
                if bundle_count > 0 || packet_count > 0 {
                    info!(%url, bundle_count, packet_count, "bundle-receiver: block-engine messages/s");
                    bundle_count = 0;
                    packet_count = 0;
                }
            }
        }
    }
}
