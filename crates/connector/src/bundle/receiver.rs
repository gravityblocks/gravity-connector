//! Receives packet and bundle streams from the Jito block engines and routes
//! them to the external builder or local block-engine proxy.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use async_stream::stream;
use flux::{
    timing::{Instant, Nanos},
    utils::ArrayStr,
};
use futures_util::{Stream, StreamExt, stream::SelectAll};
use gravity_protos::block_engine::{
    SubscribeBundlesRequest, SubscribeBundlesResponse, SubscribePacketsRequest,
    SubscribePacketsResponse,
};
use gravity_types::{
    BlockEngineConnectionError, BundleId, JitoClient, SigPrefix, create_client, make_endpoint,
};
use rtrb::{Producer, PushError};
use rustc_hash::FxHashSet;
use solana_keypair::Keypair;
use tokio::time::timeout;
use tonic::{Request, Streaming, transport::Endpoint};
use tracing::{error, info, warn};
use url::Url;

use crate::{
    bundle::{BlockEngineProxyHandle, CONN_TIMEOUT},
    metrics::BlockEngineMetrics,
    network::packet_sig_prefix,
};

const MIN_RECONNECT_FREQ: std::time::Duration = std::time::Duration::from_millis(750);

type EndpointStream = Pin<Box<dyn Stream<Item = BlockEngineReceiverMsg> + Send>>;

/// Raw block-engine stream message: (response, received at, source URI).
pub enum BlockEngineReceiverMsg {
    Bundles(SubscribeBundlesResponse, Nanos, ArrayStr<64>),
    Packets(SubscribePacketsResponse, Nanos, ArrayStr<64>),
}

impl BlockEngineReceiverMsg {
    fn source_uri(&self) -> ArrayStr<64> {
        match self {
            Self::Bundles(_, _, source_uri) | Self::Packets(_, _, source_uri) => *source_uri,
        }
    }
}

/// Polls every block-engine endpoint from one Tokio task, removes duplicates
/// shared by Jito regions, and forwards one filtered stream to the bridge.
pub async fn block_engine_receiver_loop(
    block_engine_urls: Vec<Url>,
    identity_kp: Keypair,
    mut tx: Producer<BlockEngineReceiverMsg>,
    builder_is_connected: Arc<AtomicBool>,
    block_engine_proxy: Option<BlockEngineProxyHandle>,
    dedup_epoch: Arc<AtomicU64>,
) {
    let mut streams = SelectAll::new();
    for url in block_engine_urls {
        streams.push(block_engine_endpoint_stream(&url, identity_kp.insecure_clone()));
    }

    let mut seen_packets = FxHashSet::<SigPrefix>::default();
    let mut seen_bundles = FxHashSet::<BundleId>::default();
    let mut dup_packets_dropped = 0_u64;
    let mut dup_bundles_dropped = 0_u64;
    let mut current_epoch = dedup_epoch.load(Ordering::Relaxed);

    while let Some(mut msg) = streams.next().await {
        if !builder_is_connected.load(Ordering::Relaxed) {
            if let Some(proxy) = block_engine_proxy.as_ref() {
                match msg {
                    BlockEngineReceiverMsg::Bundles(resp, _, _) => proxy.publish_bundles(resp),
                    BlockEngineReceiverMsg::Packets(resp, _, _) => proxy.publish_packets(resp),
                }
            }
            continue;
        }

        let next_epoch = dedup_epoch.load(Ordering::Relaxed);
        if current_epoch != next_epoch {
            info!(
                packets = seen_packets.len(),
                bundles = seen_bundles.len(),
                dup_packets_dropped,
                dup_bundles_dropped,
                "clearing block-engine receiver dedup sets"
            );
            seen_packets.clear();
            seen_bundles.clear();
            dup_packets_dropped = 0;
            dup_bundles_dropped = 0;
            current_epoch = next_epoch;
        }

        let should_forward = match &mut msg {
            BlockEngineReceiverMsg::Bundles(resp, _, _) => {
                dedup_bundles(resp, &mut seen_bundles, &mut dup_bundles_dropped)
            }
            BlockEngineReceiverMsg::Packets(resp, _, _) => {
                dedup_packets(resp, &mut seen_packets, &mut dup_packets_dropped)
            }
        };
        if !should_forward {
            continue;
        }

        if let Err(PushError::Full(msg)) = tx.push(msg) {
            error!(source_uri = %msg.source_uri(), "block-engine master channel is full! dropping response");
        }
    }
}

fn dedup_packets(
    resp: &mut SubscribePacketsResponse,
    seen: &mut FxHashSet<SigPrefix>,
    duplicates: &mut u64,
) -> bool {
    let Some(batch) = resp.batch.as_mut() else { return false };
    let before = batch.packets.len();
    batch.packets.retain(|packet| {
        let Some(sig_prefix) = packet_sig_prefix(packet) else { return true };
        seen.insert(sig_prefix)
    });
    *duplicates += (before - batch.packets.len()) as u64;
    !batch.packets.is_empty()
}

fn dedup_bundles(
    resp: &mut SubscribeBundlesResponse,
    seen: &mut FxHashSet<BundleId>,
    duplicates: &mut u64,
) -> bool {
    let before = resp.bundles.len();
    resp.bundles.retain(|bundle| {
        let Some(bundle_id) = BundleId::from_hex(&bundle.uuid) else { return true };
        seen.insert(bundle_id)
    });
    *duplicates += (before - resp.bundles.len()) as u64;
    !resp.bundles.is_empty()
}

fn block_engine_endpoint_stream(block_engine_url: &Url, identity_kp: Keypair) -> EndpointStream {
    let endpoint = make_endpoint(block_engine_url);
    let url = endpoint.uri().to_string();
    let source_uri = ArrayStr::<64>::from_str_truncate(&url);
    let metrics = BlockEngineMetrics::new(&url);

    Box::pin(stream! {
        loop {
            let client = get_client(&endpoint, &identity_kp).await;
            match subscribe_streams(client).await {
                Ok((mut packets_stream, mut bundles_stream)) => {
                    info!(%url, "bundle-receiver: subscribed to packet and bundle streams");
                    metrics.set_connected(true);
                    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
                    let mut bundle_count = 0_u64;
                    let mut packet_count = 0_u64;

                    loop {
                        tokio::select! {
                            msg = bundles_stream.next() => {
                                match msg {
                                    Some(Ok(resp)) => {
                                        bundle_count += resp.bundles.len() as u64;
                                        yield BlockEngineReceiverMsg::Bundles(resp, Nanos::now(), source_uri);
                                    }
                                    Some(Err(err)) => {
                                        warn!(%url, ?err, "bundle-receiver: bundle stream failed");
                                        break;
                                    }
                                    None => {
                                        warn!(%url, "bundle-receiver: bundle stream closed by remote");
                                        break;
                                    }
                                }
                            }
                            msg = packets_stream.next() => {
                                match msg {
                                    Some(Ok(resp)) => {
                                        packet_count += resp.batch.as_ref().map_or(0, |batch| batch.packets.len()) as u64;
                                        yield BlockEngineReceiverMsg::Packets(resp, Nanos::now(), source_uri);
                                    }
                                    Some(Err(err)) => {
                                        warn!(%url, ?err, "bundle-receiver: packet stream failed");
                                        break;
                                    }
                                    None => {
                                        warn!(%url, "bundle-receiver: packet stream closed by remote");
                                        break;
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
                Err(err) => warn!(%url, ?err, "bundle receiver stream ended, reconnecting"),
            }
            metrics.set_connected(false);
            metrics.record_reconnect();
            tokio::time::sleep(MIN_RECONNECT_FREQ).await;
        }
    })
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

async fn subscribe_streams(
    mut client: JitoClient,
) -> Result<
    (Streaming<SubscribePacketsResponse>, Streaming<SubscribeBundlesResponse>),
    BlockEngineConnectionError,
> {
    let packets_resp =
        timeout(CONN_TIMEOUT, client.subscribe_packets(Request::new(SubscribePacketsRequest {})))
            .await??;
    let bundles_resp =
        timeout(CONN_TIMEOUT, client.subscribe_bundles(Request::new(SubscribeBundlesRequest {})))
            .await??;
    Ok((packets_resp.into_inner(), bundles_resp.into_inner()))
}
