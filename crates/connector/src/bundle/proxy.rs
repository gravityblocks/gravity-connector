use std::{
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use gravity_protos::{
    auth::{
        GenerateAuthChallengeRequest, GenerateAuthChallengeResponse, GenerateAuthTokensRequest,
        GenerateAuthTokensResponse, RefreshAccessTokenRequest, RefreshAccessTokenResponse, Token,
        auth_service_server::{AuthService, AuthServiceServer},
    },
    block_engine::{
        BlockBuilderFeeInfoResponse, BlockEngineEndpoint, GetBlockEngineEndpointRequest,
        GetBlockEngineEndpointResponse, SubscribeBundlesRequest, SubscribeBundlesResponse,
        SubscribePacketsRequest, SubscribePacketsResponse,
        block_engine_validator_server::{BlockEngineValidator, BlockEngineValidatorServer},
    },
};
use prost_types::Timestamp;
use rustc_hash::FxHashSet;
use tonic::{Request, Response, Status, transport::Server};
use tracing::{error, info, warn};

use crate::{bundle::BlockBuilderFeeInfo, network::packet_sig_prefix};

const TOKEN_TTL_SECS: i64 = 24 * 60 * 60;
type PacketStream = Pin<
    Box<
        dyn tonic::codegen::tokio_stream::Stream<Item = Result<SubscribePacketsResponse, Status>>
            + Send,
    >,
>;
type BundleStream = Pin<
    Box<
        dyn tonic::codegen::tokio_stream::Stream<Item = Result<SubscribeBundlesResponse, Status>>
            + Send,
    >,
>;

#[derive(Clone)]
pub struct BlockEngineProxyHandle {
    state: Arc<BlockEngineProxyState>,
}

impl BlockEngineProxyHandle {
    pub fn new(listen_addr: SocketAddr) -> Self {
        let advertised_url = format!("http://{listen_addr}");
        Self {
            state: Arc::new(BlockEngineProxyState {
                listen_addr,
                advertised_url,
                block_builder_info: RwLock::new(None),
                packet_tx: tokio::sync::broadcast::channel(4096).0,
                bundle_tx: tokio::sync::broadcast::channel(4096).0,
                dedup_epoch: AtomicU64::new(0),
            }),
        }
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.state.listen_addr
    }

    pub fn advertised_url(&self) -> &str {
        &self.state.advertised_url
    }

    pub fn set_block_builder_info(&self, info: BlockBuilderFeeInfo) {
        let response = BlockBuilderFeeInfoResponse {
            pubkey: info.block_builder.to_string(),
            commission: info.block_builder_commission,
        };
        *self.state.block_builder_info.write().unwrap() = Some(response);
    }

    pub fn publish_bundles(&self, bundles: SubscribeBundlesResponse) {
        let _ = self.state.bundle_tx.send(bundles);
    }

    pub fn publish_packets(&self, packets: SubscribePacketsResponse) {
        let _ = self.state.packet_tx.send(packets);
    }

    pub fn bump_epoch_counter(&self) {
        self.state.dedup_epoch.fetch_add(1, Ordering::Relaxed);
    }
}

struct BlockEngineProxyState {
    listen_addr: SocketAddr,
    advertised_url: String,
    block_builder_info: RwLock<Option<BlockBuilderFeeInfoResponse>>,
    packet_tx: tokio::sync::broadcast::Sender<SubscribePacketsResponse>,
    bundle_tx: tokio::sync::broadcast::Sender<SubscribeBundlesResponse>,
    dedup_epoch: AtomicU64,
}

pub async fn spawn_block_engine_proxy(handle: BlockEngineProxyHandle) {
    let addr = handle.listen_addr();
    let advertised_url = handle.advertised_url().to_owned();
    info!(%addr, %advertised_url, "starting local block-engine proxy");

    let block_engine = ConnectorBlockEngine { state: handle.state.clone() };

    if let Err(err) = Server::builder()
        .add_service(AuthServiceServer::new(ConnectorAuthService))
        .add_service(BlockEngineValidatorServer::new(block_engine))
        .serve(addr)
        .await
    {
        error!(?err, "local block-engine proxy exited");
    }
}

struct ConnectorAuthService;

#[tonic::async_trait]
impl AuthService for ConnectorAuthService {
    async fn generate_auth_challenge(
        &self,
        _request: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        Ok(Response::new(GenerateAuthChallengeResponse {
            challenge: format!("connector-{}", unix_timestamp().seconds),
        }))
    }

    async fn generate_auth_tokens(
        &self,
        _request: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        Ok(Response::new(GenerateAuthTokensResponse {
            access_token: Some(token("connector-access")),
            refresh_token: Some(token("connector-refresh")),
        }))
    }

    async fn refresh_access_token(
        &self,
        _request: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        Ok(Response::new(RefreshAccessTokenResponse {
            access_token: Some(token("connector-access")),
        }))
    }
}

struct ConnectorBlockEngine {
    state: Arc<BlockEngineProxyState>,
}

#[tonic::async_trait]
impl BlockEngineValidator for ConnectorBlockEngine {
    type SubscribePacketsStream = PacketStream;
    type SubscribeBundlesStream = BundleStream;

    async fn subscribe_packets(
        &self,
        _request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        info!("local block-engine proxy packet stream subscribed");
        let mut rx = self.state.packet_tx.subscribe();
        let proxy_state = self.state.clone();

        let stream = async_stream::try_stream! {
            let mut seen = FxHashSet::default();
            let mut dups_dropped: u64 = 0;
            let mut dedup_epoch = proxy_state.dedup_epoch.load(Ordering::Relaxed);

            loop {
                match rx.recv().await {
                    Ok(mut resp) => {
                        let next_epoch = proxy_state.dedup_epoch.load(Ordering::Relaxed);
                        if dedup_epoch != next_epoch {
                            info!(
                                cleared = seen.len(),
                                dups_dropped,
                                "clearing packet dedup map"
                            );
                            seen.clear();
                            dups_dropped = 0;
                            dedup_epoch = next_epoch;
                        }
                        if let Some(batch) = resp.batch.as_mut() {
                            let before = batch.packets.len();
                            batch
                                .packets
                                .retain(|p| packet_sig_prefix(p).is_none_or(|k| seen.insert(k)));
                            dups_dropped += (before - batch.packets.len()) as u64;
                            if batch.packets.is_empty() {
                                continue;
                            }
                        }
                        yield resp;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "local block-engine proxy packet subscriber lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::SubscribePacketsStream))
    }

    async fn subscribe_bundles(
        &self,
        _request: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        info!("local block-engine proxy bundle stream subscribed");
        let mut rx = self.state.bundle_tx.subscribe();

        let proxy_state = self.state.clone();

        let stream = async_stream::try_stream! {
            let mut seen = FxHashSet::default();
            let mut dups_dropped: u64 = 0;
            let mut dedup_epoch = proxy_state.dedup_epoch.load(Ordering::Relaxed);

            loop {
                match rx.recv().await {
                    Ok(mut resp) => {
                        let next_epoch = proxy_state.dedup_epoch.load(Ordering::Relaxed);
                        if dedup_epoch != next_epoch {
                            info!(
                                cleared = seen.len(),
                                dups_dropped,
                                "clearing bundle dedup map"
                            );
                            seen.clear();
                            dups_dropped = 0;
                            dedup_epoch = next_epoch;
                        }

                        let before = resp.bundles.len();
                        resp.bundles
                            .retain(|b| b.uuid.is_empty() || seen.insert(b.uuid.clone()));
                        dups_dropped += (before - resp.bundles.len()) as u64;
                        if resp.bundles.is_empty() {
                            continue;
                        }
                        yield resp;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "local block-engine proxy bundle subscriber lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        };
        Ok(Response::new(Box::pin(stream) as Self::SubscribeBundlesStream))
    }

    async fn get_block_builder_fee_info(
        &self,
        _request: Request<gravity_protos::block_engine::BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        let Some(info) = self.state.block_builder_info.read().unwrap().clone() else {
            return Err(Status::unavailable("block builder info unavailable"));
        };
        Ok(Response::new(info))
    }

    async fn get_block_engine_endpoints(
        &self,
        _request: Request<GetBlockEngineEndpointRequest>,
    ) -> Result<Response<GetBlockEngineEndpointResponse>, Status> {
        Ok(Response::new(GetBlockEngineEndpointResponse {
            global_endpoint: Some(BlockEngineEndpoint {
                block_engine_url: self.state.advertised_url.clone(),
                shredstream_receiver_address: String::new(),
            }),
            regioned_endpoints: Vec::new(),
        }))
    }
}

fn token(value: &str) -> Token {
    let mut expires_at_utc = unix_timestamp();
    expires_at_utc.seconds += TOKEN_TTL_SECS;
    Token { value: value.to_owned(), expires_at_utc: Some(expires_at_utc) }
}

fn unix_timestamp() -> Timestamp {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    Timestamp { seconds: duration.as_secs() as i64, nanos: duration.subsec_nanos() as i32 }
}
