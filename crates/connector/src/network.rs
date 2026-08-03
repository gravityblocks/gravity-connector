use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    ptr::copy_nonoverlapping,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use flux::{
    timing::{Duration, Instant, Nanos, Repeater},
    utils::ArrayStr,
};
use flux_network::{
    Token,
    tcp::{TcpEvent, TcpGroup, TcpGroupConfig, TcpNetwork},
};
use gravity_protos::{block_engine::SubscribePacketsResponse, packet::Packet};
use gravity_types::{
    BundleId, LeaderState, ProgressTracker, SigPrefix,
    consts::MAX_ALLOCATION_SZ,
    order::{BundleOffset, TxBytesOffset},
    runtime::background_runtime,
    wire::{
        AuthProof, BatchExecutionResult, BootstrapFrame, BuilderToConnector, ClientHello,
        ConnectorToBuilder, Handshake, WireSharableBundle, WireSharableTx, decode_bootstrap_frame,
        encode_bootstrap_frame, sign_auth_proof,
    },
};
use rts_alloc::Allocator;
use rustc_hash::{FxHashMap, FxHashSet};
use solana_keypair::Keypair;
use solana_signer::Signer;
use tokio::sync::mpsc::Receiver;
use tracing::{error, info, warn};

use crate::{
    BridgeToNetwork, Failsafe, NetworkToBridge, RelayEndpoint, StopCodes,
    bundle::{BlockEngineProxyHandle, BlockEngineReceiverMsg},
    domain::DomainHandle,
    messages::ConnectorMiniBlockMsg,
    metrics, set_shred_receiver_addresses,
};

const BUILDER_DISCONNECT_PANIC_MINS: u64 = 10;
const RELAY_AUTH_TIMEOUT_SECS: u64 = 10;
const RELAY_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Most shred receiver addresses the validator will accept.
pub const MAX_SHRED_RECEIVER_ADDRESSES: usize = 32;
/// Slots kept free in that list for relay-provided addresses.
pub const RESERVED_RELAY_SHRED_RECEIVERS: usize = 2;

pub fn dedup_shred_receivers(addresses: &mut Vec<SocketAddr>) {
    let configured = addresses.len();
    let mut seen = Vec::with_capacity(configured);
    addresses.retain(|addr| {
        if seen.contains(addr) {
            return false;
        }
        seen.push(*addr);
        true
    });
    if addresses.len() != configured {
        warn!(
            configured,
            unique = addresses.len(),
            "dropped duplicate shred receivers from config"
        );
    }
}

pub struct NetworkTile {
    relay_conn: RelayConnection,
    tx: rtrb::Producer<NetworkToBridge>,
    rx: rtrb::Consumer<BridgeToNetwork>,
    exec_rx: rtrb::Consumer<BatchExecutionResult>,
    block_engine_rx: Receiver<BlockEngineReceiverMsg>,
    block_engine_proxy: Option<BlockEngineProxyHandle>,
    slot_info: ProgressTracker,
    disconnected_since: Option<Instant>,
    log_repeater: Repeater,
    shred_receiver_repeater: Repeater,
    admin_rpc_path: PathBuf,
    relay_shred_receivers: Option<Vec<SocketAddr>>,
    base_shred_receivers: Vec<SocketAddr>,

    seen_txs: FxHashSet<SigPrefix>,
    seen_bundles: FxHashSet<BundleId>,
    dup_txs_dropped: u64,
    dup_bundles_dropped: u64,
}

impl NetworkTile {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        relay_addrs: &[RelayEndpoint],
        handshake: Handshake,
        tx: rtrb::Producer<NetworkToBridge>,
        rx: rtrb::Consumer<BridgeToNetwork>,
        exec_rx: rtrb::Consumer<BatchExecutionResult>,
        block_engine_rx: Receiver<BlockEngineReceiverMsg>,
        block_engine_proxy: Option<BlockEngineProxyHandle>,
        relay_is_connected: Arc<AtomicBool>,
        admin_rpc_path: PathBuf,
        base_shred_receivers: Vec<SocketAddr>,
        validator_keypair: Keypair,
    ) -> Self {
        let builder_conn =
            RelayConnection::new(handshake, relay_addrs, relay_is_connected, validator_keypair);

        Self {
            relay_conn: builder_conn,
            tx,
            rx,
            exec_rx,
            block_engine_rx,
            block_engine_proxy,
            slot_info: ProgressTracker::default(),
            disconnected_since: None,
            log_repeater: Repeater::every(Duration::from_secs(10)),
            shred_receiver_repeater: Repeater::every(Duration::from_secs(5)),
            admin_rpc_path,
            relay_shred_receivers: None,
            base_shred_receivers,

            seen_txs: FxHashSet::default(),
            seen_bundles: FxHashSet::default(),
            dup_txs_dropped: 0,
            dup_bundles_dropped: 0,
        }
    }

    fn process_block_engine_messages(&mut self, allocator: &Allocator) {
        while let Ok(msg) = self.block_engine_rx.try_recv() {
            match msg {
                BlockEngineReceiverMsg::Bundles(resp, received_at, source_uri) => {
                    self.process_block_engine_bundles(resp, received_at, source_uri, allocator);
                }
                BlockEngineReceiverMsg::Packets(resp, received_at, source_uri) => {
                    self.process_block_engine_packets(resp, received_at, source_uri, allocator);
                }
            }
        }
    }

    fn process_block_engine_bundles(
        &mut self,
        resp: gravity_protos::block_engine::SubscribeBundlesResponse,
        received_at: Nanos,
        source_uri: ArrayStr<64>,
        allocator: &Allocator,
    ) {
        for bundle_uuid in resp.bundles {
            // jito block engine sends periodic keep-alive bundles with empty txs
            if bundle_uuid.uuid == "keep_alive_bundle" {
                continue;
            }

            let Some(bundle) = bundle_uuid.bundle else {
                continue;
            };

            if self.slot_info.current_slot == 0 ||
                self.slot_info.leader_state == LeaderState::Inactive
            {
                continue;
            }

            let bundle_id = if let Some(id) = BundleId::from_hex(&bundle_uuid.uuid) {
                if !self.seen_bundles.insert(id) {
                    self.dup_bundles_dropped += 1;
                    continue;
                }
                id
            } else {
                warn!(id = %bundle_uuid.uuid, "can't parse bundle id");
                BundleId::new_synthetic()
            };

            let bundle_offset =
                match BundleOffset::new_from_jito(bundle_id, &bundle.packets, allocator) {
                    Ok(bundle_offset) => bundle_offset,
                    Err(err) => {
                        warn!(id = %bundle_uuid.uuid, ?err, "dropping invalid jito bundle");
                        continue;
                    }
                };

            if let Err(rtrb::PushError::Full(NetworkToBridge::JitoBundle { bundle })) =
                self.tx.push(NetworkToBridge::JitoBundle { bundle: bundle_offset })
            {
                bundle.free(allocator);
                warn!("bridge channel is full; dropping jito bundle");
                continue;
            }

            let to_builder = ConnectorToBuilder::Bundle {
                bundle: WireSharableBundle::from_shmem(&bundle_offset, allocator),
                source_uri,
                received_at,
            };
            self.relay_conn.send(&to_builder);
        }
    }

    fn process_block_engine_packets(
        &mut self,
        resp: SubscribePacketsResponse,
        received_at: Nanos,
        source_uri: ArrayStr<64>,
        allocator: &Allocator,
    ) {
        let Some(batch) = resp.batch else { return };

        for packet in batch.packets {
            if self.slot_info.current_slot == 0 ||
                self.slot_info.leader_state == LeaderState::Inactive
            {
                continue;
            }

            let Some(sig_prefix) = packet_sig_prefix(&packet) else {
                warn!(%source_uri, "dropping invalid block-engine packet");
                continue;
            };

            if !self.seen_txs.insert(sig_prefix) {
                self.dup_txs_dropped += 1;
                continue;
            }

            let Some(tx_offset) = alloc_packet_tx(&packet, allocator) else {
                warn!(
                    %source_uri,
                    len = packet.data.len(),
                    "dropping unallocatable block-engine packet"
                );
                continue;
            };

            if let Err(rtrb::PushError::Full(NetworkToBridge::JitoTransaction { tx, .. })) =
                self.tx.push(NetworkToBridge::JitoTransaction { sig_prefix, tx: tx_offset })
            {
                tx.free(allocator);
                warn!("bridge channel is full; dropping block-engine packet");
                continue;
            }

            let to_builder = ConnectorToBuilder::Transaction {
                order: WireSharableTx::from_shmem(&tx_offset, allocator),
                received_at,
                src_addr: packet_src_addr(&packet),
                sent_at: Nanos::now(),
                source_uri: Some(source_uri),
            };
            self.relay_conn.send(&to_builder);
        }
    }

    pub fn wait_for_builder(&mut self, stop: &AtomicUsize) {
        info!("waiting for builder connection before startup");
        while stop.load(Ordering::Relaxed) == StopCodes::CONTINUE as usize &&
            !self.relay_conn.is_active()
        {
            self.poll_startup();
            if !self.relay_conn.is_active() {
                if self.log_repeater.fired() {
                    info!("still waiting for builder connection before startup");
                }
            }
        }
    }

    pub fn poll_startup(&mut self) {
        let _ = self.relay_conn.poll(|_| {});
    }

    pub fn poll_delete_failsafe(&mut self) -> bool {
        let mut delete = false;
        self.relay_conn.poll(|msg| {
            if matches!(msg, BuilderToConnector::DeleteFailsafe) {
                info!("builder requested failsafe deletion");
                delete = true;
            }
        });
        delete
    }

    pub fn loop_body(&mut self, allocator: &Allocator) {
        if self.log_repeater.fired() {
            if self.relay_conn.is_active() {
                info!("builder connected");
                self.disconnected_since = None;
            } else {
                info!("waiting for builder connection");
                if self.disconnected_since.is_none() {
                    self.disconnected_since = Some(Instant::now());
                }
                if self.disconnected_since.unwrap().elapsed() >=
                    Duration::from_mins(BUILDER_DISCONNECT_PANIC_MINS)
                {
                    error!("Builder disconnecting for too long! Panicking!");
                    panic!("Builder offline!");
                }
            }
        }

        if let Ok(msg) = self.rx.pop() {
            let to_builder = match msg {
                BridgeToNetwork::TpuTransaction { tx, received_at, src_addr } => {
                    let order = WireSharableTx::from_shmem(&tx, allocator);
                    self.seen_txs.insert(order.sig_prefix());
                    ConnectorToBuilder::Transaction {
                        order,
                        received_at,
                        src_addr,
                        sent_at: Nanos::now(),
                        source_uri: None,
                    }
                }
                BridgeToNetwork::Progress(msg) => {
                    if self.slot_info.update_from_slot_message(msg) {
                        if let Some(proxy) = &self.block_engine_proxy {
                            proxy.bump_epoch_counter();
                        }
                        info!(
                            txs = self.seen_txs.len(),
                            bundles = self.seen_bundles.len(),
                            dup_txs_dropped = self.dup_txs_dropped,
                            dup_bundles_dropped = self.dup_bundles_dropped,
                            "clearing block-engine dedup sets"
                        );
                        self.seen_txs.clear();
                        self.seen_bundles.clear();
                        self.dup_txs_dropped = 0;
                        self.dup_bundles_dropped = 0;
                    }
                    ConnectorToBuilder::Progress(msg)
                }
                BridgeToNetwork::ReadyForTips(slot) => ConnectorToBuilder::ReadyForTips(slot),
                BridgeToNetwork::CrankBundle { bundle } => {
                    let wire_bundle = WireSharableBundle::from_shmem(&bundle, allocator);
                    ConnectorToBuilder::CrankBundle(wire_bundle)
                }
            };

            self.relay_conn.send(&to_builder);
        }

        if let Ok(result) = self.exec_rx.pop() {
            let to_builder = ConnectorToBuilder::ExecutionResult(result);
            self.relay_conn.send(&to_builder);
        }

        self.process_block_engine_messages(allocator);
        let mut relay_shred_receivers = None;
        let active_relay_disconnected = self.relay_conn.poll(|msg| match msg {
            BuilderToConnector::MiniBlockGraph { graph, orders } => {
                let msg = ConnectorMiniBlockMsg::new(graph, &orders, allocator);
                self.tx
                    .push(NetworkToBridge::MiniBlockGraph { received_at: Nanos::now(), msg })
                    .unwrap();
            }
            BuilderToConnector::DeleteFailsafe => {
                info!("builder requested failsafe deletion");
                Failsafe::remove();
            }
            BuilderToConnector::ShredReceiverAddresses(value) => {
                relay_shred_receivers = Some(value);
            }
            BuilderToConnector::PreviousTipReceiver { slot, tip_receiver, block_builder } => {
                if self
                    .tx
                    .push(NetworkToBridge::PreviousTipReceiver {
                        slot,
                        tip_receiver,
                        block_builder,
                    })
                    .is_err()
                {
                    warn!("failed forwarding previous tip receiver to bridge");
                }
            }
        });

        if active_relay_disconnected {
            self.relay_shred_receivers = None;
        } else if relay_shred_receivers.is_some() {
            self.relay_shred_receivers = relay_shred_receivers;
        }

        if self.shred_receiver_repeater.fired() {
            if self.relay_conn.relay_is_connected.load(Ordering::Relaxed) &&
                let Some(addresses) = &self.relay_shred_receivers
            {
                self.apply_shred_receiver_update(addresses.clone());
            } else {
                self.apply_shred_receiver_update(Vec::new());
            }
        }
    }

    fn apply_shred_receiver_update(&self, relay_addresses: Vec<SocketAddr>) {
        let base_len = self.base_shred_receivers.len();
        let relay_len = relay_addresses.len();
        let capacity = (base_len + relay_len).min(MAX_SHRED_RECEIVER_ADDRESSES);
        let mut addresses: Vec<SocketAddr> = Vec::with_capacity(capacity);
        addresses.extend_from_slice(&self.base_shred_receivers);
        for addr in relay_addresses {
            if addresses.len() == MAX_SHRED_RECEIVER_ADDRESSES {
                warn!(
                    base_len,
                    relay_len,
                    max_len = MAX_SHRED_RECEIVER_ADDRESSES,
                    "truncating shred receiver addresses"
                );
                break;
            }
            if !addresses.contains(&addr) {
                addresses.push(addr);
            }
        }
        background_runtime()
            .spawn(set_shred_receiver_addresses(self.admin_rpc_path.clone(), addresses));
    }
}

pub(crate) fn packet_sig_prefix(packet: &Packet) -> Option<SigPrefix> {
    if packet.data.first() == Some(&0) || packet.data.len() <= SigPrefix::LEN {
        return None;
    }

    let mut sig_prefix = [0u8; SigPrefix::LEN];
    sig_prefix.copy_from_slice(&packet.data[1..=SigPrefix::LEN]);
    Some(SigPrefix::new(sig_prefix))
}

fn alloc_packet_tx(packet: &Packet, allocator: &Allocator) -> Option<TxBytesOffset> {
    let tx_length = packet.data.len();
    if tx_length == 0 || tx_length > MAX_ALLOCATION_SZ {
        return None;
    }

    let allocation = allocator.allocate(tx_length as u32)?;
    let tx_offset = unsafe {
        copy_nonoverlapping(packet.data.as_ptr(), allocation.as_ptr(), tx_length);
        allocator.offset(allocation)
    };
    Some(TxBytesOffset::new(tx_offset, tx_length))
}

fn packet_src_addr(packet: &Packet) -> [u8; 16] {
    let Some(meta) = &packet.meta else { return [0; 16] };
    match meta.addr.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => ip.to_ipv6_mapped().octets(),
        Ok(IpAddr::V6(ip)) => ip.octets(),
        Err(_) => [0; 16],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayState {
    NotConnected,
    AwaitingServerHello { since: Instant },
    AwaitingAcceptance { since: Instant },
    Authenticated,
}

impl RelayState {
    fn is_authenticated(self) -> bool {
        matches!(self, Self::Authenticated)
    }
}

struct RelayInfo {
    domain: DomainHandle,
    token: Option<Token>,
    state: RelayState,
    addr: Option<SocketAddr>,
    connect_started: Option<Instant>,
}

struct RelayConnection {
    network: TcpNetwork,
    group: TcpGroup,
    relay_is_connected: Arc<AtomicBool>,
    validator_keypair: Keypair,
    handshake: Handshake,
    relays: Vec<RelayInfo>,
    active_idx: Option<usize>,
    token_to_idx: FxHashMap<Token, usize>,
    proof_scratch: Vec<(Token, AuthProof)>,
    handshake_scratch: Vec<Token>,
    disconnect_scratch: Vec<Token>,
    reconnect_scratch: Vec<usize>,
}

impl RelayConnection {
    fn new(
        handshake: Handshake,
        relay_addrs: &[RelayEndpoint],
        relay_is_connected: Arc<AtomicBool>,
        validator_keypair: Keypair,
    ) -> Self {
        assert!(!relay_addrs.is_empty(), "empty relays list");
        assert_eq!(
            handshake.identity,
            validator_keypair.pubkey(),
            "connector handshake identity must match the authentication keypair"
        );
        assert!(handshake.num_threads > 0, "connector must advertise at least one thread");
        let client_hello_frame =
            encode_bootstrap_frame(&BootstrapFrame::ClientHello(ClientHello {
                identity: handshake.identity,
            }));
        let mut network = TcpNetwork::default();
        let group = network.add_group(TcpGroupConfig {
            name: "relays",
            on_connect_msg: Some(client_hello_frame),
            socket_buf_size: Some(64 * 1024 * 1024),
            reconnect_interval: Duration::from_secs(1),
            ..TcpGroupConfig::default()
        });

        let mut relays = Vec::with_capacity(relay_addrs.len());
        let token_to_idx = FxHashMap::default();
        for endpoint in relay_addrs {
            relays.push(RelayInfo {
                domain: DomainHandle::new(endpoint.clone()),
                token: None,
                state: RelayState::NotConnected,
                addr: None,
                connect_started: None,
            });
        }
        relay_is_connected.store(false, Ordering::Relaxed);
        let mut connection = Self {
            network,
            group,
            relay_is_connected,
            validator_keypair,
            handshake,
            relays,
            active_idx: None,
            token_to_idx,
            proof_scratch: Vec::with_capacity(16),
            handshake_scratch: Vec::with_capacity(16),
            disconnect_scratch: Vec::with_capacity(16),
            reconnect_scratch: Vec::with_capacity(16),
        };
        connection.poll_domains();
        connection
    }

    fn poll(&mut self, mut on_msg: impl FnMut(BuilderToConnector)) -> bool {
        self.disconnect_scratch.clear();
        self.reconnect_scratch.clear();
        self.poll_domains();
        self.refresh_disconnected_relays();

        let active_idx_before_poll = self.active_idx;
        let mut dropped_msgs = 0;
        self.network.poll_with(|event| match event {
            TcpEvent::Connected { token, peer_addr, .. } => {
                let Some(&idx) = self.token_to_idx.get(&token) else {
                    warn!("unknown token connected {:?} from {}", token, peer_addr);
                    return;
                };
                self.relays[idx].state = RelayState::AwaitingServerHello { since: Instant::now() };
                self.relays[idx].connect_started = None;
                info!(
                    endpoint = %self.relays[idx].domain.endpoint(),
                    ?peer_addr,
                    "relay connected; sent bootstrap hello"
                );
            }
            TcpEvent::Disconnected { token, .. } => {
                let Some(&idx) = self.token_to_idx.get(&token) else {
                    warn!("disconnected relay is not in the list");
                    return;
                };
                metrics::RELAY_DISCONNECTS.inc();
                self.relays[idx].state = RelayState::NotConnected;
                self.relays[idx].connect_started = Some(Instant::now());
                self.relays[idx].domain.request_refresh();
                self.reconnect_scratch.push(idx);
                if self.active_idx == Some(idx) {
                    self.active_idx = None;
                }
            }
            TcpEvent::Message { token, payload, .. } => {
                let Some(&sender_idx) = self.token_to_idx.get(&token) else {
                    warn!("received message from unknown token {:?}", token);
                    return;
                };
                let sender = &mut self.relays[sender_idx];
                match sender.state {
                    RelayState::AwaitingServerHello { .. } => {
                        match decode_bootstrap_frame(payload) {
                            Ok(BootstrapFrame::ServerHello(server_hello)) => {
                                let proof = sign_auth_proof(
                                    &self.validator_keypair,
                                    &server_hello.challenge,
                                );
                                self.proof_scratch.push((token, proof));
                                sender.state =
                                    RelayState::AwaitingAcceptance { since: Instant::now() };
                            }
                            Ok(BootstrapFrame::Rejected { reason }) => {
                                warn!(endpoint = %sender.domain.endpoint(), addr = ?sender.addr, ?reason, "relay rejected bootstrap");
                                self.disconnect_scratch.push(token);
                            }
                            Ok(frame) => {
                                warn!(
                                    endpoint = %sender.domain.endpoint(),
                                    addr = ?sender.addr,
                                    ?frame,
                                    "unexpected bootstrap frame while awaiting server hello"
                                );
                                self.disconnect_scratch.push(token);
                            }
                            Err(err) => {
                                warn!(
                                    endpoint = %sender.domain.endpoint(),
                                    addr = ?sender.addr,
                                    ?err,
                                    "invalid relay bootstrap server hello"
                                );
                                self.disconnect_scratch.push(token);
                            }
                        }
                    }
                    RelayState::AwaitingAcceptance { .. } => {
                        match decode_bootstrap_frame(payload) {
                            Ok(BootstrapFrame::Accepted) => {
                                metrics::RELAY_CONNECTS.inc();
                                self.handshake_scratch.push(token);
                                sender.state = RelayState::Authenticated;
                                if self.active_idx.is_none() {
                                    self.active_idx = Some(sender_idx);
                                }
                                info!(endpoint = %sender.domain.endpoint(), addr = ?sender.addr, "authenticated with relay");
                            }
                            Ok(BootstrapFrame::Rejected { reason }) => {
                                warn!(endpoint = %sender.domain.endpoint(), addr = ?sender.addr, ?reason, "relay rejected auth proof");
                                self.disconnect_scratch.push(token);
                            }
                            Ok(frame) => {
                                warn!(
                                    endpoint = %sender.domain.endpoint(),
                                    addr = ?sender.addr,
                                    ?frame,
                                    "unexpected bootstrap frame while awaiting auth acceptance"
                                );
                                self.disconnect_scratch.push(token);
                            }
                            Err(err) => {
                                warn!(
                                    endpoint = %sender.domain.endpoint(),
                                    addr = ?sender.addr,
                                    ?err,
                                    "invalid relay bootstrap auth acceptance"
                                );
                                self.disconnect_scratch.push(token);
                            }
                        }
                    }
                    RelayState::Authenticated if self.active_idx == Some(sender_idx) => {
                        match wincode::deserialize::<BuilderToConnector>(payload) {
                            Ok(msg) => on_msg(msg),
                            Err(err) => {
                                warn!(endpoint = %sender.domain.endpoint(), addr = ?sender.addr, ?err, "invalid relay session message");
                                self.disconnect_scratch.push(token);
                            }
                        }
                    }
                    RelayState::Authenticated => {
                        dropped_msgs += 1;
                    }
                    RelayState::NotConnected => {
                        warn!(endpoint = %sender.domain.endpoint(), addr = ?sender.addr, "relay message received while disconnected");
                        self.disconnect_scratch.push(token);
                    }
                }
            }
            TcpEvent::Accepted { .. } => unreachable!("relay group has no listener"),
        });

        if dropped_msgs > 0 {
            warn!("dropped {} msgs", dropped_msgs);
        }
        while let Some(idx) = self.reconnect_scratch.pop() {
            self.rotate_address(idx);
        }

        for (token, proof) in self.proof_scratch.drain(..) {
            if self.disconnect_scratch.contains(&token) {
                continue;
            }
            let frame = encode_bootstrap_frame(&BootstrapFrame::AuthProof(proof));
            self.network.send_with(token, |buf| {
                buf.extend_from_slice(&frame);
            });
        }

        for token in self.handshake_scratch.drain(..) {
            if self.disconnect_scratch.contains(&token) {
                continue;
            }
            let message = ConnectorToBuilder::Handshake(self.handshake.clone());
            self.network.send_with(token, |buf| {
                wincode::serialize_into(buf, &message).unwrap();
            });
        }

        let timeout = Duration::from_secs(RELAY_AUTH_TIMEOUT_SECS);
        for relay in &self.relays {
            let timed_out = match relay.state {
                RelayState::AwaitingServerHello { since } |
                RelayState::AwaitingAcceptance { since, .. } => since.elapsed() >= timeout,
                RelayState::NotConnected | RelayState::Authenticated => false,
            };
            if timed_out &&
                let Some(token) = relay.token &&
                !self.disconnect_scratch.contains(&token)
            {
                warn!(endpoint = %relay.domain.endpoint(), addr = ?relay.addr, "relay bootstrap timed out");
                self.disconnect_scratch.push(token);
            }
        }

        for token in self.disconnect_scratch.drain(..) {
            if let Some(&idx) = self.token_to_idx.get(&token) {
                self.relays[idx].state = RelayState::NotConnected;
                self.relays[idx].connect_started = Some(Instant::now());
                if self.active_idx == Some(idx) {
                    self.active_idx = None;
                }
            }
            self.network.disconnect(token);
        }

        if self.active_idx.is_none_or(|idx| !self.relays[idx].state.is_authenticated()) {
            self.active_idx = self.relays.iter().position(|relay| relay.state.is_authenticated());
        }
        self.relay_is_connected.store(self.active_idx.is_some(), Ordering::Relaxed);
        metrics::RELAY_CONNECTED.set(i64::from(self.active_idx.is_some()));
        active_idx_before_poll.is_some_and(|idx| self.active_idx != Some(idx))
    }

    fn send(&mut self, msg: &ConnectorToBuilder) {
        if let Some(active_idx) = self.active_idx {
            let active = &self.relays[active_idx];
            if let Some(token) = active.token {
                self.network.send_with(token, |buf| {
                    wincode::serialize_into(buf, msg).unwrap();
                });
            }
        }
    }

    fn is_active(&self) -> bool {
        self.active_idx.is_some()
    }

    fn poll_domains(&mut self) {
        for relay in &mut self.relays {
            relay.domain.poll();
        }
        for idx in 0..self.relays.len() {
            if self.relays[idx].token.is_none() {
                self.rotate_address(idx);
            }
        }
    }

    fn refresh_disconnected_relays(&mut self) {
        for idx in 0..self.relays.len() {
            let timed_out = {
                let relay = &self.relays[idx];
                relay.state == RelayState::NotConnected &&
                    relay.connect_started.is_some_and(|since| {
                        since.elapsed() >= Duration::from_secs(RELAY_CONNECT_TIMEOUT_SECS)
                    })
            };
            if timed_out {
                let relay = &mut self.relays[idx];
                relay.connect_started = Some(Instant::now());
                relay.domain.request_refresh();
                self.rotate_address(idx);
            }
        }
    }

    fn rotate_address(&mut self, relay_idx: usize) {
        if let Some(addr) = self.relays[relay_idx].domain.next_addr() {
            self.replace_endpoint(relay_idx, addr);
        }
    }

    fn replace_endpoint(&mut self, relay_idx: usize, addr: SocketAddr) {
        let relay = &mut self.relays[relay_idx];
        if relay.addr == Some(addr) && relay.token.is_some() {
            return;
        }

        if let Some(old_token) = relay.token.take() {
            self.token_to_idx.remove(&old_token);
            self.network.remove(old_token);
        }
        if self.active_idx == Some(relay_idx) {
            self.active_idx = None;
        }

        let token = self.network.connect(self.group, addr);
        relay.token = Some(token);
        relay.addr = Some(addr);
        relay.state = RelayState::NotConnected;
        relay.connect_started = Some(Instant::now());
        self.token_to_idx.insert(token, relay_idx);
        info!(endpoint = %relay.domain.endpoint(), %addr, "connecting to resolved relay address");
    }
}
