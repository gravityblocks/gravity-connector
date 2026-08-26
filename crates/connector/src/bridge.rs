use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use agave_scheduler_bindings::{
    LEADER_STARTING, PackToWorkerMessage, ProgressMessage as AgaveProgressMessage,
    SharableTransactionBatchRegion, SharableTransactionRegion, TpuToPackMessage,
    pack_message_flags::{self},
    tpu_message_flags,
};
use agave_scheduling_utils::{
    handshake::ClientWorkerSession, transaction_ptr::TransactionPtrBatch,
};
use flux::{
    timing::{Duration, Instant, Nanos},
    utils::{ArrayVec, safe_assert, safe_assert_eq},
};
use gravity_types::{
    BundleId, ExecutionResult, LeaderState, MiniBlockUuid, NotIncludedReason, SigPrefix,
    SlotProgress,
    consts::MAX_TXS_PER_MESSAGE,
    order::{BundleOffset, TxBytesOffset},
    wire::{
        BUNDLE_EXECUTION_FLAGS, BatchExecutionResult, BatchOrders, NodeOrderRef, SlotNum,
        WireMiniBlockGraph,
    },
};
use rts_alloc::Allocator;
use rustc_hash::{FxBuildHasher, FxHashMap};
use shaq::spsc::Consumer;
use solana_address::Address;
use solana_transaction::versioned::VersionedTransaction;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::{
    StopCodes,
    bundle::{CrankTrigger, PrevTipConfig},
    cache::StateCache,
    config::ClientVariant,
    dispatch::{Dag, MAX_INFLIGHT_PER_WORKER, PendingBatch},
    messages::ConnectorProgressTracker,
    metrics,
    network::{Network, NetworkEvent},
    worker_to_pack::{BatchId, ExecutionMsg},
};

/// Bound fanout dedup state while the connector is not scheduling.
const INACTIVE_DEDUP_WINDOW_SLOTS: u64 = 8;

struct AgaveWorkers {
    workers: Vec<ClientWorkerSession>,
    last_sent: Vec<Instant>,

    slow_worker_dur: Duration,
    ping_dur: Duration,

    thread_inflight: Vec<u8>,
}

impl AgaveWorkers {
    fn new(workers: Vec<ClientWorkerSession>) -> Self {
        let num_workers = workers.len();
        Self {
            last_sent: vec![Instant::now(); workers.len()],
            workers,

            slow_worker_dur: Duration::from_micros(50),
            ping_dur: Duration::from_micros(500),

            thread_inflight: vec![0; num_workers],
        }
    }

    fn send_worker_message(&mut self, i: usize, msg: PackToWorkerMessage) {
        let worker = &mut self.workers[i];
        let start = Instant::now();

        worker.pack_to_worker.sync();
        worker.pack_to_worker.try_write(msg).expect("failed sending check request");
        worker.pack_to_worker.commit();
        let now = Instant::now();
        self.last_sent[i] = now;

        let el = now.saturating_sub(start);
        if el > self.slow_worker_dur {
            warn!(%el, worker = i, "slow worker send");
        }
    }

    fn maybe_ping(&mut self) {
        for (i, ping) in self.last_sent.iter_mut().enumerate() {
            if ping.elapsed() > self.ping_dur {
                // this will fail immediately in the agave and worker and trigger a
                // WorkerToPackError::UnknownProcessedCode
                let msg = PackToWorkerMessage {
                    flags: 0,
                    max_working_slot: 0,
                    batch: SharableTransactionBatchRegion {
                        num_transactions: 0,
                        transactions_offset: 0,
                    },
                };
                let worker = &mut self.workers[i];
                let start = Instant::now();

                worker.pack_to_worker.sync();
                worker.pack_to_worker.try_write(msg).expect("failed sending check request");
                worker.pack_to_worker.commit();
                let now = Instant::now();
                *ping = now;

                let el = now.saturating_sub(start);
                if el > self.slow_worker_dur {
                    warn!(%el, worker = i, "slow worker ping");
                }
            }
        }
    }

    fn len(&self) -> usize {
        self.workers.len()
    }
}

pub struct ConnectorTile {
    network: Network,
    pending_relay_events: VecDeque<NetworkEvent>,
    tpu_to_pack: Consumer<TpuToPackMessage>,
    progress_tracker: Consumer<AgaveProgressMessage>,
    last_slot_seen: SlotNum,
    workers: AgaveWorkers,
    slot_info: ConnectorProgressTracker,
    cache: StateCache,
    allocator: Allocator,
    slot_duration_override_ms: Option<u64>,
    client_variant: ClientVariant,
    last_progress: Instant,
    valid_schedule: usize,

    dag: Dag,
    pending_scheduled: FxHashMap<BatchId, PendingBatch>,
    pending_graph: VecDeque<(Nanos, WireMiniBlockGraph)>,
    graph_received_at: Nanos,

    // jito
    connector_crank_enabled: bool,
    cranked_this_leader: bool,
    tx_buffer: Vec<u8>,
    crank_bundle_rx: mpsc::Receiver<VersionedTransaction>,
    crank_trigger_tx: mpsc::Sender<CrankTrigger>,
    /// Waiting for execution of the crank bundle
    pending_jito: Option<(BatchId, Instant)>,
    /// Latest crank bundle ready to send
    crank_bundle: Option<BundleOffset>,
    inflight_crank_req: Option<Instant>,
    last_crank_attempt: Instant,
    prev_tip_config: Option<PrevTipConfig>,
}

impl ConnectorTile {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        network: Network,
        tpu_to_pack: Consumer<TpuToPackMessage>,
        progress_tracker: Consumer<AgaveProgressMessage>,
        workers: Vec<ClientWorkerSession>,
        connector_crank_enabled: bool,
        crank_bundle_rx: mpsc::Receiver<VersionedTransaction>,
        crank_trigger_tx: mpsc::Sender<CrankTrigger>,
        allocator: Allocator,
        slot_duration_override_ms: Option<u64>,
        client_variant: ClientVariant,
    ) -> Self {
        Self {
            network,
            pending_relay_events: VecDeque::with_capacity(16),
            tpu_to_pack,
            progress_tracker,
            last_slot_seen: 0,
            workers: AgaveWorkers::new(workers),
            slot_info: ConnectorProgressTracker::default(),
            cache: StateCache::new(),

            allocator,
            slot_duration_override_ms,
            client_variant,
            last_progress: Instant::now(),
            valid_schedule: 0,

            dag: Dag::new(),
            pending_scheduled: FxHashMap::with_capacity_and_hasher(1024, FxBuildHasher),
            pending_graph: VecDeque::with_capacity(10),
            graph_received_at: Nanos::ZERO,

            connector_crank_enabled,
            cranked_this_leader: false,
            tx_buffer: Vec::with_capacity(4096),
            crank_bundle_rx,
            crank_trigger_tx,
            pending_jito: None,
            crank_bundle: None,
            inflight_crank_req: None,
            last_crank_attempt: Instant::now(),
            prev_tip_config: None,
        }
    }

    pub fn run(mut self, stop: &Arc<AtomicUsize>) {
        while stop.load(Ordering::Relaxed) == StopCodes::CONTINUE as usize {
            self.handle_progress_message();
            self.handle_tpu();
            self.handle_workers_messages();

            if matches!(self.slot_info.leader_state, LeaderState::Warmup | LeaderState::Sequencing)
            {
                self.workers.maybe_ping();
            }

            self.recv_crank_bundle();
            self.maybe_crank();

            self.network.loop_body(
                &self.allocator,
                &self.slot_info,
                &mut self.cache,
                &mut self.pending_relay_events,
            );
            self.handle_relay_events();

            if self.slot_info.leader_state == LeaderState::Sequencing {
                self.dispatch();
            }

            self.check_progress(stop);
        }
    }

    fn handle_relay_events(&mut self) {
        while let Some(event) = self.pending_relay_events.pop_front() {
            match event {
                NetworkEvent::MiniBlockGraph { received_at, graph } => {
                    self.ingest_graph(received_at, graph);
                }
                NetworkEvent::PreviousTipReceiver { slot, tip_receiver, block_builder } => {
                    self.handle_previous_tip_receiver(slot, tip_receiver, block_builder);
                }
            }
        }
    }

    /// Here we receive raw packets from TPU, after sigverify. The deduper there
    /// is probabilistic + resets periodically so it is possible we receive
    /// duplicates here
    fn handle_tpu(&mut self) {
        let start = Instant::now();
        let loop_time = Duration::from_micros(250);
        self.tpu_to_pack.sync();

        while let Some(msg) = self.tpu_to_pack.try_read() &&
            start.elapsed() < loop_time
        {
            let received_at = Nanos::now();
            let tx_offset =
                TxBytesOffset::new(msg.transaction.offset, msg.transaction.length as usize);
            let retain_for_scheduling = self.slot_info.retain_for_scheduling();

            if !retain_for_scheduling && msg.flags & tpu_message_flags::IS_SIMPLE_VOTE != 0 {
                tx_offset.free(&self.allocator);
                continue;
            }

            // SAFETY: `tx_offset` refers to the live allocation received from Agave.
            let Some(sig_prefix) =
                (unsafe { SigPrefix::try_from_allocator(tx_offset, &self.allocator) })
            else {
                tx_offset.free(&self.allocator);
                continue;
            };
            if !self.network.dedup_transaction(sig_prefix) {
                tx_offset.free(&self.allocator);
                continue;
            }

            if retain_for_scheduling && !self.cache.new_tx(sig_prefix, tx_offset) {
                continue;
            }

            self.network.send_tpu_transaction(
                tx_offset,
                received_at,
                msg.src_addr,
                &self.allocator,
            );
            if !retain_for_scheduling {
                tx_offset.free(&self.allocator);
            }
        }

        self.tpu_to_pack.finalize();
    }

    /// Here we are get memory created in agave: `TransactionResponseRegion`,
    /// and the `SharableTransactionBatchRegion`
    fn handle_workers_messages(&mut self) {
        for worker in &mut self.workers.workers {
            worker.worker_to_pack.sync();

            while let Some(msg) = worker.worker_to_pack.try_read() {
                let Some(exec) = ExecutionMsg::try_decode(msg, &self.allocator) else {
                    continue;
                };

                let res_received_at = Nanos::now();

                let batch_id = exec.id();

                let results = exec.iter_results().collect::<ArrayVec<_, MAX_TXS_PER_MESSAGE>>();

                if let Some((wait_hash, sent_at)) = &self.pending_jito &&
                    *wait_hash == batch_id
                {
                    // crank bundle executed
                    if results.iter().all(|r| matches!(r, ExecutionResult::Included { .. })) {
                        info!(
                            time =% sent_at.elapsed(),
                            "crank bundle was successful, processing bundles for this slot"
                        );
                        if let Some(bundle) = self.crank_bundle {
                            self.network.send_crank_bundle(&bundle, &self.allocator);
                        }
                        self.cranked_this_leader = true;
                        self.crank_bundle = None;
                        self.network.send_ready_for_tips(self.slot_info.current_slot);
                    } else if results.iter().any(|r| {
                        matches!(
                            r,
                            ExecutionResult::NotIncluded(
                                NotIncludedReason::INSTRUCTION_ERROR |
                                    NotIncludedReason::SANITIZE_FAILURE
                            )
                        )
                    }) {
                        info!(
                            time =% sent_at.elapsed(),
                            ?results,
                            "crank bundle hit an instruction error (a validator likely cranked first), ready for tips"
                        );
                        self.cranked_this_leader = true;
                        self.crank_bundle = None;
                        self.network.send_ready_for_tips(self.slot_info.current_slot);
                    } else {
                        info!(
                            time =% sent_at.elapsed(),
                            ?results,
                            "crank bundle failed, will retry"
                        );
                    }
                    self.pending_jito = None;
                    self.workers.thread_inflight[0] =
                        self.workers.thread_inflight[0].saturating_sub(1);
                } else if let Some(pending) = self.pending_scheduled.remove(&batch_id) {
                    let w = pending.worker as usize;
                    self.workers.thread_inflight[w] =
                        self.workers.thread_inflight[w].saturating_sub(1);

                    // Ignore results for a since-cleared or superseded graph.
                    if self.dag.graph_id() == Some(pending.graph_id) {
                        self.dag.apply_results(pending.node, results.as_ref());
                    }

                    let result = BatchExecutionResult {
                        slot: pending.graph_id.0,
                        mini_block_uuid: pending.graph_id.1,
                        orders: pending.orders,
                        results,
                        batch_received_at: pending.received_at,
                        forwarded_at: pending.forwarded_at,
                        res_received_at,
                        worker: pending.worker,
                    };
                    self.network.send_execution_result(&result);
                } else {
                    debug!("missing batch for offsets after end of leadership");
                    safe_assert_eq!(self.slot_info.leader_state, LeaderState::Inactive);
                }

                exec.free(&self.allocator);
            }

            worker.worker_to_pack.finalize();
        }
    }

    fn handle_progress_message(&mut self) {
        self.progress_tracker.sync();
        while let Some(agave_progress) = self.progress_tracker.try_read() {
            self.last_progress = Instant::now();
            metrics::record_agave_progress();
            // If we're in a leader slot, without a working bank, for all intents and
            // purposes, we're not in a leader slot.
            if self.slot_info.current_slot == agave_progress.current_slot ||
                agave_progress.leader_state == LEADER_STARTING
            {
                continue;
            }
            let slot_num_backwards = if self.slot_info.current_slot > agave_progress.current_slot {
                warn!(
                    ?agave_progress,
                    current_slot = self.slot_info.current_slot,
                    "agave progress message slot going backward! Caused by previous leadership extending their slot length a lot"
                );
                true
            } else {
                false
            };

            let client_adjustment_ms = if self.client_variant == ClientVariant::Agave {
                if cfg!(feature = "test_validator") { 25 } else { -40 }
            } else {
                0
            };
            let slot_duration_override_ms = self
                .slot_duration_override_ms
                .map(|duration| duration.saturating_add_signed(client_adjustment_ms));
            let progress =
                SlotProgress::from_agave_progress(*agave_progress, slot_duration_override_ms);

            #[cfg(feature = "test_validator")]
            let progress = {
                use gravity_types::{NextLeaderRange, ffi_safety::FfiOption};
                const CYCLE_LENGTH: u64 = 12;
                const LEADER_SLOTS: u64 = 4;
                // Cycles on-off leadership for test-validator
                let current_slot = progress.slot_num;
                let cycle = current_slot / CYCLE_LENGTH;
                let pos_in_cycle = current_slot % CYCLE_LENGTH;

                let leader_cycle = if pos_in_cycle < LEADER_SLOTS { cycle } else { cycle + 1 };
                let leader_start = leader_cycle * CYCLE_LENGTH;
                let leader_end = leader_start + LEADER_SLOTS - 1;

                SlotProgress {
                    next_leadership: FfiOption::Some(NextLeaderRange {
                        start: leader_start,
                        end: leader_end,
                    }),
                    ..progress
                }
            };

            // We don't perform 0-scheduled check both if we've just moved backwards, and
            // also if we've just moved forwards after moving backwards
            if agave_progress.current_slot > self.last_slot_seen &&
                self.slot_info.leader_state == LeaderState::Sequencing &&
                self.valid_schedule == 0
            {
                // was supposed to be sequencing but did not receive anything
                let msg = format!(
                    "nothing scheduled for slot {}, triggering failsafe and exiting!",
                    self.slot_info.current_slot
                );
                error!("{msg}");

                #[cfg(not(feature = "test_validator"))]
                {
                    use crate::Failsafe;
                    Failsafe::write_no_schedule();
                    panic!("{msg}");
                }
            }

            if !slot_num_backwards && self.pending_jito.take().is_some() {
                warn!("crank bundle execution result never arrived");
                self.workers.thread_inflight[0] = self.workers.thread_inflight[0].saturating_sub(1);
            }

            self.last_slot_seen = self.last_slot_seen.max(agave_progress.current_slot);
            self.valid_schedule = 0;

            let prev_slot = self.slot_info.current_slot;
            let prev_state = self.slot_info.leader_state;
            let was_retaining = self.slot_info.retain_for_scheduling();
            let exited = self.slot_info.update(progress);
            let is_retaining = self.slot_info.retain_for_scheduling();

            // Off-window orders must not suppress retention when its window begins.
            let started_retaining = !was_retaining && is_retaining;
            let rotated_inactive_window = self.slot_info.leader_state == LeaderState::Inactive &&
                prev_slot / INACTIVE_DEDUP_WINDOW_SLOTS !=
                    self.slot_info.current_slot / INACTIVE_DEDUP_WINDOW_SLOTS;
            if started_retaining || (rotated_inactive_window && !exited) {
                self.network.clear_block_engine_dedup();
            }

            // Progress must reach the builder before results from the old slot and
            // any ReadyForTips notification for the new slot.
            self.network.send_progress(progress, exited);

            // Each slot is an independent graph. In-flight batches keep
            // draining; their stale graph_id keeps them off the next dag.
            self.reject_undispatched();
            self.dag.on_new_slot();

            // slot number is not monotonic, so Warmup -> Inactive -> Warmup is possible
            // in case we sent some orders during the first Warmup, we don't want to clear
            // them here
            if exited && prev_state != LeaderState::Warmup {
                info!("exiting from leadership state");
                self.cache.clear(&self.allocator);
                self.pending_scheduled.clear();
                self.workers.thread_inflight.fill(0);
            }

            match self.slot_info.leader_state {
                LeaderState::Inactive => {
                    info!(slot = self.slot_info.current_slot, "new slot (not active)");
                    self.cranked_this_leader = false;
                    self.crank_bundle = None;
                    self.inflight_crank_req = None;
                    self.prev_tip_config = None;
                }
                LeaderState::Warmup => {
                    info!(slot = self.slot_info.current_slot, "new slot (warming up)");
                }
                LeaderState::Cooldown(x) => {
                    info!(
                        slot = self.slot_info.current_slot,
                        cooldown_start = x,
                        "new slot (cooling down)"
                    );
                    self.cranked_this_leader = false;
                    self.crank_bundle = None;
                    self.inflight_crank_req = None;
                    self.prev_tip_config = None;
                }
                LeaderState::Sequencing => {
                    info!(slot = self.slot_info.current_slot, "new slot (sequencing)");
                    if self.cranked_this_leader || !self.connector_crank_enabled {
                        self.network.send_ready_for_tips(self.slot_info.current_slot);
                        info!(from_first_progress =% self.slot_info.first_progress_observed_at.elapsed(), "no crank bundle needed for this slot");
                    } else if let Some(bundle) = self.crank_bundle {
                        self.send_crank_bundle(&bundle);
                    } else {
                        warn!(
                            slot = self.slot_info.current_slot,
                            "still waiting to receive crank bundle!"
                        );
                    }
                }
            }
        }
        self.progress_tracker.finalize();
    }

    fn ingest_graph(&mut self, received_at: Nanos, graph: WireMiniBlockGraph) {
        // A graph planned for an already-ended slot; return its orders.
        if graph.slot != self.slot_info.current_slot {
            warn!(
                graph_slot = graph.slot,
                current_slot = self.slot_info.current_slot,
                "dropping mini-block graph for a different slot"
            );
            for node in &graph.nodes {
                self.reject_order_ref(
                    node.order_ref,
                    NotIncludedReason::SLOT_ENDED,
                    (graph.slot, graph.uuid),
                );
            }
            return;
        }

        if !self.dag.is_empty() {
            self.pending_graph.push_back((received_at, graph));
            return;
        }

        self.load_graph(received_at, &graph);
    }

    fn load_graph(&mut self, received_at: Nanos, graph: &WireMiniBlockGraph) {
        if let Err(err) = self.dag.load(graph) {
            error!(?err, slot = graph.slot, "failed to load mini-block graph");
            // Return the orders so the builder doesn't wait on results forever.
            for node in &graph.nodes {
                self.reject_order_ref(
                    node.order_ref,
                    NotIncludedReason::UNKNOWN_ORDERS,
                    (graph.slot, graph.uuid),
                );
            }
            return;
        }
        self.graph_received_at = received_at;
    }

    fn dispatch(&mut self) {
        if self.dag.is_empty() {
            let Some((received_at, graph)) = self.pending_graph.pop_front() else {
                return;
            };
            self.load_graph(received_at, &graph);
        }
        for worker in 0..self.workers.len() {
            if self.workers.thread_inflight[worker] < MAX_INFLIGHT_PER_WORKER {
                if !self.dispatch_one(worker) {
                    break;
                }
            }
        }
    }

    /// Returns false when nothing is ready to dispatch.
    fn dispatch_one(&mut self, worker: usize) -> bool {
        let Some(node) = self.dag.take_ready() else {
            return false;
        };

        let graph_id = self.dag.graph_id().expect("dispatching requires a loaded graph");
        let order_ref = self.dag.order_ref(node);
        let mut offsets: ArrayVec<TxBytesOffset, MAX_TXS_PER_MESSAGE> = ArrayVec::new();
        let orders = match order_ref {
            NodeOrderRef::Bundle { id } => {
                let Some(txs) = self.cache.get_bundle_offsets(&id).copied() else {
                    self.dag.complete(node);
                    self.reject_order_ref(order_ref, NotIncludedReason::UNKNOWN_ORDERS, graph_id);
                    return true;
                };
                for o in &txs {
                    offsets.push(*o);
                }
                BatchOrders::Bundle { id }
            }
            NodeOrderRef::Tx { sig } => {
                let Some(offset) = self.cache.get_tx_offset(&sig) else {
                    self.dag.complete(node);
                    self.reject_order_ref(order_ref, NotIncludedReason::UNKNOWN_ORDERS, graph_id);
                    return true;
                };
                offsets.push(offset);
                let mut sigs: ArrayVec<SigPrefix, MAX_TXS_PER_MESSAGE> = ArrayVec::new();
                sigs.push(sig);
                BatchOrders::Transactions { txs: sigs }
            }
        };
        let (msg, batch_id) =
            self.handle_schedule_inner(&offsets, orders.execution_flags(), graph_id.0);
        let num_txs = msg.batch.num_transactions;

        self.workers.thread_inflight[worker] =
            self.workers.thread_inflight[worker].saturating_add(1);
        self.valid_schedule += num_txs as usize;

        self.workers.send_worker_message(worker, msg);

        let prev = self.pending_scheduled.insert(batch_id, PendingBatch {
            orders,
            worker: worker as u8,
            node,
            graph_id,
            received_at: self.graph_received_at,
            forwarded_at: Nanos::now(),
        });
        // A collision loses the previous batch's node and wedges the dag.
        safe_assert!(prev.is_none(), "batch id collision in pending_scheduled");
        true
    }

    fn reject_order_ref(
        &mut self,
        order_ref: NodeOrderRef,
        failure: NotIncludedReason,
        graph_id: (SlotNum, MiniBlockUuid),
    ) {
        let (orders, num_txs) = match order_ref {
            NodeOrderRef::Tx { sig } => {
                let mut one = ArrayVec::new();
                one.push(sig);
                (BatchOrders::Transactions { txs: one }, 1)
            }
            NodeOrderRef::Bundle { id } => {
                // 1 if the bundle never made it into the cache.
                let num_txs = self.cache.get_bundle_offsets(&id).map_or(1usize, ArrayVec::len);
                (BatchOrders::Bundle { id }, num_txs)
            }
        };
        let result = BatchExecutionResult::reject_orders(
            graph_id.0,
            graph_id.1,
            orders,
            num_txs,
            self.graph_received_at,
            failure,
        );
        self.network.send_execution_result(&result);
    }

    /// Return undispatched orders to the builder so it can reschedule them.
    fn reject_undispatched(&mut self) {
        if let Some(graph_id) = self.dag.graph_id() {
            let undispatched: Vec<NodeOrderRef> = self.dag.undispatched().collect();
            for r in undispatched {
                self.reject_order_ref(r, NotIncludedReason::SLOT_ENDED, graph_id);
            }
        }
        while let Some((_, graph)) = self.pending_graph.pop_front() {
            for n in &graph.nodes {
                self.reject_order_ref(
                    n.order_ref,
                    NotIncludedReason::SLOT_ENDED,
                    (graph.slot, graph.uuid),
                );
            }
        }
    }

    // need to return the PackToWorkerMessage to avoid annoying borrow checker
    fn handle_schedule_inner(
        &self,
        txs: &[TxBytesOffset], // assume <= MAX_TXS_PER_MESSAGE, > 0
        execution_flags: u16,
        max_working_slot: u64,
    ) -> (PackToWorkerMessage, BatchId) {
        assert!(!txs.is_empty());
        assert!(txs.len() <= MAX_TXS_PER_MESSAGE);

        let batch_ptr = self
            .allocator
            .allocate(TransactionPtrBatch::<()>::TRANSACTION_META_END as u32)
            .expect("failed to allocate");

        let mut msg = PackToWorkerMessage {
            flags: pack_message_flags::EXECUTE | execution_flags,
            max_working_slot,
            batch: SharableTransactionBatchRegion {
                num_transactions: 0,
                transactions_offset: unsafe { self.allocator.offset(batch_ptr) },
            },
        };

        for tx in txs {
            let tx = SharableTransactionRegion { offset: tx.0.offset, length: tx.0.length as u32 };
            unsafe {
                self.allocator
                    .ptr_from_offset(msg.batch.transactions_offset)
                    .cast::<SharableTransactionRegion>()
                    .add(usize::from(msg.batch.num_transactions))
                    .write(tx);
            };
            msg.batch.num_transactions += 1;
        }

        (msg, BatchId(msg.batch.transactions_offset))
    }

    fn handle_previous_tip_receiver(
        &mut self,
        slot: u64,
        tip_receiver: Address,
        block_builder: Address,
    ) {
        if !self.connector_crank_enabled || self.cranked_this_leader {
            return;
        }
        let prev = PrevTipConfig { tip_receiver, block_builder };
        self.prev_tip_config = Some(prev);
        info!(
            slot,
            %tip_receiver,
            %block_builder,
            from_first_progress =% self.slot_info.first_progress_observed_at.elapsed(),
            "received previous tip receiver from builder"
        );
        self.trigger_crank_build(slot, Some(prev));
    }

    fn trigger_crank_build(&mut self, slot: u64, prev: Option<PrevTipConfig>) {
        if let Some(sent_at) = &self.inflight_crank_req {
            if sent_at.elapsed() < Duration::from_millis(200) {
                return;
            }
            warn!("crank build request timed out, retrying");
        }
        if let Err(err) = self.crank_trigger_tx.try_send(CrankTrigger { slot, prev }) {
            error!(?err, "failed to trigger crank");
            return;
        }
        self.inflight_crank_req = Some(Instant::now());
    }

    fn maybe_crank(&mut self) {
        if !self.connector_crank_enabled || self.cranked_this_leader || self.pending_jito.is_some()
        {
            return;
        }
        if self.slot_info.leader_state != LeaderState::Sequencing {
            return;
        }
        if self.last_crank_attempt.elapsed() < Duration::from_millis(15) {
            return;
        }

        let prev = if let Some(prev) = self.prev_tip_config {
            Some(prev)
        } else if self.slot_info.first_progress_observed_at.elapsed() >= Nanos::from_millis(100) {
            None
        } else {
            return;
        };

        self.last_crank_attempt = Instant::now();
        self.trigger_crank_build(self.slot_info.current_slot, prev);

        if let Some(bundle) = self.crank_bundle {
            self.send_crank_bundle(&bundle);
        }
    }

    fn recv_crank_bundle(&mut self) {
        let Ok(crank) = self.crank_bundle_rx.try_recv() else { return };
        self.inflight_crank_req = None;
        info!(from_first_progress =% self.slot_info.first_progress_observed_at.elapsed(), "received crank bundle");

        self.tx_buffer.clear();
        bincode::serialize_into(&mut self.tx_buffer, &crank).expect("failed serialization");

        // safe since we generated this internally
        match BundleOffset::new_unchecked(
            BundleId::new_synthetic(),
            std::iter::once(self.tx_buffer.as_slice()),
            &self.allocator,
        ) {
            Ok(bundle) => {
                self.cache.new_bundle(&bundle);
                self.crank_bundle = Some(bundle);
                if self.slot_info.leader_state == LeaderState::Sequencing &&
                    self.pending_jito.is_none()
                {
                    self.send_crank_bundle(&bundle);
                }
            }
            Err(err) => {
                warn!(?err, "failed to allocate crank bundle into shmem");
            }
        }
    }

    fn send_crank_bundle(&mut self, bundle: &BundleOffset) {
        let (msg, hash) = self.handle_schedule_inner(
            &bundle.txs,
            BUNDLE_EXECUTION_FLAGS,
            self.slot_info.current_slot,
        );

        self.workers.send_worker_message(0, msg);
        // Keep the dispatch loop off worker 0 while the crank bundle executes.
        self.workers.thread_inflight[0] = self.workers.thread_inflight[0].saturating_add(1);
        self.pending_jito = Some((hash, Instant::now()));
    }

    fn check_progress(&self, stop: &Arc<AtomicUsize>) {
        if self.last_progress.elapsed() > Duration::from_secs(2) {
            warn!("no recent update from agave, is the validator restarting?");
            stop.store(StopCodes::AGAVE_NO_PROGRESS as usize, Ordering::Relaxed);
        }
    }
}
