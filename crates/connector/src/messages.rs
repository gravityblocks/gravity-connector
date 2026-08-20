use flux::timing::Nanos;
use gravity_types::{
    LeaderState, SigPrefix, SlotProgress,
    order::{BundleOffset, TxBytesOffset},
    wire::{BuilderOriginatedOrders, WireMiniBlockGraph},
};
use rts_alloc::Allocator;
use solana_address::Address;
use tracing::{error, warn};

pub struct IngestedOrders {
    pub txs: Vec<(SigPrefix, TxBytesOffset)>,
    pub bundles: Vec<BundleOffset>,
}

pub struct ConnectorMiniBlockMsg {
    pub graph: WireMiniBlockGraph,
    pub new_orders: IngestedOrders,
}

impl ConnectorMiniBlockMsg {
    pub fn new(
        graph: WireMiniBlockGraph,
        orders: &BuilderOriginatedOrders<'_>,
        allocator: &Allocator,
    ) -> Self {
        let mut txs = Vec::with_capacity(orders.txs.len());
        for tx in &orders.txs {
            let Some(sig_prefix) = tx.try_sig_prefix() else {
                warn!("dropping builder tx with invalid signature layout");
                continue;
            };
            match tx.to_shmem(allocator) {
                Ok(offset) => txs.push((sig_prefix, offset)),
                Err(err) => error!(?err, "failed to alloc builder tx in shmem"),
            }
        }

        let mut bundles = Vec::with_capacity(orders.bundles.len());
        for bundle in &orders.bundles {
            match bundle.to_shmem(allocator) {
                Ok(offset) => bundles.push(offset),
                Err(err) => error!(?err, "failed to alloc builder bundle in shmem"),
            }
        }

        Self { graph, new_orders: IngestedOrders { txs, bundles } }
    }
}

#[derive(Clone, Copy)]
pub enum BridgeToNetworkFlow {
    TpuTransaction {
        sig_prefix: SigPrefix,
        tx: TxBytesOffset,
        received_at: Nanos,
        src_addr: [u8; 16],
        alloc_gen: u64,
    },
}

#[derive(Clone, Copy)]
pub enum BridgeToNetworkControl {
    Progress { progress: SlotProgress, alloc_gen: u64 },
    ReadyForTips(u64),
    CrankBundle { bundle: BundleOffset },
}

#[derive(Clone, Copy, Default)]
pub struct ConnectorProgressTracker {
    pub current_slot: u64,
    pub leader_state: LeaderState,
    pub first_progress_observed_at: Nanos,
    /// Advanced whenever the bridge clears its order lookups.
    pub alloc_gen: u64,
}

impl ConnectorProgressTracker {
    pub fn accepts_allocations(&self) -> bool {
        self.current_slot != 0 && self.leader_state != LeaderState::Inactive
    }

    pub fn accepts_alloc_gen(&self, alloc_gen: u64) -> bool {
        self.accepts_allocations() && alloc_gen == self.alloc_gen
    }

    pub fn advance_alloc_gen(&mut self) {
        self.alloc_gen += 1;
    }

    pub fn update(&mut self, progress: SlotProgress) -> bool {
        if self.current_slot != progress.slot_num || self.first_progress_observed_at == Nanos::ZERO
        {
            self.first_progress_observed_at = progress.observed_at;
        }
        self.current_slot = progress.slot_num;
        self.leader_state.update(progress.slot_num, progress.next_leadership.into())
    }
}

pub enum NetworkToBridge {
    MiniBlockGraph { received_at: Nanos, alloc_gen: u64, msg: ConnectorMiniBlockMsg },
    JitoTransaction { sig_prefix: SigPrefix, tx: TxBytesOffset, alloc_gen: u64 },
    JitoBundle { bundle: BundleOffset, alloc_gen: u64 },
    CrankBundle { bundle: BundleOffset, alloc_gen: u64 },
    PreviousTipReceiver { slot: u64, tip_receiver: Address, block_builder: Address },
}
