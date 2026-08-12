use flux::timing::Nanos;
use gravity_types::{
    LeaderState, SigPrefix, SlotProgress,
    order::{BundleOffset, TxBytesOffset},
    wire::{BuilderOriginatedOrders, WireMiniBlockGraph},
};
use rts_alloc::Allocator;
use solana_address::Address;
use tracing::error;

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
            match tx.to_shmem(allocator) {
                Ok(offset) => txs.push((tx.sig_prefix(), offset)),
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
pub enum BridgeToNetwork {
    TpuTransaction { tx: TxBytesOffset, received_at: Nanos, src_addr: [u8; 16] },
    Progress(SlotProgress),
    ReadyForTips(u64),
    CrankBundle { bundle: BundleOffset },
}

#[derive(Clone, Copy, Default)]
pub struct ConnectorProgressTracker {
    pub current_slot: u64,
    pub leader_state: LeaderState,
    pub first_progress_observed_at: Nanos,
}

impl ConnectorProgressTracker {
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
    MiniBlockGraph { received_at: Nanos, msg: ConnectorMiniBlockMsg },
    JitoTransaction { sig_prefix: SigPrefix, tx: TxBytesOffset },
    JitoBundle { bundle: BundleOffset },
    PreviousTipReceiver { slot: u64, tip_receiver: Address, block_builder: Address },
}
