use flux::timing::Nanos;
use gravity_types::{
    SigPrefix, SlotMessage,
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
    Progress(SlotMessage),
    ReadyForTips(u64),
    CrankBundle { bundle: BundleOffset },
}

pub enum NetworkToBridge {
    MiniBlockGraph { received_at: Nanos, msg: ConnectorMiniBlockMsg },
    JitoTransaction { sig_prefix: SigPrefix, tx: TxBytesOffset },
    JitoBundle { bundle: BundleOffset },
    PreviousTipReceiver { slot: u64, tip_receiver: Address, block_builder: Address },
}
