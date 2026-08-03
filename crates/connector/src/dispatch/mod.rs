mod dag;

pub use dag::Dag;
use flux::timing::Nanos;
use gravity_types::{
    MiniBlockUuid,
    wire::{BatchOrders, SlotNum},
};

pub const MAX_INFLIGHT_PER_WORKER: u8 = 1;

pub struct PendingBatch {
    pub orders: BatchOrders,
    pub worker: u8,
    pub node: u32,
    pub graph_id: (SlotNum, MiniBlockUuid),
    pub received_at: Nanos,
    pub forwarded_at: Nanos,
}
