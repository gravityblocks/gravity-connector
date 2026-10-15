use std::collections::VecDeque;

use flux::utils::ArrayVec;
use gravity_types::{
    BundleId, SigPrefix,
    consts::MAX_TXS_PER_BUNDLE,
    order::{BundleOffset, TxBytesOffset},
};
use rts_alloc::Allocator;
use rustc_hash::{FxBuildHasher, FxHashMap};
use tracing::info;

const EXPECTED_ORDERS_PER_SLOT: usize = 100_000;

/// All data that is kept for a full leadership (generally 4 slots)
pub struct StateCache {
    // tx sig-prefix -> offset, also serves as dedup
    txs: FxHashMap<SigPrefix, TxBytesOffset>,
    // bundle id -> tx offsets
    bundles: FxHashMap<BundleId, ArrayVec<TxBytesOffset, MAX_TXS_PER_BUNDLE>>,
    allocs: VecDeque<TxBytesOffset>,
    /// Retired allocations occupy the front; new orders append behind them.
    retired_allocs: usize,
}

impl Default for StateCache {
    fn default() -> Self {
        Self::new()
    }
}

impl StateCache {
    pub fn new() -> Self {
        Self {
            txs: FxHashMap::with_capacity_and_hasher(EXPECTED_ORDERS_PER_SLOT, FxBuildHasher),
            bundles: FxHashMap::with_capacity_and_hasher(EXPECTED_ORDERS_PER_SLOT, FxBuildHasher),
            allocs: VecDeque::with_capacity(EXPECTED_ORDERS_PER_SLOT),
            retired_allocs: 0,
        }
    }

    /// Takes ownership of memory, returns whether the tx is new
    pub fn new_tx(&mut self, sig_prefix: SigPrefix, tx: TxBytesOffset) -> bool {
        self.allocs.push_back(tx);
        self.txs.insert(sig_prefix, tx).is_none()
    }

    pub fn new_bundle(&mut self, bundle: &BundleOffset) {
        for tx in bundle.txs {
            self.allocs.push_back(tx);
        }
        self.bundles.insert(bundle.ext_uuid, bundle.txs);
    }

    pub fn get_bundle_offsets(
        &self,
        id: &BundleId,
    ) -> Option<&ArrayVec<TxBytesOffset, MAX_TXS_PER_BUNDLE>> {
        self.bundles.get(id)
    }

    pub fn get_tx_offset(&self, sig_prefix: &SigPrefix) -> Option<TxBytesOffset> {
        self.txs.get(sig_prefix).copied()
    }

    pub fn clear(&mut self) {
        info!(
            txs = self.txs.len(),
            bundles = self.bundles.len(),
            allocs = self.allocs.len() - self.retired_allocs,
            "retiring cached orders"
        );

        self.retired_allocs = self.allocs.len();
        self.txs.clear();
        self.bundles.clear();
    }

    pub fn free_retired(&mut self, allocator: &Allocator, limit: usize) {
        let count = self.retired_allocs.min(limit);
        for tx in self.allocs.drain(..count) {
            tx.free(allocator);
        }
        self.retired_allocs -= count;
    }
}
