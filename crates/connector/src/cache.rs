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
    /// Inserted when receiving new transactions from TPU
    allocs: Vec<TxBytesOffset>,
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
            allocs: Vec::with_capacity(EXPECTED_ORDERS_PER_SLOT),
        }
    }

    /// Takes ownership of memory, returns whether the tx is new
    pub fn new_tx(&mut self, sig_prefix: SigPrefix, tx: TxBytesOffset) -> bool {
        self.allocs.push(tx);
        self.txs.insert(sig_prefix, tx).is_none()
    }

    pub fn new_bundle(&mut self, bundle: &BundleOffset) {
        for tx in bundle.txs {
            self.allocs.push(tx);
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

    pub fn is_empty(&self) -> bool {
        self.allocs.is_empty()
    }

    pub fn clear(&mut self, allocator: &Allocator) {
        info!(
            txs = self.txs.len(),
            bundles = self.bundles.len(),
            allocs = self.allocs.len(),
            "freeing memory"
        );

        for tx in self.allocs.drain(..) {
            tx.0.free(allocator);
        }

        self.txs.clear();
        self.bundles.clear();
    }
}
