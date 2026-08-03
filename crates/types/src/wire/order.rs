use std::ptr::copy_nonoverlapping;

use flux::{
    type_hash_derive::{TypeHash, type_hash_lock},
    utils::ArrayVec,
};
use rts_alloc::Allocator;
use tracing::warn;

use crate::{
    BundleId, SigPrefix,
    consts::MAX_TXS_PER_BUNDLE,
    order::{BundleOffset, OrderAllocError, TxBytesOffset, TxBytesSlice},
};

wincode::pod_wrapper! {
    unsafe struct PodBundleId(BundleId);
}

/// Zero-copy wire type. Holds references into network buffer.
#[derive(Debug, Clone, Copy, wincode_derive::SchemaRead, wincode_derive::SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 8763772824538344631)]
pub struct WireSharableTx<'a> {
    pub tx_bytes: &'a [u8],
}

impl<'a> WireSharableTx<'a> {
    #[inline]
    pub fn from_shmem(tx: &TxBytesOffset, allocator: &'a Allocator) -> Self {
        WireSharableTx { tx_bytes: TxBytesSlice::from_offset(*tx, allocator).0.to_static_slice() }
    }

    #[inline]
    pub fn to_shmem(&self, allocator: &'a Allocator) -> Result<TxBytesOffset, OrderAllocError> {
        let tx_length = self.tx_len();

        if tx_length == 0 {
            return Err(OrderAllocError::EmptyTx);
        }
        let Some(tx_allocation) = allocator.allocate(tx_length as u32) else {
            return Err(OrderAllocError::AllocFailed);
        };
        let tx_offset = unsafe {
            copy_nonoverlapping(self.tx_bytes.as_ptr(), tx_allocation.as_ptr(), tx_length);
            allocator.offset(tx_allocation)
        };

        Ok(TxBytesOffset::new(tx_offset, tx_length))
    }

    #[inline]
    pub fn tx_len(&self) -> usize {
        self.tx_bytes.len()
    }

    #[inline]
    pub fn sig_prefix(&self) -> SigPrefix {
        assert_ne!(self.tx_bytes[0], 0);
        assert!(self.tx_bytes.len() > SigPrefix::LEN);
        let mut out = [0u8; SigPrefix::LEN];
        out.copy_from_slice(&self.tx_bytes[1..=SigPrefix::LEN]);
        SigPrefix(out)
    }
}

/// Zero-copy wire type. `txs` hold references into network buffer.
#[derive(Debug, Clone, Copy, wincode_derive::SchemaRead, wincode_derive::SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 9205047370917394423)]
pub struct WireSharableBundle<'a> {
    #[wincode(with = "PodBundleId")]
    pub ext_uuid: BundleId,
    pub txs: ArrayVec<WireSharableTx<'a>, MAX_TXS_PER_BUNDLE>,
}

impl WireSharableBundle<'_> {
    #[inline]
    pub fn from_shmem<'b>(
        bundle: &BundleOffset,
        allocator: &'b Allocator,
    ) -> WireSharableBundle<'b> {
        let mut txs = ArrayVec::new();
        for tx in &bundle.txs {
            txs.push(WireSharableTx::from_shmem(tx, allocator));
        }

        WireSharableBundle { txs, ext_uuid: bundle.ext_uuid }
    }

    /// Allocate wire bundle into shmem.
    pub fn to_shmem(&self, allocator: &Allocator) -> Result<BundleOffset, OrderAllocError> {
        if self.txs.is_empty() {
            return Err(OrderAllocError::EmptyBundle);
        }
        let mut txs: ArrayVec<TxBytesOffset, MAX_TXS_PER_BUNDLE> = ArrayVec::new();
        for (idx, wire_tx) in self.txs.iter().enumerate() {
            match wire_tx.to_shmem(allocator) {
                Ok(tx) => txs.push(tx),
                Err(err) => {
                    warn!(ext_uuid=?self.ext_uuid, idx, ?err, "failed to alloc tx in bundle");
                    for tx in &txs {
                        tx.free(allocator);
                    }
                    return Err(err);
                }
            }
        }
        Ok(BundleOffset { txs, ext_uuid: self.ext_uuid })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn make_allocator() -> anyhow::Result<Allocator> {
        let file = tempfile::tempfile()?;
        unsafe { Allocator::create(&file, 30 * 1024 * 1024 * 1024, 1, 4 * 1024 * 1024) }
            .map_err(Into::into)
    }

    #[test]
    fn wire_tx_roundtrip() {
        let allocator = make_allocator().unwrap();

        let tx_data = vec![0x4; 152];

        let wire_tx = WireSharableTx { tx_bytes: &tx_data };

        // Test ser/ deser from raw data
        let serialised = wincode::serialize(&wire_tx).unwrap();
        let wire_tx_deser: WireSharableTx = wincode::deserialize(&serialised).unwrap();

        assert_eq!(wire_tx.tx_bytes, wire_tx_deser.tx_bytes);

        // Test alloc
        let sharable_tx = wire_tx_deser.to_shmem(&allocator).unwrap();

        let shmem_tx_bytes = TxBytesSlice::from_offset(sharable_tx, &allocator);

        assert_eq!(&*shmem_tx_bytes.0, &tx_data);

        // Test ser/ deser from alloc
        let wire_tx_from_shmem = WireSharableTx::from_shmem(&sharable_tx, &allocator);
        let serialised_from_shmem = wincode::serialize(&wire_tx_from_shmem).unwrap();
        assert_eq!(serialised, serialised_from_shmem);

        sharable_tx.free(&allocator);
    }

    #[test]
    fn wire_bundle_roundtrip() {
        let allocator = make_allocator().unwrap();

        let tx1_data = vec![0x41; 100];
        let tx2_data = vec![0x42; 200];
        let tx3_data = vec![0x43; 150];

        let mut txs = ArrayVec::new();
        txs.push(WireSharableTx { tx_bytes: &tx1_data });
        txs.push(WireSharableTx { tx_bytes: &tx2_data });
        txs.push(WireSharableTx { tx_bytes: &tx3_data });
        let wire_bundle = WireSharableBundle { ext_uuid: BundleId::new_synthetic(), txs };

        // Test ser/deser from raw data
        let serialised = wincode::serialize(&wire_bundle).unwrap();
        let wire_bundle_deser: WireSharableBundle = wincode::deserialize(&serialised).unwrap();

        assert_eq!(wire_bundle.txs.len(), wire_bundle_deser.txs.len());

        // Test alloc
        let sharable_bundle = wire_bundle_deser.to_shmem(&allocator).unwrap();

        assert_eq!(sharable_bundle.txs.len(), 3);

        let tx0_bytes = TxBytesSlice::from_offset(sharable_bundle.txs[0], &allocator);
        assert_eq!(&*tx0_bytes.0, &tx1_data);

        let tx1_bytes = TxBytesSlice::from_offset(sharable_bundle.txs[1], &allocator);
        assert_eq!(&*tx1_bytes.0, &tx2_data);

        // Test ser/deser from shmem
        let wire_bundle_from_shmem = WireSharableBundle::from_shmem(&sharable_bundle, &allocator);
        let serialised_from_shmem = wincode::serialize(&wire_bundle_from_shmem).unwrap();

        let deser_from_shmem: WireSharableBundle =
            wincode::deserialize(&serialised_from_shmem).unwrap();

        assert_eq!(wire_bundle_deser.txs.len(), deser_from_shmem.txs.len());

        for (tx1, tx2) in wire_bundle_deser.txs.iter().zip(deser_from_shmem.txs.iter()) {
            assert_eq!(tx1.tx_bytes, tx2.tx_bytes);
        }

        sharable_bundle.free(&allocator);
    }
}
