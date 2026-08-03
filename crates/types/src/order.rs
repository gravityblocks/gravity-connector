use agave_scheduler_bindings::SharableTransactionRegion;
use flux::utils::ArrayVec;
use gravity_protos::packet::Packet;
use rts_alloc::Allocator;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    BundleId, CSlice, CSliceOffset,
    consts::{MAX_ALLOCATION_SZ, MAX_TXS_PER_BUNDLE},
};

#[derive(Debug, thiserror::Error)]
pub enum OrderAllocError {
    #[error("tx empty")]
    EmptyTx,
    #[error("allocation failed")]
    AllocFailed,
    #[error("bundle empty")]
    EmptyBundle,
    #[error("too many txs in bundle: {0}")]
    TooManyTxsInBundle(usize),
    #[error("transaction is too large: tx {0}, size: {1}")]
    TxTooLarge(usize, usize),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(C)]
pub enum OrderData<T, B> {
    Tx(T),
    Bundle(B),
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BundleOffset {
    pub txs: ArrayVec<TxBytesOffset, MAX_TXS_PER_BUNDLE>,
    pub ext_uuid: BundleId,
}

impl BundleOffset {
    #[inline]
    pub fn free(self, allocator: &Allocator) {
        for tx in self.txs {
            tx.free(allocator);
        }
    }

    /// Validates and allocates an external bundle
    pub fn new_from_jito(
        ext_uuid: BundleId,
        packets: &[Packet],
        allocator: &Allocator,
    ) -> Result<Self, OrderAllocError> {
        if packets.is_empty() {
            return Err(OrderAllocError::EmptyBundle);
        }

        if packets.len() > MAX_TXS_PER_BUNDLE {
            return Err(OrderAllocError::TooManyTxsInBundle(packets.len()));
        }

        for (i, tx) in packets.iter().enumerate() {
            if tx.data.is_empty() {
                return Err(OrderAllocError::EmptyTx);
            }

            if tx.data.len() > MAX_ALLOCATION_SZ {
                return Err(OrderAllocError::TxTooLarge(i, tx.data.len()));
            }
        }

        Self::new_unchecked(ext_uuid, packets.iter().map(|p| p.data.as_slice()), allocator)
    }

    /// Allocate Jito bundle into shmem.
    /// Takes protobuf bundle type and converts to our internal type.
    pub fn new_unchecked<'a>(
        ext_uuid: BundleId,
        packets: impl Iterator<Item = &'a [u8]>,
        allocator: &Allocator,
    ) -> Result<Self, OrderAllocError> {
        let mut txs: ArrayVec<TxBytesOffset, MAX_TXS_PER_BUNDLE> = ArrayVec::new();

        for (idx, packet) in packets.enumerate() {
            let tx_length = packet.len();
            let Some(allocation) = allocator.allocate(tx_length as u32) else {
                warn!(idx, tx_length, ?ext_uuid, "failed to alloc tx in jito bundle");
                for tx in &txs {
                    tx.free(allocator);
                }
                return Err(OrderAllocError::AllocFailed);
            };
            let tx_offset = unsafe {
                core::ptr::copy_nonoverlapping(packet.as_ptr(), allocation.as_ptr(), tx_length);
                allocator.offset(allocation)
            };
            txs.push(TxBytesOffset::new(tx_offset, tx_length));
        }

        Ok(Self { txs, ext_uuid })
    }
}

#[derive(Clone, Copy)]
pub struct TxBytesSlice(pub CSlice<u8>);
impl TxBytesSlice {
    pub fn from_offset(offset: TxBytesOffset, allocator: &Allocator) -> Self {
        Self(CSlice::from_offset(offset.0, allocator))
    }
    pub fn free(&self, allocator: &Allocator) {
        self.0.free(allocator);
    }
}
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct TxBytesOffset(pub CSliceOffset<u8>);
impl TxBytesOffset {
    pub fn new(offset: usize, length: usize) -> Self {
        Self(CSliceOffset::new(offset, length))
    }

    pub fn free(&self, allocator: &Allocator) {
        self.0.free(allocator);
    }
}

impl From<SharableTransactionRegion> for TxBytesOffset {
    fn from(value: SharableTransactionRegion) -> Self {
        Self(CSliceOffset::new(value.offset, value.length as usize))
    }
}
