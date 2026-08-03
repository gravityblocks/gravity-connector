//! Wire messages for connector <-> builder communication, any change here is
//! potentially a breaking change!

mod bootstrap;
mod order;

use std::net::SocketAddr;

use agave_scheduler_bindings::pack_message_flags::execution_flags::{
    ALL_OR_NOTHING, DROP_ON_FAILURE,
};
pub use bootstrap::{
    AuthProof, BOOTSTRAP_MAGIC, BootstrapDecodeError, BootstrapFrame, ClientHello, RejectReason,
    ServerHello, decode_bootstrap_frame, encode_bootstrap_frame, is_bootstrap_frame,
    sign_auth_proof, verify_auth_proof,
};
use flux::{
    timing::Nanos,
    type_hash_derive::{TypeHash, type_hash_lock},
    utils::{ArrayStr, ArrayVec},
};
pub use order::{WireSharableBundle, WireSharableTx};
use serde::{Deserialize, Serialize};
use solana_address::Address;
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::{
    BatchUuid, BundleId, MiniBlockUuid, NotIncludedReason, SigPrefix,
    consts::{MAX_TXS_PER_BUNDLE, MAX_TXS_PER_MESSAGE},
    execution_result::ExecutionResult,
    progress::SlotMessage,
};

wincode::pod_wrapper! {
    unsafe struct PodPubkey(Address);
    unsafe struct PodBundleId(BundleId);
}

/// `ThreadIdx::MAX` must be at least [`crate::consts::MAX_THREADS`]
pub type ThreadIdx = u8;
pub type NumThreads = u8;
pub type CuCost = u64;
pub type SlotNum = u64;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, SchemaRead, SchemaWrite, TypeHash)]
pub enum ConnectorToBuilder<'a> {
    #[wincode(tag = 0)]
    Handshake(Handshake),
    #[wincode(tag = 1)]
    Transaction {
        order: WireSharableTx<'a>,
        sent_at: Nanos,
        received_at: Nanos,
        // octets
        src_addr: [u8; 16],
        #[type_hash(literal = "Option<ArrayStr<64>>")]
        source_uri: Option<ArrayStr<64>>,
    },
    #[wincode(tag = 2)]
    Progress(SlotMessage),
    #[wincode(tag = 3)]
    ExecutionResult(BatchExecutionResult),
    #[wincode(tag = 4)]
    ReadyForTips(u64),
    #[wincode(tag = 5)]
    CrankBundle(WireSharableBundle<'a>),
    #[wincode(tag = 6)]
    Bundle {
        bundle: WireSharableBundle<'a>,
        #[type_hash(literal = "ArrayStr<64>")]
        source_uri: ArrayStr<64>,
        received_at: Nanos,
    },
}

#[derive(Debug, Copy, Clone, SchemaRead, SchemaWrite, TypeHash)]
#[repr(C)]
pub struct WireDispatchTelem {
    pub batch_uuid: BatchUuid,
    pub orders: BatchOrders,
    pub worker: ThreadIdx,
    pub num_txs: u8,
    pub slot: SlotNum,
    /// The mini-block graph this batch came from.
    pub mini_block_uuid: MiniBlockUuid,
    /// Time taken to write the batch to the Agave worker.
    pub send_ns: u64,
}

#[allow(clippy::large_enum_variant)]
#[derive(SchemaRead, SchemaWrite, TypeHash)]
pub enum BuilderToConnector<'a> {
    #[wincode(tag = 0)]
    MiniBlockGraph { graph: WireMiniBlockGraph, orders: BuilderOriginatedOrders<'a> },
    #[wincode(tag = 1)]
    DeleteFailsafe,
    #[wincode(tag = 2)]
    PreviousTipReceiver {
        slot: SlotNum,
        #[wincode(with = "PodPubkey")]
        #[type_hash(literal = "Address")]
        tip_receiver: Address,
        #[wincode(with = "PodPubkey")]
        #[type_hash(literal = "Address")]
        block_builder: Address,
    },
    #[wincode(tag = 3)]
    ShredReceiverAddresses(#[type_hash(literal = "Vec<SocketAddr>")] Vec<SocketAddr>),
}

/// Mini-block dependency DAG in CSR form.
///
/// Node `i`'s successors are `col_indices[row_offsets[i]..row_offsets[i + 1]]`;
/// edge `i -> s` means `i` must complete before `s` is ready. The connector
/// derives in-degrees itself.
#[derive(Debug, Clone, SchemaRead, SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 12249800253876661219)]
pub struct WireMiniBlockGraph {
    pub slot: SlotNum,
    pub index_in_slot: u32,
    pub uuid: MiniBlockUuid,
    pub row_offsets: Vec<u32>,
    pub col_indices: Vec<u32>,
    pub nodes: Vec<CsrNode>,
}

#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite, TypeHash)]
#[repr(C)]
pub struct CsrNode {
    pub order_ref: NodeOrderRef,
    pub depth: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite, TypeHash, Hash)]
#[repr(C)]
pub enum NodeOrderRef {
    Tx {
        sig: SigPrefix,
    },
    Bundle {
        #[wincode(with = "PodBundleId")]
        #[type_hash(literal = "BundleId")]
        id: BundleId,
    },
}

/// Builder-originated bytes for a graph: loose txs plus every referenced bundle
/// (the connector resolves graph nodes only from these, so send them all).
#[derive(SchemaRead, SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 13577110504667255382)]
pub struct BuilderOriginatedOrders<'a> {
    pub txs: Vec<WireSharableTx<'a>>,
    pub bundles: Vec<WireSharableBundle<'a>>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, TypeHash)]
#[type_hash_lock(hash = 17404160882666623473)]
pub struct Handshake {
    /// Vote identity of the validator
    #[wincode(with = "PodPubkey")]
    #[type_hash(literal = "Address")]
    pub identity: Address,
    /// Connector version
    pub conn_version: String,
    /// Number of threads available for execution
    pub num_threads: NumThreads,
    pub filter_ofac: bool,
}

#[derive(Debug, Copy, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize, TypeHash)]
#[type_hash_lock(hash = 3424787256992275135)]
#[repr(C)]
pub struct BatchExecutionResult {
    pub slot: SlotNum,
    pub mini_block_uuid: MiniBlockUuid,
    /// Which orders these results pertain to (connector owns batch
    /// composition).
    pub orders: BatchOrders,
    pub results: ArrayVec<ExecutionResult, MAX_TXS_PER_MESSAGE>,
    // network received it
    pub batch_received_at: Nanos,
    // forwarded to agave
    pub forwarded_at: Nanos,
    // agave returned it
    pub res_received_at: Nanos,
    pub worker: ThreadIdx,
}

impl BatchExecutionResult {
    pub fn reject_orders(
        slot: SlotNum,
        mini_block_uuid: MiniBlockUuid,
        orders: BatchOrders,
        num_txs: usize,
        received_at: Nanos,
        failure: NotIncludedReason,
    ) -> Self {
        assert!(num_txs <= MAX_TXS_PER_MESSAGE, "num_txs too large");
        // Same shape as a real failed bundle: the failing tx's reason first,
        // the rest all-or-nothing failures.
        let results = (0..num_txs)
            .map(|i| {
                ExecutionResult::NotIncluded(if i == 0 {
                    failure
                } else {
                    NotIncludedReason::ALL_OR_NOTHING_BATCH_FAILURE
                })
            })
            .collect();
        Self {
            slot,
            mini_block_uuid,
            orders,
            results,
            batch_received_at: received_at,
            forwarded_at: Nanos::ZERO,
            res_received_at: Nanos::ZERO,
            worker: ThreadIdx::MAX,
        }
    }
}

/// Currently we assume these are always together, such that if any tx fails the
/// whole bundle reverts. Since we can't specify the index of which txs are
/// allowed to fail this is probably ok to assume
pub const BUNDLE_EXECUTION_FLAGS: u16 = ALL_OR_NOTHING | DROP_ON_FAILURE;

const _: () = assert!(MAX_TXS_PER_BUNDLE < MAX_TXS_PER_MESSAGE);

#[allow(clippy::large_enum_variant)]
#[derive(
    Debug,
    Clone,
    Copy,
    SchemaRead,
    SchemaWrite,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    TypeHash,
)]
#[repr(C)]
pub enum BatchOrders {
    Transactions {
        txs: ArrayVec<SigPrefix, MAX_TXS_PER_MESSAGE>,
    },
    Bundle {
        #[wincode(with = "PodBundleId")]
        #[type_hash(literal = "BundleId")]
        id: BundleId,
    },
}

impl BatchOrders {
    pub fn execution_flags(&self) -> u16 {
        match self {
            Self::Transactions { .. } => 0,
            Self::Bundle { .. } => BUNDLE_EXECUTION_FLAGS,
        }
    }
}
