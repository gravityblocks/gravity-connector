use agave_scheduler_bindings::{
    SharableTransactionRegion, WorkerToPackMessage, processed_codes,
    worker_message_types::{self, ExecutionResponse, not_included_reasons},
};
use gravity_types::{CSlice, EitherIter2, ExecutionResult, NotIncludedReason};
use rts_alloc::Allocator;
use tracing::warn;

/// Offset of the [`SharableTransactionBatchRegion`], used to correlated results
/// from agave to what we scheduled
#[derive(Clone, Copy, Hash, PartialEq, Eq)]
#[repr(transparent)]
pub struct BatchId(pub usize);

pub struct ExecutionMsg {
    id: BatchId,
    batch: CSlice<SharableTransactionRegion>,
    ptr: CSlice<ExecutionResponse>,
    processed_code: ProcessedCode,
}

impl ExecutionMsg {
    pub fn id(&self) -> BatchId {
        self.id
    }

    pub fn iter_results(&self) -> impl Iterator<Item = ExecutionResult> {
        match self.processed_code {
            ProcessedCode::PROCESSED => {
                let iter = self.ptr.iter().map(|response| {
                    // unwrap ok, as means agave is sending unknown values
                    try_decode_ex_result(response).unwrap()
                });

                EitherIter2::V1(iter)
            }

            ProcessedCode::MAX_WORKING_SLOT_EXCEEDED => {
                let iter = std::iter::repeat_n(
                    ExecutionResult::NotIncluded(NotIncludedReason::MAX_WORKING_SLOT_EXCEEDED),
                    self.batch.len(),
                );
                EitherIter2::V2(iter)
            }
        }
    }

    #[inline]
    pub fn free(self, allocator: &Allocator) {
        self.batch.free(allocator);
        self.ptr.free(allocator);
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WorkerToPackError {
    #[error("agave sent an invalid message tag: {0}")]
    InvalidMessageTag(u8),
    #[error("agave sent an invalid processed code: {0}")]
    InvalidProcessedCode(u8),
    #[error("we sent an invalid code to agave")]
    Invalid,
    #[error("agave sent an invalid not_included_reasons: {0}")]
    InvalidNotIncludedReasons(u8),
}

impl ExecutionMsg {
    /// Here we get back two allocations that we now can free safely:
    /// - an [`agave_scheduler_bindings::SharableTransactionBatchRegion`] that
    ///   we allocated when sending a
    ///   [`agave_scheduler_bindings::PackToWorkerMessage`], this can be now
    ///   freed safely (potentially the pointed txs as well, but those are
    ///   managed separately via Epochs)
    /// - an [`agave_scheduler_bindings::TransactionResponseRegion`] that agave
    ///   allocated with the inner response types (Check or Execution), that is
    ///   safe to free after being processed
    ///
    /// For simplicity we free both of those immediately here, in a future
    /// optimization we could keep them around and reuse the memory / free later
    /// Three potential leaks:
    /// - we (eventually) fail to free the tx pointers
    /// - we fail decoding the processed code or tag and error, but this should
    ///   be unrecoverable anyways
    /// - we fail to receive the `TransactionPtrBatch` back (agave doesnt send
    ///   it, queue is corrupted ..)
    pub fn try_decode(msg: &WorkerToPackMessage, allocator: &Allocator) -> Option<Self> {
        let batch = CSlice {
            ptr: unsafe { allocator.ptr_from_offset(msg.batch.transactions_offset) }
                .as_ptr()
                .cast(),
            length: msg.batch.num_transactions as usize,
        };

        let ptr = CSlice {
            ptr: unsafe { allocator.ptr_from_offset(msg.responses.transaction_responses_offset) }
                .as_ptr()
                .cast(),
            length: msg.responses.num_transaction_responses as usize,
        };

        match Self::validate(msg) {
            Ok(processed_code) => Some(Self {
                id: BatchId(msg.batch.transactions_offset),
                batch,
                ptr,
                processed_code,
            }),
            Err(err) => {
                // triggered by pings
                if err != WorkerToPackError::Invalid {
                    warn!(?err, "failed processing worker message");
                }
                // SAFETY: this is safe to free as both have a check on the length and we assume
                // the message is well formed from agave
                batch.free(allocator);
                ptr.free(allocator);
                None
            }
        }
    }

    fn validate(msg: &WorkerToPackMessage) -> Result<ProcessedCode, WorkerToPackError> {
        let processed_code = ProcessedCode::try_decode(msg.processed_code)?;
        let tag = WorkerMessageTag::try_decode(msg.responses.tag)?;

        match tag {
            WorkerMessageTag::EXECUTION_RESPONSE => Ok(processed_code),
        }
    }
}

fn try_decode_ex_result(msg: &ExecutionResponse) -> Result<ExecutionResult, WorkerToPackError> {
    let not_included_reasons = try_decode_not_included_reason(msg.not_included_reason)?;

    match not_included_reasons {
        NotIncludedReason::NONE => Ok(ExecutionResult::Included {
            execution_slot: msg.execution_slot,
            cost_units: msg.cost_units,
        }),
        reason => Ok(ExecutionResult::NotIncluded(reason)),
    }
}

#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum ProcessedCode {
    PROCESSED = processed_codes::PROCESSED,
    MAX_WORKING_SLOT_EXCEEDED = processed_codes::MAX_WORKING_SLOT_EXCEEDED,
}

impl ProcessedCode {
    fn try_decode(code: u8) -> Result<Self, WorkerToPackError> {
        match code {
            processed_codes::PROCESSED => Ok(Self::PROCESSED),
            processed_codes::MAX_WORKING_SLOT_EXCEEDED => Ok(Self::MAX_WORKING_SLOT_EXCEEDED),
            processed_codes::INVALID => Err(WorkerToPackError::Invalid),
            code => Err(WorkerToPackError::InvalidProcessedCode(code)),
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum WorkerMessageTag {
    EXECUTION_RESPONSE = worker_message_types::EXECUTION_RESPONSE,
    // CHECK_RESPONSE = worker_message_types::CHECK_RESPONSE,
}

impl WorkerMessageTag {
    fn try_decode(tag: u8) -> Result<Self, WorkerToPackError> {
        match tag {
            worker_message_types::EXECUTION_RESPONSE => Ok(Self::EXECUTION_RESPONSE),
            // worker_message_types::CHECK_RESPONSE => Ok(Self::CHECK_RESPONSE),
            tag => Err(WorkerToPackError::InvalidMessageTag(tag)),
        }
    }
}

fn try_decode_not_included_reason(reason: u8) -> Result<NotIncludedReason, WorkerToPackError> {
    match reason {
        not_included_reasons::NONE => Ok(NotIncludedReason::NONE),
        not_included_reasons::BANK_NOT_AVAILABLE => Ok(NotIncludedReason::BANK_NOT_AVAILABLE),
        not_included_reasons::PARTIAL_BATCH_CANCELLED |
        not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE => {
            Ok(NotIncludedReason::ALL_OR_NOTHING_BATCH_FAILURE)
        }
        not_included_reasons::ACCOUNT_IN_USE => Ok(NotIncludedReason::ACCOUNT_IN_USE),
        not_included_reasons::ACCOUNT_LOADED_TWICE => Ok(NotIncludedReason::ACCOUNT_LOADED_TWICE),
        not_included_reasons::ACCOUNT_NOT_FOUND => Ok(NotIncludedReason::ACCOUNT_NOT_FOUND),
        not_included_reasons::PROGRAM_ACCOUNT_NOT_FOUND => {
            Ok(NotIncludedReason::PROGRAM_ACCOUNT_NOT_FOUND)
        }
        not_included_reasons::INSUFFICIENT_FUNDS_FOR_FEE => {
            Ok(NotIncludedReason::INSUFFICIENT_FUNDS_FOR_FEE)
        }
        not_included_reasons::INVALID_ACCOUNT_FOR_FEE => {
            Ok(NotIncludedReason::INVALID_ACCOUNT_FOR_FEE)
        }
        not_included_reasons::ALREADY_PROCESSED => Ok(NotIncludedReason::ALREADY_PROCESSED),
        not_included_reasons::BLOCKHASH_NOT_FOUND => Ok(NotIncludedReason::BLOCKHASH_NOT_FOUND),
        not_included_reasons::INSTRUCTION_ERROR => Ok(NotIncludedReason::INSTRUCTION_ERROR),
        not_included_reasons::CALL_CHAIN_TOO_DEEP => Ok(NotIncludedReason::CALL_CHAIN_TOO_DEEP),
        not_included_reasons::MISSING_SIGNATURE_FOR_FEE => {
            Ok(NotIncludedReason::MISSING_SIGNATURE_FOR_FEE)
        }
        not_included_reasons::INVALID_ACCOUNT_INDEX => Ok(NotIncludedReason::INVALID_ACCOUNT_INDEX),
        not_included_reasons::SIGNATURE_FAILURE => Ok(NotIncludedReason::SIGNATURE_FAILURE),
        not_included_reasons::INVALID_PROGRAM_FOR_EXECUTION => {
            Ok(NotIncludedReason::INVALID_PROGRAM_FOR_EXECUTION)
        }
        not_included_reasons::SANITIZE_FAILURE => Ok(NotIncludedReason::SANITIZE_FAILURE),
        not_included_reasons::CLUSTER_MAINTENANCE => Ok(NotIncludedReason::CLUSTER_MAINTENANCE),
        not_included_reasons::ACCOUNT_BORROW_OUTSTANDING => {
            Ok(NotIncludedReason::ACCOUNT_BORROW_OUTSTANDING)
        }
        not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT => {
            Ok(NotIncludedReason::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT)
        }
        not_included_reasons::UNSUPPORTED_VERSION => Ok(NotIncludedReason::UNSUPPORTED_VERSION),
        not_included_reasons::INVALID_WRITABLE_ACCOUNT => {
            Ok(NotIncludedReason::INVALID_WRITABLE_ACCOUNT)
        }
        not_included_reasons::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT => {
            Ok(NotIncludedReason::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT)
        }
        not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT => {
            Ok(NotIncludedReason::WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT)
        }
        not_included_reasons::TOO_MANY_ACCOUNT_LOCKS => {
            Ok(NotIncludedReason::TOO_MANY_ACCOUNT_LOCKS)
        }
        not_included_reasons::ADDRESS_LOOKUP_TABLE_NOT_FOUND => {
            Ok(NotIncludedReason::ADDRESS_LOOKUP_TABLE_NOT_FOUND)
        }
        not_included_reasons::INVALID_ADDRESS_LOOKUP_TABLE_OWNER => {
            Ok(NotIncludedReason::INVALID_ADDRESS_LOOKUP_TABLE_OWNER)
        }
        not_included_reasons::INVALID_ADDRESS_LOOKUP_TABLE_DATA => {
            Ok(NotIncludedReason::INVALID_ADDRESS_LOOKUP_TABLE_DATA)
        }
        not_included_reasons::INVALID_ADDRESS_LOOKUP_TABLE_INDEX => {
            Ok(NotIncludedReason::INVALID_ADDRESS_LOOKUP_TABLE_INDEX)
        }
        not_included_reasons::INVALID_RENT_PAYING_ACCOUNT => {
            Ok(NotIncludedReason::INVALID_RENT_PAYING_ACCOUNT)
        }
        not_included_reasons::WOULD_EXCEED_MAX_VOTE_COST_LIMIT => {
            Ok(NotIncludedReason::WOULD_EXCEED_MAX_VOTE_COST_LIMIT)
        }
        not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT => {
            Ok(NotIncludedReason::WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT)
        }
        not_included_reasons::DUPLICATE_INSTRUCTION => Ok(NotIncludedReason::DUPLICATE_INSTRUCTION),
        not_included_reasons::INSUFFICIENT_FUNDS_FOR_RENT => {
            Ok(NotIncludedReason::INSUFFICIENT_FUNDS_FOR_RENT)
        }
        not_included_reasons::MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED => {
            Ok(NotIncludedReason::MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED)
        }
        not_included_reasons::INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT => {
            Ok(NotIncludedReason::INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT)
        }
        not_included_reasons::RESANITIZATION_NEEDED => Ok(NotIncludedReason::RESANITIZATION_NEEDED),
        not_included_reasons::PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED => {
            Ok(NotIncludedReason::PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED)
        }
        not_included_reasons::UNBALANCED_TRANSACTION => {
            Ok(NotIncludedReason::UNBALANCED_TRANSACTION)
        }
        not_included_reasons::PROGRAM_CACHE_HIT_MAX_LIMIT => {
            Ok(NotIncludedReason::PROGRAM_CACHE_HIT_MAX_LIMIT)
        }

        r => Err(WorkerToPackError::InvalidNotIncludedReasons(r)),
    }
}
