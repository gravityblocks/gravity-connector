use agave_scheduler_bindings::worker_message_types::not_included_reasons;
use flux::type_hash_derive::TypeHash;
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::wire::CuCost;

#[derive(
    Debug, Clone, Copy, SchemaRead, SchemaWrite, serde::Serialize, serde::Deserialize, TypeHash,
)]
#[repr(C)]
pub enum ExecutionResult {
    Included {
        /// The slot this transaction was executed.
        ///
        /// # Note
        ///
        /// The current semantics are:
        ///
        /// - If we successfully get a bank and try your request, this slot is
        ///   the latest bank we attempted (during a slot roll we can try 2
        ///   banks ).
        /// - If we do not attempt or attempt and fail to get a bank, this field
        ///   will be zero.
        execution_slot: u64,
        /// Cost units used by the transaction.
        cost_units: CuCost,
    },
    // This is guaranteed to be != NotIncludedReason::NONE
    NotIncluded(NotIncludedReason),
}

#[allow(non_camel_case_types)]
#[derive(
    Debug,
    Clone,
    Copy,
    SchemaRead,
    SchemaWrite,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Hash,
    TypeHash,
)]
#[repr(u8)]
pub enum NotIncludedReason {
    /// The transaction was included in the block.
    NONE = not_included_reasons::NONE,

    /// The transaction could not attempt processing because the
    /// working bank was unavailable.
    BANK_NOT_AVAILABLE = not_included_reasons::BANK_NOT_AVAILABLE,

    /// Transaction dropped because the batch was marked as
    /// `all_or_nothing` and a different transacation failed.
    ALL_OR_NOTHING_BATCH_FAILURE = not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE,

    // Remaining errors are translations from SDK.
    // Moved up to 64 so we have room to add custom reasons in the future.
    // Also allows for easy distinguishing between custom scheduling errors
    // and sdk errors.
    /// An account is already being processed in another transaction in a way
    /// that does not support parallelism
    ACCOUNT_IN_USE = not_included_reasons::ACCOUNT_IN_USE,
    /// A `Pubkey` appears twice in the transaction's `account_keys`.
    /// Instructions can reference `Pubkey`s more than once but the message
    /// must contain a list with no duplicate keys
    ACCOUNT_LOADED_TWICE = not_included_reasons::ACCOUNT_LOADED_TWICE,
    /// Attempt to debit an account but found no record of a prior credit.
    ACCOUNT_NOT_FOUND = not_included_reasons::ACCOUNT_NOT_FOUND,
    /// Attempt to load a program that does not exist
    PROGRAM_ACCOUNT_NOT_FOUND = not_included_reasons::PROGRAM_ACCOUNT_NOT_FOUND,
    /// The from `Pubkey` does not have sufficient balance to pay the fee to
    /// schedule the transaction
    INSUFFICIENT_FUNDS_FOR_FEE = not_included_reasons::INSUFFICIENT_FUNDS_FOR_FEE,
    /// This account may not be used to pay transaction fees
    INVALID_ACCOUNT_FOR_FEE = not_included_reasons::INVALID_ACCOUNT_FOR_FEE,
    /// The bank has seen this transaction before. This can occur under normal
    /// operation
    ALREADY_PROCESSED = not_included_reasons::ALREADY_PROCESSED,
    /// The bank has not seen the given `recent_blockhash`
    BLOCKHASH_NOT_FOUND = not_included_reasons::BLOCKHASH_NOT_FOUND,
    /// An error occurred while processing an instruction.
    INSTRUCTION_ERROR = not_included_reasons::INSTRUCTION_ERROR,
    /// Loader call chain is too deep
    CALL_CHAIN_TOO_DEEP = not_included_reasons::CALL_CHAIN_TOO_DEEP,
    /// Transaction requires a fee but has no signature present
    MISSING_SIGNATURE_FOR_FEE = not_included_reasons::MISSING_SIGNATURE_FOR_FEE,
    /// Transaction contains an invalid account reference
    INVALID_ACCOUNT_INDEX = not_included_reasons::INVALID_ACCOUNT_INDEX,
    /// Transaction did not pass signature verification
    SIGNATURE_FAILURE = not_included_reasons::SIGNATURE_FAILURE,
    /// This program may not be used for executing instructions
    INVALID_PROGRAM_FOR_EXECUTION = not_included_reasons::INVALID_PROGRAM_FOR_EXECUTION,
    /// Transaction failed to sanitize accounts offsets correctly
    SANITIZE_FAILURE = not_included_reasons::SANITIZE_FAILURE,
    CLUSTER_MAINTENANCE = not_included_reasons::CLUSTER_MAINTENANCE,
    /// Transaction processing left an account with an outstanding borrowed
    /// reference
    ACCOUNT_BORROW_OUTSTANDING = not_included_reasons::ACCOUNT_BORROW_OUTSTANDING,
    /// Transaction would exceed max Block Cost Limit
    WOULD_EXCEED_MAX_BLOCK_COST_LIMIT = not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT,
    /// Transaction version is unsupported
    UNSUPPORTED_VERSION = not_included_reasons::UNSUPPORTED_VERSION,
    /// Transaction loads a writable account that cannot be written
    INVALID_WRITABLE_ACCOUNT = not_included_reasons::INVALID_WRITABLE_ACCOUNT,
    /// Transaction would exceed max account limit within the block
    WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT = not_included_reasons::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT,
    /// Transaction would exceed account data limit within the block
    WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT =
        not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT,
    /// Transaction locked too many accounts
    TOO_MANY_ACCOUNT_LOCKS = not_included_reasons::TOO_MANY_ACCOUNT_LOCKS,
    /// Address lookup table not found
    ADDRESS_LOOKUP_TABLE_NOT_FOUND = not_included_reasons::ADDRESS_LOOKUP_TABLE_NOT_FOUND,
    /// Attempted to lookup addresses from an account owned by the wrong program
    INVALID_ADDRESS_LOOKUP_TABLE_OWNER = not_included_reasons::INVALID_ADDRESS_LOOKUP_TABLE_OWNER,
    /// Attempted to lookup addresses from an invalid account
    INVALID_ADDRESS_LOOKUP_TABLE_DATA = not_included_reasons::INVALID_ADDRESS_LOOKUP_TABLE_DATA,
    /// Address table lookup uses an invalid index
    INVALID_ADDRESS_LOOKUP_TABLE_INDEX = not_included_reasons::INVALID_ADDRESS_LOOKUP_TABLE_INDEX,
    /// Transaction leaves an account with a lower balance than rent-exempt
    /// minimum
    INVALID_RENT_PAYING_ACCOUNT = not_included_reasons::INVALID_RENT_PAYING_ACCOUNT,
    /// Transaction would exceed max Vote Cost Limit
    WOULD_EXCEED_MAX_VOTE_COST_LIMIT = not_included_reasons::WOULD_EXCEED_MAX_VOTE_COST_LIMIT,
    /// Transaction would exceed total account data limit
    WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT =
        not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT,
    /// Transaction contains a duplicate instruction that is not allowed
    DUPLICATE_INSTRUCTION = not_included_reasons::DUPLICATE_INSTRUCTION,
    /// Transaction results in an account with insufficient funds for rent
    INSUFFICIENT_FUNDS_FOR_RENT = not_included_reasons::INSUFFICIENT_FUNDS_FOR_RENT,
    /// Transaction exceeded max loaded accounts data size cap
    MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED =
        not_included_reasons::MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED,
    /// `LoadedAccountsDataSizeLimit` set for transaction must be greater than
    /// 0.
    INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT =
        not_included_reasons::INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT,
    /// Sanitized transaction differed before/after feature activation. Needs to
    /// be resanitized.
    RESANITIZATION_NEEDED = not_included_reasons::RESANITIZATION_NEEDED,
    /// Program execution is temporarily restricted on an account.
    PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED =
        not_included_reasons::PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED,
    /// The total balance before the transaction does not equal the total
    /// balance after the transaction
    UNBALANCED_TRANSACTION = not_included_reasons::UNBALANCED_TRANSACTION,
    /// Program cache hit max limit.
    PROGRAM_CACHE_HIT_MAX_LIMIT = not_included_reasons::PROGRAM_CACHE_HIT_MAX_LIMIT,

    // Remaining errors are not from agave and don't have a corresponding
    // [`not_included_reasons`], bumped to 200 to avoid conflicts
    /// Represent getting a [`processed_codes::MAX_WORKING_SLOT_EXCEEDED`]
    MAX_WORKING_SLOT_EXCEEDED = 200,

    /// This would result in a [`not_included_reasons::BANK_NOT_AVAILABLE`] but
    /// it chokes a worker thread for 50-100ms because of some timeout logic in
    /// agave
    NOT_SEQUENCING = 220,

    /// Builder send an id an unknown order
    UNKNOWN_ORDERS,

    /// The slot ended before the connector dispatched the order.
    SLOT_ENDED,
}
