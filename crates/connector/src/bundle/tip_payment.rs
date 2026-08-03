//! Port of `<https://github.com/jito-foundation/jito-solana/blob/v3.1.6-jito/core/src/tip_manager/tip_payment.rs>` with no meaningful change
#![allow(unused)]

use borsh::{BorshDeserialize, BorshSerialize};
use solana_account::{Account, ReadableAccount};
use solana_address::Address;
use thiserror::Error;
use tracing::error;

#[derive(Debug, PartialEq, Clone, Eq, Error)]
pub enum TipPaymentError {
    #[error("Invalid account owner")]
    InvalidAccountOwner,
    #[error("Invalid discriminator")]
    InvalidDiscriminator,
    #[error("Deserialization error")]
    DeserializationError,
    #[error("Serialization error")]
    SerializationError,
}

pub type TipPaymentResult<T> = std::result::Result<T, TipPaymentError>;

/// `<https://github.com/jito-foundation/jito-programs/blob/8f55af0a9b31ac2192415b59ce2c47329ee255a2/mev-programs/programs/tip-payment/src/lib.rs#L715>`
#[derive(Debug, BorshDeserialize)]
pub struct JitoTipPaymentConfig {
    /// The account claiming tips from the `mev_payment` accounts.
    tip_receiver: Address,

    /// Block builder that receives a % of fees
    block_builder: Address,

    block_builder_commission_pct: u64,

    /// Bumps used to derive PDAs
    bumps: JitoTipPaymentInitBumps,
}

impl JitoTipPaymentConfig {
    const DISCRIMINATOR: &'static [u8] = &[155, 12, 170, 224, 30, 250, 204, 130];

    // CHANGE: use Account (rpc) instead of AccountSharedData (bank)
    pub(crate) fn from_account_data(
        account_data: &Account,
        program_id: &Address,
    ) -> TipPaymentResult<Self> {
        if account_data.owner() != program_id {
            return Err(TipPaymentError::InvalidAccountOwner);
        }

        // run cargo expand -p jito-tip-payment to find this discriminator
        if &account_data.data()[0..8] != Self::DISCRIMINATOR {
            return Err(TipPaymentError::InvalidDiscriminator);
        }

        Self::try_from_slice(&account_data.data()[8..]).map_err(|e| {
            error!("Error deserializing tip payment config account: {e}");
            TipPaymentError::DeserializationError
        })
    }

    pub fn find_program_address(program_id: &Address) -> (Address, u8) {
        Address::find_program_address(&[b"CONFIG_ACCOUNT"], program_id)
    }

    pub fn find_tip_payment_account_pdas(program_id: &Address) -> Vec<(Address, u8)> {
        vec![
            Address::find_program_address(&[b"TIP_ACCOUNT_0"], program_id),
            Address::find_program_address(&[b"TIP_ACCOUNT_1"], program_id),
            Address::find_program_address(&[b"TIP_ACCOUNT_2"], program_id),
            Address::find_program_address(&[b"TIP_ACCOUNT_3"], program_id),
            Address::find_program_address(&[b"TIP_ACCOUNT_4"], program_id),
            Address::find_program_address(&[b"TIP_ACCOUNT_5"], program_id),
            Address::find_program_address(&[b"TIP_ACCOUNT_6"], program_id),
            Address::find_program_address(&[b"TIP_ACCOUNT_7"], program_id),
        ]
    }

    pub fn tip_receiver(&self) -> Address {
        self.tip_receiver
    }

    pub fn block_builder(&self) -> Address {
        self.block_builder
    }

    pub fn block_builder_commission_pct(&self) -> u64 {
        self.block_builder_commission_pct
    }

    pub fn bumps(&self) -> &JitoTipPaymentInitBumps {
        &self.bumps
    }
}

/// `<https://github.com/jito-foundation/jito-programs/blob/8f55af0a9b31ac2192415b59ce2c47329ee255a2/mev-programs/programs/tip-payment/src/lib.rs#L362>`
#[derive(Debug, BorshDeserialize)]
pub struct JitoTipPaymentInitBumps {
    pub config: u8,
    pub tip_payment_account_0: u8,
    pub tip_payment_account_1: u8,
    pub tip_payment_account_2: u8,
    pub tip_payment_account_3: u8,
    pub tip_payment_account_4: u8,
    pub tip_payment_account_5: u8,
    pub tip_payment_account_6: u8,
    pub tip_payment_account_7: u8,
}

pub struct InitializeTipPaymentInstruction;

impl InitializeTipPaymentInstruction {
    const DISCRIMINATOR: &'static [u8] = &[175, 175, 109, 31, 13, 152, 155, 237];

    #[allow(clippy::too_many_arguments)]
    pub fn to_instruction_data(
        config_bump: u8,
        tip_payment_account_0_bump: u8,
        tip_payment_account_1_bump: u8,
        tip_payment_account_2_bump: u8,
        tip_payment_account_3_bump: u8,
        tip_payment_account_4_bump: u8,
        tip_payment_account_5_bump: u8,
        tip_payment_account_6_bump: u8,
        tip_payment_account_7_bump: u8,
    ) -> TipPaymentResult<Vec<u8>> {
        #[derive(BorshSerialize)]
        struct InitBumps {
            config: u8,
            tip_payment_account_0: u8,
            tip_payment_account_1: u8,
            tip_payment_account_2: u8,
            tip_payment_account_3: u8,
            tip_payment_account_4: u8,
            tip_payment_account_5: u8,
            tip_payment_account_6: u8,
            tip_payment_account_7: u8,
        }

        let mut data = Vec::with_capacity(Self::DISCRIMINATOR.len() + 9);
        data.extend_from_slice(Self::DISCRIMINATOR);
        data.extend(
            borsh::to_vec(&InitBumps {
                config: config_bump,
                tip_payment_account_0: tip_payment_account_0_bump,
                tip_payment_account_1: tip_payment_account_1_bump,
                tip_payment_account_2: tip_payment_account_2_bump,
                tip_payment_account_3: tip_payment_account_3_bump,
                tip_payment_account_4: tip_payment_account_4_bump,
                tip_payment_account_5: tip_payment_account_5_bump,
                tip_payment_account_6: tip_payment_account_6_bump,
                tip_payment_account_7: tip_payment_account_7_bump,
            })
            .map_err(|e| {
                error!("Error serializing init bumps: {e}");
                TipPaymentError::SerializationError
            })?,
        );
        Ok(data)
    }
}

pub struct ChangeTipReceiverInstruction;

impl ChangeTipReceiverInstruction {
    const DISCRIMINATOR: &'static [u8] = &[69, 99, 22, 71, 11, 231, 86, 143];

    pub fn to_instruction_data() -> Vec<u8> {
        let mut data = Vec::with_capacity(Self::DISCRIMINATOR.len());
        data.extend_from_slice(Self::DISCRIMINATOR);
        data
    }
}

pub struct ChangeBlockBuilderInstruction;

impl ChangeBlockBuilderInstruction {
    const DISCRIMINATOR: &'static [u8] = &[134, 80, 38, 137, 165, 21, 114, 123];

    pub fn to_instruction_data(block_builder_commission: u64) -> TipPaymentResult<Vec<u8>> {
        let mut data = Vec::with_capacity(Self::DISCRIMINATOR.len() + 8);
        data.extend_from_slice(Self::DISCRIMINATOR);
        data.extend(borsh::to_vec(&block_builder_commission).map_err(|e| {
            error!("Error serializing block builder commission: {e}");
            TipPaymentError::SerializationError
        })?);
        Ok(data)
    }
}
