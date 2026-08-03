//! Port of `<https://github.com/jito-foundation/jito-solana/blob/v3.1.6-jito/core/src/tip_manager.rs>` with a few key changes.
//! The original version had access to the `Bank` to fetch account data,
//! blockhash and current epoch. Since this is not possible in the connector,
//! we use instead a blocking RPC Client pointing to the local agave. This is
//! not ideal but the calls should be made at most once at the beginning of
//! every sequencing slot.

use flux::timing::Instant;
use solana_account::{Account, ReadableAccount};
use solana_address::Address;
use solana_clock::Epoch;
use solana_commitment_config::CommitmentConfig;
use solana_hash::Hash;
use solana_instruction::{AccountMeta, Instruction};
use solana_keypair::Keypair;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::client_error::Error as ClientError;
use solana_sdk_ids::system_program;
use solana_signer::Signer;
use solana_transaction::{Transaction, versioned::VersionedTransaction};
use thiserror::Error;
use tracing::{debug, info};

use crate::bundle::{
    BlockBuilderFeeInfo,
    tip_distribution::{
        InitializeTipDistributionAccountInstruction, JitoTipDistributionConfig,
        TipDistributionAccount, TipDistributionError,
    },
    tip_payment::{
        ChangeBlockBuilderInstruction, ChangeTipReceiverInstruction, JitoTipPaymentConfig,
        TipPaymentError,
    },
};

#[derive(Debug, Error)]
pub enum TipManagerError {
    #[error("Tip payment config missing")]
    TipPaymentConfigMissing,
    #[error("Tip payment error: {0}")]
    TipPaymentError(#[from] TipPaymentError),
    #[error("Tip distribution error: {0}")]
    TipDistributionError(#[from] TipDistributionError),
    #[error("Rpc error: {0}")]
    RpcError(#[from] ClientError),
}

#[derive(Debug, Clone)]
struct TipPaymentProgramInfo {
    program_id: Address,

    config_pda_bump: (Address, u8),
    tip_pda_0: (Address, u8),
    tip_pda_1: (Address, u8),
    tip_pda_2: (Address, u8),
    tip_pda_3: (Address, u8),
    tip_pda_4: (Address, u8),
    tip_pda_5: (Address, u8),
    tip_pda_6: (Address, u8),
    tip_pda_7: (Address, u8),
}

/// Contains metadata regarding the tip-distribution account.
/// The PDAs contained in this struct are presumed to be owned by the program.
#[derive(Debug, Clone)]
struct TipDistributionProgramInfo {
    /// The tip-distribution `program_id`.
    program_id: Address,

    /// Singleton [Config] PDA and bump tuple.
    config_pda_and_bump: (Address, u8),
}

/// This config is used on each invocation to the
/// `initialize_tip_distribution_account` instruction.
#[derive(Debug, Clone)]
pub struct TipDistributionAccountConfig {
    /// The account with authority to upload merkle-roots to this validator's
    /// [`TipDistributionAccount`].
    pub merkle_root_upload_authority: Address,

    /// This validator's vote account.
    pub vote_account: Address,

    /// This validator's commission rate BPS for tips in the
    /// [`TipDistributionAccount`].
    pub commission_bps: u16,
}

#[derive(Clone)]
#[allow(clippy::struct_field_names)]
pub struct TipManager {
    tip_payment_program_info: TipPaymentProgramInfo,
    tip_distribution_program_info: TipDistributionProgramInfo,
    tip_distribution_account_config: TipDistributionAccountConfig,
    // tip_accounts: HashSet<Address>,
}

#[derive(Clone)]
pub struct TipManagerConfig {
    pub tip_payment_program_id: Address,
    pub tip_distribution_program_id: Address,
    pub tip_distribution_account_config: TipDistributionAccountConfig,
}

impl TipManager {
    pub fn new(config: TipManagerConfig) -> Self {
        let TipManagerConfig {
            tip_payment_program_id,
            tip_distribution_program_id,
            tip_distribution_account_config,
        } = config;

        // `<https://github.com/jito-foundation/jito-programs/blob/8f55af0a9b31ac2192415b59ce2c47329ee255a2/mev-programs/programs/tip-payment/src/lib.rs#L33C42-L33C56>`
        let tip_payment_config_pda_bump =
            JitoTipPaymentConfig::find_program_address(&tip_payment_program_id);
        let tip_payment_account_pdas =
            JitoTipPaymentConfig::find_tip_payment_account_pdas(&tip_payment_program_id);

        let tip_distribution_config_pubkey_bump =
            JitoTipDistributionConfig::find_program_address(&tip_distribution_program_id);

        // let tip_accounts =
        // HashSet::from_iter(tip_payment_account_pdas.iter().map(|pda| pda.0));

        Self {
            tip_payment_program_info: TipPaymentProgramInfo {
                program_id: tip_payment_program_id,
                config_pda_bump: tip_payment_config_pda_bump,
                tip_pda_0: tip_payment_account_pdas[0],
                tip_pda_1: tip_payment_account_pdas[1],
                tip_pda_2: tip_payment_account_pdas[2],
                tip_pda_3: tip_payment_account_pdas[3],
                tip_pda_4: tip_payment_account_pdas[4],
                tip_pda_5: tip_payment_account_pdas[5],
                tip_pda_6: tip_payment_account_pdas[6],
                tip_pda_7: tip_payment_account_pdas[7],
            },
            tip_distribution_program_info: TipDistributionProgramInfo {
                program_id: tip_distribution_program_id,
                config_pda_and_bump: tip_distribution_config_pubkey_bump,
            },
            tip_distribution_account_config,
            // tip_accounts,
        }
    }

    // CHANGE: rpc methods to proxy the Bank
    async fn get_rpc_account(
        client: &RpcClient,
        pubkey: &Address,
        tag: &str,
    ) -> Result<Option<Account>, TipManagerError> {
        debug!(%pubkey, "fetching account: {tag}");
        client
            .get_account_with_commitment(pubkey, CommitmentConfig::processed())
            .await
            .map(|a| a.value)
            .map_err(TipManagerError::from)
    }

    pub fn tip_payment_program_id(&self) -> &Address {
        &self.tip_payment_program_info.program_id
    }

    pub fn tip_distribution_program_id(&self) -> &Address {
        &self.tip_distribution_program_info.program_id
    }

    /// Returns the [Config] account owned by the tip-payment program.
    pub fn tip_payment_config_pubkey(&self) -> &Address {
        &self.tip_payment_program_info.config_pda_bump.0
    }

    /// Returns the [Config] account owned by the tip-distribution program.
    pub fn tip_distribution_config_pubkey(&self) -> &Address {
        &self.tip_distribution_program_info.config_pda_and_bump.0
    }

    // pub fn get_tip_accounts(&self) -> &HashSet<Address> {
    //     &self.tip_accounts
    // }

    // CHANGE: removed bank and use rpc
    async fn get_tip_payment_config_account(
        &self,
        client: &RpcClient,
    ) -> Result<JitoTipPaymentConfig, TipManagerError> {
        let config_data = Self::get_rpc_account(
            client,
            &self.tip_payment_program_info.config_pda_bump.0,
            "tip_payment_program_info.config_pda_bump",
        )
        .await?;
        let Some(config_data) = config_data else {
            return Err(TipManagerError::TipPaymentConfigMissing);
        };

        JitoTipPaymentConfig::from_account_data(
            &config_data,
            &self.tip_payment_program_info.program_id,
        )
        .map_err(TipManagerError::TipPaymentError)
    }

    // UNUSED IN MAINNET, see get_initialize_tip_programs_bundle below
    // /// Only called once during contract creation.
    // pub fn initialize_tip_payment_program_tx(
    //     &self,
    //     bank: &Bank,
    //     keypair: &Keypair,
    // ) -> Result<RuntimeTransaction<SanitizedTransaction>> {
    //     let init_ix = Instruction {
    //         program_id: self.tip_payment_program_info.program_id,
    //         data: InitializeTipPaymentInstruction::to_instruction_data(
    //             self.tip_payment_program_info.config_pda_bump.1,
    //             self.tip_payment_program_info.tip_pda_0.1,
    //             self.tip_payment_program_info.tip_pda_1.1,
    //             self.tip_payment_program_info.tip_pda_2.1,
    //             self.tip_payment_program_info.tip_pda_3.1,
    //             self.tip_payment_program_info.tip_pda_4.1,
    //             self.tip_payment_program_info.tip_pda_5.1,
    //             self.tip_payment_program_info.tip_pda_6.1,
    //             self.tip_payment_program_info.tip_pda_7.1,
    //         )?,
    //         accounts: vec![
    //             AccountMeta::new(self.tip_payment_program_info.config_pda_bump.0,
    // false),
    // AccountMeta::new(self.tip_payment_program_info.tip_pda_0.0, false),
    //             AccountMeta::new(self.tip_payment_program_info.tip_pda_1.0,
    // false),
    // AccountMeta::new(self.tip_payment_program_info.tip_pda_2.0, false),
    //             AccountMeta::new(self.tip_payment_program_info.tip_pda_3.0,
    // false),
    // AccountMeta::new(self.tip_payment_program_info.tip_pda_4.0, false),
    //             AccountMeta::new(self.tip_payment_program_info.tip_pda_5.0,
    // false),
    // AccountMeta::new(self.tip_payment_program_info.tip_pda_6.0, false),
    //             AccountMeta::new(self.tip_payment_program_info.tip_pda_7.0,
    // false),             AccountMeta::new_readonly(system_program::id(),
    // false),             AccountMeta::new(keypair.pubkey(), true),
    //         ],
    //     };
    //     let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
    //         &[init_ix],
    //         Some(&keypair.pubkey()),
    //         &[keypair],
    //         bank.last_blockhash(),
    //     ));
    //     Ok(RuntimeTransaction::try_create(
    //         tx,
    //         MessageHash::Compute,
    //         None,
    //         bank,
    //         bank.get_reserved_account_keys(),
    //         true,
    //     )
    //     .unwrap())
    // }

    /// Returns this validator's [`TipDistributionAccount`] PDA derived from the
    /// provided epoch.
    pub fn get_my_tip_distribution_pda(&self, epoch: Epoch) -> (Address, u8) {
        TipDistributionAccount::find_program_address(
            &self.tip_distribution_program_info.program_id,
            &self.tip_distribution_account_config.vote_account,
            epoch,
        )
    }

    // UNUSED IN MAINNET, see get_initialize_tip_programs_bundle below
    // /// Returns whether or not the tip-payment program should be initialized.
    // pub fn should_initialize_tip_payment_program(&self, bank: &Bank) -> bool {
    //     match bank.get_account(&self.tip_payment_config_pubkey()) {
    //         None => true,
    //         Some(account) => account.owner() !=
    // &self.tip_payment_program_info.program_id,     }
    // }

    // UNUSED IN MAINNET, see get_initialize_tip_programs_bundle below
    // /// Returns whether or not the tip-distribution program's [Config] PDA
    // /// should be initialized.
    // pub fn should_initialize_tip_distribution_config(&self, bank: &Bank) -> bool
    // {     match bank.get_account(&self.tip_distribution_config_pubkey()) {
    //         None => true,
    //         Some(account) => account.owner() !=
    // &self.tip_distribution_program_info.program_id,     }
    // }

    /// Returns whether or not the current [`TipDistributionAccount`] PDA
    /// should be initialized for this epoch.
    async fn should_init_tip_distribution_account(
        &self,
        client: &RpcClient,
        tip_distribution_account_pda: Address,
    ) -> Result<bool, TipManagerError> {
        Self::get_rpc_account(client, &tip_distribution_account_pda, "tip_distribution_account_pda")
            .await?
            .map_or(Ok(true), |account| {
                Ok(account.owner() != &self.tip_distribution_program_info.program_id)
            })
    }

    // UNUSED IN MAINNET, see get_initialize_tip_programs_bundle below
    // /// Creates an [Initialize] transaction object.
    // pub fn initialize_tip_distribution_config_tx(
    //     &self,
    //     bank: &Bank,
    //     kp: &Keypair,
    // ) -> Result<RuntimeTransaction<SanitizedTransaction>> {
    //     let ix = Instruction {
    //         program_id: self.tip_distribution_program_info.program_id,
    //         data:
    // InitializeTipDistributionConfigInstruction::to_instruction_data(
    //             kp.pubkey(),
    //             kp.pubkey(),
    //             10,
    //             10_000,
    //             self.tip_distribution_program_info.config_pda_and_bump.1,
    //         )?,
    //         accounts: vec![
    //
    // AccountMeta::new(self.tip_distribution_program_info.config_pda_and_bump.0,
    // false),             AccountMeta::new_readonly(system_program::id(),
    // false),             AccountMeta::new(kp.pubkey(), true),
    //         ],
    //     };

    //     let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
    //         &[ix],
    //         Some(&kp.pubkey()),
    //         &[kp],
    //         bank.last_blockhash(),
    //     ));
    //     Ok(RuntimeTransaction::try_create(
    //         tx,
    //         MessageHash::Compute,
    //         None,
    //         bank,
    //         bank.get_reserved_account_keys(),
    //         true,
    //     )
    //     .unwrap())
    // }

    /// CHANGE: in jito-solana this returns a [`RuntimeTransaction`] since it's
    /// immediately consumed by the Bank, but here we only return a
    /// [`VersionedTransaction`] since we need to insert it to shmem
    ///
    /// Creates an [`InitializeTipDistributionAccount`] transaction object
    /// using the provided Epoch.
    pub fn initialize_tip_distribution_account_tx(
        &self,
        tip_distribution_account_pda: Address,
        bump: u8,
        kp: &Keypair,
        last_blockhash: Hash,
    ) -> Result<VersionedTransaction, TipManagerError> {
        let ix = Instruction {
            program_id: self.tip_distribution_program_info.program_id,
            data: InitializeTipDistributionAccountInstruction::to_instruction_data(
                self.tip_distribution_account_config.merkle_root_upload_authority,
                self.tip_distribution_account_config.commission_bps,
                bump,
            )?,
            accounts: vec![
                AccountMeta::new_readonly(
                    self.tip_distribution_program_info.config_pda_and_bump.0,
                    false,
                ),
                AccountMeta::new(tip_distribution_account_pda, false),
                AccountMeta::new_readonly(self.tip_distribution_account_config.vote_account, false),
                AccountMeta::new(kp.pubkey(), true),
                AccountMeta::new_readonly(system_program::id(), false),
            ],
        };

        Ok(VersionedTransaction::from(Transaction::new_signed_with_payer(
            &[ix],
            Some(&kp.pubkey()),
            &[kp],
            last_blockhash,
        )))
    }

    /// CHANGE: in jito-solana this returns a [`RuntimeTransaction`] since it's
    /// immediately consumed by the Bank, but here we only return a
    /// [`VersionedTransaction`] since we need to insert it to shmem
    ///
    /// Builds a transaction that changes the current tip receiver to
    /// `new_tip_receiver`. The on-chain program will transfer tips sitting in
    /// the tip accounts to the tip receiver before changing ownership.
    pub fn change_tip_receiver_and_block_builder_tx(
        &self,
        new_tip_receiver: &Address,
        keypair: &Keypair,
        block_builder: &Address,
        block_builder_commission: u64,
        tip_payment_config: &JitoTipPaymentConfig,
        last_blockhash: Hash,
    ) -> Result<VersionedTransaction, TipManagerError> {
        self.build_change_tip_receiver_and_block_builder_tx(
            &tip_payment_config.tip_receiver(),
            new_tip_receiver,
            keypair,
            &tip_payment_config.block_builder(),
            block_builder,
            block_builder_commission,
            last_blockhash,
        )
    }

    /// CHANGE: in jito-solana this returns a [`RuntimeTransaction`] since it's
    /// immediately consumed by the Bank, but here we only return a
    /// [`VersionedTransaction`] since we need to insert it to shmem
    #[allow(clippy::too_many_arguments)]
    pub fn build_change_tip_receiver_and_block_builder_tx(
        &self,
        old_tip_receiver: &Address,
        new_tip_receiver: &Address,
        keypair: &Keypair,
        old_block_builder: &Address,
        block_builder: &Address,
        block_builder_commission: u64,
        last_blockhash: Hash,
    ) -> Result<VersionedTransaction, TipManagerError> {
        let change_tip_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: ChangeTipReceiverInstruction::to_instruction_data(),
            accounts: vec![
                AccountMeta::new(self.tip_payment_program_info.config_pda_bump.0, false),
                AccountMeta::new(*old_tip_receiver, false),
                AccountMeta::new(*new_tip_receiver, false),
                AccountMeta::new(*old_block_builder, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_0.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_1.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_2.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_3.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_4.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_5.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_6.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_7.0, false),
                AccountMeta::new(keypair.pubkey(), true),
            ],
        };

        let change_block_builder_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: ChangeBlockBuilderInstruction::to_instruction_data(block_builder_commission)?,
            accounts: vec![
                AccountMeta::new(self.tip_payment_program_info.config_pda_bump.0, false),
                AccountMeta::new(*new_tip_receiver, false), /* tip receiver will have just
                                                             * changed in previous ix */
                AccountMeta::new(*old_block_builder, false),
                AccountMeta::new(*block_builder, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_0.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_1.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_2.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_3.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_4.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_5.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_6.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_7.0, false),
                AccountMeta::new(keypair.pubkey(), true),
            ],
        };

        Ok(VersionedTransaction::from(Transaction::new_signed_with_payer(
            &[change_tip_ix, change_block_builder_ix],
            Some(&keypair.pubkey()),
            &[keypair],
            last_blockhash,
        )))
    }

    // if needed, will send a normal tx via rpc to init the epoch PDA account
    pub(super) async fn check_init_once(
        &self,
        client: &RpcClient,
        keypair: &Keypair,
        epoch: u64,
        last_blockhash: Hash,
    ) -> Result<(), TipManagerError> {
        let start = Instant::now();
        let (tip_distribution_account_pda, bump) = self.get_my_tip_distribution_pda(epoch);

        if !self.should_init_tip_distribution_account(client, tip_distribution_account_pda).await? {
            info!(epoch, %tip_distribution_account_pda, "init tip receiver not needed");
            return Ok(());
        }

        let tx = self.initialize_tip_distribution_account_tx(
            tip_distribution_account_pda,
            bump,
            keypair,
            last_blockhash,
        )?;

        info!(
            epoch,
            time =% start.elapsed(),
            %tip_distribution_account_pda,
            sig =% tx.signatures[0],
            "init tip receiver"
        );

        let sig = client.send_and_confirm_transaction(&tx).await?;
        info!(%sig, "init tip receiver successful");

        Ok(())
    }

    // Not implemented since not actually needed in prod
    //
    // /// Return a bundle that is capable of calling the initialize instructions
    // /// on the two tip payment programs This is mainly helpful for local
    // /// development and shouldn't run on testnet and mainnet, assuming the
    // /// correct TipManager configuration is set.
    // pub fn get_initialize_tip_programs_bundle(
    //     &self,
    //     bank: &Bank,
    //     keypair: &Keypair,
    // ) -> Result<Vec<RuntimeTransaction<SanitizedTransaction>>> {
    //     let mut transactions = Vec::new();
    //     if self.should_initialize_tip_payment_program(bank) {
    //         info!("should_initialize_tip_payment_program=true");
    //         transactions.push(self.initialize_tip_payment_program_tx(bank,
    // keypair)?);     }

    //     if self.should_initialize_tip_distribution_config(bank) {
    //         info!("should_initialize_tip_distribution_config=true");
    //         transactions.push(self.initialize_tip_distribution_config_tx(bank,
    // keypair)?);     }
    //     Ok(transactions)
    // }

    #[allow(clippy::too_many_arguments)]
    pub fn crank_bundle_with_prev_config(
        &self,
        keypair: &Keypair,
        block_builder_fee_info: &BlockBuilderFeeInfo,
        epoch: u64,
        last_blockhash: Hash,
        old_tip_receiver: &Address,
        old_block_builder: &Address,
    ) -> Result<VersionedTransaction, TipManagerError> {
        let (tip_distribution_account_pda, _bump) = self.get_my_tip_distribution_pda(epoch);

        let tx = self.build_change_tip_receiver_and_block_builder_tx(
            old_tip_receiver,
            &tip_distribution_account_pda,
            keypair,
            old_block_builder,
            &block_builder_fee_info.block_builder,
            block_builder_fee_info.block_builder_commission,
            last_blockhash,
        )?;

        info!(
            epoch,
            sig =% tx.signatures[0],
            %tip_distribution_account_pda,
            %old_tip_receiver,
            %old_block_builder,
            ?block_builder_fee_info,
            "changing tip receiver (builder-supplied state)"
        );

        Ok(tx)
    }

    /// Called on the first leader slot that we are sequencing for. Returns a
    /// bundle if we need to crank the tip program for this slot.
    /// It's important that this is called before any bundle is
    /// scheduled (and committed).
    ///
    /// In jito-solana this is called:
    /// - in the bundle stage for every bundle batch
    /// - in the consume worker for every transaction
    /// - on every slot
    ///
    /// Since we blacklist transactions that touch the tip program, we can just
    /// call it once on the first leader slot and be done with it
    /// begininning of the slot
    /// In jito-solana there's also a check for initialisation of the tip
    /// program, but that's not used in public clusters.
    ///
    /// NOTE:
    /// 1) There's a difference in behaviour since jito-solana would not trigger
    ///    the crank if it does not receive any bundle nor any tx tipping the
    ///    tip accounts. In practice this should be rare and negligible extra
    ///    cost
    ///
    /// 2) jito-solana also calls the `initialize_tip_distribution_account_tx`
    ///    here, but since it's only done once per epoch we do it on a separate
    ///    task
    ///
    /// CHANGE: avoid using bank and fetch epoch from rpc
    pub async fn get_tip_programs_crank_bundle(
        &self,
        client: &RpcClient,
        keypair: &Keypair,
        block_builder_fee_info: &BlockBuilderFeeInfo,
        epoch: u64,
        last_blockhash: Hash,
    ) -> Result<VersionedTransaction, TipManagerError> {
        // processed to get the latest state, but it is a bit tricky vs using the actual
        // bank

        let start = Instant::now();
        let tip_payment_config = self.get_tip_payment_config_account(client).await?;

        let (tip_distribution_account_pda, _bump) = self.get_my_tip_distribution_pda(epoch);

        // since we call it once per leader slots, we dont even check if it needs to be
        // changed

        let tx = self.change_tip_receiver_and_block_builder_tx(
            &tip_distribution_account_pda,
            keypair,
            &block_builder_fee_info.block_builder,
            block_builder_fee_info.block_builder_commission,
            &tip_payment_config,
            last_blockhash,
        )?;

        info!(
            epoch,
            time =% start.elapsed(),
            sig =% tx.signatures[0],
            %tip_distribution_account_pda,
            ?tip_payment_config,
            ?block_builder_fee_info,
            "changing tip receiver"
        );

        Ok(tx)
    }
}
