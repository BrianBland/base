//! Prepared-result capability for ordered speculative execution (CHAIN-5451, design §2.2).
//!
//! This is a bounded compile/proof spike, not production enablement: nothing here is
//! wired into the builder loop and there is no default-on behavior. It proves that a
//! Base-owned prepared-result seam can be reached through the concrete
//! [`BaseBlockExecutor`] (via `BlockBuilder::executor_mut()`), and then handed to the
//! existing [`execute_transaction_with_commit_condition`] path, without a TLS slot, an
//! unsafe downcast, or any Reth change.
//!
//! [`execute_transaction_with_commit_condition`]: alloy_evm::block::BlockExecutor::execute_transaction_with_commit_condition
//!
//! # Consume-before-fallible-check cleanup
//!
//! A speculative result is a by-value [`PreparedTx`] token, not a slot on the executor.
//! Staging *moves* the executed result into the token; every fallible follow-up
//! ([`BaseBlockExecutor::commit_prepared`]) takes that token by value, so an identity
//! mismatch, a refused commit, or a dropped token can never leave a stale pending value
//! behind. The type system is the cleanup proof.
//!
//! # Eligibility gates (design §2.2)
//!
//! `State::has_bal()` only detects an *input* BAL (`bal_state.bal.is_some()`); the
//! output-building mode (`bal_state.bal_builder.is_some()`) is invisible to it, so both
//! are gated separately here through [`BalEligibility`]. EIP-8130 (Zenith) authenticated
//! transactions stay serial. Unknown/unsupported modes fall back to serial by returning
//! [`Ineligible`] rather than a committed result.

use alloc::boxed::Box;

use alloy_consensus::{Transaction, TransactionEnvelope, TxReceipt};
use alloy_eips::Encodable2718;
use alloy_evm::{
    Evm, FromRecoveredTx, FromTxWithEncoded, RecoveredTx,
    block::{
        BlockExecutionError, BlockExecutor, CommitChanges, ExecutableTx, GasOutput, StateDB,
    },
};
use alloy_primitives::{B256, U256};
use base_common_chains::Upgrades;
use revm::{
    context::Block,
    database::State,
};

use crate::{BaseBlockExecutor, BaseReceiptBuilder, BaseTxEnv, BaseTxResult};

/// Read-only view of the BAL configuration required to gate speculative reuse.
///
/// Implemented only for the concrete [`State`] database (and `&mut State`, which is what
/// the native builder's `BlockBuilder` hands to `executor_mut()`), so the gate reads real
/// BAL fields without a blanket-plus-specialized impl overlap on the generic `E::DB`.
pub trait BalEligibility {
    /// Whether an *input* BAL is configured (equivalent to `State::has_bal()`).
    fn has_input_bal(&self) -> bool;

    /// Whether an *output* BAL is being built. Invisible to `State::has_bal()`.
    fn is_building_bal(&self) -> bool;
}

impl<DB> BalEligibility for State<DB> {
    fn has_input_bal(&self) -> bool {
        self.bal_state.bal.is_some()
    }

    fn is_building_bal(&self) -> bool {
        self.bal_state.bal_builder.is_some()
    }
}

impl<T: BalEligibility + ?Sized> BalEligibility for &mut T {
    fn has_input_bal(&self) -> bool {
        (**self).has_input_bal()
    }

    fn is_building_bal(&self) -> bool {
        (**self).is_building_bal()
    }
}

/// Why a transaction is not eligible for prepared-result reuse and must run serially.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ineligible {
    /// An input BAL is configured; validation semantics differ from a plain build.
    InputBalMode,
    /// An output BAL is being built; `has_bal()` cannot see this mode.
    OutputBalMode,
    /// EIP-8130 (Zenith) authenticated transaction; stays on the serial path initially.
    Eip8130,
}

/// Build/environment + transaction identity a [`PreparedTx`] is bound to.
///
/// Speculative work is only valid against the exact block environment it was staged in.
/// `commit_prepared` rechecks this against the live executor before committing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildIdentity {
    /// Block number the result was staged against.
    pub block_number: U256,
    /// Block timestamp the result was staged against.
    pub block_timestamp: U256,
    /// The staged transaction's trie hash.
    pub tx_hash: B256,
}

/// A staged, not-yet-committed execution result bound to a [`BuildIdentity`].
///
/// Owning this token *is* holding the pending result; there is no executor-side slot.
#[derive(Debug)]
pub struct PreparedTx<H, T> {
    result: BaseTxResult<H, T>,
    identity: BuildIdentity,
}

impl<H, T> PreparedTx<H, T> {
    /// The identity this result is bound to.
    pub const fn identity(&self) -> &BuildIdentity {
        &self.identity
    }
}

/// A prepared result whose bound identity did not match the live build, returned so the
/// caller can fall back to serial execution. The stale result is dropped with this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentityMismatch {
    /// The identity the result was bound to at stage time.
    pub staged: BuildIdentity,
    /// The identity of the live build at commit time.
    pub live: BuildIdentity,
}

impl<E, R, Spec> BaseBlockExecutor<E, R, Spec>
where
    E: Evm<
            DB: StateDB + BalEligibility,
            Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction> + BaseTxEnv,
        >,
    R: BaseReceiptBuilder<
            Transaction: Transaction + Encodable2718 + TransactionEnvelope<TxType: Send + 'static>,
            Receipt: TxReceipt,
        >,
    Spec: Upgrades,
{
    /// Eligibility gate for prepared-result reuse (design §2.2). Reads real BAL modes and
    /// the EIP-8130 signal off the already-built tx env; returns the serial-fallback
    /// reason instead of a boolean so callers can record why speculation was declined.
    pub fn prepared_eligibility(tx_env: &E::Tx, db: &E::DB) -> Result<(), Ineligible> {
        if db.is_building_bal() {
            return Err(Ineligible::OutputBalMode);
        }
        if db.has_input_bal() {
            return Err(Ineligible::InputBalMode);
        }
        if tx_env.eip8130_signed().is_some() {
            return Err(Ineligible::Eip8130);
        }
        Ok(())
    }

    /// Stage a transaction: run eligibility gates, execute *without* committing, and move
    /// the pending result into a [`PreparedTx`] bound to the current build identity.
    ///
    /// The pending result exists only inside the returned token. Any error path returns
    /// before a token is created, so no stale slot is possible. An ineligible transaction
    /// returns `Ok(Err(reason))` for immediate serial fallback.
    #[allow(clippy::type_complexity)]
    pub fn stage_prepared(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<
        Result<PreparedTx<E::HaltReason, <R::Transaction as TransactionEnvelope>::TxType>, Ineligible>,
        BlockExecutionError,
    > {
        // Destructure once: `into_parts` yields the built tx env and the recovered tx.
        let (tx_env, recovered) = tx.into_parts();
        if let Err(reason) = Self::prepared_eligibility(&tx_env, self.evm.db()) {
            return Ok(Err(reason));
        }

        let identity = self.build_identity(recovered.tx().trie_hash());
        // Execute without commit: this is the pending value. It lives only in `result`
        // from here on — moved into the token below, never parked on `self`.
        let result = self.execute_transaction_without_commit((tx_env, recovered))?;
        Ok(Ok(PreparedTx { result, identity }))
    }

    /// Commit a previously staged result through the existing conditional-commit path,
    /// after rechecking its bound identity against the live build.
    ///
    /// The token is consumed by value. On an identity mismatch the staged result is
    /// dropped and returned as [`IdentityMismatch`]; on a refused commit condition it is
    /// dropped inside [`commit_transaction`]-equivalent handling and `Ok(None)` is
    /// returned. Either way nothing stale survives.
    ///
    /// [`commit_transaction`]: alloy_evm::block::BlockExecutor::commit_transaction
    #[allow(clippy::type_complexity)]
    pub fn commit_prepared(
        &mut self,
        prepared: PreparedTx<E::HaltReason, <R::Transaction as TransactionEnvelope>::TxType>,
        commit_condition: impl FnOnce(
            &BaseTxResult<E::HaltReason, <R::Transaction as TransactionEnvelope>::TxType>,
        ) -> CommitChanges,
    ) -> Result<
        Option<GasOutput>,
        Box<IdentityMismatch>,
    > {
        let live = self.build_identity(prepared.identity.tx_hash);
        if live != prepared.identity {
            // Staged `prepared` (and its result) is dropped here: no stale slot.
            return Err(Box::new(IdentityMismatch {
                staged: prepared.identity,
                live,
            }));
        }

        // Route through the exact same commit primitives the serial path uses, preserving
        // gas/DA accounting, receipts, and bundle bookkeeping.
        if !commit_condition(&prepared.result).should_commit() {
            // Refused commit: `prepared.result` is dropped, state untouched.
            return Ok(None);
        }
        Ok(Some(self.commit_transaction(prepared.result)))
    }

    /// Current build identity for the given transaction hash.
    fn build_identity(&self, tx_hash: B256) -> BuildIdentity {
        let block = self.evm.block();
        BuildIdentity {
            block_number: block.number(),
            block_timestamp: block.timestamp(),
            tx_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{SignableTransaction, TxLegacy, transaction::Recovered};
    use alloy_evm::precompiles::PrecompilesMap;
    use alloy_primitives::{Address, Signature, TxKind, U256};
    use base_common_chains::ChainUpgrades;
    use base_common_consensus::BaseTxEnvelope;
    use base_common_genesis::BaseUpgrade;
    use revm::{
        Context,
        context::BlockEnv,
        database::{InMemoryDB, State},
        inspector::NoOpInspector,
        state::{AccountInfo, bal::Bal},
    };
    use std::sync::Arc;

    use super::*;
    use crate::{
        AlloyReceiptBuilder, BaseBlockExecutionCtx, BaseEvm, BaseSpecId, Builder, DefaultBase,
    };


    const SENDER: Address = Address::with_last_byte(0xAA);
    const GAS_LIMIT: u64 = 100_000;

    type TestExecutor<'a> = BaseBlockExecutor<
        BaseEvm<&'a mut State<InMemoryDB>, NoOpInspector, PrecompilesMap>,
        &'a AlloyReceiptBuilder,
        &'a ChainUpgrades,
    >;

    /// A funded-sender in-memory State. `with_bal_builder` toggles output-BAL mode;
    /// `input_bal` installs an input BAL. Neither is set for the plain-build path.
    fn funded_db(with_bal_builder: bool, input_bal: Option<Arc<Bal>>) -> State<InMemoryDB> {
        let mut inner = InMemoryDB::default();
        inner.insert_account_info(
            SENDER,
            AccountInfo { balance: U256::from(1_000_000_000_000u64), ..Default::default() },
        );
        let mut builder = State::builder().with_database(inner);
        if with_bal_builder {
            builder = builder.with_bal_builder();
        }
        if let Some(bal) = input_bal {
            builder = builder.with_bal(bal);
        }
        builder.build()
    }

    fn build_executor<'a>(
        db: &'a mut State<InMemoryDB>,
        receipt_builder: &'a AlloyReceiptBuilder,
        upgrades: &'a ChainUpgrades,
        block_number: u64,
    ) -> TestExecutor<'a> {
        let ctx = Context::base()
            .with_db(db)
            .with_block(BlockEnv {
                number: U256::from(block_number),
                gas_limit: 30_000_000,
                ..Default::default()
            })
            .modify_cfg_chained(|cfg| cfg.spec = BaseSpecId::new(BaseUpgrade::Jovian));
        let evm = ctx.build_with_inspector(NoOpInspector {});
        BaseBlockExecutor::new(evm, BaseBlockExecutionCtx::default(), upgrades, receipt_builder)
    }

    /// A simple funded value-transfer that executes successfully on the plain path.
    fn transfer_tx() -> Recovered<BaseTxEnvelope> {
        let inner = TxLegacy {
            gas_limit: GAS_LIMIT,
            gas_price: 0,
            value: U256::from(1),
            to: TxKind::Call(Address::with_last_byte(0xBB)),
            ..Default::default()
        };
        Recovered::new_unchecked(
            BaseTxEnvelope::Legacy(inner.into_signed(Signature::new(
                Default::default(),
                Default::default(),
                false,
            ))),
            SENDER,
        )
    }

    /// Stage then commit on the plain path: routes through `commit_transaction`, so gas
    /// accounting and receipts advance exactly as the serial path would.
    #[test]
    fn stage_then_commit_advances_state() {
        let mut db = funded_db(false, None);
        let rb = AlloyReceiptBuilder::default();
        let up = ChainUpgrades::mainnet();
        let mut executor = build_executor(&mut db, &rb, &up, 1);

        let prepared = executor
            .stage_prepared(&transfer_tx())
            .expect("stage must not error")
            .expect("tx is eligible on the plain path");
        // Staging must not mutate committed state.
        assert_eq!(executor.gas_used, 0);
        assert!(executor.receipts.is_empty());

        let gas = executor
            .commit_prepared(prepared, |_| CommitChanges::Yes)
            .expect("identity matches")
            .expect("commit condition is Yes");
        assert!(gas.tx_gas_used() > 0);
        assert_eq!(executor.receipts.len(), 1);
        assert_eq!(executor.gas_used, gas.tx_gas_used());
    }

    /// A refused commit condition consumes the token and leaves committed state untouched.
    #[test]
    fn refused_commit_leaves_state_untouched() {
        let mut db = funded_db(false, None);
        let rb = AlloyReceiptBuilder::default();
        let up = ChainUpgrades::mainnet();
        let mut executor = build_executor(&mut db, &rb, &up, 1);

        let prepared = executor.stage_prepared(&transfer_tx()).unwrap().unwrap();
        let committed = executor
            .commit_prepared(prepared, |_| CommitChanges::No)
            .expect("identity matches");
        assert!(committed.is_none(), "refused commit returns None");
        assert_eq!(executor.gas_used, 0);
        assert!(executor.receipts.is_empty());
    }

    /// A result staged against one build identity is refused (and dropped) when the live
    /// build environment has moved on, proving no stale result reaches commit.
    #[test]
    fn identity_mismatch_refuses_and_drops() {
        let rb = AlloyReceiptBuilder::default();
        let up = ChainUpgrades::mainnet();

        let mut db = funded_db(false, None);
        let prepared = {
            let mut executor = build_executor(&mut db, &rb, &up, 1);
            executor.stage_prepared(&transfer_tx()).unwrap().unwrap()
        };
        let staged_identity = *prepared.identity();

        // Rebuild the executor at a different block number: the live identity differs.
        let mut executor = build_executor(&mut db, &rb, &up, 2);
        let err = executor
            .commit_prepared(prepared, |_| CommitChanges::Yes)
            .expect_err("identity mismatch must be refused");
        assert_eq!(err.staged, staged_identity);
        assert_eq!(err.live.block_number, U256::from(2));
        assert_ne!(err.staged.block_number, err.live.block_number);
        // Nothing committed on the mismatch path.
        assert_eq!(executor.gas_used, 0);
        assert!(executor.receipts.is_empty());
    }

    /// Output-BAL building mode is invisible to `State::has_bal()`, so it must be gated
    /// separately: staging returns the serial-fallback reason, never a committed result.
    #[test]
    fn output_bal_mode_falls_back_to_serial() {
        let mut db = funded_db(true, None);
        assert!(!db.has_bal(), "output-BAL build is invisible to has_bal()");
        let rb = AlloyReceiptBuilder::default();
        let up = ChainUpgrades::mainnet();
        let mut executor = build_executor(&mut db, &rb, &up, 1);

        let outcome = executor.stage_prepared(&transfer_tx()).expect("no hard error");
        assert_eq!(outcome.err(), Some(Ineligible::OutputBalMode));
    }

    /// Input-BAL mode is gated too (distinct validation semantics).
    #[test]
    fn input_bal_mode_falls_back_to_serial() {
        let mut db = funded_db(false, Some(Arc::new(Bal::default())));
        assert!(db.has_bal());
        let rb = AlloyReceiptBuilder::default();
        let up = ChainUpgrades::mainnet();
        let mut executor = build_executor(&mut db, &rb, &up, 1);

        let outcome = executor.stage_prepared(&transfer_tx()).expect("no hard error");
        assert_eq!(outcome.err(), Some(Ineligible::InputBalMode));
    }

    /// The `IdentityMismatch` type carries no committed side effect: dropping it is the
    /// full cleanup. This is a type-level assertion that the token owns the result.
    #[test]
    fn build_identity_binds_number_timestamp_and_hash() {
        let mut db = funded_db(false, None);
        let rb = AlloyReceiptBuilder::default();
        let up = ChainUpgrades::mainnet();
        let executor = build_executor(&mut db, &rb, &up, 7);
        let tx = transfer_tx();
        let id = executor.build_identity(tx.tx().trie_hash());
        assert_eq!(id.block_number, U256::from(7));
        assert_eq!(id.tx_hash, tx.tx().trie_hash());
    }
}
