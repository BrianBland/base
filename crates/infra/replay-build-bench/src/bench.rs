//! Replayed-building benchmark over a private, read-only Base mainnet snapshot.

use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use alloy_consensus::{Transaction, transaction::Recovered};
use alloy_eips::{
    BlockHashOrNumber,
    eip2718::{Encodable2718, WithEncoded},
};
use alloy_primitives::{Address, B64, Bytes};
use base_common_chains::Upgrades;
use base_common_consensus::{
    BasePrimitives, BaseTransactionSigned, DEPOSIT_TX_TYPE_ID, JovianExtraData,
};
use base_execution_chainspec::BaseChainSpec;
use base_execution_evm::BaseEvmConfig;
use base_execution_payload_builder::{
    BasePayloadBuilderAttributes, ParkableBestPayloadTransactions,
    builder::{BasePayloadBuilderCtx, Builder},
    config::BaseBuilderConfig,
    payload::EthPayloadBuilderAttributes,
};
use base_execution_txpool::{BaseOrdering, BasePooledTransaction, ParkedBestTransactions};
use base_node_runner::BaseNode;
use clap::Parser;
use eyre::{Result, ensure, eyre};
use reth_basic_payload_builder::{BuildOutcomeKind, PayloadConfig};
use reth_execution_cache::{CacheFillMode, CachedStateProvider, ExecutionCache};
use reth_evm::execute::{BasicBlockExecutor, Executor};
use reth_payload_builder::PayloadId;
use reth_payload_primitives::PayloadAttributes as _;
use reth_primitives_traits::NodePrimitives;
use reth_provider::{
    BlockNumReader, BlockReader, HeaderProvider, StageCheckpointReader, StorageSettingsCache,
    providers::ReadOnlyConfig,
};
use reth_revm::{cached::CachedReads, cancelled::CancelOnDrop, database::StateProviderDatabase};
use reth_stages_types::StageId;
use reth_storage_api::{ReceiptProvider as _, TransactionVariant};
use reth_tasks::{RayonConfig, RuntimeBuilder, RuntimeConfig, TokioConfig};
use reth_transaction_pool::{
    BestTransactions as _, BestTransactionsAttributes, TransactionOrigin, ValidPoolTransaction,
    identifier::{SenderId, TransactionId},
    pool::PendingPool,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Persisted checkpoints controlling latest-state reads (not ancillary file tips).
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateStages {
    /// Finish checkpoint.
    pub finish: Option<u64>,
    /// Execution checkpoint.
    pub execution: Option<u64>,
    /// Account hashing checkpoint.
    pub account_hashing: Option<u64>,
    /// Storage hashing checkpoint.
    pub storage_hashing: Option<u64>,
    /// Whether latest reads use hashed state.
    pub use_hashed_state: bool,
}

impl StateStages {
    /// Reads metadata only.
    pub fn read<P: StageCheckpointReader + StorageSettingsCache>(provider: &P) -> Result<Self> {
        Ok(Self {
            finish: provider.get_stage_checkpoint(StageId::Finish)?.map(|s| s.block_number),
            execution: provider.get_stage_checkpoint(StageId::Execution)?.map(|s| s.block_number),
            account_hashing: provider
                .get_stage_checkpoint(StageId::AccountHashing)?
                .map(|s| s.block_number),
            storage_hashing: provider
                .get_stage_checkpoint(StageId::StorageHashing)?
                .map(|s| s.block_number),
            use_hashed_state: provider.cached_storage_settings().use_hashed_state(),
        })
    }

    /// Fails closed if the selected state tables do not represent the persisted head.
    pub fn validate(&self, head: u64) -> Result<()> {
        ensure!(
            self.finish == Some(head) && self.execution == Some(head),
            "Finish/Execution mismatch: {self:?}, head {head}"
        );
        if self.use_hashed_state {
            ensure!(
                self.account_hashing == Some(head) && self.storage_hashing == Some(head),
                "hashing checkpoint mismatch: {self:?}"
            );
        }
        Ok(())
    }
}

/// Linux process IO counters.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct IoSnapshot {
    /// Bytes actually fetched from storage.
    pub read_bytes: u64,
    /// Bytes returned by read-family syscalls (not mmap page faults).
    pub rchar: u64,
    /// Read-family syscall count.
    pub syscr: u64,
}

impl IoSnapshot {
    /// Reads procfs; non-Linux reports `None`.
    pub fn read() -> Result<Option<Self>> {
        if !cfg!(target_os = "linux") {
            return Ok(None);
        }
        let text = fs::read_to_string("/proc/self/io")?;
        let field = |name: &str| -> Result<u64> {
            let line = text
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .ok_or_else(|| eyre!("missing procfs IO field {name}"))?;
            Ok(line.trim().parse()?)
        };
        Ok(Some(Self {
            read_bytes: field("read_bytes:")?,
            rchar: field("rchar:")?,
            syscr: field("syscr:")?,
        }))
    }

    /// Difference between snapshots.
    pub const fn delta(&self, before: &Self) -> Self {
        Self {
            read_bytes: self.read_bytes.saturating_sub(before.read_bytes),
            rchar: self.rchar.saturating_sub(before.rchar),
            syscr: self.syscr.saturating_sub(before.syscr),
        }
    }
}

/// Aggregated statistics of one built payload.
#[derive(Debug)]
pub struct BuildStats {
    /// Build phase wall time in nanoseconds.
    pub build_ns: u128,
    /// Outcome kind: `sealed`, `aborted`, or `cancelled`.
    pub outcome: &'static str,
    /// Transactions sealed into the built payload.
    pub txs: usize,
    /// Gas used by the built payload.
    pub gas_used: u64,
    /// Gas limit of the built payload.
    pub gas_limit: u64,
    /// Fees collected by the built payload.
    pub fees: String,
}

/// One replayed block: build statistics next to the canonical reference.
#[derive(Debug, Serialize)]
pub struct BlockRow {
    /// Canonical block number.
    pub block: u64,
    /// Canonical block hash.
    pub hash: String,
    /// Canonical deposits, regular transactions, gas and timestamp.
    pub canonical: Value,
    /// Build phase statistics (absent with `--no-build`).
    pub build: Option<BuildRow>,
    /// Canonical execution wall time in nanoseconds.
    pub execute_ns: u128,
    /// Canonical receipts agreed with the snapshot receipts.
    pub receipts_match: bool,
    /// Executed gas used agreed with the canonical header.
    pub gas_used_match: bool,
}

/// Serialized build statistics for one block.
#[derive(Debug, Serialize)]
pub struct BuildRow {
    /// Build phase wall time in nanoseconds.
    pub build_ns: u128,
    /// Outcome kind: `sealed`, `aborted`, or `cancelled`.
    pub outcome: &'static str,
    /// Transactions sealed into the built payload.
    pub txs: usize,
    /// Gas used by the built payload.
    pub gas_used: u64,
    /// Gas limit of the built payload.
    pub gas_limit: u64,
    /// Fees collected by the built payload.
    pub fees: String,
}

impl From<BuildStats> for BuildRow {
    fn from(stats: BuildStats) -> Self {
        Self {
            build_ns: stats.build_ns,
            outcome: stats.outcome,
            txs: stats.txs,
            gas_used: stats.gas_used,
            gas_limit: stats.gas_limit,
            fees: stats.fees,
        }
    }
}

/// CLI for an immutable local MDBX snapshot. No network access is performed.
#[derive(Debug, Parser)]
#[command(about = "Replayed building benchmark over a private Base mainnet snapshot")]
pub struct ReplayBuildBench {
    /// Base datadir containing the private MDBX snapshot.
    #[arg(long)]
    pub datadir: PathBuf,
    /// Inspect persisted anchor/stages only; performs no block reads.
    #[arg(long, group = "mode")]
    pub inspect: bool,
    /// Execute the replayed build loop in this fresh process.
    #[arg(long, group = "mode")]
    pub run: bool,
    /// Skip the build phase; canonical execution + validation only.
    #[arg(long)]
    pub no_build: bool,
    /// First replayed block's parent (anchor). Defaults to `head - count`.
    #[arg(long)]
    pub from: Option<u64>,
    /// Number of canonical blocks to replay-build (maximum 1000).
    #[arg(long, default_value_t = 64)]
    pub count: usize,
    /// Process-local `ExecutionCache` size in `MiB`.
    #[arg(long, default_value_t = 4096)]
    pub cache_mib: usize,
    /// JSON destination, or '-' for stdout. Existing files are never overwritten.
    #[arg(long, default_value = "-")]
    pub output: PathBuf,
}

impl ReplayBuildBench {
    /// Decodes Jovian extra data into builder-attribute parameters.
    ///
    /// Returns `(eip_1559_params, min_base_fee)` where the `B64` packs
    /// `max_change_denominator` and `elasticity_multiplier` big-endian, matching
    /// the on-chain extra-data layout used by `EIP1559ParamEncoder`.
    pub fn jovian_params(extra_data: &[u8]) -> Result<(B64, u64)> {
        let (elasticity, denominator, min_base_fee) = JovianExtraData::decode(extra_data)?;
        let mut packed = [0u8; 8];
        packed[..4].copy_from_slice(&denominator.to_be_bytes());
        packed[4..].copy_from_slice(&elasticity.to_be_bytes());
        Ok((B64::new(packed), min_base_fee))
    }

    /// Builds attributes for the canonical block from its header and deposits.
    pub fn attributes_for(
        chain: &BaseChainSpec,
        header: &<BasePrimitives as NodePrimitives>::BlockHeader,
        parent_hash: alloy_primitives::B256,
        deposits: Vec<WithEncoded<BaseTransactionSigned>>,
    ) -> Result<BasePayloadBuilderAttributes<BaseTransactionSigned>> {
        let timestamp = header.timestamp;
        let (eip_1559_params, min_base_fee) = if chain.is_jovian_active_at_timestamp(timestamp) {
            let (params, min_base_fee) = Self::jovian_params(&header.extra_data)?;
            (Some(params), Some(min_base_fee))
        } else {
            ensure!(
                header.extra_data.is_empty(),
                "unexpected non-empty extra_data {:?} before Jovian",
                header.extra_data
            );
            (None, None)
        };
        Ok(BasePayloadBuilderAttributes {
            payload_attributes: EthPayloadBuilderAttributes {
                id: PayloadId::default(),
                parent: parent_hash,
                timestamp,
                suggested_fee_recipient: header.beneficiary,
                prev_randao: header.mix_hash,
                has_withdrawals: false,
                withdrawals: Default::default(),
                parent_beacon_block_root: header.parent_beacon_block_root,
                slot_number: None,
            },
            no_tx_pool: false,
            transactions: deposits,
            gas_limit: Some(header.gas_limit),
            eip_1559_params,
            min_base_fee,
        })
    }

    /// Creates the real pending pool holding one canonical block's regular
    /// transactions. Canonical nonces fix per-sender ordering exactly as the
    /// live pool would see them.
    pub fn pool_for(
        transactions: Vec<Recovered<BaseTransactionSigned>>,
    ) -> PendingPool<BaseOrdering<BasePooledTransaction>> {
        let mut pool = PendingPool::new(BaseOrdering::coinbase_tip());
        let mut sender_ids: HashMap<Address, u64> = HashMap::new();
        for recovered in transactions {
            let (transaction, sender) = recovered.into_parts();
            let nonce = transaction.nonce();
            let encoded_length = transaction.encode_2718_len();
            let next = sender_ids.len() as u64 + 1;
            let sender_id = *sender_ids.entry(sender).or_insert(next);
            let pooled = BasePooledTransaction::new(
                Recovered::new_unchecked(transaction, sender),
                encoded_length,
            );
            pool.add_transaction(
                Arc::new(ValidPoolTransaction {
                    transaction_id: TransactionId::new(SenderId::from(sender_id), nonce),
                    transaction: pooled,
                    propagate: false,
                    timestamp: Instant::now(),
                    origin: TransactionOrigin::External,
                    authority_ids: None,
                }),
                0,
            );
        }
        pool
    }

    /// Writes a single JSON document, outside all build-loop timings.
    pub fn write_json(&self, value: &Value) -> Result<()> {
        let mut out: Box<dyn Write> = if self.output.as_os_str() == "-" {
            Box::new(io::stdout())
        } else {
            Box::new(File::create_new(&self.output)?)
        };
        serde_json::to_writer_pretty(&mut out, value)?;
        writeln!(out)?;
        Ok(())
    }

    /// Opens all databases read-only, validates the snapshot, and runs the selected mode.
    pub fn execute(self) -> Result<()> {
        let startup = Instant::now();
        ensure!(self.inspect || self.run, "choose --inspect or --run");
        ensure!(
            self.count > 0 && self.count <= 1000 && self.cache_mib <= 256 * 1024,
            "count must be in 1..=1000 and cache_mib bounded"
        );
        if self.output.as_os_str() != "-" {
            let parent = self
                .output
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| std::path::Path::new("."));
            ensure!(
                !parent.canonicalize()?.starts_with(self.datadir.canonicalize()?),
                "output must be outside datadir"
            );
        }
        let chain = Arc::new(BaseChainSpec::mainnet());
        let runtime = RuntimeBuilder::new(
            RuntimeConfig::default()
                .with_tokio(TokioConfig::with_worker_threads(2))
                .with_rayon(RayonConfig {
                    cpu_threads: Some(1),
                    storage_threads: Some(1),
                    state_trie_overlay_worker_threads: Some(1),
                    ..Default::default()
                }),
        )
        .build()?;
        let factory = BaseNode::provider_factory_builder().open_read_only(
            Arc::clone(&chain),
            ReadOnlyConfig::from_datadir(&self.datadir).no_watch(),
            runtime,
        )?;
        let head = factory.best_block_number()?;
        let header = factory
            .header_by_number(head)?
            .ok_or_else(|| eyre!("missing persisted header"))?;
        let hash = header.hash_slow();
        let stages = StateStages::read(&factory)?;
        stages.validate(head)?;
        let anchor = json!({
            "block_number": head,
            "block_hash": hash,
            "state_root": header.state_root,
            "timestamp": header.timestamp,
            "chain_id": 8453,
            "state_stages": stages,
        });
        let recheck = || -> Result<()> {
            ensure!(factory.best_block_number()? == head, "persisted head changed");
            ensure!(
                factory.header_by_number(head)?.is_some_and(|h| h.hash_slow() == hash),
                "pinned header changed"
            );
            ensure!(StateStages::read(&factory)? == stages, "state stages changed");
            Ok(())
        };
        if self.inspect {
            recheck()?;
            return self.write_json(&json!({
                "benchmark": "base-replay-build", "mode": "inspect", "anchor": anchor,
                "count": self.count, "cache_mib": self.cache_mib,
            }));
        }

        let from = self.from.unwrap_or_else(|| head.saturating_sub(self.count as u64));
        ensure!(from < head, "--from {from} must be below snapshot head {head}");
        let last = from + self.count as u64;
        ensure!(last <= head, "replay range end {last} exceeds snapshot head {head}");
        // The whole replay range must be retrievable from the snapshot.
        ensure!(
            factory
                .recovered_block(BlockHashOrNumber::Number(last), TransactionVariant::WithHash)?
                .is_some(),
            "canonical block {last} is not retrievable from the snapshot"
        );

        let evm_config = BaseEvmConfig::base(Arc::clone(&chain));
        let cache = ExecutionCache::new(self.cache_mib * 1024 * 1024);
        // Anchor state is fixed; the ExecutionCache accumulates the canonical
        // bundles of every replayed block, mirroring the live cross-block cache.
        let anchor_state = factory.history_by_block_number(from)?;
        let state = CachedStateProvider::new_with_mode(
            anchor_state,
            cache.clone(),
            CacheFillMode::FillOnMiss,
            None,
            None,
        );

        let io_before = IoSnapshot::read()?;
        let mut rows: Vec<BlockRow> = Vec::with_capacity(self.count);
        let loop_start = Instant::now();
        for block_number in (from + 1)..=last {
            let block = factory
                .recovered_block(
                    BlockHashOrNumber::Number(block_number),
                    TransactionVariant::WithHash,
                )?
                .ok_or_else(|| eyre!("missing canonical block {block_number}"))?;
            let header = block.header();
            let canonical_hash = block.hash();
            let parent = factory
                .sealed_header(block_number - 1)?
                .ok_or_else(|| eyre!("missing sealed parent header {}", block_number - 1))?;

            // Split the canonical body: deposits ride the payload attributes
            // (sequencer transactions); regular transactions enter the pool.
            let mut deposits: Vec<WithEncoded<BaseTransactionSigned>> = Vec::new();
            let mut regular: Vec<Recovered<BaseTransactionSigned>> = Vec::new();
            for recovered in block.transactions_recovered() {
                let (transaction, sender) = recovered.into_parts();
                let transaction = transaction.clone();
                let encoded: Bytes = transaction.encoded_2718().into();
                if transaction.tx_type() == DEPOSIT_TX_TYPE_ID {
                    deposits.push(WithEncoded::new(encoded, transaction));
                } else {
                    regular.push(Recovered::new_unchecked(transaction, sender));
                }
            }
            let canonical = json!({
                "deposits": deposits.len(),
                "regular": regular.len(),
                "gas_used": header.gas_used,
                "gas_limit": header.gas_limit,
                "timestamp": header.timestamp,
            });

            let build = if self.no_build {
                None
            } else {
                let attributes =
                    Self::attributes_for(&chain, header, parent.hash(), deposits)?;
                let payload_id = attributes.payload_id(&parent.hash());
                let ctx = BasePayloadBuilderCtx {
                    evm_config: evm_config.clone(),
                    builder_config: BaseBuilderConfig::default(),
                    chain_spec: Arc::clone(&chain),
                    config: PayloadConfig::new(Arc::new(parent), attributes, payload_id),
                    cancel: CancelOnDrop::default(),
                    best_payload: None,
                };
                let pool = Self::pool_for(regular);
                let mut cursor = pool.best();
                cursor.no_updates();
                let build = Instant::now();
                let outcome = Builder::new(
                    move |attributes: BestTransactionsAttributes| {
                        ParkableBestPayloadTransactions::new(Box::new(
                            ParkedBestTransactions::new(
                                cursor,
                                BaseOrdering::coinbase_tip(),
                                attributes.basefee,
                            ),
                        ))
                    },
                )
                .build(
                    CachedReads::default().as_db_mut(StateProviderDatabase::new(&state)),
                    &state,
                    None,
                    ctx,
                )?;
                let build_ns = build.elapsed().as_nanos();
                let stats = match outcome {
                    BuildOutcomeKind::Better { payload }
                    | BuildOutcomeKind::Freeze(payload) => {
                        let built = payload.block();
                        BuildStats {
                            build_ns,
                            outcome: "sealed",
                            txs: built.body().transactions().count(),
                            gas_used: built.header().gas_used,
                            gas_limit: built.header().gas_limit,
                            fees: payload.fees().to_string(),
                        }
                    }
                    BuildOutcomeKind::Aborted { fees } => BuildStats {
                        build_ns,
                        outcome: "aborted",
                        txs: 0,
                        gas_used: 0,
                        gas_limit: 0,
                        fees: fees.to_string(),
                    },
                    BuildOutcomeKind::Cancelled => {
                        return Err(eyre!("build cancelled for block {block_number}"))
                    }
                };
                Some(stats)
            };

            // Advance canonical state (untimed) and validate against the chain.
            let execute = Instant::now();
            let executor =
                BasicBlockExecutor::new(evm_config.clone(), StateProviderDatabase::new(&state));
            let output = executor.execute(&block)?;
            let execute_ns = execute.elapsed().as_nanos();
            cache
                .insert_state(&output.state)
                .map_err(|()| eyre!("inconsistent bundle state after block {block_number}"))?;

            let canonical_receipts = factory
                .receipts_by_block(BlockHashOrNumber::Number(block_number))?
                .ok_or_else(|| eyre!("missing canonical receipts for {block_number}"))?;
            let receipts_match = output.result.receipts.len() == canonical_receipts.len()
                && output
                    .result
                    .receipts
                    .iter()
                    .zip(canonical_receipts.iter())
                    .all(|(executed, canonical)| {
                        executed.as_receipt().cumulative_gas_used
                            == canonical.as_receipt().cumulative_gas_used
                            && executed.as_receipt().logs.len()
                                == canonical.as_receipt().logs.len()
                    });
            let gas_used_match = output.result.gas_used == header.gas_used;
            ensure!(
                receipts_match && gas_used_match,
                "canonical divergence at block {block_number}: receipts_match={receipts_match} \
                 gas_used_match={gas_used_match} (executed {} vs header {})",
                output.result.gas_used,
                header.gas_used
            );

            rows.push(BlockRow {
                block: block_number,
                hash: canonical_hash.to_string(),
                canonical,
                build: build.map(Into::into),
                execute_ns,
                receipts_match,
                gas_used_match,
            });
        }
        let loop_ns = loop_start.elapsed().as_nanos();
        let io_after = IoSnapshot::read()?;
        recheck()?;

        let built_rows: Vec<&BlockRow> = rows.iter().filter(|row| row.build.is_some()).collect();
        let build_ns_total: u128 =
            built_rows.iter().filter_map(|row| row.build.as_ref().map(|b| b.build_ns)).sum();
        let execute_ns_total: u128 = rows.iter().map(|row| row.execute_ns).sum();
        self.write_json(&json!({
            "benchmark": "base-replay-build",
            "version": 1,
            "mode": if self.no_build { "execute-only" } else { "replayed-building" },
            "anchor": anchor,
            "from": from,
            "count": self.count,
            "cache_mib": self.cache_mib,
            "startup_ns": startup.elapsed().as_nanos().saturating_sub(loop_ns),
            "loop_ns": loop_ns,
            "build_ns_total": build_ns_total,
            "execute_ns_total": execute_ns_total,
            "built_blocks": built_rows.len(),
            "sealed_blocks": built_rows.iter().filter(|row| row.build.as_ref().is_some_and(|b| b.outcome == "sealed")).count(),
            "io_before": io_before,
            "io_after": io_after,
            "io_delta": io_before.zip(io_after).map(|(before, after)| after.delta(&before)),
            "blocks": rows,
            "scope": "real builder path with synchronous state root; built payloads discarded; \
                      canonical advancement via ExecutionCache; validated against snapshot receipts",
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{IoSnapshot, ReplayBuildBench, StateStages};
    use alloy_primitives::{B64, bytes};
    use base_common_consensus::JovianExtraData;

    #[test]
    fn jovian_params_roundtrip_matches_encoder_layout() {
        // 1 || denominator(4) || elasticity(4) || min_base_fee(8), big-endian.
        let extra_data = bytes!("0100000008000000080000000000000101");
        let (params, min_base_fee) = ReplayBuildBench::jovian_params(&extra_data).unwrap();
        assert_eq!(min_base_fee, 257);
        // B64 packs denominator || elasticity.
        assert_eq!(params, B64::new(0x0000000800000008u64.to_be_bytes()));
        // Re-encode through the production encoder and compare byte-for-byte.
        let reencoded = JovianExtraData::encode(
            params,
            alloy_eips::eip1559::BaseFeeParams::new(80, 60),
            min_base_fee,
        )
        .unwrap();
        assert_eq!(reencoded, extra_data);
    }

    #[test]
    fn jovian_params_rejects_wrong_length() {
        assert!(ReplayBuildBench::jovian_params(&[1u8; 16]).is_err());
        assert!(ReplayBuildBench::jovian_params(&[1u8; 18]).is_err());
    }

    #[test]
    fn state_stages_fail_closed_on_mismatch() {
        let mut stages = StateStages {
            finish: Some(10),
            execution: Some(10),
            account_hashing: Some(9),
            storage_hashing: Some(9),
            use_hashed_state: false,
        };
        assert!(stages.validate(10).is_ok());
        stages.use_hashed_state = true;
        assert!(stages.validate(10).is_err());
        stages.account_hashing = Some(10);
        stages.storage_hashing = Some(10);
        assert!(stages.validate(10).is_ok());
        assert!(stages.validate(11).is_err());
    }

    #[test]
    fn io_snapshot_delta_saturates() {
        let before = IoSnapshot { read_bytes: 10, rchar: 20, syscr: 2 };
        let after = IoSnapshot { read_bytes: 8, rchar: 25, syscr: 5 };
        let delta = after.delta(&before);
        assert_eq!((delta.read_bytes, delta.rchar, delta.syscr), (0, 5, 3));
    }
}
