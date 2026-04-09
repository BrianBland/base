use core::convert::Infallible;
use std::{
    collections::HashMap as StdHashMap,
    thread::{self, sleep},
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256};
use revm::{
    Database, DatabaseCommit,
    primitives::{StorageKey, StorageValue},
    state::AccountInfo,
};

use crate::{
    Erc20Context, Erc20StorageLayout, Erc20SwapContext, Erc20SwapLeg, LatencyInjectingDb,
    LatencyInjectingDbConfig, PrefetchBuffer, PrefetchCostModel, PrefetchExecutionPlan,
    PrefetchHintBuilder, PrefetchPlanner, PrefetchScenario, PrefetchingDb, TxShape,
};

/// Execution mode used by [`PrefetchExperiment`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchMode {
    /// No prefetching.
    Baseline,
    /// Prefetch all hints synchronously before execution.
    Synchronous,
    /// Prefetch hints concurrently while execution proceeds.
    Asynchronous,
}

/// Configuration for the benchmark-only synthetic prefetch experiment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefetchExperimentConfig {
    /// Number of iterations per run.
    pub iterations: usize,
    /// Simulated miss latency (applied to account/code/storage/block hash cold reads).
    pub miss_latency: Duration,
    /// Simulated compute gap between storage reads.
    pub execution_gap: Duration,
    /// Lead time to grant async prefetch before main execution starts.
    pub prefetch_lead: Duration,
    /// Number of hinted storage slots to prewarm before execution starts.
    pub prewarmed_storage_hints: usize,
    /// Whether to prewarm token account reads.
    pub prewarm_account_read: bool,
    /// Whether to prewarm token bytecode reads.
    pub prewarm_code_read: bool,
    /// Whether to prewarm block hash reads.
    pub prewarm_block_hash_read: bool,
    /// Swap transfer legs used when `context.tx_shape == TxShape::Swap`.
    pub swap_legs: Vec<Erc20SwapLeg>,
    /// Extra token storage slots read during synthetic execution.
    pub extra_read_slots: Vec<StorageKey>,
    /// Enables cost-aware prefetch planning to skip unprofitable prefetching.
    pub use_prefetch_planner: bool,
    /// Cost model used when `use_prefetch_planner` is enabled.
    pub prefetch_cost_model: PrefetchCostModel,
    /// ERC-20 call context.
    pub context: Erc20Context,
}

impl Default for PrefetchExperimentConfig {
    fn default() -> Self {
        Self {
            iterations: 64,
            miss_latency: Duration::from_micros(100),
            execution_gap: Duration::from_micros(25),
            prefetch_lead: Duration::from_micros(50),
            prewarmed_storage_hints: 0,
            prewarm_account_read: false,
            prewarm_code_read: false,
            prewarm_block_hash_read: false,
            swap_legs: Vec::new(),
            extra_read_slots: Vec::new(),
            use_prefetch_planner: true,
            prefetch_cost_model: PrefetchCostModel::default(),
            context: Erc20Context {
                token: Address::with_last_byte(0xAA),
                from: Address::with_last_byte(0x01),
                to: Address::with_last_byte(0x02),
                spender: Address::with_last_byte(0x03),
                tx_shape: TxShape::TransferFrom,
                layout: Erc20StorageLayout::default(),
            },
        }
    }
}

/// Timing result for one experiment mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefetchRunResult {
    /// Mode that produced this result.
    pub mode: PrefetchMode,
    /// Per-iteration wall-clock durations.
    pub iteration_durations: Vec<Duration>,
}

impl PrefetchRunResult {
    /// Returns p50 latency.
    pub fn p50(&self) -> Duration {
        self.percentile(50)
    }

    /// Returns p95 latency.
    pub fn p95(&self) -> Duration {
        self.percentile(95)
    }

    /// Returns arithmetic mean latency.
    pub fn mean(&self) -> Duration {
        if self.iteration_durations.is_empty() {
            return Duration::ZERO;
        }
        let total = self.iteration_durations.iter().map(Duration::as_secs_f64).sum::<f64>();
        Duration::from_secs_f64(total / self.iteration_durations.len() as f64)
    }

    fn percentile(&self, pct: usize) -> Duration {
        if self.iteration_durations.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.iteration_durations.clone();
        sorted.sort_unstable();
        let idx = ((sorted.len() - 1) * pct) / 100;
        sorted[idx]
    }
}

/// Benchmark-only synthetic prefetch experiment.
#[derive(Debug, Clone)]
pub struct PrefetchExperiment {
    /// Static experiment configuration.
    pub config: PrefetchExperimentConfig,
    db: LatencyInjectingDb,
    code_hash: B256,
}

impl PrefetchExperiment {
    /// Builds a new synthetic experiment and seeds read targets in the database.
    pub fn new(config: PrefetchExperimentConfig) -> Self {
        let db = LatencyInjectingDb::new(LatencyInjectingDbConfig {
            account_miss_latency: config.miss_latency,
            storage_miss_latency: config.miss_latency,
            code_miss_latency: config.miss_latency,
            block_hash_miss_latency: config.miss_latency,
        });
        let code_hash = B256::with_last_byte(1);
        db.insert_account(config.context.token, AccountInfo { code_hash, ..Default::default() });
        db.insert_bytecode(code_hash, Default::default());
        db.insert_block_hash(0, B256::with_last_byte(2));

        let hints = Self::build_hints_from_config(&config);
        for (idx, (address, slot)) in hints.into_iter().enumerate() {
            db.insert_storage(address, slot, StorageValue::from((idx as u64) + 1));
        }

        Self { config, db, code_hash }
    }

    /// Runs `iterations` synthetic executions for the selected mode.
    pub fn run(&self, mode: PrefetchMode) -> PrefetchRunResult {
        let mut iteration_durations = Vec::with_capacity(self.config.iterations);
        for _ in 0..self.config.iterations {
            iteration_durations.push(self.run_once(mode));
        }
        PrefetchRunResult { mode, iteration_durations }
    }

    /// Runs a single synthetic execution.
    pub fn run_once(&self, mode: PrefetchMode) -> Duration {
        self.db.reset_cold_reads();
        self.db.reset_stats();

        let planner_immediate_skip = self.prefetch_planner_immediate_skip(mode);
        let needs_prefetch_hints = mode != PrefetchMode::Baseline && !planner_immediate_skip;
        let needs_prewarm_hints = self.config.prewarmed_storage_hints > 0;
        let hints = if needs_prefetch_hints || needs_prewarm_hints {
            Self::build_hints_from_config(&self.config)
        } else {
            Vec::new()
        };
        let prefetch_plan = if planner_immediate_skip {
            PrefetchExecutionPlan {
                should_prefetch: false,
                hint_limit: 0,
                estimated_hidden_lookups_x100: 0,
                projected_net_gain_ns: 0,
            }
        } else {
            self.prefetch_plan(mode, hints.len())
        };
        let prefetch_hints = self.prefetch_hints_for_plan(&hints, prefetch_plan);
        self.apply_prewarm_state(&hints);
        let start = Instant::now();

        match mode {
            PrefetchMode::Baseline => {
                let mut db = self.db.clone();
                self.execute_synthetic_trace(&mut db);
            }
            PrefetchMode::Synchronous => {
                if prefetch_plan.should_prefetch {
                    let buffer = Self::prefetch_to_frozen_buffer(self.db.clone(), prefetch_hints);
                    let mut db = PrefetchingDb::new(self.db.clone(), buffer);
                    self.execute_synthetic_trace(&mut db);
                } else {
                    let mut db = self.db.clone();
                    self.execute_synthetic_trace(&mut db);
                }
            }
            PrefetchMode::Asynchronous => {
                if prefetch_plan.should_prefetch {
                    let buffer = PrefetchBuffer::concurrent(prefetch_hints.len());
                    let prefetch_db = self.db.clone();
                    let prefetch_buffer = buffer.clone();
                    let handle = thread::spawn(move || {
                        Self::prefetch_slots(prefetch_db, prefetch_hints, prefetch_buffer);
                    });
                    if !self.config.prefetch_lead.is_zero() {
                        sleep(self.config.prefetch_lead);
                    }
                    let mut db = PrefetchingDb::new(self.db.clone(), buffer);
                    self.execute_synthetic_trace(&mut db);
                    let _ = handle.join();
                } else {
                    let mut db = self.db.clone();
                    self.execute_synthetic_trace(&mut db);
                }
            }
        }

        start.elapsed()
    }

    fn prefetch_planner_immediate_skip(&self, mode: PrefetchMode) -> bool {
        mode != PrefetchMode::Baseline
            && self.config.use_prefetch_planner
            && self.config.miss_latency < self.config.prefetch_cost_model.minimum_miss_latency
    }

    fn build_hints_from_config(config: &PrefetchExperimentConfig) -> Vec<(Address, StorageKey)> {
        match config.context.tx_shape {
            TxShape::Transfer | TxShape::TransferFrom => {
                PrefetchHintBuilder::erc20_standard(&config.context, &config.extra_read_slots)
            }
            TxShape::Swap => PrefetchHintBuilder::erc20_swap(
                &Erc20SwapContext {
                    token: config.context.token,
                    layout: config.context.layout,
                    legs: config.swap_legs.clone(),
                },
                &config.extra_read_slots,
            ),
        }
    }

    fn prefetch_plan(&self, mode: PrefetchMode, hint_count: usize) -> PrefetchExecutionPlan {
        if !self.config.use_prefetch_planner {
            return PrefetchExecutionPlan {
                should_prefetch: mode != PrefetchMode::Baseline && hint_count > 0,
                hint_limit: hint_count,
                estimated_hidden_lookups_x100: (hint_count.saturating_mul(100))
                    .min(u32::MAX as usize) as u32,
                projected_net_gain_ns: 0,
            };
        }

        PrefetchPlanner::plan(
            mode,
            self.prefetch_scenario(),
            hint_count,
            self.config.miss_latency,
            self.config.prefetch_cost_model,
        )
    }

    const fn prefetch_scenario(&self) -> PrefetchScenario {
        match self.config.context.tx_shape {
            TxShape::Transfer => PrefetchScenario::Transfer,
            TxShape::TransferFrom => PrefetchScenario::TransferFrom,
            TxShape::Swap => PrefetchScenario::Swap { legs: self.config.swap_legs.len() },
        }
    }

    fn prefetch_hints_for_plan(
        &self,
        hints: &[(Address, StorageKey)],
        prefetch_plan: PrefetchExecutionPlan,
    ) -> Vec<(Address, StorageKey)> {
        if !prefetch_plan.should_prefetch {
            return Vec::new();
        }

        let hint_limit = prefetch_plan.hint_limit.min(hints.len());
        if hint_limit == 0 {
            return Vec::new();
        }

        if self.config.context.tx_shape == TxShape::Swap && hint_limit < hints.len() {
            return self.rank_swap_hints_by_reuse(hints, hint_limit);
        }

        hints.iter().copied().take(hint_limit).collect()
    }

    fn rank_swap_hints_by_reuse(
        &self,
        hints: &[(Address, StorageKey)],
        hint_limit: usize,
    ) -> Vec<(Address, StorageKey)> {
        let context = &self.config.context;
        let mut counts_and_first_index = StdHashMap::with_capacity(hints.len());
        let mut observed_index = 0_usize;

        if let Some(paused_slot) = context.layout.paused_slot {
            Self::record_hint_observation(
                &mut counts_and_first_index,
                (context.token, paused_slot),
                observed_index,
            );
            observed_index = observed_index.saturating_add(1);
        }

        for leg in &self.config.swap_legs {
            if let Some(spender) = leg.allowance_spender {
                Self::record_hint_observation(
                    &mut counts_and_first_index,
                    (
                        context.token,
                        PrefetchHintBuilder::erc20_allowance_slot(
                            leg.from,
                            spender,
                            context.layout.allowances_slot,
                        ),
                    ),
                    observed_index,
                );
                observed_index = observed_index.saturating_add(1);
            }

            Self::record_hint_observation(
                &mut counts_and_first_index,
                (
                    context.token,
                    PrefetchHintBuilder::erc20_balance_slot(leg.from, context.layout.balances_slot),
                ),
                observed_index,
            );
            observed_index = observed_index.saturating_add(1);

            Self::record_hint_observation(
                &mut counts_and_first_index,
                (
                    context.token,
                    PrefetchHintBuilder::erc20_balance_slot(leg.to, context.layout.balances_slot),
                ),
                observed_index,
            );
            observed_index = observed_index.saturating_add(1);
        }

        for slot in &self.config.extra_read_slots {
            Self::record_hint_observation(
                &mut counts_and_first_index,
                (context.token, *slot),
                observed_index,
            );
            observed_index = observed_index.saturating_add(1);
        }

        let mut original_order = StdHashMap::with_capacity(hints.len());
        for (index, hint) in hints.iter().copied().enumerate() {
            original_order.insert(hint, index);
        }

        let mut ranked = hints.to_vec();
        ranked.sort_by(|left, right| {
            let (left_count, left_first) =
                counts_and_first_index.get(left).copied().unwrap_or((0, usize::MAX));
            let (right_count, right_first) =
                counts_and_first_index.get(right).copied().unwrap_or((0, usize::MAX));
            right_count.cmp(&left_count).then(left_first.cmp(&right_first)).then(
                original_order
                    .get(left)
                    .copied()
                    .unwrap_or(usize::MAX)
                    .cmp(&original_order.get(right).copied().unwrap_or(usize::MAX)),
            )
        });
        ranked.truncate(hint_limit);
        ranked
    }

    fn record_hint_observation(
        counts_and_first_index: &mut StdHashMap<(Address, StorageKey), (u32, usize)>,
        hint: (Address, StorageKey),
        index: usize,
    ) {
        if let Some((count, _)) = counts_and_first_index.get_mut(&hint) {
            *count = count.saturating_add(1);
            return;
        }
        counts_and_first_index.insert(hint, (1, index));
    }

    fn apply_prewarm_state(&self, hints: &[(Address, StorageKey)]) {
        if self.config.prewarm_account_read {
            self.db.warm_account(self.config.context.token);
        }

        if self.config.prewarm_code_read {
            self.db.warm_code_hash(self.code_hash);
        }

        if self.config.prewarm_block_hash_read {
            self.db.warm_block_hash(0);
        }

        let prewarmed_hints = self.config.prewarmed_storage_hints.min(hints.len());
        for (address, slot) in hints.iter().take(prewarmed_hints) {
            self.db.warm_storage(*address, *slot);
        }
    }

    fn prefetch_to_frozen_buffer(
        mut db: LatencyInjectingDb,
        hints: Vec<(Address, StorageKey)>,
    ) -> PrefetchBuffer {
        let mut entries = StdHashMap::with_capacity(hints.len());
        for (address, slot) in hints {
            let value = db.storage(address, slot).expect("latency db reads are infallible");
            entries.insert((address, slot), value);
        }
        PrefetchBuffer::frozen(entries)
    }

    fn prefetch_slots(
        mut db: LatencyInjectingDb,
        hints: Vec<(Address, StorageKey)>,
        buffer: PrefetchBuffer,
    ) {
        for (address, slot) in hints {
            let value = db.storage(address, slot).expect("latency db reads are infallible");
            let _ = buffer.insert(address, slot, value);
        }
    }

    fn execute_synthetic_trace<DB>(&self, db: &mut DB)
    where
        DB: Database<Error = Infallible> + DatabaseCommit,
    {
        let context = &self.config.context;

        let _ = db.basic(context.token).expect("latency db reads are infallible");
        let _ = db.code_by_hash(self.code_hash).expect("latency db reads are infallible");

        if context.tx_shape == TxShape::Swap {
            self.execute_swap_trace(db);
            return;
        }

        if let Some(paused_slot) = context.layout.paused_slot {
            let _ =
                db.storage(context.token, paused_slot).expect("latency db reads are infallible");
        }

        if context.tx_shape == TxShape::TransferFrom {
            let allowance_slot = PrefetchHintBuilder::erc20_allowance_slot(
                context.from,
                context.spender,
                context.layout.allowances_slot,
            );
            let _ =
                db.storage(context.token, allowance_slot).expect("latency db reads are infallible");
            self.sleep_execution_gap();
        }

        let from_balance_slot =
            PrefetchHintBuilder::erc20_balance_slot(context.from, context.layout.balances_slot);
        let _ =
            db.storage(context.token, from_balance_slot).expect("latency db reads are infallible");
        self.sleep_execution_gap();

        let to_balance_slot =
            PrefetchHintBuilder::erc20_balance_slot(context.to, context.layout.balances_slot);
        let _ =
            db.storage(context.token, to_balance_slot).expect("latency db reads are infallible");

        for slot in &self.config.extra_read_slots {
            let _ = db.storage(context.token, *slot).expect("latency db reads are infallible");
            self.sleep_execution_gap();
        }
    }

    fn execute_swap_trace<DB>(&self, db: &mut DB)
    where
        DB: Database<Error = Infallible> + DatabaseCommit,
    {
        let context = &self.config.context;
        if let Some(paused_slot) = context.layout.paused_slot {
            let _ =
                db.storage(context.token, paused_slot).expect("latency db reads are infallible");
            self.sleep_execution_gap();
        }

        for leg in &self.config.swap_legs {
            if let Some(spender) = leg.allowance_spender {
                let allowance_slot = PrefetchHintBuilder::erc20_allowance_slot(
                    leg.from,
                    spender,
                    context.layout.allowances_slot,
                );
                let _ = db
                    .storage(context.token, allowance_slot)
                    .expect("latency db reads are infallible");
                self.sleep_execution_gap();
            }

            let from_balance_slot =
                PrefetchHintBuilder::erc20_balance_slot(leg.from, context.layout.balances_slot);
            let _ = db
                .storage(context.token, from_balance_slot)
                .expect("latency db reads are infallible");
            self.sleep_execution_gap();

            let to_balance_slot =
                PrefetchHintBuilder::erc20_balance_slot(leg.to, context.layout.balances_slot);
            let _ = db
                .storage(context.token, to_balance_slot)
                .expect("latency db reads are infallible");
            self.sleep_execution_gap();
        }

        for slot in &self.config.extra_read_slots {
            let _ = db.storage(context.token, *slot).expect("latency db reads are infallible");
            self.sleep_execution_gap();
        }
    }

    fn sleep_execution_gap(&self) {
        if !self.config.execution_gap.is_zero() {
            sleep(self.config.execution_gap);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alloy_primitives::Address;
    use revm::primitives::StorageKey;

    use super::{
        Erc20SwapLeg, PrefetchCostModel, PrefetchExperiment, PrefetchExperimentConfig,
        PrefetchHintBuilder, PrefetchMode, TxShape,
    };

    #[test]
    fn returns_requested_iteration_count() {
        let experiment = PrefetchExperiment::new(PrefetchExperimentConfig {
            iterations: 5,
            ..Default::default()
        });

        let baseline = experiment.run(PrefetchMode::Baseline);
        assert_eq!(baseline.iteration_durations.len(), 5);
    }

    #[test]
    fn async_prefetch_improves_median_for_miss_heavy_case() {
        let experiment = PrefetchExperiment::new(PrefetchExperimentConfig {
            iterations: 8,
            miss_latency: Duration::from_millis(2),
            execution_gap: Duration::from_millis(1),
            prefetch_lead: Duration::from_millis(3),
            ..Default::default()
        });

        let baseline = experiment.run(PrefetchMode::Baseline);
        let asynchronous = experiment.run(PrefetchMode::Asynchronous);

        assert!(asynchronous.p50() < baseline.p50());
    }

    #[test]
    fn prewarming_storage_hints_improves_median_for_miss_heavy_case() {
        let baseline = PrefetchExperiment::new(PrefetchExperimentConfig {
            iterations: 8,
            miss_latency: Duration::from_millis(2),
            execution_gap: Duration::from_millis(0),
            prewarmed_storage_hints: 0,
            ..Default::default()
        })
        .run(PrefetchMode::Baseline);

        let prewarmed = PrefetchExperiment::new(PrefetchExperimentConfig {
            iterations: 8,
            miss_latency: Duration::from_millis(2),
            execution_gap: Duration::from_millis(0),
            prewarmed_storage_hints: 2,
            ..Default::default()
        })
        .run(PrefetchMode::Baseline);

        assert!(prewarmed.p50() < baseline.p50());
    }

    #[test]
    fn prewarming_non_storage_reads_reduces_median_for_miss_heavy_case() {
        let baseline = PrefetchExperiment::new(PrefetchExperimentConfig {
            iterations: 8,
            miss_latency: Duration::from_millis(2),
            execution_gap: Duration::from_millis(0),
            ..Default::default()
        })
        .run(PrefetchMode::Baseline);

        let prewarmed = PrefetchExperiment::new(PrefetchExperimentConfig {
            iterations: 8,
            miss_latency: Duration::from_millis(2),
            execution_gap: Duration::from_millis(0),
            prewarm_account_read: true,
            prewarm_code_read: true,
            prewarm_block_hash_read: true,
            extra_read_slots: vec![StorageKey::from(11_u64)],
            prewarmed_storage_hints: 1,
            ..Default::default()
        })
        .run(PrefetchMode::Baseline);

        assert!(prewarmed.p50() < baseline.p50());
    }

    #[test]
    fn planner_skips_async_prefetch_for_warm_case() {
        let experiment = PrefetchExperiment::new(PrefetchExperimentConfig {
            miss_latency: Duration::from_nanos(500),
            ..Default::default()
        });
        let plan = experiment.prefetch_plan(PrefetchMode::Asynchronous, 4);
        assert!(!plan.should_prefetch);
        assert_eq!(plan.hint_limit, 0);
    }

    #[test]
    fn swap_async_prefetch_improves_median_for_miss_heavy_case() {
        let mut config = PrefetchExperimentConfig {
            iterations: 8,
            miss_latency: Duration::from_millis(2),
            execution_gap: Duration::from_millis(1),
            prefetch_lead: Duration::from_millis(4),
            ..Default::default()
        };
        config.context.tx_shape = TxShape::Swap;
        config.swap_legs = vec![
            Erc20SwapLeg {
                from: Address::with_last_byte(0x10),
                to: Address::with_last_byte(0x20),
                allowance_spender: Some(Address::with_last_byte(0xF0)),
            },
            Erc20SwapLeg {
                from: Address::with_last_byte(0x20),
                to: Address::with_last_byte(0x30),
                allowance_spender: None,
            },
            Erc20SwapLeg {
                from: Address::with_last_byte(0x30),
                to: Address::with_last_byte(0x20),
                allowance_spender: Some(Address::with_last_byte(0xF0)),
            },
            Erc20SwapLeg {
                from: Address::with_last_byte(0x20),
                to: Address::with_last_byte(0x40),
                allowance_spender: None,
            },
        ];

        let experiment = PrefetchExperiment::new(config);
        let baseline = experiment.run(PrefetchMode::Baseline);
        let asynchronous = experiment.run(PrefetchMode::Asynchronous);
        assert!(asynchronous.p50() < baseline.p50());
    }

    #[test]
    fn swap_hint_selection_prioritizes_reused_pool_balance_when_capped() {
        let token = Address::with_last_byte(0xAA);
        let pool = Address::with_last_byte(0x10);
        let router = Address::with_last_byte(0xF0);

        let mut config = PrefetchExperimentConfig {
            iterations: 1,
            miss_latency: Duration::from_millis(2),
            execution_gap: Duration::from_millis(1),
            prefetch_lead: Duration::ZERO,
            prefetch_cost_model: PrefetchCostModel { max_prefetch_hints: 1, ..Default::default() },
            ..Default::default()
        };
        config.context.token = token;
        config.context.tx_shape = TxShape::Swap;
        config.swap_legs = vec![
            Erc20SwapLeg {
                from: Address::with_last_byte(0x11),
                to: pool,
                allowance_spender: Some(router),
            },
            Erc20SwapLeg { from: pool, to: Address::with_last_byte(0x12), allowance_spender: None },
            Erc20SwapLeg {
                from: Address::with_last_byte(0x13),
                to: pool,
                allowance_spender: Some(router),
            },
        ];

        let experiment = PrefetchExperiment::new(config);
        let hints = PrefetchExperiment::build_hints_from_config(&experiment.config);
        let selected = experiment.prefetch_hints_for_plan(
            &hints,
            experiment.prefetch_plan(PrefetchMode::Asynchronous, hints.len()),
        );

        let pool_balance_slot = PrefetchHintBuilder::erc20_balance_slot(
            pool,
            experiment.config.context.layout.balances_slot,
        );
        assert_eq!(selected, vec![(token, pool_balance_slot)]);
    }

    #[test]
    fn planner_immediate_skip_detects_warm_async_case() {
        let experiment = PrefetchExperiment::new(PrefetchExperimentConfig {
            miss_latency: Duration::from_nanos(500),
            ..Default::default()
        });
        assert!(experiment.prefetch_planner_immediate_skip(PrefetchMode::Asynchronous));
    }
}
