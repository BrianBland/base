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

/// One synthetic database target used by the benchmark-only experiment harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrefetchSyntheticTarget {
    /// Warm one account lookup.
    Account {
        /// Address whose account metadata is expected to be read.
        address: Address,
    },
    /// Warm one account lookup followed by the runtime bytecode load.
    AccountCode {
        /// Address whose account metadata is expected to be read.
        address: Address,
        /// Runtime code hash expected for `address`.
        code_hash: B256,
    },
    /// Warm one storage slot.
    Storage {
        /// Address that owns the storage slot.
        address: Address,
        /// Storage slot key.
        slot: StorageKey,
    },
    /// Warm one block-hash lookup.
    BlockHash {
        /// Block number to read.
        number: u64,
    },
}

impl PrefetchSyntheticTarget {
    /// Creates an account target.
    pub const fn account(address: Address) -> Self {
        Self::Account { address }
    }

    /// Creates an account+code target.
    pub const fn account_code(address: Address, code_hash: B256) -> Self {
        Self::AccountCode { address, code_hash }
    }

    /// Creates a storage target.
    pub const fn storage(address: Address, slot: StorageKey) -> Self {
        Self::Storage { address, slot }
    }

    /// Creates a block-hash target.
    pub const fn block_hash(number: u64) -> Self {
        Self::BlockHash { number }
    }

    /// Returns the hidden-lookups estimate for this target, scaled by 100.
    pub const fn hidden_lookups_x100(self) -> u32 {
        match self {
            Self::Account { .. } | Self::Storage { .. } | Self::BlockHash { .. } => 100,
            Self::AccountCode { .. } => 200,
        }
    }

    /// Returns `true` if this target is a storage lookup.
    pub const fn is_storage(self) -> bool {
        matches!(self, Self::Storage { .. })
    }
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
    /// Optional scripted trace used for deeper-tree synthetic benchmarks.
    pub synthetic_trace_reads: Vec<PrefetchSyntheticTarget>,
    /// Optional explicit target list to prefetch for a scripted trace.
    ///
    /// When empty, the experiment falls back to `synthetic_trace_reads` with duplicate targets
    /// removed while preserving order.
    pub synthetic_prefetch_targets: Vec<PrefetchSyntheticTarget>,
    /// Optional scenario override for scripted traces.
    pub synthetic_scenario: Option<PrefetchScenario>,
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
            synthetic_trace_reads: Vec::new(),
            synthetic_prefetch_targets: Vec::new(),
            synthetic_scenario: None,
        }
    }
}

impl PrefetchExperimentConfig {
    /// Returns `true` when the experiment should use the scripted deeper-tree trace.
    pub const fn uses_synthetic_trace(&self) -> bool {
        !self.synthetic_trace_reads.is_empty()
    }

    /// Builds a benchmark-only Universal Router style 2-hop swap trace.
    pub fn universal_router_two_hop_swap_like() -> Self {
        let trader = Address::with_last_byte(0x11);
        let recipient = Address::with_last_byte(0x12);
        let router = Address::with_last_byte(0x90);
        let permit2 = Address::with_last_byte(0x91);
        let token_in = Address::with_last_byte(0xA1);
        let weth = Address::with_last_byte(0xA2);
        let token_out = Address::with_last_byte(0xA3);
        let pair_0 = Address::with_last_byte(0xB1);
        let pair_1 = Address::with_last_byte(0xB2);

        let router_code_hash = B256::with_last_byte(0x21);
        let permit2_code_hash = B256::with_last_byte(0x22);
        let token_in_code_hash = B256::with_last_byte(0x31);
        let weth_code_hash = B256::with_last_byte(0x32);
        let token_out_code_hash = B256::with_last_byte(0x33);
        let pair_0_code_hash = B256::with_last_byte(0x41);
        let pair_1_code_hash = B256::with_last_byte(0x42);

        let token_layout = Erc20StorageLayout {
            balances_slot: StorageKey::ZERO,
            allowances_slot: StorageKey::from(1_u64),
            paused_slot: Some(StorageKey::from(2_u64)),
        };
        let token_in_trader_balance =
            PrefetchHintBuilder::erc20_balance_slot(trader, token_layout.balances_slot);
        let token_in_pair_0_balance =
            PrefetchHintBuilder::erc20_balance_slot(pair_0, token_layout.balances_slot);
        let weth_pair_0_balance =
            PrefetchHintBuilder::erc20_balance_slot(pair_0, token_layout.balances_slot);
        let weth_pair_1_balance =
            PrefetchHintBuilder::erc20_balance_slot(pair_1, token_layout.balances_slot);
        let token_out_pair_1_balance =
            PrefetchHintBuilder::erc20_balance_slot(pair_1, token_layout.balances_slot);
        let token_out_recipient_balance =
            PrefetchHintBuilder::erc20_balance_slot(recipient, token_layout.balances_slot);
        let permit2_allowance_slot = StorageKey::from(0x500_u64);
        let pair_reserve_slot = StorageKey::from(8_u64);

        let synthetic_trace_reads = vec![
            PrefetchSyntheticTarget::account_code(router, router_code_hash),
            PrefetchSyntheticTarget::account_code(permit2, permit2_code_hash),
            PrefetchSyntheticTarget::storage(permit2, permit2_allowance_slot),
            PrefetchSyntheticTarget::account_code(token_in, token_in_code_hash),
            PrefetchSyntheticTarget::storage(token_in, token_layout.paused_slot.expect("set")),
            PrefetchSyntheticTarget::storage(token_in, token_in_trader_balance),
            PrefetchSyntheticTarget::storage(token_in, token_in_pair_0_balance),
            PrefetchSyntheticTarget::account_code(pair_0, pair_0_code_hash),
            PrefetchSyntheticTarget::storage(pair_0, pair_reserve_slot),
            PrefetchSyntheticTarget::account_code(weth, weth_code_hash),
            PrefetchSyntheticTarget::storage(weth, weth_pair_0_balance),
            PrefetchSyntheticTarget::storage(weth, weth_pair_1_balance),
            PrefetchSyntheticTarget::account_code(pair_1, pair_1_code_hash),
            PrefetchSyntheticTarget::storage(pair_1, pair_reserve_slot),
            PrefetchSyntheticTarget::account_code(token_out, token_out_code_hash),
            PrefetchSyntheticTarget::storage(token_out, token_out_pair_1_balance),
            PrefetchSyntheticTarget::storage(token_out, token_out_recipient_balance),
        ];
        let synthetic_prefetch_targets = synthetic_trace_reads.iter().copied().skip(1).collect();

        Self {
            iterations: 64,
            miss_latency: Duration::from_micros(100),
            execution_gap: Duration::from_micros(25),
            prefetch_lead: Duration::from_micros(50),
            context: Erc20Context {
                token: token_in,
                from: trader,
                to: recipient,
                spender: router,
                tx_shape: TxShape::Swap,
                layout: token_layout,
            },
            synthetic_trace_reads,
            synthetic_prefetch_targets,
            synthetic_scenario: Some(PrefetchScenario::Swap { legs: 3 }),
            ..Default::default()
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

        if config.uses_synthetic_trace() {
            Self::seed_synthetic_targets(
                &db,
                config
                    .synthetic_trace_reads
                    .iter()
                    .copied()
                    .chain(config.synthetic_prefetch_targets.iter().copied()),
            );
        } else {
            let hints = Self::build_hints_from_config(&config);
            for (idx, (address, slot)) in hints.into_iter().enumerate() {
                db.insert_storage(address, slot, StorageValue::from((idx as u64) + 1));
            }
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
        let synthetic_targets = if self.config.uses_synthetic_trace()
            && (needs_prefetch_hints || needs_prewarm_hints)
        {
            Self::build_synthetic_targets_from_config(&self.config)
        } else {
            Vec::new()
        };
        let hints = if !self.config.uses_synthetic_trace()
            && (needs_prefetch_hints || needs_prewarm_hints)
        {
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
            self.prefetch_plan(mode, &synthetic_targets, hints.len())
        };
        let prefetch_targets = if self.config.uses_synthetic_trace() {
            self.prefetch_synthetic_targets_for_plan(&synthetic_targets, prefetch_plan)
        } else {
            Vec::new()
        };
        let prefetch_hints = if self.config.uses_synthetic_trace() {
            Vec::new()
        } else {
            self.prefetch_hints_for_plan(&hints, prefetch_plan)
        };
        if self.config.uses_synthetic_trace() {
            self.apply_synthetic_prewarm_state(&synthetic_targets);
        } else {
            self.apply_prewarm_state(&hints);
        }
        let start = Instant::now();

        match mode {
            PrefetchMode::Baseline => {
                let mut db = self.db.clone();
                self.execute_trace(&mut db);
            }
            PrefetchMode::Synchronous => {
                if prefetch_plan.should_prefetch {
                    let buffer = if self.config.uses_synthetic_trace() {
                        Self::prefetch_synthetic_targets_to_frozen_buffer(
                            self.db.clone(),
                            prefetch_targets,
                        )
                    } else {
                        Self::prefetch_to_frozen_buffer(self.db.clone(), prefetch_hints)
                    };
                    let mut db = PrefetchingDb::new(self.db.clone(), buffer);
                    self.execute_trace(&mut db);
                } else {
                    let mut db = self.db.clone();
                    self.execute_trace(&mut db);
                }
            }
            PrefetchMode::Asynchronous => {
                if prefetch_plan.should_prefetch {
                    let buffer =
                        PrefetchBuffer::concurrent(prefetch_hints.len().max(
                            prefetch_targets.iter().filter(|target| target.is_storage()).count(),
                        ));
                    let prefetch_db = self.db.clone();
                    let prefetch_buffer = buffer.clone();
                    let handle = if self.config.uses_synthetic_trace() {
                        thread::spawn(move || {
                            Self::prefetch_synthetic_targets(
                                prefetch_db,
                                prefetch_targets,
                                prefetch_buffer,
                            );
                        })
                    } else {
                        thread::spawn(move || {
                            Self::prefetch_slots(prefetch_db, prefetch_hints, prefetch_buffer);
                        })
                    };
                    if !self.config.prefetch_lead.is_zero() {
                        sleep(self.config.prefetch_lead);
                    }
                    let mut db = PrefetchingDb::new(self.db.clone(), buffer);
                    self.execute_trace(&mut db);
                    let _ = handle.join();
                } else {
                    let mut db = self.db.clone();
                    self.execute_trace(&mut db);
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

    fn build_synthetic_targets_from_config(
        config: &PrefetchExperimentConfig,
    ) -> Vec<PrefetchSyntheticTarget> {
        if config.synthetic_prefetch_targets.is_empty() {
            return Self::dedup_synthetic_targets(&config.synthetic_trace_reads);
        }
        Self::dedup_synthetic_targets(&config.synthetic_prefetch_targets)
    }

    fn dedup_synthetic_targets(
        targets: &[PrefetchSyntheticTarget],
    ) -> Vec<PrefetchSyntheticTarget> {
        let mut seen = std::collections::HashSet::with_capacity(targets.len());
        let mut unique = Vec::with_capacity(targets.len());
        for target in targets {
            if seen.insert(*target) {
                unique.push(*target);
            }
        }
        unique
    }

    fn prefetch_plan(
        &self,
        mode: PrefetchMode,
        synthetic_targets: &[PrefetchSyntheticTarget],
        hint_count: usize,
    ) -> PrefetchExecutionPlan {
        if !self.config.use_prefetch_planner {
            let target_count = if self.config.uses_synthetic_trace() {
                synthetic_targets.len()
            } else {
                hint_count
            };
            return PrefetchExecutionPlan {
                should_prefetch: mode != PrefetchMode::Baseline && target_count > 0,
                hint_limit: target_count,
                estimated_hidden_lookups_x100: (target_count.saturating_mul(100))
                    .min(u32::MAX as usize) as u32,
                projected_net_gain_ns: 0,
            };
        }

        if self.config.uses_synthetic_trace() {
            let synthetic_hidden_lookups_x100 = synthetic_targets
                .iter()
                .map(|target| target.hidden_lookups_x100())
                .fold(0_u32, u32::saturating_add);
            return PrefetchPlanner::plan_with_hidden_lookups(
                mode,
                synthetic_targets.len(),
                synthetic_hidden_lookups_x100,
                self.config.miss_latency,
                self.config.prefetch_cost_model,
            );
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
        if let Some(synthetic_scenario) = self.config.synthetic_scenario {
            return synthetic_scenario;
        }

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

    fn prefetch_synthetic_targets_for_plan(
        &self,
        targets: &[PrefetchSyntheticTarget],
        prefetch_plan: PrefetchExecutionPlan,
    ) -> Vec<PrefetchSyntheticTarget> {
        if !prefetch_plan.should_prefetch {
            return Vec::new();
        }

        targets.iter().copied().take(prefetch_plan.hint_limit.min(targets.len())).collect()
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

    fn apply_synthetic_prewarm_state(&self, targets: &[PrefetchSyntheticTarget]) {
        if self.config.prewarm_account_read
            && let Some(address) = targets.iter().find_map(|target| match target {
                PrefetchSyntheticTarget::Account { address }
                | PrefetchSyntheticTarget::AccountCode { address, .. } => Some(*address),
                PrefetchSyntheticTarget::Storage { .. }
                | PrefetchSyntheticTarget::BlockHash { .. } => None,
            })
        {
            self.db.warm_account(address);
        }

        if self.config.prewarm_code_read
            && let Some(code_hash) = targets.iter().find_map(|target| match target {
                PrefetchSyntheticTarget::AccountCode { code_hash, .. } => Some(*code_hash),
                PrefetchSyntheticTarget::Account { .. }
                | PrefetchSyntheticTarget::Storage { .. }
                | PrefetchSyntheticTarget::BlockHash { .. } => None,
            })
        {
            self.db.warm_code_hash(code_hash);
        }

        if self.config.prewarm_block_hash_read
            && let Some(number) = targets.iter().find_map(|target| match target {
                PrefetchSyntheticTarget::BlockHash { number } => Some(*number),
                PrefetchSyntheticTarget::Account { .. }
                | PrefetchSyntheticTarget::AccountCode { .. }
                | PrefetchSyntheticTarget::Storage { .. } => None,
            })
        {
            self.db.warm_block_hash(number);
        }

        let mut prewarmed_storage_hints = 0_usize;
        for target in targets {
            if prewarmed_storage_hints >= self.config.prewarmed_storage_hints {
                break;
            }
            if let PrefetchSyntheticTarget::Storage { address, slot } = target {
                self.db.warm_storage(*address, *slot);
                prewarmed_storage_hints = prewarmed_storage_hints.saturating_add(1);
            }
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

    fn prefetch_synthetic_targets_to_frozen_buffer(
        mut db: LatencyInjectingDb,
        targets: Vec<PrefetchSyntheticTarget>,
    ) -> PrefetchBuffer {
        let mut entries = StdHashMap::with_capacity(targets.len());
        for target in targets {
            match target {
                PrefetchSyntheticTarget::Account { address } => {
                    let _ = db.basic(address).expect("latency db reads are infallible");
                }
                PrefetchSyntheticTarget::AccountCode { address, code_hash } => {
                    let _ = db.basic(address).expect("latency db reads are infallible");
                    let _ = db.code_by_hash(code_hash).expect("latency db reads are infallible");
                }
                PrefetchSyntheticTarget::Storage { address, slot } => {
                    let value = db.storage(address, slot).expect("latency db reads are infallible");
                    entries.insert((address, slot), value);
                }
                PrefetchSyntheticTarget::BlockHash { number } => {
                    let _ = db.block_hash(number).expect("latency db reads are infallible");
                }
            }
        }
        PrefetchBuffer::frozen(entries)
    }

    fn prefetch_synthetic_targets(
        mut db: LatencyInjectingDb,
        targets: Vec<PrefetchSyntheticTarget>,
        buffer: PrefetchBuffer,
    ) {
        for target in targets {
            match target {
                PrefetchSyntheticTarget::Account { address } => {
                    let _ = db.basic(address).expect("latency db reads are infallible");
                }
                PrefetchSyntheticTarget::AccountCode { address, code_hash } => {
                    let _ = db.basic(address).expect("latency db reads are infallible");
                    let _ = db.code_by_hash(code_hash).expect("latency db reads are infallible");
                }
                PrefetchSyntheticTarget::Storage { address, slot } => {
                    let value = db.storage(address, slot).expect("latency db reads are infallible");
                    let _ = buffer.insert(address, slot, value);
                }
                PrefetchSyntheticTarget::BlockHash { number } => {
                    let _ = db.block_hash(number).expect("latency db reads are infallible");
                }
            }
        }
    }

    fn execute_trace<DB>(&self, db: &mut DB)
    where
        DB: Database<Error = Infallible> + DatabaseCommit,
    {
        if self.config.uses_synthetic_trace() {
            self.execute_scripted_trace(db);
            return;
        }

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

    fn execute_scripted_trace<DB>(&self, db: &mut DB)
    where
        DB: Database<Error = Infallible> + DatabaseCommit,
    {
        for (index, target) in self.config.synthetic_trace_reads.iter().copied().enumerate() {
            match target {
                PrefetchSyntheticTarget::Account { address } => {
                    let _ = db.basic(address).expect("latency db reads are infallible");
                }
                PrefetchSyntheticTarget::AccountCode { address, code_hash } => {
                    let _ = db.basic(address).expect("latency db reads are infallible");
                    let _ = db.code_by_hash(code_hash).expect("latency db reads are infallible");
                }
                PrefetchSyntheticTarget::Storage { address, slot } => {
                    let _ = db.storage(address, slot).expect("latency db reads are infallible");
                }
                PrefetchSyntheticTarget::BlockHash { number } => {
                    let _ = db.block_hash(number).expect("latency db reads are infallible");
                }
            }

            if index + 1 != self.config.synthetic_trace_reads.len() {
                self.sleep_execution_gap();
            }
        }
    }

    fn seed_synthetic_targets(
        db: &LatencyInjectingDb,
        targets: impl IntoIterator<Item = PrefetchSyntheticTarget>,
    ) {
        for (index, target) in
            Self::dedup_synthetic_targets(&targets.into_iter().collect::<Vec<_>>())
                .into_iter()
                .enumerate()
        {
            match target {
                PrefetchSyntheticTarget::Account { address } => {
                    db.insert_account(address, AccountInfo::default());
                }
                PrefetchSyntheticTarget::AccountCode { address, code_hash } => {
                    db.insert_account(address, AccountInfo { code_hash, ..Default::default() });
                    db.insert_bytecode(code_hash, Default::default());
                }
                PrefetchSyntheticTarget::Storage { address, slot } => {
                    db.insert_storage(address, slot, StorageValue::from((index as u64) + 1));
                }
                PrefetchSyntheticTarget::BlockHash { number } => {
                    db.insert_block_hash(
                        number,
                        B256::with_last_byte((index as u8).saturating_add(1)),
                    );
                }
            }
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
        let plan = experiment.prefetch_plan(PrefetchMode::Asynchronous, &[], 4);
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
            experiment.prefetch_plan(PrefetchMode::Asynchronous, &[], hints.len()),
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

    #[test]
    fn synthetic_universal_router_async_prefetch_improves_median_for_miss_heavy_case() {
        let mut config = PrefetchExperimentConfig::universal_router_two_hop_swap_like();
        config.iterations = 8;
        config.miss_latency = Duration::from_millis(2);
        config.execution_gap = Duration::from_millis(1);
        config.prefetch_lead = Duration::from_millis(6);

        let experiment = PrefetchExperiment::new(config);
        let baseline = experiment.run(PrefetchMode::Baseline);
        let asynchronous = experiment.run(PrefetchMode::Asynchronous);

        assert!(asynchronous.p50() < baseline.p50());
    }
}
