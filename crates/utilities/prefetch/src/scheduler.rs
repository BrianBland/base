use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    time::Duration,
};

use alloy_primitives::{Address, B256};
use revm::primitives::StorageKey;

use crate::{
    DowseSelector, FrontierPrefetchPlan, PrefetchCostModel, PrefetchExecutionPlan, PrefetchMode,
    PrefetchPlanner, PrefetchRuntimeDecisionReason, PrefetchRuntimeKey, PrefetchRuntimePolicy,
    PrefetchScenario, PrefetchTask, PrefetchTaskClass, PrefetchTaskKind,
};

/// Limits applied while scheduling frontier-prefetch work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchTaskBudget {
    /// Maximum total queued tasks across all depths and classes.
    pub max_total_tasks: usize,
    /// Maximum queued tasks for one absolute EVM depth.
    pub max_tasks_per_depth: usize,
    /// Maximum queued account-only tasks for one absolute depth.
    pub max_account_tasks_per_depth: usize,
    /// Maximum queued account+code tasks for one absolute depth.
    pub max_account_code_tasks_per_depth: usize,
    /// Maximum queued storage tasks for one absolute depth.
    pub max_storage_tasks_per_depth: usize,
}

impl PrefetchTaskBudget {
    /// Returns the per-depth limit for `task_class`.
    pub const fn class_limit(&self, task_class: PrefetchTaskClass) -> usize {
        match task_class {
            PrefetchTaskClass::Account => self.max_account_tasks_per_depth,
            PrefetchTaskClass::AccountCode => self.max_account_code_tasks_per_depth,
            PrefetchTaskClass::Storage => self.max_storage_tasks_per_depth,
        }
    }
}

impl Default for PrefetchTaskBudget {
    fn default() -> Self {
        Self {
            max_total_tasks: 24,
            max_tasks_per_depth: 12,
            max_account_tasks_per_depth: 4,
            max_account_code_tasks_per_depth: 8,
            max_storage_tasks_per_depth: 8,
        }
    }
}

/// Frontier scheduling policy applied before speculative work is queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchFrontierConfig {
    /// Maximum number of frames ahead that the scheduler may queue.
    pub max_depth_ahead: u8,
    /// Minimum planner confidence required for a task to be considered.
    pub min_confidence_x10000: u16,
    /// Multiplicative hidden-lookup penalty applied per frame of lookahead.
    pub speculative_depth_penalty_x10000: u16,
    /// Queue budget across depths and task classes.
    pub task_budget: PrefetchTaskBudget,
}

impl PrefetchFrontierConfig {
    /// Returns the maximum absolute depth allowed from `base_depth`.
    pub const fn depth_limit(&self, base_depth: u8) -> u8 {
        base_depth.saturating_add(self.max_depth_ahead)
    }

    /// Returns the multiplicative depth penalty for `depth_delta`, scaled by 10,000.
    pub fn depth_penalty_x10000(&self, depth_delta: u8) -> u32 {
        let mut penalty_x10000 = 10_000_u128;
        let mut remaining = depth_delta;
        while remaining > 0 {
            penalty_x10000 = penalty_x10000
                .saturating_mul(self.speculative_depth_penalty_x10000 as u128)
                / 10_000;
            remaining = remaining.saturating_sub(1);
        }
        penalty_x10000.min(u32::MAX as u128) as u32
    }

    /// Returns the depth-adjusted hidden-lookup estimate for `task`, scaled by 100.
    pub fn weighted_hidden_lookups_x100(&self, task: PrefetchTask, base_depth: u8) -> u32 {
        let depth_delta = task.depth.saturating_sub(base_depth);
        ((task.weighted_hidden_lookups_x100() as u128)
            .saturating_mul(self.depth_penalty_x10000(depth_delta) as u128)
            / 10_000)
            .min(u32::MAX as u128) as u32
    }
}

impl Default for PrefetchFrontierConfig {
    fn default() -> Self {
        Self {
            max_depth_ahead: 1,
            min_confidence_x10000: 4_000,
            speculative_depth_penalty_x10000: 8_000,
            task_budget: PrefetchTaskBudget::default(),
        }
    }
}

/// Runtime inputs shared by one frontier scheduling pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchFrontierRuntimeContext {
    /// Optional code hash for the current frame.
    pub code_hash: Option<B256>,
    /// Active selector for the current frame.
    pub selector: DowseSelector,
    /// Execution scenario associated with this frame.
    pub scenario: PrefetchScenario,
    /// Active prefetch mode.
    pub mode: PrefetchMode,
    /// Estimated cold storage-miss latency seen by the current execution.
    pub miss_latency: Duration,
    /// Current frame depth.
    pub base_depth: u8,
}

impl PrefetchFrontierRuntimeContext {
    /// Builds a runtime-policy key for `bucket`.
    pub const fn runtime_key(&self, bucket: PrefetchTaskBucket) -> PrefetchRuntimeKey {
        PrefetchRuntimeKey {
            code_hash: self.code_hash,
            selector: self.selector,
            scenario: self.scenario,
            depth: bucket.depth,
            task_class: Some(bucket.task_class),
        }
    }
}

/// One scheduling bucket keyed by relative depth and task class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PrefetchTaskBucket {
    /// Relative depth from the current frame.
    pub depth: u8,
    /// High-level task class.
    pub task_class: PrefetchTaskClass,
}

impl PrefetchTaskBucket {
    /// Returns the absolute depth for `base_depth`.
    pub const fn absolute_depth(&self, base_depth: u8) -> u8 {
        base_depth.saturating_add(self.depth)
    }
}

/// Filter and admission statistics from one scheduling pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PrefetchTaskFilterStats {
    /// Number of tasks inspected.
    pub inspected_tasks: u64,
    /// Tasks rejected because they target an older generation.
    pub dropped_stale_tasks: u64,
    /// Tasks collapsed because another task for the same key won.
    pub dropped_duplicate_tasks: u64,
    /// Tasks rejected because the targeted storage slot was already dirtied locally.
    pub dropped_dirty_tasks: u64,
    /// Tasks rejected because planner confidence was below the configured minimum.
    pub dropped_low_confidence_tasks: u64,
    /// Tasks rejected because they reached too far ahead in the call tree.
    pub dropped_depth_tasks: u64,
    /// Tasks that fit the runtime decision but were trimmed by scheduler budgets.
    pub dropped_budget_tasks: u64,
    /// Tasks trimmed by planner/runtime-policy gating.
    pub dropped_runtime_disabled_tasks: u64,
    /// Tasks actually admitted into the pending queue.
    pub enqueued_tasks: u64,
}

/// One runtime decision made for a scheduling bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchFrontierBucketDecision {
    /// Bucket that was evaluated.
    pub bucket: PrefetchTaskBucket,
    /// Candidate tasks available after static filtering.
    pub available_tasks: usize,
    /// Tasks admitted into the queue from this bucket.
    pub selected_tasks: usize,
    /// Planner/runtime decision for the bucket before queue-budget trimming.
    pub plan: PrefetchExecutionPlan,
    /// Runtime-policy explanation, if a runtime policy was consulted.
    pub runtime_reason: Option<PrefetchRuntimeDecisionReason>,
}

/// Result of one frontier scheduling pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrefetchSchedule {
    /// Tasks newly admitted into the queue.
    pub tasks: Vec<PrefetchTask>,
    /// Filter statistics gathered while producing the schedule.
    pub filter_stats: PrefetchTaskFilterStats,
    /// Per-bucket planner decisions.
    pub bucket_decisions: Vec<PrefetchFrontierBucketDecision>,
}

impl PrefetchSchedule {
    /// Sorts tasks from most useful to least useful.
    pub fn sort_tasks(&mut self) {
        self.tasks.sort_by(PrefetchFrontierScheduler::task_ordering);
    }
}

/// Queueing scheduler for deeper-tree prefetch work.
#[derive(Debug, Clone)]
pub struct PrefetchFrontierScheduler {
    config: PrefetchFrontierConfig,
    cost_model: PrefetchCostModel,
    runtime_policy: Option<PrefetchRuntimePolicy>,
    generation_floor: u64,
    pending_tasks: HashMap<PrefetchTaskKind, PrefetchTask>,
    dirty_storage_targets: HashSet<(Address, StorageKey)>,
}

impl PrefetchFrontierScheduler {
    /// Creates a new scheduler with the provided queue and runtime controls.
    pub fn new(
        config: PrefetchFrontierConfig,
        cost_model: PrefetchCostModel,
        runtime_policy: Option<PrefetchRuntimePolicy>,
    ) -> Self {
        Self {
            config,
            cost_model,
            runtime_policy,
            generation_floor: 0,
            pending_tasks: HashMap::new(),
            dirty_storage_targets: HashSet::new(),
        }
    }

    /// Returns the active frontier configuration.
    pub const fn config(&self) -> PrefetchFrontierConfig {
        self.config
    }

    /// Returns the active cost model.
    pub const fn cost_model(&self) -> PrefetchCostModel {
        self.cost_model
    }

    /// Returns the shared runtime policy, if one is configured.
    pub const fn runtime_policy(&self) -> Option<&PrefetchRuntimePolicy> {
        self.runtime_policy.as_ref()
    }

    /// Returns the minimum generation accepted by the scheduler.
    pub const fn generation_floor(&self) -> u64 {
        self.generation_floor
    }

    /// Updates the accepted generation floor and drops older queued tasks.
    pub fn set_generation_floor(&mut self, generation_floor: u64) {
        self.generation_floor = generation_floor;
        self.pending_tasks.retain(|_, task| task.generation >= generation_floor);
    }

    /// Marks one storage slot dirty and drops any queued prefetch for it.
    pub fn mark_storage_dirty(&mut self, address: Address, slot: StorageKey) {
        self.dirty_storage_targets.insert((address, slot));
        self.pending_tasks.retain(|_, task| task.kind.storage_target() != Some((address, slot)));
    }

    /// Clears all dirty-slot suppression state.
    pub fn clear_dirty_storage(&mut self) {
        self.dirty_storage_targets.clear();
    }

    /// Returns a sorted snapshot of currently queued tasks.
    pub fn pending_tasks(&self) -> Vec<PrefetchTask> {
        let mut tasks = self.pending_tasks.values().copied().collect::<Vec<_>>();
        tasks.sort_by(Self::task_ordering);
        tasks
    }

    /// Drains up to `max_count` highest-priority tasks from the queue.
    pub fn drain(&mut self, max_count: usize) -> Vec<PrefetchTask> {
        let tasks = self.pending_tasks();
        let drained = tasks.into_iter().take(max_count).collect::<Vec<_>>();
        for task in &drained {
            self.pending_tasks.remove(&task.kind);
        }
        drained
    }

    /// Schedules the tasks contained in `plan` according to the current policy.
    pub fn schedule(
        &mut self,
        context: PrefetchFrontierRuntimeContext,
        plan: FrontierPrefetchPlan,
    ) -> PrefetchSchedule {
        let mut schedule = PrefetchSchedule::default();
        let mut candidates_by_kind = HashMap::new();

        for task in plan.tasks {
            schedule.filter_stats.inspected_tasks =
                schedule.filter_stats.inspected_tasks.saturating_add(1);

            if task.generation < self.generation_floor {
                schedule.filter_stats.dropped_stale_tasks =
                    schedule.filter_stats.dropped_stale_tasks.saturating_add(1);
                continue;
            }

            if task.confidence_x10000 < self.config.min_confidence_x10000 {
                schedule.filter_stats.dropped_low_confidence_tasks =
                    schedule.filter_stats.dropped_low_confidence_tasks.saturating_add(1);
                continue;
            }

            if task.depth > self.config.depth_limit(context.base_depth) {
                schedule.filter_stats.dropped_depth_tasks =
                    schedule.filter_stats.dropped_depth_tasks.saturating_add(1);
                continue;
            }

            if let Some(storage_target) = task.kind.storage_target()
                && self.dirty_storage_targets.contains(&storage_target)
            {
                schedule.filter_stats.dropped_dirty_tasks =
                    schedule.filter_stats.dropped_dirty_tasks.saturating_add(1);
                continue;
            }

            let existing_task = candidates_by_kind
                .get(&task.kind)
                .copied()
                .or_else(|| self.pending_tasks.get(&task.kind).copied());
            if let Some(existing_task) = existing_task {
                schedule.filter_stats.dropped_duplicate_tasks =
                    schedule.filter_stats.dropped_duplicate_tasks.saturating_add(1);
                if Self::prefers_task(task, existing_task) {
                    candidates_by_kind.insert(task.kind, task);
                }
                continue;
            }

            candidates_by_kind.insert(task.kind, task);
        }

        let mut buckets = HashMap::<PrefetchTaskBucket, Vec<PrefetchTask>>::new();
        for task in candidates_by_kind.into_values() {
            let bucket = PrefetchTaskBucket {
                depth: task.depth.saturating_sub(context.base_depth),
                task_class: task.class(),
            };
            buckets.entry(bucket).or_default().push(task);
        }

        let mut bucket_entries = buckets.into_iter().collect::<Vec<_>>();
        bucket_entries.sort_by(|(left_bucket, _), (right_bucket, _)| {
            left_bucket
                .depth
                .cmp(&right_bucket.depth)
                .then(left_bucket.task_class.rank().cmp(&right_bucket.task_class.rank()))
        });

        let mut pending_total = self.pending_tasks.len();
        let mut pending_by_depth = self.count_pending_by_depth();
        let mut pending_by_depth_and_class = self.count_pending_by_depth_and_class();

        for (bucket, mut tasks) in bucket_entries {
            tasks.sort_by(Self::task_ordering);
            let available_tasks = tasks.len();
            let estimated_hidden_lookups_x100 = tasks
                .iter()
                .map(|task| self.config.weighted_hidden_lookups_x100(*task, context.base_depth))
                .fold(0_u32, u32::saturating_add);
            let base_plan = PrefetchPlanner::plan_with_hidden_lookups(
                context.mode,
                available_tasks,
                estimated_hidden_lookups_x100,
                context.miss_latency,
                self.cost_model,
            );
            let (plan, runtime_reason) =
                self.runtime_policy().map_or((base_plan, None), |runtime_policy| {
                    let decision = runtime_policy.decide(
                        context.runtime_key(bucket),
                        context.mode,
                        base_plan,
                        context.miss_latency,
                    );
                    (decision.plan, Some(decision.reason))
                });

            let planned_limit =
                if plan.should_prefetch { plan.hint_limit.min(available_tasks) } else { 0 };
            let absolute_depth = bucket.absolute_depth(context.base_depth);
            let remaining_total =
                self.config.task_budget.max_total_tasks.saturating_sub(pending_total);
            let remaining_depth = self
                .config
                .task_budget
                .max_tasks_per_depth
                .saturating_sub(*pending_by_depth.get(&absolute_depth).unwrap_or(&0));
            let remaining_class =
                self.config.task_budget.class_limit(bucket.task_class).saturating_sub(
                    *pending_by_depth_and_class
                        .get(&(absolute_depth, bucket.task_class))
                        .unwrap_or(&0),
                );
            let selected_limit =
                planned_limit.min(remaining_total).min(remaining_depth).min(remaining_class);

            schedule.filter_stats.dropped_runtime_disabled_tasks = schedule
                .filter_stats
                .dropped_runtime_disabled_tasks
                .saturating_add(available_tasks.saturating_sub(planned_limit) as u64);
            schedule.filter_stats.dropped_budget_tasks = schedule
                .filter_stats
                .dropped_budget_tasks
                .saturating_add(planned_limit.saturating_sub(selected_limit) as u64);

            schedule.bucket_decisions.push(PrefetchFrontierBucketDecision {
                bucket,
                available_tasks,
                selected_tasks: selected_limit,
                plan,
                runtime_reason,
            });

            for task in tasks.into_iter().take(selected_limit) {
                self.pending_tasks.insert(task.kind, task);
                pending_total = pending_total.saturating_add(1);
                *pending_by_depth.entry(task.depth).or_default() += 1;
                *pending_by_depth_and_class.entry((task.depth, task.class())).or_default() += 1;
                schedule.tasks.push(task);
            }
        }

        schedule.filter_stats.enqueued_tasks = schedule.tasks.len() as u64;
        schedule.sort_tasks();
        schedule
    }

    /// Returns `true` when `left` should replace `right`.
    pub fn prefers_task(left: PrefetchTask, right: PrefetchTask) -> bool {
        Self::task_ordering(&left, &right).is_lt()
    }

    /// Returns the queue ordering used for pending and selected tasks.
    pub fn task_ordering(left: &PrefetchTask, right: &PrefetchTask) -> Ordering {
        right
            .generation
            .cmp(&left.generation)
            .then(right.priority.cmp(&left.priority))
            .then(left.earliest_use_rank.cmp(&right.earliest_use_rank))
            .then(left.depth.cmp(&right.depth))
            .then(right.confidence_x10000.cmp(&left.confidence_x10000))
    }

    /// Returns current pending-task counts keyed by absolute EVM depth.
    pub fn count_pending_by_depth(&self) -> HashMap<u8, usize> {
        let mut counts = HashMap::new();
        for task in self.pending_tasks.values() {
            *counts.entry(task.depth).or_default() += 1;
        }
        counts
    }

    /// Returns current pending-task counts keyed by `(depth, class)`.
    pub fn count_pending_by_depth_and_class(&self) -> HashMap<(u8, PrefetchTaskClass), usize> {
        let mut counts = HashMap::new();
        for task in self.pending_tasks.values() {
            *counts.entry((task.depth, task.class())).or_default() += 1;
        }
        counts
    }
}

impl Default for PrefetchFrontierScheduler {
    fn default() -> Self {
        Self::new(PrefetchFrontierConfig::default(), PrefetchCostModel::default(), None)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alloy_primitives::{Address, B256};
    use revm::primitives::StorageKey;

    use super::{
        PrefetchFrontierConfig, PrefetchFrontierRuntimeContext, PrefetchFrontierScheduler,
        PrefetchTaskBucket, PrefetchTaskBudget,
    };
    use crate::{
        FrontierPrefetchPlan, PrefetchMetricsSnapshot, PrefetchMode, PrefetchRuntimeConfig,
        PrefetchRuntimeDecisionReason, PrefetchRuntimePolicy, PrefetchRuntimeSample,
        PrefetchScenario, PrefetchTask, PrefetchTaskClass, PrefetchTaskKind,
    };

    fn sample_runtime_context() -> PrefetchFrontierRuntimeContext {
        PrefetchFrontierRuntimeContext {
            code_hash: Some(B256::with_last_byte(1)),
            selector: None,
            scenario: PrefetchScenario::Swap { legs: 2 },
            mode: PrefetchMode::Asynchronous,
            miss_latency: Duration::from_micros(25),
            base_depth: 0,
        }
    }

    fn sample_plan(tasks: Vec<PrefetchTask>) -> FrontierPrefetchPlan {
        FrontierPrefetchPlan { tasks, predicted_calls: Vec::new() }
    }

    #[test]
    fn depth_budget_keeps_nearby_tasks_and_drops_far_future_work() {
        let mut scheduler = PrefetchFrontierScheduler::new(
            PrefetchFrontierConfig { max_depth_ahead: 1, ..Default::default() },
            Default::default(),
            None,
        );
        let token = Address::with_last_byte(0x11);
        let plan = sample_plan(vec![
            PrefetchTask::storage(1, 0, 900, 10_000, 0, token, StorageKey::from(1_u64)),
            PrefetchTask::account_code(1, 1, 800, 9_000, 1, Address::with_last_byte(0x22)),
            PrefetchTask::account_code(1, 2, 700, 9_000, 2, Address::with_last_byte(0x33)),
        ]);

        let schedule = scheduler.schedule(sample_runtime_context(), plan);

        assert_eq!(schedule.tasks.len(), 2);
        assert!(
            schedule
                .tasks
                .iter()
                .all(|task| task.depth <= sample_runtime_context().base_depth.saturating_add(1))
        );
        assert_eq!(schedule.filter_stats.dropped_depth_tasks, 1);
    }

    #[test]
    fn dirty_storage_and_duplicates_are_suppressed() {
        let mut scheduler = PrefetchFrontierScheduler::default();
        let token = Address::with_last_byte(0x44);
        let dirty_slot = StorageKey::from(9_u64);
        scheduler.mark_storage_dirty(token, dirty_slot);

        let duplicate =
            PrefetchTask::account_code(1, 1, 800, 9_000, 0, Address::with_last_byte(0x55));
        let better_duplicate =
            PrefetchTask::account_code(2, 1, 900, 9_500, 0, Address::with_last_byte(0x55));
        let schedule = scheduler.schedule(
            sample_runtime_context(),
            sample_plan(vec![
                PrefetchTask::storage(1, 0, 950, 10_000, 0, token, dirty_slot),
                duplicate,
                better_duplicate,
            ]),
        );

        assert_eq!(schedule.tasks, vec![better_duplicate]);
        assert_eq!(schedule.filter_stats.dropped_dirty_tasks, 1);
        assert_eq!(schedule.filter_stats.dropped_duplicate_tasks, 1);
    }

    #[test]
    fn runtime_policy_can_disable_speculative_account_code_bucket() {
        let policy = PrefetchRuntimePolicy::new(PrefetchRuntimeConfig {
            min_observations: 1,
            ..Default::default()
        });
        let runtime_context = sample_runtime_context();
        policy.record_enabled(
            runtime_context.runtime_key(PrefetchTaskBucket {
                depth: 1,
                task_class: PrefetchTaskClass::AccountCode,
            }),
            PrefetchRuntimeSample {
                requested_hint_count: 2,
                buffer_metrics: PrefetchMetricsSnapshot {
                    prefetched_entries: 2,
                    storage_prefetch_hits: 0,
                    storage_prefetch_misses: 2,
                    ..Default::default()
                },
                observed_storage_miss_latency: Some(Duration::from_micros(4)),
                observed_prefetch_overhead: Some(Duration::from_micros(20)),
            },
        );

        let mut scheduler =
            PrefetchFrontierScheduler::new(Default::default(), Default::default(), Some(policy));
        let token = Address::with_last_byte(0x66);
        let plan = sample_plan(vec![
            PrefetchTask::storage(1, 0, 950, 10_000, 0, token, StorageKey::from(1_u64)),
            PrefetchTask::account_code(1, 1, 900, 9_000, 0, Address::with_last_byte(0x77)),
        ]);

        let schedule = scheduler.schedule(runtime_context, plan);

        assert_eq!(schedule.tasks.len(), 1);
        assert!(matches!(
            schedule.bucket_decisions.iter().find(|decision| decision.bucket.depth == 1),
            Some(decision)
                if decision.runtime_reason == Some(PrefetchRuntimeDecisionReason::AdaptiveDisabled)
                    && decision.selected_tasks == 0
        ));
        assert!(matches!(schedule.tasks[0].kind, PrefetchTaskKind::Storage { .. }));
    }

    #[test]
    fn queue_budget_limits_one_depth_bucket() {
        let mut scheduler = PrefetchFrontierScheduler::new(
            PrefetchFrontierConfig {
                task_budget: PrefetchTaskBudget {
                    max_total_tasks: 8,
                    max_tasks_per_depth: 1,
                    max_account_tasks_per_depth: 1,
                    max_account_code_tasks_per_depth: 1,
                    max_storage_tasks_per_depth: 1,
                },
                ..Default::default()
            },
            Default::default(),
            None,
        );

        let schedule = scheduler.schedule(
            sample_runtime_context(),
            sample_plan(vec![
                PrefetchTask::storage(
                    1,
                    0,
                    950,
                    10_000,
                    0,
                    Address::with_last_byte(0x88),
                    StorageKey::from(1_u64),
                ),
                PrefetchTask::storage(
                    1,
                    0,
                    940,
                    10_000,
                    1,
                    Address::with_last_byte(0x89),
                    StorageKey::from(2_u64),
                ),
            ]),
        );

        assert_eq!(schedule.tasks.len(), 1);
        assert_eq!(schedule.filter_stats.dropped_budget_tasks, 1);
    }

    #[test]
    fn drain_returns_tasks_in_queue_priority_order() {
        let mut scheduler = PrefetchFrontierScheduler::default();
        let runtime_context = sample_runtime_context();
        let first = PrefetchTask::storage(
            1,
            0,
            900,
            10_000,
            1,
            Address::with_last_byte(0x90),
            StorageKey::from(1_u64),
        );
        let second = PrefetchTask::account_code(2, 1, 950, 9_000, 0, Address::with_last_byte(0x91));
        scheduler.schedule(runtime_context, sample_plan(vec![first, second]));

        let drained = scheduler.drain(2);

        assert_eq!(drained, vec![second, first]);
        assert!(scheduler.pending_tasks().is_empty());
    }
}
