use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;
use dashmap::DashMap;

use crate::{
    DowseSelector, PrefetchExecutionPlan, PrefetchMetricsSnapshot, PrefetchMode, PrefetchScenario,
    PrefetchTaskClass,
};

/// Stable key used to track runtime prefetch effectiveness over repeated executions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PrefetchRuntimeKey {
    /// Optional code hash for the contract currently being executed.
    pub code_hash: Option<B256>,
    /// Active selector for the current call, or `None` for wildcard/no-selector contexts.
    pub selector: DowseSelector,
    /// High-level execution scenario associated with the call.
    pub scenario: PrefetchScenario,
    /// Relative depth of the bucket being adapted, where `0` means the current frame.
    pub depth: u8,
    /// Optional task class for frontier-task buckets.
    pub task_class: Option<PrefetchTaskClass>,
}

/// Configuration for adaptive runtime prefetch enablement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchRuntimeConfig {
    /// Minimum number of observations required before runtime adaptation overrides static plans.
    pub min_observations: u64,
    /// Number of disabled executions between probe runs.
    pub probe_interval: u64,
    /// Maximum hints to prefetch during a probe run.
    pub probe_hint_limit: usize,
    /// EWMA update weight, scaled by 10,000.
    pub ewma_weight_x10000: u16,
}

impl Default for PrefetchRuntimeConfig {
    fn default() -> Self {
        Self {
            min_observations: 8,
            probe_interval: 32,
            probe_hint_limit: 2,
            ewma_weight_x10000: 2_500,
        }
    }
}

/// One runtime observation used to adapt future prefetch plans.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PrefetchRuntimeSample {
    /// Number of hints the caller attempted or planned to prefetch.
    pub requested_hint_count: usize,
    /// Shared prefetch telemetry collected during the execution.
    pub buffer_metrics: PrefetchMetricsSnapshot,
    /// Observed average latency for database-backed storage reads in this execution.
    pub observed_storage_miss_latency: Option<Duration>,
    /// Observed prefetch overhead attributable to the execution.
    pub observed_prefetch_overhead: Option<Duration>,
}

impl PrefetchRuntimeSample {
    /// Returns the useful-prefetch ratio, scaled by 10,000.
    pub fn useful_prefetch_ratio_x10000(&self) -> u32 {
        let requested_hint_count =
            self.requested_hint_count.max(self.buffer_metrics.unique_prefetched_entries() as usize);
        self.buffer_metrics.useful_prefetch_ratio_x10000(requested_hint_count)
    }

    /// Returns the best available storage miss latency estimate for this sample.
    pub fn storage_miss_latency(&self) -> Duration {
        self.observed_storage_miss_latency
            .unwrap_or_else(|| self.buffer_metrics.average_storage_db_latency())
    }

    /// Returns the best available prefetch overhead estimate for this sample.
    pub fn prefetch_overhead(&self) -> Duration {
        self.observed_prefetch_overhead.unwrap_or_default()
    }
}

/// Aggregated runtime effectiveness stats for a prefetch key.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PrefetchRuntimeStats {
    /// Total observations recorded for this key.
    pub observations: u64,
    /// Observations where prefetching was enabled.
    pub enabled_observations: u64,
    /// Observations where prefetching was disabled.
    pub disabled_observations: u64,
    /// EWMA of useful-prefetch ratio, scaled by 10,000.
    pub ewma_useful_prefetch_ratio_x10000: u32,
    /// EWMA of observed storage miss latency in nanoseconds.
    pub ewma_storage_miss_latency_ns: u64,
    /// EWMA of observed prefetch overhead in nanoseconds.
    pub ewma_prefetch_overhead_ns: u64,
    /// Most recent runtime-projected net gain in nanoseconds.
    pub last_projected_net_gain_ns: i128,
}

impl PrefetchRuntimeStats {
    /// Returns the EWMA storage miss latency.
    pub const fn ewma_storage_miss_latency(&self) -> Duration {
        Duration::from_nanos(self.ewma_storage_miss_latency_ns)
    }

    /// Returns the EWMA prefetch overhead.
    pub const fn ewma_prefetch_overhead(&self) -> Duration {
        Duration::from_nanos(self.ewma_prefetch_overhead_ns)
    }
}

/// Reason why runtime policy returned a particular plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchRuntimeDecisionReason {
    /// The static plan was used because runtime adaptation had insufficient data.
    StaticPlan,
    /// Runtime data supports keeping prefetch enabled, possibly with a reduced hint limit.
    AdaptiveEnabled,
    /// Runtime data indicates prefetch should remain disabled.
    AdaptiveDisabled,
    /// Runtime data indicates prefetch is currently disabled, but a probe run should be attempted.
    AdaptiveProbe,
}

/// Result of applying runtime adaptation to a static prefetch plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchRuntimeDecision {
    /// Final plan to execute.
    pub plan: PrefetchExecutionPlan,
    /// Explanation for why this plan was chosen.
    pub reason: PrefetchRuntimeDecisionReason,
}

/// Adaptive runtime controller for prefetch enablement and periodic probe runs.
#[derive(Debug, Clone)]
pub struct PrefetchRuntimePolicy {
    config: PrefetchRuntimeConfig,
    stats: Arc<DashMap<PrefetchRuntimeKey, PrefetchRuntimeStats>>,
}

impl PrefetchRuntimePolicy {
    /// Creates a new adaptive runtime controller.
    pub fn new(config: PrefetchRuntimeConfig) -> Self {
        Self { config, stats: Arc::new(DashMap::new()) }
    }

    /// Returns the runtime configuration.
    pub const fn config(&self) -> PrefetchRuntimeConfig {
        self.config
    }

    /// Returns the currently known stats for `key`, if any.
    pub fn stats(&self, key: PrefetchRuntimeKey) -> Option<PrefetchRuntimeStats> {
        self.stats.get(&key).map(|entry| *entry)
    }

    /// Returns a point-in-time copy of all runtime stats.
    pub fn all_stats(&self) -> Vec<(PrefetchRuntimeKey, PrefetchRuntimeStats)> {
        self.stats.iter().map(|entry| (*entry.key(), *entry.value())).collect()
    }

    /// Applies runtime adaptation to the provided static plan.
    pub fn decide(
        &self,
        key: PrefetchRuntimeKey,
        mode: PrefetchMode,
        base_plan: PrefetchExecutionPlan,
        miss_latency: Duration,
    ) -> PrefetchRuntimeDecision {
        if mode == PrefetchMode::Baseline || !base_plan.should_prefetch || base_plan.hint_limit == 0
        {
            return PrefetchRuntimeDecision {
                plan: PrefetchExecutionPlan { should_prefetch: false, hint_limit: 0, ..base_plan },
                reason: PrefetchRuntimeDecisionReason::StaticPlan,
            };
        }

        let Some(stats) = self.stats(key) else {
            return PrefetchRuntimeDecision {
                plan: base_plan,
                reason: PrefetchRuntimeDecisionReason::StaticPlan,
            };
        };

        if stats.observations < self.config.min_observations {
            return PrefetchRuntimeDecision {
                plan: base_plan,
                reason: PrefetchRuntimeDecisionReason::StaticPlan,
            };
        }

        let useful_ratio_x10000 = stats.ewma_useful_prefetch_ratio_x10000.max(1);
        let observed_miss_latency_ns = stats
            .ewma_storage_miss_latency_ns
            .max(miss_latency.as_nanos().min(u64::MAX as u128) as u64);
        let static_overhead_ns =
            Self::static_overhead(mode).as_nanos().min(u64::MAX as u128) as u64;
        let overhead_ns = stats.ewma_prefetch_overhead_ns.max(static_overhead_ns);
        let adjusted_hidden_x100 = ((base_plan.estimated_hidden_lookups_x100 as u128)
            .saturating_mul(u128::from(useful_ratio_x10000))
            / 10_000)
            .min(u32::MAX as u128) as u32;
        let adjusted_saved_ns = ((adjusted_hidden_x100 as u128)
            .saturating_mul(u128::from(observed_miss_latency_ns)))
            / 100;
        let adjusted_projected_net_gain_ns = adjusted_saved_ns as i128 - i128::from(overhead_ns);

        if adjusted_projected_net_gain_ns > 0 {
            let scaled_hint_limit = ((base_plan.hint_limit as u128)
                .saturating_mul(u128::from(useful_ratio_x10000))
                .saturating_add(9_999))
                / 10_000;
            let hint_limit =
                base_plan.hint_limit.min(scaled_hint_limit.max(1).min(usize::MAX as u128) as usize);
            return PrefetchRuntimeDecision {
                plan: PrefetchExecutionPlan {
                    should_prefetch: true,
                    hint_limit,
                    estimated_hidden_lookups_x100: adjusted_hidden_x100,
                    projected_net_gain_ns: adjusted_projected_net_gain_ns,
                },
                reason: PrefetchRuntimeDecisionReason::AdaptiveEnabled,
            };
        }

        let next_disabled_observations = stats.disabled_observations.saturating_add(1);
        let should_probe = self.config.probe_interval > 0
            && next_disabled_observations % self.config.probe_interval == 0;
        if should_probe {
            return PrefetchRuntimeDecision {
                plan: PrefetchExecutionPlan {
                    should_prefetch: true,
                    hint_limit: base_plan.hint_limit.min(self.config.probe_hint_limit.max(1)),
                    estimated_hidden_lookups_x100: adjusted_hidden_x100,
                    projected_net_gain_ns: adjusted_projected_net_gain_ns,
                },
                reason: PrefetchRuntimeDecisionReason::AdaptiveProbe,
            };
        }

        PrefetchRuntimeDecision {
            plan: PrefetchExecutionPlan {
                should_prefetch: false,
                hint_limit: 0,
                estimated_hidden_lookups_x100: adjusted_hidden_x100,
                projected_net_gain_ns: adjusted_projected_net_gain_ns,
            },
            reason: PrefetchRuntimeDecisionReason::AdaptiveDisabled,
        }
    }

    /// Records an execution where prefetching was enabled.
    pub fn record_enabled(&self, key: PrefetchRuntimeKey, sample: PrefetchRuntimeSample) {
        self.record_sample(key, true, sample);
    }

    /// Records an execution where prefetching was disabled.
    pub fn record_disabled(&self, key: PrefetchRuntimeKey, sample: PrefetchRuntimeSample) {
        self.record_sample(key, false, sample);
    }

    /// Records one runtime sample and updates the adaptive stats.
    pub fn record_sample(
        &self,
        key: PrefetchRuntimeKey,
        prefetched: bool,
        sample: PrefetchRuntimeSample,
    ) {
        let mut stats = self.stats(key).unwrap_or_default();
        stats.observations = stats.observations.saturating_add(1);
        if prefetched {
            stats.enabled_observations = stats.enabled_observations.saturating_add(1);
        } else {
            stats.disabled_observations = stats.disabled_observations.saturating_add(1);
        }

        stats.ewma_useful_prefetch_ratio_x10000 = Self::update_ewma_u32(
            stats.ewma_useful_prefetch_ratio_x10000,
            sample.useful_prefetch_ratio_x10000(),
            self.config.ewma_weight_x10000,
        );

        let observed_storage_miss_latency_ns =
            sample.storage_miss_latency().as_nanos().min(u64::MAX as u128) as u64;
        if observed_storage_miss_latency_ns > 0 {
            stats.ewma_storage_miss_latency_ns = Self::update_ewma_u64(
                stats.ewma_storage_miss_latency_ns,
                observed_storage_miss_latency_ns,
                self.config.ewma_weight_x10000,
            );
        }

        let observed_prefetch_overhead_ns =
            sample.prefetch_overhead().as_nanos().min(u64::MAX as u128) as u64;
        if observed_prefetch_overhead_ns > 0 {
            stats.ewma_prefetch_overhead_ns = Self::update_ewma_u64(
                stats.ewma_prefetch_overhead_ns,
                observed_prefetch_overhead_ns,
                self.config.ewma_weight_x10000,
            );
        }

        self.stats.insert(key, stats);
    }

    const fn static_overhead(mode: PrefetchMode) -> Duration {
        match mode {
            PrefetchMode::Baseline => Duration::ZERO,
            PrefetchMode::Synchronous => Duration::from_micros(14),
            PrefetchMode::Asynchronous => Duration::from_micros(8),
        }
    }

    const fn update_ewma_u32(previous: u32, observed: u32, weight_x10000: u16) -> u32 {
        if previous == 0 {
            return observed;
        }
        let inverse_weight = (10_000_u16 - weight_x10000) as u128;
        let weight = weight_x10000 as u128;
        (((previous as u128).saturating_mul(inverse_weight)
            + (observed as u128).saturating_mul(weight))
            / 10_000) as u32
    }

    const fn update_ewma_u64(previous: u64, observed: u64, weight_x10000: u16) -> u64 {
        if previous == 0 {
            return observed;
        }
        let inverse_weight = (10_000_u16 - weight_x10000) as u128;
        let weight = weight_x10000 as u128;
        (((previous as u128).saturating_mul(inverse_weight)
            + (observed as u128).saturating_mul(weight))
            / 10_000) as u64
    }
}

impl Default for PrefetchRuntimePolicy {
    fn default() -> Self {
        Self::new(PrefetchRuntimeConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alloy_primitives::B256;

    use super::{
        PrefetchRuntimeConfig, PrefetchRuntimeDecisionReason, PrefetchRuntimeKey,
        PrefetchRuntimePolicy, PrefetchRuntimeSample,
    };
    use crate::{PrefetchExecutionPlan, PrefetchMetricsSnapshot, PrefetchMode, PrefetchScenario};

    fn sample_key() -> PrefetchRuntimeKey {
        PrefetchRuntimeKey {
            code_hash: Some(B256::with_last_byte(1)),
            selector: None,
            scenario: PrefetchScenario::TransferFrom,
            depth: 0,
            task_class: None,
        }
    }

    fn sample_plan() -> PrefetchExecutionPlan {
        PrefetchExecutionPlan {
            should_prefetch: true,
            hint_limit: 4,
            estimated_hidden_lookups_x100: 250,
            projected_net_gain_ns: 0,
        }
    }

    #[test]
    fn falls_back_to_static_plan_without_enough_observations() {
        let policy = PrefetchRuntimePolicy::new(PrefetchRuntimeConfig {
            min_observations: 2,
            ..Default::default()
        });
        let key = sample_key();
        policy.record_enabled(
            key,
            PrefetchRuntimeSample {
                requested_hint_count: 4,
                buffer_metrics: PrefetchMetricsSnapshot {
                    prefetched_entries: 4,
                    storage_prefetch_hits: 4,
                    ..Default::default()
                },
                observed_storage_miss_latency: Some(Duration::from_micros(25)),
                observed_prefetch_overhead: Some(Duration::from_micros(8)),
            },
        );

        let decision = policy.decide(
            key,
            PrefetchMode::Asynchronous,
            sample_plan(),
            Duration::from_micros(25),
        );

        assert_eq!(decision.reason, PrefetchRuntimeDecisionReason::StaticPlan);
        assert_eq!(decision.plan.hint_limit, 4);
    }

    #[test]
    fn disables_prefetch_when_observed_usefulness_collapses() {
        let policy = PrefetchRuntimePolicy::new(PrefetchRuntimeConfig {
            min_observations: 1,
            probe_interval: 8,
            ..Default::default()
        });
        let key = sample_key();
        policy.record_enabled(
            key,
            PrefetchRuntimeSample {
                requested_hint_count: 4,
                buffer_metrics: PrefetchMetricsSnapshot {
                    prefetched_entries: 4,
                    storage_prefetch_hits: 0,
                    storage_prefetch_misses: 4,
                    ..Default::default()
                },
                observed_storage_miss_latency: Some(Duration::from_micros(4)),
                observed_prefetch_overhead: Some(Duration::from_micros(12)),
            },
        );

        let decision =
            policy.decide(key, PrefetchMode::Asynchronous, sample_plan(), Duration::from_micros(4));

        assert_eq!(decision.reason, PrefetchRuntimeDecisionReason::AdaptiveDisabled);
        assert!(!decision.plan.should_prefetch);
        assert_eq!(decision.plan.hint_limit, 0);
    }

    #[test]
    fn schedules_probe_runs_while_adaptively_disabled() {
        let policy = PrefetchRuntimePolicy::new(PrefetchRuntimeConfig {
            min_observations: 1,
            probe_interval: 3,
            probe_hint_limit: 1,
            ..Default::default()
        });
        let key = sample_key();
        policy.record_disabled(
            key,
            PrefetchRuntimeSample {
                requested_hint_count: 4,
                buffer_metrics: PrefetchMetricsSnapshot::default(),
                observed_storage_miss_latency: Some(Duration::from_micros(4)),
                observed_prefetch_overhead: Some(Duration::from_micros(12)),
            },
        );

        let disabled_decision =
            policy.decide(key, PrefetchMode::Asynchronous, sample_plan(), Duration::from_micros(4));
        assert_eq!(disabled_decision.reason, PrefetchRuntimeDecisionReason::AdaptiveDisabled);

        policy.record_disabled(
            key,
            PrefetchRuntimeSample {
                requested_hint_count: 4,
                buffer_metrics: PrefetchMetricsSnapshot::default(),
                observed_storage_miss_latency: Some(Duration::from_micros(4)),
                observed_prefetch_overhead: Some(Duration::from_micros(12)),
            },
        );

        let probe_decision =
            policy.decide(key, PrefetchMode::Asynchronous, sample_plan(), Duration::from_micros(4));
        assert_eq!(probe_decision.reason, PrefetchRuntimeDecisionReason::AdaptiveProbe);
        assert!(probe_decision.plan.should_prefetch);
        assert_eq!(probe_decision.plan.hint_limit, 1);
    }

    #[test]
    fn scales_hint_limit_down_to_match_observed_usefulness() {
        let policy = PrefetchRuntimePolicy::new(PrefetchRuntimeConfig {
            min_observations: 1,
            ..Default::default()
        });
        let key = sample_key();
        policy.record_enabled(
            key,
            PrefetchRuntimeSample {
                requested_hint_count: 4,
                buffer_metrics: PrefetchMetricsSnapshot {
                    prefetched_entries: 4,
                    storage_prefetch_hits: 1,
                    ..Default::default()
                },
                observed_storage_miss_latency: Some(Duration::from_micros(100)),
                observed_prefetch_overhead: Some(Duration::from_micros(8)),
            },
        );

        let decision = policy.decide(
            key,
            PrefetchMode::Asynchronous,
            sample_plan(),
            Duration::from_micros(100),
        );

        assert_eq!(decision.reason, PrefetchRuntimeDecisionReason::AdaptiveEnabled);
        assert!(decision.plan.should_prefetch);
        assert_eq!(decision.plan.hint_limit, 1);
    }
}
