use std::time::Duration;

use crate::PrefetchMode;

/// Estimated execution scenario used to choose prefetch policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchScenario {
    /// One `transfer`-style read pattern.
    Transfer,
    /// One `transferFrom`-style read pattern.
    TransferFrom,
    /// Swap-like flow with multiple transfer legs.
    Swap {
        /// Number of transfer legs expected during execution.
        legs: usize,
    },
}

/// Cost model used by [`PrefetchPlanner`] to decide whether prefetching should run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchCostModel {
    /// Fixed wall-clock overhead of asynchronous prefetching.
    pub async_fixed_overhead: Duration,
    /// Fixed wall-clock overhead of synchronous prefetching.
    pub sync_fixed_overhead: Duration,
    /// Estimated hidden lookups for a transfer, scaled by 100.
    pub transfer_hidden_lookups_x100: u32,
    /// Estimated hidden lookups for a transferFrom, scaled by 100.
    pub transfer_from_hidden_lookups_x100: u32,
    /// Estimated hidden lookups per swap leg, scaled by 100.
    pub swap_hidden_lookups_per_leg_x100: u32,
    /// Maximum number of hints to prefetch for one execution.
    pub max_prefetch_hints: usize,
    /// Minimum miss latency for prefetching to be considered.
    pub minimum_miss_latency: Duration,
}

impl Default for PrefetchCostModel {
    fn default() -> Self {
        Self {
            async_fixed_overhead: Duration::from_micros(8),
            sync_fixed_overhead: Duration::from_micros(14),
            transfer_hidden_lookups_x100: 250,
            transfer_from_hidden_lookups_x100: 274,
            swap_hidden_lookups_per_leg_x100: 225,
            max_prefetch_hints: 16,
            minimum_miss_latency: Duration::from_micros(3),
        }
    }
}

/// Planner output describing whether to run prefetching and how many hints to prefetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchExecutionPlan {
    /// Whether prefetching should be executed for this run.
    pub should_prefetch: bool,
    /// Number of hints to prefetch (if enabled).
    pub hint_limit: usize,
    /// Estimated hidden lookups, scaled by 100.
    pub estimated_hidden_lookups_x100: u32,
    /// Estimated net gain in nanoseconds (negative means loss).
    pub projected_net_gain_ns: i128,
}

/// Cost-aware planner for deciding prefetch eligibility.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchPlanner;

impl PrefetchPlanner {
    /// Computes a prefetch execution plan for the given scenario and mode.
    pub fn plan(
        mode: PrefetchMode,
        scenario: PrefetchScenario,
        hint_count: usize,
        miss_latency: Duration,
        cost_model: PrefetchCostModel,
    ) -> PrefetchExecutionPlan {
        if mode == PrefetchMode::Baseline || hint_count == 0 {
            return PrefetchExecutionPlan {
                should_prefetch: false,
                hint_limit: 0,
                estimated_hidden_lookups_x100: 0,
                projected_net_gain_ns: 0,
            };
        }

        if miss_latency < cost_model.minimum_miss_latency {
            return PrefetchExecutionPlan {
                should_prefetch: false,
                hint_limit: 0,
                estimated_hidden_lookups_x100: 0,
                projected_net_gain_ns: 0,
            };
        }

        let scenario_hidden_x100 = Self::scenario_hidden_lookups_x100(scenario, cost_model);
        let hint_bound_x100 = hint_count.saturating_mul(100).min(u32::MAX as usize) as u32;
        let effective_hidden_x100 = scenario_hidden_x100.min(hint_bound_x100);
        let estimated_hint_limit = Self::hint_limit_from_hidden(effective_hidden_x100).max(1);
        let hint_limit = hint_count.min(cost_model.max_prefetch_hints).min(estimated_hint_limit);

        let miss_ns = miss_latency.as_nanos();
        let saved_ns = ((effective_hidden_x100 as u128).saturating_mul(miss_ns)) / 100;
        let overhead_ns = Self::mode_overhead(mode, cost_model).as_nanos();
        let projected_net_gain_ns = (saved_ns as i128) - (overhead_ns as i128);

        PrefetchExecutionPlan {
            should_prefetch: projected_net_gain_ns > 0 && hint_limit > 0,
            hint_limit: if projected_net_gain_ns > 0 { hint_limit } else { 0 },
            estimated_hidden_lookups_x100: effective_hidden_x100,
            projected_net_gain_ns,
        }
    }

    const fn mode_overhead(mode: PrefetchMode, cost_model: PrefetchCostModel) -> Duration {
        match mode {
            PrefetchMode::Baseline => Duration::ZERO,
            PrefetchMode::Synchronous => cost_model.sync_fixed_overhead,
            PrefetchMode::Asynchronous => cost_model.async_fixed_overhead,
        }
    }

    const fn scenario_hidden_lookups_x100(
        scenario: PrefetchScenario,
        cost_model: PrefetchCostModel,
    ) -> u32 {
        match scenario {
            PrefetchScenario::Transfer => cost_model.transfer_hidden_lookups_x100,
            PrefetchScenario::TransferFrom => cost_model.transfer_from_hidden_lookups_x100,
            PrefetchScenario::Swap { legs } => {
                cost_model.swap_hidden_lookups_per_leg_x100.saturating_mul(legs as u32)
            }
        }
    }

    const fn hint_limit_from_hidden(hidden_x100: u32) -> usize {
        hidden_x100.saturating_add(99).saturating_div(100) as usize
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{PrefetchCostModel, PrefetchPlanner, PrefetchScenario};
    use crate::PrefetchMode;

    #[test]
    fn skips_prefetch_for_low_miss_latency() {
        let plan = PrefetchPlanner::plan(
            PrefetchMode::Asynchronous,
            PrefetchScenario::TransferFrom,
            5,
            Duration::from_nanos(500),
            PrefetchCostModel::default(),
        );
        assert!(!plan.should_prefetch);
        assert_eq!(plan.hint_limit, 0);
    }

    #[test]
    fn enables_prefetch_for_miss_heavy_transfer_from() {
        let plan = PrefetchPlanner::plan(
            PrefetchMode::Asynchronous,
            PrefetchScenario::TransferFrom,
            5,
            Duration::from_micros(100),
            PrefetchCostModel::default(),
        );
        assert!(plan.should_prefetch);
        assert!(plan.hint_limit > 0);
        assert!(plan.projected_net_gain_ns > 0);
    }

    #[test]
    fn caps_hints_to_estimated_hidden_reads() {
        let plan = PrefetchPlanner::plan(
            PrefetchMode::Asynchronous,
            PrefetchScenario::Transfer,
            20,
            Duration::from_micros(50),
            PrefetchCostModel::default(),
        );
        assert!(plan.should_prefetch);
        assert!(plan.hint_limit < 20);
    }

    #[test]
    fn swap_scenario_scales_hidden_reads_by_leg_count() {
        let three_legs = PrefetchPlanner::plan(
            PrefetchMode::Asynchronous,
            PrefetchScenario::Swap { legs: 3 },
            12,
            Duration::from_micros(50),
            PrefetchCostModel::default(),
        );
        let one_leg = PrefetchPlanner::plan(
            PrefetchMode::Asynchronous,
            PrefetchScenario::Swap { legs: 1 },
            12,
            Duration::from_micros(50),
            PrefetchCostModel::default(),
        );
        assert!(three_legs.estimated_hidden_lookups_x100 > one_leg.estimated_hidden_lookups_x100);
    }
}
