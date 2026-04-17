#![doc = include_str!("../README.md")]

mod dowse_hints;
pub use dowse_hints::{
    DowseHintTable, DowseHintTableLoadError, DowseHintTableMetadata, DowseHintTableStore,
    DowsePrefetchContext, DowsePrefetchItem, DowseResolvedPrefetchTarget, DowseSelector,
    DowseSelectorMap, DowseSlotExpression,
};

mod experiment;
pub use experiment::{
    PrefetchExperiment, PrefetchExperimentConfig, PrefetchMode, PrefetchRunResult,
    PrefetchSyntheticTarget,
};

mod frontier;
pub use frontier::{
    ChildCallPrediction, Erc20FrontierPlanner, FrontierFramePlanner, FrontierPlannerHandle,
    FrontierPlannerRegistry, FrontierPrefetchPlan, PrefetchAbiDecoder, PrefetchCallsiteContext,
    PrefetchExternalCallKind, PrefetchFrameContext, PrefetchTask, PrefetchTaskClass,
    PrefetchTaskKind, PrefetchWellKnownSelectors, UniswapV2FactoryConfig,
    UniswapV2PairFrontierPlanner, UniswapV2RouterFrontierPlanner, Weth9FrontierPlanner,
};

mod hints;
pub use hints::{
    Erc20Context, Erc20StorageLayout, Erc20SwapContext, Erc20SwapLeg, PrefetchHintBuilder, TxShape,
};

mod latency_db;
pub use latency_db::{LatencyDbStats, LatencyInjectingDb, LatencyInjectingDbConfig};

mod planner;
pub use planner::{PrefetchCostModel, PrefetchExecutionPlan, PrefetchPlanner, PrefetchScenario};

mod prefetching_db;
pub use prefetching_db::{PrefetchBuffer, PrefetchMetrics, PrefetchMetricsSnapshot, PrefetchingDb};

mod runtime_policy;
pub use runtime_policy::{
    PrefetchRuntimeConfig, PrefetchRuntimeDecision, PrefetchRuntimeDecisionReason,
    PrefetchRuntimeKey, PrefetchRuntimePolicy, PrefetchRuntimeSample, PrefetchRuntimeStats,
};

mod scheduler;
pub use scheduler::{
    PrefetchFrontierBucketDecision, PrefetchFrontierConfig, PrefetchFrontierRuntimeContext,
    PrefetchFrontierScheduler, PrefetchSchedule, PrefetchTaskBucket, PrefetchTaskBudget,
    PrefetchTaskFilterStats,
};

mod state_view;
pub use state_view::{PrefetchStateViewFactory, PrefetchStateViewId};
