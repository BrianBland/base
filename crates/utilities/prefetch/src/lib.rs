#![doc = include_str!("../README.md")]

mod experiment;
pub use experiment::{
    PrefetchExperiment, PrefetchExperimentConfig, PrefetchMode, PrefetchRunResult,
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
pub use prefetching_db::{PrefetchBuffer, PrefetchingDb};
