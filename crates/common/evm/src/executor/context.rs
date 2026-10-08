//! Contains the context for base block execution.

use alloy_primitives::{B256, Bytes};

/// Context for base block execution.
#[derive(Debug, Default, Clone)]
pub struct BaseBlockExecutionCtx {
    /// Optional ahead-of-builder workers; never installed by production builders in Phase 1.
    #[cfg(feature = "parallel")]
    pub speculator: Option<std::sync::Arc<crate::Speculator>>,
    /// Full payload transactions, present only for explicitly gated payload validation.
    #[cfg(feature = "parallel")]
    pub parallel: Option<crate::ParallelPayload>,
    /// Parent block hash.
    pub parent_hash: B256,
    /// Parent beacon block root.
    pub parent_beacon_block_root: Option<B256>,
    /// The block's extra data.
    pub extra_data: Bytes,
}

impl BaseBlockExecutionCtx {
    /// Creates a sequential execution context, independent of optional executor features.
    pub const fn new(
        parent_hash: B256,
        parent_beacon_block_root: Option<B256>,
        extra_data: Bytes,
    ) -> Self {
        Self {
            parent_hash,
            parent_beacon_block_root,
            extra_data,
            #[cfg(feature = "parallel")]
            parallel: None,
            #[cfg(feature = "parallel")]
            speculator: None,
        }
    }
}
