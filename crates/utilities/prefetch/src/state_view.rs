use alloy_eips::BlockNumberOrTag;
use alloy_primitives::B256;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{StateProviderBox, StateProviderFactory, errors::provider::ProviderResult};

/// Consistent state-view selector for prefetch execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchStateViewId {
    /// Latest canonical state.
    Latest,
    /// Pending state if available, otherwise latest.
    Pending,
    /// Historical or pending state by block hash, matching `StateProviderFactory`.
    BlockHash(B256),
    /// Historical canonical state by block number or tag.
    BlockNumberOrTag(BlockNumberOrTag),
}

/// Thin wrapper that builds prefetch state views from an existing `StateProviderFactory`.
#[derive(Debug)]
pub struct PrefetchStateViewFactory<Factory> {
    factory: Factory,
}

impl<Factory> PrefetchStateViewFactory<Factory> {
    /// Creates a new view factory from an existing provider factory.
    pub const fn new(factory: Factory) -> Self {
        Self { factory }
    }

    /// Returns the wrapped provider factory.
    pub const fn factory(&self) -> &Factory {
        &self.factory
    }
}

impl<Factory> PrefetchStateViewFactory<Factory>
where
    Factory: StateProviderFactory,
{
    /// Builds a consistent state provider for the requested view.
    pub fn state_provider(&self, view: PrefetchStateViewId) -> ProviderResult<StateProviderBox> {
        match view {
            PrefetchStateViewId::Latest => self.factory.latest(),
            PrefetchStateViewId::Pending => self.factory.pending(),
            PrefetchStateViewId::BlockHash(block_hash) => {
                self.factory.state_by_block_hash(block_hash)
            }
            PrefetchStateViewId::BlockNumberOrTag(block_number_or_tag) => {
                self.factory.state_by_block_number_or_tag(block_number_or_tag)
            }
        }
    }

    /// Builds a REVM-compatible database wrapper for the requested view.
    pub fn database(
        &self,
        view: PrefetchStateViewId,
    ) -> ProviderResult<StateProviderDatabase<StateProviderBox>> {
        Ok(StateProviderDatabase::new(self.state_provider(view)?))
    }
}
