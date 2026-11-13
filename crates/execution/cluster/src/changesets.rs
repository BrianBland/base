//! reth's persisted account and storage changesets, the only source of an owner's history index.

use std::{
    fmt,
    ops::RangeInclusive,
    sync::{Arc, Weak},
};

use reth_db_api::{
    DatabaseError,
    models::{AccountBeforeTx, BlockNumberAddress},
};
use reth_primitives_traits::StorageEntry;
use reth_provider::{
    ChangeSetReader, DatabaseProviderFactory, ProviderError, StaticFileProviderFactory,
    StaticFileSegment, StorageChangeSetReader,
};

/// The changesets of a range of persisted blocks, as reth stores them: plain keys and the values
/// before each block, in ascending block order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlainChangesets {
    /// Accounts changed by each block with their previous value (`None`: did not exist).
    pub accounts: Vec<(u64, AccountBeforeTx)>,
    /// Storage slots changed by each block with their previous value (zero: absent).
    pub storages: Vec<(BlockNumberAddress, StorageEntry)>,
}

/// Reads reth's persisted changesets for an owner.
pub trait ChangesetSource: Send + Sync {
    /// The blocks whose account and storage changesets are both on disk, or `None` when there
    /// are none (or the node is shutting down).
    fn available(&self) -> Result<Option<RangeInclusive<u64>>, DatabaseError>;

    /// The changesets of `blocks`, which lie inside [`Self::available`].
    fn read(&self, blocks: RangeInclusive<u64>) -> Result<PlainChangesets, DatabaseError>;
}

/// A changeset source installed on a node.
pub type ChangesetReader = Box<dyn ChangesetSource>;

/// Reads changesets through a reth provider factory's read-only database providers, which read
/// only persisted data (on storage v2, from static files).
///
/// The factory is held weakly: the node's provider owns the clustered database, which owns the
/// node and so this reader, and a strong reference would keep all of them alive forever. Once
/// the factory is dropped the reader reads nothing.
pub struct ProviderChangesets<F> {
    factory: Weak<F>,
}

impl<F> fmt::Debug for ProviderChangesets<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderChangesets").finish_non_exhaustive()
    }
}

impl<F> ProviderChangesets<F> {
    /// A reader over `factory`, which must outlive the reads.
    pub fn new(factory: &Arc<F>) -> Self {
        Self { factory: Arc::downgrade(factory) }
    }

    fn failed(error: ProviderError) -> DatabaseError {
        DatabaseError::Other(format!("reading changesets: {error}"))
    }
}

impl<F> ChangesetSource for ProviderChangesets<F>
where
    F: DatabaseProviderFactory,
    F::Provider: ChangeSetReader + StorageChangeSetReader + StaticFileProviderFactory,
{
    fn available(&self) -> Result<Option<RangeInclusive<u64>>, DatabaseError> {
        let Some(factory) = self.factory.upgrade() else { return Ok(None) };
        let files = factory.database_provider_ro().map_err(Self::failed)?.static_file_provider();
        let segments = [StaticFileSegment::AccountChangeSets, StaticFileSegment::StorageChangeSets];
        let lowest = segments.map(|segment| files.get_lowest_range_start(segment));
        let highest = segments.map(|segment| files.get_highest_static_file_block(segment));
        let [Some(account_low), Some(storage_low)] = lowest else { return Ok(None) };
        let [Some(account_high), Some(storage_high)] = highest else { return Ok(None) };
        let blocks = account_low.max(storage_low)..=account_high.min(storage_high);
        Ok((!blocks.is_empty()).then_some(blocks))
    }

    fn read(&self, blocks: RangeInclusive<u64>) -> Result<PlainChangesets, DatabaseError> {
        let Some(factory) = self.factory.upgrade() else { return Ok(PlainChangesets::default()) };
        let provider = factory.database_provider_ro().map_err(Self::failed)?;
        Ok(PlainChangesets {
            accounts: provider.account_changesets_range(blocks.clone()).map_err(Self::failed)?,
            storages: provider.storage_changesets_range(blocks).map_err(Self::failed)?,
        })
    }
}
