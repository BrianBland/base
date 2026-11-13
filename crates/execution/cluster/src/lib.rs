#![doc = include_str!("../README.md")]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod database;
pub use database::ClusteredDatabase;

mod tx;
pub use tx::{ClusteredTx, TxSharding};

mod cursor;
pub use cursor::{ClusteredCursor, CursorSharding};

mod reads;
pub use reads::{CursorPosition, FoundEntry, ShardedReads};

mod split;
pub use split::SplitCursor;

mod snapshot;
pub use snapshot::ReadSnapshot;

mod node;
pub use node::{BlockHashLookup, ClusterNode, ClusterStartError, CommitRefusal};

mod stats;
pub use stats::{
    ApplyCounts, ApplyFallback, ApplyStats, RemoteReadCounts, RemoteReadKind, RemoteReadStats,
};

mod placement;
pub use placement::{KeyPlacement, NodeId, Shard, ShardMap, ShardedTable};

mod config;
pub use config::{Cluster, ClusterConfig, ConfigError, NodeConfig};

mod bootstrap;
pub use bootstrap::{
    BatchLimit, Bootstrap, BootstrapError, BootstrapReport, BootstrapTarget, RowBatch, RowCount,
    TableCopy,
};

mod partition;
pub use partition::{
    ClaimedPartition, OrderedPartitions, PartitionClaims, PartitionConsumer, PartitionRows,
    PendingWork, RawRow, ReadParallelism, TablePartitions, TablePosition, TableRange, WorkItem,
    WorkQueue,
};

mod storage_report;
pub use storage_report::{ContractRows, CountRange, StorageCounter, StorageReport, StorageScan};

mod protocol;
pub use protocol::{
    DuplicateValues, Frame, FrameError, Query, ReadError, ReadResult, Request, Response,
    ServableRange, ShardEntries, SnapshotId,
};

mod owner;
pub use owner::{
    BlockUpdates, CachedOverlay, CanonicalBlock, OwnerStore, PersistencePoint, ResolvedSnapshot,
    RetainedChain, SnapshotView, StateOverlay, StoreError,
};

mod executed;
pub use executed::{
    ExecutedBlockParts, ExecutedBlockResult, ExecutedBlocks, RecentResults, ResultCapture,
};

mod server;
pub use server::OwnerServer;

mod client;
pub use client::{
    ClientError, ClientOptions, ClusterClient, DeadlineStream, OwnerAttempt, OwnerFailure,
};
