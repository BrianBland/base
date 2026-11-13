#![doc = include_str!("../README.md")]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod database;
pub use database::ClusteredDatabase;

mod tx;
pub use tx::{ClusteredTx, TxSharding};

mod cursor;
pub use cursor::{ClusteredCursor, CursorSharding};

mod reads;
pub use reads::{CursorPosition, PointRead, ReadMode, RowPlace, ShardedReads};

mod split;
pub use split::SplitCursor;

mod snapshot;
pub use snapshot::ReadSnapshot;

mod local;
pub use local::LocalReads;

mod node;
pub use node::{BlockHashLookup, ClusterNode, ClusterStartError, CommitRefusal};

mod readiness;
pub use readiness::{OwnerState, OwnerStates, OwnersNotReady};

mod stats;
pub use stats::{
    ApplyCounts, ApplyFallback, ApplyStats, OwnerAnswer, OwnerCounts, OwnerStats, RemoteReadCounts,
    RemoteReadKind, RemoteReadPeaks, RemoteReadStats,
};

mod slice;
pub use slice::{SideCall, TrieSlice, TrieSliceError, TrieSlicePlan};

mod slice_stats;
pub use slice_stats::{TrieSliceCounts, TrieSliceStats};

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

mod changesets;
pub use changesets::{ChangesetReader, ChangesetSource, PlainChangesets, ProviderChangesets};

mod history;
pub use history::{HistoryIndex, HistoryKey};

mod owner;
pub use owner::{CanonicalBlock, IndexCoverage, OwnerHistory, OwnerStore, StoreError, ValueSource};

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

mod root_assembly;
pub use root_assembly::{AssemblyError, RootAssembly, ShardHash};

mod root_capture;
pub use root_capture::{
    BlockShardHashes, CaptureReads, CapturedBlock, CapturedHashes, RootCaptures, SplitShardProof,
    TrieHashCapture,
};

mod root_check;
pub use root_check::{
    CheckOutcome, CheckedBlock, ClusterHashSource, MarkerError, RootCheckConfig, RootCheckMode,
    RootChecker, RootMismatch, RootMismatchMarker, ShardHashSource, ShardSource, ShardSources,
    SkipReason, SourceFailure,
};

mod root_check_stats;
pub use root_check_stats::{RootCheckCounts, RootCheckSchedule, RootCheckStats, StepOutcome};
