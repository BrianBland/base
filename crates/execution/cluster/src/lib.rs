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

mod snapshot;
pub use snapshot::ReadSnapshot;

mod node;
pub use node::{BlockHashLookup, ClusterNode, ClusterStartError, CommitRefusal};

mod placement;
pub use placement::{KeyPlacement, NodeId, Shard, ShardMap, ShardedTable};

mod config;
pub use config::{Cluster, ClusterConfig, ConfigError, NodeConfig};

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

mod server;
pub use server::OwnerServer;

mod client;
pub use client::{ClientError, ClientOptions, ClusterClient, OwnerAttempt, OwnerFailure};
