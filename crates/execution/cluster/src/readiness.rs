//! Waiting, before a block executes, until owners can serve this node's persisted block.

use std::{collections::BTreeMap, fmt, time::Duration};

use reth_db::DatabaseError;
use thiserror::Error;

use crate::{NodeId, ServableRange};

/// What an owner reported when asked for its servable range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerState {
    /// It can serve state reads as of these blocks.
    Serves(ServableRange),
    /// It can serve nothing yet (its history index is being built, or it stopped serving).
    Nothing,
    /// It could not be reached or did not answer in time.
    Unreachable,
}

/// The state each asked owner reported, by node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnerStates(pub BTreeMap<NodeId, OwnerState>);

impl OwnerStates {
    /// Whether `node` can serve state reads as of `block`.
    pub fn serves(&self, node: NodeId, block: u64) -> bool {
        matches!(
            self.0.get(&node),
            Some(OwnerState::Serves(range)) if range.lowest <= block && block <= range.highest
        )
    }

    /// Whether `node` reported a range whose lowest block is above `block`: it will never serve
    /// `block`.
    pub fn below_history(&self, node: NodeId, block: u64) -> bool {
        matches!(self.0.get(&node), Some(OwnerState::Serves(range)) if range.lowest > block)
    }
}

impl fmt::Display for OwnerStates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, (node, state)) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            match state {
                OwnerState::Serves(range) => {
                    write!(f, "node {}: {}..={}", node.0, range.lowest, range.highest)?
                }
                OwnerState::Nothing => write!(f, "node {}: nothing yet", node.0)?,
                OwnerState::Unreachable => write!(f, "node {}: unreachable", node.0)?,
            }
        }
        Ok(())
    }
}

/// Why a clustered node may not execute a block now. Every case is retryable from the engine's
/// point of view: the block is not invalid.
#[derive(Debug, Error)]
pub enum OwnersNotReady {
    /// Owners of some shards stayed behind, unreachable or not ready for the whole budget.
    #[error(
        "no owner of {shards} shards this node does not hold could serve its persisted block \
         {block} within {waited:?} (owners: {owners}); retry once they catch up"
    )]
    Behind {
        /// This node's persisted block `P`.
        block: u64,
        /// Shards without a ready owner.
        shards: usize,
        /// Time waited.
        waited: Duration,
        /// What each owner last reported.
        owners: OwnerStates,
    },
    /// Every owner of a shard keeps history only above this node's persisted block.
    #[error(
        "persisted block {block} is below the history every owner of shard {shard} keeps \
         (owners: {owners}); this node is more than retention_blocks behind them and must be \
         re-cloned"
    )]
    BelowHistory {
        /// This node's persisted block `P`.
        block: u64,
        /// The shard no owner can serve at `block`.
        shard: u8,
        /// What each owner reported.
        owners: OwnerStates,
    },
    /// This node's persisted block could not be read.
    #[error("reading the persisted block: {0}")]
    Database(#[from] DatabaseError),
}
