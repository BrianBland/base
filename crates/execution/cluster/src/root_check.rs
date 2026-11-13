//! The state-root check: assembling a block's state root from the subtree hashes its owners
//! captured, comparing it with the header, and what happens on a mismatch.

use std::{
    collections::{BTreeSet, HashMap},
    fmt, io,
    path::{Path, PathBuf},
};

use alloy_primitives::{B256, map::B256Map};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::error;

use crate::{
    BlockShardHashes, Cluster, ClusterNode, NodeId, OwnerFailure, RootAssembly, Shard, ShardHash,
};

/// What a node does when an assembled state root differs from the header's.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RootCheckMode {
    /// Log, write the [`RootMismatchMarker`] and exit the process.
    #[default]
    Exit,
    /// Log and count only.
    Log,
    /// Do not check.
    Off,
}

/// The `root_check` and `root_check_warn_blocks` keys of the cluster configuration file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootCheckConfig {
    /// What a mismatch does.
    #[serde(default, rename = "root_check")]
    pub mode: RootCheckMode,
    /// Blocks without a verified state root after which the check warns.
    #[serde(default = "RootCheckConfig::default_warn_blocks", rename = "root_check_warn_blocks")]
    pub warn_blocks: u64,
}

impl RootCheckConfig {
    /// Default [`Self::warn_blocks`]: about ten minutes of Base blocks.
    pub const DEFAULT_WARN_BLOCKS: u64 = 300;

    const fn default_warn_blocks() -> u64 {
        Self::DEFAULT_WARN_BLOCKS
    }
}

impl RootCheckMode {
    /// Logs `mismatch` and, in [`Self::Exit`] mode, writes the [`RootMismatchMarker`] in
    /// `datadir`. Returns whether the process must now exit.
    pub fn on_mismatch(self, mismatch: &RootMismatch, datadir: &Path) -> bool {
        mismatch.log();
        if self != Self::Exit {
            return false;
        }
        match RootMismatchMarker::write(datadir, mismatch) {
            Ok(path) => error!(
                marker = %path.display(),
                "cluster node stopping on a state root mismatch; it will not start until the \
                 marker is removed"
            ),
            Err(error) => error!(
                error = %error,
                "cluster node stopping on a state root mismatch; failed to write the marker"
            ),
        }
        true
    }
}

impl Default for RootCheckConfig {
    fn default() -> Self {
        Self { mode: RootCheckMode::default(), warn_blocks: Self::DEFAULT_WARN_BLOCKS }
    }
}

/// Why one owner did not supply hashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFailure {
    /// The owner has no capture of the block: behind, restarted, evicted, or on another block
    /// at that number.
    NotAvailable,
    /// The owner could not be reached or answered something else.
    Unreachable,
}

impl From<OwnerFailure> for SourceFailure {
    fn from(failure: OwnerFailure) -> Self {
        match failure {
            OwnerFailure::Refused(_) => Self::NotAvailable,
            OwnerFailure::Unreachable(_) | OwnerFailure::UnexpectedResponse => Self::Unreachable,
        }
    }
}

/// Where a verifier gets owners' captured hashes.
pub trait ShardHashSource {
    /// `owner`'s captured subtree hashes of block `number` with hash `hash`.
    fn shard_hashes(
        &self,
        owner: NodeId,
        number: u64,
        hash: B256,
    ) -> Result<BlockShardHashes, SourceFailure>;

    /// `owner`'s hashes, at that block, of its held account shards that contain split contracts,
    /// recomputed with `storage_roots`.
    fn split_shard_hashes(
        &self,
        owner: NodeId,
        number: u64,
        hash: B256,
        storage_roots: &[(B256, B256)],
    ) -> Result<Vec<(Shard, ShardHash)>, SourceFailure>;
}

/// A cluster node's hash source: its own captures for itself, its client for peers.
#[derive(Debug, Clone, Copy)]
pub struct ClusterHashSource<'a> {
    /// The verifying node.
    pub node: &'a ClusterNode,
}

impl ShardHashSource for ClusterHashSource<'_> {
    fn shard_hashes(
        &self,
        owner: NodeId,
        number: u64,
        hash: B256,
    ) -> Result<BlockShardHashes, SourceFailure> {
        if owner == self.node.cluster().node_id() {
            let capture = self.node.root_captures().get(number, hash);
            return capture
                .map(|capture| capture.shard_hashes())
                .ok_or(SourceFailure::NotAvailable);
        }
        Ok(self.node.client().shard_hashes(owner, number, hash)?)
    }

    fn split_shard_hashes(
        &self,
        owner: NodeId,
        number: u64,
        hash: B256,
        storage_roots: &[(B256, B256)],
    ) -> Result<Vec<(Shard, ShardHash)>, SourceFailure> {
        if owner == self.node.cluster().node_id() {
            let capture = self.node.root_captures().get(number, hash);
            let roots = storage_roots.iter().copied().collect();
            return capture
                .map(|capture| capture.split_shard_hashes(&roots))
                .ok_or(SourceFailure::NotAvailable);
        }
        Ok(self.node.client().split_shard_hashes(owner, number, hash, storage_roots)?)
    }
}

/// A canonical block to check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckedBlock {
    /// Block number.
    pub number: u64,
    /// Block hash.
    pub hash: B256,
    /// The header's state root.
    pub state_root: B256,
}

/// Why a block's state root could not be assembled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// An owner answered but has no capture of the block, and no other owner had it.
    NotAvailable,
    /// A required subtree is not a hashed branch node: the trie is too small or oddly shaped.
    Unverifiable,
    /// No owner of a required subtree could be reached.
    Unreachable,
}

/// One subtree value used in an assembled root and the owner that supplied it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardSource {
    /// The split contract whose storage range this is; `None` for an account shard.
    pub contract: Option<B256>,
    /// The shard or range.
    pub shard: Shard,
    /// The owner asked.
    pub owner: NodeId,
    /// Its answer.
    pub value: ShardHash,
}

/// An assembled state root that differs from the header's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootMismatch {
    /// The block.
    pub block: CheckedBlock,
    /// The root assembled from the owners' hashes.
    pub assembled_root: B256,
    /// Every subtree value used, with its owner: split contracts' ranges, then account shards.
    pub sources: Vec<ShardSource>,
}

impl RootMismatch {
    /// Logs the mismatch as an error with every subtree's owner and value.
    pub fn log(&self) {
        error!(
            block = self.block.number,
            hash = %self.block.hash,
            header_root = %self.block.state_root,
            assembled_root = %self.assembled_root,
            shards = %ShardSources(&self.sources),
            "cluster state root mismatch"
        );
    }
}

/// `contract/shard=owner:value` entries, space separated.
#[derive(Debug)]
pub struct ShardSources<'a>(pub &'a [ShardSource]);

impl fmt::Display for ShardSources<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, source) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(" ")?;
            }
            if let Some(contract) = source.contract {
                write!(f, "{contract}/")?;
            }
            write!(f, "{:02x}={}:", source.shard.0, source.owner.0)?;
            match source.value {
                ShardHash::Empty => f.write_str("empty")?,
                ShardHash::Hash(hash) => write!(f, "{hash}")?,
                ShardHash::NotHashedBranch => f.write_str("unhashed")?,
            }
        }
        Ok(())
    }
}

/// The result of one attempt to check a block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    /// The assembled root equals the header's.
    Verified,
    /// The assembled root differs.
    Mismatch(Box<RootMismatch>),
    /// The root could not be assembled.
    Skipped(SkipReason),
}

/// Assembles block state roots from owners' captured hashes: for each split contract its 256
/// range hashes give its storage root; the owners of account shards that contain split
/// contracts recompute those shards with the storage roots; the 256 shard hashes give the state
/// root. One owner per subtree is used: this node first when it owns it, then the others in
/// placement order, moving on when one has no capture.
#[derive(Debug)]
pub struct RootChecker<'a, S> {
    cluster: &'a Cluster,
    source: &'a S,
}

impl<'a, S: ShardHashSource> RootChecker<'a, S> {
    /// A checker of `cluster`'s node asking `source`.
    pub const fn new(cluster: &'a Cluster, source: &'a S) -> Self {
        Self { cluster, source }
    }

    /// Tries once to check `block`.
    pub fn check(&self, block: CheckedBlock) -> CheckOutcome {
        self.assemble(block).unwrap_or_else(CheckOutcome::Skipped)
    }

    fn assemble(&self, block: CheckedBlock) -> Result<CheckOutcome, SkipReason> {
        let split: BTreeSet<B256> = self.cluster.split_storage().iter().copied().collect();
        let split_shards: BTreeSet<Shard> =
            split.iter().map(|contract| Shard(contract[0])).collect();
        let mut sources = Vec::with_capacity(RootAssembly::SUBTREES * (split.len() + 1));
        let mut captured = HashMap::new();
        let fetch = |owner| self.source.shard_hashes(owner, block.number, block.hash);

        let mut storage_roots = B256Map::default();
        for contract in &split {
            let mut ranges = [ShardHash::Empty; RootAssembly::SUBTREES];
            for shard in Shard::all() {
                let find = |hashes: &BlockShardHashes| {
                    let (_, ranges) = hashes.storages.iter().find(|(held, _)| held == contract)?;
                    Self::find(ranges, shard)
                };
                let (owner, value) = self.pick(shard, &mut captured, fetch, find)?;
                ranges[usize::from(shard.0)] = value;
                sources.push(ShardSource { contract: Some(*contract), shard, owner, value });
            }
            let root = RootAssembly::root(&ranges).map_err(|_| SkipReason::Unverifiable)?;
            storage_roots.insert(*contract, root);
        }

        let mut shards = [ShardHash::Empty; RootAssembly::SUBTREES];
        for shard in Shard::all().filter(|shard| !split_shards.contains(shard)) {
            let find = |hashes: &BlockShardHashes| Self::find(&hashes.accounts, shard);
            let (owner, value) = self.pick(shard, &mut captured, fetch, find)?;
            shards[usize::from(shard.0)] = value;
            sources.push(ShardSource { contract: None, shard, owner, value });
        }
        let roots: Vec<_> =
            storage_roots.iter().map(|(contract, root)| (*contract, *root)).collect();
        let mut recomputed = HashMap::new();
        let fetch_split =
            |owner| self.source.split_shard_hashes(owner, block.number, block.hash, &roots);
        for shard in split_shards {
            let find = |hashes: &Vec<(Shard, ShardHash)>| Self::find(hashes, shard);
            let (owner, value) = self.pick(shard, &mut recomputed, fetch_split, find)?;
            shards[usize::from(shard.0)] = value;
            sources.push(ShardSource { contract: None, shard, owner, value });
        }

        let assembled_root = RootAssembly::root(&shards).map_err(|_| SkipReason::Unverifiable)?;
        Ok(if assembled_root == block.state_root {
            CheckOutcome::Verified
        } else {
            CheckOutcome::Mismatch(Box::new(RootMismatch { block, assembled_root, sources }))
        })
    }

    fn find(values: &[(Shard, ShardHash)], shard: Shard) -> Option<ShardHash> {
        values.iter().find(|(held, _)| *held == shard).map(|(_, value)| *value)
    }

    /// The value of `shard` from its first owner, in checking order, whose answer (fetched once
    /// per owner into `answers`) has it.
    fn pick<A>(
        &self,
        shard: Shard,
        answers: &mut HashMap<NodeId, Result<A, SourceFailure>>,
        fetch: impl Fn(NodeId) -> Result<A, SourceFailure>,
        find: impl Fn(&A) -> Option<ShardHash>,
    ) -> Result<(NodeId, ShardHash), SkipReason> {
        let me = self.cluster.node_id();
        let owners = self.cluster.owners(shard);
        let ordered = owners
            .iter()
            .filter(|owner| **owner == me)
            .chain(owners.iter().filter(|owner| **owner != me));
        let mut reached = false;
        for owner in ordered {
            match answers.entry(*owner).or_insert_with(|| fetch(*owner)) {
                Ok(answer) => {
                    reached = true;
                    if let Some(value) = find(answer) {
                        return Ok((*owner, value));
                    }
                }
                Err(SourceFailure::NotAvailable) => reached = true,
                Err(SourceFailure::Unreachable) => {}
            }
        }
        Err(if reached { SkipReason::NotAvailable } else { SkipReason::Unreachable })
    }
}

/// Why a node refuses to start.
#[derive(Debug, Error)]
pub enum MarkerError {
    /// A state-root mismatch was found and the marker has not been removed.
    #[error(
        "a cluster state-root mismatch was recorded in {0}; investigate, then remove the file \
         to start"
    )]
    Present(PathBuf),
    /// The marker could not be checked.
    #[error("failed to check the cluster state-root mismatch marker: {0}")]
    Io(#[from] io::Error),
}

/// The file a node with `root_check = "exit"` writes in its datadir when it finds a mismatch,
/// and refuses to start while it exists, so a supervisor restarting it does not resume on bad
/// state.
#[derive(Debug, Clone, Copy)]
pub struct RootMismatchMarker;

impl RootMismatchMarker {
    /// Name of the marker file in the datadir.
    pub const FILE_NAME: &str = "cluster-root-mismatch";

    /// The marker's path in `datadir`.
    pub fn path(datadir: &Path) -> PathBuf {
        datadir.join(Self::FILE_NAME)
    }

    /// Writes the marker describing `mismatch` in `datadir` and returns its path.
    pub fn write(datadir: &Path, mismatch: &RootMismatch) -> io::Result<PathBuf> {
        let path = Self::path(datadir);
        let contents = format!(
            "block {} {}\nheader state root {}\nassembled state root {}\nshards {}\n",
            mismatch.block.number,
            mismatch.block.hash,
            mismatch.block.state_root,
            mismatch.assembled_root,
            ShardSources(&mismatch.sources)
        );
        std::fs::write(&path, contents)?;
        Ok(path)
    }

    /// Fails when the marker exists in `datadir`.
    pub fn ensure_absent(datadir: &Path) -> Result<(), MarkerError> {
        let path = Self::path(datadir);
        if path.try_exists()? { Err(MarkerError::Present(path)) } else { Ok(()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mismatch() -> RootMismatch {
        RootMismatch {
            block: CheckedBlock {
                number: 7,
                hash: B256::repeat_byte(1),
                state_root: B256::repeat_byte(2),
            },
            assembled_root: B256::repeat_byte(3),
            sources: vec![ShardSource {
                contract: None,
                shard: Shard(0x4f),
                owner: NodeId(2),
                value: ShardHash::Hash(B256::repeat_byte(4)),
            }],
        }
    }

    #[test]
    fn exit_mode_writes_the_marker_and_start_is_refused_until_it_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        assert!(RootMismatchMarker::ensure_absent(dir.path()).is_ok());

        assert!(RootCheckMode::Exit.on_mismatch(&mismatch(), dir.path()));

        let path = RootMismatchMarker::path(dir.path());

        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(contents.contains(&format!("4f=2:{}", B256::repeat_byte(4))), "{contents}");
        assert!(matches!(
            RootMismatchMarker::ensure_absent(dir.path()),
            Err(MarkerError::Present(present)) if present == path
        ));
        std::fs::remove_file(path).unwrap();
        assert!(RootMismatchMarker::ensure_absent(dir.path()).is_ok());
    }

    #[test]
    fn log_mode_only_logs() {
        let dir = tempfile::tempdir().unwrap();

        assert!(!RootCheckMode::Log.on_mismatch(&mismatch(), dir.path()));

        assert!(RootMismatchMarker::ensure_absent(dir.path()).is_ok());
    }

    #[test]
    fn root_check_keys_parse_with_defaults() {
        let parse = |toml: &str| toml::from_str::<RootCheckConfig>(toml).unwrap();
        assert_eq!(parse(""), RootCheckConfig::default());
        assert_eq!(parse("").mode, RootCheckMode::Exit);
        assert_eq!(
            parse("root_check = \"log\"\nroot_check_warn_blocks = 5"),
            RootCheckConfig { mode: RootCheckMode::Log, warn_blocks: 5 }
        );
        assert_eq!(parse("root_check = \"off\"").mode, RootCheckMode::Off);
    }
}
