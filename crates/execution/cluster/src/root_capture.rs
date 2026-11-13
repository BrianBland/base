//! What a storing node keeps of each executed block for the state-root check: the hashes of the
//! subtrees it holds at that block, and the proof nodes needed to recompute the hashes of held
//! account shards that contain split contracts.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use alloy_primitives::{
    B256, Bytes,
    map::{B256Map, HashMap, HashSet},
};
use parking_lot::Mutex;
use reth_trie_common::{
    BranchNodeCompact, HashedPostState, HashedStorage, Nibbles, updates::TrieUpdates,
};
use serde::{Deserialize, Serialize};

use crate::{Cluster, RootAssembly, Shard, ShardHash};

/// One block's captured subtree hashes as an owner sends them to a verifier.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockShardHashes {
    /// Held account shards that contain no split contract, ascending.
    pub accounts: Vec<(Shard, ShardHash)>,
    /// For each split contract, its held storage ranges, ascending.
    pub storages: Vec<(B256, Vec<(Shard, ShardHash)>)>,
}

/// The proof nodes of a held account shard that contains split contracts, pinned at one block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitShardProof {
    /// The split contracts whose account is in the shard.
    pub contracts: Vec<B256>,
    /// The shard's nodes on the paths to those contracts' leaves (paths of two or more nibbles),
    /// with the leaves' fields at the block and don't-care storage roots.
    pub nodes: Vec<(Nibbles, Bytes)>,
}

/// Everything one storing node captured of one executed block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedHashes {
    /// The block.
    pub block: CapturedBlock,
    /// Held account shards that contain no split contract.
    pub accounts: BTreeMap<Shard, ShardHash>,
    /// For each split contract, its held storage ranges.
    pub storages: BTreeMap<B256, BTreeMap<Shard, ShardHash>>,
    /// Held account shards that contain split contracts.
    pub split_shards: BTreeMap<Shard, Arc<SplitShardProof>>,
}

impl CapturedHashes {
    /// The subtree hashes a verifier asks for.
    pub fn shard_hashes(&self) -> BlockShardHashes {
        BlockShardHashes {
            accounts: self.accounts.iter().map(|(shard, hash)| (*shard, *hash)).collect(),
            storages: self
                .storages
                .iter()
                .map(|(contract, ranges)| {
                    (*contract, ranges.iter().map(|(shard, hash)| (*shard, *hash)).collect())
                })
                .collect(),
        }
    }

    /// The hashes of the held account shards that contain split contracts, each recomputed with
    /// the split contracts' `storage_roots`. A shard missing a storage root is left out; a shard
    /// whose proof cannot be re-encoded is [`ShardHash::NotHashedBranch`].
    pub fn split_shard_hashes(&self, storage_roots: &B256Map<B256>) -> Vec<(Shard, ShardHash)> {
        self.split_shards
            .iter()
            .filter(|(_, proof)| {
                proof.contracts.iter().all(|contract| storage_roots.contains_key(contract))
            })
            .map(|(shard, proof)| {
                let hash = RootAssembly::split_shard_hash(
                    *shard,
                    &proof.contracts,
                    &proof.nodes,
                    storage_roots,
                )
                .unwrap_or(ShardHash::NotHashedBranch);
                (*shard, hash)
            })
            .collect()
    }
}

/// A block's number, hash and parent hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapturedBlock {
    /// Block number.
    pub number: u64,
    /// Block hash.
    pub hash: B256,
    /// Hash of the parent block.
    pub parent_hash: B256,
}

/// The captures of the last [`Self::WINDOW_BLOCKS`] executed blocks, oldest first. Lookups are by
/// exact hash, and by (number, hash) for verifiers.
#[derive(Debug, Default)]
pub struct RootCaptures {
    window: Mutex<VecDeque<Arc<CapturedHashes>>>,
}

impl RootCaptures {
    /// Blocks kept: about ten minutes of Base blocks, so a verifier a few blocks behind the head
    /// finds every in-sync owner's capture.
    pub const WINDOW_BLOCKS: usize = 300;

    /// Keeps `capture`, replacing one of the same block, and evicts the oldest beyond the window.
    pub fn insert(&self, capture: CapturedHashes) {
        let mut window = self.window.lock();
        window.retain(|kept| kept.block.hash != capture.block.hash);
        window.push_back(Arc::new(capture));
        while window.len() > Self::WINDOW_BLOCKS {
            window.pop_front();
        }
    }

    /// The capture of block `hash`.
    pub fn by_hash(&self, hash: B256) -> Option<Arc<CapturedHashes>> {
        self.window.lock().iter().rev().find(|kept| kept.block.hash == hash).cloned()
    }

    /// The capture of block `number` with hash `hash`; `None` when not captured, evicted, or
    /// the capture at that number is of another block.
    pub fn get(&self, number: u64, hash: B256) -> Option<Arc<CapturedHashes>> {
        self.by_hash(hash).filter(|capture| capture.block.number == number)
    }
}

/// Reads a capture needs beyond the block's trie updates, all pinned at the block being
/// captured or its parent.
pub trait CaptureReads {
    /// Why a read failed.
    type Error;

    /// The stored account-trie node at `path` as of the parent block.
    fn parent_account_node(
        &mut self,
        path: Nibbles,
    ) -> Result<Option<BranchNodeCompact>, Self::Error>;

    /// The stored node at `path` of `contract`'s storage trie as of the parent block.
    fn parent_storage_node(
        &mut self,
        contract: B256,
        path: Nibbles,
    ) -> Result<Option<BranchNodeCompact>, Self::Error>;

    /// The account-trie proof nodes, as of the block being captured, of the paths to `targets`.
    fn proof_nodes(&mut self, targets: &[B256]) -> Result<Vec<(Nibbles, Bytes)>, Self::Error>;
}

/// Builds each block's [`CapturedHashes`] for one node of a cluster.
#[derive(Debug, Clone)]
pub struct TrieHashCapture {
    /// Held shards, ascending.
    held: Vec<Shard>,
    /// Split contracts, ascending.
    split: Vec<B256>,
    /// Held account shards that contain split contracts, with those contracts.
    split_shards: BTreeMap<Shard, Vec<B256>>,
}

impl TrieHashCapture {
    /// The capture of `cluster`'s node.
    pub fn new(cluster: &Cluster) -> Self {
        let held: Vec<_> = Shard::all().filter(|shard| cluster.owns(*shard)).collect();
        let split: BTreeSet<_> = cluster.split_storage().iter().copied().collect();
        let mut split_shards: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for contract in &split {
            let shard = Shard(contract[0]);
            if cluster.owns(shard) {
                split_shards.entry(shard).or_default().push(*contract);
            }
        }
        Self { held, split: split.into_iter().collect(), split_shards }
    }

    /// Whether this node captures anything: it holds a shard.
    pub const fn captures(&self) -> bool {
        !self.held.is_empty()
    }

    /// The accounts and storages of `state` in held account shards: the post-state a proof of a
    /// held shard needs.
    pub fn held_state(&self, state: &HashedPostState) -> HashedPostState {
        let held = |address: &B256| self.held.binary_search(&Shard(address[0])).is_ok();
        HashedPostState {
            accounts: state
                .accounts
                .iter()
                .filter(|(address, _)| held(address))
                .map(|(address, account)| (*address, *account))
                .collect(),
            storages: state
                .storages
                .iter()
                .filter(|(address, _)| held(address))
                .map(|(address, storage)| (*address, HashedStorage::clone(storage)))
                .collect(),
        }
    }

    /// Captures block `block`, whose hashed post-state is `state` and whose trie updates of this
    /// node's slice are `updates`, into `captures`. Subtree hashes come from the block's
    /// depth-1 nodes where `updates` rewrote or removed them, and otherwise from the parent's
    /// capture; without one, from the parent's stored nodes through `reads`. The proof of a held
    /// shard with split contracts is taken again when the block changed an account in it or the
    /// parent has no capture, and otherwise carried; one multiproof of all such shards' split
    /// contracts is taken per block and its nodes are split by shard.
    pub fn capture<R: CaptureReads>(
        &self,
        captures: &RootCaptures,
        block: CapturedBlock,
        state: &HashedPostState,
        updates: &TrieUpdates,
        reads: &mut R,
    ) -> Result<(), R::Error> {
        if !self.captures() {
            return Ok(());
        }
        let parent = captures.by_hash(block.parent_hash);
        let (mut accounts, mut storages, mut split_shards) = match &parent {
            Some(parent) => {
                (parent.accounts.clone(), parent.storages.clone(), parent.split_shards.clone())
            }
            None => (self.seed_accounts(reads)?, self.seed_storages(reads)?, BTreeMap::new()),
        };

        for high in 0..16u8 {
            let path = Nibbles::from_nibbles([high]);
            if let Some(node) =
                Self::rewritten(&updates.account_nodes, &updates.removed_nodes, &path)
            {
                self.record(&mut accounts, high, node, |shard| {
                    !self.split_shards.contains_key(&shard)
                });
            }
        }
        for contract in &self.split {
            let Some(trie) = updates.storage_tries.get(contract) else { continue };
            let ranges = storages.entry(*contract).or_default();
            for high in 0..16u8 {
                let path = Nibbles::from_nibbles([high]);
                if let Some(node) = Self::rewritten(&trie.storage_nodes, &trie.removed_nodes, &path)
                {
                    self.record(ranges, high, node, |_| true);
                }
            }
        }

        let touched: BTreeSet<_> =
            state.accounts.keys().chain(state.storages.keys()).map(|key| Shard(key[0])).collect();
        let refreshed: Vec<_> = self
            .split_shards
            .iter()
            .filter(|(shard, _)| parent.is_none() || touched.contains(shard))
            .collect();
        if !refreshed.is_empty() {
            let targets: Vec<_> =
                refreshed.iter().flat_map(|(_, contracts)| contracts.iter().copied()).collect();
            let mut by_shard: BTreeMap<Shard, Vec<(Nibbles, Bytes)>> = BTreeMap::new();
            for (path, node) in reads.proof_nodes(&targets)? {
                let (Some(high), Some(low)) = (path.get(0), path.get(1)) else { continue };
                by_shard.entry(Shard((high << 4) | low)).or_default().push((path, node));
            }
            for (shard, contracts) in refreshed {
                let mut nodes = by_shard.remove(shard).unwrap_or_default();
                nodes.sort_unstable_by_key(|(path, _)| *path);
                let proof = SplitShardProof { contracts: contracts.clone(), nodes };
                split_shards.insert(*shard, Arc::new(proof));
            }
        }

        captures.insert(CapturedHashes { block, accounts, storages, split_shards });
        Ok(())
    }

    /// The node at `path` as `updates` leave it: `Some(Some(node))` when rewritten,
    /// `Some(None)` when removed, `None` when untouched.
    fn rewritten<'a>(
        nodes: &'a HashMap<Nibbles, BranchNodeCompact>,
        removed: &HashSet<Nibbles>,
        path: &Nibbles,
    ) -> Option<Option<&'a BranchNodeCompact>> {
        nodes.get(path).map(Some).or_else(|| removed.contains(path).then_some(None))
    }

    /// Records, for every held shard `[high, low]` that `include` accepts, what depth-1 node
    /// `node` (`None`: no stored node) says about child `low`.
    fn record(
        &self,
        values: &mut BTreeMap<Shard, ShardHash>,
        high: u8,
        node: Option<&BranchNodeCompact>,
        include: impl Fn(Shard) -> bool,
    ) {
        for shard in self.held.iter().copied().filter(|shard| shard.0 >> 4 == high) {
            if include(shard) {
                let hash = node.map_or(ShardHash::NotHashedBranch, |node| {
                    ShardHash::of_child(node, shard.0 & 0x0f)
                });
                values.insert(shard, hash);
            }
        }
    }

    fn seed_accounts<R: CaptureReads>(
        &self,
        reads: &mut R,
    ) -> Result<BTreeMap<Shard, ShardHash>, R::Error> {
        let mut accounts = BTreeMap::new();
        for high in 0..16u8 {
            let node = reads.parent_account_node(Nibbles::from_nibbles([high]))?;
            self.record(&mut accounts, high, node.as_ref(), |shard| {
                !self.split_shards.contains_key(&shard)
            });
        }
        Ok(accounts)
    }

    fn seed_storages<R: CaptureReads>(
        &self,
        reads: &mut R,
    ) -> Result<BTreeMap<B256, BTreeMap<Shard, ShardHash>>, R::Error> {
        let mut storages = BTreeMap::new();
        for contract in &self.split {
            let mut ranges = BTreeMap::new();
            for high in 0..16u8 {
                let node = reads.parent_storage_node(*contract, Nibbles::from_nibbles([high]))?;
                self.record(&mut ranges, high, node.as_ref(), |_| true);
            }
            storages.insert(*contract, ranges);
        }
        Ok(storages)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;
    use crate::{ClusterConfig, NodeConfig, NodeId, RootCheckConfig};

    fn hash(number: u64, fork: u8) -> B256 {
        let mut hash = B256::with_last_byte(fork);
        hash.0[..8].copy_from_slice(&number.to_be_bytes());
        hash
    }

    fn block(number: u64) -> CapturedBlock {
        CapturedBlock { number, hash: hash(number, 1), parent_hash: hash(number - 1, 1) }
    }

    fn capture(number: u64) -> CapturedHashes {
        CapturedHashes {
            block: block(number),
            accounts: BTreeMap::new(),
            storages: BTreeMap::new(),
            split_shards: BTreeMap::new(),
        }
    }

    /// Node `node_id` of two storing nodes (`R = 1`) and a non-storing node 3.
    fn cluster(node_id: u64) -> Cluster {
        cluster_splitting(node_id, &[])
    }

    /// Like [`cluster`], splitting the storage of the hashed addresses `split`.
    fn cluster_splitting(node_id: u64, split: &[B256]) -> Cluster {
        let nodes = (1..=3)
            .map(|id| NodeConfig {
                id: NodeId(id),
                address: format!("127.0.0.1:{}", 9300 + id).parse().unwrap(),
                storage: id < 3,
            })
            .collect();
        Cluster::new(ClusterConfig {
            node_id: NodeId(node_id),
            replication: 1,
            retention_blocks: ClusterConfig::MIN_RETENTION_BLOCKS,
            nodes,
            apply_from: Vec::new(),
            split_storage: split
                .iter()
                .map(|hash| Bytes::copy_from_slice(hash.as_slice()))
                .collect(),
            read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
            root_check: RootCheckConfig::default(),
        })
        .unwrap()
    }

    /// Parent nodes that are full branches with child hash `0x11`, counting the reads.
    #[derive(Default)]
    struct CountingReads {
        reads: usize,
    }

    impl CaptureReads for CountingReads {
        type Error = Infallible;

        fn parent_account_node(
            &mut self,
            _path: Nibbles,
        ) -> Result<Option<BranchNodeCompact>, Infallible> {
            self.reads += 1;
            let full = 0xffff_u16;
            Ok(Some(BranchNodeCompact::new(
                full,
                0_u16,
                full,
                vec![B256::repeat_byte(0x11); 16],
                None,
            )))
        }

        fn parent_storage_node(
            &mut self,
            _contract: B256,
            _path: Nibbles,
        ) -> Result<Option<BranchNodeCompact>, Infallible> {
            unreachable!("no split contracts")
        }

        fn proof_nodes(&mut self, _targets: &[B256]) -> Result<Vec<(Nibbles, Bytes)>, Infallible> {
            unreachable!("no split contracts")
        }
    }

    #[test]
    fn the_window_keeps_the_last_blocks_and_answers_exact_blocks_only() {
        let captures = RootCaptures::default();
        let last = RootCaptures::WINDOW_BLOCKS as u64 + 1;
        for number in 1..=last {
            captures.insert(capture(number));
        }

        assert!(captures.get(1, hash(1, 1)).is_none(), "the oldest block is evicted");
        assert!(captures.get(2, hash(2, 1)).is_some());
        assert!(captures.get(last, hash(last, 1)).is_some());
        assert!(captures.get(last, hash(last, 2)).is_none(), "another block at that number");
        assert!(captures.get(last - 1, hash(last, 1)).is_none(), "that block at another number");
    }

    #[test]
    fn the_first_block_seeds_from_its_parent_and_the_next_carries() {
        let node = TrieHashCapture::new(&cluster(1));
        let captures = RootCaptures::default();
        let mut reads = CountingReads::default();
        let empty = (HashedPostState::default(), TrieUpdates::default());

        node.capture(&captures, block(1), &empty.0, &empty.1, &mut reads).unwrap();
        assert_eq!(reads.reads, 16);
        let seeded = captures.get(1, hash(1, 1)).unwrap();
        assert!(!seeded.accounts.is_empty());
        assert!(
            seeded
                .accounts
                .values()
                .all(|value| *value == ShardHash::Hash(B256::repeat_byte(0x11)))
        );

        let mut rewritten = TrieUpdates::default();
        let node_three = BranchNodeCompact::new(0x0001_u16, 0_u16, 0_u16, vec![], None);
        rewritten.account_nodes.insert(Nibbles::from_nibbles([3]), node_three);
        node.capture(&captures, block(2), &empty.0, &rewritten, &mut reads).unwrap();
        assert_eq!(reads.reads, 16, "a block whose parent is captured reads nothing");
        let carried = captures.get(2, hash(2, 1)).unwrap();
        for (shard, value) in &carried.accounts {
            let expected = match shard.0 {
                0x30 => ShardHash::NotHashedBranch,
                0x31..=0x3f => ShardHash::Empty,
                _ => seeded.accounts[shard],
            };
            assert_eq!(*value, expected, "shard {shard:?}");
        }
        assert_eq!(
            carried.accounts.keys().collect::<Vec<_>>(),
            seeded.accounts.keys().collect::<Vec<_>>()
        );
    }

    /// Parent nodes as [`CountingReads`] gives them, and proofs that hold, for each target, its
    /// one- to three-nibble path nodes, counting the proofs taken.
    #[derive(Default)]
    struct ProofReads {
        parents: CountingReads,
        proofs: usize,
    }

    impl ProofReads {
        fn path_node(target: B256, nibbles: usize) -> (Nibbles, Bytes) {
            let path = Nibbles::unpack(target).slice(..nibbles);
            (path, Bytes::from(vec![nibbles as u8, target[0]]))
        }
    }

    impl CaptureReads for ProofReads {
        type Error = Infallible;

        fn parent_account_node(
            &mut self,
            path: Nibbles,
        ) -> Result<Option<BranchNodeCompact>, Infallible> {
            self.parents.parent_account_node(path)
        }

        fn parent_storage_node(
            &mut self,
            _contract: B256,
            path: Nibbles,
        ) -> Result<Option<BranchNodeCompact>, Infallible> {
            self.parents.parent_account_node(path)
        }

        fn proof_nodes(&mut self, targets: &[B256]) -> Result<Vec<(Nibbles, Bytes)>, Infallible> {
            self.proofs += 1;
            Ok(targets
                .iter()
                .flat_map(|target| (1..=3).map(|nibbles| Self::path_node(*target, nibbles)))
                .collect())
        }
    }

    #[test]
    fn one_proof_per_block_covers_every_refreshed_split_shard_and_is_split_by_shard() {
        let held: Vec<_> = Shard::all().filter(|shard| cluster(1).owns(*shard)).take(2).collect();
        let contracts: Vec<_> = held
            .iter()
            .map(|shard| {
                let mut contract = B256::repeat_byte(0x5a);
                contract.0[0] = shard.0;
                contract
            })
            .collect();
        let node = TrieHashCapture::new(&cluster_splitting(1, &contracts));
        let captures = RootCaptures::default();
        let mut reads = ProofReads::default();

        node.capture(
            &captures,
            block(1),
            &HashedPostState::default(),
            &TrieUpdates::default(),
            &mut reads,
        )
        .unwrap();

        assert_eq!(reads.proofs, 1, "one multiproof for both split shards");
        let captured = captures.get(1, hash(1, 1)).unwrap();
        for (shard, contract) in held.iter().zip(&contracts) {
            let proof = &captured.split_shards[shard];
            assert_eq!(proof.contracts, vec![*contract]);
            let expected: Vec<_> =
                (2..=3).map(|nibbles| ProofReads::path_node(*contract, nibbles)).collect();
            assert_eq!(proof.nodes, expected, "shard {shard:?} keeps only its own nodes");
        }

        let mut touched = HashedPostState::default();
        touched.accounts.insert(contracts[0], None);
        node.capture(&captures, block(2), &touched, &TrieUpdates::default(), &mut reads).unwrap();
        assert_eq!(reads.proofs, 2, "a block touching one split shard takes one proof");
        let carried = captures.get(2, hash(2, 1)).unwrap();
        assert_eq!(carried.split_shards[&held[1]], captured.split_shards[&held[1]]);
    }

    #[test]
    fn a_node_storing_nothing_captures_nothing() {
        let node = TrieHashCapture::new(&cluster(3));
        let captures = RootCaptures::default();
        let mut reads = CountingReads::default();

        assert!(!node.captures());
        node.capture(
            &captures,
            block(1),
            &HashedPostState::default(),
            &TrieUpdates::default(),
            &mut reads,
        )
        .unwrap();

        assert!(captures.by_hash(hash(1, 1)).is_none());
        assert_eq!(reads.reads, 0);
    }
}
