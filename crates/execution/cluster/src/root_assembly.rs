//! Assembling a trie root from the hashes of its 256 two-nibble subtrees, and recomputing the
//! hash of an account shard that contains split contracts from captured proof nodes and
//! externally supplied storage roots.

use std::collections::HashMap;

use alloy_primitives::{B256, Bytes, keccak256, map::B256Map};
use alloy_rlp::Decodable;
use alloy_trie::{
    BranchNodeCompact, EMPTY_ROOT_HASH, Nibbles, TrieAccount, TrieMask,
    nodes::{BranchNodeRef, RlpNode, TrieNode},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::Shard;

/// What a depth-1 branch node says about one of its children, the root of a two-nibble subtree
/// (an account shard, or a range of a split contract's storage trie).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ShardHash {
    /// The subtree is empty.
    Empty,
    /// The subtree's root is a branch node with this hash.
    Hash(B256),
    /// The subtree exists but its root is not a branch node with a stored hash (a leaf, an
    /// extension, or a node of unknown shape): the root cannot be assembled from it.
    NotHashedBranch,
}

impl ShardHash {
    /// The hash child `nibble` of stored branch node `node` refers to.
    pub fn of_child(node: &BranchNodeCompact, nibble: u8) -> Self {
        if !node.state_mask.is_bit_set(nibble) {
            Self::Empty
        } else if node.hash_mask.is_bit_set(nibble) {
            Self::Hash(node.hash_for_nibble(nibble))
        } else {
            Self::NotHashedBranch
        }
    }
}

/// Why a root or shard hash cannot be assembled. Every variant means "unverifiable", never
/// "wrong".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AssemblyError {
    /// A required subtree is not a hashed branch node.
    #[error("subtree {0:?} is not a hashed branch node")]
    NotHashedBranch(Shard),
    /// A branch node of the top two levels would have fewer than two children, so the trie is
    /// not shaped as two full levels of branch nodes. `None` is the root.
    #[error("top-level branch node {0:?} would have fewer than two children")]
    CollapsedBranch(Option<u8>),
    /// A captured proof node does not decode, or a leaf of a split contract is malformed.
    #[error("captured proof node does not decode")]
    MalformedProof,
    /// The storage root of a split contract in the shard was not supplied.
    #[error("no storage root supplied for split contract {0}")]
    MissingStorageRoot(B256),
}

/// Hashing of the top of a trie from its two-nibble subtrees, with alloy-trie's node encoding.
#[derive(Debug, Clone, Copy)]
pub struct RootAssembly;

impl RootAssembly {
    /// Number of two-nibble subtrees of a trie.
    pub const SUBTREES: usize = 256;

    /// The root hash of a trie whose subtree under two-nibble prefix `[x, y]` is
    /// `subtrees[x * 16 + y]`: the 16 depth-1 branch nodes, then the root branch node. Serves both
    /// the state root (from account shards) and a split contract's storage root (from its
    /// ranges).
    pub fn root(subtrees: &[ShardHash; Self::SUBTREES]) -> Result<B256, AssemblyError> {
        let mut stack = Vec::with_capacity(16);
        let mut mask = TrieMask::default();
        for (high, group) in subtrees.chunks_exact(16).enumerate() {
            let high = high as u8;
            if let Some(node) = Self::branch(high, group)? {
                stack.push(node);
                mask.set_bit(high);
            }
        }
        match mask.count_bits() {
            0 => Ok(EMPTY_ROOT_HASH),
            1 => Err(AssemblyError::CollapsedBranch(None)),
            _ => {
                let mut rlp = Vec::new();
                BranchNodeRef::new(&stack, mask).rlp(&mut rlp);
                Ok(keccak256(&rlp))
            }
        }
    }

    /// The reference to depth-1 branch node `[high]` with children `group`, or `None` when it has
    /// none.
    fn branch(high: u8, group: &[ShardHash]) -> Result<Option<RlpNode>, AssemblyError> {
        let mut stack = Vec::with_capacity(16);
        let mut mask = TrieMask::default();
        for (low, child) in group.iter().enumerate() {
            let low = low as u8;
            match child {
                ShardHash::Empty => {}
                ShardHash::Hash(hash) => {
                    stack.push(RlpNode::word_rlp(hash));
                    mask.set_bit(low);
                }
                ShardHash::NotHashedBranch => {
                    return Err(AssemblyError::NotHashedBranch(Shard(high << 4 | low)));
                }
            }
        }
        match mask.count_bits() {
            0 => Ok(None),
            1 => Err(AssemblyError::CollapsedBranch(Some(high))),
            _ => Ok(Some(BranchNodeRef::new(&stack, mask).rlp(&mut Vec::new()))),
        }
    }

    /// The hash of account shard `shard` from `proof_nodes`, the nodes of the shard's subtree on
    /// the paths to its split contracts `contracts` (paths of two or more nibbles, all pinned at
    /// one block), with each split contract's leaf re-encoded with its storage root from
    /// `storage_roots`, which must cover all of them. Nodes are re-encoded bottom-up, each
    /// replacing the references to its re-encoded children, so nodes shared by several split
    /// contracts are rebuilt once with all of them.
    pub fn split_shard_hash(
        shard: Shard,
        contracts: &[B256],
        proof_nodes: &[(Nibbles, Bytes)],
        storage_roots: &B256Map<B256>,
    ) -> Result<ShardHash, AssemblyError> {
        if let Some(missing) =
            contracts.iter().find(|contract| !storage_roots.contains_key(*contract))
        {
            return Err(AssemblyError::MissingStorageRoot(*missing));
        }
        let shard_path = Nibbles::from_nibbles([shard.0 >> 4, shard.0 & 0x0f]);
        let mut nodes = proof_nodes
            .iter()
            .filter(|(path, _)| path.starts_with(&shard_path))
            .map(|(path, rlp)| {
                TrieNode::decode(&mut rlp.as_ref())
                    .map(|node| (*path, node))
                    .map_err(|_| AssemblyError::MalformedProof)
            })
            .collect::<Result<Vec<_>, _>>()?;
        nodes.sort_unstable_by_key(|(path, _)| std::cmp::Reverse(path.len()));

        let mut rebuilt: HashMap<Nibbles, RlpNode> = HashMap::with_capacity(nodes.len());
        let mut shard_root = None;
        for (path, mut node) in nodes {
            match &mut node {
                TrieNode::Leaf(leaf) => {
                    let key = path.join(&leaf.key);
                    if key.len() == 2 * B256::len_bytes() {
                        let address = B256::from_slice(&key.pack());
                        if let Some(root) = storage_roots.get(&address)
                            && contracts.contains(&address)
                        {
                            let mut account = TrieAccount::decode(&mut leaf.value.as_slice())
                                .map_err(|_| AssemblyError::MalformedProof)?;
                            account.storage_root = *root;
                            leaf.value = alloy_rlp::encode(account);
                        }
                    }
                }
                TrieNode::Branch(branch) => {
                    let mut index = 0;
                    for nibble in 0..16u8 {
                        if !branch.state_mask.is_bit_set(nibble) {
                            continue;
                        }
                        let mut child = path;
                        child.push(nibble);
                        if let Some(reference) = rebuilt.remove(&child) {
                            branch.stack[index] = reference;
                        }
                        index += 1;
                    }
                }
                TrieNode::Extension(extension) => {
                    if let Some(reference) = rebuilt.remove(&path.join(&extension.key)) {
                        extension.child = reference;
                    }
                }
                TrieNode::EmptyRoot => return Err(AssemblyError::MalformedProof),
            }
            let reference = node.rlp(&mut Vec::new());
            if path == shard_path {
                shard_root = Some((matches!(node, TrieNode::Branch(_)), reference));
            } else {
                rebuilt.insert(path, reference);
            }
        }
        Ok(match shard_root {
            Some((true, reference)) => {
                reference.as_hash().map_or(ShardHash::NotHashedBranch, ShardHash::Hash)
            }
            _ => ShardHash::NotHashedBranch,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use alloy_primitives::U256;
    use alloy_trie::{HashBuilder, proof::ProofRetainer};
    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::*;

    /// Enough leaves that every two-nibble subtree is a branch node.
    const DENSE: usize = 5_000;

    /// A trie built by alloy-trie's hash builder, the one reth computes roots with.
    struct Built {
        root: B256,
        nodes: alloy_primitives::map::HashMap<Nibbles, BranchNodeCompact>,
        proof: Vec<(Nibbles, Bytes)>,
    }

    fn build(leaves: &BTreeMap<B256, Vec<u8>>, targets: &[B256]) -> Built {
        let retainer: ProofRetainer = targets.iter().map(Nibbles::unpack).collect();
        let mut builder = HashBuilder::default().with_updates(true).with_proof_retainer(retainer);
        for (key, value) in leaves {
            builder.add_leaf(Nibbles::unpack(key), value);
        }
        let root = builder.root();
        let (mut builder, nodes) = builder.split();
        let proof = builder.take_proof_nodes().into_inner().into_iter().collect();
        Built { root, nodes, proof }
    }

    /// The subtree values a node would capture from the trie's depth-1 nodes.
    fn subtrees(built: &Built) -> [ShardHash; RootAssembly::SUBTREES] {
        std::array::from_fn(|index| {
            let (high, low) = ((index >> 4) as u8, (index & 0x0f) as u8);
            built
                .nodes
                .get(&Nibbles::from_nibbles([high]))
                .map_or(ShardHash::NotHashedBranch, |node| ShardHash::of_child(node, low))
        })
    }

    fn key(rng: &mut StdRng, first_byte: Option<u8>) -> B256 {
        let mut key = B256::from(rng.random::<[u8; 32]>());
        if let Some(byte) = first_byte {
            key.0[0] = byte;
        }
        key
    }

    fn account(rng: &mut StdRng, storage_root: B256) -> Vec<u8> {
        alloy_rlp::encode(TrieAccount {
            nonce: rng.random_range(0..1_000),
            balance: U256::from(rng.random::<u64>()),
            storage_root,
            code_hash: keccak256(rng.random::<[u8; 8]>()),
        })
    }

    fn storage(rng: &mut StdRng, slots: usize) -> BTreeMap<B256, Vec<u8>> {
        (0..slots)
            .map(|_| (key(rng, None), alloy_rlp::encode(U256::from(rng.random_range(1..u64::MAX)))))
            .collect()
    }

    fn accounts(rng: &mut StdRng, count: usize, skip_shard: Option<u8>) -> BTreeMap<B256, Vec<u8>> {
        let mut leaves = BTreeMap::new();
        while leaves.len() < count {
            let address = key(rng, None);
            if Some(address[0]) != skip_shard {
                leaves.insert(address, account(rng, EMPTY_ROOT_HASH));
            }
        }
        leaves
    }

    #[test]
    fn storage_root_from_range_hashes_equals_the_full_storage_root() {
        let mut rng = StdRng::seed_from_u64(1);
        let built = build(&storage(&mut rng, DENSE), &[]);
        let ranges = subtrees(&built);
        assert!(ranges.iter().all(|range| matches!(range, ShardHash::Hash(_))));

        assert_eq!(RootAssembly::root(&ranges), Ok(built.root));
    }

    #[test]
    fn state_root_from_shard_hashes_equals_the_full_state_root() {
        let mut rng = StdRng::seed_from_u64(2);
        let built = build(&accounts(&mut rng, DENSE, None), &[]);

        assert_eq!(RootAssembly::root(&subtrees(&built)), Ok(built.root));
    }

    #[test]
    fn an_empty_shard_encodes_as_an_empty_child() {
        let mut rng = StdRng::seed_from_u64(3);
        let built = build(&accounts(&mut rng, DENSE, Some(0x37)), &[]);
        let shards = subtrees(&built);
        assert_eq!(shards[0x37], ShardHash::Empty);

        assert_eq!(RootAssembly::root(&shards), Ok(built.root));
    }

    #[test]
    fn a_tiny_trie_is_unverifiable_not_wrong() {
        let mut rng = StdRng::seed_from_u64(4);
        for count in [3, 40, 600] {
            let built = build(&accounts(&mut rng, count, None), &[]);

            let assembled = RootAssembly::root(&subtrees(&built));

            assert!(
                matches!(
                    assembled,
                    Err(AssemblyError::NotHashedBranch(_) | AssemblyError::CollapsedBranch(_))
                ),
                "{count} accounts: {assembled:?}"
            );
        }
    }

    #[test]
    fn a_depth_one_node_with_one_child_is_unverifiable() {
        let mut shards = [ShardHash::Hash(B256::repeat_byte(1)); RootAssembly::SUBTREES];
        shards[0x40..0x50].fill(ShardHash::Empty);
        shards[0x45] = ShardHash::Hash(B256::repeat_byte(2));

        assert_eq!(RootAssembly::root(&shards), Err(AssemblyError::CollapsedBranch(Some(4))));
    }

    /// Two split contracts in shard `0x5a` and one in `0xc3`, in a dense account trie. The nodes
    /// hold don't-care storage roots in the split contracts' leaves; the real trie holds their
    /// real storage roots.
    #[test]
    fn split_shard_hash_from_proof_nodes_and_supplied_storage_roots_equals_the_real_shard_hash() {
        let mut rng = StdRng::seed_from_u64(5);
        let split =
            [key(&mut rng, Some(0x5a)), key(&mut rng, Some(0x5a)), key(&mut rng, Some(0xc3))];
        let mut real = accounts(&mut rng, DENSE, None);
        let mut stale = real.clone();
        let mut storage_roots = B256Map::default();
        for contract in split {
            let storage_root = build(&storage(&mut rng, 300), &[]).root;
            let fields = account(&mut rng, storage_root);
            let mut decoded = TrieAccount::decode(&mut fields.as_slice()).unwrap();
            real.insert(contract, fields);
            decoded.storage_root = B256::repeat_byte(0xee);
            stale.insert(contract, alloy_rlp::encode(decoded));
            storage_roots.insert(contract, storage_root);
        }
        let real = build(&real, &[]);
        let stale = build(&stale, &split);
        let deep: Vec<_> = stale.proof.into_iter().filter(|(path, _)| path.len() >= 2).collect();

        for shard in [Shard(0x5a), Shard(0xc3)] {
            let expected = ShardHash::of_child(
                &real.nodes[&Nibbles::from_nibbles([shard.0 >> 4])],
                shard.0 & 0x0f,
            );
            assert!(matches!(expected, ShardHash::Hash(_)));
            let stale_hash = ShardHash::of_child(
                &stale.nodes[&Nibbles::from_nibbles([shard.0 >> 4])],
                shard.0 & 0x0f,
            );
            assert_ne!(stale_hash, expected, "the don't-care root must change the shard hash");

            let contracts: Vec<_> =
                split.iter().filter(|contract| contract[0] == shard.0).copied().collect();
            let recomputed =
                RootAssembly::split_shard_hash(shard, &contracts, &deep, &storage_roots);

            assert_eq!(recomputed, Ok(expected), "shard {shard:?}");
        }

        let mut one_missing = storage_roots.clone();
        one_missing.remove(&split[1]);
        assert_eq!(
            RootAssembly::split_shard_hash(Shard(0x5a), &split[..2], &deep, &one_missing),
            Err(AssemblyError::MissingStorageRoot(split[1]))
        );
        let mut one_wrong = storage_roots;
        one_wrong.insert(split[1], B256::repeat_byte(0xab));
        assert_ne!(
            RootAssembly::split_shard_hash(Shard(0x5a), &split[..2], &deep, &one_wrong),
            Ok(ShardHash::of_child(&real.nodes[&Nibbles::from_nibbles([5])], 0x0a)),
            "the second split contract of the shard contributes"
        );
    }

    #[test]
    fn a_shard_without_proof_nodes_is_not_a_hashed_branch() {
        assert_eq!(
            RootAssembly::split_shard_hash(Shard(1), &[], &[], &B256Map::default()),
            Ok(ShardHash::NotHashedBranch)
        );
    }
}
