//! Trie slices: executing clustered nodes keep only their trie slice, every meaningful row of
//! which equals an unclustered full node's.

use std::collections::{BTreeMap, BTreeSet};

use alloy_eips::eip7685::EMPTY_REQUESTS_HASH;
use alloy_primitives::{B256, Bytes, keccak256};
use alloy_provider::Provider;
use base_common_consensus::Predeploys;
use base_common_network::BaseEngineApi;
use base_common_rpc_types_engine::{BaseExecutionPayloadEnvelopeV4, BaseExecutionPayloadV4};
use base_execution_cluster::{
    CheckOutcome, CheckedBlock, Cluster, ClusterConfig, ClusterHashSource,
    NodeConfig as ClusterNodeConfig, NodeId, RootChecker, Shard, ShardMap,
};
use eyre::{Result, eyre};
use reth_db::{
    Database, DatabaseEnv,
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::DbTx,
};
use reth_trie_common::{BranchNodeCompact, Nibbles};

use crate::{
    STORAGE_CONTRACT, TestNode, apply::block_transactions, chain_spec_with, cluster_config, dump,
    free_address,
};

/// Blocks of the trie-slice test, and its genesis: dense enough that the two-nibble account-trie
/// nodes and [`STORAGE_CONTRACT`]'s two-nibble storage-trie nodes are stored branches.
const SLICE_BLOCKS: u64 = 4;
const SLICE_GENESIS_ACCOUNTS: u64 = 4_096;
const SLICE_CONTRACT_SLOTS: u64 = 4_096;

/// Ids of three storage nodes (`R = 2`) of which the first two are launched: exactly one of them
/// owns the message passer's account shard, and exactly one owns [`STORAGE_CONTRACT`]'s.
fn slice_node_ids() -> [u64; 3] {
    let message_passer = keccak256(Predeploys::L2_TO_L1_MESSAGE_PASSER)[0];
    let contract = keccak256(STORAGE_CONTRACT)[0];
    (3..)
        .map(|third| [1, 2, third])
        .find(|ids| {
            let shards = ShardMap::new(&ids.map(NodeId), 2);
            let one_launched_owner = |shard| {
                shards.owners(Shard(shard)).iter().filter(|id| ids[..2].contains(&id.0)).count()
                    == 1
            };
            one_launched_owner(message_passer) && one_launched_owner(contract)
        })
        .expect("some third node id splits both shards")
}

/// A child of a replicated one-nibble node: the storage trie's contract (`None` for the account
/// trie), the node's path, and the child nibble.
pub(crate) type ChildPosition = (Option<B256>, Nibbles, u8);

/// A child's state and tree mask bits and, when its hash bit is set, its hash.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ChildFlags {
    present: bool,
    stored: bool,
    hash: Option<B256>,
}

/// The trie rows of a node's slice that must equal a full node's, read from raw MDBX.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SliceRows {
    /// Account-trie rows of two or more nibbles in held shards, except the path to
    /// [`STORAGE_CONTRACT`]'s account leaf.
    pub(crate) accounts_trie: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Storage-trie rows of held unsplit accounts, and of held ranges of the split contract.
    pub(crate) storages_trie: BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>,
    /// State, tree and hash bits and hash of each held child of a replicated one-nibble node of
    /// the account trie (except the split contract's account shard) and of the split contract's
    /// storage trie.
    pub(crate) held_children: BTreeMap<ChildPosition, ChildFlags>,
}

impl SliceRows {
    /// The rows `cluster`'s node holds, as `env` stores them.
    pub(crate) fn read(env: &DatabaseEnv, cluster: &Cluster) -> Result<Self> {
        let split = keccak256(STORAGE_CONTRACT);
        let split_leaf = Nibbles::unpack(split);
        let owns = |path: &Nibbles| {
            cluster.owns(Shard(path.get_unchecked(0) << 4 | path.get_unchecked(1)))
        };
        // A packed trie path is 32 bytes of nibbles and then the nibble count.
        let path_of =
            |encoded: &[u8]| Nibbles::unpack(&encoded[..32]).slice(..usize::from(encoded[32]));
        let tx = env.tx()?;
        let (_, accounts) = dump::<tables::PackedAccountsTrie>(&tx)?;
        let accounts_trie = accounts
            .into_iter()
            .filter(|(key, _)| {
                let path = path_of(key);
                path.len() >= 2 && owns(&path) && !split_leaf.starts_with(&path)
            })
            .collect();
        let mut storages_trie: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
        for (address, value) in dump::<tables::PackedStoragesTrie>(&tx)?.1 {
            let held = if address.as_slice() == split.as_slice() {
                let path = path_of(&value[..33]);
                path.len() >= 2 && owns(&path)
            } else {
                cluster.owns(Shard(address[0]))
            };
            if held {
                storages_trie.entry(address).or_default().insert(value);
            }
        }

        let mut held_children = BTreeMap::new();
        let mut record = |owner: Option<B256>, path: Nibbles, node: &BranchNodeCompact| {
            for child in 0..16u8 {
                let mut child_path = path;
                child_path.push(child);
                let on_split_path = owner.is_none() && split_leaf.starts_with(&child_path);
                if owns(&child_path) && !on_split_path {
                    let hashed = node.hash_mask.is_bit_set(child);
                    held_children.insert(
                        (owner, path, child),
                        ChildFlags {
                            present: node.state_mask.is_bit_set(child),
                            stored: node.tree_mask.is_bit_set(child),
                            hash: hashed.then(|| node.hash_for_nibble(child)),
                        },
                    );
                }
            }
        };
        for entry in tx.cursor_read::<tables::PackedAccountsTrie>()?.walk(None)? {
            let (path, node) = entry?;
            if path.0.len() == 1 {
                record(None, path.0, &node);
            }
        }
        for entry in
            tx.cursor_dup_read::<tables::PackedStoragesTrie>()?.walk_dup(Some(split), None)?
        {
            let entry = entry?.1;
            if entry.nibbles.0.len() == 1 {
                record(Some(split), entry.nibbles.0, &entry.node);
            }
        }
        Ok(Self { accounts_trie, storages_trie, held_children })
    }
}

impl TestNode {
    /// Submits `payload` as a new payload without making it the head.
    async fn submit_payload(
        &self,
        payload: BaseExecutionPayloadV4,
        parent_beacon_block_root: B256,
    ) -> Result<alloy_rpc_types_engine::PayloadStatusEnum> {
        Ok(self.engine.new_payload_v4(payload, parent_beacon_block_root).await?.status)
    }
}

/// `envelope`'s payload with the given header roots replaced, resealed under its new hash.
fn resealed(
    envelope: &BaseExecutionPayloadEnvelopeV4,
    state_root: Option<B256>,
    withdrawals_root: Option<B256>,
) -> Result<BaseExecutionPayloadV4> {
    let mut payload = envelope.execution_payload.clone();
    let mut block = payload.clone().into_block_raw()?;
    // Header fields an Engine API V4 payload carries beside the payload.
    block.header.parent_beacon_block_root = Some(envelope.parent_beacon_block_root);
    block.header.requests_hash = Some(EMPTY_REQUESTS_HASH);
    let built = &payload.payload_inner.payload_inner.payload_inner;
    if block.header.hash_slow() != built.block_hash {
        return Err(eyre!("the payload's header does not reseal to its hash"));
    }
    if let Some(root) = state_root {
        block.header.state_root = root;
        payload.payload_inner.payload_inner.payload_inner.state_root = root;
    }
    if let Some(root) = withdrawals_root {
        block.header.withdrawals_root = Some(root);
        payload.withdrawals_root = root;
    }
    payload.payload_inner.payload_inner.payload_inner.block_hash = block.header.hash_slow();
    Ok(payload)
}

/// Executing clustered nodes keep only their trie slice: after the blocks persist, every
/// meaningful row each node holds equals an unclustered full node's. A clustered node accepts a
/// block whose header state root is wrong, which the unclustered node rejects; the
/// message-passer storage root is compared only by the node holding that storage trie and
/// skipped and counted by the other.
#[tokio::test(flavor = "multi_thread")]
async fn clustered_nodes_keep_trie_slices_equal_to_a_full_node() -> Result<()> {
    base_node_runner::test_utils::init_silenced_tracing();
    let chain_spec = chain_spec_with(SLICE_GENESIS_ACCOUNTS, SLICE_CONTRACT_SLOTS, [])?;
    let ids = slice_node_ids();
    let nodes = ids
        .into_iter()
        .map(|id| Ok(ClusterNodeConfig { id: NodeId(id), address: free_address()?, storage: true }))
        .collect::<Result<Vec<_>>>()?;
    let config = |id| ClusterConfig {
        split_storage: vec![Bytes::copy_from_slice(STORAGE_CONTRACT.as_slice())],
        ..cluster_config(id, &nodes)
    };
    let reference = TestNode::launch(&chain_spec, None).await?;
    let first = TestNode::launch(&chain_spec, Some(config(ids[0]))).await?;
    let second = TestNode::launch(&chain_spec, Some(config(ids[1]))).await?;
    let clustered = [&first, &second];
    let at_genesis = clustered
        .iter()
        .map(|node| SliceRows::read(&node.env, node.cluster().cluster()))
        .collect::<Result<Vec<_>>>()?;

    for number in 1..=SLICE_BLOCKS {
        let envelope = reference.produce(block_transactions(number)).await?;
        let hash = reference.head().await?.0;
        for node in clustered {
            assert_eq!(node.import(&envelope).await?, hash);
        }
    }
    for node in [&reference, &first, &second] {
        node.wait_persisted(SLICE_BLOCKS).await?;
    }

    for number in 1..=SLICE_BLOCKS {
        let block = reference
            .reads
            .get_block_by_number(number.into())
            .await?
            .ok_or_else(|| eyre!("no block {number}"))?;
        let checked =
            CheckedBlock { number, hash: block.header.hash, state_root: block.header.state_root };
        for node in clustered {
            let source = ClusterHashSource { node: node.cluster() };
            let outcome = tokio::task::block_in_place(|| {
                RootChecker::new(node.cluster().cluster(), &source).check(checked)
            });
            assert_eq!(outcome, CheckOutcome::Verified, "block {number}, {:?}", node.cluster());
        }
    }

    let message_passer = keccak256(Predeploys::L2_TO_L1_MESSAGE_PASSER);
    let contract = keccak256(STORAGE_CONTRACT);
    for (node, at_genesis) in clustered.into_iter().zip(at_genesis) {
        let cluster = node.cluster().cluster();
        let expected = SliceRows::read(&reference.env, cluster)?;
        assert_eq!(SliceRows::read(&node.env, cluster)?, expected, "node {:?}", cluster.node_id());
        assert!(expected.accounts_trie.len() > 50, "few account-trie rows compared");
        assert!(expected.held_children.len() > 50, "few replicated children compared");
        assert_ne!(expected.accounts_trie, at_genesis.accounts_trie, "no held row changed");
        let counts = node.cluster().trie_slices();
        assert_eq!(counts.blocks, SLICE_BLOCKS);
        if cluster.owns(Shard(contract[0])) {
            assert_eq!(counts.side_calls, 0, "the contract's account holder needs no side call");
        } else {
            assert_eq!(counts.side_calls, SLICE_BLOCKS);
            let key = contract.to_vec();
            assert!(
                expected.storages_trie.get(&key).is_some_and(|rows| !rows.is_empty())
                    && expected.storages_trie.get(&key) != at_genesis.storages_trie.get(&key),
                "held ranges of the split contract did not change"
            );
        }
        let skips = if cluster.owns(Shard(message_passer[0])) { 0 } else { SLICE_BLOCKS };
        assert_eq!(counts.message_passer_skips, skips);
    }

    let envelope = reference.produce(block_transactions(SLICE_BLOCKS + 1)).await?;
    let root = envelope.parent_beacon_block_root;
    let wrong_state_root = resealed(&envelope, Some(B256::repeat_byte(0x5a)), None)?;
    let wrong_withdrawals_root = resealed(&envelope, None, Some(B256::repeat_byte(0x5b)))?;
    assert!(reference.submit_payload(wrong_state_root.clone(), root).await?.is_invalid());
    assert!(reference.submit_payload(wrong_withdrawals_root.clone(), root).await?.is_invalid());
    for node in clustered {
        let status = node.submit_payload(wrong_state_root.clone(), root).await?;
        assert!(status.is_valid(), "{status:?}");
        let holds_message_passer = node.cluster().cluster().owns(Shard(message_passer[0]));
        let status = node.submit_payload(wrong_withdrawals_root.clone(), root).await?;
        assert_eq!(status.is_valid(), !holds_message_passer, "{status:?}");
    }

    for node in [reference, first, second] {
        node.remove().await?;
    }
    Ok(())
}
