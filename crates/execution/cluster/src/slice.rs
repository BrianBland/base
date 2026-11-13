//! Planning the trie computations that keep this node's slice of the state trie up to date.

use std::sync::Arc;

use alloy_primitives::{B256, map::B256Map};
use reth_primitives_traits::Account;
use reth_trie_common::{
    HashedPostState, HashedStorage, Nibbles, TrieInput,
    prefix_set::{PrefixSetMut, TriePrefixSetsMut},
    updates::TrieUpdates,
};
use thiserror::Error;

use crate::{Cluster, Shard};

/// Why a block's trie slice cannot be planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TrieSliceError {
    /// A split contract's account became empty, wiping its storage: the ranges other nodes hold
    /// cannot be deleted from here.
    #[error("the storage of split contract {0} is wiped")]
    WipedSplitStorage(B256),
}

/// One computation of a side contract: a split contract whose account shard this node does not
/// hold, but which changed slots in ranges this node holds.
#[derive(Debug, Clone)]
pub struct SideCall {
    /// Hashed address of the split contract.
    pub contract: B256,
    /// The contract with a placeholder account and its held changed slots, with matching prefix
    /// sets.
    pub input: TrieInput,
}

/// The state-root computations that maintain this node's trie slice for one block.
#[derive(Debug, Clone, Default)]
pub struct TrieSlicePlan {
    /// The held changes and their prefix sets; `None` when this node holds no changed key outside
    /// side contracts.
    pub main: Option<TrieInput>,
    /// One computation per side contract.
    pub side_calls: Vec<SideCall>,
}

impl TrieSlicePlan {
    /// Whether nothing needs computing.
    pub const fn is_empty(&self) -> bool {
        self.main.is_none() && self.side_calls.is_empty()
    }

    /// Runs the computations with `compute` (reth's state root with trie updates over this
    /// node's rows) and merges their trie updates: everything the main computation outputs, and
    /// from each side call only its contract's storage-trie updates, which replace any the main
    /// computation produced for that contract.
    pub fn compute<E>(
        self,
        mut compute: impl FnMut(TrieInput) -> Result<TrieUpdates, E>,
    ) -> Result<TrieUpdates, E> {
        let mut merged = match self.main {
            Some(input) => compute(input)?,
            None => TrieUpdates::default(),
        };
        for side in self.side_calls {
            let mut updates = compute(side.input)?;
            if let Some(storage) = updates.storage_tries.remove(&side.contract) {
                merged.storage_tries.insert(side.contract, storage);
            }
        }
        Ok(merged)
    }
}

/// Plans this node's trie slice: which account and storage prefixes each block's computations
/// cover, from the cluster's placement. Never derived with reth's `construct_prefix_sets`, which
/// covers foreign shards and misses split contracts whose account is foreign.
#[derive(Debug, Clone)]
pub struct TrieSlice {
    cluster: Arc<Cluster>,
    stores: bool,
}

impl TrieSlice {
    /// Plans for this node of `cluster`.
    pub fn new(cluster: Arc<Cluster>) -> Self {
        let stores = Shard::all().any(|shard| cluster.owns(shard));
        Self { cluster, stores }
    }

    /// Whether this node holds any shard; a node that holds none never computes anything.
    pub const fn stores(&self) -> bool {
        self.stores
    }

    /// Whether this node holds every row of `hashed_address`'s storage trie: it owns the account's
    /// shard and the contract's storage is not split.
    pub fn holds_storage_trie(&self, hashed_address: &B256) -> bool {
        self.holds_account(hashed_address) && !self.cluster.split_storage().contains(hashed_address)
    }

    /// The computations of a block whose hashed post-state is `state`.
    pub fn plan(&self, state: &HashedPostState) -> Result<TrieSlicePlan, TrieSliceError> {
        if !self.stores {
            return Ok(TrieSlicePlan::default());
        }
        if let Some(contract) = self
            .cluster
            .split_storage()
            .iter()
            .find(|contract| matches!(state.accounts.get(*contract), Some(None)))
        {
            return Err(TrieSliceError::WipedSplitStorage(*contract));
        }

        let mut main_state = HashedPostState::default();
        let mut prefix_sets = TriePrefixSetsMut::default();
        let mut side_calls = Vec::new();
        for (address, account) in &state.accounts {
            if self.holds_account(address) {
                main_state.accounts.insert(*address, *account);
                prefix_sets.account_prefix_set.insert(Nibbles::unpack(address));
                if account.is_none() {
                    prefix_sets.destroyed_accounts.insert(*address);
                }
            }
        }
        for (address, storage) in &state.storages {
            let held = self.held_slots(address, storage);
            if held.is_empty() {
                continue;
            }
            if self.holds_account(address) {
                prefix_sets.account_prefix_set.insert(Nibbles::unpack(address));
                prefix_sets.storage_prefix_sets.insert(*address, Self::slot_prefixes(&held));
                main_state.storages.insert(*address, held);
            } else {
                side_calls.push(Self::side_call(*address, held));
            }
        }
        side_calls.sort_unstable_by_key(|side| side.contract);

        let main = (!prefix_sets.is_empty())
            .then(|| TrieInput::new(TrieUpdates::default(), main_state, prefix_sets));
        Ok(TrieSlicePlan { main, side_calls })
    }

    fn holds_account(&self, hashed_address: &B256) -> bool {
        self.cluster.owns(Shard(hashed_address[0]))
    }

    /// The changed slots of `address` in rows this node holds: all of them when it holds the
    /// account and its storage is not split, those in held ranges for a split contract, and none
    /// otherwise.
    fn held_slots(&self, address: &B256, storage: &HashedStorage) -> HashedStorage {
        if !self.cluster.split_storage().contains(address) {
            return if self.holds_account(address) {
                storage.clone()
            } else {
                HashedStorage::default()
            };
        }
        let storage = storage
            .storage
            .iter()
            .filter(|(slot, _)| self.cluster.owns(Shard(slot[0])))
            .map(|(slot, value)| (*slot, *value))
            .collect();
        HashedStorage { storage }
    }

    fn slot_prefixes(storage: &HashedStorage) -> PrefixSetMut {
        let mut prefixes = PrefixSetMut::with_capacity(storage.storage.len());
        prefixes.extend_keys(storage.storage.keys().map(Nibbles::unpack));
        prefixes
    }

    /// The side call of split contract `contract` with held changed slots `held`. The
    /// placeholder account only makes the walk reach the contract's leaf; the account nodes it
    /// computes are discarded.
    fn side_call(contract: B256, held: HashedStorage) -> SideCall {
        let mut prefix_sets = TriePrefixSetsMut::default();
        prefix_sets.account_prefix_set.insert(Nibbles::unpack(contract));
        prefix_sets.storage_prefix_sets.insert(contract, Self::slot_prefixes(&held));
        let state = HashedPostState {
            accounts: B256Map::from_iter([(contract, Some(Account::default()))]),
            storages: B256Map::from_iter([(contract, held)]),
        };
        SideCall { contract, input: TrieInput::new(TrieUpdates::default(), state, prefix_sets) }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Bytes, U256};
    use reth_trie_common::{BranchNodeCompact, updates::StorageTrieUpdates};

    use super::*;
    use crate::{ClusterConfig, NodeConfig, NodeId};

    /// Node `node_id` of four storing nodes (`R = 2`) and a stateless node 5, splitting `split`.
    fn cluster(node_id: u64, split: &[B256]) -> Arc<Cluster> {
        let nodes = (1..=5)
            .map(|id| NodeConfig {
                id: NodeId(id),
                address: format!("127.0.0.1:{}", 9000 + id).parse().unwrap(),
                storage: id < 5,
            })
            .collect();
        Arc::new(
            Cluster::new(ClusterConfig {
                node_id: NodeId(node_id),
                replication: 2,
                retention_blocks: ClusterConfig::MIN_RETENTION_BLOCKS,
                nodes,
                apply_from: Vec::new(),
                split_storage: split
                    .iter()
                    .map(|hash| Bytes::copy_from_slice(hash.as_slice()))
                    .collect(),
                read_retry_secs: ClusterConfig::DEFAULT_READ_RETRY_SECS,
                simulated_round_trip_micros: 0,
                root_check: Default::default(),
            })
            .unwrap(),
        )
    }

    /// The `index`th key whose first byte is a shard node 1 owns (`held`) or does not.
    fn key(held: bool, index: u8) -> B256 {
        let view = cluster(1, &[]);
        let shard =
            Shard::all().filter(|shard| view.owns(*shard) == held).nth(usize::from(index)).unwrap();
        let mut key = B256::repeat_byte(index.wrapping_add(0x11));
        key.0[0] = shard.0;
        key
    }

    fn account(nonce: u64) -> Option<Account> {
        Some(Account { nonce, ..Default::default() })
    }

    fn storage(slots: &[B256]) -> HashedStorage {
        HashedStorage { storage: slots.iter().map(|slot| (*slot, U256::from(7))).collect() }
    }

    fn prefixes(keys: &[B256]) -> Vec<Nibbles> {
        let mut prefixes: Vec<_> = keys.iter().map(Nibbles::unpack).collect();
        prefixes.sort_unstable();
        prefixes
    }

    fn sorted(set: &PrefixSetMut) -> Vec<Nibbles> {
        let mut prefixes: Vec<_> = set.iter().copied().collect();
        prefixes.sort_unstable();
        prefixes.dedup();
        prefixes
    }

    fn main(plan: &TrieSlicePlan) -> &TrieInput {
        plan.main.as_ref().expect("a main computation")
    }

    #[test]
    fn main_covers_changed_held_accounts_only() {
        let (held, foreign) = (key(true, 0), key(false, 0));
        let state = HashedPostState {
            accounts: B256Map::from_iter([(held, account(1)), (foreign, account(2))]),
            storages: B256Map::default(),
        };

        let plan = TrieSlice::new(cluster(1, &[])).plan(&state).unwrap();

        let main = main(&plan);
        assert_eq!(sorted(&main.prefix_sets.account_prefix_set), prefixes(&[held]));
        assert!(main.prefix_sets.storage_prefix_sets.is_empty());
        assert!(main.prefix_sets.destroyed_accounts.is_empty());
        assert_eq!(main.state.accounts, B256Map::from_iter([(held, account(1))]));
        assert!(plan.side_calls.is_empty());
    }

    #[test]
    fn storage_only_change_of_a_held_account_covers_the_account_and_all_its_slots() {
        let contract = key(true, 1);
        let slots = [key(true, 2), key(false, 2), key(false, 3)];
        let state = HashedPostState {
            accounts: B256Map::default(),
            storages: B256Map::from_iter([(contract, storage(&slots))]),
        };

        let plan = TrieSlice::new(cluster(1, &[])).plan(&state).unwrap();

        let main = main(&plan);
        assert_eq!(sorted(&main.prefix_sets.account_prefix_set), prefixes(&[contract]));
        assert_eq!(sorted(&main.prefix_sets.storage_prefix_sets[&contract]), prefixes(&slots));
        assert_eq!(main.state.storages[&contract], storage(&slots));
        assert!(main.state.accounts.is_empty());
    }

    #[test]
    fn destroyed_held_accounts_are_destroyed_and_foreign_ones_ignored() {
        let (held, foreign) = (key(true, 3), key(false, 4));
        let slot = key(true, 5);
        let zeroed = HashedStorage { storage: B256Map::from_iter([(slot, U256::ZERO)]) };
        let state = HashedPostState {
            accounts: B256Map::from_iter([(held, None), (foreign, None)]),
            storages: B256Map::from_iter([(held, zeroed.clone()), (foreign, zeroed.clone())]),
        };

        let plan = TrieSlice::new(cluster(1, &[])).plan(&state).unwrap();

        let main = main(&plan);
        assert_eq!(main.prefix_sets.destroyed_accounts, [held].into_iter().collect());
        assert_eq!(sorted(&main.prefix_sets.account_prefix_set), prefixes(&[held]));
        assert_eq!(sorted(&main.prefix_sets.storage_prefix_sets[&held]), prefixes(&[slot]));
        assert_eq!(main.state.accounts, B256Map::from_iter([(held, None)]));
        assert_eq!(main.state.storages, B256Map::from_iter([(held, zeroed)]));
    }

    #[test]
    fn split_contract_with_a_held_account_covers_only_held_ranges_in_the_main_computation() {
        let contract = key(true, 6);
        let (held_slots, foreign_slots) = ([key(true, 7), key(true, 8)], [key(false, 7)]);
        let all_slots = [held_slots.as_slice(), &foreign_slots].concat();
        let state = HashedPostState {
            accounts: B256Map::default(),
            storages: B256Map::from_iter([(contract, storage(&all_slots))]),
        };

        let plan = TrieSlice::new(cluster(1, &[contract])).plan(&state).unwrap();

        let main = main(&plan);
        assert_eq!(sorted(&main.prefix_sets.account_prefix_set), prefixes(&[contract]));
        assert_eq!(sorted(&main.prefix_sets.storage_prefix_sets[&contract]), prefixes(&held_slots));
        assert_eq!(main.state.storages[&contract], storage(&held_slots));
        assert!(plan.side_calls.is_empty());
    }

    #[test]
    fn split_contract_with_a_foreign_account_gets_a_side_call_for_its_held_ranges() {
        let contract = key(false, 9);
        let (held_slots, foreign_slots) = ([key(true, 10)], [key(false, 10), key(false, 11)]);
        let all_slots = [held_slots.as_slice(), &foreign_slots].concat();
        let state = HashedPostState {
            accounts: B256Map::from_iter([(contract, account(3))]),
            storages: B256Map::from_iter([(contract, storage(&all_slots))]),
        };

        let plan = TrieSlice::new(cluster(1, &[contract])).plan(&state).unwrap();

        assert!(plan.main.is_none(), "a foreign account is not in the main computation");
        let [side] = plan.side_calls.as_slice() else { panic!("one side call expected") };
        assert_eq!(side.contract, contract);
        assert_eq!(sorted(&side.input.prefix_sets.account_prefix_set), prefixes(&[contract]));
        assert_eq!(
            sorted(&side.input.prefix_sets.storage_prefix_sets[&contract]),
            prefixes(&held_slots)
        );
        assert!(side.input.prefix_sets.destroyed_accounts.is_empty());
        assert!(matches!(side.input.state.accounts.get(&contract), Some(Some(_))));
        assert_eq!(
            side.input.state.storages,
            B256Map::from_iter([(contract, storage(&held_slots))])
        );
    }

    #[test]
    fn foreign_only_changes_plan_nothing() {
        let (account_key, contract, split) = (key(false, 12), key(false, 13), key(false, 14));
        let state = HashedPostState {
            accounts: B256Map::from_iter([(account_key, account(1)), (contract, None)]),
            storages: B256Map::from_iter([
                (contract, storage(&[key(true, 15)])),
                (split, storage(&[key(false, 15), key(false, 16)])),
            ]),
        };

        let plan = TrieSlice::new(cluster(1, &[split])).plan(&state).unwrap();

        assert!(plan.is_empty(), "{plan:?}");
    }

    #[test]
    fn a_node_storing_nothing_plans_nothing() {
        let contract = key(true, 17);
        let state = HashedPostState {
            accounts: B256Map::from_iter([(key(true, 18), account(1)), (key(false, 18), None)]),
            storages: B256Map::from_iter([(contract, storage(&[key(true, 19), key(false, 19)]))]),
        };
        let slice = TrieSlice::new(cluster(5, &[contract]));

        assert!(!slice.stores());
        assert!(slice.plan(&state).unwrap().is_empty());
    }

    #[test]
    fn a_wiped_split_storage_is_an_error() {
        for contract in [key(true, 20), key(false, 20)] {
            let state = HashedPostState {
                accounts: B256Map::from_iter([(contract, None)]),
                storages: B256Map::default(),
            };

            let error = TrieSlice::new(cluster(1, &[contract])).plan(&state).unwrap_err();

            assert_eq!(error, TrieSliceError::WipedSplitStorage(contract));
        }
    }

    #[test]
    fn holds_storage_trie_needs_the_account_shard_and_an_unsplit_contract() {
        let (held, foreign, split) = (key(true, 21), key(false, 21), key(true, 22));
        let slice = TrieSlice::new(cluster(1, &[split]));

        assert!(slice.holds_storage_trie(&held));
        assert!(!slice.holds_storage_trie(&foreign));
        assert!(!slice.holds_storage_trie(&split));
    }

    #[test]
    fn compute_keeps_main_updates_and_only_the_side_contracts_storage_trie() {
        let (held, contract, other) = (key(true, 23), key(false, 23), key(false, 24));
        let node = |nonce: u16| BranchNodeCompact::new(nonce | 1, 0, 0, vec![], None);
        let storage_trie = |nonce| StorageTrieUpdates {
            storage_nodes: [(Nibbles::from_nibbles([0, 1]), node(nonce))].into_iter().collect(),
            removed_nodes: Default::default(),
        };
        let main_updates = TrieUpdates {
            account_nodes: [(Nibbles::from_nibbles([1]), node(2))].into_iter().collect(),
            removed_nodes: [Nibbles::from_nibbles([2, 3])].into_iter().collect(),
            storage_tries: B256Map::from_iter([(held, storage_trie(4))]),
        };
        let side_updates = TrieUpdates {
            account_nodes: [(Nibbles::from_nibbles([1]), node(8))].into_iter().collect(),
            removed_nodes: [Nibbles::from_nibbles([4])].into_iter().collect(),
            storage_tries: B256Map::from_iter([
                (contract, storage_trie(16)),
                (other, storage_trie(32)),
            ]),
        };
        let plan = TrieSlicePlan {
            main: Some(TrieInput::default()),
            side_calls: vec![SideCall { contract, input: TrieInput::default() }],
        };
        let mut outputs = vec![main_updates.clone(), side_updates].into_iter();

        let merged = plan.compute(|_| Ok::<_, ()>(outputs.next().unwrap())).unwrap();

        let mut expected = main_updates;
        expected.storage_tries.insert(contract, storage_trie(16));
        assert_eq!(merged, expected);
    }
}
