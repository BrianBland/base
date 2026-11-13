//! An owner's history index: for every row it holds, the changes of the last blocks with the
//! row's previous value, built from reth's persisted changesets.

use std::collections::{BTreeMap, HashMap, VecDeque};

use alloy_primitives::{B256, U256, keccak256};
use reth_primitives_traits::Account;

use crate::{Cluster, PlainChangesets, ShardedTable};

/// The row a change of the history index is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryKey {
    /// An account, by hashed address.
    Account(B256),
    /// A storage slot, by hashed address and hashed slot.
    Storage(B256, B256),
}

/// Per-row changes of consecutive blocks, keyed by hashed key, each with the row's value before
/// the block. Changes of one row are kept in ascending block order.
#[derive(Debug, Default)]
pub struct HistoryIndex {
    accounts: HashMap<B256, VecDeque<(u64, Option<Account>)>>,
    storages: BTreeMap<(B256, B256), VecDeque<(u64, U256)>>,
    /// The rows changed by each indexed block, for pruning.
    blocks: BTreeMap<u64, Vec<HistoryKey>>,
    entries: usize,
}

impl HistoryIndex {
    /// Number of changes indexed.
    pub const fn entries(&self) -> usize {
        self.entries
    }

    /// Adds the changes in `changesets` of the rows `cluster`'s node holds. Blocks must be added
    /// in ascending order and each only once.
    pub fn extend(&mut self, cluster: &Cluster, changesets: PlainChangesets) {
        for (block, change) in changesets.accounts {
            let address = keccak256(change.address);
            if cluster.holds(ShardedTable::HashedAccounts, address.as_slice(), &[]) != Some(true) {
                continue;
            }
            self.accounts.entry(address).or_default().push_back((block, change.info));
            self.record(block, HistoryKey::Account(address));
        }
        for (block_address, entry) in changesets.storages {
            let address = keccak256(block_address.address());
            let slot = keccak256(entry.key);
            let held =
                cluster.holds(ShardedTable::HashedStorages, address.as_slice(), slot.as_slice());
            if held != Some(true) {
                continue;
            }
            let block = block_address.block_number();
            self.storages.entry((address, slot)).or_default().push_back((block, entry.value));
            self.record(block, HistoryKey::Storage(address, slot));
        }
    }

    fn record(&mut self, block: u64, key: HistoryKey) {
        self.blocks.entry(block).or_default().push(key);
        self.entries += 1;
    }

    /// Drops the changes of blocks at or below `block`.
    pub fn prune_through(&mut self, block: u64) {
        let kept = self.blocks.split_off(&(block + 1));
        for key in std::mem::replace(&mut self.blocks, kept).into_values().flatten() {
            match key {
                HistoryKey::Account(address) => {
                    if let Some(changes) = self.accounts.get_mut(&address)
                        && Self::pop_through(changes, block)
                    {
                        self.accounts.remove(&address);
                    }
                }
                HistoryKey::Storage(address, slot) => {
                    if let Some(changes) = self.storages.get_mut(&(address, slot))
                        && Self::pop_through(changes, block)
                    {
                        self.storages.remove(&(address, slot));
                    }
                }
            }
            self.entries -= 1;
        }
    }

    /// Drops the oldest change of a row if it is at or below `block`; returns whether the row has
    /// no change left.
    fn pop_through<V>(changes: &mut VecDeque<(u64, V)>, block: u64) -> bool {
        if changes.front().is_some_and(|(changed, _)| *changed <= block) {
            changes.pop_front();
        }
        changes.is_empty()
    }

    /// The value of `changes`' row before its first change in blocks `(after, through]`, or
    /// `None` when it did not change there.
    fn first_change<V: Copy>(changes: &VecDeque<(u64, V)>, after: u64, through: u64) -> Option<V> {
        let index = changes.partition_point(|(block, _)| *block <= after);
        changes.get(index).filter(|(block, _)| *block <= through).map(|(_, before)| *before)
    }

    /// The account at hashed `address` before its first change in blocks `(after, through]`:
    /// `Some(value)` when it changed there, `None` when it did not.
    pub fn account_before(
        &self,
        address: B256,
        after: u64,
        through: u64,
    ) -> Option<Option<Account>> {
        Self::first_change(self.accounts.get(&address)?, after, through)
    }

    /// The value of hashed `slot` of hashed `address` before its first change in blocks
    /// `(after, through]` (zero: absent), or `None` when it did not change there.
    pub fn storage_before(
        &self,
        address: B256,
        slot: B256,
        after: u64,
        through: u64,
    ) -> Option<U256> {
        Self::first_change(self.storages.get(&(address, slot))?, after, through)
    }

    /// Every slot of hashed `address` that changed in blocks `(after, through]`, with its value
    /// before the first such change.
    pub fn storage_changes(&self, address: B256, after: u64, through: u64) -> Vec<(B256, U256)> {
        self.storages
            .range((address, B256::ZERO)..=(address, B256::repeat_byte(0xff)))
            .filter_map(|((_, slot), changes)| {
                Self::first_change(changes, after, through).map(|before| (*slot, before))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_change_after_a_block_gives_the_value_before_it() {
        let changes = VecDeque::from([(3, 30u64), (5, 50), (9, 90)]);
        assert_eq!(HistoryIndex::first_change(&changes, 2, 10), Some(30));
        assert_eq!(HistoryIndex::first_change(&changes, 3, 10), Some(50));
        assert_eq!(HistoryIndex::first_change(&changes, 5, 8), None);
        assert_eq!(HistoryIndex::first_change(&changes, 9, 20), None);
    }
}
