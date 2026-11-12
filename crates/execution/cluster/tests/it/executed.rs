//! Execution results survive their wire encoding unchanged, and are kept within bounds.

use std::{
    sync::{Arc, mpsc},
    time::Duration,
};

use alloy_consensus::{Eip658Value, Receipt};
use alloy_eips::eip7685::Requests;
use alloy_primitives::{Address, B256, Bytes, Log, U256, map::HashMap};
use base_common_consensus::{BaseReceipt, DepositReceipt};
use base_execution_cluster::{
    ExecutedBlockParts, ExecutedBlockResult, ExecutedBlocks, Frame, ReadResult, Response,
    ResultCapture, Shard,
};
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_trie_common::SortedTrieData;
use revm::{bytecode::Bytecode, database::BundleState, state::AccountInfo};

use crate::common::{KeyUniverse, random_updates, rng};

fn account(nonce: u64, balance: u64, code_hash: B256) -> AccountInfo {
    AccountInfo { nonce, balance: U256::from(balance), code_hash, ..AccountInfo::default() }
}

const fn receipt(cumulative_gas_used: u64, logs: Vec<Log>) -> Receipt {
    Receipt { status: Eip658Value::Eip658(true), cumulative_gas_used, logs }
}

/// A block's parts with two receipts carrying logs (one a deposit with a nonce), a created
/// contract, a changed slot, a deleted account, their reverts, and random trie data.
fn parts() -> ExecutedBlockParts<BaseReceipt> {
    let created = Address::repeat_byte(1);
    let deleted = Address::repeat_byte(2);
    let code = Bytecode::new_raw(Bytes::from_static(&[0x60, 0x00, 0x60, 0x00]));
    let log = |byte| {
        Log::new_unchecked(
            Address::repeat_byte(byte),
            vec![B256::repeat_byte(byte)],
            Bytes::from(vec![byte; 3]),
        )
    };
    let receipts = vec![
        BaseReceipt::Deposit(DepositReceipt {
            inner: receipt(21_000, vec![log(7)]),
            deposit_nonce: Some(42),
            deposit_receipt_version: Some(1),
        }),
        BaseReceipt::Eip1559(receipt(63_000, vec![log(8), log(9)])),
    ];
    let state = BundleState::new(
        [
            (
                created,
                None,
                Some(account(1, 10, code.hash_slow())),
                HashMap::from_iter([(U256::from(5), (U256::ZERO, U256::from(6)))]),
            ),
            (deleted, Some(account(3, 30, B256::ZERO)), None, HashMap::default()),
        ],
        [vec![
            (created, Some(None), vec![(U256::from(5), U256::ZERO)]),
            (deleted, Some(Some(account(3, 30, B256::ZERO))), vec![]),
        ]],
        [(code.hash_slow(), code)],
    );
    let mut rng = rng(11);
    let keys = KeyUniverse::new(&mut rng, &[Shard(0x10), Shard(0x80)]);
    let updates = random_updates(&mut rng, &keys);
    ExecutedBlockParts {
        output: BlockExecutionOutput {
            result: BlockExecutionResult {
                receipts,
                requests: Requests::new(vec![Bytes::from_static(&[0x01, 0xaa])]),
                gas_used: 63_000,
                blob_gas_used: 131_072,
            },
            state,
        },
        senders: vec![Address::repeat_byte(0xa), Address::repeat_byte(0xb)],
        trie: SortedTrieData::new(updates.hashed_state, updates.trie_updates),
    }
}

#[test]
fn execution_result_round_trips_through_its_encoding() {
    let parts = parts();
    assert!(!parts.output.state.reverts.is_empty());
    assert!(!parts.trie.hashed_state.accounts.is_empty());
    assert!(!parts.trie.trie_updates.account_nodes_ref().is_empty());

    let encoded =
        ExecutedBlockResult::new(&parts.output, &parts.senders, &parts.trie).encode().unwrap();
    let decoded =
        ExecutedBlockResult::decode(&encoded).unwrap().into_parts::<BaseReceipt>().unwrap();
    assert_eq!(decoded, parts);
}

#[test]
fn corrupt_encoding_is_rejected() {
    let parts = parts();
    let encoded =
        ExecutedBlockResult::new(&parts.output, &parts.senders, &parts.trie).encode().unwrap();
    assert!(ExecutedBlockResult::decode(&encoded[..encoded.len() / 2]).is_err());
}

fn block_hash(index: usize) -> B256 {
    B256::left_padding_from(&index.to_be_bytes())
}

/// A capture whose encoder holds the first block until released keeps at most its queue of later
/// blocks, and inserts them after the held one, so the late old result cannot evict a newer one.
#[test]
fn capture_is_bounded_and_keeps_results_in_capture_order() {
    let store = Arc::new(ExecutedBlocks::default());
    let (encoding_tx, encoding) = mpsc::channel();
    let (release, release_rx) = mpsc::channel::<()>();
    let capture = ResultCapture::spawn(Arc::clone(&store), move |index: usize| {
        if index == 0 {
            encoding_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        }
        Some((block_hash(index), Bytes::from(index.to_be_bytes().to_vec())))
    })
    .unwrap();

    assert!(capture.submit(0));
    encoding.recv().unwrap();
    for index in 1..=ResultCapture::<usize>::QUEUE {
        assert!(capture.submit(index), "dropped block {index} with room in the queue");
    }
    let newest = ResultCapture::<usize>::QUEUE;
    assert!(!capture.submit(newest + 1));
    assert_eq!(capture.dropped(), 1);

    release.send(()).unwrap();
    assert!(store.wait_for(block_hash(newest), Duration::from_secs(5)).is_some());
    capture.stop().unwrap();
    assert_eq!(store.wait_for(block_hash(0), Duration::ZERO), None, "the late old result stayed");
    for index in 1..=newest {
        assert!(store.wait_for(block_hash(index), Duration::ZERO).is_some(), "lost block {index}");
    }
    assert_eq!(store.wait_for(block_hash(newest + 1), Duration::ZERO), None);
}

#[test]
fn results_too_large_to_serve_are_not_kept() {
    let store = ExecutedBlocks::default();
    let largest = Bytes::from(vec![0; ExecutedBlocks::MAX_RESULT_LEN]);
    let answer: ReadResult = Ok(Response::ExecutedBlock(largest.clone()));
    assert!(Frame::write(&mut std::io::sink(), &answer).is_ok());
    assert!(store.insert(block_hash(1), largest));

    assert!(!store.insert(block_hash(2), Bytes::from(vec![0; ExecutedBlocks::MAX_RESULT_LEN + 1])));
    assert_eq!(store.wait_for(block_hash(2), Duration::ZERO), None);
}
