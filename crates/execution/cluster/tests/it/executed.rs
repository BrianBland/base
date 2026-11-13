//! Execution results survive their wire encoding unchanged, and are kept within bounds.

use std::{
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

use alloy_consensus::{Eip658Value, Receipt};
use alloy_eips::eip7685::Requests;
use alloy_primitives::{Address, B256, Bytes, Log, U256, map::HashMap};
use base_common_consensus::{BaseReceipt, DepositReceipt};
use base_execution_cluster::{
    ExecutedBlockParts, ExecutedBlockResult, ExecutedBlocks, Frame, ReadError, ReadResult,
    Response, ResultCapture, Shard,
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
/// The dropped block is passed at once, while requests for queued blocks still wait for them.
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
        Some(Bytes::from(index.to_be_bytes().to_vec()))
    })
    .unwrap();
    let submit = |index: usize| capture.submit(block_hash(index), index as u64, index);

    assert!(submit(0));
    encoding.recv().unwrap();
    for index in 1..=ResultCapture::<usize>::QUEUE {
        assert!(submit(index), "dropped block {index} with room in the queue");
    }
    let newest = ResultCapture::<usize>::QUEUE;
    assert!(!submit(newest + 1));
    assert_eq!(capture.dropped(), 1);

    let started = Instant::now();
    let dropped = store.serve(block_hash(newest + 1), newest as u64 + 1, Duration::from_secs(5));
    assert!(matches!(dropped, Err(ReadError::NotRetained { .. })), "{dropped:?}");
    assert!(started.elapsed() < ExecutedBlocks::SERVE_WAIT, "waited {:?}", started.elapsed());

    let releasing = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        release.send(()).unwrap();
    });
    let queued = store.serve(block_hash(5), 5, Duration::from_secs(5));
    assert_eq!(queued, Ok(Bytes::from(5_usize.to_be_bytes().to_vec())), "a queued block waited");
    releasing.join().unwrap();
    assert!(store.wait_for(block_hash(newest), Duration::from_secs(5)).is_some());
    capture.stop().unwrap();
    assert_eq!(store.wait_for(block_hash(0), Duration::ZERO), None, "the late old result stayed");
    for index in 1..=newest {
        assert!(store.wait_for(block_hash(index), Duration::ZERO).is_some(), "lost block {index}");
    }
    assert_eq!(store.wait_for(block_hash(newest + 1), Duration::ZERO), None);
}

#[test]
fn a_block_that_does_not_encode_is_passed() {
    let store = Arc::new(ExecutedBlocks::default());
    let capture = ResultCapture::spawn(Arc::clone(&store), |_: ()| None).unwrap();
    assert!(capture.submit(block_hash(7), 7, ()));
    let itself = store.serve(block_hash(7), 7, Duration::from_secs(5));
    assert_eq!(itself, Err(ReadError::NotRetained { oldest: 7, newest: 7 }));
    capture.stop().unwrap();

    let started = Instant::now();
    let refusal = store.serve(block_hash(6), 6, Duration::from_secs(5));

    assert_eq!(refusal, Err(ReadError::NotRetained { oldest: 7, newest: 7 }));
    assert!(started.elapsed() < ExecutedBlocks::SERVE_WAIT, "waited {:?}", started.elapsed());
}

#[test]
fn results_too_large_to_serve_are_not_kept_but_passed() {
    let store = ExecutedBlocks::default();
    let largest = Bytes::from(vec![0; ExecutedBlocks::MAX_RESULT_LEN]);
    let answer: ReadResult = Ok(Response::ExecutedBlock(largest.clone()));
    assert!(Frame::write(&mut std::io::sink(), &answer).is_ok());
    assert!(store.insert(block_hash(1), 1, largest));

    let too_large = Bytes::from(vec![0; ExecutedBlocks::MAX_RESULT_LEN + 1]);
    assert!(!store.insert(block_hash(2), 2, too_large));
    assert_eq!(store.wait_for(block_hash(2), Duration::ZERO), None);
    let started = Instant::now();
    let refusal = store.serve(block_hash(2), 2, Duration::from_secs(5));
    assert_eq!(refusal, Err(ReadError::NotRetained { oldest: 1, newest: 2 }));
    assert!(started.elapsed() < ExecutedBlocks::SERVE_WAIT, "waited {:?}", started.elapsed());
}

#[test]
fn a_store_after_a_persisted_head_refuses_missing_blocks_up_to_it_at_once() {
    let store = ExecutedBlocks::after_persisted(10);

    let started = Instant::now();
    let refusal = store.serve(block_hash(10), 10, Duration::from_secs(5));
    assert_eq!(refusal, Err(ReadError::NotRetained { oldest: 10, newest: 10 }));
    assert!(started.elapsed() < ExecutedBlocks::SERVE_WAIT, "waited {:?}", started.elapsed());

    let above = store.serve(block_hash(11), 11, ExecutedBlocks::SERVE_WAIT);
    assert_eq!(above, Err(ReadError::NotAvailable));
}

/// The encoded result the tests keep for block `number`.
fn numbered_result(number: u64) -> Bytes {
    Bytes::from(number.to_be_bytes().to_vec())
}

/// A store that has inserted blocks `numbers`, each under `block_hash(number)`.
fn store_of(numbers: std::ops::RangeInclusive<u64>) -> ExecutedBlocks {
    let store = ExecutedBlocks::default();
    for number in numbers {
        assert!(store.insert(block_hash(number as usize), number, numbered_result(number)));
    }
    store
}

#[test]
fn missing_block_at_or_below_the_newest_inserted_is_refused_without_waiting() {
    let newest = 100;
    let store = store_of(1..=newest);
    let oldest = newest - ExecutedBlocks::CAPACITY as u64 + 1;
    let not_retained = Err(ReadError::NotRetained { oldest, newest });

    let started = Instant::now();
    assert_eq!(store.serve(block_hash(1), 1, ExecutedBlocks::SERVE_WAIT), not_retained);
    let other_fork = B256::repeat_byte(0xff);
    assert_eq!(store.serve(other_fork, newest, ExecutedBlocks::SERVE_WAIT), not_retained);
    assert!(started.elapsed() < ExecutedBlocks::SERVE_WAIT, "waited {:?}", started.elapsed());

    let kept = store.serve(block_hash(oldest as usize), oldest, ExecutedBlocks::SERVE_WAIT);
    assert_eq!(kept, Ok(numbered_result(oldest)));
}

#[test]
fn missing_block_above_the_newest_inserted_is_waited_for() {
    let store = Arc::new(store_of(1..=10));

    let started = Instant::now();
    assert_eq!(
        store.serve(block_hash(11), 11, ExecutedBlocks::SERVE_WAIT),
        Err(ReadError::NotAvailable)
    );
    assert!(started.elapsed() >= ExecutedBlocks::SERVE_WAIT, "answered before waiting");

    let finish = {
        let store = Arc::clone(&store);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            store.insert(block_hash(11), 11, numbered_result(11));
        })
    };
    assert_eq!(store.serve(block_hash(11), 11, Duration::from_secs(5)), Ok(numbered_result(11)));
    finish.join().unwrap();
}

/// A wait for a block ends as soon as another block at its number is inserted: the awaited one is
/// on a fork this node did not take.
#[test]
fn wait_ends_when_a_sibling_block_is_inserted() {
    let store = Arc::new(store_of(1..=10));
    let sibling = {
        let store = Arc::clone(&store);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            store.insert(B256::repeat_byte(0xee), 11, numbered_result(11));
        })
    };
    let started = Instant::now();
    let refusal = store.serve(block_hash(11), 11, Duration::from_secs(5));
    assert_eq!(refusal, Err(ReadError::NotRetained { oldest: 1, newest: 11 }));
    assert!(started.elapsed() < Duration::from_secs(5), "waited {:?}", started.elapsed());
    sibling.join().unwrap();
}
