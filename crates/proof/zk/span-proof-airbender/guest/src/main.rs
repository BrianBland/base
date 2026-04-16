//! Airbender guest for zk span signature proof generation.

#![no_std]
#![no_main]

extern crate alloc;

use alloc::vec::Vec;

use airbender::guest::read;
use alloy_consensus::{
    SignableTransaction, TxEip1559, TxEip2930, TxEip7702, TxEnvelope, TxLegacy, TxType,
    proofs::ordered_trie_root_encoded,
};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, B256, Signature, keccak256};
use alloy_rlp::Decodable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ZkSpanSignatureProofTransaction {
    tx_type: u8,
    signature: Vec<u8>,
    unsigned_body: Vec<u8>,
}

impl ZkSpanSignatureProofTransaction {
    fn signing_hash(&self) -> [u8; 32] {
        match self.tx_type {
            0 => keccak256(&self.unsigned_body).0,
            1 | 2 | 4 => {
                let mut payload = Vec::with_capacity(self.unsigned_body.len() + 1);
                payload.push(self.tx_type);
                payload.extend_from_slice(&self.unsigned_body);
                keccak256(payload).0
            }
            _ => panic!("unsupported witness transaction type"),
        }
    }

    fn recovered_sender(&self) -> Address {
        let signature = Signature::try_from(self.signature.as_slice())
            .expect("witness signature should decode");
        signature
            .recover_address_from_prehash(&B256::from(self.signing_hash()))
            .expect("witness signature should recover")
    }

    fn signature(&self) -> Signature {
        Signature::try_from(self.signature.as_slice()).expect("witness signature should decode")
    }

    fn signed_transaction_bytes(&self) -> Vec<u8> {
        let signature = self.signature();
        let mut unsigned_body = self.unsigned_body.as_slice();
        let envelope = match self.tx_type {
            value if value == TxType::Legacy as u8 => TxEnvelope::Legacy(
                TxLegacy::decode(&mut unsigned_body).unwrap().into_signed(signature),
            ),
            value if value == TxType::Eip2930 as u8 => TxEnvelope::Eip2930(
                TxEip2930::decode(&mut unsigned_body).unwrap().into_signed(signature),
            ),
            value if value == TxType::Eip1559 as u8 => TxEnvelope::Eip1559(
                TxEip1559::decode(&mut unsigned_body).unwrap().into_signed(signature),
            ),
            value if value == TxType::Eip7702 as u8 => TxEnvelope::Eip7702(
                TxEip7702::decode(&mut unsigned_body).unwrap().into_signed(signature),
            ),
            _ => panic!("unsupported witness transaction type"),
        };

        assert!(unsigned_body.is_empty(), "unsigned witness body should decode exactly");

        let mut encoded = Vec::new();
        envelope.encode_2718(&mut encoded);
        encoded
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ZkSpanSignatureProofInput {
    transactions: Vec<ZkSpanSignatureProofTransaction>,
    block_tx_counts: Vec<u64>,
}

impl ZkSpanSignatureProofInput {
    fn validate(&self) {
        assert!(!self.block_tx_counts.is_empty(), "proof input must include block tx counts");

        let total_transactions: u64 = self
            .block_tx_counts
            .iter()
            .try_fold(0u64, |acc, block_tx_count| acc.checked_add(*block_tx_count))
            .expect("block tx counts must fit in u64");

        assert_eq!(
            total_transactions,
            self.transactions.len() as u64,
            "block tx counts must match the transaction stream",
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ZkSpanSignatureProofJournal {
    statement_hash: [u8; 32],
}

impl ZkSpanSignatureProofJournal {
    fn from_input(input: &ZkSpanSignatureProofInput) -> Self {
        let normalized_txs_hash = Self::normalized_transactions_hash(&input.transactions);
        let tx_roots_hash = Self::tx_roots_hash(&Self::tx_roots(input));
        Self { statement_hash: Self::statement_hash(normalized_txs_hash, tx_roots_hash) }
    }

    fn normalized_transactions_hash(transactions: &[ZkSpanSignatureProofTransaction]) -> [u8; 32] {
        let mut preimage = Vec::new();
        preimage.extend_from_slice(&(transactions.len() as u64).to_be_bytes());

        for transaction in transactions {
            let recovered_sender = transaction.recovered_sender();
            preimage.push(transaction.tx_type);
            preimage.extend_from_slice(recovered_sender.as_slice());
            preimage.extend_from_slice(&(transaction.unsigned_body.len() as u64).to_be_bytes());
            preimage.extend_from_slice(&transaction.unsigned_body);
        }

        keccak256(preimage).0
    }

    fn tx_roots(input: &ZkSpanSignatureProofInput) -> Vec<B256> {
        let signed_transactions = input
            .transactions
            .iter()
            .map(ZkSpanSignatureProofTransaction::signed_transaction_bytes)
            .collect::<Vec<Vec<u8>>>();
        let mut tx_offset = 0usize;
        let mut tx_roots = Vec::with_capacity(input.block_tx_counts.len());

        for block_tx_count in &input.block_tx_counts {
            let block_tx_count =
                usize::try_from(*block_tx_count).expect("block tx count fits usize");
            let block_end = tx_offset.checked_add(block_tx_count).expect("block range should fit");
            tx_roots.push(ordered_trie_root_encoded(&signed_transactions[tx_offset..block_end]));
            tx_offset = block_end;
        }

        tx_roots
    }

    fn tx_roots_hash(tx_roots: &[B256]) -> [u8; 32] {
        let mut preimage = Vec::with_capacity(8 + tx_roots.len() * B256::len_bytes());
        preimage.extend_from_slice(&(tx_roots.len() as u64).to_be_bytes());
        for tx_root in tx_roots {
            preimage.extend_from_slice(tx_root.as_slice());
        }
        keccak256(preimage).0
    }

    fn statement_hash(normalized_txs_hash: [u8; 32], tx_roots_hash: [u8; 32]) -> [u8; 32] {
        let mut preimage = Vec::with_capacity(14 + 32 + 32);
        preimage.extend_from_slice(b"base.zkspan.v2");
        preimage.extend_from_slice(&normalized_txs_hash);
        preimage.extend_from_slice(&tx_roots_hash);
        keccak256(preimage).0
    }

    fn commitment_words(&self) -> [u32; 8] {
        let digest = self.statement_hash;

        let mut words = [0u32; 8];
        for (index, chunk) in digest.chunks_exact(4).enumerate() {
            words[index] = u32::from_be_bytes(chunk.try_into().expect("4-byte digest chunk"));
        }
        words
    }
}

/// Computes the Airbender zk-span public commitment for the provided witness input.
#[airbender::main]
fn main() -> [u32; 8] {
    let input: ZkSpanSignatureProofInput = read().expect("failed to read zk span proof input");
    input.validate();
    let journal = ZkSpanSignatureProofJournal::from_input(&input);
    journal.commitment_words()
}
