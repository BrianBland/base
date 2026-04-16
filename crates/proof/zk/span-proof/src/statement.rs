use alloy_consensus::{
    SignableTransaction, TxEip1559, TxEip2930, TxEip7702, TxEnvelope, TxLegacy, TxType,
    proofs::ordered_trie_root_encoded,
};
use alloy_eips::eip2718::{Decodable2718, Encodable2718};
use alloy_primitives::{Address, B256, Bytes, Signature};
use alloy_rlp::{Decodable, Encodable};
use base_alloy_consensus::{OpTxEnvelope, TxZkSequencer, ZkSequencerTxBody};
use base_protocol::{ZkSpanBatch, ZkSpanBatchTransactions};

use crate::{
    ZkSpanProofError, ZkSpanSignatureProofInput, ZkSpanSignatureProofJournal,
    ZkSpanSignatureProofTransaction,
};

/// Stateless helper for the zk span signature proof statement.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ZkSpanSignatureProofStatement;

impl ZkSpanSignatureProofStatement {
    /// Builds the public journal from signed transaction witness bytes.
    pub fn journal_for_input(
        input: &ZkSpanSignatureProofInput,
    ) -> Result<ZkSpanSignatureProofJournal, ZkSpanProofError> {
        input.validate()?;

        let normalized_transactions = input
            .transactions
            .iter()
            .map(ZkSpanSignatureProofTransaction::normalized_transaction)
            .collect::<Result<Vec<(u8, Address, Vec<u8>)>, ZkSpanProofError>>()?;
        let signed_transactions = input
            .transactions
            .iter()
            .map(Self::signed_transaction_from_proof_transaction)
            .collect::<Result<Vec<Vec<u8>>, ZkSpanProofError>>()?;
        let tx_roots =
            Self::tx_roots_from_signed_transactions(&signed_transactions, &input.block_tx_counts)?;

        Ok(ZkSpanSignatureProofJournal::from_normalized_transactions_and_roots(
            &normalized_transactions,
            &tx_roots,
        ))
    }

    /// Builds the public journal expected for a decoded zk span batch.
    pub fn journal_for_batch(
        batch: &ZkSpanBatch,
    ) -> Result<ZkSpanSignatureProofJournal, ZkSpanProofError> {
        Self::journal_for_batch_transactions(
            &batch.txs,
            &batch.block_tx_counts,
            &batch.tx_roots,
            batch.chain_id,
        )
    }

    /// Builds the public journal expected for sender-compressed batch transactions.
    pub fn journal_for_batch_transactions(
        transactions: &ZkSpanBatchTransactions,
        block_tx_counts: &[u64],
        tx_roots: &[B256],
        chain_id: u64,
    ) -> Result<ZkSpanSignatureProofJournal, ZkSpanProofError> {
        let canonical_transactions = transactions
            .full_txs(chain_id)
            .map_err(|error| ZkSpanProofError::BatchTransaction(error.to_string()))?;
        Self::validate_block_partition(
            canonical_transactions.len(),
            block_tx_counts,
            Some(tx_roots.len()),
        )?;

        let normalized_transactions = canonical_transactions
            .iter()
            .map(|transaction| Self::normalized_transaction_from_canonical_bytes(transaction))
            .collect::<Result<Vec<(u8, Address, Vec<u8>)>, ZkSpanProofError>>()?;

        Ok(ZkSpanSignatureProofJournal::from_normalized_transactions_and_roots(
            &normalized_transactions,
            tx_roots,
        ))
    }

    /// Validates that the per-block transaction counts match the transaction stream.
    pub fn validate_block_partition(
        transaction_count: usize,
        block_tx_counts: &[u64],
        tx_root_count: Option<usize>,
    ) -> Result<(), ZkSpanProofError> {
        if block_tx_counts.is_empty() {
            return Err(ZkSpanProofError::InvalidBlockPartition);
        }

        let total_transactions = block_tx_counts.iter().try_fold(0u64, |acc, block_tx_count| {
            acc.checked_add(*block_tx_count).ok_or(ZkSpanProofError::InvalidBlockPartition)
        })?;

        if total_transactions != transaction_count as u64 {
            return Err(ZkSpanProofError::InvalidBlockPartition);
        }

        if tx_root_count.is_some_and(|count| count != block_tx_counts.len()) {
            return Err(ZkSpanProofError::InvalidBlockPartition);
        }

        Ok(())
    }

    /// Converts signed transaction witness bytes into prepared proof witness transactions.
    pub fn proof_transactions_from_signed_bytes(
        signed_transactions: &[Vec<u8>],
    ) -> Result<Vec<ZkSpanSignatureProofTransaction>, ZkSpanProofError> {
        signed_transactions
            .iter()
            .map(|transaction| Self::proof_transaction_from_signed_bytes(transaction))
            .collect()
    }

    /// Converts a signed sequencer transaction into a prepared proof witness transaction.
    pub fn proof_transaction_from_signed_bytes(
        signed_transaction: &[u8],
    ) -> Result<ZkSpanSignatureProofTransaction, ZkSpanProofError> {
        let transaction = OpTxEnvelope::decode_2718(&mut signed_transaction.as_ref())
            .map_err(|_| ZkSpanProofError::TransactionDecoding)?;

        match transaction {
            OpTxEnvelope::Legacy(transaction) => Ok(ZkSpanSignatureProofTransaction::new(
                TxType::Legacy as u8,
                transaction.signature().as_bytes().to_vec(),
                Self::rlp_encode_body(transaction.tx()),
            )),
            OpTxEnvelope::Eip2930(transaction) => Ok(ZkSpanSignatureProofTransaction::new(
                TxType::Eip2930 as u8,
                transaction.signature().as_bytes().to_vec(),
                Self::rlp_encode_body(transaction.tx()),
            )),
            OpTxEnvelope::Eip1559(transaction) => Ok(ZkSpanSignatureProofTransaction::new(
                TxType::Eip1559 as u8,
                transaction.signature().as_bytes().to_vec(),
                Self::rlp_encode_body(transaction.tx()),
            )),
            OpTxEnvelope::Eip7702(transaction) => Ok(ZkSpanSignatureProofTransaction::new(
                TxType::Eip7702 as u8,
                transaction.signature().as_bytes().to_vec(),
                Self::rlp_encode_body(transaction.tx()),
            )),
            OpTxEnvelope::Deposit(_) => {
                Err(ZkSpanProofError::UnsupportedTransactionType("deposit"))
            }
            OpTxEnvelope::ZkSequencer(_) => {
                Err(ZkSpanProofError::UnsupportedTransactionType("zk_sequencer"))
            }
        }
    }

    /// Converts a prepared proof witness transaction into exact signed transaction bytes.
    pub fn signed_transaction_from_proof_transaction(
        transaction: &ZkSpanSignatureProofTransaction,
    ) -> Result<Vec<u8>, ZkSpanProofError> {
        let signature = Self::signature_from_bytes(&transaction.signature)?;
        let mut unsigned_body = transaction.unsigned_body.as_slice();
        let envelope = match transaction.tx_type {
            value if value == TxType::Legacy as u8 => TxEnvelope::Legacy(
                TxLegacy::decode(&mut unsigned_body)
                    .map_err(|_| ZkSpanProofError::TransactionDecoding)?
                    .into_signed(signature),
            ),
            value if value == TxType::Eip2930 as u8 => TxEnvelope::Eip2930(
                TxEip2930::decode(&mut unsigned_body)
                    .map_err(|_| ZkSpanProofError::TransactionDecoding)?
                    .into_signed(signature),
            ),
            value if value == TxType::Eip1559 as u8 => TxEnvelope::Eip1559(
                TxEip1559::decode(&mut unsigned_body)
                    .map_err(|_| ZkSpanProofError::TransactionDecoding)?
                    .into_signed(signature),
            ),
            value if value == TxType::Eip7702 as u8 => TxEnvelope::Eip7702(
                TxEip7702::decode(&mut unsigned_body)
                    .map_err(|_| ZkSpanProofError::TransactionDecoding)?
                    .into_signed(signature),
            ),
            _ => {
                return Err(ZkSpanProofError::UnsupportedTransactionType("proof_input_tx_type"));
            }
        };

        if !unsigned_body.is_empty() {
            return Err(ZkSpanProofError::TransactionDecoding);
        }

        let mut encoded = Vec::new();
        envelope.encode_2718(&mut encoded);
        Ok(encoded)
    }

    /// Computes per-block transaction trie roots from exact signed transaction bytes.
    pub fn tx_roots_from_signed_transactions(
        signed_transactions: &[Vec<u8>],
        block_tx_counts: &[u64],
    ) -> Result<Vec<B256>, ZkSpanProofError> {
        Self::validate_block_partition(signed_transactions.len(), block_tx_counts, None)?;

        let mut tx_offset = 0usize;
        let mut tx_roots = Vec::with_capacity(block_tx_counts.len());

        for block_tx_count in block_tx_counts {
            let block_tx_count = usize::try_from(*block_tx_count)
                .map_err(|_| ZkSpanProofError::InvalidBlockPartition)?;
            let block_end = tx_offset
                .checked_add(block_tx_count)
                .ok_or(ZkSpanProofError::InvalidBlockPartition)?;
            tx_roots.push(ordered_trie_root_encoded(&signed_transactions[tx_offset..block_end]));
            tx_offset = block_end;
        }

        Ok(tx_roots)
    }

    /// Computes per-block transaction trie roots directly from signed transaction witness bytes.
    pub fn tx_roots_from_signed_bytes(
        signed_transactions: &[Vec<u8>],
        block_tx_counts: &[u64],
    ) -> Result<Vec<B256>, ZkSpanProofError> {
        Self::tx_roots_from_signed_transactions(signed_transactions, block_tx_counts)
    }

    /// Converts signed transaction witness bytes into normalized tuples committed by the proof.
    pub fn normalized_transactions_from_signed_bytes(
        signed_transactions: &[Vec<u8>],
    ) -> Result<Vec<(u8, Address, Vec<u8>)>, ZkSpanProofError> {
        let transactions = Self::proof_transactions_from_signed_bytes(signed_transactions)?;
        transactions.iter().map(ZkSpanSignatureProofTransaction::normalized_transaction).collect()
    }

    /// Converts a signed sequencer transaction into a normalized proof tuple.
    pub fn normalized_transaction_from_signed_bytes(
        signed_transaction: &[u8],
    ) -> Result<(u8, Address, Vec<u8>), ZkSpanProofError> {
        Self::proof_transaction_from_signed_bytes(signed_transaction)
            .and_then(|transaction| transaction.normalized_transaction())
    }

    /// Converts canonical zk span transaction bytes into the normalized proof tuple.
    pub fn normalized_transaction_from_canonical_bytes(
        canonical_transaction: &[u8],
    ) -> Result<(u8, Address, Vec<u8>), ZkSpanProofError> {
        let transaction = OpTxEnvelope::decode_2718(&mut canonical_transaction.as_ref())
            .map_err(|_| ZkSpanProofError::TransactionDecoding)?;

        match transaction {
            OpTxEnvelope::ZkSequencer(transaction) => {
                Ok(Self::normalized_transaction_from_zk_sequencer(transaction.into_inner()))
            }
            _ => Err(ZkSpanProofError::UnsupportedTransactionType("non_zk_sequencer_batch_tx")),
        }
    }

    /// Converts a canonical zk sequencer transaction into the normalized proof tuple.
    pub fn normalized_transaction_from_zk_sequencer(
        transaction: TxZkSequencer,
    ) -> (u8, Address, Vec<u8>) {
        let tx_type = transaction.body.inner_tx_type() as u8;
        let sender = transaction.sender;
        let body = Self::rlp_encode_zk_body(&transaction.body);
        (tx_type, sender, body)
    }

    /// Converts signed transaction witness bytes into canonical `TxZkSequencer` bytes.
    pub fn canonical_transactions_from_signed_bytes(
        signed_transactions: &[Vec<u8>],
    ) -> Result<Vec<Vec<u8>>, ZkSpanProofError> {
        signed_transactions
            .iter()
            .map(|transaction| Self::canonical_transaction_from_signed_bytes(transaction))
            .collect()
    }

    /// Converts a signed sequencer transaction into canonical `TxZkSequencer` bytes.
    pub fn canonical_transaction_from_signed_bytes(
        signed_transaction: &[u8],
    ) -> Result<Vec<u8>, ZkSpanProofError> {
        let transaction = OpTxEnvelope::decode_2718(&mut signed_transaction.as_ref())
            .map_err(|_| ZkSpanProofError::TransactionDecoding)?;

        let canonical = match transaction {
            OpTxEnvelope::Legacy(transaction) => TxZkSequencer::new(
                transaction.recover_signer().map_err(|_| ZkSpanProofError::SenderRecovery)?,
                ZkSequencerTxBody::Legacy(transaction.tx().clone()),
            ),
            OpTxEnvelope::Eip2930(transaction) => TxZkSequencer::new(
                transaction.recover_signer().map_err(|_| ZkSpanProofError::SenderRecovery)?,
                ZkSequencerTxBody::Eip2930(transaction.tx().clone()),
            ),
            OpTxEnvelope::Eip1559(transaction) => TxZkSequencer::new(
                transaction.recover_signer().map_err(|_| ZkSpanProofError::SenderRecovery)?,
                ZkSequencerTxBody::Eip1559(transaction.tx().clone()),
            ),
            OpTxEnvelope::Eip7702(transaction) => TxZkSequencer::new(
                transaction.recover_signer().map_err(|_| ZkSpanProofError::SenderRecovery)?,
                ZkSequencerTxBody::Eip7702(transaction.tx().clone()),
            ),
            OpTxEnvelope::Deposit(_) => {
                return Err(ZkSpanProofError::UnsupportedTransactionType("deposit"));
            }
            OpTxEnvelope::ZkSequencer(_) => {
                return Err(ZkSpanProofError::UnsupportedTransactionType("zk_sequencer"));
            }
        };

        let mut encoded = Vec::new();
        OpTxEnvelope::from(canonical).encode_2718(&mut encoded);
        Ok(encoded)
    }

    /// Converts signed transaction witness bytes into `Bytes` for batch construction helpers.
    pub fn canonical_bytes_from_signed_bytes(
        signed_transactions: &[Vec<u8>],
    ) -> Result<Vec<Bytes>, ZkSpanProofError> {
        Self::canonical_transactions_from_signed_bytes(signed_transactions)
            .map(|transactions| transactions.into_iter().map(Bytes::from).collect())
    }

    /// Decodes a 65-byte Ethereum signature from witness bytes.
    pub fn signature_from_bytes(signature: &[u8]) -> Result<Signature, ZkSpanProofError> {
        if signature.len() != 65 {
            return Err(ZkSpanProofError::InvalidSignatureLength);
        }

        let mut raw_signature = [0u8; 65];
        raw_signature.copy_from_slice(signature);
        Signature::from_raw_array(&raw_signature)
            .map_err(|_| ZkSpanProofError::InvalidSignatureLength)
    }

    /// RLP-encodes an unsigned transaction body for proof normalization.
    pub fn rlp_encode_body<T: Encodable>(transaction: &T) -> Vec<u8> {
        let mut encoded = Vec::new();
        transaction.encode(&mut encoded);
        encoded
    }

    /// RLP-encodes a canonical zk sequencer transaction body for proof normalization.
    pub fn rlp_encode_zk_body(body: &ZkSequencerTxBody) -> Vec<u8> {
        match body {
            ZkSequencerTxBody::Legacy(transaction) => Self::rlp_encode_body(transaction),
            ZkSequencerTxBody::Eip2930(transaction) => Self::rlp_encode_body(transaction),
            ZkSequencerTxBody::Eip1559(transaction) => Self::rlp_encode_body(transaction),
            ZkSequencerTxBody::Eip7702(transaction) => Self::rlp_encode_body(transaction),
        }
    }
}
