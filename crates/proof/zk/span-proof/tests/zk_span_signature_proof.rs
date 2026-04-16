//! Integration tests for real zk span signature proofs.

use std::str::FromStr;
#[cfg(feature = "prove")]
use std::time::Instant;

use alloy_consensus::{SignableTransaction, TxEip1559};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, TxKind, address};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use base_alloy_consensus::OpTxEnvelope;
use base_protocol::ZkSpanBatchTransactions;
use base_zk_span_proof::{
    ZkSpanSignatureProof, ZkSpanSignatureProofInput, ZkSpanSignatureProofStatement,
    ZkSpanSignatureProofVerifier,
};

const TEST_CHAIN_ID: u64 = 8_453;

fn sample_signer() -> PrivateKeySigner {
    PrivateKeySigner::from_str("0x59c6995e998f97a5a0044976f7dcb9f1f6d0d5f7d28f5b6f8d64f5b5f5b0f001")
        .expect("valid test private key")
}

fn signed_transaction(nonce: u64, to: Address) -> Vec<u8> {
    let signer = sample_signer();
    let transaction = TxEip1559 {
        chain_id: TEST_CHAIN_ID,
        nonce,
        gas_limit: 21_000,
        max_fee_per_gas: 2,
        max_priority_fee_per_gas: 1,
        to: TxKind::Call(to),
        ..Default::default()
    };
    let signature = signer.sign_hash_sync(&transaction.signature_hash()).expect("signable tx");

    let mut encoded = Vec::new();
    OpTxEnvelope::from(transaction.into_signed(signature)).encode_2718(&mut encoded);
    encoded
}

#[cfg(feature = "prove")]
fn tiny_signed_transactions() -> Vec<Vec<u8>> {
    vec![signed_transaction(10, address!("3333333333333333333333333333333333333333"))]
}

#[cfg(feature = "prove")]
fn sample_batch_from_signed_transactions(
    signed_transactions: &[Vec<u8>],
    block_tx_counts: Vec<u64>,
) -> base_protocol::ZkSpanBatch {
    use base_protocol::ZkSpanBatch;

    let canonical_transactions =
        ZkSpanSignatureProofStatement::canonical_bytes_from_signed_bytes(signed_transactions)
            .unwrap();
    let tx_roots = ZkSpanSignatureProofStatement::tx_roots_from_signed_bytes(
        signed_transactions,
        &block_tx_counts,
    )
    .unwrap();
    let mut batch_transactions = ZkSpanBatchTransactions::default();
    batch_transactions.add_txs(canonical_transactions, TEST_CHAIN_ID).unwrap();

    ZkSpanBatch {
        chain_id: TEST_CHAIN_ID,
        block_tx_counts,
        tx_roots,
        txs: batch_transactions,
        ..Default::default()
    }
}

#[test]
fn signed_witness_journal_matches_batch_reconstruction() {
    let signed_transactions = vec![
        signed_transaction(1, address!("1111111111111111111111111111111111111111")),
        signed_transaction(2, address!("2222222222222222222222222222222222222222")),
    ];
    let block_tx_counts = vec![1, 1];
    let input = ZkSpanSignatureProofInput::from_signed_transactions_with_blocks(
        &signed_transactions,
        block_tx_counts.clone(),
    )
    .unwrap();

    let witness_journal = ZkSpanSignatureProofStatement::journal_for_input(&input).unwrap();
    let canonical_transactions =
        ZkSpanSignatureProofStatement::canonical_bytes_from_signed_bytes(&signed_transactions)
            .unwrap();
    let tx_roots = ZkSpanSignatureProofStatement::tx_roots_from_signed_bytes(
        &signed_transactions,
        &block_tx_counts,
    )
    .unwrap();

    let mut batch_transactions = ZkSpanBatchTransactions::default();
    batch_transactions.add_txs(canonical_transactions, TEST_CHAIN_ID).unwrap();

    let batch_journal = ZkSpanSignatureProofStatement::journal_for_batch_transactions(
        &batch_transactions,
        &block_tx_counts,
        &tx_roots,
        TEST_CHAIN_ID,
    )
    .unwrap();

    assert_eq!(witness_journal, batch_journal);
}

#[test]
fn prepared_witness_uses_exact_ethereum_signing_hash() {
    let signer = sample_signer();
    let transaction = TxEip1559 {
        chain_id: TEST_CHAIN_ID,
        nonce: 7,
        gas_limit: 21_000,
        max_fee_per_gas: 2,
        max_priority_fee_per_gas: 1,
        to: TxKind::Call(address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
        ..Default::default()
    };
    let expected_hash = transaction.signature_hash();
    let signature = signer.sign_hash_sync(&expected_hash).expect("signable tx");

    let mut encoded = Vec::new();
    OpTxEnvelope::from(transaction.into_signed(signature)).encode_2718(&mut encoded);

    let input = ZkSpanSignatureProofInput::from_signed_transactions(&vec![encoded]).unwrap();

    assert_eq!(input.transactions.len(), 1);
    assert_eq!(input.transactions[0].signing_hash().unwrap(), expected_hash);
}

#[test]
fn proof_envelope_roundtrip() {
    let proof = ZkSpanSignatureProof::new(vec![1, 2, 3, 4]);
    let encoded = proof.encode().unwrap();
    let decoded = ZkSpanSignatureProof::decode(&encoded).unwrap();

    assert_eq!(decoded, proof);
}

#[test]
fn verifier_rejects_missing_batch_proof() {
    let verifier = ZkSpanSignatureProofVerifier::new([0; 8]);
    let batch = base_protocol::ZkSpanBatch { chain_id: TEST_CHAIN_ID, ..Default::default() };

    let error = verifier.verify_batch(&batch).unwrap_err();
    assert!(matches!(error, base_zk_span_proof::ZkSpanProofError::MissingProof));
}

#[cfg(feature = "prove")]
#[test]
fn real_execution_reports_stats() {
    use base_zk_span_proof::ZkSpanSignatureProofProver;

    let signed_transactions = tiny_signed_transactions();
    let input = ZkSpanSignatureProofInput::from_signed_transactions(&signed_transactions).unwrap();
    let prover = ZkSpanSignatureProofProver::embedded().unwrap();
    let execution = prover.execute_input(&input).unwrap();

    eprintln!(
        "tiny execute stats: segments={}, user_cycles={}",
        execution.segments.len(),
        execution.cycles()
    );
}

#[cfg(feature = "prove")]
#[test]
fn real_composite_proof_verifies_against_batch_transactions() {
    use base_zk_span_proof::ZkSpanSignatureProofProver;

    let signed_transactions = tiny_signed_transactions();
    let input = ZkSpanSignatureProofInput::from_signed_transactions(&signed_transactions).unwrap();
    let prover = ZkSpanSignatureProofProver::embedded().unwrap();
    let execution = prover.execute_input(&input).unwrap();
    eprintln!(
        "tiny composite execute stats: segments={}, user_cycles={}",
        execution.segments.len(),
        execution.cycles()
    );
    let started_at = Instant::now();
    let (proof, stats) = prover.prove_input_composite_with_stats(&input).unwrap();
    eprintln!("tiny composite proof elapsed: {:?}", started_at.elapsed());
    eprintln!(
        "tiny composite prove stats: segments={}, total_cycles={}, user_cycles={}, paging_cycles={}, reserved_cycles={}",
        stats.segments,
        stats.total_cycles,
        stats.user_cycles,
        stats.paging_cycles,
        stats.reserved_cycles
    );

    let mut batch = sample_batch_from_signed_transactions(
        &signed_transactions,
        vec![signed_transactions.len() as u64],
    );
    batch.proof = proof.encode().unwrap();

    prover.verifier().verify_batch(&batch).unwrap();
}

#[cfg(feature = "prove")]
#[test]
#[ignore = "succinct local proving is currently unstable on this Apple Silicon host"]
fn real_succinct_proof_verifies_against_batch_transactions() {
    use base_zk_span_proof::ZkSpanSignatureProofProver;

    let signed_transactions = tiny_signed_transactions();
    let input = ZkSpanSignatureProofInput::from_signed_transactions(&signed_transactions).unwrap();
    let prover = ZkSpanSignatureProofProver::embedded().unwrap();
    let proof = prover.prove_input(&input).unwrap();

    let mut batch = sample_batch_from_signed_transactions(
        &signed_transactions,
        vec![signed_transactions.len() as u64],
    );
    batch.proof = proof.encode().unwrap();

    prover.verifier().verify_batch(&batch).unwrap();
}

#[cfg(feature = "prove")]
#[test]
#[ignore = "groth16 proving requires x86_64 Linux with RISC Zero groth16 components installed"]
fn real_groth16_proof_verifies_against_batch_transactions() {
    use base_zk_span_proof::ZkSpanSignatureProofProver;
    use risc0_zkvm::ProverOpts;

    let signed_transactions = tiny_signed_transactions();
    let input = ZkSpanSignatureProofInput::from_signed_transactions(&signed_transactions).unwrap();
    let prover = ZkSpanSignatureProofProver::embedded().unwrap();
    let proof = prover.prove_input_with_opts(&input, &ProverOpts::groth16()).unwrap();

    let mut batch = sample_batch_from_signed_transactions(
        &signed_transactions,
        vec![signed_transactions.len() as u64],
    );
    batch.proof = proof.encode().unwrap();

    prover.verifier().verify_batch(&batch).unwrap();
}
