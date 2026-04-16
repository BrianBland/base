# base-zk-span-proof-sp1

Experimental SP1 prover for the zk-span signature-elision statement.

This spike keeps the same proof statement as `base-zk-span-proof`:

- witness: prepared per-transaction `(tx_type, signature, unsigned_body)` records plus
  `block_tx_counts`
- public output: the same single `statement_hash` journal used by `base-zk-span-proof`

The goal is to compare SP1 execute/prove performance against the existing RISC Zero prototype
without changing the higher-level statement shape.
