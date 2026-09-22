# CHAIN-5451 — P1 prepared-result compile/proof spike

Bounded compile/proof spike for design §2.2 / §6 (P1). **Not** production enablement:
no default-on behavior, no builder-loop wiring, no consensus change. This documents
what compiles today, the eligibility gates, and the smallest API diff.

## What was proven

A Base-owned prepared-result seam (`crates/common/evm/src/executor/prepared.rs`) that:

1. Is reached through the **concrete** executor `BlockBuilder::executor_mut()` already
   returns — no TLS slot, no unsafe downcast, no Reth change.
2. Takes the pending execution result **before any fallible admission check**, as an
   owned `PreparedTx` token (no executor-side slot).
3. Binds the staged result to transaction + build/environment identity and rechecks it
   before commit, routing the accepted candidate through the **existing**
   `commit_transaction` primitive (same gas/DA accounting, receipts, bundle bookkeeping).
4. Gates **both** BAL modes, EIP-8130, and unknown/unsupported modes to immediate serial
   fallback.

### Build / test evidence

| Check | Command | Result |
| --- | --- | --- |
| Compile (std) | `cargo check -p base-common-evm` | pass |
| Compile (no_std) | `cargo check -p base-common-evm --no-default-features` | pass |
| Compile (all features) | `cargo check -p base-common-evm --all-features` | pass |
| Consumer still builds | `cargo check -p base-execution-payload-builder` | pass |
| Clippy (workspace lints) | `cargo clippy -p base-common-evm --all-targets` | clean |
| Behavioral tests | `cargo test -p base-common-evm --lib prepared` | 6/6 pass |

## The generic-bounds compile gate (the actual risk this spike closed)

The design flagged a "genuine compile/proof gate" and a reviewer proposed a trait on the
outer `BlockBuilder`. That is **not** required. Chain of concrete types:

```
ctx.block_builder(db)
  -> impl BlockBuilder<Executor = BlockExecutorForEvm<'a, Evm, DB>>   (builder.rs:778)
BlockExecutorForEvm<'a, Evm, DB> = BlockExecutorFor<.., &'a mut State<DB>, ..>   (reth aliases.rs)
BaseBlockExecutorFactory::Executor = BaseBlockExecutor<EvmF::Evm<DB,I>, R, Spec>  (factory.rs:75)
```

So `executor_mut()` yields `&mut BaseBlockExecutor<..>` directly. The capability is
inherent methods on `BaseBlockExecutor`; no blanket/specialized impl overlap, no trait on
the opaque builder.

The one real obstacle was reading BAL state on the generic `E::DB`. `has_bal()` and
`bal_builder` live on the concrete `revm::database::State<DB>`, and a
blanket-plus-specialized impl over `E::DB` would overlap (illegal on stable). Resolved with
a small `BalEligibility` query trait implemented only for `State<DB>` and `&mut State<DB>`
(the builder path resolves `E::DB = &mut State<DB>`), bounded on the capability methods.

## Consume-before-fallible-check cleanup

The pending result is an owned `PreparedTx` token, not a slot on the executor. Every
fallible follow-up consumes the token by value, so cleanup is a type-level guarantee:

- **stage** — eligibility gate and (on ineligible) return happen before the token exists;
  execution result is moved straight into the token, never parked on `self`.
- **identity mismatch** — token consumed and dropped; `Err(Box<IdentityMismatch>)` carries
  only the two identities. No committed state (test: `identity_mismatch_refuses_and_drops`).
- **refused commit** — token dropped inside `commit_prepared`; `Ok(None)`, state untouched
  (test: `refused_commit_leaves_state_untouched`).
- **accepted** — routed through the existing `commit_transaction`; gas/receipts advance
  exactly as serial (test: `stage_then_commit_advances_state`).

There is no code path that leaves a stale pending value on the executor.

## Supported-mode evidence table

Eligibility is fail-closed: anything not proven supported returns `Ineligible` (serial
fallback), never a committed speculative result. Verified against the pins below.

| Mode / dimension | Source of truth | Spike disposition |
| --- | --- | --- |
| Plain build (no BAL) | `State::has_bal()==false && bal_builder.is_none()` | **Eligible** — the only accepted mode in this spike |
| **Input** BAL | `bal_state.bal.is_some()` (`revm-database-42 state.rs:264`) | `Ineligible::InputBalMode` — serial |
| **Output** BAL building | `bal_state.bal_builder.is_some()` — **invisible to `has_bal()`** | `Ineligible::OutputBalMode` — serial. This is the §2.2 "`has_bal()` alone is insufficient" case; gated separately (test: `output_bal_mode_falls_back_to_serial`). |
| EIP-8130 (Zenith) authenticated tx | `BaseTxEnv::eip8130_signed().is_some()` | `Ineligible::Eip8130` — stays serial per §1 |
| Unknown / future mode | — | fail-closed: not eligible unless explicitly matched |

### B20 version dispatch (inventory only; not reused in this spike)

B20 semantic reuse is P5, not P1. Recorded here so the fork/version routing is pinned:

| Base upgrade | `AssetVersions::from_base_upgrade` | Notes |
| --- | --- | --- |
| < Beryl | `None` | no B20 → serial |
| Beryl | `V1` | activation fork |
| Cobalt | `V2` | adds ERC-8056 surface; not live on any network yet |
| Denim (and newer, e.g. Zenith) | `V3` | ordered-threshold routing; `BaseUpgrade` is `#[non_exhaustive]` so newer forks inherit `V3` |

`from_base_upgrade` is `crates/common/precompiles/src/b20_asset/versions.rs:195`. B20 native
dispatch runs through `BasePrecompiles`; version + fork provenance (not `tx.to` or a numeric
delta) is the required proof surface for any future rebase — out of scope here.

## Required cache originals (verified, for downstream P2/P4)

`commit_transaction` (block_executor.rs) already loads what bundle/revert bookkeeping needs
on the accepted path: it consumes the `ResultAndState` produced during `execute_transaction_
without_commit`, builds the receipt with cumulative gas, and calls `db.commit(state)`. Because
the spike reuses this exact primitive, no separate original-loading is introduced or bypassed.
Speculative *reconstruction* of originals against live canonical state (design §2.2) is P2/P4
work; this spike only proves the seam and does not stage against a separate snapshot yet.

## Smallest API diff

Additive only. No existing signature changed; no builder-loop call site touched.

- **New file** `crates/common/evm/src/executor/prepared.rs` (~230 non-test lines):
  - trait `BalEligibility` (+ impls for `State<DB>` and `&mut T`)
  - `enum Ineligible { InputBalMode, OutputBalMode, Eip8130 }`
  - `struct BuildIdentity { block_number, block_timestamp, tx_hash }`
  - `struct PreparedTx<H, T>` (owns the staged `BaseTxResult`)
  - `struct IdentityMismatch { staged, live }`
  - inherent methods on `BaseBlockExecutor`: `prepared_eligibility`, `stage_prepared`,
    `commit_prepared`, `build_identity`
- `executor/mod.rs`: `mod prepared;` + re-export (3 lines)
- `lib.rs`: extend the `executor::{..}` re-export (2 lines)

## Pins verified against

- Local branch base: `9780173f4` (main-derived, retains later predicate-index work).
- Reth: `base-v2.5.2.6` = `5877708bbf9219c44758cd2ce28a365f738661f7`.
- revm family: 42 (`revm 42.0.1`, `revm-database 42.0.0`, `revm-database-interface 43.0.0`).
- alloy-evm `0.38.0` (`BlockExecutor`, `ExecutableTx`, `CommitChanges`, `StateDB`).

Note: the design doc cited reth pin `5877708…` on PRs #5137/#5136; this branch is on the
same reth tag. The design's "revm 42 family" holds.

## Not done here (correctly deferred)

- No builder-loop integration, no snapshot/recorder, no fee replay, no B20 rebase — those
  are P2–P5 and explicitly blocked on P0/P1.
- `stage_prepared` executes against the executor's *live* db, not a separate immutable
  prefix snapshot. Anchoring speculation to a bounded prefix snapshot is P2. The identity
  binding is the seam that will let P4 reject stale speculative results.
- No kill switch / default-off flag: nothing calls this yet, so there is nothing to gate.
