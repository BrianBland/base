# Builder speculation contract (Phase 1)

The basic builder alone selects and orders transactions. A speculator predicts work, never
admission. `take` is nonblocking; misses, unsupported transactions, panics, cancellation and
failed validation all fall back to ordinary execution. No real builder is wired in Phase 1.

Each parent epoch fixes the EVM factory, environment and immutable parent database factory.
Workers own their providers. Reset/cancel prevents results from an earlier epoch escaping.
Submit replaces the predicted window, retaining matching work; discarded predictions may
only waste execution. Forwarding is optional and is never a correctness gate.

Consumption on the owner thread checks transaction identity, environment, every account
existence/nonce/code read, every storage value, and every recorded balance range against
committed State. L1 fee parameter reads are recorded too. Only after validation do balances
rebase by committed minus observed, storage originals come from the committed prefix, and
fee credits enter the ordinary ResultAndState. Lifecycle flags remain those of execution.
The normal executor alone commits; rejected commit conditions never update the overlay.
Deposits, EIP-8130 and Amsterdam or later execution remain inline.
The first ordinary transaction also executes inline to initialize the owner's block-local L1
fee cache. A committed change to L1-block storage/lifecycle disables speculation for the rest
of that context, preserving the cache's sequential semantics. Canonical transaction environments
are compared in full, not just by envelope hash; custom environments take the inline path.

Use the same production EVM factory and uninspected environment on both sides. Code served by
hash must match its hash (candidate executions reject mismatches), and block hashes belong to
the immutable parent epoch. The current factory accepts the engine's infallible, Sync read
facade; integrating fallible providers needs an error-poisoning facade, not default-value reads
that could be mistaken for authentic state. Providers are constructed/used on their worker and
need not be Send. cancel is nonblocking; Drop joins in-flight work, so provider I/O must be bounded.

Tests must exercise stale nonce/storage rejection, admitted balance drift and fee rebasing,
identity mismatch, discarded work/reset, and a declined commit followed by another choice.
The offline gate compares the same deterministic fault stream's included hashes, full
receipts and BundleState against a sequential loop at every measured iteration.
