# Builder speculation contract (Phase 1b)

The basic builder alone selects and orders transactions. A speculator predicts work, never
admission. `take` waits at most 10 ms for the chosen frontier, waking on cancellation/reset.
Unsupported transactions, panics and timeouts fall back to ordinary execution. No real builder
is wired. Owner-thread validation remains mandatory even after engine validation.

Each parent epoch fixes the EVM factory, environment and immutable parent database factory.
Workers own their providers. Reset/cancel prevents results from an earlier epoch escaping.
Submit retains a matching ordered suffix and appends new candidates to its plan. Skips remove
speculative writes and invalidate readers; reordered plans start a new generation. A bounded
generation rolls over when its index capacity is exhausted, retaining the committed overlay.
Workers forward writes, invalidate affected readers and block on ESTIMATE dependencies.
The builder alone advances the committed prefix through on_commit, including inline execution,
fees and system changes. A returned result is only a proposal: a later choice without a commit
discards its writes. Frontier retries execute on workers against the committed Store.

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
