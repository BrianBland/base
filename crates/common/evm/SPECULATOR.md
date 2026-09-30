# Builder speculation contract (Phase 1d)

The basic builder alone selects and orders transactions. A speculator predicts work, never
admission. `take` has a configurable frontier wait budget (10 ms by default), waking on reset
or cancellation. This is a scheduling budget, not a hard real-time/provider-I/O deadline.
Unsupported transactions, panics and timeouts fall back to ordinary execution. No real builder
is wired. Engine invalidation is only a scheduling hint; owner validation is never skipped.

Each parent epoch fixes the EVM factory, environment and immutable parent database factory.
Workers own their providers. Reset/cancel prevents results from an earlier epoch escaping.
Authentic immutable parent account/storage reads are cached separately from committed writes;
take validates this cache without constructing a provider. Cache misses fail closed into repair,
which constructs an owner-local provider. The factory must also support that rare owner call.
Submit retains a matching ordered suffix and appends new candidates to its plan. Skips remove
speculative writes and invalidate readers; reordered plans start a new generation. A bounded
generation has max(1024, 4 × initial-window-length) indices and rolls over when full, retaining
the committed overlay but discarding advisory work. Live slots stay bounded by the submitted window.
Workers forward writes, invalidate affected readers and block on ESTIMATE dependencies.
The builder alone advances the committed prefix through on_commit, including inline execution,
fees and system changes. A returned result is only a proposal: a later choice without a commit
discards its writes. An unpredicted choice executes inline and preserves the remaining plan.
Store validation first repairs stale frontier work on the owner using an exact-prefix reader.
At most one subsequent failed owner-State validation is retried on a worker at the exact prefix;
a second failure or a wait timeout executes inline. Each take gets its own bounded wait budget.

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

Both execution modes share AtomicSchedule: per-position phases, invalidation counters, dependency
waits and compare/exchange claims. Builder plans have a fixed-capacity append-only slot array;
workers scan the lowest pending position and publish under that position's lock, never the owner
queue lock. Retiring a running slot prevents late publication without waiting for provider I/O.
Workers retain their base reader per epoch and reuse an EVM per generation. Panic/stop wakes waiters.

`take` checks recorded values against Store and repairs invalid frontier work on the owner.
It does NOT apply state: a builder commit-condition can still decline the proposal. Only
`on_commit` applies the owner's admitted, fee-rebased state and advances the frontier. A later
choice or submit removes a declined proposal's writes and invalidates readers without changing
Store; cancellation discards every uncommitted proposal. Mandatory owner-State validation remains
independent of this Store check. Unpredicted commits invalidate readers after a publication fence.
Feeder calls/take/on_commit belong to one owner; cancellation/reset may come from another thread.

Invalid-transaction outcomes retain their transaction error and every observation just like successful
executions. Builder workers do not assume the signed nonce is valid: nonce errors read the real
visible sender. Invalid outcomes publish no writes and defer no fees. Before reusing an error,
both Store and independent owner-State validation must pass; failed executions conservatively
require exact observed balances. A changed nonce, balance or fee parameter must retry, not skip
a transaction that is now valid. Unsupported/provider failures remain misses, never transaction
errors. Removing a write-free invalid slot must not invalidate unrelated readers.
Duplicate hashes in a submitted snapshot share one advisory slot at their first position;
resubmitting the same ordered unique candidates must retain completed work. Admission of repeated
choices still belongs to the owner, and a delivered/retired result cannot be consumed twice.

Owner validation still checks every observation: Store equivalence is not assumed. Cached Store
account checks borrow metadata under shard guards instead of cloning AccountInfo/bytecode.
Adjacent owner storage checks reuse the already-loaded account but still read every slot; the
lookup counter counts actual basic/storage calls. Rebasing loads account metadata only when it
will fetch non-created storage originals. Front-of-plan retirement pops instead of scanning K.

Tests must exercise stale nonce/storage rejection, admitted balance drift and fee rebasing,
identity mismatch, discarded work/reset, and a declined commit followed by another choice.
The offline gate compares the same deterministic fault stream's included hashes, full
receipts and BundleState against a sequential loop at every measured iteration.
