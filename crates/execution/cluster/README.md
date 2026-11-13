# base-execution-cluster

Clustered Base: state sharding for a group of co-trusted Base execution nodes. Each node stores
only some shards of the live state (accounts, storage, trie nodes, bytecode) and reads the rest
from peers, so that N nodes no longer need N full copies of the state database. This crate holds
the shard map, the sharded database wrapper, the peer read protocol, and the owner-side store that
answers peer reads.

Status: experiment. Not safe for production; peers are trusted and unauthenticated.

## Scope

Sharded: the MDBX tables `HashedAccounts`, `HashedStorages`, `AccountsTrie`, `StoragesTrie`,
`Bytecodes`. Base runs reth storage v2, so the trie tables are read and written through their
packed-key views (`PackedAccountsTrie`, `PackedStoragesTrie`). Everything else (block indexes,
stage checkpoints, static files, `RocksDB` history) is kept in full on every node.

Non-goals: Byzantine peers, history sharding, pipeline (backfill) sync on a clustered node,
historical state RPC older than the persisted tip, reth storage v1.

## Placement

- A shard is one value of the first byte of a hash: 256 shards.
- `HashedAccounts`, `HashedStorages`, `StoragesTrie`: shard = first byte of the hashed address,
  except for the storage of a split contract (below).
- `AccountsTrie`: shard = first two nibbles of the trie path (byte 0 of the packed key). Paths
  shorter than two nibbles (the root and its 16 children) are **replicated**: stored and written
  on every node.
- `Bytecodes`: shard = first byte of the code hash.
- Owners of a shard: the `R` storage nodes with the highest rendezvous score
  `keccak(shard || node_id)`. `R >= 2` by default, so one node can be down. A node with
  `storage = false` owns nothing (a stateless node) but still holds the replicated keys.
- Adding or removing a node moves only the shards whose top-`R` set changes.
- The packed `AccountsTrie` key is 33 bytes: 32 bytes of packed nibbles, then the nibble count.
  A key is replicated when that count is below 2. `node_id` is a `u64`, hashed as 8 big-endian
  bytes after the shard byte.
- **Split contracts** (large contracts listed in `split_storage`): their storage rows are placed
  one by one by subkey, so one contract's storage spreads over all 256 shards. A
  `HashedStorages` row is in the shard of the first byte of its hashed slot. A `StoragesTrie` row
  is placed by its trie path exactly as an `AccountsTrie` key is: the first two nibbles, and
  paths shorter than two nibbles (the contract's storage root and its 16 children) are
  replicated. The part of a split contract in one shard is a **range**; a node holds the ranges
  of the shards it owns. Rows of every other contract are placed by hashed address as above.
  Reads, writes, shed and the owner store all use this one placement, per key and subkey.
- `split_storage` lists 20-byte addresses (hashed with keccak at load) or 32-byte hashed
  addresses (used as they are), told apart by length; any other entry is rejected. Absent or
  empty: no contract is split.
- A node reads the placement from a TOML file. The file is rejected when node ids repeat,
  `node_id` is not listed, `R` is 0 or larger than the number of storage nodes, or the history
  window `W` (`retention_blocks`, see [Owner side](#owner-side)) is below 160, or `apply_from`
  (see [Apply mode](#apply-mode)) lists an unknown node or this node, or is set on a node that
  stores shards, or a `split_storage` entry is neither 20 nor 32 bytes, or `read_retry_secs`
  (see [Read rule](#read-rule)) is 0. Every node must list the same `split_storage` and the same
  `retention_blocks`; this is not checked (see [Limits](#limits)).

```toml
node_id = 1          # this node
replication = 2      # R, optional, default 2
retention_blocks = 30000  # W, optional, default 30000; the same on every node
apply_from = [2]     # optional, default [] (execute); only on a node with storage = false
split_storage = ["0x4200000000000000000000000000000000000016"]  # optional, default []
read_retry_secs = 60 # optional, default 60; see Read rule

[[nodes]]
id = 1
address = "10.0.0.1:9650"
storage = true       # optional, default true

[[nodes]]
id = 2
address = "10.0.0.2:9650"
```

## Read rule

A clustered node reads three kinds of rows differently:

- **Trie rows** (`AccountsTrie`, `StoragesTrie`) are derived data. Every read of them is local and
  sees only the rows this node holds: rows of the shards it owns (for a split contract, the
  ranges it owns) and the replicated short paths. Every other row, including a row of a shard it
  does not own that shed has not deleted yet, is invisible: never read from a peer and never an
  error. This holds in every transaction, read-only or read-write.
- **Hashed state rows** (`HashedAccounts`, `HashedStorages`) read on a thread inside a
  [`LocalReads`] scope follow the same local, placement-filtered rule. The trie computation of a
  block holds this scope, because it walks ordered ranges and normally seeks into and past ranges
  the node does not hold.
- **Hashed state rows read outside that scope**, in a read-only transaction, are read locally
  when this node holds them and otherwise from an owner as **exact values** at the transaction's
  snapshot. That snapshot is the node's persisted tip `P`: the `Finish` stage checkpoint visible
  in the same MDBX transaction. reth layers its in-memory blocks above `P` itself, so nothing in
  reth needs to know the read was remote.

`Bytecodes` are content-addressed and read as before: local when held, otherwise an exact read
from an owner. Read-write transactions read the hashed tables and `Bytecodes` from local MDBX
only (see [Write rule](#write-rule)).

Consequences:

- Every remote read carries `P` (block number, and hash when known). The owner answers as of `P`,
  never as of its own head.
- Results are never cached beyond one cursor position.
- The owner refuses when it cannot reproduce `P` (it is behind, `P` is below its history, or it
  is on a different fork). The asker then tries the next owner, and retries for a bounded time
  when every owner is merely behind.
- An owner behind `P` is normal while the cluster follows the chain: the node reading may have
  persisted a block or two before the owner received it. The asker waits for it, retrying every
  20 ms for up to `read_retry_secs` (cluster config, default 60 seconds, 0 is rejected). An owner
  whose lowest servable block is above `P` is not waited for: that refusal is permanent. A read
  that waits more than 1 second logs one warning, `cluster read waiting for owners behind the
  requested block`, with the `table`, the requested `block`, each owner's highest servable block
  (`owners_highest`) and the time `waited`, and is counted in `waits` of the remote-read log
  line. Running out of time is an error, never "absent".
- `Bytecodes` are the exception: an owner answers from its newest data without reproducing `P`.

### Local filtered reads

The visible rows are decided from the placement alone, never from what local MDBX happens to
contain, so a node whose shed has not finished reads exactly as a shed node does. Ordered
operations (`first`, `seek`, `next`, `seek_exact`, `seek_by_key_subkey`, `next_dup`, walks)
behave like a database that contains only the visible rows, including the cursor states below.

Shards are contiguous key ranges (the first key byte), so a walk that meets a row of a shard the
node does not own seeks once to the start of the next shard instead of stepping over that
shard's leftover rows. In every shard the replicated `AccountsTrie` paths of that byte range (the
root in shard 0, the one-nibble path `n` in shard `n0`) sort before every sharded key and stay
visible. A split contract is filtered the same way one level down: its ranges are contiguous
subkey ranges (the first subkey byte), a duplicate in a range the node does not own makes the
walk seek to the next range, and its replicated storage-trie paths stay visible. A value's subkey
is encoded to place it only for a split contract; every other row is placed by its key.

### Remote exact reads

Outside a [`LocalReads`] scope, in a read-only transaction, the hashed tables answer as follows
for rows this node does not hold:

- `HashedAccounts` `get`, `get_by_encoded_key` and `seek_exact`: the account at `P` from an owner
  (`AccountValue`).
- `HashedStorages` `seek_by_key_subkey(address, slot)`: the value of exactly that slot at `P`
  from the owners of the row (`StorageValue`), returned only if that slot exists. An absent slot
  returns nothing even when a later slot exists: reth's callers accept only an exact match. For a
  split contract the owners of the slot's range answer, and a slot in a range this node owns is
  read locally with the same exact-match rule.
- A first-row probe of an account's storage (`get`, `seek_exact(address)`, or
  `seek_by_key_subkey(address, 0)`) asks an owner whether the account has any storage at `P`
  (`HasStorage`): none returns nothing; some is an unsupported-operation error naming the
  account (it is the start of a walk over a destroyed account's storage, which cannot happen
  after Cancun). A probe of a split contract is that error too: no single owner holds all of it.
- Every other operation that would read a row this node does not hold is an
  unsupported-operation error: `next` or `next_dup` from a remote answer, `first`, `seek` or
  `next` reaching a shard this node does not own (a walk inside owned shards is local), and
  `next_dup` of a split contract.

Rows this node holds are read from local MDBX with MDBX's own semantics.

### Cursor operations

| Operation | `HashedAccounts`, `AccountsTrie`, `Bytecodes` | `HashedStorages`, `StoragesTrie` |
|---|---|---|
| `get`, `get_by_encoded_key`, `seek_exact` | yes | yes (first visible duplicate; a probe outside the scope) |
| `current` | yes | yes |
| `first`, `seek`, `next`, forward walks | yes (outside the scope only inside owned shards) | error |
| `seek_by_key_subkey`, `next_dup`, `next_dup_val`, `walk_dup` with a key | | yes (outside the scope see above) |
| `last`, `prev`, reverse walks, `prev_dup`, `last_dup`, `next_no_dup`, `walk_dup` without a key | error | error |

"Error" means a `DatabaseError` on every clustered read-only cursor of that table, even when the
node holds the key, so behaviour does not depend on placement. No live read path needs these
operations.

Cursor states match MDBX: a fresh cursor's `next` is `first`; after a positioning call finds
nothing, `current` and `next` return nothing; after `next` or `next_dup` runs past the last
visible entry, `current` still returns that entry. After a write through a read-write cursor,
`next`, `next_dup` and `current` continue from wherever MDBX left the cursor, skipping invisible
rows.

A remote failure is a `DatabaseError`, never "absent".

## Write rule

In a read-write transaction, a write (`put`, `upsert`, `insert`, `append`, `append_dup`) to a key
this node does not hold is dropped. Reads of the hashed tables and `Bytecodes` are local and
unfiltered; reads of the trie tables are local and filtered (see [Read rule](#read-rule)). Local
MDBX never holds a value for a dropped key once shed has run, and before that only a stale one;
forward persistence of new blocks only reads state tables to decide between insert, update and
delete of the same key, whose write is dropped anyway, so this is sufficient there. Deletes run
locally (a stale key is simply removed).

A write of a duplicate of a split contract is kept or dropped by the duplicate's own placement:
its subkey is the start of the encoded value. Whole-contract operations act on the rows this
node holds: `delete_current_duplicates` on a split contract deletes its local rows only. reth's
live persistence of the sharded tables never uses it (only `PlainStorageState`, which is not
sharded, is wiped that way); a wiped storage reaches the hashed tables as one zeroed slot per
row, each placed by its subkey.

It is not sufficient for anything that computes from state inside a write transaction:

- Genesis initialisation and pipeline stages must run on an unclustered (pass-through) database.
- A reorg that reaches persisted blocks recomputes trie reverts from state reads and would
  corrupt the replicated trie nodes. A clustered node stops instead and must be re-cloned.

Both are enforced at commit. Live persistence advances every stage checkpoint to the same block
in the transaction that advances `Finish`, so a read-write commit is refused with a
`DatabaseError` (nothing is written) when:

- the `Headers` or `Bodies` checkpoint is ahead of `Finish`: reth's staged pipeline is running;
- `Finish` is below the last committed `Finish`: blocks are being unwound.

## Operating requirements

A clustered node refuses to start unless:

- `--engine.num-state-masking-blocks` is 0 (Base's default). With masking, the state tables lag
  the `Finish` checkpoint and `P` no longer identifies their contents.
- `--debug.skip-state-root` is not set.
- The datadir is initialised: genesis is written (a `Finish` checkpoint exists).
- reth's account and storage changesets are kept for at least `W` blocks (Base's full node
  default keeps them); an owner can serve history only as far back as they reach.

A node is clustered when started with `--cluster.config <FILE>` (`base rpc` and the `node`
command); without it the database is a pass-through to MDBX. At startup a clustered node checks
the requirements above before opening anything, opens the clustered database, starts its owner
server on its configured address, launches, and sheds in the background. Once its provider
exists it installs the block-hash lookup and the changeset reader (which builds the owner's
history index, see [Owner side](#owner-side)) and feeds every canonical-state notification to its
owner. It logs the shards it owns and shed progress, and counts remote reads and failures in the
metrics `base_cluster_remote_reads` and `base_cluster_remote_read_failures`. Every 10 seconds in
which any remote read happened it logs `cluster remote reads` with the reads of each kind
(`accounts`, `storages`, `storage_probes`, `bytecodes`), the `failures`, the reads that waited
more than 1 second on owners behind the requested block (`waits`), and the total time spent in
them (`elapsed`) since the previous line; `ClusterNode::remote_reads` returns the same counts
since start.

The block-hash lookup and the changeset reader hold the provider only weakly; the feed task holds
it strongly. The provider owns the clustered database, which owns the node, so a strong
reference would keep the node and its owner listener alive forever. Once the feed task stops (at
shutdown) the lookup knows no block and the reader reads nothing.

## Owner side

An owner answers exact reads of the rows it holds (`AccountValue`, `StorageValue`, `HasStorage`)
as of any block `P` inside its history window, from three sources:

- its MDBX database, at its persisted tip `T`;
- a **history index** of the rows it holds, keyed by hashed key (hashed address; hashed address
  and hashed slot), holding for every change of the row in blocks `(L, I]` the block and the
  row's **previous** value. It is built only from reth's persisted account and storage
  changesets (plain keys, which the owner hashes): at startup for the last `W` blocks up to `T`,
  and at every persistence commit for the newly persisted blocks. Entries of blocks at or below
  `T - W` are pruned. `L` is the real lowest block covered: `T - W` or, when history on disk is
  shorter, the block before the lowest one whose changesets are on disk;
- an **in-memory chain** of the canonical blocks above `T`, each with its number, hash, parent
  hash and the **new** values of its hashed state, taken from canonical-state notifications.
  Blocks at or below `T` are dropped at each persistence commit.

The value of row `k` at `P`, with `T` and the database row read in **one** MDBX read transaction
and the index and chain consulted under **one** lock:

- `P <= T`: needs `L <= P` and `I >= T`. The answer is the previous value of `k`'s first change
  in `(P, T]`; when `k` did not change there, the database row. Index entries above the
  transaction's `T` are ignored, so persistence advancing between the transaction and the lookup
  changes nothing. `P`'s hash must be the owner's canonical hash for that number.
- `P > T`: needs the chain to hold every block `T + 1 ..= P`, each the child of the one before
  (block `T + 1` of the canonical block `T`) and each with its hashed state. The answer is the new
  value of `k`'s last change in `(T, P]`; when `k` did not change there, the database row. `P`'s
  hash must be the chain's hash at `P`.
- `HasStorage` is answered from the same sources: the account has storage at `P` when a slot
  changed in the window has a nonzero value at `P`, or the database holds a slot that did not
  change there.

Refusals:

- `P` below `L`: `SnapshotUnavailable` with the servable range, whose lowest block is above `P`.
  The asker does not wait for it.
- A gap in the chain (a missed notification), a block without hashed state, an index that has
  not reached `T` (the changesets of a committed block could not be read yet), or no index yet:
  `SnapshotUnavailable` whose highest block is below `P` (or with no range): the owner is behind
  and the asker retries.
- A hash for `P` that differs from the owner's: `WrongFork`.
- A row the owner does not hold: `NotOwner`.

A notification of a block at a number already in the chain replaces that block and drops every
block above it; a reorg notification drops the reverted blocks and adds the new ones (a reorg of
in-memory blocks). A reorg that reverts a block at or below `T` cannot be served: the owner stops
serving state reads for good (every read is `WrongFork`), as the node cannot survive it either
(see [Write rule](#write-rule)). A hashed post-state carries no
"wipe storage" marker: a wiped storage is a zeroed slot per row, which the chain applies like any
other change.

Persistence and the index move together: the commit that advances `Finish` holds the owner's
lock across the MDBX commit and the reading of the new blocks' changesets, so a read that sees
the new `T` also sees the index extended to it. A read whose transaction predates a commit
answers `P <= T` from the index as above and, for `P > T`, opens a fresh transaction, because
the chain no longer holds the blocks it would need. Every read opens its own short read
transaction; the owner holds no long-lived one.

At startup the owner logs one line, `cluster owner history built`, with the shards it owns, the
index's lowest and highest block and its entry count. Every 10 seconds it logs `cluster owner`
with the reads `served`, refused as `behind`, and otherwise `refused` since the previous line,
the index `entries`, and the `lowest` and `highest` servable block.

Serving never issues a remote read: an owner serves only rows it holds.

## Wire protocol

Keys and values travel as the encoded table bytes MDBX stores.

| Request | Response |
|---|---|
| `AccountValue { address }` | the encoded account at `P`, or absent |
| `StorageValue { address, slot }` | the encoded storage entry of exactly that slot at `P`, or absent |
| `HasStorage { address }` | whether the account has any storage at `P` |
| `Status` | the owner's servable range of state reads, or none |
| `Get { table: Bytecodes, key }` | the bytecode, or absent |
| `ExecutedBlock { hash }` (see [Apply mode](#apply-mode)) | the block's encoded execution result |

Every request carries the snapshot `P` and a table (`HashedAccounts` or `HashedStorages` for the
exact reads; ignored by `Status` and `ExecutedBlock`). Errors: `SnapshotUnavailable` (with the
range the owner can serve), `WrongFork`, `NotOwner`, `Unsupported`, and `NotAvailable` for
`ExecutedBlock`. The new requests and responses are appended to the end of the enums, so the
encoding of the older ones is unchanged; an owner refuses the older ordered and trie requests
(`Get` of a table other than `Bytecodes`, `SeekGe`, `SeekDupGe`) with `Unsupported`, so a node of
the previous version fails loudly. Mixed versions are not supported: upgrade every node together.

- `HasStorage` of a split contract is `Unsupported`; a `StorageValue` of a split contract is
  served by the owners of the slot's range.
- `Bytecodes`: an owner that lacks the code and has not persisted up to `P` refuses with
  `SnapshotUnavailable`, advertising its persisted tip as its highest block, so an asker ahead of
  it retries until the owner persists the code.
- Each message is a 4-byte big-endian length followed by the bincode (standard configuration)
  encoding of the request, or of the response-or-error. An owner that fails internally (a
  database error) closes the connection, and the asker treats it like an unreachable owner.
- The asker tries the owners in rendezvous order, skipping itself. It retries the round, for a
  bounded time, only when every owner was unreachable or behind (`SnapshotUnavailable` with no
  range or a range below `P`) and at least one was behind. Any other refusal is returned.

Transport is blocking sockets with a small connection pool, because reads happen inside
synchronous cursor calls on engine and trie worker threads. Owners serve from their own threads.
The owner's accepting thread polls its listener every 10 ms instead of blocking in `accept`, so
stopping the server never depends on a connection arriving.

```mermaid
sequenceDiagram
    participant E as reth (execution, RPC)
    participant A as ClusteredDatabase (asker)
    participant O as Owner
    E->>A: storage cursor seek_by_key_subkey(address, slot)
    A->>A: row held? yes: local MDBX
    A->>O: StorageValue{P, address, slot}
    O->>O: one MDBX read (T, row), index (P, T] or chain (T, P]
    O-->>A: value | absent | SnapshotUnavailable
    A-->>E: entry only if it is exactly that slot
```

## Apply mode

A node in apply mode does not execute the blocks its consensus layer delivers: it takes each
block's execution result from a peer that executed it, checks it against the header, and inserts
it as an executed block. Persistence, canonical notifications, the owner feed, the pool and RPC
then see an executed block as usual. It saves the execution's remote reads on a node that stores
few or no shards.

- Roles: an **executor** executes blocks as today and serves their results; an **apply node**
  asks executors for results. Only a node that stores nothing may apply: an executor's result
  carries only the executor's trie rows. Every clustered node serves the results of blocks it
  inserted (executed or applied), so any node can be an executor.
- Config: `apply_from = [ids]` at the top level of the cluster TOML lists the nodes this node
  asks, in order. Absent or empty: the node executes. An id that is not a cluster node, or is
  this node, is rejected, and so is `apply_from` on a node with `storage = true`.
- Executor: keeps the results of its 64 most recent inserted blocks, keyed by block hash, filled
  from the engine's block-added event (the earliest point a complete result exists, before
  forkchoice). A result holds the receipts, requests, gas used, blob gas used, the bundle state
  with reverts, the transaction senders, and the block's sorted hashed state and trie updates.
  One worker encodes results in event order, so a result never evicts a newer one; at most 64
  inserted blocks wait for it. A block inserted while 64 wait is not kept (apply nodes asking
  for it execute it) and is counted in `base_cluster_dropped_results`. A result that would not
  fit in a response frame (`Frame::MAX_LEN`) is not kept. The worker stops with the event stream.
- Wire: `Get` gains a sibling query `ExecutedBlock { hash }` on the owner port (its snapshot and
  table are ignored). The executor answers the encoded result, waiting up to 250 ms for a block
  it has not inserted yet, or refuses with `NotAvailable`. Other requests keep their encoding.
- Apply node, for each new payload: convert the payload and run the header, header-against-parent
  and pre-execution body checks, as execution does; ask the `apply_from` nodes in order before
  one deadline 400 ms from the first request; then check the result: it carries no execution
  requests (Base produces none, and reth's post-execution checks do not read them), and it passes
  the post-execution checks execution runs: gas used, receipts root and logs bloom (reth's
  consensus), and Base's message-passer storage root and block-time checks against the result's
  hashed state.
- Fetch deadline: the deadline bounds every connect, write and read of every node asked, each
  socket operation waiting only the time left, and an answer completed after it is a miss
  (`unavailable`). A pooled connection gets its usual timeouts back afterwards.
- Trusted, not checked: the state root (the hashed state and trie updates are not recomputed),
  the bundle state, and the senders (signatures are not recovered).
- Fallback: when no result arrives in time, a peer is unreachable, the result does not decode or
  fails a check, the payload carries a block access list, or the parent's header or state is not
  available (a parent off the canonical chain), the node logs a warning and executes the block
  exactly as today. Apply mode never stalls a node and never changes what it accepts. Blocks
  downloaded from the network (not new payloads) are always executed.
- Counters: applied blocks, fallbacks by reason (`unavailable`, `unreachable`, `invalid`,
  `ineligible`) and time spent fetching, in `ClusterNode::applied_blocks`, the metrics
  `base_cluster_applied_blocks` and `base_cluster_apply_fallbacks{reason}`, and a `cluster apply`
  log line every 10 seconds in which a block was applied or fell back.

Limits: the fetch runs on the engine thread, so a slow executor delays forkchoice handling by up
to the 400 ms deadline per block. Two nodes that apply from each other only wait for each other and
then execute. An apply node does not warm reth's execution cache, so a fallback execution starts
cold.

## Bootstrap

Start from a full datadir (genesis initialised or snapshot restored on a pass-through database).
Shedding deletes every row of a sharded table that the node does not hold, including the ranges
of split contracts in shards it does not own. It runs on raw MDBX in
the background after launch, in batches of bounded size, each its own short write transaction so
the engine's persistence writer never waits long. Reads do not depend on it (see
[Local filtered reads](#local-filtered-reads)), and writes never add such keys, so it is idempotent and a node stopped
mid-shed resumes on its next start. Scale-up copies a shard from an owner at a persisted tip;
scale-down sheds it.

### Filtered copy

`base cluster bootstrap --from <full datadir> --cluster.config <FILE> --to <new datadir>` writes
a clustered node's state database directly from a full node's, instead of copying the whole
datadir and shedding. `--cluster.config` and `--to` repeat, paired by position, to write several
nodes' databases in one pass over the source. For each node it creates `<to>/db` holding every
table of `<from>/db` that reth's schema declares (`Tables::ALL`): unsharded tables row for row,
and for the five sharded tables only the rows that node holds by [placement](#placement):
replicated `AccountsTrie` paths are kept, and a split contract's rows are placed one by one by
subkey, its replicated storage-trie paths kept. The result is identical, row for row and byte for
byte, to a full copy on which that node's shed has finished, so a node started on it behaves
exactly as one started on a full copy.

- The source must be a full node that is stopped. The command refuses when reth's `db/lock` file
  names a live process, and holds that lock while it copies, so a node cannot start on the
  source meanwhile (it creates and removes the lock file; no table of the source changes). It
  also refuses when the source has no `Finish` stage checkpoint, when a `<to>/db` exists and is
  not empty, and when a config does not list its `node_id`; then nothing is created.
- The pairs are refused when their counts differ, when two share a `--to` path or a `node_id`,
  or when the configs differ in anything but `node_id`, `apply_from` and `read_retry_secs` (node
  list, replication, retention, split contracts), since the nodes would not agree on placement.
- A row of a sharded table that cannot be placed (a key or split-contract subkey of the wrong
  encoding) stops the copy with an error; nothing is dropped silently.
- Rows move as raw bytes in source order, in MDBX append mode. The source is read in parallel,
  because a cold read is bound by the disk's random-read latency: one cursor walk faults in one
  page at a time, while many concurrent walks keep the disk busy. Each table is split into
  partitions of consecutive rows, about 4 mebibytes of the table's pages each (from MDBX's page
  counts). Partition boundaries are 4-byte key prefixes spaced evenly over the key space.
  Each `split_storage` contract in a duplicate-key sharded table gets as many partitions again,
  at subkey prefixes; its first partition starts at the contract's first row, so rows that sort
  before the first prefix (short storage-trie paths) are kept. `--read-threads` reader threads
  (default 16; 1 reads with a single thread) each hold their own read transaction and cursor on
  the source (long-read safety disabled; the source is stopped). They claim partitions in order,
  at most `--read-threads` at once counted from the one being written, and place each row once.
  One thread takes the partitions strictly in order and hands their rows, in shared batches of
  up to 16 384 rows or 1 mebibyte, to one writer thread per destination, which appends those its
  node holds. So every destination receives the rows in exactly source order, and a run uses
  the readers, that thread, and one thread per destination.
- Memory is bounded: each partition being read queues at most 8 batches, so at most
  `read threads x 9` batches are read ahead (about 200 mebibytes at 16 threads, since a
  batch is at most about 1.4 mebibytes with its row index), plus 8 batches queued per writer.
  The slowest destination sets the pace.
- Parallelism needs keys spread over the key space. A table keyed by a big-endian block number
  is one partition and is read by one thread. A giant contract that is not in `split_storage`
  lies in one partition: one thread walks it, and while it is being written the other readers
  can read at most the window ahead and then wait. Use the storage report (below) to find such
  contracts and list them before bootstrapping.
- Each destination commits once the rows or bytes written to it since its last commit reach
  100 000 rows or 256 mebibytes, with MDBX's `SafeNoSync` sync mode, and all are synced to disk
  once at the end. The first error of any writer, else of a reader, stops every thread of the
  run, and a panicking thread's panic is propagated. An interrupted run leaves partial
  databases: delete every `<to>/db` and run again; there is no resume.
- It logs each table at start and finish (rows read, and rows written per destination) and every
  10 million rows of a large table, and prints a summary per destination: rows and bytes of each
  sharded table kept against the source and of the unsharded tables together; then the elapsed
  time and the contracts with the most `HashedStorages` rows (see below).
- Only `<to>/db` is written. The operator provides the rest of the datadir from the stopped
  source: `static_files` and `rocksdb` copied in full (they are not sharded), and `reth.toml` and
  any other node files as for any copied datadir. Consensus-layer files live outside the
  execution datadir and are handled as for any new node.
- `split_storage` must be final before bootstrapping: rows of split contracts in ranges the node
  does not own are never copied.

### Choosing split contracts

`base cluster storage-report --from <datadir> [--top N] [--read-threads T]` walks
`HashedStorages` of `<datadir>/db` once, read-only, and prints the `N` (default 20) hashed
addresses with the most rows, each with its row count and share of the table, then the table's
total, and last a ready `split_storage = [...]` line listing the hashed addresses that hold at
least 1% of the rows. `T` threads (default 16) count the table's partitions (as in the
bootstrap) in any order. A thread that sees one contract reach 1 000 000 rows hands the
contract's values with a greater first byte to the other threads, one range per byte, and the
pieces are summed, so no single thread walks a giant contract. The counts are the same as a
single walk's. It takes no lock; on a running node its long read transactions keep MDBX from
reusing pages until it finishes, so prefer a stopped node.

## Limits

- A clustered node can only follow the chain live. It must be re-cloned when it falls behind by
  more than the engine's backfill threshold, or when a reorg reaches persisted blocks.
- Nodes must stay within `W` blocks of each other: a node whose persisted tip is more than `W`
  blocks below every owner of a shard it reads cannot read that shard (the refusal is
  permanent). Any restart order works inside that window, because owners rebuild their history
  from disk. `W` must be the same on every node; nothing checks it. The index costs memory in
  proportion to the rows changed in `W` blocks.
- A read-only transaction whose `P` falls more than `W` blocks behind the owners (a long trace,
  for example) fails on its next remote read.
- One network round trip per remote exact read.
- Trie rows are never read from a peer, so anything that needs trie rows of shards this node does
  not own (`eth_getProof`, storage roots of foreign accounts, witnesses) fails or answers from the
  visible rows only. The state root of a block is not computed from foreign rows by this crate.
- An owner answers a bytecode only once it has persisted it, so reading code from a block this
  node persisted before every owner of the code's shard did waits for an owner to persist it,
  and fails if none does within the client's retry budget (`read_retry_secs`, 60 s by default).
- A read that runs out of `read_retry_secs` while owners stay behind fails. Inside transaction
  execution reth retries the block, but when the failure lands in the pre-execution system calls
  (the block-hash contract call, for example) or the post-execution checks, reth records the
  block as invalid, rejects every descendant, and the node needs a restart.
- An owner checks a block hash with the node's provider on each read below its persisted tip.
- `entries` (table statistics) of a sharded table counts local rows only.
- Shedding frees pages inside MDBX but does not shrink the file without a compacting copy.
- `split_storage` must be identical on every node. Nothing checks it: a node with a different
  list asks the wrong owners and reads wrong data. Changing the list (on every node) needs a
  re-clone from a full datadir, because rows already shed under the old placement are gone.
- Split contracts are chosen by the operator; nothing detects large contracts or rebalances.
- A storage wipe of a split contract is applied per row and per range like any other deletion.
  A wipe of an existing contract cannot happen after Cancun, so this cost is not optimised.

## Trie maintenance

A clustered node does not compute the full state root of a block. It keeps its trie slice: the
account-trie and storage-trie rows of the shards it holds, computed per block with reth's own
incremental state-root code over its database and in-memory ancestors, with prefix sets
restricted to what it holds, so unchanged and foreign subtrees are not walked. The engine's
state-root strategy (installed on every clustered node, see `ClusterStateRoot` in
`base-node-runner`) does this after execution and returns the block header's own state root with
the slice's trie updates; persistence then writes them through the [write rule](#write-rule).

**The header's state root is not verified when a block is inserted.** Execution, receipts root,
logs bloom, gas and the other header checks are unchanged; a block whose state root is wrong is
accepted. Detecting a bad state transition is the job of a separate check that compares the
header with hashes collected from the shards' owners (not built yet).

Per block, `TrieSlice::plan` turns the block's hashed post-state into one main computation and
zero or more side computations (`TrieSlicePlan`):

- Main computation: the account prefix set holds every changed account in a held shard, also
  when only its storage changed. A held account whose storage is not split gets all its changed
  slots as its storage prefix set; a split contract whose account is held gets only its changed
  slots in held ranges. Held accounts that became empty (`None`) are destroyed accounts. Its
  input state is exactly those held changes.
- Side computation, one per split contract whose account shard this node does not hold but whose
  held ranges changed: the walk computes a storage trie only when it reaches the account's leaf,
  which the main computation never does for a foreign account. Its input state is that contract
  with a placeholder account plus its changed slots in held ranges; its account prefix set is
  that contract, its storage prefix set those slots. Only that contract's storage-trie updates
  are kept.
- A split contract whose account became empty (its storage wiped) fails the block: per-range
  maintenance cannot delete the ranges other nodes hold. A wipe of an existing contract cannot
  happen after Cancun.
- A node that stores nothing (`storage = false`), and a storing node holding none of the block's
  changes, computes nothing and returns empty trie updates.

The replicated short paths (the account-trie root and its 16 children, and the same for each
split contract's storage trie) are written exactly as this node's own computations output them,
never taken from peers or from a side computation's account nodes.

Rows on disk are either meaningful or don't-care:

- Meaningful, byte for byte equal to a full node's: account-trie rows of two or more nibbles in
  held shards, the storage tries of held accounts whose storage is not split, the rows of held
  ranges of split contracts' storage tries, and in the replicated one-nibble nodes the hashes and
  mask bits of held children.
- Don't care: in replicated nodes, the hashes of children in shards this node does not hold (they
  stay as this node last wrote them and are reused as they are, so a branch keeps its 16
  children); the root nodes above them; and the path from a split contract's account leaf up to
  its shard's two-nibble node, since that leaf's storage root mixes in other nodes' ranges.
  Nothing may trust these values.

Counters: blocks whose slice was computed, side computations and the time spent, in
`ClusterNode::trie_slices`, the metrics `base_cluster_trie_slice_blocks` and
`base_cluster_trie_slice_side_calls`, and a `cluster trie slices` log line every 10 seconds in
which a block was computed or a check skipped.

### Message-passer storage root

Since Isthmus, Base checks after execution that the header's `withdrawals_root` equals the
storage root of the L2-to-L1 message passer. A clustered node compares it only when it holds
that account's shard and the contract is not in `split_storage`; otherwise it checks only that
the header carries the field, skips the comparison and counts it (`message_passer_skips` in
`ClusterNode::trie_slices`, metric `base_cluster_message_passer_root_skips`). This applies to
execution and to apply mode alike; the block-time checks are unchanged. A clustered node logs at
start-up whether it checks this root.

### Unsupported on clustered nodes

- Proofs (`eth_getProof`, witnesses) and the proofs-history extension: they read trie rows
  outside the held slice. Nothing refuses them yet; their answers are wrong.
- Building payloads: the payload builder computes a full state root over don't-care rows.
- `--debug.skip-state-root` is refused: the node already uses its own state-root strategy.

## Design record

Decided after a two-seat, two-pass design review on 2026-11-12.

- Rejected: "the owner may answer as of any block between the asker's persisted tip and its head".
  It is wrong for RPC at a non-head in-memory block, for side forks, and wherever reth reads the
  table under a deleted overlay entry.
- Rejected: a cluster-wide frozen or aligned persistence point. `P` is already fixed per
  transaction, and reth decides when to persist.
- Rejected: reusing reth's `OverlayStateProviderFactory` on the owner. Below the owner's own tip it
  computes reverts from database reads, which would touch shards the owner does not hold.
- Not verified by test at the time of writing: page-reuse cost of the held read snapshots, and
  that no Base execution extension on the node-run path reads state inside a write transaction.
- Decided on 2026-11-13 (values-only design, trie maintenance): no full state root per block on
  a clustered node, a custom state-root strategy inside reth's engine validator rather than a
  wrapper that re-builds the executed block (the strategy has reth's overlay of in-memory,
  possibly non-canonical, parents and keeps reth's deferred trie work).
- Rejected: deriving prefix sets with reth's `construct_prefix_sets`. It covers foreign shards
  and every range of a split contract, and it misses split contracts whose account is foreign.
- Side computations keep only the contract's storage-trie updates: their account nodes come from
  a walk through foreign shards and would overwrite this node's replicated nodes.
- A wiped storage is detected as a split contract's account becoming empty: reth's hashed
  post-state at this revision has no wipe flag (a destroyed account's slots arrive zeroed one by
  one).
- The message-passer skip counter is handed to Base's engine validator as a callback, so
  `base-node-core` does not depend on this crate.
