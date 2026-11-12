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
- `HashedAccounts`, `HashedStorages`, `StoragesTrie`: shard = first byte of the hashed address.
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
- A node reads the placement from a TOML file. The file is rejected when node ids repeat,
  `node_id` is not listed, or `R` is 0 or larger than the number of storage nodes.

```toml
node_id = 1          # this node
replication = 2      # R, optional, default 2

[[nodes]]
id = 1
address = "10.0.0.1:9650"
storage = true       # optional, default true

[[nodes]]
id = 2
address = "10.0.0.2:9650"
```

## Read rule

A read of a key this node does not own must return exactly what this node's own MDBX would have
returned at the transaction's snapshot if the node stored everything. That snapshot is the node's
persisted tip `P`: the `Finish` stage checkpoint visible in the same MDBX transaction. reth layers
its in-memory blocks above `P` itself, so nothing in reth needs to know the read was remote.

Consequences:

- Every remote read carries `P` (block number, and hash when known). The owner answers as of `P`,
  never as of its own head.
- Results may be cached for the lifetime of one transaction only.
- The owner refuses when it cannot reproduce `P` (it is behind, has discarded `P`, or is on a
  different fork). The asker then tries the next owner, and retries for a bounded time when every
  owner is merely behind.
- `Bytecodes` is the exception: entries are content-addressed and never change, so an owner
  answers from its newest data without reproducing `P`.

## Write rule

In a read-write transaction, a write to a key this node does not own is dropped, and a read of
such a key returns "absent". Forward persistence of new blocks only reads state tables to decide
between insert, update and delete of the same key, so this is sufficient there.

It is not sufficient for anything that computes from state inside a write transaction:

- Genesis initialisation and pipeline stages must run on an unclustered (pass-through) database.
- A reorg that reaches persisted blocks recomputes trie reverts from state reads and would
  corrupt the replicated trie nodes. A clustered node stops instead and must be re-cloned.

## Operating requirements

A clustered node refuses to start unless:

- `--engine.num-state-masking-blocks` is 0 (Base's default). With masking, the state tables lag
  the `Finish` checkpoint and `P` no longer identifies their contents.
- State roots are computed for every block, so every canonical block carries trie updates.

## Owner side

An owner answers "state at `P`" from:

- a held MDBX read snapshot at one of its own persistence points `S <= P` (long-read safety
  disabled for that transaction), plus
- the sorted hashed-state and trie updates of canonical blocks `(S, P]`, retained from
  canonical-state notifications and applied oldest to newest.

In those updates a deletion is always explicit and per key: an account or trie node of `None`, a
storage slot of zero. There is no "wipe storage" or "delete trie" marker to honour. reth's
in-memory cursor overlays (`HashedPostStateCursorFactory`, `InMemoryTrieCursorFactory`) already
implement this merge over a database cursor factory.

Retention: an owner keeps snapshots and per-block updates covering the last `W` blocks, `W >= 160`
(Base's persistence backpressure threshold). Each retained block is identified by number and hash;
an owner serves `P` only when its retained chain from `S` to `P` is unbroken. A block that arrives
without trie updates makes the owner unable to serve past it.

- The window is `[head - W + 1, head]`, where `head` is the newest block or persistence point the
  owner has seen. `P` outside it is refused with `SnapshotUnavailable`.
- A block or persistence point at an already retained number replaces it and drops everything
  retained above it (a reorg of in-memory blocks).
- `P` with a hash that differs from the owner's block at that number is refused with `WrongFork`.
  Any other way of failing to reach `P` (a gap, a block without trie updates, no snapshot at or
  below `P`) is `SnapshotUnavailable`. Its range is the lowest and highest block the owner can
  serve; the owner may still refuse blocks inside it.

Serving never issues a remote read: an owner serves only shards it owns.

## Wire protocol

Keys and values travel as the encoded table bytes MDBX stores, so the protocol is table-agnostic.

| Request | Tables | Response |
|---|---|---|
| `Get { table, key }` | `HashedAccounts`, `AccountsTrie`, `Bytecodes` | value or absent |
| `SeekGe { table, key, limit }` | `HashedAccounts`, `AccountsTrie` | up to `limit` entries with key `>=` the given key, ascending, within the key's shard; `end_of_shard` flag |
| `SeekDupGe { table, key, subkey, limit }` | `HashedStorages`, `StoragesTrie` | up to `limit` duplicate entries of exactly `key` with subkey `>=` the given subkey; `end_of_key` flag |

Every request carries the snapshot `P`. Errors: `SnapshotUnavailable` (with the range the owner can
serve), `WrongFork`, `NotOwner`, `Unsupported`. Iterating across keys of a duplicate-key table is
`Unsupported` remotely; no live read path needs it, and so is a key or subkey of the wrong length.

- The shard of a `SeekGe` is the shard of its key. Replicated `AccountsTrie` keys are never
  returned by `SeekGe` (every node holds them), but `Get` answers them.
- An owner may return fewer than `limit` entries without setting the end flag; the asker seeks
  again from after the last entry.
- `Bytecodes`: an owner that lacks the code and has not persisted up to `P` refuses with
  `SnapshotUnavailable` instead of answering absent.
- Each message is a 4-byte big-endian length followed by the bincode (standard configuration)
  encoding of the request, or of the response-or-error. An owner that fails internally (a
  database error) closes the connection, and the asker treats it like an unreachable owner.
- The asker tries the owners in rendezvous order, skipping itself. It retries the round, for a
  bounded time, only when every owner was unreachable or behind (`SnapshotUnavailable` with no
  range or a range below `P`) and at least one was behind. Any other refusal is returned.

Transport is blocking sockets with a small connection pool, because reads happen inside
synchronous cursor calls on engine and trie worker threads. Owners serve from their own threads.

```mermaid
sequenceDiagram
    participant E as reth (execution, trie, RPC)
    participant A as ClusteredDatabase (asker)
    participant O as Owner store
    E->>A: cursor.seek(key)
    A->>A: shard(key) owned? yes: local MDBX
    A->>O: SeekGe{P, table, key}
    O->>O: snapshot S <= P, merge updates (S, P]
    O-->>A: entries | SnapshotUnavailable
    A-->>E: entry (as if local at P)
```

## Bootstrap

Start from a full datadir (genesis initialised or snapshot restored on a pass-through database),
then `shed`: delete every key of a shard the node does not own. Scale-up copies a shard from an
owner at a persisted tip; scale-down sheds it.

## Limits

- A clustered node can only follow the chain live. It must be re-cloned when it falls behind by
  more than the engine's backfill threshold, when it restarts more than `W` blocks behind its
  peers, or when a reorg reaches persisted blocks.
- A read-only transaction older than `W` blocks (a long trace, for example) fails on its next
  remote read.
- One network round trip per remote seek until read-ahead batching is tuned.
- Shedding frees pages inside MDBX but does not shrink the file without a compacting copy.

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
