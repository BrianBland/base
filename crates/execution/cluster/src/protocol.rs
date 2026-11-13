//! Peer read protocol: requests, responses, errors, and their framed binary encoding.

use std::io::{self, Read, Write};

use alloy_primitives::{B256, Bytes};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{BlockShardHashes, JoinPoint, Shard, ShardHash, ShardedTable};

/// The block a read is answered as of: the asker's persisted tip `P`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotId {
    /// Block number of `P`.
    pub number: u64,
    /// Block hash of `P`, when the asker knows it.
    pub hash: Option<B256>,
}

/// A read of one sharded table as of a snapshot, or a request for a block's execution result
/// ([`Query::ExecutedBlock`], [`Query::ExecutedBlockAt`]) or an owner's servable range
/// ([`Query::Status`]), which ignore the snapshot and the table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    /// The block the read is answered as of.
    pub snapshot: SnapshotId,
    /// The table read.
    pub table: ShardedTable,
    /// What is read.
    pub query: Query,
}

/// The read operation. Keys, subkeys and values are the encoded bytes MDBX stores.
///
/// New variants are appended at the end so the encoding of the older ones never shifts. An owner
/// refuses [`Self::Get`] of a table other than `Bytecodes`, [`Self::SeekGe`] and
/// [`Self::SeekDupGe`] with [`ReadError::Unsupported`]: they are kept only so that a node of the
/// previous version is refused instead of misread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Query {
    /// The value of exactly `key`. Served for `Bytecodes` only.
    Get {
        /// Encoded key.
        key: Vec<u8>,
    },
    /// Up to `limit` entries with key `>= key`, ascending, within the key's shard. Refused.
    SeekGe {
        /// Encoded key to start from.
        key: Vec<u8>,
        /// Largest number of entries wanted.
        limit: u32,
    },
    /// Up to `limit` duplicate values of exactly `key` with subkey `>= subkey`, ascending.
    /// Refused.
    SeekDupGe {
        /// Encoded key.
        key: Vec<u8>,
        /// Encoded subkey to start from.
        subkey: Vec<u8>,
        /// Largest number of values wanted.
        limit: u32,
    },
    /// The execution result of block `hash`, encoded as an
    /// [`ExecutedBlockResult`](crate::ExecutedBlockResult). Answered by any node that inserted
    /// the block recently; the snapshot and table of the request are ignored.
    ExecutedBlock {
        /// Hash of the block.
        hash: B256,
    },
    /// The encoded account of hashed `address` at the snapshot. Table: `HashedAccounts`.
    AccountValue {
        /// Hashed address.
        address: B256,
    },
    /// The encoded storage entry of exactly hashed `slot` of hashed `address` at the snapshot.
    /// Table: `HashedStorages`.
    StorageValue {
        /// Hashed address.
        address: B256,
        /// Hashed slot.
        slot: B256,
    },
    /// Whether hashed `address` has any storage at the snapshot. Table: `HashedStorages`.
    HasStorage {
        /// Hashed address.
        address: B256,
    },
    /// The range of blocks the owner can answer state reads as of; the snapshot and table of the
    /// request are ignored.
    Status,
    /// The subtree hashes the node captured for block `number` with hash `hash`, answered with
    /// [`Response::ShardHashes`] or refused with [`ReadError::NotAvailable`]; the snapshot and
    /// table of the request are ignored.
    ShardHashes {
        /// Block number.
        number: u64,
        /// Block hash.
        hash: B256,
    },
    /// The hashes, at block `number` with hash `hash`, of the node's held account shards that
    /// contain split contracts, recomputed with the split contracts' `storage_roots`, answered
    /// with [`Response::SplitShardHashes`] or refused with [`ReadError::NotAvailable`]; the
    /// snapshot and table of the request are ignored.
    SplitShardHashes {
        /// Block number.
        number: u64,
        /// Block hash.
        hash: B256,
        /// `(hashed address, storage root)` of each split contract.
        storage_roots: Vec<(B256, B256)>,
    },
    /// [`Self::ExecutedBlock`] of a block also identified by its number, which lets a node that
    /// has already inserted a block at or above that number refuse at once with
    /// [`ReadError::NotRetained`] instead of waiting for a block that cannot arrive.
    ExecutedBlockAt {
        /// Hash of the block.
        hash: B256,
        /// Number of the block.
        number: u64,
    },
    /// The headers and block body a node that stores nothing needs to start at the answering
    /// node's persisted tip, answered with [`Response::JoinPoint`] or refused with
    /// [`ReadError::NotAvailable`]; the snapshot and table of the request are ignored (see
    /// [`JoinPoint`]).
    JoinPoint,
}

/// Entries of the refused [`Query::SeekGe`]; kept for the encoding of [`Response`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardEntries {
    /// Encoded `(key, value)` pairs, ascending by key.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// No entry of the shard follows the last returned one.
    pub end_of_shard: bool,
}

/// Duplicate values of the refused [`Query::SeekDupGe`]; kept for the encoding of [`Response`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicateValues {
    /// Encoded values (each starting with its subkey), ascending by subkey.
    pub values: Vec<Vec<u8>>,
    /// No value of the key follows the last returned one.
    pub end_of_key: bool,
}

/// A successful answer to a [`Request`]. New variants are appended at the end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    /// Answer to [`Query::Get`], [`Query::AccountValue`] and [`Query::StorageValue`]: the encoded
    /// value, or `None` when absent.
    Value(Option<Vec<u8>>),
    /// Answer to [`Query::SeekGe`].
    Entries(ShardEntries),
    /// Answer to [`Query::SeekDupGe`].
    Duplicates(DuplicateValues),
    /// Answer to [`Query::ExecutedBlock`] and [`Query::ExecutedBlockAt`]: the encoded execution
    /// result.
    ExecutedBlock(Bytes),
    /// Answer to [`Query::HasStorage`].
    HasStorage(bool),
    /// Answer to [`Query::Status`]: the servable range, or `None` when the owner can serve none.
    Status(Option<ServableRange>),
    /// Answer to [`Query::ShardHashes`].
    ShardHashes(BlockShardHashes),
    /// Answer to [`Query::SplitShardHashes`]: each held account shard with split contracts whose
    /// storage roots were all supplied, and its hash.
    SplitShardHashes(Vec<(Shard, ShardHash)>),
    /// Answer to [`Query::JoinPoint`].
    JoinPoint(JoinPoint),
}

/// Inclusive range of blocks an owner can answer as of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServableRange {
    /// Lowest servable block number.
    pub lowest: u64,
    /// Highest servable block number.
    pub highest: u64,
}

/// An owner's refusal to answer a [`Request`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, Serialize, Deserialize)]
pub enum ReadError {
    /// The owner cannot reproduce the snapshot: it is behind, the snapshot is below its history,
    /// or it has a gap.
    #[error("snapshot unavailable, servable range {servable:?}")]
    SnapshotUnavailable {
        /// Blocks the owner can serve, or `None` when it can serve none.
        servable: Option<ServableRange>,
    },
    /// The owner's block at the snapshot's number has a different hash, or the owner stopped
    /// serving after a reorg of its persisted blocks.
    #[error("owner is on a different fork")]
    WrongFork,
    /// The key is in a shard the owner does not own.
    #[error("owner does not own the key's shard")]
    NotOwner,
    /// The query is not supported for the table, or a key is malformed.
    #[error("unsupported query")]
    Unsupported,
    /// The node has no execution result, or no captured hashes, for the requested block.
    #[error("not available for this block")]
    NotAvailable,
    /// Answer to [`Query::ExecutedBlockAt`]: the node has no execution result for the requested
    /// block and will not get one, because it has already inserted a block at or above its
    /// number.
    #[error("execution result not retained, retained blocks {oldest}..={newest}")]
    NotRetained {
        /// Lowest block number whose result the node keeps.
        oldest: u64,
        /// Highest block number the node has inserted.
        newest: u64,
    },
}

impl ReadError {
    /// Whether this refusal means the owner has not yet reached block `number`, so asking again
    /// later may succeed.
    pub fn is_behind(&self, number: u64) -> bool {
        match self {
            Self::SnapshotUnavailable { servable } => {
                servable.is_none_or(|range| range.highest < number)
            }
            Self::WrongFork
            | Self::NotOwner
            | Self::Unsupported
            | Self::NotAvailable
            | Self::NotRetained { .. } => false,
        }
    }
}

/// What an owner sends back for a [`Request`].
pub type ReadResult = Result<Response, ReadError>;

/// Why a frame could not be sent or received.
#[derive(Debug, Error)]
pub enum FrameError {
    /// The socket failed.
    #[error("frame io: {0}")]
    Io(#[from] io::Error),
    /// The message could not be encoded.
    #[error("frame encode: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    /// The bytes received are not a valid message.
    #[error("frame decode: {0}")]
    Decode(#[from] bincode::error::DecodeError),
    /// The frame exceeds [`Frame::MAX_LEN`].
    #[error("frame of {0} bytes exceeds the limit")]
    TooLarge(usize),
}

/// Length-prefixed message framing: a 4-byte big-endian length, then the bincode (standard
/// configuration) encoding of the message.
#[derive(Debug, Clone, Copy)]
pub struct Frame;

impl Frame {
    /// Largest accepted frame payload, in bytes.
    pub const MAX_LEN: usize = 64 << 20;

    /// Encodes `message` and writes it as one frame.
    pub fn write<T: Serialize>(writer: &mut impl Write, message: &T) -> Result<(), FrameError> {
        let mut frame = vec![0u8; 4];
        bincode::serde::encode_into_std_write(message, &mut frame, bincode::config::standard())?;
        let len = frame.len() - 4;
        if len > Self::MAX_LEN {
            return Err(FrameError::TooLarge(len));
        }
        frame[..4].copy_from_slice(&(len as u32).to_be_bytes());
        writer.write_all(&frame)?;
        Ok(writer.flush()?)
    }

    /// Reads one frame and decodes its message.
    pub fn read<T: DeserializeOwned>(reader: &mut impl Read) -> Result<T, FrameError> {
        let mut len = [0u8; 4];
        reader.read_exact(&mut len)?;
        let len = u32::from_be_bytes(len) as usize;
        if len > Self::MAX_LEN {
            return Err(FrameError::TooLarge(len));
        }
        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload)?;
        let (message, _) =
            bincode::serde::decode_from_slice(&payload, bincode::config::standard())?;
        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_oversized_frames_before_allocating() {
        let mut frame = ((Frame::MAX_LEN + 1) as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&[0; 8]);
        let result: Result<Request, _> = Frame::read(&mut frame.as_slice());
        assert!(matches!(result, Err(FrameError::TooLarge(_))));
    }

    /// The variant index bincode writes first, after the frame's length prefix.
    fn discriminant<T: Serialize>(message: &T) -> u8 {
        let mut frame = Vec::new();
        Frame::write(&mut frame, message).unwrap();
        frame[4]
    }

    /// Nodes of one version decode each other's messages by variant position, so a merge that
    /// reorders or inserts a variant must fail here rather than silently renumber the wire.
    #[test]
    fn variant_positions_on_the_wire_never_shift() {
        let hash = B256::ZERO;
        let queries = [
            (Query::Get { key: Vec::new() }, 0),
            (Query::SeekGe { key: Vec::new(), limit: 0 }, 1),
            (Query::SeekDupGe { key: Vec::new(), subkey: Vec::new(), limit: 0 }, 2),
            (Query::ExecutedBlock { hash }, 3),
            (Query::AccountValue { address: hash }, 4),
            (Query::StorageValue { address: hash, slot: hash }, 5),
            (Query::HasStorage { address: hash }, 6),
            (Query::Status, 7),
            (Query::ShardHashes { number: 0, hash }, 8),
            (Query::SplitShardHashes { number: 0, hash, storage_roots: Vec::new() }, 9),
            (Query::ExecutedBlockAt { hash, number: 0 }, 10),
            (Query::JoinPoint, 11),
        ];
        let responses = [
            (Response::Value(None), 0),
            (Response::Entries(ShardEntries::default()), 1),
            (Response::Duplicates(DuplicateValues::default()), 2),
            (Response::ExecutedBlock(Bytes::new()), 3),
            (Response::HasStorage(false), 4),
            (Response::Status(None), 5),
            (Response::ShardHashes(BlockShardHashes::default()), 6),
            (Response::SplitShardHashes(Vec::new()), 7),
            (Response::JoinPoint(JoinPoint::default()), 8),
        ];
        let errors = [
            (ReadError::SnapshotUnavailable { servable: None }, 0),
            (ReadError::WrongFork, 1),
            (ReadError::NotOwner, 2),
            (ReadError::Unsupported, 3),
            (ReadError::NotAvailable, 4),
            (ReadError::NotRetained { oldest: 0, newest: 0 }, 5),
        ];

        for (query, position) in queries {
            assert_eq!(discriminant(&query), position, "{query:?}");
        }
        for (response, position) in responses {
            assert_eq!(discriminant(&response), position, "{response:?}");
        }
        for (error, position) in errors {
            assert_eq!(discriminant(&error), position, "{error:?}");
        }
    }

    #[test]
    fn behind_means_no_range_or_a_range_below_the_snapshot() {
        let behind = |servable| ReadError::SnapshotUnavailable { servable }.is_behind(10);
        assert!(behind(None));
        assert!(behind(Some(ServableRange { lowest: 1, highest: 9 })));
        assert!(!behind(Some(ServableRange { lowest: 11, highest: 20 })));
        assert!(!ReadError::WrongFork.is_behind(10));
    }
}
