//! Peer read protocol: requests, responses, errors, and their framed binary encoding.

use std::io::{self, Read, Write};

use alloy_primitives::B256;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::ShardedTable;

/// The block a read is answered as of: the asker's persisted tip `P`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotId {
    /// Block number of `P`.
    pub number: u64,
    /// Block hash of `P`, when the asker knows it.
    pub hash: Option<B256>,
}

/// A read of one sharded table as of a snapshot.
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Query {
    /// The value of exactly `key`. Tables: `HashedAccounts`, `AccountsTrie`, `Bytecodes`.
    Get {
        /// Encoded key.
        key: Vec<u8>,
    },
    /// Up to `limit` entries with key `>= key`, ascending, within the key's shard.
    /// Tables: `HashedAccounts`, `AccountsTrie`.
    SeekGe {
        /// Encoded key to start from.
        key: Vec<u8>,
        /// Largest number of entries wanted.
        limit: u32,
    },
    /// Up to `limit` duplicate values of exactly `key` with subkey `>= subkey`, ascending.
    /// Tables: `HashedStorages`, `StoragesTrie`.
    SeekDupGe {
        /// Encoded key.
        key: Vec<u8>,
        /// Encoded subkey to start from.
        subkey: Vec<u8>,
        /// Largest number of values wanted.
        limit: u32,
    },
}

/// Entries returned by [`Query::SeekGe`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardEntries {
    /// Encoded `(key, value)` pairs, ascending by key.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// No entry of the shard follows the last returned one.
    pub end_of_shard: bool,
}

/// Duplicate values returned by [`Query::SeekDupGe`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicateValues {
    /// Encoded values (each starting with its subkey), ascending by subkey.
    pub values: Vec<Vec<u8>>,
    /// No value of the key follows the last returned one.
    pub end_of_key: bool,
}

/// A successful answer to a [`Request`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    /// Answer to [`Query::Get`]: the encoded value, or `None` when absent.
    Value(Option<Vec<u8>>),
    /// Answer to [`Query::SeekGe`].
    Entries(ShardEntries),
    /// Answer to [`Query::SeekDupGe`].
    Duplicates(DuplicateValues),
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
    /// The owner cannot reproduce the snapshot: it is behind, has discarded it, or has a gap.
    #[error("snapshot unavailable, servable range {servable:?}")]
    SnapshotUnavailable {
        /// Blocks the owner can serve, or `None` when it can serve none.
        servable: Option<ServableRange>,
    },
    /// The owner's block at the snapshot's number has a different hash.
    #[error("owner is on a different fork")]
    WrongFork,
    /// The key is in a shard the owner does not own.
    #[error("owner does not own the key's shard")]
    NotOwner,
    /// The query is not supported for the table, or a key is malformed.
    #[error("unsupported query")]
    Unsupported,
}

impl ReadError {
    /// Whether this refusal means the owner has not yet reached block `number`, so asking again
    /// later may succeed.
    pub fn is_behind(&self, number: u64) -> bool {
        match self {
            Self::SnapshotUnavailable { servable } => {
                servable.is_none_or(|range| range.highest < number)
            }
            Self::WrongFork | Self::NotOwner | Self::Unsupported => false,
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

    #[test]
    fn behind_means_no_range_or_a_range_below_the_snapshot() {
        let behind = |servable| ReadError::SnapshotUnavailable { servable }.is_behind(10);
        assert!(behind(None));
        assert!(behind(Some(ServableRange { lowest: 1, highest: 9 })));
        assert!(!behind(Some(ServableRange { lowest: 11, highest: 20 })));
        assert!(!ReadError::WrongFork.is_behind(10));
    }
}
