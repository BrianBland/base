//! Joining a cluster on an empty datadir: a node that stores no state starts at a peer's
//! persisted block instead of from a copied datadir.

use std::{
    io,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use alloy_primitives::{B256, Bytes, U256};
use alloy_rlp::Encodable;
use reth_db_api::{
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::{BlockHeader, FullBlockHeader, NodePrimitives, SealedHeader};
use reth_provider::{
    DatabaseProvider, ProviderError, PruneCheckpointWriter, StageCheckpointWriter,
    StaticFileProviderFactory, StaticFileSegment, StaticFileWriter,
    providers::NodeTypesForProvider,
};
use reth_prune_types::{PruneCheckpoint, PruneSegment};
use reth_stages_types::{StageCheckpoint, StageId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Cluster, ClusterClient, Frame, NodeId};

/// Returns the join point of the canonical headers of a range of block numbers
/// ([`JoinPoint::encode`]), or `None` when they cannot be read.
pub type JoinHeaderLookup = Box<dyn Fn(RangeInclusive<u64>) -> Option<JoinPoint> + Send + Sync>;

/// What a node that stores no state needs from a peer to start at the peer's persisted tip `P`:
/// the RLP-encoded canonical headers of [`Self::window`]`(P)`, ascending. The newest is `P`'s;
/// the older ones give the joined node the block hashes the `BLOCKHASH` opcode reads. A peer
/// still at genesis answers no header: the joining node then starts from genesis.
///
/// Nothing else is transferred. Transaction numbers restart at 0 above `P`, as after reth's
/// snap sync: they are local storage indexes that no peer compares. The replicated trie rows
/// (short account-trie paths, short storage-trie paths of split contracts) are not transferred
/// either: a node that stores nothing never computes a trie ([`crate::TrieSlice::plan`]), never
/// captures hashes for the state-root check, and refuses proof RPCs, so nothing reads them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinPoint {
    /// Encoded headers, ascending, the last at the answering node's persisted tip.
    pub headers: Vec<Bytes>,
}

impl JoinPoint {
    /// Headers in a join point: the block hashes the `BLOCKHASH` opcode can read.
    pub const MAX_HEADERS: u64 = 256;

    /// Bincode overhead allowed per header (its length prefix) and per message, for
    /// [`Self::fits_frame`].
    const HEADER_OVERHEAD: usize = 9;
    const MESSAGE_OVERHEAD: usize = 16;

    /// The block numbers of the join point at tip `tip`: the [`Self::MAX_HEADERS`] blocks ending
    /// at `tip`, never genesis.
    pub fn window(tip: u64) -> RangeInclusive<u64> {
        tip.saturating_sub(Self::MAX_HEADERS - 1).max(1)..=tip
    }

    /// The join point of `headers`, ascending.
    pub fn encode<H: Encodable>(headers: &[H]) -> Self {
        Self { headers: headers.iter().map(|header| alloy_rlp::encode(header).into()).collect() }
    }

    /// Whether the encoded answer carrying this join point fits in one frame
    /// ([`Frame::MAX_LEN`]), from an upper bound of its encoded size.
    pub fn fits_frame(&self) -> bool {
        let headers: usize =
            self.headers.iter().map(|header| header.len() + Self::HEADER_OVERHEAD).sum();
        headers + Self::MESSAGE_OVERHEAD <= Frame::MAX_LEN
    }

    /// Decodes and checks the headers: at most [`Self::MAX_HEADERS`], consecutive, each linked
    /// to its parent by hash, and the first after genesis linked to `genesis_hash`. `None` when
    /// there is none: the answering node is at genesis.
    pub fn decode<H: BlockHeader>(
        &self,
        genesis_hash: B256,
    ) -> Result<Option<JoinHeaders<H>>, JoinError> {
        if self.headers.len() as u64 > Self::MAX_HEADERS {
            return Err(JoinError::TooManyHeaders(self.headers.len()));
        }
        let mut headers: Vec<SealedHeader<H>> = Vec::with_capacity(self.headers.len());
        for encoded in &self.headers {
            let header = SealedHeader::seal_slow(H::decode(&mut encoded.as_ref())?);
            let parent = match headers.last() {
                Some(previous) => {
                    if header.number() != previous.number() + 1 {
                        return Err(JoinError::NotConsecutive {
                            previous: previous.number(),
                            next: header.number(),
                        });
                    }
                    Some(previous.hash())
                }
                None => (header.number() == 1).then_some(genesis_hash),
            };
            if header.number() == 0 || parent.is_some_and(|parent| header.parent_hash() != parent) {
                return Err(JoinError::BrokenLink(header.number()));
            }
            headers.push(header);
        }
        Ok((!headers.is_empty()).then_some(JoinHeaders { headers }))
    }
}

/// The checked headers of a [`JoinPoint`], the last at the join block `P`.
#[derive(Debug, Clone)]
pub struct JoinHeaders<H> {
    headers: Vec<SealedHeader<H>>,
}

impl<H: BlockHeader> JoinHeaders<H> {
    /// The join block `P`'s header.
    pub fn tip(&self) -> &SealedHeader<H> {
        self.headers.last().expect("decoded join headers are not empty")
    }

    /// The hash of block `number`, when it is in the join point.
    pub fn hash_of(&self, number: u64) -> Option<B256> {
        let first = self.headers.first()?.number();
        let index = usize::try_from(number.checked_sub(first)?).ok()?;
        self.headers.get(index).map(SealedHeader::hash)
    }

    /// Writes the join point into a datadir holding only genesis, in `provider`'s transaction;
    /// the caller commits. Afterwards the datadir starts as a node whose persisted tip is `P`:
    ///
    /// - the headers go to the header static files and `HeaderNumbers`; headers that would sit
    ///   between genesis and its static file's end are skipped, because a static file holds
    ///   consecutive blocks from its first;
    /// - every other static-file segment restarts after `P`, with history below `P` counted as
    ///   pruned ([`DatabaseProvider::anchor_pruned_static_files`]), and every prune checkpoint
    ///   says so;
    /// - every stage checkpoint, `Finish` included, is at `P`.
    ///
    /// Fails when `P` itself lies in genesis's header static file beyond the first
    /// [`JoinPoint::MAX_HEADERS`] blocks: those headers would have to start right after genesis.
    /// reth keeps one static-file writer per segment for the life of a static-file provider, so
    /// `provider` must come from a factory opened after genesis was written (whose header writer
    /// is not open on genesis's file); otherwise this fails with [`JoinError::HeaderFileInUse`].
    pub fn write<TX, N>(&self, provider: &DatabaseProvider<TX, N>) -> Result<(), JoinError>
    where
        TX: DbTxMut + DbTx + 'static,
        N: NodeTypesForProvider<Primitives: NodePrimitives<BlockHeader = H>>,
        H: FullBlockHeader,
    {
        let tip = self.tip().number();
        let static_files = provider.static_file_provider();
        let genesis_file_end = static_files.find_fixed_range(StaticFileSegment::Headers, 0).end();
        let first = self.headers[0].number();
        let skip = match first {
            1 => 0,
            _ if first > genesis_file_end => 0,
            _ if tip > genesis_file_end => genesis_file_end + 1 - first,
            _ => return Err(JoinError::YoungChain { tip, genesis_file_end }),
        };
        let written = &self.headers[usize::try_from(skip).expect("below MAX_HEADERS")..];
        {
            let start = written[0].number();
            let mut writer = static_files.get_writer(start, StaticFileSegment::Headers)?;
            if start == 1 {
                for header in written {
                    writer.append_header(header.header(), &header.hash())?;
                }
            } else {
                let fresh = writer.user_header().block_range().is_none()
                    && writer.user_header().expected_block_range().contains(start);
                if !fresh {
                    return Err(JoinError::HeaderFileInUse(start));
                }
                writer.user_header_mut().set_block_range(start, start);
                writer.append_header_direct(written[0].header(), U256::ZERO, &written[0].hash())?;
                for header in &written[1..] {
                    writer.append_header(header.header(), &header.hash())?;
                }
            }
        }
        for header in written {
            provider.tx_ref().put::<tables::HeaderNumbers>(header.hash(), header.number())?;
        }
        provider.anchor_pruned_static_files(tip)?;
        for stage in StageId::ALL {
            provider.save_stage_checkpoint(stage, StageCheckpoint::new(tip))?;
        }
        for segment in PruneSegment::variants() {
            provider.save_prune_checkpoint(segment, PruneCheckpoint::pruned_through(tip))?;
        }
        Ok(())
    }
}

/// Fetches a confirmed join point for this node from its `apply_from` peers.
#[derive(Debug, Clone, Copy)]
pub struct ClusterJoin;

impl ClusterJoin {
    /// Pause between two rounds over the `apply_from` peers.
    pub const RETRY_INTERVAL: Duration = Duration::from_millis(500);

    /// Asks the `apply_from` peers of `cluster` in order for their join point, and accepts the
    /// first that decodes ([`JoinPoint::decode`]) and that a second node confirms: the first
    /// other cluster node whose own join point holds `P` with the same hash, or else the same
    /// peer asked again. Peers are trusted; this catches a peer answering from a different
    /// chain or fork, not a malicious one. Rounds repeat every [`Self::RETRY_INTERVAL`] until
    /// `budget` runs out, each exchange due by then. Returns the peer and its headers, which
    /// are `None` when the peer is at genesis (nothing to confirm or write).
    pub fn fetch<H: BlockHeader>(
        client: &ClusterClient,
        cluster: &Cluster,
        genesis_hash: B256,
        budget: Duration,
    ) -> Result<(NodeId, Option<JoinHeaders<H>>), JoinError> {
        let deadline = Instant::now() + budget;
        loop {
            let mut failures = Vec::new();
            for &source in cluster.apply_from() {
                let confirmed =
                    Self::ask_confirmed(client, cluster, source, genesis_hash, deadline);
                match confirmed {
                    Ok(headers) => return Ok((source, headers)),
                    Err(failure) => failures.push(format!("node {}: {failure}", source.0)),
                }
            }
            if Instant::now() + Self::RETRY_INTERVAL > deadline {
                return Err(JoinError::NoPeer { budget, failures: failures.join("; ") });
            }
            std::thread::sleep(Self::RETRY_INTERVAL);
        }
    }

    /// `source`'s join point, once another node confirmed it.
    fn ask_confirmed<H: BlockHeader>(
        client: &ClusterClient,
        cluster: &Cluster,
        source: NodeId,
        genesis_hash: B256,
        deadline: Instant,
    ) -> Result<Option<JoinHeaders<H>>, String> {
        let headers = Self::ask(client, source, genesis_hash, deadline)?;
        if let Some(theirs) = &headers {
            Self::confirm(client, cluster, source, theirs, genesis_hash, deadline)?;
        }
        Ok(headers)
    }

    fn ask<H: BlockHeader>(
        client: &ClusterClient,
        node: NodeId,
        genesis_hash: B256,
        deadline: Instant,
    ) -> Result<Option<JoinHeaders<H>>, String> {
        let point = client.join_point(node, deadline).map_err(|failure| format!("{failure:?}"))?;
        point.decode(genesis_hash).map_err(|error| error.to_string())
    }

    /// Checks `headers`' tip against the join point of another node, then of `source` again.
    fn confirm<H: BlockHeader>(
        client: &ClusterClient,
        cluster: &Cluster,
        source: NodeId,
        headers: &JoinHeaders<H>,
        genesis_hash: B256,
        deadline: Instant,
    ) -> Result<(), String> {
        let tip = headers.tip();
        let others = cluster
            .config()
            .nodes
            .iter()
            .map(|node| node.id)
            .filter(|&node| node != source && node != cluster.node_id());
        let mut answers = Vec::new();
        for verifier in others.chain(std::iter::once(source)) {
            match Self::ask::<H>(client, verifier, genesis_hash, deadline) {
                Ok(Some(theirs)) => match theirs.hash_of(tip.number()) {
                    Some(hash) if hash == tip.hash() => return Ok(()),
                    Some(hash) => answers.push(format!("node {} has {hash}", verifier.0)),
                    None => {
                        answers.push(format!("node {} is at {}", verifier.0, theirs.tip().number()))
                    }
                },
                Ok(None) => answers.push(format!("node {} is at genesis", verifier.0)),
                Err(failure) => answers.push(format!("node {}: {failure}", verifier.0)),
            }
        }
        Err(format!("block {} {} not confirmed ({})", tip.number(), tip.hash(), answers.join(", ")))
    }
}

/// The file that marks a datadir whose join has started and not finished. It is created before
/// genesis is written and removed after the join point is committed, so a datadir a crash left
/// half-joined never starts as if it were initialised.
#[derive(Debug, Clone, Copy)]
pub struct JoinMarker;

impl JoinMarker {
    /// Name of the marker file in the datadir.
    pub const FILE_NAME: &str = "cluster-join-incomplete";

    /// The marker's path in `datadir`.
    pub fn path(datadir: &Path) -> PathBuf {
        datadir.join(Self::FILE_NAME)
    }

    /// Creates the marker in `datadir` (and the directory) and syncs it to disk.
    pub fn create(datadir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(datadir)?;
        std::fs::File::create(Self::path(datadir))?.sync_all()
    }

    /// Removes the marker from `datadir`.
    pub fn remove(datadir: &Path) -> io::Result<()> {
        std::fs::remove_file(Self::path(datadir))
    }

    /// Fails when the marker exists in `datadir`.
    pub fn ensure_absent(datadir: &Path) -> Result<(), JoinError> {
        let path = Self::path(datadir);
        if path.try_exists()? { Err(JoinError::Interrupted(path)) } else { Ok(()) }
    }
}

/// Why a node could not join its cluster.
#[derive(Debug, Error)]
pub enum JoinError {
    /// The join point holds more headers than [`JoinPoint::MAX_HEADERS`].
    #[error("the join point holds {0} headers, more than allowed")]
    TooManyHeaders(usize),
    /// A header does not decode.
    #[error("a join point header does not decode: {0}")]
    Decode(#[from] alloy_rlp::Error),
    /// Two headers are not consecutive.
    #[error("join point header {next} follows header {previous}")]
    NotConsecutive {
        /// Number of the earlier header.
        previous: u64,
        /// Number of the header after it.
        next: u64,
    },
    /// A header is genesis, or its parent hash is not the hash of the header before it (of
    /// this chain's genesis for block 1).
    #[error("join point header {0} does not extend this chain")]
    BrokenLink(u64),
    /// The join block lies in genesis's header static file but not among its first blocks.
    #[error(
        "cannot join at block {tip}: a joined node's headers must start right after genesis \
         below block {genesis_file_end}; join once the chain is past it, or copy a datadir"
    )]
    YoungChain {
        /// The join block.
        tip: u64,
        /// The last block of genesis's header static file.
        genesis_file_end: u64,
    },
    /// The header static-file writer is not on a fresh file for the first written header: the
    /// datadir holds more than genesis, or the static-file provider wrote genesis itself.
    #[error(
        "the header static-file writer for block {0} is not on a fresh file: the datadir holds          more than genesis, or the provider was not opened again after writing genesis"
    )]
    HeaderFileInUse(u64),
    /// No `apply_from` peer gave a confirmed join point in time.
    #[error(
        "no apply_from node gave a join point another node confirmed within {budget:?}: {failures}"
    )]
    NoPeer {
        /// How long the peers were asked.
        budget: Duration,
        /// Each peer's failure in the last round.
        failures: String,
    },
    /// A join was interrupted.
    #[error(
        "a cluster join was interrupted in this datadir ({0}); delete the datadir and start again"
    )]
    Interrupted(PathBuf),
    /// The marker could not be checked.
    #[error("join marker: {0}")]
    Io(#[from] io::Error),
    /// The database or static files failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
}

impl From<reth_db_api::DatabaseError> for JoinError {
    fn from(error: reth_db_api::DatabaseError) -> Self {
        Self::Provider(error.into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy_consensus::{BlockBody, Header, Signed, TxLegacy};
    use alloy_primitives::{Address, Signature};
    use reth_chainspec::MAINNET;
    use reth_db_common::init::init_genesis_with_settings;
    use reth_execution_types::ExecutionOutcome;
    use reth_primitives_traits::RecoveredBlock;
    use reth_provider::{
        BlockBodyIndicesProvider, BlockHashReader, BlockNumReader, BlockWriter, DBProvider,
        DatabaseProviderFactory, HeaderProvider, NodePrimitivesProvider, ProviderFactory,
        RocksDBProviderFactory, StageCheckpointReader, StorageSettings,
        providers::StaticFileProvider,
        test_utils::{MockNodeTypesWithDB, create_test_provider_factory},
    };
    use reth_trie_common::HashedPostStateSorted;
    use revm::database::{BundleState, states::reverts::Reverts};

    use super::*;

    type Primitives = <ProviderFactory<MockNodeTypesWithDB> as NodePrimitivesProvider>::Primitives;
    type TestBlock = <Primitives as NodePrimitives>::Block;
    type TestReceipt = <Primitives as NodePrimitives>::Receipt;

    /// A join block in a static file far from genesis's, as on a live network.
    const TIP: u64 = 57_000_123;

    /// `count` linked headers ending at `tip`, the first linked to `parent`.
    fn chain(tip: u64, count: u64, parent: B256) -> Vec<Header> {
        let mut parent = parent;
        (tip + 1 - count..=tip)
            .map(|number| {
                let header = Header { number, parent_hash: parent, ..Header::default() };
                parent = header.hash_slow();
                header
            })
            .collect()
    }

    /// A storage-v2 datadir holding mainnet's genesis, written as a node's launch writes it.
    fn genesis_datadir() -> ProviderFactory<MockNodeTypesWithDB> {
        let factory = create_test_provider_factory();
        init_genesis_with_settings(&factory, StorageSettings::v2()).unwrap();
        factory
    }

    /// The datadir of `factory` opened again, as a restarted node opens it.
    fn reopen(
        factory: &ProviderFactory<MockNodeTypesWithDB>,
    ) -> ProviderFactory<MockNodeTypesWithDB> {
        let static_files =
            StaticFileProvider::read_write(factory.static_file_provider().directory()).unwrap();
        ProviderFactory::new(
            Arc::clone(factory.db_ref()),
            MAINNET.clone(),
            static_files,
            factory.rocksdb_provider(),
            reth_tasks::Runtime::test(),
        )
        .unwrap()
    }

    fn join(
        factory: &ProviderFactory<MockNodeTypesWithDB>,
        headers: &[Header],
    ) -> Result<(), JoinError> {
        let joined = JoinPoint::encode(headers)
            .decode::<Header>(MAINNET.genesis_hash())?
            .expect("headers to join at");
        let provider = factory.database_provider_rw()?;
        joined.write(&provider)?;
        provider.commit()?;
        Ok(())
    }

    /// Block `number` on `parent` with one transaction from `sender`.
    fn block(number: u64, parent: B256, sender: Address) -> RecoveredBlock<TestBlock> {
        let tx = Signed::new_unhashed(TxLegacy::default(), Signature::test_signature()).into();
        let header = Header { number, parent_hash: parent, ..Header::default() };
        let body = BlockBody { transactions: vec![tx], ommers: Vec::new(), withdrawals: None };
        RecoveredBlock::new_unhashed(TestBlock::new(header, body), vec![sender])
    }

    #[test]
    fn a_joined_datadir_opens_at_the_join_block_and_appends_every_segment_after_it() {
        let factory = reopen(&genesis_datadir());
        let headers = chain(TIP, JoinPoint::MAX_HEADERS, B256::repeat_byte(7));
        join(&factory, &headers).unwrap();
        let tip_hash = headers.last().unwrap().hash_slow();

        let factory = reopen(&factory);
        assert_eq!(factory.check_consistency().unwrap(), (None, None));
        let provider = factory.provider().unwrap();
        assert_eq!(provider.best_block_number().unwrap(), TIP);
        assert_eq!(provider.block_hash(TIP).unwrap(), Some(tip_hash));
        assert_eq!(provider.block_hash(TIP - 255).unwrap(), Some(headers[0].hash_slow()));
        assert_eq!(provider.block_hash(0).unwrap(), Some(MAINNET.genesis_hash()));
        assert_eq!(provider.header(tip_hash).unwrap().map(|header| header.number), Some(TIP));
        for stage in StageId::ALL {
            assert_eq!(provider.get_stage_checkpoint(stage).unwrap().unwrap().block_number, TIP);
        }
        drop(provider);

        let provider = factory.database_provider_rw().unwrap();
        let next = block(TIP + 1, tip_hash, Address::repeat_byte(1));
        // One (empty) revert per block, as an executed block carries, so its changesets are
        // written.
        let bundle = BundleState { reverts: Reverts::new(vec![Vec::new()]), ..Default::default() };
        let outcome = ExecutionOutcome::<TestReceipt> {
            bundle,
            receipts: vec![vec![TestReceipt::default()]],
            first_block: TIP + 1,
            ..Default::default()
        };
        provider
            .append_blocks_with_state(vec![next], &outcome, HashedPostStateSorted::default())
            .unwrap();
        for stage in StageId::ALL {
            provider.save_stage_checkpoint(stage, StageCheckpoint::new(TIP + 1)).unwrap();
        }
        provider.commit().unwrap();

        let factory = reopen(&factory);
        assert_eq!(factory.check_consistency().unwrap(), (None, None));
        let static_files = factory.static_file_provider();
        for segment in StaticFileSegment::iter() {
            assert_eq!(
                static_files.get_highest_static_file_block(segment),
                Some(TIP + 1),
                "{segment}"
            );
        }
        let indices = factory.provider().unwrap().block_body_indices(TIP + 1).unwrap().unwrap();
        assert_eq!((indices.first_tx_num, indices.tx_count), (0, 1));
        assert_eq!(
            static_files.get_highest_static_file_tx(StaticFileSegment::Transactions),
            Some(0)
        );
    }

    #[test]
    fn a_join_through_the_provider_that_wrote_genesis_is_refused() {
        let factory = genesis_datadir();
        let headers = chain(TIP, JoinPoint::MAX_HEADERS, B256::repeat_byte(7));
        assert!(matches!(join(&factory, &headers), Err(JoinError::HeaderFileInUse(_))));
    }

    #[test]
    fn a_young_chain_joins_with_every_header_after_genesis() {
        let factory = reopen(&genesis_datadir());
        let headers = chain(20, 20, MAINNET.genesis_hash());
        join(&factory, &headers).unwrap();

        let factory = reopen(&factory);
        assert_eq!(factory.check_consistency().unwrap(), (None, None));
        let provider = factory.provider().unwrap();
        assert_eq!(provider.best_block_number().unwrap(), 20);
        assert_eq!(provider.block_hash(1).unwrap(), Some(headers[0].hash_slow()));
    }

    #[test]
    fn a_join_block_inside_genesis_static_file_needs_every_header_after_genesis() {
        let factory = reopen(&genesis_datadir());
        let headers = chain(10_000, JoinPoint::MAX_HEADERS, B256::repeat_byte(7));
        assert!(matches!(join(&factory, &headers), Err(JoinError::YoungChain { tip: 10_000, .. })));
    }

    #[test]
    fn decoding_rejects_headers_that_do_not_form_a_chain() {
        let genesis = MAINNET.genesis_hash();
        let mut gap = chain(100, 10, B256::ZERO);
        gap.remove(4);
        assert!(matches!(
            JoinPoint::encode(&gap).decode::<Header>(genesis),
            Err(JoinError::NotConsecutive { previous: 94, next: 96 })
        ));
        let mut forked = chain(100, 10, B256::ZERO);
        forked[5].parent_hash = B256::repeat_byte(1);
        assert!(matches!(
            JoinPoint::encode(&forked).decode::<Header>(genesis),
            Err(JoinError::BrokenLink(96))
        ));
        let other_genesis = chain(5, 5, B256::repeat_byte(2));
        assert!(matches!(
            JoinPoint::encode(&other_genesis).decode::<Header>(genesis),
            Err(JoinError::BrokenLink(1))
        ));
        let too_many = chain(1_000, JoinPoint::MAX_HEADERS + 1, B256::ZERO);
        assert!(matches!(
            JoinPoint::encode(&too_many).decode::<Header>(genesis),
            Err(JoinError::TooManyHeaders(257))
        ));
        assert!(matches!(JoinPoint::default().decode::<Header>(genesis), Ok(None)));
    }

    #[test]
    fn the_window_ends_at_the_tip_and_never_includes_genesis() {
        assert_eq!(JoinPoint::window(TIP), TIP - 255..=TIP);
        assert_eq!(JoinPoint::window(20), 1..=20);
    }

    #[test]
    fn a_join_point_fits_a_frame_only_below_the_frame_limit() {
        let small = JoinPoint::encode(&chain(TIP, JoinPoint::MAX_HEADERS, B256::ZERO));
        assert!(small.fits_frame());
        let huge = JoinPoint { headers: vec![Bytes::from(vec![0; Frame::MAX_LEN])] };
        assert!(!huge.fits_frame());
    }

    #[test]
    fn a_join_point_round_trips_through_a_frame() {
        let point = JoinPoint::encode(&chain(TIP, JoinPoint::MAX_HEADERS, B256::ZERO));
        let mut frame = Vec::new();
        Frame::write(&mut frame, &crate::ReadResult::Ok(crate::Response::JoinPoint(point.clone())))
            .unwrap();
        let read: crate::ReadResult = Frame::read(&mut frame.as_slice()).unwrap();
        assert_eq!(read, Ok(crate::Response::JoinPoint(point)));
    }

    #[test]
    fn an_interrupted_join_is_refused_until_the_marker_is_gone() {
        let dir = tempfile::tempdir().unwrap();
        let datadir = dir.path().join("datadir");
        assert!(JoinMarker::ensure_absent(&datadir).is_ok());
        JoinMarker::create(&datadir).unwrap();
        let refused = JoinMarker::ensure_absent(&datadir).unwrap_err();
        assert!(refused.to_string().contains("delete the datadir"), "{refused}");
        JoinMarker::remove(&datadir).unwrap();
        assert!(JoinMarker::ensure_absent(&datadir).is_ok());
    }
}
