//! The state-root check of a clustered node: the capture reads a block's state-root strategy
//! runs, and the background task that checks recent canonical blocks.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{B256, Bytes, map::B256Set};
use base_execution_cluster::{
    CaptureReads, CheckedBlock, ClusterHashSource, ClusterNode, RootCheckMode, RootCheckSchedule,
    RootChecker, StepOutcome, TrieHashCapture,
};
use reth_primitives_traits::AlloyBlockHeader;
use reth_provider::{BlockNumReader, HeaderProvider, ProviderError, StateProofProvider};
use reth_trie::trie_cursor::{TrieCursor, TrieCursorFactory};
use reth_trie_common::{
    BranchNodeCompact, HashedPostState, MultiProofTargets, Nibbles, TrieInput,
    prefix_set::TriePrefixSetsMut, updates::TrieUpdates,
};
use tracing::{error, info};

/// The reads of one block's capture, over the engine's overlay provider for the block's parent:
/// stored nodes as of the parent, and proofs as of the block with its held post-state and trie
/// updates layered on top.
#[derive(Debug)]
pub struct OverlayCaptureReads<'a, P> {
    /// The overlay provider of the block's parent.
    pub provider: &'a P,
    /// The node's capture, which selects the held post-state.
    pub capture: &'a TrieHashCapture,
    /// The block's hashed post-state.
    pub state: &'a HashedPostState,
    /// The block's trie updates of this node's slice.
    pub updates: &'a TrieUpdates,
}

impl<P: TrieCursorFactory + StateProofProvider> CaptureReads for OverlayCaptureReads<'_, P> {
    type Error = ProviderError;

    fn parent_account_node(
        &mut self,
        path: Nibbles,
    ) -> Result<Option<BranchNodeCompact>, ProviderError> {
        Ok(self.provider.account_trie_cursor()?.seek_exact(path)?.map(|(_, node)| node))
    }

    fn parent_storage_node(
        &mut self,
        contract: B256,
        path: Nibbles,
    ) -> Result<Option<BranchNodeCompact>, ProviderError> {
        Ok(self.provider.storage_trie_cursor(contract)?.seek_exact(path)?.map(|(_, node)| node))
    }

    fn proof_nodes(&mut self, targets: &[B256]) -> Result<Vec<(Nibbles, Bytes)>, ProviderError> {
        let input = TrieInput::new(
            self.updates.clone(),
            self.capture.held_state(self.state),
            TriePrefixSetsMut::default(),
        );
        let targets = MultiProofTargets::from_iter(
            targets.iter().map(|target| (*target, B256Set::default())),
        );
        let proof = self.provider.multiproof(input, targets)?;
        Ok(proof.account_subtree.into_inner().into_iter().collect())
    }
}

/// The background state-root check of a clustered node: every [`Self::POLL`] it advances a
/// [`RootCheckSchedule`] over the provider's canonical blocks, asking owners through the node's
/// client (itself first), on a blocking thread. It never touches block insertion.
#[derive(Debug)]
pub struct RootCheckTask<P> {
    node: Arc<ClusterNode>,
    provider: P,
    datadir: PathBuf,
    schedule: RootCheckSchedule,
}

impl<P> RootCheckTask<P>
where
    P: BlockNumReader + HeaderProvider<Header: AlloyBlockHeader> + Send + 'static,
{
    /// Pause between two steps.
    pub const POLL: Duration = Duration::from_millis(500);

    /// Exit code of a node stopped by a mismatch with `root_check = "exit"`.
    pub const MISMATCH_EXIT_CODE: i32 = 3;

    /// The check of `node` over `provider`'s canonical blocks above its current head, counting
    /// into the node's [`ClusterNode::root_check_stats`] and writing the mismatch marker in
    /// `datadir`; `None` when the configuration turns it off.
    pub fn new(
        node: Arc<ClusterNode>,
        provider: P,
        datadir: PathBuf,
    ) -> Result<Option<Self>, ProviderError> {
        let config = node.cluster().config().root_check;
        info!(mode = ?config.mode, warn_blocks = config.warn_blocks, "cluster state root check");
        if config.mode == RootCheckMode::Off {
            return Ok(None);
        }
        let stats = Arc::clone(node.root_check_stats());
        let started_at = provider.best_block_number()?;
        let schedule = RootCheckSchedule::new(stats, config.warn_blocks, started_at);
        Ok(Some(Self { node, provider, datadir, schedule }))
    }

    /// Runs until the task is dropped, each step on a blocking thread.
    pub async fn run(mut self) {
        loop {
            let stepped = tokio::task::spawn_blocking(move || {
                std::thread::sleep(Self::POLL);
                self.step();
                self
            })
            .await;
            match stepped {
                Ok(task) => self = task,
                Err(error) => {
                    error!(error = %error, "cluster state root check stopped");
                    return;
                }
            }
        }
    }

    /// One step: checks at most one block and handles a mismatch.
    pub fn step(&mut self) {
        let head = match self.provider.best_block_number() {
            Ok(head) => head,
            Err(error) => {
                error!(error = %error, "cluster state root check could not read the head");
                return;
            }
        };
        let header = |number| {
            let header = self.provider.sealed_header(number).ok().flatten()?;
            Some(CheckedBlock { number, hash: header.hash(), state_root: header.state_root() })
        };
        let source = ClusterHashSource { node: &self.node };
        let checker = RootChecker::new(self.node.cluster(), &source);
        let outcome =
            self.schedule.step(head, Instant::now(), header, |block| checker.check(block));
        if let StepOutcome::Mismatch(mismatch) = outcome {
            let mode = self.node.cluster().config().root_check.mode;
            if mode.on_mismatch(&mismatch, &self.datadir) {
                std::process::exit(Self::MISMATCH_EXIT_CODE);
            }
        }
    }
}
