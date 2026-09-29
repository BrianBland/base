//! Payload-only parallel execution with database reads served by the owning thread.

use std::{
    convert::Infallible,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, OnceLock,
        mpsc::{self, Receiver, Sender},
    },
};

use alloy_eips::eip2718::Decodable2718;
use alloy_evm::{Database, EvmEnv};
use alloy_primitives::{Address, B256, Bytes, U256, map::DefaultHashBuilder};
use base_common_consensus::BaseTxEnvelope;
use dashmap::DashMap;
use revm::{
    DatabaseRef,
    context::result::ResultAndState,
    state::{AccountInfo, Bytecode},
};

use crate::{
    BaseEvmFactory, BaseHaltReason, BaseSpecId, ParallelOutcome, Schedule, Store, Workers,
};

static COMPLETED_RUNS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Immutable transaction list and persistent workers for one gated payload.
#[derive(Debug, Clone)]
pub struct ParallelPayload {
    /// Full EIP-2718 transaction list, including the sequential deposit prefix.
    pub transactions: Arc<[Bytes]>,
    /// Shared worker pool. The extra caller in `Workers` is the database service thread.
    pub workers: Arc<Workers>,
    /// Production factory, including the chain's activation administrator.
    pub factory: BaseEvmFactory,
}

/// A transaction result checked against the transaction delivered by the outer executor.
#[derive(Debug)]
pub struct ParallelTransaction {
    /// Hash of the executed envelope.
    pub hash: B256,
    /// Independently recovered signer.
    pub signer: Address,
    /// Rebased result for the normal commit path.
    pub output: ResultAndState<BaseHaltReason>,
}

impl ParallelTransaction {
    /// Preserves the generic executor API without imposing Base halt reasons on other EVMs.
    pub fn into_output<H: 'static>(self) -> Option<ResultAndState<H>> {
        let output: Box<dyn std::any::Any> = Box::new(self.output);
        output.downcast::<ResultAndState<H>>().ok().map(|output| *output)
    }
}

impl ParallelPayload {
    /// Only the production Base environment is supported; other EVMs stay sequential.
    pub fn environment<E: alloy_evm::Evm>(evm: &E) -> Option<EvmEnv<BaseSpecId>> {
        if std::any::TypeId::of::<E::HaltReason>() != std::any::TypeId::of::<BaseHaltReason>() {
            return None;
        }
        let cfg = (evm.cfg_env() as &dyn std::any::Any)
            .downcast_ref::<revm::context::CfgEnv<BaseSpecId>>()?;
        if cfg.spec.into_eth_spec().is_enabled_in(revm::primitives::hardfork::SpecId::AMSTERDAM) {
            return None;
        }
        let block =
            (evm.block() as &dyn std::any::Any).downcast_ref::<revm::context::BlockEnv>()?;
        Some(EvmEnv::new(cfg.clone(), block.clone()))
    }

    /// Successful parallel suffix executions, for deployment and fixture diagnostics.
    pub fn completed_runs() -> u64 {
        COMPLETED_RUNS.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Reads the runtime gate once. Invalid values and pool construction failures disable it.
    pub fn configured_workers() -> Option<Arc<Workers>> {
        static WORKERS: OnceLock<Option<Arc<Workers>>> = OnceLock::new();
        WORKERS
            .get_or_init(|| {
                let threads =
                    std::env::var("BASE_PARALLEL_EXECUTION_THREADS").ok()?.parse::<usize>().ok()?;
                if threads == 0 {
                    return None;
                }
                Some(Arc::new(Workers::new(threads.checked_add(1)?).ok()?))
            })
            .clone()
    }

    /// Executes the suffix without committing to `db`. Any failure discards all results.
    /// The owner services reads itself, so even non-`Send` databases remain on their owner thread.
    pub fn execute<DB: Database>(
        &self,
        db: &mut DB,
        env: EvmEnv<BaseSpecId>,
        first: usize,
    ) -> Result<Vec<ParallelTransaction>, String> {
        let (sender, receiver) = mpsc::channel();
        let mut failure = None;
        let result = self.workers.dispatch(
            || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let transactions = self
                        .transactions
                        .get(first..)
                        .ok_or("invalid transaction index")?
                        .iter()
                        .map(|raw| {
                            BaseTxEnvelope::decode_2718(&mut raw.as_ref())
                                .map_err(|e| e.to_string())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if transactions.iter().any(|tx| matches!(tx, BaseTxEnvelope::Eip8130(_))) {
                        return Err("EIP-8130 requires sequential execution".into());
                    }
                    let pre = ReadServiceDb {
                        sender: sender.clone(),
                        accounts: DashMap::default(),
                        storage: DashMap::default(),
                        codes: DashMap::default(),
                        hashes: DashMap::default(),
                    };
                    let store = Store::new(&pre);
                    let outcome = ParallelOutcome::execute(
                        &self.factory,
                        env,
                        &transactions,
                        &store,
                        &self.workers,
                        Schedule::default(),
                        false,
                    )
                    .map_err(|e| e.to_string())?;
                    transactions
                        .iter()
                        .zip(outcome.states)
                        .zip(outcome.signers)
                        .map(|((tx, output), signer)| {
                            Ok(ParallelTransaction { hash: tx.tx_hash(), signer, output })
                        })
                        .collect::<Result<Vec<_>, String>>()
                }))
                .unwrap_or_else(|_| Err("parallel worker panicked".into()));
                let _ = sender.send(ReadRequest::Finished(result));
            },
            || {
                let receiver = receiver;
                loop {
                    match receiver.recv().map_err(|e| e.to_string())? {
                        ReadRequest::Account(address, reply) => {
                            let result = db.basic(address).map_err(|e| e.to_string());
                            let _ = reply.send(Self::read_or_default(result, &mut failure));
                        }
                        ReadRequest::Storage(address, slot, reply) => {
                            let result = db.storage(address, slot).map_err(|e| e.to_string());
                            let _ = reply.send(Self::read_or_default(result, &mut failure));
                        }
                        ReadRequest::Code(hash, reply) => {
                            let result = db.code_by_hash(hash).map_err(|e| e.to_string());
                            let _ = reply.send(Self::read_or_default(result, &mut failure));
                        }
                        ReadRequest::Hash(number, reply) => {
                            let result = db.block_hash(number).map_err(|e| e.to_string());
                            let _ = reply.send(Self::read_or_default(result, &mut failure));
                        }
                        ReadRequest::Finished(result) => break result,
                    }
                }
            },
        );
        let result = failure.map_or(result, Err);
        if result.is_ok() {
            COMPLETED_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    /// A failed speculative read poisons the entire result, never the caller's state.
    pub fn read_or_default<T: Default>(
        result: Result<T, String>,
        failure: &mut Option<String>,
    ) -> T {
        result.unwrap_or_else(|error| {
            *failure = Some(error);
            T::default()
        })
    }
}

/// Requests handled only by the database owner.
#[derive(Debug)]
pub enum ReadRequest {
    /// Load an account.
    Account(Address, Sender<Option<AccountInfo>>),
    /// Load a storage slot.
    Storage(Address, U256, Sender<U256>),
    /// Load bytecode.
    Code(B256, Sender<Bytecode>),
    /// Load a historical block hash.
    Hash(u64, Sender<B256>),
    /// All workers have exited; no further reads can arrive.
    Finished(Result<Vec<ParallelTransaction>, String>),
}

/// Concurrent read-through cache over an owner-thread database service.
#[derive(Debug)]
pub struct ReadServiceDb {
    sender: Sender<ReadRequest>,
    accounts: DashMap<Address, Option<AccountInfo>, DefaultHashBuilder>,
    storage: DashMap<(Address, U256), U256, DefaultHashBuilder>,
    codes: DashMap<B256, Bytecode, DefaultHashBuilder>,
    hashes: DashMap<u64, B256, DefaultHashBuilder>,
}

impl ReadServiceDb {
    /// Waits for one read; disconnect means the owning execution has unwound.
    pub fn receive<T>(receiver: Receiver<T>) -> T {
        receiver.recv().expect("parallel database owner disconnected")
    }
}

impl DatabaseRef for ReadServiceDb {
    type Error = Infallible;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(value) = self.accounts.get(&address) {
            return Ok(value.clone());
        }
        let (reply, receiver) = mpsc::channel();
        self.sender
            .send(ReadRequest::Account(address, reply))
            .expect("database owner disconnected");
        let value = Self::receive(receiver);
        self.accounts.insert(address, value.clone());
        Ok(value)
    }

    fn storage_ref(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        if let Some(value) = self.storage.get(&(address, slot)) {
            return Ok(*value);
        }
        let (reply, receiver) = mpsc::channel();
        self.sender
            .send(ReadRequest::Storage(address, slot, reply))
            .expect("database owner disconnected");
        let value = Self::receive(receiver);
        self.storage.insert((address, slot), value);
        Ok(value)
    }

    fn code_by_hash_ref(&self, hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(value) = self.codes.get(&hash) {
            return Ok(value.clone());
        }
        let (reply, receiver) = mpsc::channel();
        self.sender.send(ReadRequest::Code(hash, reply)).expect("database owner disconnected");
        let value = Self::receive(receiver);
        self.codes.insert(hash, value.clone());
        Ok(value)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        if let Some(value) = self.hashes.get(&number) {
            return Ok(*value);
        }
        let (reply, receiver) = mpsc::channel();
        self.sender.send(ReadRequest::Hash(number, reply)).expect("database owner disconnected");
        let value = Self::receive(receiver);
        self.hashes.insert(number, value);
        Ok(value)
    }
}

#[cfg(test)]
pub use tests::{OwnerDb, ReadFailure};

#[cfg(test)]
mod tests {
    //! Non-Send ownership probe for revm's external Database trait. Its Rc state and owner-panic
    //! injection exercise thread affinity and scope unwinding, not an internal mock interaction.

    use std::{cell::Cell, rc::Rc, time::Duration};

    use super::*;

    /// Injected database read failure.
    #[derive(Debug, thiserror::Error)]
    #[error("injected read failure")]
    pub struct ReadFailure;
    impl revm::context::DBErrorMarker for ReadFailure {}

    #[derive(Debug)]
    /// Non-Send database with injected failures for owner-thread tests.
    pub struct OwnerDb {
        reads: Rc<Cell<usize>>,
        fail: bool,
        panic: bool,
    }

    impl revm::Database for OwnerDb {
        type Error = ReadFailure;

        fn basic(&mut self, _: Address) -> Result<Option<AccountInfo>, Self::Error> {
            self.reads.set(self.reads.get() + 1);
            assert!(!self.panic, "injected owner panic");
            if self.fail {
                return Err(ReadFailure);
            }
            Ok(None)
        }
        fn storage(&mut self, _: Address, _: U256) -> Result<U256, Self::Error> {
            Ok(U256::ZERO)
        }
        fn code_by_hash(&mut self, _: B256) -> Result<Bytecode, Self::Error> {
            Ok(Bytecode::default())
        }
        fn block_hash(&mut self, _: u64) -> Result<B256, Self::Error> {
            Ok(B256::ZERO)
        }
    }

    fn payload() -> ParallelPayload {
        ParallelPayload {
            transactions: Arc::from([]),
            workers: Arc::new(Workers::new(3).unwrap()),
            factory: BaseEvmFactory::default(),
        }
    }

    #[test]
    fn owner_reads_support_non_send_databases_and_errors_discard_results() {
        for fail in [false, true] {
            let reads = Rc::new(Cell::new(0));
            let mut db = OwnerDb { reads: Rc::clone(&reads), fail, panic: false };
            let result = payload().execute(&mut db, EvmEnv::default(), 0);
            assert!(reads.get() > 0);
            assert_eq!(result.is_err(), fail);
        }
    }

    #[test]
    fn owner_panic_disconnects_waiting_workers() {
        let (sent, received) = mpsc::channel();
        std::thread::spawn(move || {
            let mut db = OwnerDb { reads: Rc::new(Cell::new(0)), fail: false, panic: true };
            let result =
                catch_unwind(AssertUnwindSafe(|| payload().execute(&mut db, EvmEnv::default(), 0)));
            sent.send(result.is_err()).unwrap();
        });
        assert!(received.recv_timeout(Duration::from_secs(5)).expect("worker did not exit"));
    }
}
