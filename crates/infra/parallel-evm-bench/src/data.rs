//! Block fixtures: a block plus the exact pre-state its sequential execution touches.

use std::{collections::HashMap, convert::Infallible, future::IntoFuture, sync::Mutex};

use alloy_consensus::{BlockBody, Header};
use alloy_eips::{BlockId, eip2718::Decodable2718};
use alloy_primitives::{Address, B256, Bytes, U256, keccak256, map::HashMap as FastMap};
use alloy_provider::{Provider, RootProvider};
use base_common_consensus::{BaseBlock, BaseTxEnvelope};
use base_common_network::Base;
use eyre::{Result, eyre};
use reth_primitives_traits::RecoveredBlock;
use revm::{
    DatabaseRef,
    context::DBErrorMarker,
    primitives::KECCAK_EMPTY,
    state::{AccountInfo, Bytecode},
};
use serde::{Deserialize, Serialize};

/// Account fields as fetched from the parent-block state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreAccount {
    /// Balance.
    pub balance: U256,
    /// Nonce.
    pub nonce: u64,
    /// Code hash.
    pub code_hash: B256,
}

/// Every piece of parent state touched by the block, as serialized on disk.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Prestate {
    /// Accounts (`None` = does not exist).
    pub accounts: HashMap<Address, Option<PreAccount>>,
    /// Storage slots.
    pub storage: HashMap<Address, HashMap<U256, U256>>,
    /// Bytecode by hash.
    pub codes: HashMap<B256, Bytes>,
    /// Block hashes.
    pub block_hashes: HashMap<u64, B256>,
}

/// A fixture file.
#[derive(Debug, Serialize, Deserialize)]
pub struct BlockFixture {
    /// Header.
    pub header: Header,
    /// EIP-2718 encoded transactions.
    pub txs: Vec<Bytes>,
    /// Recovered senders.
    pub senders: Vec<Address>,
    /// Pre-state.
    pub prestate: Prestate,
}

impl BlockFixture {
    /// Rebuilds the recovered block.
    pub fn block(&self) -> Result<RecoveredBlock<BaseBlock>> {
        let transactions = self
            .txs
            .iter()
            .map(|raw| BaseTxEnvelope::decode_2718(&mut raw.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        let body = BlockBody { transactions, ommers: vec![], withdrawals: Some(Default::default()) };
        Ok(RecoveredBlock::new_unhashed(
            BaseBlock { header: self.header.clone(), body },
            self.senders.clone(),
        ))
    }
}

/// In-memory, read-only pre-state used by every benchmark arm.
#[derive(Debug, Default)]
pub struct PreDb {
    accounts: FastMap<Address, Option<AccountInfo>>,
    storage: FastMap<(Address, U256), U256>,
    codes: FastMap<B256, Bytecode>,
    block_hashes: FastMap<u64, B256>,
}

impl PreDb {
    /// Builds the in-memory database from a fixture.
    pub fn new(pre: &Prestate) -> Self {
        let codes: FastMap<B256, Bytecode> = pre
            .codes
            .iter()
            .map(|(hash, code)| (*hash, Bytecode::new_raw(code.clone())))
            .collect();
        let accounts = pre
            .accounts
            .iter()
            .map(|(address, account)| {
                let info = account.as_ref().map(|account| AccountInfo {
                    balance: account.balance,
                    nonce: account.nonce,
                    code_hash: account.code_hash,
                    code: codes.get(&account.code_hash).cloned(),
                    account_id: None,
                });
                (*address, info)
            })
            .collect();
        let storage = pre
            .storage
            .iter()
            .flat_map(|(address, slots)| {
                slots.iter().map(move |(slot, value)| ((*address, *slot), *value))
            })
            .collect();
        let block_hashes = pre.block_hashes.iter().map(|(n, h)| (*n, *h)).collect();
        Self { accounts, storage, codes, block_hashes }
    }
}

// Missing entries can only be hit by speculative executions on a wrong path (the fixture holds
// everything the canonical execution reads), so they resolve to empty state.
impl DatabaseRef for PreDb {
    type Error = Infallible;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        Ok(self.accounts.get(&address).cloned().flatten())
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        Ok(self.codes.get(&code_hash).cloned().unwrap_or_default())
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        Ok(self.storage.get(&(address, index)).copied().unwrap_or_default())
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        Ok(self.block_hashes.get(&number).copied().unwrap_or_default())
    }
}

/// RPC failure surfaced through revm's database interface.
#[derive(Debug)]
pub struct FetchError(pub String);

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FetchError {}
impl DBErrorMarker for FetchError {}

/// Database that reads parent-block state over RPC and records everything it served.
#[derive(Debug)]
pub struct RpcRecorder {
    rt: tokio::runtime::Runtime,
    provider: RootProvider<Base>,
    at: BlockId,
    /// Everything served so far.
    pub pre: Mutex<Prestate>,
}

impl RpcRecorder {
    /// Creates a recorder reading state at `parent`.
    pub fn new(rpc: &str, parent: u64) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let provider = RootProvider::<Base>::new_http(rpc.parse()?);
        Ok(Self { rt, provider, at: BlockId::number(parent), pre: Mutex::default() })
    }

    /// Fetches block `number` with full transactions.
    pub fn fetch_block(&self, number: u64) -> Result<(Header, Vec<Bytes>, Vec<Address>)> {
        let block = self
            .retry(|| self.provider.get_block_by_number(number.into()).full().into_future())?
            .ok_or_else(|| eyre!("block {number} not found"))?;
        let header = block.header.inner.inner.clone();
        let (txs, senders) = block
            .transactions
            .into_transactions()
            .map(|tx| {
                let recovered = tx.inner.inner;
                let sender = recovered.signer();
                (alloy_eips::eip2718::Encodable2718::encoded_2718(recovered.inner()).into(), sender)
            })
            .unzip();
        Ok((header, txs, senders))
    }

    fn is_rate_limit(err: &str) -> bool {
        err.contains("429") || err.contains("rate limit") || err.contains("-32016")
    }

    /// Runs a request, backing off while the endpoint rate-limits.
    fn retry<T, E: std::fmt::Display, F: std::future::Future<Output = Result<T, E>>>(
        &self,
        mut request: impl FnMut() -> F,
    ) -> Result<T, FetchError> {
        let mut delay = std::time::Duration::from_millis(100);
        for _ in 0..200 {
            match self.rt.block_on(request()) {
                Ok(value) => return Ok(value),
                Err(err) if Self::is_rate_limit(&err.to_string()) => {
                    std::thread::sleep(delay);
                    delay = (delay * 2).min(std::time::Duration::from_secs(5));
                }
                Err(err) => return Err(FetchError(err.to_string())),
            }
        }
        Err(FetchError("rate limited for too long".into()))
    }
}

impl DatabaseRef for RpcRecorder {
    type Error = FetchError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let cached = self.pre.lock().unwrap().accounts.get(&address).cloned();
        let account = match cached {
            Some(account) => account,
            None => {
                // Historical `eth_getProof` is unavailable on archive reth, so use plain getters.
                let p = &self.provider;
                let (balance, nonce, code) = self.retry(|| async {
                    tokio::try_join!(
                        p.get_balance(address).block_id(self.at).into_future(),
                        p.get_transaction_count(address).block_id(self.at).into_future(),
                        p.get_code_at(address).block_id(self.at).into_future(),
                    )
                })?;
                let code_hash = if code.is_empty() { KECCAK_EMPTY } else { keccak256(&code) };
                let exists = nonce != 0 || !balance.is_zero() || code_hash != KECCAK_EMPTY;
                let account = exists.then_some(PreAccount { balance, nonce, code_hash });
                if code_hash != KECCAK_EMPTY {
                    self.pre.lock().unwrap().codes.insert(code_hash, code);
                }
                self.pre.lock().unwrap().accounts.insert(address, account.clone());
                account
            }
        };
        let pre = self.pre.lock().unwrap();
        Ok(account.map(|account| AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            code_hash: account.code_hash,
            code: pre.codes.get(&account.code_hash).cloned().map(Bytecode::new_raw),
            account_id: None,
        }))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let pre = self.pre.lock().unwrap();
        Ok(pre.codes.get(&code_hash).cloned().map(Bytecode::new_raw).unwrap_or_default())
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        if let Some(value) =
            self.pre.lock().unwrap().storage.get(&address).and_then(|s| s.get(&index))
        {
            return Ok(*value);
        }
        let value = self.retry(|| {
            self.provider.get_storage_at(address, index).block_id(self.at).into_future()
        })?;
        self.pre.lock().unwrap().storage.entry(address).or_default().insert(index, value);
        Ok(value)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        if let Some(hash) = self.pre.lock().unwrap().block_hashes.get(&number) {
            return Ok(*hash);
        }
        let hash = self
            .retry(|| self.provider.get_block_by_number(number.into()).into_future())?
            .ok_or_else(|| FetchError(format!("block {number} not found")))?
            .header
            .hash;
        self.pre.lock().unwrap().block_hashes.insert(number, hash);
        Ok(hash)
    }
}
