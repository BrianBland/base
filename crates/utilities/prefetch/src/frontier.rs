use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use alloy_primitives::{Address, B256, FixedBytes, U256, keccak256};
use revm::primitives::StorageKey;

use crate::{DowseSelector, Erc20Context, Erc20StorageLayout, PrefetchHintBuilder, TxShape};

/// Well-known 4-byte selectors used by the frontier planners.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchWellKnownSelectors;

impl PrefetchWellKnownSelectors {
    /// ERC-20 `transfer(address,uint256)`.
    pub const ERC20_TRANSFER: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];
    /// ERC-20 `transferFrom(address,address,uint256)`.
    pub const ERC20_TRANSFER_FROM: [u8; 4] = [0x23, 0xb8, 0x72, 0xdd];
    /// ERC-20 `balanceOf(address)`.
    pub const ERC20_BALANCE_OF: [u8; 4] = [0x70, 0xa0, 0x82, 0x31];
    /// WETH9 `deposit()`.
    pub const WETH9_DEPOSIT: [u8; 4] = [0xd0, 0xe3, 0x0d, 0xb0];
    /// WETH9 `withdraw(uint256)`.
    pub const WETH9_WITHDRAW: [u8; 4] = [0x2e, 0x1a, 0x7d, 0x4d];
    /// Uniswap V2 pair `swap(uint256,uint256,address,bytes)`.
    pub const UNISWAP_V2_PAIR_SWAP: [u8; 4] = [0x02, 0x2c, 0x0d, 0x9f];
    /// Uniswap V2 pair `mint(address)`.
    pub const UNISWAP_V2_PAIR_MINT: [u8; 4] = [0x6a, 0x62, 0x7c, 0xa1];
    /// Uniswap V2 pair `burn(address)`.
    pub const UNISWAP_V2_PAIR_BURN: [u8; 4] = [0x89, 0xaf, 0xcb, 0x44];
    /// Uniswap V2 router `swapExactTokensForTokens(uint256,uint256,address[],address,uint256)`.
    pub const UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_TOKENS: [u8; 4] = [0x38, 0xed, 0x17, 0x39];
    /// Uniswap V2 router `swapTokensForExactTokens(uint256,uint256,address[],address,uint256)`.
    pub const UNISWAP_V2_ROUTER_SWAP_TOKENS_FOR_EXACT_TOKENS: [u8; 4] = [0x88, 0x03, 0xdb, 0xee];
    /// Uniswap V2 router `swapExactETHForTokens(uint256,address[],address,uint256)`.
    pub const UNISWAP_V2_ROUTER_SWAP_EXACT_ETH_FOR_TOKENS: [u8; 4] = [0x7f, 0xf3, 0x6a, 0xb5];
    /// Uniswap V2 router `swapETHForExactTokens(uint256,address[],address,uint256)`.
    pub const UNISWAP_V2_ROUTER_SWAP_ETH_FOR_EXACT_TOKENS: [u8; 4] = [0xfb, 0x3b, 0xdb, 0x41];
    /// Uniswap V2 router `swapExactTokensForETH(uint256,uint256,address[],address,uint256)`.
    pub const UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_ETH: [u8; 4] = [0x18, 0xcb, 0xaf, 0xe5];
    /// Uniswap V2 router `swapTokensForExactETH(uint256,uint256,address[],address,uint256)`.
    pub const UNISWAP_V2_ROUTER_SWAP_TOKENS_FOR_EXACT_ETH: [u8; 4] = [0x4a, 0x25, 0xd9, 0x4a];
    /// Uniswap V2 router `swapExactTokensForTokensSupportingFeeOnTransferTokens(...)`.
    pub const UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_TOKENS_FEE_ON_TRANSFER: [u8; 4] =
        [0x5c, 0x11, 0xd7, 0x95];
    /// Uniswap V2 router `swapExactETHForTokensSupportingFeeOnTransferTokens(...)`.
    pub const UNISWAP_V2_ROUTER_SWAP_EXACT_ETH_FOR_TOKENS_FEE_ON_TRANSFER: [u8; 4] =
        [0xb6, 0xf9, 0xde, 0x95];
    /// Uniswap V2 router `swapExactTokensForETHSupportingFeeOnTransferTokens(...)`.
    pub const UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_ETH_FEE_ON_TRANSFER: [u8; 4] =
        [0x79, 0x1a, 0xc9, 0x47];

    /// Returns `selector` as a `DowseSelector`.
    pub fn selector(selector: [u8; 4]) -> DowseSelector {
        Some(FixedBytes::from(selector))
    }

    /// Returns `true` if the provided selector matches `expected`.
    pub fn matches(selector: DowseSelector, expected: [u8; 4]) -> bool {
        selector == Self::selector(expected)
    }
}

/// ABI decoding helpers for frame-entry and callsite planners.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchAbiDecoder;

impl PrefetchAbiDecoder {
    /// Returns the first 4-byte selector from calldata, if present.
    pub fn selector(calldata: &[u8]) -> DowseSelector {
        if calldata.len() < 4 {
            return None;
        }
        Some(FixedBytes::from_slice(&calldata[..4]))
    }

    /// Returns the `word_index`th ABI word after the selector.
    pub fn word(calldata: &[u8], word_index: usize) -> Option<B256> {
        let start = 4 + word_index.saturating_mul(32);
        let end = start.saturating_add(32);
        if calldata.len() < end {
            return None;
        }
        Some(B256::from_slice(&calldata[start..end]))
    }

    /// Returns the `arg_index`th ABI argument as an address.
    pub fn address_arg(calldata: &[u8], arg_index: usize) -> Option<Address> {
        let word = Self::word(calldata, arg_index)?;
        Some(Address::from_word(word))
    }

    /// Returns the `arg_index`th ABI argument as a `U256`.
    pub fn u256_arg(calldata: &[u8], arg_index: usize) -> Option<U256> {
        let word = Self::word(calldata, arg_index)?;
        Some(word.into())
    }

    /// Returns the `arg_index`th ABI argument as a dynamic `address[]`.
    pub fn address_array_arg(calldata: &[u8], arg_index: usize) -> Option<Vec<Address>> {
        let offset_word = Self::word(calldata, arg_index)?;
        let offset = Self::word_to_usize(offset_word)?;
        let array_start = 4 + offset;
        let length = Self::word_at(calldata, array_start).and_then(Self::word_to_usize)?;
        let mut addresses = Vec::with_capacity(length);
        let mut cursor = array_start + 32;
        for _ in 0..length {
            let word = Self::word_at(calldata, cursor)?;
            addresses.push(Address::from_word(word));
            cursor = cursor.saturating_add(32);
        }
        Some(addresses)
    }

    /// Returns the 32-byte ABI word at `start`, which is an absolute byte offset.
    pub fn word_at(calldata: &[u8], start: usize) -> Option<B256> {
        let end = start.saturating_add(32);
        if calldata.len() < end {
            return None;
        }
        Some(B256::from_slice(&calldata[start..end]))
    }

    /// Returns `true` if the `arg_index`th ABI argument is nonzero.
    pub fn nonzero_u256_arg(calldata: &[u8], arg_index: usize) -> bool {
        Self::u256_arg(calldata, arg_index).is_some_and(|value| value != U256::ZERO)
    }

    /// Converts a 32-byte ABI word to a `usize`, rejecting values that overflow.
    pub fn word_to_usize(word: B256) -> Option<usize> {
        if word.as_slice()[..24].iter().any(|byte| *byte != 0) {
            return None;
        }
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&word.as_slice()[24..]);
        Some(u64::from_be_bytes(bytes) as usize)
    }
}

/// External call kind observed at a callsite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrefetchExternalCallKind {
    /// `CALL`.
    Call,
    /// `STATICCALL`.
    StaticCall,
    /// `DELEGATECALL`.
    DelegateCall,
    /// `CALLCODE`.
    CallCode,
}

/// One concrete task that the frontier prefetcher can schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PrefetchTask {
    /// Monotonic generation used to reject stale speculative work.
    pub generation: u64,
    /// EVM frame depth where this task is expected to become useful.
    pub depth: u8,
    /// Higher priorities should be prefetched first.
    pub priority: u32,
    /// Planner confidence, scaled by 10,000.
    pub confidence_x10000: u16,
    /// Lower ranks are expected to be used earlier in execution.
    pub earliest_use_rank: u16,
    /// Concrete task kind.
    pub kind: PrefetchTaskKind,
}

impl PrefetchTask {
    /// Creates an account prefetch task.
    pub const fn account(
        generation: u64,
        depth: u8,
        priority: u32,
        confidence_x10000: u16,
        earliest_use_rank: u16,
        address: Address,
    ) -> Self {
        Self {
            generation,
            depth,
            priority,
            confidence_x10000,
            earliest_use_rank,
            kind: PrefetchTaskKind::Account { address },
        }
    }

    /// Creates an account+code prefetch task.
    pub const fn account_code(
        generation: u64,
        depth: u8,
        priority: u32,
        confidence_x10000: u16,
        earliest_use_rank: u16,
        address: Address,
    ) -> Self {
        Self {
            generation,
            depth,
            priority,
            confidence_x10000,
            earliest_use_rank,
            kind: PrefetchTaskKind::AccountCode { address },
        }
    }

    /// Creates a storage prefetch task.
    pub const fn storage(
        generation: u64,
        depth: u8,
        priority: u32,
        confidence_x10000: u16,
        earliest_use_rank: u16,
        address: Address,
        slot: StorageKey,
    ) -> Self {
        Self {
            generation,
            depth,
            priority,
            confidence_x10000,
            earliest_use_rank,
            kind: PrefetchTaskKind::Storage { address, slot },
        }
    }
}

/// Concrete task kind for frontier prefetching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrefetchTaskKind {
    /// Prefetch account metadata.
    Account {
        /// Address to prefetch.
        address: Address,
    },
    /// Prefetch both account metadata and code for `address`.
    AccountCode {
        /// Address to prefetch.
        address: Address,
    },
    /// Prefetch a storage slot on `address`.
    Storage {
        /// Contract address that owns the slot.
        address: Address,
        /// Concrete slot key.
        slot: StorageKey,
    },
}

/// High-level task class used for budgeting and runtime adaptation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrefetchTaskClass {
    /// Prefetch account metadata only.
    Account,
    /// Prefetch account metadata and code.
    AccountCode,
    /// Prefetch one concrete storage slot.
    Storage,
}

impl PrefetchTaskClass {
    /// Returns the maximum hidden lookups contributed by one task of this class, scaled by 100.
    pub const fn max_hidden_lookups_x100(self) -> u32 {
        match self {
            Self::AccountCode => 200,
            Self::Account | Self::Storage => 100,
        }
    }

    /// Returns the scheduler class rank, where lower ranks are preferred first.
    pub const fn rank(self) -> u8 {
        match self {
            Self::Storage => 0,
            Self::AccountCode => 1,
            Self::Account => 2,
        }
    }
}

impl PrefetchTaskKind {
    /// Returns the high-level class of this task.
    pub const fn class(&self) -> PrefetchTaskClass {
        match self {
            Self::Account { .. } => PrefetchTaskClass::Account,
            Self::AccountCode { .. } => PrefetchTaskClass::AccountCode,
            Self::Storage { .. } => PrefetchTaskClass::Storage,
        }
    }

    /// Returns the address targeted by this task.
    pub const fn address(&self) -> Address {
        match self {
            Self::Account { address }
            | Self::AccountCode { address }
            | Self::Storage { address, .. } => *address,
        }
    }

    /// Returns the storage target for this task, if it is a storage task.
    pub const fn storage_target(&self) -> Option<(Address, StorageKey)> {
        match self {
            Self::Storage { address, slot } => Some((*address, *slot)),
            Self::Account { .. } | Self::AccountCode { .. } => None,
        }
    }
}

impl PrefetchTask {
    /// Returns the high-level class of this task.
    pub const fn class(&self) -> PrefetchTaskClass {
        self.kind.class()
    }

    /// Returns the maximum hidden lookups this task could cover, scaled by 100.
    pub const fn max_hidden_lookups_x100(&self) -> u32 {
        self.class().max_hidden_lookups_x100()
    }

    /// Returns the confidence-weighted hidden lookup estimate, scaled by 100.
    pub const fn weighted_hidden_lookups_x100(&self) -> u32 {
        ((self.max_hidden_lookups_x100() as u128).saturating_mul(self.confidence_x10000 as u128)
            / 10_000) as u32
    }
}

/// Speculative child-call prediction emitted by a planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChildCallPrediction {
    /// Predicted child callee.
    pub callee: Address,
    /// Optional known code hash for the child contract.
    pub code_hash: Option<B256>,
    /// Optional predicted selector for the child call.
    pub selector: DowseSelector,
    /// Planner confidence, scaled by 10,000.
    pub confidence_x10000: u16,
    /// Lower ranks are expected to be used earlier in execution.
    pub earliest_use_rank: u16,
}

impl ChildCallPrediction {
    /// Converts this prediction into an account+code warming task.
    pub const fn into_account_code_task(
        &self,
        generation: u64,
        depth: u8,
        priority: u32,
    ) -> PrefetchTask {
        PrefetchTask::account_code(
            generation,
            depth,
            priority,
            self.confidence_x10000,
            self.earliest_use_rank,
            self.callee,
        )
    }
}

/// Per-frame execution context used by the frontier planners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchFrameContext<'a> {
    /// Monotonic generation for speculative work.
    pub generation: u64,
    /// EVM frame depth.
    pub depth: u8,
    /// Current contract address.
    pub contract: Address,
    /// Effective caller for the frame.
    pub caller: Address,
    /// Optional known code hash for `contract`.
    pub code_hash: Option<B256>,
    /// Selector of the current frame, if any.
    pub selector: DowseSelector,
    /// Full calldata for the frame.
    pub calldata: &'a [u8],
}

impl<'a> PrefetchFrameContext<'a> {
    /// Creates a new frame context and derives the selector from calldata.
    pub fn new(
        generation: u64,
        depth: u8,
        contract: Address,
        caller: Address,
        code_hash: Option<B256>,
        calldata: &'a [u8],
    ) -> Self {
        Self {
            generation,
            depth,
            contract,
            caller,
            code_hash,
            selector: PrefetchAbiDecoder::selector(calldata),
            calldata,
        }
    }
}

/// Actual callsite context observed before dispatching an external call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchCallsiteContext<'a> {
    /// Parent frame context.
    pub parent: PrefetchFrameContext<'a>,
    /// Child callee address.
    pub callee: Address,
    /// Kind of external call.
    pub call_kind: PrefetchExternalCallKind,
    /// Actual child calldata slice.
    pub calldata: &'a [u8],
}

impl<'a> PrefetchCallsiteContext<'a> {
    /// Creates a child frame context from this callsite and an optional child code hash.
    pub fn child_frame_context(&self, child_code_hash: Option<B256>) -> PrefetchFrameContext<'a> {
        PrefetchFrameContext::new(
            self.parent.generation,
            self.parent.depth.saturating_add(1),
            self.callee,
            self.parent.contract,
            child_code_hash,
            self.calldata,
        )
    }
}

/// Frontier planning output for one frame or callsite.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FrontierPrefetchPlan {
    /// Concrete tasks to schedule immediately.
    pub tasks: Vec<PrefetchTask>,
    /// Speculative child-call predictions for deeper tree prefetching.
    pub predicted_calls: Vec<ChildCallPrediction>,
}

impl FrontierPrefetchPlan {
    /// Creates an empty plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one task to the plan.
    pub fn push_task(&mut self, task: PrefetchTask) {
        self.tasks.push(task);
    }

    /// Adds one child-call prediction to the plan.
    pub fn push_prediction(&mut self, prediction: ChildCallPrediction) {
        self.predicted_calls.push(prediction);
    }

    /// Extends the plan with additional tasks.
    pub fn extend_tasks(&mut self, tasks: impl IntoIterator<Item = PrefetchTask>) {
        self.tasks.extend(tasks);
    }

    /// Extends the plan with additional predictions.
    pub fn extend_predictions(
        &mut self,
        predictions: impl IntoIterator<Item = ChildCallPrediction>,
    ) {
        self.predicted_calls.extend(predictions);
    }

    /// Sorts tasks by descending priority, then by earliest-use rank and depth.
    pub fn sort_tasks(&mut self) {
        self.tasks.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then(left.earliest_use_rank.cmp(&right.earliest_use_rank))
                .then(left.depth.cmp(&right.depth))
        });
    }
}

/// Planner interface for frame-entry and pre-call deeper-tree prefetching.
pub trait FrontierFramePlanner: Debug + Send + Sync {
    /// Plans work when a frame is first entered.
    fn plan_frame_enter(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan;

    /// Plans additional work immediately before an external call.
    fn plan_before_call(&self, context: &PrefetchCallsiteContext<'_>) -> FrontierPrefetchPlan;
}

/// Shared planner handle used by the registry.
pub type FrontierPlannerHandle = Arc<dyn FrontierFramePlanner>;

/// Registry that dispatches planners by `(code_hash, selector)` with wildcard fallback.
#[derive(Debug, Clone, Default)]
pub struct FrontierPlannerRegistry {
    planners: HashMap<Option<B256>, HashMap<DowseSelector, FrontierPlannerHandle>>,
}

impl FrontierPlannerRegistry {
    /// Creates an empty planner registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a planner for `code_hash` and `selector`.
    pub fn register(
        &mut self,
        code_hash: Option<B256>,
        selector: DowseSelector,
        planner: FrontierPlannerHandle,
    ) {
        self.planners.entry(code_hash).or_default().insert(selector, planner);
    }

    /// Plans frame-entry work using the best-matching planner, if any.
    pub fn plan_frame_enter(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan {
        self.lookup(context.code_hash, context.selector)
            .map_or_else(FrontierPrefetchPlan::new, |planner| planner.plan_frame_enter(context))
    }

    /// Plans deeper-tree work immediately before an external call.
    pub fn plan_before_call(&self, context: &PrefetchCallsiteContext<'_>) -> FrontierPrefetchPlan {
        self.lookup(context.parent.code_hash, context.parent.selector)
            .map_or_else(FrontierPrefetchPlan::new, |planner| planner.plan_before_call(context))
    }

    /// Returns the best matching planner for `code_hash` and `selector`.
    pub fn lookup(
        &self,
        code_hash: Option<B256>,
        selector: DowseSelector,
    ) -> Option<FrontierPlannerHandle> {
        self.lookup_exact(code_hash, selector)
            .or_else(|| self.lookup_exact(code_hash, None))
            .or_else(|| self.lookup_exact(None, selector))
            .or_else(|| self.lookup_exact(None, None))
    }

    /// Returns the exact planner registered for `code_hash` and `selector`, if any.
    pub fn lookup_exact(
        &self,
        code_hash: Option<B256>,
        selector: DowseSelector,
    ) -> Option<FrontierPlannerHandle> {
        self.planners.get(&code_hash).and_then(|selectors| selectors.get(&selector).cloned())
    }
}

/// Planner for standard ERC-20 frame-local reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Erc20FrontierPlanner {
    /// ERC-20 storage layout.
    pub storage_layout: Erc20StorageLayout,
}

impl Erc20FrontierPlanner {
    /// Creates a new ERC-20 planner.
    pub const fn new(storage_layout: Erc20StorageLayout) -> Self {
        Self { storage_layout }
    }

    /// Builds frame-local tasks for transfer-like ERC-20 calls.
    pub fn plan_transfer_like(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan {
        let tx_context = if PrefetchWellKnownSelectors::matches(
            context.selector,
            PrefetchWellKnownSelectors::ERC20_TRANSFER,
        ) {
            PrefetchAbiDecoder::address_arg(context.calldata, 0).map(|to| Erc20Context {
                token: context.contract,
                from: context.caller,
                to,
                spender: context.caller,
                tx_shape: TxShape::Transfer,
                layout: self.storage_layout,
            })
        } else if PrefetchWellKnownSelectors::matches(
            context.selector,
            PrefetchWellKnownSelectors::ERC20_TRANSFER_FROM,
        ) {
            PrefetchAbiDecoder::address_arg(context.calldata, 0)
                .zip(PrefetchAbiDecoder::address_arg(context.calldata, 1))
                .map(|(from, to)| Erc20Context {
                    token: context.contract,
                    from,
                    to,
                    spender: context.caller,
                    tx_shape: TxShape::TransferFrom,
                    layout: self.storage_layout,
                })
        } else {
            None
        };

        let Some(tx_context) = tx_context else {
            return FrontierPrefetchPlan::new();
        };

        let mut plan = FrontierPrefetchPlan::new();
        for (index, (address, slot)) in
            PrefetchHintBuilder::erc20_standard(&tx_context, &[]).into_iter().enumerate()
        {
            plan.push_task(PrefetchTask::storage(
                context.generation,
                context.depth,
                900_u32.saturating_sub(index as u32),
                10_000,
                index as u16,
                address,
                slot,
            ));
        }
        plan
    }

    /// Builds frame-local tasks for `balanceOf(address)`.
    pub fn plan_balance_of(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan {
        if !PrefetchWellKnownSelectors::matches(
            context.selector,
            PrefetchWellKnownSelectors::ERC20_BALANCE_OF,
        ) {
            return FrontierPrefetchPlan::new();
        }
        let Some(owner) = PrefetchAbiDecoder::address_arg(context.calldata, 0) else {
            return FrontierPrefetchPlan::new();
        };
        let slot =
            PrefetchHintBuilder::erc20_balance_slot(owner, self.storage_layout.balances_slot);
        let mut plan = FrontierPrefetchPlan::new();
        plan.push_task(PrefetchTask::storage(
            context.generation,
            context.depth,
            900,
            10_000,
            0,
            context.contract,
            slot,
        ));
        plan
    }
}

impl FrontierFramePlanner for Erc20FrontierPlanner {
    fn plan_frame_enter(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan {
        let mut plan = self.plan_transfer_like(context);
        if plan.tasks.is_empty() {
            plan = self.plan_balance_of(context);
        }
        plan.sort_tasks();
        plan
    }

    fn plan_before_call(&self, _context: &PrefetchCallsiteContext<'_>) -> FrontierPrefetchPlan {
        FrontierPrefetchPlan::new()
    }
}

/// Planner for WETH9 frame-local reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Weth9FrontierPlanner {
    /// Underlying ERC-20-like storage layout used by WETH9.
    pub storage_layout: Erc20StorageLayout,
}

impl Weth9FrontierPlanner {
    /// Creates a new WETH9 planner.
    pub const fn new(storage_layout: Erc20StorageLayout) -> Self {
        Self { storage_layout }
    }

    /// Builds frame-local tasks for WETH9-specific operations.
    pub fn plan_weth_specific(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan {
        if !(PrefetchWellKnownSelectors::matches(
            context.selector,
            PrefetchWellKnownSelectors::WETH9_DEPOSIT,
        ) || PrefetchWellKnownSelectors::matches(
            context.selector,
            PrefetchWellKnownSelectors::WETH9_WITHDRAW,
        )) {
            return FrontierPrefetchPlan::new();
        }

        let slot = PrefetchHintBuilder::erc20_balance_slot(
            context.caller,
            self.storage_layout.balances_slot,
        );
        let mut plan = FrontierPrefetchPlan::new();
        plan.push_task(PrefetchTask::storage(
            context.generation,
            context.depth,
            950,
            10_000,
            0,
            context.contract,
            slot,
        ));
        if let Some(paused_slot) = self.storage_layout.paused_slot {
            plan.push_task(PrefetchTask::storage(
                context.generation,
                context.depth,
                975,
                10_000,
                0,
                context.contract,
                paused_slot,
            ));
        }
        plan
    }
}

impl FrontierFramePlanner for Weth9FrontierPlanner {
    fn plan_frame_enter(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan {
        let mut plan = self.plan_weth_specific(context);
        if plan.tasks.is_empty() {
            let erc20 = Erc20FrontierPlanner::new(self.storage_layout);
            plan = erc20.plan_frame_enter(context);
        }
        plan.sort_tasks();
        plan
    }

    fn plan_before_call(&self, _context: &PrefetchCallsiteContext<'_>) -> FrontierPrefetchPlan {
        FrontierPrefetchPlan::new()
    }
}

/// Planner for Uniswap V2 pair contracts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniswapV2PairFrontierPlanner {
    /// Token0 address.
    pub token0: Address,
    /// Token1 address.
    pub token1: Address,
    /// Optional known code hash for token0.
    pub token0_code_hash: Option<B256>,
    /// Optional known code hash for token1.
    pub token1_code_hash: Option<B256>,
    /// Storage slots that are frequently read before swap/mint/burn logic continues.
    pub hot_read_slots: Vec<StorageKey>,
}

impl UniswapV2PairFrontierPlanner {
    /// Creates a new Uniswap V2 pair planner.
    pub const fn new(
        token0: Address,
        token1: Address,
        token0_code_hash: Option<B256>,
        token1_code_hash: Option<B256>,
        hot_read_slots: Vec<StorageKey>,
    ) -> Self {
        Self { token0, token1, token0_code_hash, token1_code_hash, hot_read_slots }
    }

    /// Creates a planner with the common V2 reserve slot default.
    pub fn with_default_reserve_slot(
        token0: Address,
        token1: Address,
        token0_code_hash: Option<B256>,
        token1_code_hash: Option<B256>,
    ) -> Self {
        Self::new(token0, token1, token0_code_hash, token1_code_hash, vec![StorageKey::from(8_u64)])
    }

    /// Builds frame-local reserve/config tasks plus speculative token calls.
    pub fn plan_pair_frame(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan {
        let is_swap = PrefetchWellKnownSelectors::matches(
            context.selector,
            PrefetchWellKnownSelectors::UNISWAP_V2_PAIR_SWAP,
        );
        let is_mint = PrefetchWellKnownSelectors::matches(
            context.selector,
            PrefetchWellKnownSelectors::UNISWAP_V2_PAIR_MINT,
        );
        let is_burn = PrefetchWellKnownSelectors::matches(
            context.selector,
            PrefetchWellKnownSelectors::UNISWAP_V2_PAIR_BURN,
        );
        if !(is_swap || is_mint || is_burn) {
            return FrontierPrefetchPlan::new();
        }

        let mut plan = FrontierPrefetchPlan::new();
        for (index, slot) in self.hot_read_slots.iter().copied().enumerate() {
            plan.push_task(PrefetchTask::storage(
                context.generation,
                context.depth,
                950_u32.saturating_sub(index as u32),
                10_000,
                index as u16,
                context.contract,
                slot,
            ));
        }

        let next_depth = context.depth.saturating_add(1);
        let token0_balance_of = ChildCallPrediction {
            callee: self.token0,
            code_hash: self.token0_code_hash,
            selector: PrefetchWellKnownSelectors::selector(
                PrefetchWellKnownSelectors::ERC20_BALANCE_OF,
            ),
            confidence_x10000: 8_500,
            earliest_use_rank: 0,
        };
        let token1_balance_of = ChildCallPrediction {
            callee: self.token1,
            code_hash: self.token1_code_hash,
            selector: PrefetchWellKnownSelectors::selector(
                PrefetchWellKnownSelectors::ERC20_BALANCE_OF,
            ),
            confidence_x10000: 8_500,
            earliest_use_rank: 1,
        };
        plan.push_prediction(token0_balance_of);
        plan.push_prediction(token1_balance_of);
        plan.push_task(token0_balance_of.into_account_code_task(
            context.generation,
            next_depth,
            820,
        ));
        plan.push_task(token1_balance_of.into_account_code_task(
            context.generation,
            next_depth,
            810,
        ));

        if is_swap {
            if PrefetchAbiDecoder::nonzero_u256_arg(context.calldata, 0) {
                let prediction = ChildCallPrediction {
                    callee: self.token0,
                    code_hash: self.token0_code_hash,
                    selector: PrefetchWellKnownSelectors::selector(
                        PrefetchWellKnownSelectors::ERC20_TRANSFER,
                    ),
                    confidence_x10000: 9_000,
                    earliest_use_rank: 0,
                };
                plan.push_prediction(prediction);
                plan.push_task(prediction.into_account_code_task(
                    context.generation,
                    next_depth,
                    880,
                ));
            }
            if PrefetchAbiDecoder::nonzero_u256_arg(context.calldata, 1) {
                let prediction = ChildCallPrediction {
                    callee: self.token1,
                    code_hash: self.token1_code_hash,
                    selector: PrefetchWellKnownSelectors::selector(
                        PrefetchWellKnownSelectors::ERC20_TRANSFER,
                    ),
                    confidence_x10000: 9_000,
                    earliest_use_rank: 1,
                };
                plan.push_prediction(prediction);
                plan.push_task(prediction.into_account_code_task(
                    context.generation,
                    next_depth,
                    870,
                ));
            }
        }

        plan
    }
}

impl FrontierFramePlanner for UniswapV2PairFrontierPlanner {
    fn plan_frame_enter(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan {
        let mut plan = self.plan_pair_frame(context);
        plan.sort_tasks();
        plan
    }

    fn plan_before_call(&self, _context: &PrefetchCallsiteContext<'_>) -> FrontierPrefetchPlan {
        FrontierPrefetchPlan::new()
    }
}

/// Configuration needed to derive Uniswap V2 pair addresses from a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniswapV2FactoryConfig {
    /// Factory address.
    pub factory: Address,
    /// Pair init-code hash.
    pub pair_init_code_hash: B256,
    /// Optional known pair runtime code hash.
    pub pair_code_hash: Option<B256>,
}

impl UniswapV2FactoryConfig {
    /// Derives the pair address for `(token_a, token_b)`.
    pub fn pair_address(&self, token_a: Address, token_b: Address) -> Address {
        let (token0, token1) = Self::sorted_tokens(token_a, token_b);
        let mut salt_preimage = [0_u8; 40];
        salt_preimage[..20].copy_from_slice(token0.as_slice());
        salt_preimage[20..].copy_from_slice(token1.as_slice());
        let salt = keccak256(salt_preimage);

        let mut preimage = [0_u8; 85];
        preimage[0] = 0xff;
        preimage[1..21].copy_from_slice(self.factory.as_slice());
        preimage[21..53].copy_from_slice(salt.as_slice());
        preimage[53..85].copy_from_slice(self.pair_init_code_hash.as_slice());

        let hash = keccak256(preimage);
        Address::from_slice(&hash.as_slice()[12..32])
    }

    /// Sorts tokens into canonical `(token0, token1)` order.
    pub fn sorted_tokens(token_a: Address, token_b: Address) -> (Address, Address) {
        if token_a.as_slice() <= token_b.as_slice() {
            (token_a, token_b)
        } else {
            (token_b, token_a)
        }
    }
}

/// Planner for Uniswap V2 router swap calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniswapV2RouterFrontierPlanner {
    /// Factory config used to derive pair addresses from path hops.
    pub factory: UniswapV2FactoryConfig,
    /// Optional WETH address used by the router.
    pub weth: Option<Address>,
    /// Known token runtime code hashes.
    pub token_code_hashes: HashMap<Address, B256>,
}

impl UniswapV2RouterFrontierPlanner {
    /// Creates a new router planner.
    pub const fn new(
        factory: UniswapV2FactoryConfig,
        weth: Option<Address>,
        token_code_hashes: HashMap<Address, B256>,
    ) -> Self {
        Self { factory, weth, token_code_hashes }
    }

    /// Returns the known code hash for `address`, if available.
    pub fn code_hash(&self, address: Address) -> Option<B256> {
        self.token_code_hashes.get(&address).copied()
    }

    /// Returns the ABI path argument index for the current selector.
    pub fn path_arg_index(&self, selector: DowseSelector) -> Option<usize> {
        if [
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_ETH_FOR_TOKENS,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_ETH_FOR_EXACT_TOKENS,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_ETH_FOR_TOKENS_FEE_ON_TRANSFER,
        ]
        .into_iter()
        .any(|known| PrefetchWellKnownSelectors::matches(selector, known))
        {
            return Some(1);
        }

        if [
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_TOKENS,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_TOKENS_FOR_EXACT_TOKENS,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_ETH,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_TOKENS_FOR_EXACT_ETH,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_TOKENS_FEE_ON_TRANSFER,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_ETH_FEE_ON_TRANSFER,
        ]
        .into_iter()
        .any(|known| PrefetchWellKnownSelectors::matches(selector, known))
        {
            return Some(2);
        }

        None
    }

    /// Returns `true` if the selector implies the router will first pull tokens from the caller.
    pub fn requires_input_transfer_from(&self, selector: DowseSelector) -> bool {
        [
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_TOKENS,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_TOKENS_FOR_EXACT_TOKENS,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_ETH,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_TOKENS_FOR_EXACT_ETH,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_TOKENS_FEE_ON_TRANSFER,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_ETH_FEE_ON_TRANSFER,
        ]
        .into_iter()
        .any(|known| PrefetchWellKnownSelectors::matches(selector, known))
    }

    /// Returns `true` if the selector implies the router will wrap ETH into WETH.
    pub fn requires_weth_deposit(&self, selector: DowseSelector) -> bool {
        [
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_ETH_FOR_TOKENS,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_ETH_FOR_EXACT_TOKENS,
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_ETH_FOR_TOKENS_FEE_ON_TRANSFER,
        ]
        .into_iter()
        .any(|known| PrefetchWellKnownSelectors::matches(selector, known))
    }

    /// Parses the router path from calldata.
    pub fn path(&self, calldata: &[u8], selector: DowseSelector) -> Option<Vec<Address>> {
        let path_index = self.path_arg_index(selector)?;
        PrefetchAbiDecoder::address_array_arg(calldata, path_index)
    }

    /// Builds speculative token/pair warming tasks for a router frame.
    pub fn plan_router_frame(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan {
        let Some(path) = self.path(context.calldata, context.selector) else {
            return FrontierPrefetchPlan::new();
        };
        if path.len() < 2 {
            return FrontierPrefetchPlan::new();
        }

        let mut plan = FrontierPrefetchPlan::new();
        let next_depth = context.depth.saturating_add(1);
        let mut seen_accounts = HashSet::new();

        for (index, token) in path.iter().copied().enumerate() {
            if seen_accounts.insert(token) {
                let prediction = ChildCallPrediction {
                    callee: token,
                    code_hash: self.code_hash(token),
                    selector: None,
                    confidence_x10000: 8_000,
                    earliest_use_rank: index as u16,
                };
                plan.push_prediction(prediction);
                plan.push_task(prediction.into_account_code_task(
                    context.generation,
                    next_depth,
                    760_u32.saturating_sub(index as u32),
                ));
            }
        }

        for (index, window) in path.windows(2).enumerate() {
            let pair = self.factory.pair_address(window[0], window[1]);
            if seen_accounts.insert(pair) {
                let prediction = ChildCallPrediction {
                    callee: pair,
                    code_hash: self.factory.pair_code_hash,
                    selector: PrefetchWellKnownSelectors::selector(
                        PrefetchWellKnownSelectors::UNISWAP_V2_PAIR_SWAP,
                    ),
                    confidence_x10000: 9_000,
                    earliest_use_rank: index as u16,
                };
                plan.push_prediction(prediction);
                plan.push_task(prediction.into_account_code_task(
                    context.generation,
                    next_depth,
                    850_u32.saturating_sub(index as u32),
                ));
            }
        }

        if self.requires_input_transfer_from(context.selector) {
            let prediction = ChildCallPrediction {
                callee: path[0],
                code_hash: self.code_hash(path[0]),
                selector: PrefetchWellKnownSelectors::selector(
                    PrefetchWellKnownSelectors::ERC20_TRANSFER_FROM,
                ),
                confidence_x10000: 9_000,
                earliest_use_rank: 0,
            };
            plan.push_prediction(prediction);
            plan.push_task(prediction.into_account_code_task(context.generation, next_depth, 920));
        }

        if self.requires_weth_deposit(context.selector)
            && let Some(weth) = self.weth
        {
            let prediction = ChildCallPrediction {
                callee: weth,
                code_hash: self.code_hash(weth),
                selector: PrefetchWellKnownSelectors::selector(
                    PrefetchWellKnownSelectors::WETH9_DEPOSIT,
                ),
                confidence_x10000: 9_500,
                earliest_use_rank: 0,
            };
            plan.push_prediction(prediction);
            plan.push_task(prediction.into_account_code_task(context.generation, next_depth, 940));
        }

        plan
    }
}

impl FrontierFramePlanner for UniswapV2RouterFrontierPlanner {
    fn plan_frame_enter(&self, context: &PrefetchFrameContext<'_>) -> FrontierPrefetchPlan {
        let mut plan = self.plan_router_frame(context);
        plan.sort_tasks();
        plan
    }

    fn plan_before_call(&self, context: &PrefetchCallsiteContext<'_>) -> FrontierPrefetchPlan {
        let _ = context;
        FrontierPrefetchPlan::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use alloy_primitives::{Address, B256, address};

    use super::{
        ChildCallPrediction, Erc20FrontierPlanner, FrontierFramePlanner, FrontierPlannerRegistry,
        PrefetchAbiDecoder, PrefetchFrameContext, PrefetchWellKnownSelectors,
        UniswapV2FactoryConfig, UniswapV2PairFrontierPlanner, UniswapV2RouterFrontierPlanner,
        Weth9FrontierPlanner,
    };
    use crate::Erc20StorageLayout;

    fn encode_address_word(address: Address) -> [u8; 32] {
        let mut word = [0_u8; 32];
        word[12..32].copy_from_slice(address.as_slice());
        word
    }

    fn encode_u256_word(value: u64) -> [u8; 32] {
        let mut word = [0_u8; 32];
        word[24..32].copy_from_slice(&value.to_be_bytes());
        word
    }

    fn encode_router_path_call(selector: [u8; 4], path: &[Address]) -> Vec<u8> {
        let mut calldata = Vec::new();
        calldata.extend_from_slice(&selector);
        calldata.extend_from_slice(&encode_u256_word(1));
        calldata.extend_from_slice(&encode_u256_word(1));
        calldata.extend_from_slice(&encode_u256_word(5 * 32));
        calldata.extend_from_slice(&encode_address_word(Address::with_last_byte(0x42)));
        calldata.extend_from_slice(&encode_u256_word(999));
        calldata.extend_from_slice(&encode_u256_word(path.len() as u64));
        for address in path {
            calldata.extend_from_slice(&encode_address_word(*address));
        }
        calldata
    }

    #[test]
    fn abi_decoder_extracts_dynamic_address_array() {
        let path = vec![
            Address::with_last_byte(0x01),
            Address::with_last_byte(0x02),
            Address::with_last_byte(0x03),
        ];
        let calldata = encode_router_path_call(
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_TOKENS,
            &path,
        );

        let decoded = PrefetchAbiDecoder::address_array_arg(&calldata, 2).expect("path");
        assert_eq!(decoded, path);
    }

    #[test]
    fn erc20_frontier_planner_prefetches_transfer_from_slots() {
        let token = address!("4200000000000000000000000000000000000006");
        let from = address!("0000000000000000000000000000000000001337");
        let to = address!("0000000000000000000000000000000000001338");
        let spender = address!("0000000000000000000000000000000000001339");
        let selector = PrefetchWellKnownSelectors::ERC20_TRANSFER_FROM;
        let mut calldata = Vec::new();
        calldata.extend_from_slice(&selector);
        calldata.extend_from_slice(&encode_address_word(from));
        calldata.extend_from_slice(&encode_address_word(to));
        calldata.extend_from_slice(&encode_u256_word(1));

        let planner = Erc20FrontierPlanner::new(Erc20StorageLayout {
            paused_slot: Some(revm::primitives::StorageKey::from(9_u64)),
            ..Default::default()
        });
        let context = PrefetchFrameContext::new(7, 1, token, spender, None, &calldata);
        let plan = planner.plan_frame_enter(&context);

        assert_eq!(plan.tasks.len(), 4);
        assert!(plan.tasks.iter().any(|task| matches!(
            task.kind,
            super::PrefetchTaskKind::Storage { address, slot }
                if address == token && slot == revm::primitives::StorageKey::from(9_u64)
        )));
    }

    #[test]
    fn weth9_frontier_planner_prefetches_caller_balance_for_deposit() {
        let weth = Address::with_last_byte(0x0b);
        let caller = Address::with_last_byte(0x99);
        let calldata = PrefetchWellKnownSelectors::WETH9_DEPOSIT.to_vec();
        let planner = Weth9FrontierPlanner::new(Erc20StorageLayout::default());
        let context = PrefetchFrameContext::new(1, 0, weth, caller, None, &calldata);

        let plan = planner.plan_frame_enter(&context);
        assert_eq!(plan.tasks.len(), 1);
        assert!(plan.tasks.iter().all(|task| matches!(
            task.kind,
            super::PrefetchTaskKind::Storage { address, .. } if address == weth
        )));
    }

    #[test]
    fn uniswap_v2_pair_planner_prefetches_reserves_and_output_token() {
        let pair = Address::with_last_byte(0x77);
        let token0 = Address::with_last_byte(0x10);
        let token1 = Address::with_last_byte(0x11);
        let selector = PrefetchWellKnownSelectors::UNISWAP_V2_PAIR_SWAP;
        let mut calldata = Vec::new();
        calldata.extend_from_slice(&selector);
        calldata.extend_from_slice(&encode_u256_word(5));
        calldata.extend_from_slice(&encode_u256_word(0));
        calldata.extend_from_slice(&encode_address_word(Address::with_last_byte(0x20)));
        calldata.extend_from_slice(&encode_u256_word(4 * 32));
        calldata.extend_from_slice(&encode_u256_word(0));

        let planner = UniswapV2PairFrontierPlanner::with_default_reserve_slot(
            token0,
            token1,
            Some(B256::with_last_byte(1)),
            Some(B256::with_last_byte(2)),
        );
        let context = PrefetchFrameContext::new(4, 1, pair, Address::ZERO, None, &calldata);
        let plan = planner.plan_frame_enter(&context);

        assert!(plan.tasks.iter().any(|task| matches!(
            task.kind,
            super::PrefetchTaskKind::Storage { address, slot }
                if address == pair && slot == revm::primitives::StorageKey::from(8_u64)
        )));
        assert!(plan.predicted_calls.iter().any(|prediction| {
            prediction.callee == token0
                && prediction.selector
                    == PrefetchWellKnownSelectors::selector(
                        PrefetchWellKnownSelectors::ERC20_TRANSFER,
                    )
        }));
    }

    #[test]
    fn uniswap_v2_router_planner_derives_pair_and_token_warms() {
        let token0 = Address::with_last_byte(0x01);
        let weth = Address::with_last_byte(0x02);
        let token1 = Address::with_last_byte(0x03);
        let factory = UniswapV2FactoryConfig {
            factory: Address::with_last_byte(0xf0),
            pair_init_code_hash: B256::with_last_byte(0xaa),
            pair_code_hash: Some(B256::with_last_byte(0xbb)),
        };
        let calldata = encode_router_path_call(
            PrefetchWellKnownSelectors::UNISWAP_V2_ROUTER_SWAP_EXACT_TOKENS_FOR_TOKENS,
            &[token0, weth, token1],
        );
        let planner = UniswapV2RouterFrontierPlanner::new(
            factory,
            Some(weth),
            HashMap::from([
                (token0, B256::with_last_byte(1)),
                (weth, B256::with_last_byte(2)),
                (token1, B256::with_last_byte(3)),
            ]),
        );
        let context = PrefetchFrameContext::new(
            9,
            0,
            Address::with_last_byte(0x44),
            Address::ZERO,
            None,
            &calldata,
        );
        let plan = planner.plan_frame_enter(&context);

        let pair0 = factory.pair_address(token0, weth);
        let pair1 = factory.pair_address(weth, token1);
        let warmed = plan
            .tasks
            .iter()
            .filter_map(|task| match task.kind {
                super::PrefetchTaskKind::AccountCode { address } => Some(address),
                _ => None,
            })
            .collect::<HashSet<_>>();

        assert!(warmed.contains(&token0));
        assert!(warmed.contains(&weth));
        assert!(warmed.contains(&token1));
        assert!(warmed.contains(&pair0));
        assert!(warmed.contains(&pair1));
        assert!(plan.predicted_calls.iter().any(|prediction| {
            prediction.callee == token0
                && prediction.selector
                    == PrefetchWellKnownSelectors::selector(
                        PrefetchWellKnownSelectors::ERC20_TRANSFER_FROM,
                    )
        }));
    }

    #[test]
    fn registry_dispatches_registered_planners() {
        let token = Address::with_last_byte(0x99);
        let selector =
            PrefetchWellKnownSelectors::selector(PrefetchWellKnownSelectors::ERC20_BALANCE_OF);
        let calldata = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&PrefetchWellKnownSelectors::ERC20_BALANCE_OF);
            bytes.extend_from_slice(&encode_address_word(Address::with_last_byte(0x55)));
            bytes
        };
        let mut registry = FrontierPlannerRegistry::new();
        registry.register(None, selector, Arc::new(Erc20FrontierPlanner::new(Default::default())));
        let context = PrefetchFrameContext::new(2, 0, token, Address::ZERO, None, &calldata);

        let plan = registry.plan_frame_enter(&context);
        assert_eq!(plan.tasks.len(), 1);
    }

    #[test]
    fn child_prediction_converts_to_account_code_task() {
        let prediction = ChildCallPrediction {
            callee: Address::with_last_byte(0xaa),
            code_hash: Some(B256::with_last_byte(1)),
            selector: None,
            confidence_x10000: 9_000,
            earliest_use_rank: 2,
        };

        let task = prediction.into_account_code_task(5, 3, 77);
        assert!(matches!(
            task.kind,
            super::PrefetchTaskKind::AccountCode { address } if address == prediction.callee
        ));
        assert_eq!(task.generation, 5);
        assert_eq!(task.depth, 3);
    }
}
