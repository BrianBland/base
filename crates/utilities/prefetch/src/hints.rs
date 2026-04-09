use std::collections::HashSet;

use alloy_primitives::{Address, B256, keccak256};
use revm::primitives::StorageKey;

/// Transaction shape used by the synthetic prefetch experiment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxShape {
    /// ERC-20 `transfer(to, amount)`.
    Transfer,
    /// ERC-20 `transferFrom(from, to, amount)`.
    TransferFrom,
    /// Swap-like flow with multiple transfer legs involving a pool/router path.
    Swap,
}

/// Storage layout for an ERC-20-like token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Erc20StorageLayout {
    /// Slot index used by `mapping(address => uint256) balances`.
    pub balances_slot: StorageKey,
    /// Slot index used by `mapping(address => mapping(address => uint256)) allowances`.
    pub allowances_slot: StorageKey,
    /// Optional "paused" flag slot for pausible tokens.
    pub paused_slot: Option<StorageKey>,
}

impl Default for Erc20StorageLayout {
    fn default() -> Self {
        Self {
            // OpenZeppelin default: `_balances` at slot 0.
            balances_slot: StorageKey::ZERO,
            // OpenZeppelin default: `_allowances` at slot 1.
            allowances_slot: StorageKey::from(1_u64),
            paused_slot: None,
        }
    }
}

/// Execution context needed to derive common ERC-20 storage hints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Erc20Context {
    /// Token contract address.
    pub token: Address,
    /// Transfer source account.
    pub from: Address,
    /// Transfer destination account.
    pub to: Address,
    /// Effective caller (spender) for allowance checks.
    pub spender: Address,
    /// Transaction shape.
    pub tx_shape: TxShape,
    /// Token storage layout.
    pub layout: Erc20StorageLayout,
}

/// One ERC-20 transfer leg inside a swap-like execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Erc20SwapLeg {
    /// Transfer source account.
    pub from: Address,
    /// Transfer destination account.
    pub to: Address,
    /// Optional spender that implies an allowance read for `from`.
    pub allowance_spender: Option<Address>,
}

/// Execution context for deriving swap-oriented ERC-20 hints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Erc20SwapContext {
    /// Token contract address.
    pub token: Address,
    /// Token storage layout.
    pub layout: Erc20StorageLayout,
    /// Ordered transfer legs expected during execution.
    pub legs: Vec<Erc20SwapLeg>,
}

/// Builds storage-key hints for synthetic prefetch experiments.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchHintBuilder;

impl PrefetchHintBuilder {
    /// Builds ERC-20 storage read hints for a transfer-like execution context.
    pub fn erc20_standard(
        context: &Erc20Context,
        extra_read_slots: &[StorageKey],
    ) -> Vec<(Address, StorageKey)> {
        let mut hints = Vec::with_capacity(4 + extra_read_slots.len());
        let mut seen = HashSet::with_capacity(4 + extra_read_slots.len());

        if let Some(paused_slot) = context.layout.paused_slot {
            Self::push_unique(&mut hints, &mut seen, (context.token, paused_slot));
        }

        if context.tx_shape == TxShape::TransferFrom {
            Self::push_unique(
                &mut hints,
                &mut seen,
                (
                    context.token,
                    Self::erc20_allowance_slot(
                        context.from,
                        context.spender,
                        context.layout.allowances_slot,
                    ),
                ),
            );
        }

        Self::push_unique(
            &mut hints,
            &mut seen,
            (context.token, Self::erc20_balance_slot(context.from, context.layout.balances_slot)),
        );
        Self::push_unique(
            &mut hints,
            &mut seen,
            (context.token, Self::erc20_balance_slot(context.to, context.layout.balances_slot)),
        );

        for slot in extra_read_slots {
            Self::push_unique(&mut hints, &mut seen, (context.token, *slot));
        }

        hints
    }

    /// Builds ERC-20 storage read hints for swap-like flows with multiple transfer legs.
    pub fn erc20_swap(
        context: &Erc20SwapContext,
        extra_read_slots: &[StorageKey],
    ) -> Vec<(Address, StorageKey)> {
        let mut hints = Vec::with_capacity((context.legs.len() * 3) + 1 + extra_read_slots.len());
        let mut seen =
            HashSet::with_capacity((context.legs.len() * 3) + 1 + extra_read_slots.len());

        if let Some(paused_slot) = context.layout.paused_slot {
            Self::push_unique(&mut hints, &mut seen, (context.token, paused_slot));
        }

        for leg in &context.legs {
            if let Some(spender) = leg.allowance_spender {
                Self::push_unique(
                    &mut hints,
                    &mut seen,
                    (
                        context.token,
                        Self::erc20_allowance_slot(
                            leg.from,
                            spender,
                            context.layout.allowances_slot,
                        ),
                    ),
                );
            }
            Self::push_unique(
                &mut hints,
                &mut seen,
                (context.token, Self::erc20_balance_slot(leg.from, context.layout.balances_slot)),
            );
            Self::push_unique(
                &mut hints,
                &mut seen,
                (context.token, Self::erc20_balance_slot(leg.to, context.layout.balances_slot)),
            );
        }

        for slot in extra_read_slots {
            Self::push_unique(&mut hints, &mut seen, (context.token, *slot));
        }

        hints
    }

    /// Computes `keccak256(abi.encode(owner, balances_slot))`.
    pub fn erc20_balance_slot(owner: Address, balances_slot: StorageKey) -> StorageKey {
        Self::mapping_slot(owner, balances_slot)
    }

    /// Computes `keccak256(abi.encode(spender, keccak256(abi.encode(owner, allowances_slot))))`.
    pub fn erc20_allowance_slot(
        owner: Address,
        spender: Address,
        allowances_slot: StorageKey,
    ) -> StorageKey {
        let outer = Self::mapping_slot(owner, allowances_slot);
        let mut buf = [0_u8; 64];
        buf[12..32].copy_from_slice(spender.as_slice());
        buf[32..64].copy_from_slice(B256::new(outer.to_be_bytes::<32>()).as_slice());
        StorageKey::from_be_bytes(keccak256(buf).0)
    }

    fn mapping_slot(address: Address, slot: StorageKey) -> StorageKey {
        let mut buf = [0_u8; 64];
        buf[12..32].copy_from_slice(address.as_slice());
        buf[32..64].copy_from_slice(&slot.to_be_bytes::<32>());
        StorageKey::from_be_bytes(keccak256(buf).0)
    }

    fn push_unique(
        hints: &mut Vec<(Address, StorageKey)>,
        seen: &mut HashSet<(Address, StorageKey)>,
        hint: (Address, StorageKey),
    ) {
        if seen.insert(hint) {
            hints.push(hint);
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, address};
    use revm::primitives::StorageKey;

    use super::{
        Erc20Context, Erc20StorageLayout, Erc20SwapContext, Erc20SwapLeg, PrefetchHintBuilder,
        TxShape,
    };

    #[test]
    fn transfer_hints_exclude_allowance_slot() {
        let context = Erc20Context {
            token: address!("4200000000000000000000000000000000000006"),
            from: address!("0000000000000000000000000000000000001337"),
            to: address!("0000000000000000000000000000000000001338"),
            spender: address!("0000000000000000000000000000000000001339"),
            tx_shape: TxShape::Transfer,
            layout: Erc20StorageLayout::default(),
        };

        let hints = PrefetchHintBuilder::erc20_standard(&context, &[]);

        assert_eq!(hints.len(), 2);
    }

    #[test]
    fn transfer_from_hints_include_allowance_and_paused_slot() {
        let context = Erc20Context {
            token: Address::with_last_byte(7),
            from: Address::with_last_byte(1),
            to: Address::with_last_byte(2),
            spender: Address::with_last_byte(3),
            tx_shape: TxShape::TransferFrom,
            layout: Erc20StorageLayout {
                paused_slot: Some(StorageKey::from(9_u64)),
                ..Default::default()
            },
        };

        let hints = PrefetchHintBuilder::erc20_standard(&context, &[StorageKey::from(11_u64)]);

        assert_eq!(hints.len(), 5);
        assert!(hints.contains(&(context.token, StorageKey::from(9_u64))));
        assert!(hints.contains(&(context.token, StorageKey::from(11_u64))));
    }

    #[test]
    fn transfer_from_hint_order_matches_execution() {
        let context = Erc20Context {
            token: Address::with_last_byte(7),
            from: Address::with_last_byte(1),
            to: Address::with_last_byte(2),
            spender: Address::with_last_byte(3),
            tx_shape: TxShape::TransferFrom,
            layout: Erc20StorageLayout {
                paused_slot: Some(StorageKey::from(9_u64)),
                ..Default::default()
            },
        };
        let hints = PrefetchHintBuilder::erc20_standard(&context, &[StorageKey::from(11_u64)]);

        let paused = (context.token, StorageKey::from(9_u64));
        let allowance = (
            context.token,
            PrefetchHintBuilder::erc20_allowance_slot(
                context.from,
                context.spender,
                context.layout.allowances_slot,
            ),
        );
        let from_balance = (
            context.token,
            PrefetchHintBuilder::erc20_balance_slot(context.from, context.layout.balances_slot),
        );
        let to_balance = (
            context.token,
            PrefetchHintBuilder::erc20_balance_slot(context.to, context.layout.balances_slot),
        );

        assert_eq!(hints[0], paused);
        assert_eq!(hints[1], allowance);
        assert_eq!(hints[2], from_balance);
        assert_eq!(hints[3], to_balance);
    }

    #[test]
    fn swap_hints_include_pool_path_balances_and_allowances_without_duplicates() {
        let token = Address::with_last_byte(0xAA);
        let pool = Address::with_last_byte(0x10);
        let trader_a = Address::with_last_byte(0x11);
        let trader_b = Address::with_last_byte(0x12);
        let router = Address::with_last_byte(0x20);
        let context = Erc20SwapContext {
            token,
            layout: Erc20StorageLayout {
                paused_slot: Some(StorageKey::from(9_u64)),
                ..Default::default()
            },
            legs: vec![
                Erc20SwapLeg { from: trader_a, to: pool, allowance_spender: Some(router) },
                Erc20SwapLeg { from: pool, to: trader_b, allowance_spender: None },
                Erc20SwapLeg { from: trader_b, to: pool, allowance_spender: Some(router) },
            ],
        };

        let hints = PrefetchHintBuilder::erc20_swap(&context, &[StorageKey::from(77_u64)]);
        assert!(hints.contains(&(token, StorageKey::from(9_u64))));
        assert!(hints.contains(&(token, StorageKey::from(77_u64))));

        let pool_balance_slot = PrefetchHintBuilder::erc20_balance_slot(pool, StorageKey::ZERO);
        let pool_entries = hints
            .iter()
            .filter(|(address, slot)| *address == token && *slot == pool_balance_slot)
            .count();
        assert_eq!(pool_entries, 1);
    }

    #[test]
    fn swap_hint_order_starts_with_paused_then_first_leg_allowance() {
        let token = Address::with_last_byte(0xAA);
        let pool = Address::with_last_byte(0x10);
        let trader = Address::with_last_byte(0x11);
        let router = Address::with_last_byte(0x20);
        let context = Erc20SwapContext {
            token,
            layout: Erc20StorageLayout {
                paused_slot: Some(StorageKey::from(9_u64)),
                ..Default::default()
            },
            legs: vec![Erc20SwapLeg { from: trader, to: pool, allowance_spender: Some(router) }],
        };
        let hints = PrefetchHintBuilder::erc20_swap(&context, &[]);
        let allowance = (
            token,
            PrefetchHintBuilder::erc20_allowance_slot(
                trader,
                router,
                context.layout.allowances_slot,
            ),
        );

        assert_eq!(hints[0], (token, StorageKey::from(9_u64)));
        assert_eq!(hints[1], allowance);
    }
}
