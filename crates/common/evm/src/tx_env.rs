use alloy_primitives::Bytes;
use base_common_consensus::Eip8130Signed;

use crate::BaseTransaction;

/// Trait for Base transaction environments. Allows to recover the transaction encoded bytes if
/// they're available.
pub trait BaseTxEnv {
    /// Exact production transaction environment for speculative identity checking.
    /// Custom transaction environments default to inline execution.
    #[cfg(feature = "parallel")]
    fn speculative_transaction(&self) -> Option<&BaseTransaction<revm::context::TxEnv>> {
        None
    }

    /// Returns the encoded bytes of the transaction.
    fn encoded_bytes(&self) -> Option<&Bytes>;

    /// Returns the signed EIP-8130 envelope for an EIP-8130 transaction, or
    /// [`None`] for every other transaction type.
    ///
    /// This lives on [`BaseTxEnv`] (rather than the richer [`BaseTxTr`], which
    /// requires a revm `Transaction` supertrait) so block-gas accounting can
    /// reach the payer authentication blob through the transaction-env bound the
    /// executor and builders already carry, without spreading a `BaseTxTr` bound
    /// through every downstream generic.
    ///
    /// [`BaseTxTr`]: crate::BaseTxTr
    fn eip8130_signed(&self) -> Option<&Eip8130Signed>;
}

impl<T: revm::context::Transaction + 'static> BaseTxEnv for BaseTransaction<T> {
    #[cfg(feature = "parallel")]
    fn speculative_transaction(&self) -> Option<&BaseTransaction<revm::context::TxEnv>> {
        (self as &dyn core::any::Any).downcast_ref()
    }

    fn encoded_bytes(&self) -> Option<&Bytes> {
        self.enveloped_tx.as_ref()
    }

    fn eip8130_signed(&self) -> Option<&Eip8130Signed> {
        self.eip8130.as_ref().map(|parts| &parts.signed)
    }
}
