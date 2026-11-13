//! The RPC methods a clustered node refuses.

use eyre::Result;
use jsonrpsee::{
    RpcModule,
    types::{ErrorObject, ErrorObjectOwned, error::METHOD_NOT_FOUND_CODE},
};
use reth_rpc_server_types::RethRpcModule;

use crate::BaseRpcContext;

/// The RPC methods that compute proofs, witnesses or state roots from the state trie. A clustered
/// node holds only its slice of that trie, so they would fail deep inside or answer from partial
/// data; on a clustered node each is replaced by a method that refuses with
/// [`METHOD_NOT_FOUND_CODE`] and a message naming the reason.
#[derive(Debug, Clone, Copy)]
pub struct ClusterUnsupportedRpc;

impl ClusterUnsupportedRpc {
    /// Refused methods of the `eth` namespace.
    pub const ETH_METHODS: [&'static str; 3] =
        ["eth_getProof", "eth_getMultiProof", "eth_getAccount"];

    /// Refused methods of the `debug` namespace.
    pub const DEBUG_METHODS: [&'static str; 6] = [
        "debug_executionWitness",
        "debug_executionWitnessByBlockHash",
        "debug_executePayload",
        "debug_accountAt",
        "debug_stateRootWithUpdates",
        "debug_intermediateRoots",
    ];

    /// Replaces the refused methods in every transport where their namespace is configured.
    pub fn install(ctx: &mut BaseRpcContext<'_>) -> Result<()> {
        ctx.modules.add_or_replace_if_module_configured(
            RethRpcModule::Eth,
            Self::module(&Self::ETH_METHODS)?,
        )?;
        ctx.modules.add_or_replace_if_module_configured(
            RethRpcModule::Debug,
            Self::module(&Self::DEBUG_METHODS)?,
        )?;
        Ok(())
    }

    /// A module in which each of `methods` refuses (see [`Self::refusal`]).
    pub fn module(methods: &[&'static str]) -> Result<RpcModule<()>> {
        let mut module = RpcModule::new(());
        for &method in methods {
            module.register_method(method, move |_, _, _| Err::<(), _>(Self::refusal(method)))?;
        }
        Ok(module)
    }

    /// The error answering `method` on a clustered node.
    pub fn refusal(method: &str) -> ErrorObjectOwned {
        ErrorObject::owned(
            METHOD_NOT_FOUND_CODE,
            format!(
                "{method} is not supported on a clustered node: it holds only a slice of the \
                 state trie; ask an unclustered node"
            ),
            None::<()>,
        )
    }
}
