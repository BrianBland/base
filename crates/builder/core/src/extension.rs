//! Builder API RPC extension for registering the `base_insertValidatedTransaction` endpoint.

use std::fmt;

use base_execution_txpool::{BuilderApiImpl, BuilderApiServer};
use base_node_runner::{BaseNodeExtension, BaseRpcContext, FromExtensionConfig, NodeHooks};

/// Extension that registers the Builder API RPC module (`base_insertValidatedTransaction`).
#[derive(Debug, Default)]
pub struct BuilderApiExtension;

impl<E> BaseNodeExtension<E> for BuilderApiExtension
where
    E: fmt::Debug + Clone + Send + Sync + Unpin + 'static,
{
    fn apply(self: Box<Self>, builder: NodeHooks<E>) -> NodeHooks<E> {
        builder.add_rpc_module(move |ctx: &mut BaseRpcContext<'_, E>| {
            let api = BuilderApiImpl::new(ctx.pool().clone());
            ctx.modules.merge_configured(api.into_rpc())?;
            Ok(())
        })
    }
}

impl<E> FromExtensionConfig<E> for BuilderApiExtension
where
    E: fmt::Debug + Clone + Send + Sync + Unpin + 'static,
{
    type Config = ();

    fn from_config(_config: Self::Config) -> Self {
        Self
    }
}
