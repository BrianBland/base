//! Builder-specific node extensions.

use std::fmt;

use base_builder_core::SharedMeteringProvider;
use base_node_runner::{BaseNodeExtension, BaseRpcContext, FromExtensionConfig, NodeHooks};

use crate::{BaseApiExtServer, MeteringStoreExt};

/// Extension that registers the [`MeteringStoreExt`] RPC module.
#[derive(Debug)]
pub struct MeteringStoreExtension {
    metering_provider: SharedMeteringProvider,
}

impl<E> BaseNodeExtension<E> for MeteringStoreExtension
where
    E: fmt::Debug + Clone + Send + Sync + Unpin + 'static,
{
    fn apply(self: Box<Self>, hooks: NodeHooks<E>) -> NodeHooks<E> {
        let metering_provider = self.metering_provider;
        hooks.add_rpc_module(move |ctx: &mut BaseRpcContext<'_, E>| {
            let ext = MeteringStoreExt::new(metering_provider);
            ctx.modules.add_or_replace_configured(ext.into_rpc())?;
            Ok(())
        })
    }
}

impl<E> FromExtensionConfig<E> for MeteringStoreExtension
where
    E: fmt::Debug + Clone + Send + Sync + Unpin + 'static,
{
    type Config = SharedMeteringProvider;

    fn from_config(config: Self::Config) -> Self {
        Self { metering_provider: config }
    }
}
