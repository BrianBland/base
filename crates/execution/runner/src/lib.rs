#![doc = include_str!("../README.md")]
#![doc(
    html_logo_url = "https://avatars.githubusercontent.com/u/16627100?s=200&v=4",
    html_favicon_url = "https://avatars.githubusercontent.com/u/16627100?s=200&v=4",
    issue_tracker_base_url = "https://github.com/base/base/issues/"
)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod builder;
pub use builder::{BaseNodeAdapter, BaseRpcContext, NodeHooks, RethNodeBuilder};

mod extension;
pub use extension::{BaseNodeExtension, FromExtensionConfig};

mod runner;
pub use runner::{BaseNodeRunner, LaunchedBaseNode};

mod cluster;
pub use cluster::ClusterFeed;

mod cluster_rpc;
pub use cluster_rpc::ClusterUnsupportedRpc;

mod apply;
pub use apply::{
    ApplyEngineValidator, ApplyEngineValidatorBuilder, ApplyError, BlockApplier,
    PayloadBuildingUnsupported,
};

mod root;
pub use root::ClusterStateRoot;

mod root_check;
pub use root_check::{OverlayCaptureReads, RootCheckTask};

mod service;
pub use service::{DefaultPayloadServiceBuilder, PayloadServiceBuilder};

mod types;
pub use types::{BaseComponentsBuilder, BaseNodeBuilder, BaseNodeTypes, BaseProvider};

mod node;
pub use node::BaseNode;

#[cfg(feature = "test-utils")]
pub mod test_utils;
