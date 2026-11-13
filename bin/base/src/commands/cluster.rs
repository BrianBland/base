//! `base cluster` subcommand group: Clustered Base maintenance.

use std::{num::NonZeroUsize, path::PathBuf};

use base_execution_cluster::{Bootstrap, BootstrapTarget, Cluster, ReadParallelism, StorageReport};
use clap::{Args, Parser, Subcommand};
use eyre::{WrapErr, ensure};

/// Clustered Base maintenance utilities.
#[derive(Parser, Debug)]
pub(crate) struct ClusterCommand {
    /// The cluster subcommand to execute.
    #[command(subcommand)]
    pub(crate) command: ClusterSubcommand,
}

/// Subcommands for `base cluster`.
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub(crate) enum ClusterSubcommand {
    /// Write clustered nodes' state databases from a stopped full node's, in one pass, keeping
    /// for each node only the rows it holds.
    #[command(name = "bootstrap")]
    Bootstrap(BootstrapArgs),
    /// List the contracts with the most storage rows, to choose `split_storage`.
    #[command(name = "storage-report")]
    StorageReport(StorageReportArgs),
}

/// Arguments of `base cluster bootstrap`.
#[derive(Args, Debug)]
pub(crate) struct BootstrapArgs {
    /// Datadir of the stopped full node to copy from.
    #[arg(long, value_name = "DATADIR")]
    pub(crate) from: PathBuf,
    /// Cluster config of a node to bootstrap; repeat once per node, paired by position with
    /// `--to`.
    #[arg(long = "cluster.config", value_name = "FILE", required = true)]
    pub(crate) cluster_config: Vec<PathBuf>,
    /// New datadir of a node to bootstrap; repeat once per `--cluster.config`. Only its `db` is
    /// written.
    #[arg(long, value_name = "DATADIR", required = true)]
    pub(crate) to: Vec<PathBuf>,
    /// Threads reading the source, each with its own read transaction; 1 reads it with one
    /// thread.
    #[arg(long, default_value_t = ReadParallelism::DEFAULT_THREADS)]
    pub(crate) read_threads: NonZeroUsize,
}

/// Arguments of `base cluster storage-report`.
#[derive(Args, Debug)]
pub(crate) struct StorageReportArgs {
    /// Datadir of the node to report on.
    #[arg(long, value_name = "DATADIR")]
    pub(crate) from: PathBuf,
    /// Number of contracts to list.
    #[arg(long, default_value_t = StorageReport::DEFAULT_TOP)]
    pub(crate) top: usize,
    /// Threads reading the database, each with its own read transaction.
    #[arg(long, default_value_t = ReadParallelism::DEFAULT_THREADS)]
    pub(crate) read_threads: NonZeroUsize,
}

impl ClusterSubcommand {
    /// Database directory of a datadir.
    const DB_DIR: &str = "db";

    pub(crate) fn run(self) -> eyre::Result<()> {
        match self {
            Self::Bootstrap(args) => {
                ensure!(
                    args.cluster_config.len() == args.to.len(),
                    "{} --cluster.config and {} --to given; pass one --to per --cluster.config",
                    args.cluster_config.len(),
                    args.to.len()
                );
                let targets = args
                    .cluster_config
                    .iter()
                    .zip(&args.to)
                    .map(|(config, to)| {
                        let cluster = Cluster::load(config)
                            .wrap_err_with(|| format!("cluster config {}", config.display()))?;
                        Ok(BootstrapTarget { cluster, destination: to.join(Self::DB_DIR) })
                    })
                    .collect::<eyre::Result<Vec<_>>>()?;
                let report = Bootstrap::new(targets)?
                    .with_read_parallelism(ReadParallelism::with_threads(args.read_threads))
                    .run(&args.from.join(Self::DB_DIR))?;
                println!("{report}");
                Ok(())
            }
            Self::StorageReport(args) => {
                let report = StorageReport::scan_with(
                    &args.from.join(Self::DB_DIR),
                    args.top,
                    ReadParallelism::with_threads(args.read_threads),
                    StorageReport::HAND_OFF_ROWS,
                )?;
                println!("{report}");
                Ok(())
            }
        }
    }
}
