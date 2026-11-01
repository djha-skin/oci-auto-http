use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "oci-auto-http",
    version,
    about = "Serve OCI-backed byte trees over HTTP",
    long_about = "Serve OCI-backed byte trees over HTTP. Durable content is stored in OCI registries."
)]
pub struct Cli {
    /// Log format written to stderr.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Text)]
    pub log_format: LogFormat,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start the HTTP listener.
    Serve(ServeArgs),
    /// Create, read, list, update, or delete tree artifacts.
    Artifact {
        #[command(subcommand)]
        command: ArtifactCommand,
    },
    /// Create and manage HTTP-to-registry mounts.
    Mount {
        #[command(subcommand)]
        command: MountCommand,
    },
}

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// TCP address on which to listen.
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,
}

#[derive(Debug, Subcommand)]
pub enum ArtifactCommand {
    /// Create a file or directory at a served path.
    Create { path: String },
    /// Read a file or list a directory at a served path.
    Read { path: String },
    /// List a directory at a served path.
    List { path: Option<String> },
    /// Replace a file at a served path.
    Update { path: String },
    /// Delete a file or directory at a served path.
    Delete { path: String },
}

#[derive(Debug, Subcommand)]
pub enum MountCommand {
    /// Create a mount backed by an OCI registry repository.
    Create {
        name: String,
        #[arg(long)]
        prefix: String,
        #[arg(long)]
        registry: String,
        #[arg(long)]
        repository: String,
        #[arg(long, default_value = "oci-auto-http-tree-v1")]
        root_tag: String,
    },
    /// List configured mounts.
    List,
    /// Show one configured mount.
    Show { name: String },
    /// Update a configured mount.
    Update { name: String },
    /// Delete a configured mount.
    Delete { name: String },
}
