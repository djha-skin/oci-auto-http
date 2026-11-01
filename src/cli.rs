use std::{net::SocketAddr, path::PathBuf};

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
    Artifact(ArtifactArgs),
    /// Create and manage HTTP-to-registry mounts.
    Mount {
        #[command(subcommand)]
        command: MountCommand,
    },
}

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Version-1 TOML configuration file.
    #[arg(long, value_name = "FILE")]
    pub config: PathBuf,
    /// Override the listen address configured in the file.
    #[arg(long)]
    pub listen: Option<SocketAddr>,
}

#[derive(Debug, Args)]
pub struct ArtifactArgs {
    /// Version-1 TOML configuration file selecting the registry mount.
    #[arg(long, value_name = "FILE")]
    pub config: PathBuf,
    /// Additional PEM CA certificate for a private registry.
    #[arg(long, global = true, value_name = "FILE")]
    pub ca_cert: Option<PathBuf>,
    #[command(subcommand)]
    pub command: ArtifactCommand,
}

#[derive(Debug, Subcommand)]
pub enum ArtifactCommand {
    /// Create a file or empty directory at a served path.
    Create {
        /// Absolute served URL path, for example /packages/release.bin.
        path: String,
        /// Read file bytes from FILE; omit or use - for standard input.
        #[arg(short, long, value_name = "FILE", conflicts_with = "directory")]
        input: Option<PathBuf>,
        /// Create an empty directory instead of a file.
        #[arg(long, conflicts_with = "input")]
        directory: bool,
    },
    /// Stream a file at a served path to standard output or FILE.
    Read {
        /// Absolute served URL path, for example /packages/release.bin.
        path: String,
        /// Write to FILE atomically after digest verification; use - for standard output.
        #[arg(short, long, value_name = "FILE", default_value = "-")]
        output: PathBuf,
    },
    /// List one page of a directory at a served path as JSON.
    List {
        /// Absolute served URL path. Omit only when the config has one mount.
        path: Option<String>,
        /// Maximum number of entries in this page.
        #[arg(long, default_value_t = 1000)]
        limit: usize,
        /// Continue a prior listing using its opaque cursor.
        #[arg(long, value_name = "TOKEN")]
        cursor: Option<String>,
    },
    /// Replace an existing file at a served path.
    Update {
        /// Absolute served URL path, for example /packages/release.bin.
        path: String,
        /// Read replacement bytes from FILE; omit or use - for standard input.
        #[arg(short, long, value_name = "FILE")]
        input: Option<PathBuf>,
    },
    /// Delete a file or directory at a served path.
    Delete {
        /// Absolute served URL path.
        path: String,
        /// Allow deleting a non-empty directory recursively.
        #[arg(long)]
        recursive: bool,
    },
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
