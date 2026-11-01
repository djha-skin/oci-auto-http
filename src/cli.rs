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
    Mount(MountArgs),
}

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Version-1 TOML configuration file.
    #[arg(long, value_name = "FILE")]
    pub config: PathBuf,
    /// Additional PEM CA certificate for the configured registries.
    #[arg(long, value_name = "FILE")]
    pub ca_cert: Option<PathBuf>,
    /// PEM certificate chain for an HTTPS listener (required with --tls-key).
    #[arg(long, value_name = "FILE", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,
    /// PEM private key for an HTTPS listener (required with --tls-cert).
    #[arg(long, value_name = "FILE", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
    /// Override the listen address configured in the file.
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    /// Maximum in-flight HTTP requests (1-1024).
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=1024))]
    pub max_concurrent_requests: Option<u64>,
    /// Maximum total time for one HTTP request, in seconds (1-3600).
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub request_timeout_seconds: Option<u64>,
    /// Maximum duration of one upstream registry HTTP operation, in seconds (1-3600).
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub registry_request_timeout_seconds: Option<u64>,
    /// Maximum idle time between HTTP response chunks, in seconds (1-3600).
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub stream_idle_timeout_seconds: Option<u64>,
    /// Maximum rendered byte size of one directory autoindex spool.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=1_073_741_824))]
    pub max_autoindex_spool_bytes: Option<u64>,
    /// Maximum aggregate autoindex spool storage across in-flight requests.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=8_589_934_592))]
    pub max_autoindex_spool_total_bytes: Option<u64>,
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
    /// Stream a directory listing at a served path as newline-delimited JSON.
    List {
        /// Absolute served URL path. Omit only when the config has one mount.
        path: Option<String>,
        /// Maximum entries fetched per registry-backed page.
        #[arg(long, default_value_t = 1000)]
        limit: usize,
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

#[derive(Debug, Args)]
pub struct MountArgs {
    /// Version-1 TOML configuration file to manage.
    #[arg(long, value_name = "FILE")]
    pub config: PathBuf,
    #[command(subcommand)]
    pub command: MountCommand,
}

#[derive(Debug, Subcommand)]
pub enum MountCommand {
    /// Create a mount backed by an OCI registry repository.
    Create {
        /// Stable mount name.
        name: String,
        /// Canonical absolute URL prefix.
        #[arg(long)]
        prefix: String,
        /// HTTPS registry origin.
        #[arg(long)]
        registry: String,
        /// OCI repository name.
        #[arg(long)]
        repository: String,
        /// Root OCI tag (defaults to oci-auto-http-tree-v1).
        #[arg(long)]
        root_tag: Option<String>,
        /// Exact HTTPS authority allowed for blob redirects (repeatable).
        #[arg(long, value_name = "AUTHORITY", action = clap::ArgAction::Append)]
        redirect_allowlist: Vec<String>,
    },
    /// List configured mounts as a JSON array.
    List,
    /// Show one configured mount as JSON.
    Show { name: String },
    /// Update explicitly selected fields of a configured mount.
    Update {
        /// Stable mount name (cannot be changed).
        name: String,
        /// New canonical absolute URL prefix.
        #[arg(long)]
        prefix: Option<String>,
        /// New HTTPS registry origin.
        #[arg(long)]
        registry: Option<String>,
        /// New OCI repository name.
        #[arg(long)]
        repository: Option<String>,
        /// New root OCI tag.
        #[arg(long, conflicts_with = "reset_root_tag")]
        root_tag: Option<String>,
        /// Reset the root tag to its default.
        #[arg(long, conflicts_with = "root_tag")]
        reset_root_tag: bool,
        /// Replace the redirect allowlist (repeatable).
        #[arg(long, value_name = "AUTHORITY", action = clap::ArgAction::Append, conflicts_with = "clear_redirect_allowlist")]
        redirect_allowlist: Option<Vec<String>>,
        /// Remove all permitted blob redirect authorities.
        #[arg(long, conflicts_with = "redirect_allowlist")]
        clear_redirect_allowlist: bool,
    },
    /// Delete a configured mount. The final mount cannot be deleted.
    Delete { name: String },
}
