use std::{io, path::PathBuf};

use crate::{
    registry::{ErrorKind, RegistryError},
    tree::TreeError,
};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("could not bind HTTP listener at {address}: {source}")]
    BindListener {
        address: std::net::SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("HTTP server failed: {0}")]
    Serve(#[from] std::io::Error),
    #[error("invalid configuration in {path}: {reason}")]
    InvalidConfig { path: PathBuf, reason: String },
    #[error("{command} is not available yet; no registry request was made")]
    NotImplemented { command: &'static str },
    #[error("artifact path must be an absolute served path matching a configured mount")]
    InvalidServedPath,
    #[error("artifact path does not match a configured mount")]
    NoMountForPath,
    #[error("artifact list path is required when the config has multiple mounts")]
    ListPathRequired,
    #[error("artifact input failed: {0}")]
    ArtifactInput(#[source] io::Error),
    #[error("artifact output failed: {0}")]
    ArtifactOutput(#[source] io::Error),
    #[error("could not read private registry CA certificate: {0}")]
    Certificate(#[source] io::Error),
    #[error("could not serialize artifact listing: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("could not prepare artifact input: {0}")]
    InputTask(#[source] tokio::task::JoinError),
    #[error("artifact operation failed: {0}")]
    Tree(#[from] TreeError),
    #[error("could not initialize OCI registry client: {0}")]
    Registry(#[from] RegistryError),
    #[error("standard input is a terminal; provide --input FILE or pipe file bytes")]
    InteractiveInput,
    #[error("could not initialize logging: {0}")]
    Logging(String),
}

impl AppError {
    /// Exit code 69 follows sysexits(3)'s EX_UNAVAILABLE for reserved commands.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::NotImplemented { .. } => 69,
            Self::InvalidConfig { .. }
            | Self::InvalidServedPath
            | Self::NoMountForPath
            | Self::ListPathRequired => 2,
            Self::Tree(error) => tree_exit_code(error),
            Self::Registry(error) => registry_exit_code(error),
            Self::ArtifactInput(_) | Self::ArtifactOutput(_) | Self::Certificate(_) => 74,
            Self::Serialize(_) | Self::InputTask(_) => 70,
            Self::InteractiveInput => 2,
            Self::BindListener { .. } | Self::Serve(_) | Self::Logging(_) => 1,
        }
    }
}

fn tree_exit_code(error: &TreeError) -> u8 {
    match error {
        TreeError::InvalidPath | TreeError::InvalidPageSize | TreeError::InvalidCursor => 2,
        TreeError::NotFound => 66,
        TreeError::AlreadyExists
        | TreeError::DirectoryNotEmpty
        | TreeError::IsDirectory
        | TreeError::NotDirectory
        | TreeError::NotFile => 73,
        TreeError::StaleCursor | TreeError::CommitOutcomeUnknown => 75,
        TreeError::InvalidFileSize | TreeError::InvalidFileContent | TreeError::IntegrityError => {
            65
        }
        TreeError::Registry(error) => registry_exit_code(error),
        TreeError::InvalidMountTarget => 78,
        TreeError::InputStream => 74,
        TreeError::CryptoFailure => 70,
    }
}

fn registry_exit_code(error: &RegistryError) -> u8 {
    match error.kind {
        ErrorKind::InvalidConfiguration | ErrorKind::InvalidReference => 2,
        ErrorKind::NotFound => 66,
        ErrorKind::Unauthorized | ErrorKind::Forbidden => 77,
        ErrorKind::InvalidResponse | ErrorKind::DigestMismatch | ErrorKind::SizeMismatch => 65,
        ErrorKind::PaginationLimit => 75,
        _ => 74,
    }
}
