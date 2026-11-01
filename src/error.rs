use std::path::PathBuf;

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
    #[error("could not initialize logging: {0}")]
    Logging(String),
}

impl AppError {
    /// Exit code 69 follows sysexits(3)'s EX_UNAVAILABLE for reserved commands.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::NotImplemented { .. } => 69,
            Self::InvalidConfig { .. } => 2,
            Self::BindListener { .. } | Self::Serve(_) | Self::Logging(_) => 1,
        }
    }
}
