use tracing_subscriber::EnvFilter;

use crate::{cli::LogFormat, error::AppError};

pub fn init(format: LogFormat) -> Result<(), AppError> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    match format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| AppError::Logging(error.to_string())),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_writer(std::io::stderr)
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| AppError::Logging(error.to_string())),
    }
}
