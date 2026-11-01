mod artifact;
mod mount;

use crate::cli::Command;
use oci_auto_http::error::AppError;

pub async fn run(command: Command) -> Result<(), AppError> {
    match command {
        Command::Serve(args) => crate::server::serve(args).await,
        Command::Artifact(args) => artifact::run(args).await,
        Command::Mount(args) => mount::run(args),
    }
}
