#![forbid(unsafe_code)]

mod cli;
mod commands;
mod error;
mod logging;
mod server;

use std::process::ExitCode;

use clap::Parser;
use cli::Cli;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Err(error) = logging::init(cli.log_format) {
        eprintln!("error: {error}");
        return ExitCode::from(1);
    }

    match commands::run(cli.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(exit_code = error.exit_code(), error = %error, "command failed");
            eprintln!("error: {error}");
            ExitCode::from(error.exit_code())
        }
    }
}
