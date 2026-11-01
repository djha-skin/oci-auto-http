#![forbid(unsafe_code)]

mod cli;
mod commands;
mod config;
mod error;
mod logging;
mod server;

use std::process::ExitCode;

use clap::Parser;
use cli::{Cli, LogFormat};
use error::AppError;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let log_format = cli.log_format;

    if let Err(error) = logging::init(log_format) {
        report_error(log_format, &error);
        return ExitCode::from(1);
    }

    match commands::run(cli.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            report_error(log_format, &error);
            ExitCode::from(error.exit_code())
        }
    }
}

fn report_error(format: LogFormat, error: &AppError) {
    match format {
        LogFormat::Text => eprintln!("error: {error}"),
        LogFormat::Json => {
            let record = serde_json::json!({
                "level": "ERROR",
                "target": "oci_auto_http",
                "fields": {
                    "message": "command failed",
                    "exit_code": error.exit_code(),
                    "error": error.to_string(),
                },
            });
            eprintln!("{record}");
        }
    }
}
