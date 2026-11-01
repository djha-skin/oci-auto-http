mod artifact;

use crate::{
    cli::{Command, MountCommand},
    server,
};
use oci_auto_http::error::AppError;

pub async fn run(command: Command) -> Result<(), AppError> {
    match command {
        Command::Serve(args) => server::serve(args).await,
        Command::Artifact(args) => artifact::run(args).await,
        Command::Mount { command } => Err(AppError::NotImplemented {
            command: match command {
                MountCommand::Create { .. } => "mount create",
                MountCommand::List => "mount list",
                MountCommand::Show { .. } => "mount show",
                MountCommand::Update { .. } => "mount update",
                MountCommand::Delete { .. } => "mount delete",
            },
        }),
    }
}
