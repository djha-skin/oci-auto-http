use crate::{
    cli::{ArtifactCommand, Command, MountCommand},
    error::AppError,
    server,
};

pub async fn run(command: Command) -> Result<(), AppError> {
    match command {
        Command::Serve(args) => server::serve(args).await,
        Command::Artifact { command } => Err(AppError::NotImplemented {
            command: match command {
                ArtifactCommand::Create { .. } => "artifact create",
                ArtifactCommand::Read { .. } => "artifact read",
                ArtifactCommand::List { .. } => "artifact list",
                ArtifactCommand::Update { .. } => "artifact update",
                ArtifactCommand::Delete { .. } => "artifact delete",
            },
        }),
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
