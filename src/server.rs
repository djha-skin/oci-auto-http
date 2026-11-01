use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::cli::ServeArgs;
use oci_auto_http::{config::Config, error::AppError};

pub async fn serve(args: ServeArgs) -> Result<(), AppError> {
    let mut config = Config::load(&args.config)?;
    if let Some(listen) = args.listen {
        config.listen = listen;
    }

    let listener =
        TcpListener::bind(config.listen)
            .await
            .map_err(|source| AppError::BindListener {
                address: config.listen,
                source,
            })?;
    let address = listener.local_addr().map_err(AppError::Serve)?;

    let app = Router::new()
        .route("/healthz", get(health).head(health))
        .fallback(not_ready);

    info!(%address, mounts = config.mounts.len(), "HTTP listener started; mounted storage serving is not implemented in this build");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(AppError::Serve)?;
    info!(%address, "HTTP listener stopped");
    Ok(())
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn not_ready() -> impl IntoResponse {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "mounted storage serving is not implemented in this build\n",
    )
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(signal) => Some(signal),
            Err(error) => {
                warn!(%error, "could not install SIGTERM handler; Ctrl-C remains available");
                None
            }
        };

        if let Some(terminate) = terminate.as_mut() {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    if let Err(error) = result {
                        warn!(%error, "could not receive Ctrl-C signal");
                    }
                }
                _ = terminate.recv() => {}
            }
        } else if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "could not receive Ctrl-C signal");
        }
    }

    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!(%error, "could not receive Ctrl-C signal");
    }

    info!("shutdown signal received");
}
