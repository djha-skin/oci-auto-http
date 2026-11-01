mod file;

use std::{
    fs::File,
    io::{self, Read},
    path::Path,
    sync::Arc,
};

use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, Request, StatusCode, header},
    middleware::{self, Next},
    response::Response,
    routing::get,
};
use oci_auto_http::{
    config::{Config, ResolvedPath},
    error::AppError,
    registry::{ClientPolicy, ErrorKind, RegistryClient},
    tree::{TreeError, TreeStore},
};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::cli::ServeArgs;

const MAX_CA_CERT_BYTES: u64 = 1024 * 1024;

#[derive(Clone)]
struct ServeState {
    config: Config,
    stores: Vec<TreeStore>,
}

pub async fn serve(args: ServeArgs) -> Result<(), AppError> {
    let mut config = Config::load(&args.config)?;
    if let Some(listen) = args.listen {
        config.listen = listen;
    }

    let mut policy = ClientPolicy::default();
    if let Some(certificate_path) = args.ca_cert {
        let certificate = read_ca_certificate(&certificate_path)?;
        policy = policy.with_root_certificate_pem(&certificate);
    }
    let stores = config
        .mounts
        .iter()
        .map(|mount| {
            let registry = RegistryClient::new(mount.clone(), policy.clone())?;
            Ok(TreeStore::new(registry)?)
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    let state = Arc::new(ServeState { config, stores });

    let listener = TcpListener::bind(state.config.listen)
        .await
        .map_err(|source| AppError::BindListener {
            address: state.config.listen,
            source,
        })?;
    let address = listener.local_addr().map_err(AppError::Serve)?;

    let app = Router::new()
        .route("/healthz", get(health).head(health))
        .fallback(route_request)
        .layer(middleware::from_fn(reject_cleartext_authorization))
        .with_state(state.clone());

    info!(%address, mounts = state.config.mounts.len(), "HTTP listener started");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(AppError::Serve)?;
    info!(%address, "HTTP listener stopped");
    Ok(())
}

async fn reject_cleartext_authorization(request: Request<Body>, next: Next) -> Response {
    let method = request.method().clone();
    let mut authorization = request.headers().get_all(header::AUTHORIZATION).iter();
    if authorization.next().is_some() {
        let response = if authorization.next().is_some() {
            error_response(StatusCode::BAD_REQUEST, "invalid request\n")
        } else {
            // This listener currently accepts cleartext HTTP only. Never forward a
            // credential or relay its challenge across an untrusted cleartext hop.
            error_response(StatusCode::FORBIDDEN, "forbidden\n")
        };
        return bodyless_for_head(&method, response);
    }
    next.run(request).await
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn route_request(
    State(state): State<Arc<ServeState>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let is_head = method == Method::HEAD;
    let response = route_request_inner(state, method, uri, headers).await;
    if is_head {
        response.map(|_| Body::empty())
    } else {
        response
    }
}

async fn route_request_inner(
    state: Arc<ServeState>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return method_not_allowed();
    }

    let resolved = match state.config.resolve_served_path(uri.path()) {
        Ok(Some(resolved)) => resolved,
        Ok(None) => return error_response(StatusCode::NOT_FOUND, "not found\n"),
        Err(TreeError::InvalidPath) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid path\n");
        }
        Err(error) => return tree_error_response(error),
    };
    let mount_index = state
        .config
        .mounts
        .iter()
        .position(|mount| std::ptr::eq(mount, resolved.mount))
        .expect("resolved mount belongs to this config");
    let store = &state.stores[mount_index];

    if resolved.trailing_slash {
        return match store.list_page(&resolved.storage_path, 1, None, None).await {
            Ok(_) => directory_not_implemented(method),
            Err(error) => tree_error_response(error),
        };
    }

    if method == Method::HEAD {
        return match store.stat_file(&resolved.storage_path, None).await {
            Ok(descriptor) => file::head_response(&descriptor, &resolved.storage_path, &headers),
            Err(TreeError::IsDirectory) => {
                match store.list_page(&resolved.storage_path, 1, None, None).await {
                    Ok(_) => permanent_redirect(&resolved),
                    Err(error) => tree_error_response(error),
                }
            }
            Err(error) => tree_error_response(error),
        };
    }

    let file_result = store.read_file(&resolved.storage_path, None).await;
    match file_result {
        Ok(file) => file::get_response(file, &resolved.storage_path, &headers).await,
        Err(TreeError::IsDirectory) => {
            match store.list_page(&resolved.storage_path, 1, None, None).await {
                Ok(_) => permanent_redirect(&resolved),
                Err(error) => tree_error_response(error),
            }
        }
        Err(error) => tree_error_response(error),
    }
}

fn bodyless_for_head(method: &Method, response: Response) -> Response {
    if method == Method::HEAD {
        response.map(|_| Body::empty())
    } else {
        response
    }
}

fn method_not_allowed() -> Response {
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header(header::ALLOW, "GET, HEAD")
        .header(header::CONTENT_LENGTH, "0")
        .body(Body::empty())
        .expect("static method response headers are valid")
}

fn permanent_redirect(path: &ResolvedPath<'_>) -> Response {
    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header(header::LOCATION, path.canonical_url_path(true))
        .header(header::CACHE_CONTROL, "private, no-store")
        .header(header::VARY, "Authorization")
        .header(header::CONTENT_LENGTH, "0")
        .body(Body::empty())
        .expect("canonical location is a valid header")
}

fn directory_not_implemented(method: Method) -> Response {
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from("directory listings are not implemented yet\n")
    };
    Response::builder()
        .status(StatusCode::NOT_IMPLEMENTED)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(header::CACHE_CONTROL, "private, no-store")
        .header(header::VARY, "Accept, Authorization")
        .body(body)
        .expect("static directory response headers are valid")
}

fn tree_error_response(error: TreeError) -> Response {
    let (status, message) = match &error {
        TreeError::InvalidPath => (StatusCode::BAD_REQUEST, "invalid path\n"),
        TreeError::NotFound => (StatusCode::NOT_FOUND, "not found\n"),
        TreeError::MissingRoot => (StatusCode::SERVICE_UNAVAILABLE, "service unavailable\n"),
        TreeError::IsDirectory | TreeError::NotDirectory | TreeError::NotFile => {
            (StatusCode::NOT_FOUND, "not found\n")
        }
        TreeError::Registry(error) => match error.kind {
            ErrorKind::Timeout => (StatusCode::GATEWAY_TIMEOUT, "gateway timeout\n"),
            ErrorKind::RateLimited | ErrorKind::Transport | ErrorKind::Server => {
                (StatusCode::SERVICE_UNAVAILABLE, "service unavailable\n")
            }
            ErrorKind::Forbidden => (StatusCode::FORBIDDEN, "forbidden\n"),
            ErrorKind::Unauthorized => (StatusCode::BAD_GATEWAY, "bad gateway\n"),
            _ => (StatusCode::BAD_GATEWAY, "bad gateway\n"),
        },
        TreeError::InvalidMountTarget => (StatusCode::BAD_GATEWAY, "bad gateway\n"),
        _ => (StatusCode::BAD_GATEWAY, "bad gateway\n"),
    };
    warn!(%status, %error, "OCI-backed request failed");
    error_response(status, message)
}

fn error_response(status: StatusCode, message: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(header::CONTENT_LENGTH, message.len().to_string())
        .header(header::CACHE_CONTROL, "private, no-store")
        .header(header::VARY, "Authorization")
        .body(Body::from(message))
        .expect("static error response headers are valid")
}

fn read_ca_certificate(path: &Path) -> Result<Vec<u8>, AppError> {
    let file = File::open(path).map_err(AppError::Certificate)?;
    let mut limited = file.take(MAX_CA_CERT_BYTES + 1);
    let mut certificate = Vec::new();
    limited
        .read_to_end(&mut certificate)
        .map_err(AppError::Certificate)?;
    if certificate.len() as u64 > MAX_CA_CERT_BYTES {
        return Err(AppError::Certificate(io::Error::other(
            "certificate file exceeds the 1 MiB limit",
        )));
    }
    Ok(certificate)
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
