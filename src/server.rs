mod auth;
mod file;

use std::{
    fs::File,
    future::{Future, IntoFuture},
    io::{self, Read},
    path::Path,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, Request, StatusCode, header},
    middleware::{self, Next},
    response::Response,
    routing::get,
    serve::Listener,
};
use futures_util::{StreamExt, stream, stream::FuturesUnordered};
use oci_auto_http::{
    config::{Config, ResolvedPath},
    error::AppError,
    registry::{ClientPolicy, ErrorKind, RegistryClient},
    tree::{TreeError, TreeStore},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, oneshot},
};
use tokio_rustls::rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tracing::{debug, info, warn};
use zeroize::Zeroizing;

use crate::cli::ServeArgs;

const MAX_CA_CERT_BYTES: u64 = 1024 * 1024;
const MAX_TLS_MATERIAL_BYTES: u64 = 1024 * 1024;
const MAX_PENDING_TLS_HANDSHAKES: usize = 64;
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_REQUEST_HEADER_FIELDS: usize = 64;
const MAX_REQUEST_HEADER_BYTES: usize = 16 * 1024;

#[derive(Clone)]
struct ServeState {
    config: Config,
    stores: Vec<TreeStore>,
    secure_listener: bool,
    request_slots: Arc<Semaphore>,
    autoindex_spool_budget: file::AutoIndexSpoolBudget,
}

pub async fn serve(args: ServeArgs) -> Result<(), AppError> {
    let mut config = Config::load(&args.config)?;
    if let Some(listen) = args.listen {
        config.listen = listen;
    }
    apply_server_overrides(&mut config, &args)?;
    let tls_acceptor = match (args.tls_cert.as_deref(), args.tls_key.as_deref()) {
        (Some(certificate), Some(key)) => Some(load_tls_acceptor(certificate, key)?),
        (None, None) => None,
        _ => return Err(AppError::InvalidTlsMaterial),
    };
    let secure_listener = tls_acceptor.is_some();

    let mut policy = ClientPolicy {
        request_timeout: config.server.registry_request_timeout,
        ..ClientPolicy::default()
    };
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
    let request_slots = Arc::new(Semaphore::new(config.server.max_concurrent_requests));
    let autoindex_spool_budget = file::AutoIndexSpoolBudget::new(
        config.server.max_autoindex_spool_bytes,
        config.server.max_autoindex_spool_total_bytes,
    );
    let state = Arc::new(ServeState {
        config,
        stores,
        secure_listener,
        request_slots,
        autoindex_spool_budget,
    });

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
        .layer(middleware::from_fn_with_state(
            state.clone(),
            enforce_request_limits,
        ))
        .layer(middleware::from_fn_with_state(
            secure_listener,
            enforce_authorization_transport,
        ))
        .with_state(state.clone());

    if let Some(acceptor) = tls_acceptor {
        info!(%address, mounts = state.config.mounts.len(), "HTTPS listener started");
        let (shutdown_started, shutdown_notified) = oneshot::channel();
        let server = axum::serve(HttpsListener::new(listener, acceptor), app)
            .with_graceful_shutdown(shutdown_signal(shutdown_started))
            .into_future();
        serve_until_shutdown(
            server,
            shutdown_notified,
            state.config.server.graceful_shutdown_timeout,
        )
        .await
        .map_err(AppError::Serve)?;
    } else {
        info!(%address, mounts = state.config.mounts.len(), "HTTP listener started");
        let (shutdown_started, shutdown_notified) = oneshot::channel();
        let server = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal(shutdown_started))
            .into_future();
        serve_until_shutdown(
            server,
            shutdown_notified,
            state.config.server.graceful_shutdown_timeout,
        )
        .await
        .map_err(AppError::Serve)?;
    }
    info!(%address, "HTTP listener stopped");
    Ok(())
}

fn apply_server_overrides(config: &mut Config, args: &ServeArgs) -> Result<(), AppError> {
    if let Some(value) = args.max_concurrent_requests {
        config.server.max_concurrent_requests =
            usize::try_from(value).map_err(|_| AppError::InvalidConfig {
                path: args.config.clone(),
                reason: "server max-concurrent-requests is out of range".to_owned(),
            })?;
    }
    if let Some(value) = args.request_timeout_seconds {
        config.server.request_timeout = Duration::from_secs(value);
    }
    if let Some(value) = args.registry_request_timeout_seconds {
        config.server.registry_request_timeout = Duration::from_secs(value);
    }
    if let Some(value) = args.stream_idle_timeout_seconds {
        config.server.stream_idle_timeout = Duration::from_secs(value);
    }
    if let Some(value) = args.graceful_shutdown_timeout_seconds {
        config.server.graceful_shutdown_timeout = Duration::from_secs(value);
    }
    if let Some(value) = args.max_autoindex_spool_bytes {
        config.server.max_autoindex_spool_bytes = value;
    }
    if let Some(value) = args.max_autoindex_spool_total_bytes {
        config.server.max_autoindex_spool_total_bytes = value;
    }
    if config.server.max_autoindex_spool_total_bytes < config.server.max_autoindex_spool_bytes {
        return Err(AppError::InvalidConfig {
            path: args.config.clone(),
            reason:
                "server max-autoindex-spool-total-bytes must be at least max-autoindex-spool-bytes"
                    .to_owned(),
        });
    }
    Ok(())
}

async fn enforce_request_limits(
    State(state): State<Arc<ServeState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let permit = match Arc::clone(&state.request_slots).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return bodyless_for_head(
                &method,
                error_response(StatusCode::SERVICE_UNAVAILABLE, "service unavailable\n"),
            );
        }
    };
    let deadline = tokio::time::Instant::now() + state.config.server.request_timeout;
    let (response, body_deadline) = match tokio::time::timeout_at(deadline, next.run(request)).await
    {
        Ok(response) => (response, deadline),
        Err(_) => {
            warn!("HTTP request exceeded configured total timeout");
            let response = bodyless_for_head(
                &method,
                error_response(StatusCode::GATEWAY_TIMEOUT, "gateway timeout\n"),
            );
            let error_body_deadline =
                tokio::time::Instant::now() + state.config.server.stream_idle_timeout;
            (response, error_body_deadline)
        }
    };
    let (parts, body) = response.into_parts();
    let body_stream = body.into_data_stream();
    let idle_timeout = state.config.server.stream_idle_timeout;
    let guarded_body = stream::unfold(Some((body_stream, permit)), move |state| async move {
        let (mut body_stream, permit) = state?;
        let remaining = body_deadline.saturating_duration_since(tokio::time::Instant::now());
        let timeout = remaining.min(idle_timeout);
        if timeout.is_zero() {
            warn!("HTTP response stream exceeded configured timeout");
            return Some((
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "HTTP response stream timed out",
                )),
                None,
            ));
        }
        match tokio::time::timeout(timeout, body_stream.next()).await {
            Ok(Some(Ok(bytes))) => Some((Ok(bytes), Some((body_stream, permit)))),
            Ok(Some(Err(_))) => {
                warn!("HTTP response stream failed");
                Some((Err(io::Error::other("HTTP response stream failed")), None))
            }
            Ok(None) => None,
            Err(_) => {
                warn!("HTTP response stream exceeded configured timeout");
                Some((
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "HTTP response stream timed out",
                    )),
                    None,
                ))
            }
        }
    });
    Response::from_parts(parts, Body::from_stream(guarded_body))
}

async fn enforce_authorization_transport(
    State(secure_listener): State<bool>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let method = request.method().clone();
    if request.uri().scheme().is_some()
        || request.uri().authority().is_some()
        || !request.uri().path().starts_with('/')
    {
        return bodyless_for_head(
            &method,
            error_response(StatusCode::BAD_REQUEST, "invalid request\n"),
        );
    }
    let mut authorization = request.headers().get_all(header::AUTHORIZATION).iter();
    if authorization.next().is_some() {
        let response = if authorization.next().is_some() {
            error_response(StatusCode::BAD_REQUEST, "invalid request\n")
        } else if !secure_listener {
            // Never forward caller credentials over a cleartext client connection.
            error_response(StatusCode::FORBIDDEN, "forbidden\n")
        } else {
            if !request_headers_within_limits(request.headers()) {
                return bodyless_for_head(
                    &method,
                    error_response(
                        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                        "request headers too large\n",
                    ),
                );
            }
            if request_has_body(request.headers()) {
                return bodyless_for_head(
                    &method,
                    error_response(StatusCode::BAD_REQUEST, "invalid request\n"),
                );
            }
            return next.run(request).await;
        };
        return bodyless_for_head(&method, response);
    }
    if !request_headers_within_limits(request.headers()) {
        return bodyless_for_head(
            &method,
            error_response(
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                "request headers too large\n",
            ),
        );
    }
    if request_has_body(request.headers()) {
        return bodyless_for_head(
            &method,
            error_response(StatusCode::BAD_REQUEST, "invalid request\n"),
        );
    }
    next.run(request).await
}

fn request_headers_within_limits(headers: &HeaderMap) -> bool {
    let mut field_count = 0_usize;
    let mut byte_count = 0_usize;
    for name in headers.keys() {
        for value in headers.get_all(name).iter() {
            field_count += 1;
            byte_count = match byte_count
                .checked_add(name.as_str().len())
                .and_then(|count| count.checked_add(value.as_bytes().len()))
            {
                Some(count) => count,
                None => return false,
            };
            if field_count > MAX_REQUEST_HEADER_FIELDS || byte_count > MAX_REQUEST_HEADER_BYTES {
                return false;
            }
        }
    }
    true
}

fn request_has_body(headers: &HeaderMap) -> bool {
    if headers.contains_key(header::TRANSFER_ENCODING) {
        return true;
    }
    headers.get_all(header::CONTENT_LENGTH).iter().any(|value| {
        value
            .to_str()
            .ok()
            .and_then(|length| length.parse::<u64>().ok())
            .is_none_or(|length| length > 0)
    })
}

type PendingTlsHandshake = Pin<
    Box<
        dyn Future<
                Output = (
                    Result<TlsStream<TcpStream>, io::Error>,
                    std::net::SocketAddr,
                ),
            > + Send,
    >,
>;

struct HttpsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    handshakes: FuturesUnordered<PendingTlsHandshake>,
}

impl HttpsListener {
    fn new(listener: TcpListener, acceptor: TlsAcceptor) -> Self {
        Self {
            listener,
            acceptor,
            handshakes: FuturesUnordered::new(),
        }
    }
}

impl Listener for HttpsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let can_accept = self.handshakes.len() < MAX_PENDING_TLS_HANDSHAKES;
            tokio::select! {
                result = self.handshakes.next(), if !self.handshakes.is_empty() => {
                    match result {
                        Some((Ok(stream), address)) => return (stream, address),
                        Some((Err(_), _)) => debug!("HTTPS client handshake rejected"),
                        None => {}
                    }
                }
                result = self.listener.accept(), if can_accept => {
                    match result {
                        Ok((stream, address)) => {
                            let acceptor = self.acceptor.clone();
                            self.handshakes.push(Box::pin(async move {
                                let handshake = tokio::time::timeout(
                                    TLS_HANDSHAKE_TIMEOUT,
                                    acceptor.accept(stream),
                                )
                                .await
                                .unwrap_or_else(|_| {
                                    Err(io::Error::new(
                                        io::ErrorKind::TimedOut,
                                        "TLS handshake timed out",
                                    ))
                                });
                                (handshake, address)
                            }));
                        }
                        Err(error) if matches!(
                            error.kind(),
                            io::ErrorKind::ConnectionRefused
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::ConnectionReset
                        ) => {}
                        Err(error) => {
                            warn!(%error, "HTTPS listener accept failed");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

fn load_tls_acceptor(certificate_path: &Path, key_path: &Path) -> Result<TlsAcceptor, AppError> {
    let certificate_pem = read_tls_material(certificate_path)?;
    let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| AppError::InvalidTlsMaterial)?
        .into_iter()
        .map(CertificateDer::into_owned)
        .collect::<Vec<_>>();
    if certificates.is_empty() {
        return Err(AppError::InvalidTlsMaterial);
    }

    let key_pem = read_tls_material(key_path)?;
    let private_key =
        PrivateKeyDer::from_pem_slice(&key_pem).map_err(|_| AppError::InvalidTlsMaterial)?;
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|_| AppError::InvalidTlsMaterial)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn read_tls_material(path: &Path) -> Result<Zeroizing<Vec<u8>>, AppError> {
    let file = File::open(path).map_err(AppError::TlsMaterial)?;
    let mut limited = file.take(MAX_TLS_MATERIAL_BYTES + 1);
    let mut material = Zeroizing::new(Vec::new());
    limited
        .read_to_end(&mut material)
        .map_err(AppError::TlsMaterial)?;
    if material.len() as u64 > MAX_TLS_MATERIAL_BYTES {
        return Err(AppError::InvalidTlsMaterial);
    }
    Ok(material)
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
    let directory_head = is_head
        && response.status() == StatusCode::OK
        && response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value == "text/html; charset=utf-8");
    if is_head && !directory_head {
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

    let authorization = headers.get(header::AUTHORIZATION).cloned();
    let resolved = match state.config.resolve_served_path(uri.path()) {
        Ok(Some(resolved)) => resolved,
        Ok(None) => return error_response(StatusCode::NOT_FOUND, "not found\n"),
        Err(TreeError::InvalidPath) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid path\n");
        }
        Err(error) => return tree_error_response(error, state.secure_listener),
    };
    let mount_index = state
        .config
        .mounts
        .iter()
        .position(|mount| std::ptr::eq(mount, resolved.mount))
        .expect("resolved mount belongs to this config");
    let store = &state.stores[mount_index];

    if resolved.trailing_slash {
        if !file::accepts_html(&headers) {
            return error_response(
                StatusCode::NOT_ACCEPTABLE,
                "directory listing is not acceptable\n",
            );
        }
        return match store
            .list_page(
                &resolved.storage_path,
                file::AUTO_INDEX_PAGE_SIZE,
                None,
                authorization.as_ref(),
            )
            .await
        {
            Ok(page) => match file::directory_response(
                store.clone(),
                &resolved,
                page,
                method,
                authorization,
                state.autoindex_spool_budget.clone(),
            )
            .await
            {
                Ok(response) => response,
                Err(file::DirectoryResponseError::Tree(error)) => {
                    tree_error_response(error, state.secure_listener)
                }
                Err(file::DirectoryResponseError::TemporaryStorage) => {
                    error_response(StatusCode::SERVICE_UNAVAILABLE, "service unavailable\n")
                }
            },
            Err(error) => tree_error_response(error, state.secure_listener),
        };
    }

    if method == Method::HEAD {
        return match store
            .stat_file(&resolved.storage_path, authorization.as_ref())
            .await
        {
            Ok(descriptor) => file::head_response(&descriptor, &resolved.storage_path, &headers),
            Err(TreeError::IsDirectory) => {
                match store
                    .list_page(&resolved.storage_path, 1, None, authorization.as_ref())
                    .await
                {
                    Ok(_) => permanent_redirect(&resolved),
                    Err(error) => tree_error_response(error, state.secure_listener),
                }
            }
            Err(error) => tree_error_response(error, state.secure_listener),
        };
    }

    let file_result = store
        .read_file(&resolved.storage_path, authorization.as_ref())
        .await;
    match file_result {
        Ok(file) => {
            file::get_response(
                file,
                &resolved.storage_path,
                &headers,
                state.secure_listener,
            )
            .await
        }
        Err(TreeError::IsDirectory) => {
            match store
                .list_page(&resolved.storage_path, 1, None, authorization.as_ref())
                .await
            {
                Ok(_) => permanent_redirect(&resolved),
                Err(error) => tree_error_response(error, state.secure_listener),
            }
        }
        Err(error) => tree_error_response(error, state.secure_listener),
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

fn tree_error_response(error: TreeError, secure_client: bool) -> Response {
    let auth_status = match &error {
        TreeError::Registry(error) if error.kind == ErrorKind::Unauthorized => {
            Some(if !error.challenge_relay_allowed {
                StatusCode::BAD_GATEWAY
            } else if !secure_client {
                StatusCode::FORBIDDEN
            } else if auth::valid_challenges(&error.challenges) {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::BAD_GATEWAY
            })
        }
        _ => None,
    };
    let challenges = (auth_status == Some(StatusCode::UNAUTHORIZED)).then(|| match &error {
        TreeError::Registry(error) => error.challenges.clone(),
        _ => unreachable!(),
    });
    let retry_after = match &error {
        TreeError::Registry(error)
            if error.kind == ErrorKind::RateLimited
                && error.retry_after.as_ref().is_some_and(valid_retry_after) =>
        {
            error.retry_after.clone()
        }
        _ => None,
    };
    let (status, message) = match auth_status {
        Some(StatusCode::UNAUTHORIZED) => (StatusCode::UNAUTHORIZED, "unauthorized\n"),
        Some(StatusCode::FORBIDDEN) => (StatusCode::FORBIDDEN, "forbidden\n"),
        Some(_) => (StatusCode::BAD_GATEWAY, "bad gateway\n"),
        None => match &error {
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
        },
    };
    let mut response = logged_error_response(&error, status, message);
    if let Some(retry_after) = retry_after {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, retry_after);
    }
    if let Some(challenges) = challenges {
        for challenge in challenges {
            response
                .headers_mut()
                .append(header::WWW_AUTHENTICATE, challenge);
        }
    }
    response
}

fn logged_error_response(error: &TreeError, status: StatusCode, message: &'static str) -> Response {
    warn!(%status, %error, "OCI-backed request failed");
    error_response(status, message)
}

fn valid_retry_after(value: &axum::http::HeaderValue) -> bool {
    let bytes = value.as_bytes();
    (!bytes.is_empty() && bytes.iter().all(u8::is_ascii_digit))
        || value
            .to_str()
            .is_ok_and(|date| httpdate::parse_http_date(date).is_ok())
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

async fn serve_until_shutdown<F>(
    server: F,
    shutdown_started: oneshot::Receiver<()>,
    grace_period: Duration,
) -> io::Result<()>
where
    F: Future<Output = io::Result<()>>,
{
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result,
        started = shutdown_started => match started {
            Ok(()) => match tokio::time::timeout(grace_period, &mut server).await {
                Ok(result) => result,
                Err(_) => {
                    warn!(grace_period_seconds = grace_period.as_secs(), "graceful shutdown timed out; canceling active requests");
                    Ok(())
                }
            },
            Err(_) => (&mut server).await,
        }
    }
}

async fn shutdown_signal(notify_started: oneshot::Sender<()>) {
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

    info!("shutdown signal received; draining in-flight requests");
    let _ = notify_started.send(());
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderValue, StatusCode};

    use super::*;
    use oci_auto_http::registry::RegistryError;

    fn rate_limit_error(retry_after: Option<&'static str>) -> TreeError {
        TreeError::Registry(RegistryError {
            kind: ErrorKind::RateLimited,
            status: Some(StatusCode::TOO_MANY_REQUESTS),
            challenges: Vec::new(),
            retry_after: retry_after.map(HeaderValue::from_static),
            challenge_relay_allowed: true,
        })
    }

    fn unauthorized_error(challenges: &[&'static str], relay_allowed: bool) -> TreeError {
        TreeError::Registry(RegistryError {
            kind: ErrorKind::Unauthorized,
            status: Some(StatusCode::UNAUTHORIZED),
            challenges: challenges
                .iter()
                .map(|value| HeaderValue::from_static(value))
                .collect(),
            retry_after: None,
            challenge_relay_allowed: relay_allowed,
        })
    }

    #[test]
    fn request_header_and_body_limits_fail_closed() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        assert!(!request_has_body(&headers));
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("1"));
        assert!(request_has_body(&headers));
        headers.remove(header::CONTENT_LENGTH);
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        assert!(request_has_body(&headers));

        let mut headers = HeaderMap::new();
        for index in 0..=MAX_REQUEST_HEADER_FIELDS {
            let name =
                axum::http::HeaderName::from_bytes(format!("x-test-{index}").as_bytes()).unwrap();
            headers.insert(name, HeaderValue::from_static("v"));
        }
        assert!(!request_headers_within_limits(&headers));

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::HeaderName::from_static("x-test"),
            HeaderValue::from_bytes(&vec![b'a'; MAX_REQUEST_HEADER_BYTES]).unwrap(),
        );
        assert!(!request_headers_within_limits(&headers));
    }

    #[test]
    fn relays_only_valid_retry_after_values_on_rate_limit_responses() {
        for value in ["120", "Wed, 21 Oct 2015 07:28:00 GMT"] {
            let response = tree_error_response(rate_limit_error(Some(value)), true);
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[header::RETRY_AFTER], value);
        }

        for value in ["", "soon", "-1", "1.5"] {
            let response = tree_error_response(rate_limit_error(Some(value)), true);
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(!response.headers().contains_key(header::RETRY_AFTER));
        }

        let response = tree_error_response(rate_limit_error(None), true);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!response.headers().contains_key(header::RETRY_AFTER));
    }

    #[test]
    fn relays_only_valid_registry_challenges_to_secure_clients() {
        let challenges = [
            r#"Basic realm="registry""#,
            r#"Bearer realm="https://auth.example/token",service="registry""#,
            r#"Bearer realm="example""#,
        ];
        let response = tree_error_response(unauthorized_error(&challenges, true), true);
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let received = response
            .headers()
            .get_all(header::WWW_AUTHENTICATE)
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(received, challenges);
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "private, no-store"
        );
        assert_eq!(response.headers()[header::VARY], "Authorization");
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "13");
    }

    #[test]
    fn suppresses_invalid_or_untrusted_challenges_and_cleartext_challenges() {
        for (error, secure_client, expected_status) in [
            (
                unauthorized_error(&[r#"Bearer realm="https://@auth.example/token""#], true),
                true,
                StatusCode::BAD_GATEWAY,
            ),
            (
                unauthorized_error(&[r#"Bearer realm="https://auth.example/token%""#], true),
                true,
                StatusCode::BAD_GATEWAY,
            ),
            (
                unauthorized_error(&[r#"Bearer realm="http://auth.example/token""#], true),
                true,
                StatusCode::BAD_GATEWAY,
            ),
            (
                unauthorized_error(&[r#"Bearer realm="https://auth.example/token""#], false),
                true,
                StatusCode::BAD_GATEWAY,
            ),
            (
                unauthorized_error(&[r#"Bearer realm="https://auth.example/token""#], true),
                false,
                StatusCode::FORBIDDEN,
            ),
        ] {
            let response = tree_error_response(error, secure_client);
            assert_eq!(response.status(), expected_status);
            assert!(!response.headers().contains_key(header::WWW_AUTHENTICATE));
        }
    }
}
