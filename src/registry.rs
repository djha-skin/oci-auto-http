//! A repository-scoped OCI Distribution API client.
//!
//! This module owns protocol, transport, authentication forwarding, retry, and
//! redirect policy. It deliberately does not interpret the OCI tree format or
//! derive registry targets from request paths.

use std::{
    collections::HashSet,
    fmt, io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

pub use bytes::Bytes;
pub use futures_util::Stream;
use futures_util::StreamExt;
pub use reqwest::header::HeaderValue;
use reqwest::{
    Client, Method, Response, StatusCode, Url,
    header::{
        ACCEPT, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, LINK,
        LOCATION, RETRY_AFTER, WWW_AUTHENTICATE,
    },
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use crate::config::Mount;

pub const OCI_IMAGE_INDEX: &str = "application/vnd.oci.image.index.v1+json";
pub const OCI_IMAGE_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
pub const OCI_ARTIFACT_MANIFEST: &str = "application/vnd.oci.artifact.manifest.v1+json";
pub const OCI_EMPTY_CONFIG: &str = "application/vnd.oci.empty.v1+json";
pub const OCI_BLOB: &str = "application/octet-stream";

const MAX_REDIRECTS: usize = 5;
const MAX_RETRIES: u8 = 5;
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_MANIFEST_BYTES: usize = 1_048_576;
const MAX_BUFFERED_BYTES: usize = 16 * 1_024 * 1_024;
const MAX_DESCRIPTOR_SIZE: u64 = i64::MAX as u64;
const MAX_TAGS: usize = 100_000;
const MAX_TAG_PAGES: usize = 1_000;
const ACCEPTED_DELETE: [StatusCode; 2] = [StatusCode::OK, StatusCode::ACCEPTED];

/// Bounded transport behavior for one configured registry.
#[derive(Clone)]
pub struct ClientPolicy {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    /// Retries apply only to GET/HEAD requests, never to writes.
    pub max_retries: u8,
    /// Maximum response size for buffered manifests and tag-list pages.
    pub max_manifest_bytes: usize,
    /// Maximum number of registry-provided blob redirects.
    pub max_redirects: usize,
    /// Additional PEM CA certificates, useful for private registries.
    pub root_certificate_pem: Vec<Vec<u8>>,
}

impl fmt::Debug for ClientPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientPolicy")
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("max_retries", &self.max_retries)
            .field("max_manifest_bytes", &self.max_manifest_bytes)
            .field("max_redirects", &self.max_redirects)
            .field("root_certificate_count", &self.root_certificate_pem.len())
            .finish()
    }
}

impl ClientPolicy {
    pub fn with_root_certificate_pem(mut self, certificate: &[u8]) -> Self {
        self.root_certificate_pem.push(certificate.to_vec());
        self
    }
}

impl Default for ClientPolicy {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_retries: 2,
            max_manifest_bytes: MAX_MANIFEST_BYTES,
            max_redirects: MAX_REDIRECTS,
            root_certificate_pem: Vec::new(),
        }
    }
}

/// A validated OCI content digest.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Digest(String);

impl Digest {
    pub fn parse(value: &str) -> Result<Self, RegistryError> {
        let Some(hex) = value.strip_prefix("sha256:") else {
            return Err(RegistryError::new(ErrorKind::InvalidReference));
        };
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(RegistryError::new(ErrorKind::InvalidReference));
        }
        Ok(Self(value.to_owned()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let hash = Sha256::digest(bytes);
        let mut value = String::with_capacity(71);
        value.push_str("sha256:");
        for byte in hash {
            use fmt::Write as _;
            let _ = write!(value, "{byte:02x}");
        }
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A tag or digest used in an OCI manifest endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestReference {
    Tag(String),
    Digest(Digest),
}

impl ManifestReference {
    pub fn parse(value: &str) -> Result<Self, RegistryError> {
        if value.starts_with("sha256:") {
            return Digest::parse(value).map(Self::Digest);
        }
        if valid_tag(value) {
            Ok(Self::Tag(value.to_owned()))
        } else {
            Err(RegistryError::new(ErrorKind::InvalidReference))
        }
    }

    fn as_str(&self) -> &str {
        match self {
            Self::Tag(tag) => tag,
            Self::Digest(digest) => digest.as_str(),
        }
    }
}

/// OCI descriptor metadata, with a validated SHA-256 digest and nonnegative size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Descriptor {
    pub media_type: String,
    pub digest: Digest,
    pub size: u64,
}

/// A manifest's original bytes and the registry-derived descriptor for those bytes.
#[derive(Clone)]
pub struct Manifest {
    pub descriptor: Descriptor,
    pub bytes: Bytes,
}

impl fmt::Debug for Manifest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Manifest")
            .field("descriptor", &self.descriptor)
            .field("byte_length", &self.bytes.len())
            .finish()
    }
}

/// A repository-scoped OCI Distribution client.
///
/// The constructor accepts only an HTTPS origin and a repository name already
/// validated by the versioned configuration loader. All error displays and
/// debug output omit request URLs, registry response bodies, and credentials.
pub struct RegistryClient {
    mount: Mount,
    http: Client,
    policy: ClientPolicy,
}

impl fmt::Debug for RegistryClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistryClient")
            .field("mount_name", &self.mount.name)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl RegistryClient {
    pub fn new(mount: Mount, policy: ClientPolicy) -> Result<Self, RegistryError> {
        if mount.registry.scheme() != "https"
            || mount.registry.host_str().is_none()
            || mount.registry.as_str().contains('@')
            || !mount.registry.username().is_empty()
            || mount.registry.password().is_some()
            || mount.registry.path() != "/"
            || mount.registry.query().is_some()
            || mount.registry.fragment().is_some()
            || !valid_repository(&mount.repository)
            || policy.connect_timeout.is_zero()
            || policy.connect_timeout > MAX_CONNECT_TIMEOUT
            || policy.request_timeout.is_zero()
            || policy.request_timeout > MAX_REQUEST_TIMEOUT
            || policy.max_retries > MAX_RETRIES
            || policy.max_redirects > MAX_REDIRECTS
            || policy.max_manifest_bytes == 0
            || policy.max_manifest_bytes > MAX_BUFFERED_BYTES
        {
            return Err(RegistryError::new(ErrorKind::InvalidConfiguration));
        }

        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(policy.connect_timeout)
            .timeout(policy.request_timeout);
        for certificate in &policy.root_certificate_pem {
            let certificate = reqwest::Certificate::from_pem(certificate)
                .map_err(|_| RegistryError::new(ErrorKind::InvalidConfiguration))?;
            builder = builder.add_root_certificate(certificate);
        }
        let http = builder
            .build()
            .map_err(|_| RegistryError::new(ErrorKind::Transport))?;
        Ok(Self {
            mount,
            http,
            policy,
        })
    }

    /// Resolve a tag to an immutable digest using HEAD, with a GET fallback for
    /// registries that explicitly do not implement HEAD for manifests.
    pub async fn resolve_tag(
        &self,
        tag: &str,
        accept: &[&str],
        authorization: Option<&HeaderValue>,
    ) -> Result<Descriptor, RegistryError> {
        let reference = ManifestReference::Tag(tag.to_owned());
        if !valid_tag(tag) || accept.is_empty() {
            return Err(RegistryError::new(ErrorKind::InvalidReference));
        }
        let url = self.manifest_url(&reference)?;
        let headers = accept_header(accept)?;
        let response = self
            .send_request(
                Method::HEAD,
                url.clone(),
                authorization,
                headers,
                None,
                true,
            )
            .await?;

        if matches!(
            response.status(),
            StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED
        ) {
            return self
                .get_manifest(&reference, accept, authorization)
                .await
                .map(|manifest| manifest.descriptor);
        }
        require_status(&response, &[StatusCode::OK])?;
        descriptor_from_headers(response.headers())
    }

    /// Fetch a manifest while preserving and verifying its exact wire bytes.
    pub async fn get_manifest(
        &self,
        reference: &ManifestReference,
        accept: &[&str],
        authorization: Option<&HeaderValue>,
    ) -> Result<Manifest, RegistryError> {
        if accept.is_empty() {
            return Err(RegistryError::new(ErrorKind::InvalidReference));
        }
        let url = self.manifest_url(reference)?;
        let headers = accept_header(accept)?;
        let response = self
            .send_request(Method::GET, url, authorization, headers, None, true)
            .await?;
        require_status(&response, &[StatusCode::OK])?;
        if !identity_content_encoding(response.headers()) {
            return Err(RegistryError::new(ErrorKind::InvalidResponse));
        }

        let media_type = content_type(response.headers())?;
        if !accept
            .iter()
            .any(|accepted| media_type.eq_ignore_ascii_case(accepted))
        {
            return Err(RegistryError::new(ErrorKind::InvalidResponse));
        }
        let expected_size = content_length(response.headers())?;
        if expected_size.is_some_and(|size| size > self.policy.max_manifest_bytes as u64) {
            return Err(RegistryError::new(ErrorKind::ResponseTooLarge));
        }
        let claimed_digest = optional_digest(response.headers())?;
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(transport_error)?;
            if body.len().saturating_add(chunk.len()) > self.policy.max_manifest_bytes {
                return Err(RegistryError::new(ErrorKind::ResponseTooLarge));
            }
            body.extend_from_slice(&chunk);
        }
        if expected_size.is_some_and(|size| size != body.len() as u64) {
            return Err(RegistryError::new(ErrorKind::InvalidResponse));
        }
        let digest = Digest::from_bytes(&body);
        if claimed_digest
            .as_ref()
            .is_some_and(|claimed| claimed != &digest)
            || matches!(reference, ManifestReference::Digest(expected) if expected != &digest)
        {
            return Err(RegistryError::new(ErrorKind::DigestMismatch));
        }
        let descriptor = Descriptor {
            media_type,
            digest,
            size: body.len() as u64,
        };
        Ok(Manifest {
            descriptor,
            bytes: Bytes::from(body),
        })
    }

    /// Publish the exact supplied manifest bytes under a tag or its own digest.
    /// Writes are never retried because a transport failure can have an
    /// ambiguous commit outcome.
    pub async fn put_manifest(
        &self,
        reference: &ManifestReference,
        media_type: &str,
        bytes: Bytes,
        authorization: Option<&HeaderValue>,
    ) -> Result<Descriptor, RegistryError> {
        if bytes.len() > self.policy.max_manifest_bytes {
            return Err(RegistryError::new(ErrorKind::ResponseTooLarge));
        }
        let media_type_header = HeaderValue::from_str(media_type)
            .map_err(|_| RegistryError::new(ErrorKind::InvalidReference))?;
        let digest = Digest::from_bytes(&bytes);
        if matches!(reference, ManifestReference::Digest(expected) if expected != &digest) {
            return Err(RegistryError::new(ErrorKind::DigestMismatch));
        }
        let url = self.manifest_url(reference)?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, media_type_header);
        let response = self
            .send_request(
                Method::PUT,
                url,
                authorization,
                headers,
                Some(bytes.clone()),
                false,
            )
            .await?;
        require_status(&response, &[StatusCode::CREATED])?;
        if optional_digest(response.headers())?.is_some_and(|claimed| claimed != digest) {
            return Err(RegistryError::new(ErrorKind::DigestMismatch));
        }
        Ok(Descriptor {
            media_type: media_type.to_owned(),
            digest,
            size: bytes.len() as u64,
        })
    }

    /// Remove a manifest by immutable digest. Registry-specific tag deletion is
    /// intentionally unsupported.
    pub async fn delete_manifest(
        &self,
        digest: &Digest,
        authorization: Option<&HeaderValue>,
    ) -> Result<(), RegistryError> {
        let url = self.manifest_url(&ManifestReference::Digest(digest.clone()))?;
        let response = self
            .send_request(
                Method::DELETE,
                url,
                authorization,
                HeaderMap::new(),
                None,
                false,
            )
            .await?;
        require_status(&response, &ACCEPTED_DELETE)?;
        Ok(())
    }

    pub async fn head_blob(
        &self,
        digest: &Digest,
        authorization: Option<&HeaderValue>,
    ) -> Result<Descriptor, RegistryError> {
        let response = self
            .send_blob_request(
                Method::HEAD,
                self.blob_url(digest)?,
                authorization,
                HeaderMap::new(),
            )
            .await?;
        require_status(&response, &[StatusCode::OK])?;
        let size = content_length(response.headers())?
            .filter(|size| *size <= MAX_DESCRIPTOR_SIZE)
            .ok_or_else(|| RegistryError::new(ErrorKind::InvalidResponse))?;
        if optional_digest(response.headers())?.is_some_and(|claimed| claimed != *digest) {
            return Err(RegistryError::new(ErrorKind::DigestMismatch));
        }
        Ok(Descriptor {
            media_type: response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or(OCI_BLOB)
                .to_owned(),
            digest: digest.clone(),
            size,
        })
    }

    /// Fetch a blob as a bounded-memory stream. Each chunk is yielded once; the
    /// final poll verifies both the descriptor size and SHA-256 digest.
    pub async fn get_blob(
        &self,
        descriptor: &Descriptor,
        authorization: Option<&HeaderValue>,
    ) -> Result<VerifiedBlobStream, RegistryError> {
        if descriptor.size > MAX_DESCRIPTOR_SIZE {
            return Err(RegistryError::new(ErrorKind::InvalidReference));
        }
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static(OCI_BLOB));
        let response = self
            .send_blob_request(
                Method::GET,
                self.blob_url(&descriptor.digest)?,
                authorization,
                headers,
            )
            .await?;
        require_status(&response, &[StatusCode::OK])?;

        if content_length(response.headers())?.is_some_and(|size| size != descriptor.size)
            || optional_digest(response.headers())?
                .is_some_and(|claimed| claimed != descriptor.digest)
            || !identity_content_encoding(response.headers())
        {
            return Err(RegistryError::new(ErrorKind::InvalidResponse));
        }

        let stream = response
            .bytes_stream()
            .map(|chunk| chunk.map_err(transport_error));
        Ok(VerifiedBlobStream {
            inner: Box::pin(stream),
            hasher: Sha256::new(),
            received: 0,
            expected_size: descriptor.size,
            expected_digest: descriptor.digest.clone(),
            finished: false,
        })
    }

    /// Upload a blob using the standard Distribution upload protocol. This
    /// convenience wrapper computes the digest and delegates to the streaming
    /// API; writes are non-retryable to avoid hiding ambiguous outcomes.
    pub async fn put_blob(
        &self,
        bytes: Bytes,
        authorization: Option<&HeaderValue>,
    ) -> Result<Descriptor, RegistryError> {
        let digest = Digest::from_bytes(&bytes);
        let size = bytes.len() as u64;
        let source = futures_util::stream::once(async move { Ok::<Bytes, io::Error>(bytes) });
        self.put_blob_stream(digest, size, source, authorization)
            .await
    }

    /// Stream a blob into a standard registry upload session. The declared
    /// digest and size are checked while the PATCH body is streamed; a mismatch
    /// leaves only an incomplete upload session and prevents the commit PUT.
    pub async fn put_blob_stream<S>(
        &self,
        digest: Digest,
        size: u64,
        source: S,
        authorization: Option<&HeaderValue>,
    ) -> Result<Descriptor, RegistryError>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
    {
        if size > MAX_DESCRIPTOR_SIZE {
            return Err(RegistryError::new(ErrorKind::InvalidReference));
        }
        let start_url = self.blob_upload_url()?;
        let mut start_headers = HeaderMap::new();
        start_headers.insert(ACCEPT, HeaderValue::from_static(OCI_BLOB));
        let response = self
            .send_request(
                Method::POST,
                start_url.clone(),
                authorization,
                start_headers,
                None,
                false,
            )
            .await?;
        require_status(&response, &[StatusCode::ACCEPTED])?;
        let mut upload_url = response_location(&response, &start_url)?;
        self.validate_upload_url(&upload_url)?;

        let state = Arc::new(Mutex::new(UploadState::default()));
        let body_stream = HashingUploadStream {
            inner: Box::pin(source),
            state: Arc::clone(&state),
            expected_size: size,
        };
        if size > 0 {
            let mut patch_headers = HeaderMap::new();
            patch_headers.insert(CONTENT_TYPE, HeaderValue::from_static(OCI_BLOB));
            patch_headers.insert(
                "content-range",
                HeaderValue::from_str(&format!("0-{}", size - 1))
                    .map_err(|_| RegistryError::new(ErrorKind::InvalidResponse))?,
            );
            let response = self
                .send_stream_request(
                    Method::PATCH,
                    upload_url.clone(),
                    authorization,
                    patch_headers,
                    body_stream,
                )
                .await;
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    let source_error = state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .error;
                    return Err(RegistryError::new(source_error.unwrap_or(error.kind)));
                }
            };
            self.validate_upload_source(&state, size, &digest)?;
            require_status(&response, &[StatusCode::ACCEPTED])?;
            upload_url = response_location(&response, &upload_url)?;
            self.validate_upload_url(&upload_url)?;
        } else {
            let mut body_stream = body_stream;
            let deadline = tokio::time::Instant::now() + self.policy.request_timeout;
            loop {
                let next = tokio::time::timeout_at(deadline, body_stream.next())
                    .await
                    .map_err(|_| RegistryError::new(ErrorKind::Timeout))?;
                match next {
                    Some(Err(_)) => {
                        let kind = state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .error
                            .unwrap_or(ErrorKind::Transport);
                        return Err(RegistryError::new(kind));
                    }
                    Some(Ok(_)) => {}
                    None => break,
                }
            }
            self.validate_upload_source(&state, size, &digest)?;
        }

        if upload_url.query_pairs().any(|(key, _)| key == "digest") {
            return Err(RegistryError::new(ErrorKind::InvalidResponse));
        }
        upload_url
            .query_pairs_mut()
            .append_pair("digest", digest.as_str());
        let mut finish_headers = HeaderMap::new();
        finish_headers.insert(CONTENT_TYPE, HeaderValue::from_static(OCI_BLOB));
        finish_headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
        let response = self
            .send_request(
                Method::PUT,
                upload_url,
                authorization,
                finish_headers,
                Some(Bytes::new()),
                false,
            )
            .await?;
        require_status(&response, &[StatusCode::CREATED])?;
        if optional_digest(response.headers())?.is_some_and(|claimed| claimed != digest) {
            return Err(RegistryError::new(ErrorKind::DigestMismatch));
        }
        Ok(Descriptor {
            media_type: OCI_BLOB.to_owned(),
            digest,
            size,
        })
    }

    /// Delete a blob by digest. Tree-storage callers must not delete blobs that
    /// may still be reachable from a committed manifest.
    pub async fn delete_blob(
        &self,
        digest: &Digest,
        authorization: Option<&HeaderValue>,
    ) -> Result<(), RegistryError> {
        let response = self
            .send_request(
                Method::DELETE,
                self.blob_url(digest)?,
                authorization,
                HeaderMap::new(),
                None,
                false,
            )
            .await?;
        require_status(&response, &ACCEPTED_DELETE)?;
        Ok(())
    }

    /// List tags in this repository, following bounded same-origin `Link`
    /// pagination. The storage format must not use this API as its tree index.
    pub async fn list_tags(
        &self,
        authorization: Option<&HeaderValue>,
    ) -> Result<Vec<String>, RegistryError> {
        let mut url = self.tags_url()?;
        url.query_pairs_mut().append_pair("n", "100");
        let mut visited = HashSet::new();
        let mut tags = Vec::new();

        for _ in 0..MAX_TAG_PAGES {
            if !visited.insert(url.as_str().to_owned()) {
                return Err(RegistryError::new(ErrorKind::InvalidResponse));
            }
            let mut headers = HeaderMap::new();
            headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
            let response = self
                .send_request(Method::GET, url.clone(), authorization, headers, None, true)
                .await?;
            require_status(&response, &[StatusCode::OK])?;
            let next = next_link(response.headers(), &url, &self.mount)?;
            let bytes = read_bounded(response, self.policy.max_manifest_bytes).await?;
            let page: TagPage = serde_json::from_slice(&bytes)
                .map_err(|_| RegistryError::new(ErrorKind::InvalidResponse))?;
            if page.name != self.mount.repository {
                return Err(RegistryError::new(ErrorKind::InvalidResponse));
            }
            let page_tags = page.tags.unwrap_or_default();
            if tags.len().saturating_add(page_tags.len()) > MAX_TAGS {
                return Err(RegistryError::new(ErrorKind::ResponseTooLarge));
            }
            if page_tags.iter().any(|tag| !valid_tag(tag)) {
                return Err(RegistryError::new(ErrorKind::InvalidResponse));
            }
            tags.extend(page_tags);
            match next {
                Some(next) => url = next,
                None => return Ok(tags),
            }
        }
        Err(RegistryError::new(ErrorKind::PaginationLimit))
    }

    fn manifest_url(&self, reference: &ManifestReference) -> Result<Url, RegistryError> {
        match reference {
            ManifestReference::Tag(tag) if !valid_tag(tag) => {
                return Err(RegistryError::new(ErrorKind::InvalidReference));
            }
            ManifestReference::Digest(digest) if Digest::parse(digest.as_str()).is_err() => {
                return Err(RegistryError::new(ErrorKind::InvalidReference));
            }
            _ => {}
        }
        self.repository_url(&format!("manifests/{}", reference.as_str()))
    }

    fn blob_url(&self, digest: &Digest) -> Result<Url, RegistryError> {
        self.repository_url(&format!("blobs/{digest}"))
    }

    fn blob_upload_url(&self) -> Result<Url, RegistryError> {
        self.repository_url("blobs/uploads/")
    }

    fn tags_url(&self) -> Result<Url, RegistryError> {
        self.repository_url("tags/list")
    }

    fn repository_url(&self, suffix: &str) -> Result<Url, RegistryError> {
        self.mount
            .registry
            .join(&format!("v2/{}/{suffix}", self.mount.repository))
            .map_err(|_| RegistryError::new(ErrorKind::InvalidConfiguration))
    }

    async fn send_request(
        &self,
        method: Method,
        url: Url,
        authorization: Option<&HeaderValue>,
        headers: HeaderMap,
        body: Option<Bytes>,
        retryable: bool,
    ) -> Result<Response, RegistryError> {
        let max_attempts = if retryable {
            usize::from(self.policy.max_retries) + 1
        } else {
            1
        };
        for attempt in 0..max_attempts {
            let mut request = self
                .http
                .request(method.clone(), url.clone())
                .headers(headers.clone());
            if self.should_forward_authorization(&url)
                && let Some(authorization) = authorization
            {
                request = request.header(AUTHORIZATION, authorization.clone());
            }
            if let Some(body) = body.clone() {
                request = request.body(body);
            }
            match request.send().await {
                Ok(response)
                    if retryable
                        && attempt + 1 < max_attempts
                        && retryable_status(response.status()) =>
                {
                    let delay = retry_delay(response.headers(), attempt);
                    drop(response);
                    tokio::time::sleep(delay).await;
                }
                Ok(response) => return Ok(response),
                Err(error) if retryable && attempt + 1 < max_attempts => {
                    let delay = backoff_delay(attempt);
                    let _timed_out = error.is_timeout();
                    tokio::time::sleep(delay).await;
                }
                Err(error) => return Err(transport_error(error)),
            }
        }
        Err(RegistryError::new(ErrorKind::Transport))
    }

    async fn send_blob_request(
        &self,
        method: Method,
        mut url: Url,
        authorization: Option<&HeaderValue>,
        headers: HeaderMap,
    ) -> Result<Response, RegistryError> {
        let mut visited = HashSet::new();
        for hop in 0..=self.policy.max_redirects {
            if !self.permitted_blob_target(&url) || !visited.insert(url.as_str().to_owned()) {
                return Err(RegistryError::new(ErrorKind::RedirectDenied));
            }
            let response = self
                .send_request(
                    method.clone(),
                    url.clone(),
                    authorization,
                    headers.clone(),
                    None,
                    true,
                )
                .await?;
            if !is_redirect(response.status()) {
                return Ok(response);
            }
            if hop == self.policy.max_redirects {
                return Err(RegistryError::new(ErrorKind::RedirectDenied));
            }
            let location = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| RegistryError::new(ErrorKind::InvalidResponse))?;
            url = url
                .join(location)
                .map_err(|_| RegistryError::new(ErrorKind::InvalidResponse))?;
        }
        Err(RegistryError::new(ErrorKind::RedirectDenied))
    }

    async fn send_stream_request<S>(
        &self,
        method: Method,
        url: Url,
        authorization: Option<&HeaderValue>,
        headers: HeaderMap,
        body: S,
    ) -> Result<Response, RegistryError>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
    {
        let mut request = self.http.request(method, url.clone()).headers(headers);
        if self.should_forward_authorization(&url)
            && let Some(authorization) = authorization
        {
            request = request.header(AUTHORIZATION, authorization.clone());
        }
        request
            .body(reqwest::Body::wrap_stream(body))
            .send()
            .await
            .map_err(transport_error)
    }

    fn validate_upload_url(&self, url: &Url) -> Result<(), RegistryError> {
        if !self.permitted_blob_target(url) || url.query_pairs().any(|(key, _)| key == "digest") {
            return Err(RegistryError::new(ErrorKind::RedirectDenied));
        }
        Ok(())
    }

    fn validate_upload_source(
        &self,
        state: &Arc<Mutex<UploadState>>,
        expected_size: u64,
        expected_digest: &Digest,
    ) -> Result<(), RegistryError> {
        let state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(kind) = state.error {
            return Err(RegistryError::new(kind));
        }
        if !state.complete || state.received != expected_size {
            return Err(RegistryError::new(ErrorKind::SizeMismatch));
        }
        if Digest::from_hash(state.hasher.clone().finalize().as_slice()) != *expected_digest {
            return Err(RegistryError::new(ErrorKind::DigestMismatch));
        }
        Ok(())
    }

    fn should_forward_authorization(&self, url: &Url) -> bool {
        url.origin() == self.mount.registry.origin()
    }

    fn permitted_blob_target(&self, url: &Url) -> bool {
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return false;
        }
        if url.origin() == self.mount.registry.origin() {
            return true;
        }
        let Some(host) = url.host_str() else {
            return false;
        };
        let authority = format!("{host}:{}", url.port_or_known_default().unwrap_or(443));
        self.mount
            .redirect_allowlist
            .iter()
            .any(|item| item == &authority)
    }
}

#[derive(Default)]
struct UploadState {
    hasher: Sha256,
    received: u64,
    complete: bool,
    error: Option<ErrorKind>,
}

struct HashingUploadStream<S> {
    inner: Pin<Box<S>>,
    state: Arc<Mutex<UploadState>>,
    expected_size: u64,
}

impl<S> Stream for HashingUploadStream<S>
where
    S: Stream<Item = Result<Bytes, io::Error>> + Send,
{
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(_))) => {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.error = Some(ErrorKind::Transport);
                Poll::Ready(Some(Err(io::Error::other("blob source failed"))))
            }
            Poll::Ready(Some(Ok(bytes))) => {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Some(received) = u64::try_from(bytes.len())
                    .ok()
                    .and_then(|length| state.received.checked_add(length))
                else {
                    state.error = Some(ErrorKind::SizeMismatch);
                    return Poll::Ready(Some(Err(io::Error::other("blob size overflow"))));
                };
                if received > self.expected_size {
                    state.error = Some(ErrorKind::SizeMismatch);
                    return Poll::Ready(Some(Err(io::Error::other("blob exceeds declared size"))));
                }
                state.hasher.update(&bytes);
                state.received = received;
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(None) => {
                self.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .complete = true;
                Poll::Ready(None)
            }
        }
    }
}

/// A blob stream that checks the advertised descriptor size and digest when
/// the body reaches EOF. A consumer must read it to completion to observe the
/// verification result.
pub struct VerifiedBlobStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, RegistryError>> + Send>>,
    hasher: Sha256,
    received: u64,
    expected_size: u64,
    expected_digest: Digest,
    finished: bool,
}

impl fmt::Debug for VerifiedBlobStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedBlobStream")
            .field("received", &self.received)
            .field("expected_size", &self.expected_size)
            .field("expected_digest", &self.expected_digest)
            .finish_non_exhaustive()
    }
}

impl Stream for VerifiedBlobStream {
    type Item = Result<Bytes, RegistryError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(error))) => {
                self.finished = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(bytes))) => {
                let Ok(length) = u64::try_from(bytes.len()) else {
                    self.finished = true;
                    return Poll::Ready(Some(Err(RegistryError::new(ErrorKind::InvalidResponse))));
                };
                let Some(received) = self.received.checked_add(length) else {
                    self.finished = true;
                    return Poll::Ready(Some(Err(RegistryError::new(ErrorKind::InvalidResponse))));
                };
                if received > self.expected_size {
                    self.finished = true;
                    return Poll::Ready(Some(Err(RegistryError::new(ErrorKind::SizeMismatch))));
                }
                self.received = received;
                self.hasher.update(&bytes);
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(None) => {
                self.finished = true;
                let actual = Digest::from_hash(self.hasher.clone().finalize().as_slice());
                if self.received != self.expected_size {
                    Poll::Ready(Some(Err(RegistryError::new(ErrorKind::SizeMismatch))))
                } else if actual != self.expected_digest {
                    Poll::Ready(Some(Err(RegistryError::new(ErrorKind::DigestMismatch))))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}

impl Digest {
    fn from_hash(hash: &[u8]) -> Self {
        let mut value = String::with_capacity(71);
        value.push_str("sha256:");
        for byte in hash {
            use fmt::Write as _;
            let _ = write!(value, "{byte:02x}");
        }
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidConfiguration,
    InvalidReference,
    NotFound,
    Unauthorized,
    Forbidden,
    RateLimited,
    Timeout,
    Transport,
    Server,
    UnexpectedStatus,
    InvalidResponse,
    ResponseTooLarge,
    DigestMismatch,
    SizeMismatch,
    RedirectDenied,
    PaginationLimit,
}

/// Safe protocol failure. Challenge and `Retry-After` headers are available to
/// the HTTP boundary for exact relay, but are deliberately omitted from Debug
/// and Display because they can contain sensitive data.
pub struct RegistryError {
    pub kind: ErrorKind,
    pub status: Option<StatusCode>,
    pub challenges: Vec<HeaderValue>,
    pub retry_after: Option<HeaderValue>,
}

impl RegistryError {
    fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            status: None,
            challenges: Vec::new(),
            retry_after: None,
        }
    }
}

impl fmt::Debug for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistryError")
            .field("kind", &self.kind)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.status {
            Some(status) => write!(formatter, "OCI registry returned HTTP status {status}"),
            None => write!(formatter, "OCI registry request failed ({:?})", self.kind),
        }
    }
}

impl std::error::Error for RegistryError {}

fn require_status(response: &Response, accepted: &[StatusCode]) -> Result<(), RegistryError> {
    if accepted.contains(&response.status()) {
        Ok(())
    } else {
        Err(response_error(response))
    }
}

fn response_location(response: &Response, base: &Url) -> Result<Url, RegistryError> {
    let location = response
        .headers()
        .get(LOCATION)
        .ok_or_else(|| RegistryError::new(ErrorKind::InvalidResponse))?
        .to_str()
        .map_err(|_| RegistryError::new(ErrorKind::InvalidResponse))?;
    base.join(location)
        .map_err(|_| RegistryError::new(ErrorKind::InvalidResponse))
}

fn response_error(response: &Response) -> RegistryError {
    response_error_parts(response.status(), response.headers())
}

fn response_error_parts(status: StatusCode, headers: &HeaderMap) -> RegistryError {
    let kind = match status {
        StatusCode::NOT_FOUND => ErrorKind::NotFound,
        StatusCode::UNAUTHORIZED => ErrorKind::Unauthorized,
        StatusCode::FORBIDDEN => ErrorKind::Forbidden,
        StatusCode::TOO_MANY_REQUESTS => ErrorKind::RateLimited,
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => ErrorKind::Timeout,
        status if status.is_server_error() => ErrorKind::Server,
        _ => ErrorKind::UnexpectedStatus,
    };
    RegistryError {
        kind,
        status: Some(status),
        challenges: headers.get_all(WWW_AUTHENTICATE).iter().cloned().collect(),
        retry_after: headers.get(RETRY_AFTER).cloned(),
    }
}

fn transport_error(error: reqwest::Error) -> RegistryError {
    RegistryError::new(if error.is_timeout() {
        ErrorKind::Timeout
    } else {
        ErrorKind::Transport
    })
}

fn accept_header(accept: &[&str]) -> Result<HeaderMap, RegistryError> {
    let value = accept.join(", ");
    let value = HeaderValue::from_str(&value)
        .map_err(|_| RegistryError::new(ErrorKind::InvalidReference))?;
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, value);
    Ok(headers)
}

fn descriptor_from_headers(headers: &HeaderMap) -> Result<Descriptor, RegistryError> {
    let size = content_length(headers)?
        .filter(|size| *size <= MAX_DESCRIPTOR_SIZE)
        .ok_or_else(|| RegistryError::new(ErrorKind::InvalidResponse))?;
    Ok(Descriptor {
        media_type: content_type(headers)?,
        digest: optional_digest(headers)?
            .ok_or_else(|| RegistryError::new(ErrorKind::InvalidResponse))?,
        size,
    })
}

fn content_type(headers: &HeaderMap) -> Result<String, RegistryError> {
    let value = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| RegistryError::new(ErrorKind::InvalidResponse))?;
    let media_type = value.split(';').next().unwrap_or_default().trim();
    if media_type.is_empty() {
        return Err(RegistryError::new(ErrorKind::InvalidResponse));
    }
    Ok(media_type.to_owned())
}

fn identity_content_encoding(headers: &HeaderMap) -> bool {
    headers
        .get_all(CONTENT_ENCODING)
        .iter()
        .all(|encoding| encoding.as_bytes().eq_ignore_ascii_case(b"identity"))
}

fn content_length(headers: &HeaderMap) -> Result<Option<u64>, RegistryError> {
    let Some(value) = headers.get(CONTENT_LENGTH) else {
        return Ok(None);
    };
    let length = value
        .to_str()
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| RegistryError::new(ErrorKind::InvalidResponse))?;
    Ok(Some(length))
}

fn optional_digest(headers: &HeaderMap) -> Result<Option<Digest>, RegistryError> {
    let Some(value) = headers.get("docker-content-digest") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| RegistryError::new(ErrorKind::InvalidResponse))?;
    Digest::parse(value).map(Some)
}

async fn read_bounded(response: Response, limit: usize) -> Result<Vec<u8>, RegistryError> {
    if !identity_content_encoding(response.headers()) {
        return Err(RegistryError::new(ErrorKind::InvalidResponse));
    }
    if content_length(response.headers())?.is_some_and(|size| size > limit as u64) {
        return Err(RegistryError::new(ErrorKind::ResponseTooLarge));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(transport_error)?;
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(RegistryError::new(ErrorKind::ResponseTooLarge));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Deserialize)]
struct TagPage {
    name: String,
    tags: Option<Vec<String>>,
}

fn next_link(
    headers: &HeaderMap,
    current: &Url,
    mount: &Mount,
) -> Result<Option<Url>, RegistryError> {
    for value in headers.get_all(LINK).iter() {
        let raw = value
            .to_str()
            .map_err(|_| RegistryError::new(ErrorKind::InvalidResponse))?;
        if raw.trim().is_empty() || !raw.contains('<') {
            return Err(RegistryError::new(ErrorKind::InvalidResponse));
        }
        let mut remaining = raw;
        while let Some(start) = remaining.find('<') {
            let Some(relative_end) = remaining[start + 1..].find('>') else {
                return Err(RegistryError::new(ErrorKind::InvalidResponse));
            };
            let end = start + 1 + relative_end;
            let target = &remaining[start + 1..end];
            let params_end = remaining[end + 1..]
                .find('<')
                .map_or(remaining.len(), |next| end + 1 + next);
            let params = &remaining[end + 1..params_end];
            let is_next = params.split(';').any(|parameter| {
                parameter.trim().strip_prefix("rel=").is_some_and(|value| {
                    value
                        .trim_matches('"')
                        .split_ascii_whitespace()
                        .any(|relation| relation == "next")
                })
            });
            if is_next {
                let url = current
                    .join(target)
                    .map_err(|_| RegistryError::new(ErrorKind::InvalidResponse))?;
                if url.origin() != mount.registry.origin()
                    || url.scheme() != "https"
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.fragment().is_some()
                {
                    return Err(RegistryError::new(ErrorKind::RedirectDenied));
                }
                return Ok(Some(url));
            }
            remaining = &remaining[params_end..];
        }
    }
    Ok(None)
}

fn retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn retry_delay(headers: &HeaderMap, attempt: usize) -> Duration {
    let retry_after = headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs);
    retry_after
        .unwrap_or_else(|| backoff_delay(attempt))
        .min(Duration::from_secs(2))
}

fn backoff_delay(attempt: usize) -> Duration {
    Duration::from_millis(100u64.saturating_mul(1u64 << attempt.min(4)))
}

fn is_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

fn valid_tag(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_alphanumeric() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'.' | b'-'))
}

fn valid_repository(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.split('/').all(|component| {
            let bytes = component.as_bytes();
            if bytes.is_empty()
                || !bytes[0].is_ascii_lowercase() && !bytes[0].is_ascii_digit()
                || !bytes[bytes.len() - 1].is_ascii_lowercase()
                    && !bytes[bytes.len() - 1].is_ascii_digit()
            {
                return false;
            }
            let mut last_was_alphanumeric = true;
            for byte in &bytes[1..] {
                if byte.is_ascii_lowercase() || byte.is_ascii_digit() {
                    last_was_alphanumeric = true;
                } else if matches!(*byte, b'.' | b'_' | b'-') && last_was_alphanumeric {
                    last_was_alphanumeric = false;
                } else {
                    return false;
                }
            }
            last_was_alphanumeric
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sha256_digests_and_references_strictly() {
        let digest = format!("sha256:{}", "a".repeat(64));
        assert_eq!(Digest::parse(&digest).unwrap().as_str(), digest);
        assert!(Digest::parse(&format!("sha256:{}", "A".repeat(64))).is_err());
        assert!(Digest::parse(&format!("sha512:{}", "a".repeat(128))).is_err());
        assert!(matches!(
            ManifestReference::parse("release_1.0"),
            Ok(ManifestReference::Tag(_))
        ));
        assert!(ManifestReference::parse("../latest").is_err());
    }

    #[test]
    fn manifest_debug_does_not_disclose_manifest_bytes() {
        let bytes = Bytes::from_static(b"secret-mount-entry-name");
        let manifest = Manifest {
            descriptor: Descriptor {
                media_type: OCI_IMAGE_INDEX.to_owned(),
                digest: Digest::from_bytes(&bytes),
                size: bytes.len() as u64,
            },
            bytes,
        };
        assert!(!format!("{manifest:?}").contains("secret-mount-entry-name"));
    }

    #[test]
    fn error_formatting_does_not_disclose_challenge_values() {
        let mut error = RegistryError::new(ErrorKind::Unauthorized);
        error.status = Some(StatusCode::UNAUTHORIZED);
        let mut headers = HeaderMap::new();
        headers.append(
            WWW_AUTHENTICATE,
            HeaderValue::from_static(
                "Bearer realm=\"https://auth.example/token?secret=do-not-log\"",
            ),
        );
        headers.insert(RETRY_AFTER, HeaderValue::from_static("30"));
        let error = response_error_parts(StatusCode::UNAUTHORIZED, &headers);
        assert_eq!(error.kind, ErrorKind::Unauthorized);
        assert_eq!(error.challenges, [headers[WWW_AUTHENTICATE].clone()]);
        assert_eq!(error.retry_after, headers.get(RETRY_AFTER).cloned());
        assert!(!format!("{error}").contains("do-not-log"));
        assert!(!format!("{error:?}").contains("do-not-log"));
    }

    #[test]
    fn retries_are_narrow_and_retry_after_is_bounded() {
        assert!(retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!retryable_status(StatusCode::UNAUTHORIZED));
        assert_eq!(
            retry_delay(&HeaderMap::new(), 0),
            Duration::from_millis(100)
        );
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("60"));
        assert_eq!(retry_delay(&headers, 0), Duration::from_secs(2));
    }

    #[tokio::test]
    async fn verified_blob_stream_checks_size_and_digest_at_eof() {
        use futures_util::TryStreamExt;

        let bytes = Bytes::from_static(b"opaque file bytes");
        let digest = Digest::from_bytes(&bytes);
        let stream = VerifiedBlobStream {
            inner: Box::pin(futures_util::stream::iter(vec![Ok(bytes.clone())])),
            hasher: Sha256::new(),
            received: 0,
            expected_size: bytes.len() as u64,
            expected_digest: digest.clone(),
            finished: false,
        };
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        assert_eq!(chunks.concat(), bytes);

        let wrong_digest = VerifiedBlobStream {
            inner: Box::pin(futures_util::stream::iter(vec![Ok(bytes.clone())])),
            hasher: Sha256::new(),
            received: 0,
            expected_size: bytes.len() as u64,
            expected_digest: Digest::from_bytes(b"different bytes"),
            finished: false,
        };
        let error = wrong_digest.try_collect::<Vec<Bytes>>().await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::DigestMismatch);

        let short_size = VerifiedBlobStream {
            inner: Box::pin(futures_util::stream::iter(vec![Ok(bytes.clone())])),
            hasher: Sha256::new(),
            received: 0,
            expected_size: (bytes.len() - 1) as u64,
            expected_digest: digest,
            finished: false,
        };
        let error = short_size.try_collect::<Vec<Bytes>>().await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::SizeMismatch);
    }

    #[test]
    fn tag_pagination_accepts_only_same_origin_next_links() {
        let mount = test_mount();
        let current =
            Url::parse("https://registry.example/v2/team/releases/tags/list?n=100").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            LINK,
            HeaderValue::from_static(
                "</v2/team/releases/tags/list?n=100&last=alpha>; rel=\"next\"",
            ),
        );
        let next = next_link(&headers, &current, &mount).unwrap().unwrap();
        assert_eq!(next.path(), "/v2/team/releases/tags/list");
        assert_eq!(next.query(), Some("n=100&last=alpha"));

        headers.insert(
            LINK,
            HeaderValue::from_static("<https://evil.example/tags>; rel=\"next\""),
        );
        assert_eq!(
            next_link(&headers, &current, &mount).unwrap_err().kind,
            ErrorKind::RedirectDenied
        );
        headers.insert(LINK, HeaderValue::from_static("not a valid Link header"));
        assert_eq!(
            next_link(&headers, &current, &mount).unwrap_err().kind,
            ErrorKind::InvalidResponse
        );
    }

    #[tokio::test]
    async fn upload_stream_hashes_bytes_and_rejects_length_overrun() {
        use futures_util::TryStreamExt;

        let bytes = Bytes::from_static(b"upload payload");
        let digest = Digest::from_bytes(&bytes);
        let state = Arc::new(Mutex::new(UploadState::default()));
        let source = futures_util::stream::iter(vec![Ok::<_, io::Error>(bytes.clone())]);
        let tracked = HashingUploadStream {
            inner: Box::pin(source),
            state: Arc::clone(&state),
            expected_size: bytes.len() as u64,
        };
        let sent: Vec<Bytes> = tracked.try_collect().await.unwrap();
        assert_eq!(sent.concat(), bytes);
        let client = RegistryClient::new(test_mount(), ClientPolicy::default()).unwrap();
        client
            .validate_upload_source(&state, bytes.len() as u64, &digest)
            .unwrap();

        let too_long = HashingUploadStream {
            inner: Box::pin(futures_util::stream::iter(vec![Ok::<_, io::Error>(
                bytes.clone(),
            )])),
            state: Arc::new(Mutex::new(UploadState::default())),
            expected_size: (bytes.len() - 1) as u64,
        };
        let error = too_long.try_collect::<Vec<Bytes>>().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
    }

    #[test]
    fn repository_urls_are_scoped_to_the_configured_repository() {
        let mount = test_mount();
        let client = RegistryClient::new(mount, ClientPolicy::default()).unwrap();
        let manifest = client
            .manifest_url(&ManifestReference::parse("v1").unwrap())
            .unwrap();
        assert_eq!(
            manifest.as_str(),
            "https://registry.example/v2/team/releases/manifests/v1"
        );
        let malformed = ManifestReference::Tag("../../outside".to_owned());
        assert_eq!(
            client.manifest_url(&malformed).unwrap_err().kind,
            ErrorKind::InvalidReference
        );
    }

    #[test]
    fn blob_targets_require_https_and_same_or_allowlisted_origin() {
        let mut mount = test_mount();
        mount.redirect_allowlist = vec!["cdn.example:443".to_owned()];
        let client = RegistryClient::new(mount, ClientPolicy::default()).unwrap();
        assert!(
            client
                .permitted_blob_target(&Url::parse("https://registry.example/objects/x").unwrap())
        );
        assert!(client.permitted_blob_target(
            &Url::parse("https://cdn.example/objects/x?signature=secret").unwrap()
        ));
        assert!(
            !client.permitted_blob_target(&Url::parse("https://other.example/objects/x").unwrap())
        );
        assert!(
            !client.permitted_blob_target(&Url::parse("http://cdn.example/objects/x").unwrap())
        );
        assert!(
            !client
                .permitted_blob_target(&Url::parse("https://user@cdn.example/objects/x").unwrap())
        );
        assert!(
            client
                .should_forward_authorization(&Url::parse("https://registry.example/v2").unwrap())
        );
        assert!(!client.should_forward_authorization(
            &Url::parse("https://cdn.example/v2?signature=secret").unwrap()
        ));
    }

    #[test]
    fn client_policy_enforces_transport_resource_bounds() {
        let policy = ClientPolicy {
            max_retries: MAX_RETRIES + 1,
            ..ClientPolicy::default()
        };
        assert_eq!(
            RegistryClient::new(test_mount(), policy).unwrap_err().kind,
            ErrorKind::InvalidConfiguration
        );
        let policy = ClientPolicy {
            max_manifest_bytes: MAX_BUFFERED_BYTES + 1,
            ..ClientPolicy::default()
        };
        assert_eq!(
            RegistryClient::new(test_mount(), policy).unwrap_err().kind,
            ErrorKind::InvalidConfiguration
        );
    }

    fn test_mount() -> Mount {
        crate::config::Config::parse(
            std::path::Path::new("config.toml"),
            r#"
                version = 1
                [[mounts]]
                name = "test"
                prefix = "/test"
                registry = "https://registry.example"
                repository = "team/releases"
            "#,
        )
        .unwrap()
        .mounts
        .into_iter()
        .next()
        .unwrap()
    }
}
