//! OCI-backed hierarchical byte-tree storage.
//!
//! The root tag is the only mutable pointer. Directory pages, file manifests,
//! and blobs are immutable and referenced by OCI digest; this layer owns the
//! tree format and consistency rules while [`crate::registry::RegistryClient`]
//! owns Distribution HTTP and transport policy.

mod cursor;
mod format;
mod read;
mod write;

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_util::Stream;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    config::Mount,
    registry::{Digest, RegistryClient, RegistryError},
};

const MAX_PATH_BYTES: usize = 4096;
const MAX_COMPONENT_BYTES: usize = 255;
const MAX_PATH_COMPONENTS: usize = 128;
const MAX_INDEX_DESCRIPTORS: usize = 128;
const MAX_INDEX_BYTES: usize = 1_048_576;
pub(super) const MAX_INDEX_DEPTH: usize = 64;
pub(super) const MAX_ROOT_PROMOTION_PAGES: usize = 256;
pub(super) const MAX_LISTING_PAGE_SIZE: usize = 10_000;
const MAX_FILE_CONFIG_BYTES: usize = 16 * 1_024;

/// Typed failures from tree path validation, registry-backed storage, and commits.
#[derive(Debug, Error)]
pub enum TreeError {
    #[error("invalid storage path")]
    InvalidPath,
    #[error("invalid OCI tree mount target")]
    InvalidMountTarget,
    #[error("file size exceeds the OCI descriptor limit")]
    InvalidFileSize,
    #[error("file input does not match its declared digest and size")]
    InvalidFileContent,
    #[error("file input stream failed")]
    InputStream,
    #[error("tree entry was not found")]
    NotFound,
    #[error("configured OCI tree root is missing")]
    MissingRoot,
    #[error("tree entry is not a file")]
    NotFile,
    #[error("tree entry is not a directory")]
    NotDirectory,
    #[error("tree entry is a directory")]
    IsDirectory,
    #[error("tree entry already exists")]
    AlreadyExists,
    #[error("directory is not empty")]
    DirectoryNotEmpty,
    #[error("reachable OCI tree metadata is corrupt")]
    IntegrityError,
    #[error("OCI directory index exceeds the maximum supported branch depth")]
    IndexDepthLimit,
    #[error("OCI directory index exceeds the root-promotion page scan budget")]
    IndexPageScanLimit,
    #[error("invalid directory-list continuation token")]
    InvalidCursor,
    #[error("invalid directory-list page size")]
    InvalidPageSize,
    #[error("directory-list snapshot is stale")]
    StaleCursor,
    #[error("tree mutation commit outcome is unknown")]
    CommitOutcomeUnknown,
    #[error("could not initialize process-local cursor integrity key")]
    CryptoFailure,
    #[error(transparent)]
    Registry(#[from] RegistryError),
}

/// A decoded, validated relative path in the logical tree.
///
/// This type never percent-decodes. The HTTP router must validate and decode
/// the request target once before constructing it; CLI callers supply literal
/// UTF-8 names. The empty path names the root directory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct StoragePath(Vec<String>);

impl StoragePath {
    pub fn root() -> Self {
        Self(Vec::new())
    }

    pub fn parse_relative(path: &str) -> Result<Self, TreeError> {
        if path.is_empty() {
            return Ok(Self::root());
        }
        Self::from_components(path.split('/').map(str::to_owned).collect())
    }

    pub fn from_components(components: Vec<String>) -> Result<Self, TreeError> {
        if components.len() > MAX_PATH_COMPONENTS {
            return Err(TreeError::InvalidPath);
        }
        let mut total_bytes = 0usize;
        for component in &components {
            let bytes = component.as_bytes();
            if bytes.is_empty()
                || bytes.len() > MAX_COMPONENT_BYTES
                || component == "."
                || component == ".."
                || component
                    .chars()
                    .any(|character| matches!(character, '/' | '\\') || is_control(character))
            {
                return Err(TreeError::InvalidPath);
            }
            total_bytes = total_bytes
                .checked_add(bytes.len())
                .ok_or(TreeError::InvalidPath)?;
            if total_bytes > MAX_PATH_BYTES {
                return Err(TreeError::InvalidPath);
            }
        }
        total_bytes = total_bytes
            .checked_add(components.len().saturating_sub(1))
            .ok_or(TreeError::InvalidPath)?;
        if total_bytes > MAX_PATH_BYTES {
            return Err(TreeError::InvalidPath);
        }
        Ok(Self(components))
    }

    pub fn components(&self) -> &[String] {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn child(&self, name: impl Into<String>) -> Result<Self, TreeError> {
        let mut components = self.0.clone();
        components.push(name.into());
        Self::from_components(components)
    }

    pub fn parent(&self) -> Option<Self> {
        (!self.0.is_empty()).then(|| Self(self.0[..self.0.len() - 1].to_vec()))
    }

    pub fn name(&self) -> Option<&str> {
        self.0.last().map(String::as_str)
    }
}

fn is_control(character: char) -> bool {
    matches!(character as u32, 0x00..=0x1f | 0x7f..=0x9f)
}

fn writer_lock_key(mount: &Mount) -> String {
    format!(
        "{}\0{}\0{}",
        mount.registry.as_str(),
        mount.repository,
        mount.root_tag
    )
}

static WRITER_LOCKS: OnceLock<StdMutex<HashMap<String, Weak<Mutex<()>>>>> = OnceLock::new();

fn writer_lock(mount: &Mount) -> Arc<Mutex<()>> {
    let key = writer_lock_key(mount);
    let locks = WRITER_LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(existing) = locks.get(&key).and_then(Weak::upgrade) {
        return existing;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

#[derive(Clone)]
pub(super) struct PageRef {
    pub descriptor: format::OciDescriptor,
    pub expected_range: Option<(String, String)>,
}

/// The kind of an immediate child in a directory listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Directory,
}

/// One file or directory exposed by a deterministic tree listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub name: String,
    pub kind: EntryKind,
    pub digest: Digest,
    pub size: u64,
}

/// A page of immediate directory children pinned to one root digest.
#[derive(Debug, Clone)]
pub struct DirectoryPage {
    pub root_digest: Digest,
    pub entries: Vec<DirectoryEntry>,
    pub next_cursor: Option<String>,
}

/// File metadata and a streaming body returned by [`TreeStore::read_file`].
pub struct TreeFile {
    pub descriptor: crate::registry::Descriptor,
    pub body: TreeBlobStream,
}

/// A bounded-memory blob body whose EOF poll verifies its digest and size.
pub struct TreeBlobStream {
    inner: Pin<Box<crate::registry::VerifiedBlobStream>>,
}

impl TreeBlobStream {
    fn new(inner: crate::registry::VerifiedBlobStream) -> Self {
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Stream for TreeBlobStream {
    type Item = Result<Bytes, TreeError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(read::map_reachable_error(error, false))))
            }
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

/// A single-writer, copy-on-write view of one configured tree.
///
/// Reads pin a root digest and never initialize the tree. Mutations hold a
/// process-local lock for the configured registry/repository/root-tag tuple;
/// deployments with external writers must coordinate them or remain read-only.
#[derive(Clone)]
pub struct TreeStore {
    registry: Arc<RegistryClient>,
    root_tag: String,
    writer: Arc<Mutex<()>>,
}

impl TreeStore {
    /// Bind a validated registry client to its configured root tag and shared
    /// process-local writer lock.
    pub fn new(registry: RegistryClient) -> Result<Self, TreeError> {
        let (root_tag, writer) = {
            let mount = registry.mount();
            if !matches!(
                crate::registry::ManifestReference::parse(&mount.root_tag),
                Ok(crate::registry::ManifestReference::Tag(_))
            ) {
                return Err(TreeError::InvalidMountTarget);
            }
            (mount.root_tag.clone(), writer_lock(mount))
        };
        Ok(Self {
            registry: Arc::new(registry),
            root_tag,
            writer,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_paths_validate_decoded_components_and_limits() {
        assert!(StoragePath::parse_relative("").unwrap().is_root());
        assert_eq!(
            StoragePath::parse_relative("a/+b/☃").unwrap().components(),
            &["a", "+b", "☃"]
        );
        for invalid in ["/absolute", "trailing/", "a//b", "a/../b"] {
            assert!(StoragePath::parse_relative(invalid).is_err(), "{invalid}");
        }
        assert_eq!(
            StoragePath::parse_relative("a/%2F").unwrap().components()[1],
            "%2F",
            "the storage layer does not perform URL percent-decoding"
        );
        assert!(StoragePath::from_components(vec!["x".repeat(255)]).is_ok());
        assert!(StoragePath::from_components(vec!["x".repeat(256)]).is_err());
        assert!(StoragePath::from_components(vec!["a".to_owned(); 129]).is_err());
    }

    #[test]
    fn storage_path_rejects_controls_backslashes_and_total_length() {
        assert!(StoragePath::from_components(vec!["a\\b".to_owned()]).is_err());
        assert!(StoragePath::from_components(vec!["nul\0byte".to_owned()]).is_err());
        assert!(StoragePath::from_components(vec!["c1\u{0085}".to_owned()]).is_err());
        let long = "x".repeat(MAX_COMPONENT_BYTES);
        let components = vec![long; 17];
        assert!(StoragePath::from_components(components).is_err());
    }
}
