use std::{collections::HashSet, pin::Pin};

use futures_util::StreamExt;
use reqwest::header::HeaderValue;

use crate::registry::{
    Descriptor, Digest, ErrorKind, ManifestReference, OCI_IMAGE_INDEX, OCI_IMAGE_MANIFEST,
    RegistryError,
};

use super::{
    DirectoryEntry, DirectoryPage, EntryKind, MAX_FILE_CONFIG_BYTES, PageRef, StoragePath,
    TreeError, TreeFile, TreeStore, cursor,
    format::{FileConfig, FileManifest, IndexKind, OciDescriptor, OciIndex},
};

pub(super) struct ListingCollector<'a> {
    pub(super) after: Option<&'a str>,
    pub(super) limit: usize,
    pub(super) stale_cursor: bool,
    pub(super) authorization: Option<&'a HeaderValue>,
    pub(super) visited: HashSet<Digest>,
    pub(super) entries: Vec<DirectoryEntry>,
}

impl TreeStore {
    /// Read a file from one pinned root snapshot. Directory paths return
    /// `IsDirectory`; an absent root tag is not initialized as a side effect.
    /// Resolve verified file metadata without downloading its data layer.
    pub async fn stat_file(
        &self,
        path: &StoragePath,
        authorization: Option<&HeaderValue>,
    ) -> Result<Descriptor, TreeError> {
        if path.is_root() {
            return Err(TreeError::IsDirectory);
        }
        let root = self.resolve_root(authorization).await?;
        let entry = self
            .lookup_entry(&root, path.components(), false, authorization)
            .await?;
        if entry.media_type == OCI_IMAGE_INDEX {
            return Err(TreeError::IsDirectory);
        }
        if entry.media_type != OCI_IMAGE_MANIFEST {
            return Err(TreeError::IntegrityError);
        }
        let layer = self.file_layer_descriptor(&entry, authorization).await?;
        let actual = self
            .registry
            .head_blob(&layer.digest, authorization)
            .await
            .map_err(|error| map_reachable_error(error, false))?;
        if actual.size != layer.size || actual.digest != layer.digest {
            return Err(TreeError::IntegrityError);
        }
        Ok(layer)
    }

    /// Resolve and return a verified, streaming view of one file.
    pub async fn read_file(
        &self,
        path: &StoragePath,
        authorization: Option<&HeaderValue>,
    ) -> Result<TreeFile, TreeError> {
        if path.is_root() {
            return Err(TreeError::IsDirectory);
        }
        let root = self.resolve_root(authorization).await?;
        let entry = self
            .lookup_entry(&root, path.components(), false, authorization)
            .await?;
        if entry.media_type == OCI_IMAGE_INDEX {
            return Err(TreeError::IsDirectory);
        }
        if entry.media_type != OCI_IMAGE_MANIFEST {
            return Err(TreeError::IntegrityError);
        }
        self.load_file(&entry, authorization).await
    }

    /// Return one page of immediate children in unsigned UTF-8 byte order.
    /// Continuations stay pinned to the root digest in the signed token even if
    /// the configured root tag is subsequently updated.
    /// List immediate children in bytewise order, pinned to one root snapshot.
    /// The page size must be in `1..=10_000`.
    pub async fn list_page(
        &self,
        path: &StoragePath,
        limit: usize,
        continuation: Option<&str>,
        authorization: Option<&HeaderValue>,
    ) -> Result<DirectoryPage, TreeError> {
        self.list_page_with_root(path, limit, continuation, None, authorization)
            .await
    }

    /// List a page from a caller-pinned root digest instead of resolving the
    /// mutable root tag. This lets a multi-pass reader preserve one snapshot.
    pub async fn list_page_pinned(
        &self,
        path: &StoragePath,
        limit: usize,
        root_digest: &Digest,
        continuation: Option<&str>,
        authorization: Option<&HeaderValue>,
    ) -> Result<DirectoryPage, TreeError> {
        self.list_page_with_root(path, limit, continuation, Some(root_digest), authorization)
            .await
    }

    async fn list_page_with_root(
        &self,
        path: &StoragePath,
        limit: usize,
        continuation: Option<&str>,
        pinned_root: Option<&Digest>,
        authorization: Option<&HeaderValue>,
    ) -> Result<DirectoryPage, TreeError> {
        if limit == 0 || limit > super::MAX_LISTING_PAGE_SIZE {
            return Err(TreeError::InvalidPageSize);
        }

        let (root, cursor_state) = if let Some(token) = continuation {
            let payload = cursor::decode(token)?;
            if payload.directory != path.components() || payload.limit as usize != limit {
                return Err(TreeError::InvalidCursor);
            }
            let digest =
                Digest::parse(&payload.root_digest).map_err(|_| TreeError::InvalidCursor)?;
            if pinned_root.is_some_and(|expected| expected != &digest) {
                return Err(TreeError::InvalidCursor);
            }
            let root = self.root_by_digest(&digest, true, authorization).await?;
            (root, Some(payload))
        } else if let Some(pinned_root) = pinned_root {
            (
                self.root_by_digest(pinned_root, true, authorization)
                    .await?,
                None,
            )
        } else {
            (self.resolve_root(authorization).await?, None)
        };
        let stale_snapshot = cursor_state.is_some() || pinned_root.is_some();

        let directory = if path.is_root() {
            root.clone()
        } else {
            let descriptor = self
                .lookup_entry(&root, path.components(), stale_snapshot, authorization)
                .await?;
            if descriptor.media_type != OCI_IMAGE_INDEX {
                return Err(TreeError::NotDirectory);
            }
            page_ref_from_directory_entry(descriptor)?
        };
        if let Some(payload) = cursor_state.as_ref()
            && directory.descriptor.digest()?
                != Digest::parse(&payload.directory_digest).map_err(|_| TreeError::InvalidCursor)?
        {
            return Err(TreeError::InvalidCursor);
        }

        let after = cursor_state
            .as_ref()
            .map(|payload| payload.last_name.as_str());
        let mut listing = ListingCollector {
            after,
            limit: limit.saturating_add(1),
            stale_cursor: stale_snapshot,
            authorization,
            visited: HashSet::new(),
            entries: Vec::with_capacity(limit.saturating_add(1)),
        };
        self.collect_entries(&directory, &mut listing).await?;
        let mut entries = listing.entries;

        let has_more = entries.len() > limit;
        entries.truncate(limit);
        for entry in &entries {
            self.validate_listed_entry(entry, stale_snapshot, authorization)
                .await?;
        }
        let next_cursor = if has_more {
            let last_name = entries
                .last()
                .ok_or(TreeError::IntegrityError)?
                .name
                .as_str();
            Some(cursor::encode(&cursor::CursorPayload::new(
                &root.descriptor.digest()?,
                &directory.descriptor.digest()?,
                path,
                last_name,
                limit,
            )?)?)
        } else {
            None
        };
        Ok(DirectoryPage {
            root_digest: root.descriptor.digest()?,
            entries,
            next_cursor,
        })
    }

    pub(super) async fn resolve_root(
        &self,
        authorization: Option<&HeaderValue>,
    ) -> Result<PageRef, TreeError> {
        let descriptor = self
            .registry
            .resolve_tag(&self.root_tag, &[OCI_IMAGE_INDEX], authorization)
            .await
            .map_err(|error| {
                if error.kind == ErrorKind::NotFound {
                    TreeError::MissingRoot
                } else {
                    TreeError::Registry(error)
                }
            })?;
        if descriptor.media_type != OCI_IMAGE_INDEX {
            return Err(TreeError::IntegrityError);
        }
        let root = PageRef {
            descriptor: OciDescriptor::new(
                &descriptor.media_type,
                &descriptor.digest,
                descriptor.size,
            ),
            expected_range: None,
        };
        self.load_index(&root, false, authorization).await?;
        Ok(root)
    }

    async fn root_by_digest(
        &self,
        digest: &Digest,
        stale_cursor: bool,
        authorization: Option<&HeaderValue>,
    ) -> Result<PageRef, TreeError> {
        let manifest = self
            .registry
            .get_manifest(
                &ManifestReference::Digest(digest.clone()),
                &[OCI_IMAGE_INDEX],
                authorization,
            )
            .await
            .map_err(|error| map_reachable_error(error, stale_cursor))?;
        if manifest.descriptor.media_type != OCI_IMAGE_INDEX {
            return Err(TreeError::IntegrityError);
        }
        let root = PageRef {
            descriptor: OciDescriptor::new(
                &manifest.descriptor.media_type,
                &manifest.descriptor.digest,
                manifest.descriptor.size,
            ),
            expected_range: None,
        };
        OciIndex::parse(&manifest.bytes)?;
        Ok(root)
    }

    pub(super) async fn load_index(
        &self,
        page: &PageRef,
        stale_cursor: bool,
        authorization: Option<&HeaderValue>,
    ) -> Result<OciIndex, TreeError> {
        if page.descriptor.media_type != OCI_IMAGE_INDEX {
            return Err(TreeError::IntegrityError);
        }
        page.descriptor.validate()?;
        let manifest = self
            .registry
            .get_manifest(
                &ManifestReference::Digest(page.descriptor.digest()?),
                &[OCI_IMAGE_INDEX],
                authorization,
            )
            .await
            .map_err(|error| map_reachable_error(error, stale_cursor))?;
        if manifest.descriptor.media_type != OCI_IMAGE_INDEX
            || manifest.descriptor.size != page.descriptor.size
        {
            return Err(TreeError::IntegrityError);
        }
        let index = OciIndex::parse(&manifest.bytes)?;
        if let Some(expected) = &page.expected_range
            && index.bounds()? != Some((expected.0.as_str(), expected.1.as_str()))
        {
            return Err(TreeError::IntegrityError);
        }
        Ok(index)
    }

    pub(super) async fn lookup_entry(
        &self,
        root: &PageRef,
        components: &[String],
        stale_cursor: bool,
        authorization: Option<&HeaderValue>,
    ) -> Result<OciDescriptor, TreeError> {
        if components.is_empty() {
            return Err(TreeError::IsDirectory);
        }
        let mut current_directory = root.clone();
        let mut directory_ancestors = HashSet::new();
        for (index, name) in components.iter().enumerate() {
            let digest = current_directory.descriptor.digest()?;
            if !directory_ancestors.insert(digest) {
                return Err(TreeError::IntegrityError);
            }
            let entry = self
                .find_page_entry(
                    &current_directory,
                    name,
                    stale_cursor,
                    authorization,
                    &mut HashSet::new(),
                )
                .await?
                .ok_or(TreeError::NotFound)?;
            if index + 1 == components.len() {
                return Ok(entry);
            }
            if entry.media_type != OCI_IMAGE_INDEX {
                return Err(TreeError::NotDirectory);
            }
            current_directory = page_ref_from_directory_entry(entry)?;
        }
        Err(TreeError::NotFound)
    }

    pub(super) fn find_page_entry<'a>(
        &'a self,
        page: &'a PageRef,
        name: &'a str,
        stale_cursor: bool,
        authorization: Option<&'a HeaderValue>,
        visited: &'a mut HashSet<Digest>,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<Option<OciDescriptor>, TreeError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let digest = page.descriptor.digest()?;
            if !visited.insert(digest.clone()) {
                return Err(TreeError::IntegrityError);
            }
            let index = self.load_index(page, stale_cursor, authorization).await?;
            match index.kind()? {
                IndexKind::Leaf => Ok(index
                    .manifests
                    .into_iter()
                    .find(|descriptor| descriptor.entry_name() == Some(name))),
                IndexKind::Branch => {
                    let child = index.manifests.into_iter().find(|descriptor| {
                        descriptor.range().is_some_and(|(first, last)| {
                            first.as_bytes() <= name.as_bytes()
                                && name.as_bytes() <= last.as_bytes()
                        })
                    });
                    let Some(child) = child else {
                        return Ok(None);
                    };
                    let child = page_ref_from_branch_child(child)?;
                    self.find_page_entry(&child, name, stale_cursor, authorization, visited)
                        .await
                }
            }
        })
    }

    pub(super) async fn collect_entries(
        &self,
        root: &PageRef,
        listing: &mut ListingCollector<'_>,
    ) -> Result<(), TreeError> {
        let mut stack = vec![root.clone()];
        while let Some(page) = stack.pop() {
            if listing.entries.len() >= listing.limit {
                break;
            }
            let digest = page.descriptor.digest()?;
            if !listing.visited.insert(digest) {
                return Err(TreeError::IntegrityError);
            }
            let index = self
                .load_index(&page, listing.stale_cursor, listing.authorization)
                .await?;
            match index.kind()? {
                IndexKind::Leaf => {
                    for descriptor in index.manifests {
                        let name = descriptor
                            .entry_name()
                            .ok_or(TreeError::IntegrityError)?
                            .to_owned();
                        if listing
                            .after
                            .is_some_and(|after| after.as_bytes() >= name.as_bytes())
                        {
                            continue;
                        }
                        let kind = match descriptor.media_type.as_str() {
                            OCI_IMAGE_INDEX => EntryKind::Directory,
                            OCI_IMAGE_MANIFEST => EntryKind::File,
                            _ => return Err(TreeError::IntegrityError),
                        };
                        listing.entries.push(DirectoryEntry {
                            name,
                            kind,
                            digest: descriptor.digest()?,
                            size: descriptor.size,
                        });
                        if listing.entries.len() >= listing.limit {
                            break;
                        }
                    }
                }
                IndexKind::Branch => {
                    let mut children = Vec::new();
                    for descriptor in index.manifests {
                        let (_, last) = descriptor.range().ok_or(TreeError::IntegrityError)?;
                        if listing
                            .after
                            .is_some_and(|after| after.as_bytes() >= last.as_bytes())
                        {
                            continue;
                        }
                        children.push(page_ref_from_branch_child(descriptor)?);
                    }
                    stack.extend(children.into_iter().rev());
                }
            }
        }
        Ok(())
    }

    async fn validate_listed_entry(
        &self,
        entry: &DirectoryEntry,
        stale_cursor: bool,
        authorization: Option<&HeaderValue>,
    ) -> Result<(), TreeError> {
        let media_type = match entry.kind {
            EntryKind::Directory => OCI_IMAGE_INDEX,
            EntryKind::File => OCI_IMAGE_MANIFEST,
        };
        let descriptor = OciDescriptor::new(media_type, &entry.digest, entry.size);
        if entry.kind == EntryKind::Directory {
            let page = page_ref_from_directory_entry(descriptor)?;
            self.load_index(&page, stale_cursor, authorization).await?;
        } else {
            self.load_file_manifest(&descriptor, authorization).await?;
        }
        Ok(())
    }

    async fn load_file(
        &self,
        descriptor: &OciDescriptor,
        authorization: Option<&HeaderValue>,
    ) -> Result<TreeFile, TreeError> {
        let layer = self
            .file_layer_descriptor(descriptor, authorization)
            .await?;
        let body = self
            .registry
            .get_blob(&layer, authorization)
            .await
            .map_err(|error| map_reachable_error(error, false))?;
        Ok(TreeFile {
            descriptor: layer,
            body: super::TreeBlobStream::new(body),
        })
    }

    async fn load_file_manifest(
        &self,
        descriptor: &OciDescriptor,
        authorization: Option<&HeaderValue>,
    ) -> Result<FileManifest, TreeError> {
        descriptor.validate()?;
        let manifest = self
            .registry
            .get_manifest(
                &ManifestReference::Digest(descriptor.digest()?),
                &[OCI_IMAGE_MANIFEST],
                authorization,
            )
            .await
            .map_err(|error| map_reachable_error(error, false))?;
        if manifest.descriptor.media_type != OCI_IMAGE_MANIFEST
            || manifest.descriptor.size != descriptor.size
        {
            return Err(TreeError::IntegrityError);
        }
        FileManifest::parse(&manifest.bytes)
    }

    pub(super) async fn file_layer_descriptor(
        &self,
        descriptor: &OciDescriptor,
        authorization: Option<&HeaderValue>,
    ) -> Result<Descriptor, TreeError> {
        let file = self.load_file_manifest(descriptor, authorization).await?;
        let config = descriptor_as_registry(&file.config)?;
        if config.size > MAX_FILE_CONFIG_BYTES as u64 {
            return Err(TreeError::IntegrityError);
        }
        let config_bytes = self
            .read_small_blob(&config, MAX_FILE_CONFIG_BYTES, authorization)
            .await?;
        let file_config = FileConfig::parse(&config_bytes)?;
        let layer_descriptor = &file.layers[0];
        if file_config.size != layer_descriptor.size {
            return Err(TreeError::IntegrityError);
        }
        descriptor_as_registry(layer_descriptor)
    }

    async fn read_small_blob(
        &self,
        descriptor: &Descriptor,
        max_bytes: usize,
        authorization: Option<&HeaderValue>,
    ) -> Result<Vec<u8>, TreeError> {
        if descriptor.size > max_bytes as u64 {
            return Err(TreeError::IntegrityError);
        }
        let capacity = usize::try_from(descriptor.size).map_err(|_| TreeError::IntegrityError)?;
        let mut stream = self
            .registry
            .get_blob(descriptor, authorization)
            .await
            .map_err(|error| map_reachable_error(error, false))?;
        let mut bytes = Vec::with_capacity(capacity);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| map_reachable_error(error, false))?;
            if bytes.len().saturating_add(chunk.len()) > max_bytes {
                return Err(TreeError::IntegrityError);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != descriptor.size {
            return Err(TreeError::IntegrityError);
        }
        Ok(bytes)
    }
}

impl TreeStore {
    /// Explicitly initialize a missing tree as an empty directory leaf.
    /// File and directory creation also initialize on first write; reads never
    /// initialize, and existing unsupported roots are never overwritten.
    pub async fn initialize(
        &self,
        authorization: Option<&HeaderValue>,
    ) -> Result<Digest, TreeError> {
        let _guard = self.writer.lock().await;
        match self.resolve_root(authorization).await {
            Ok(root) => Ok(root.descriptor.digest()?),
            Err(TreeError::MissingRoot) => {
                let root = self
                    .upload_index(&OciIndex::new(IndexKind::Leaf, Vec::new()), authorization)
                    .await?;
                let result = self.publish_root(&root, None, authorization).await?;
                Ok(result)
            }
            Err(error) => Err(error),
        }
    }
}

pub(super) fn page_ref_from_directory_entry(
    descriptor: OciDescriptor,
) -> Result<PageRef, TreeError> {
    descriptor.validate()?;
    if descriptor.media_type != OCI_IMAGE_INDEX {
        return Err(TreeError::IntegrityError);
    }
    let descriptor = OciDescriptor::new(
        &descriptor.media_type,
        &descriptor.digest()?,
        descriptor.size,
    );
    Ok(PageRef {
        descriptor,
        expected_range: None,
    })
}

pub(super) fn page_ref_from_branch_child(descriptor: OciDescriptor) -> Result<PageRef, TreeError> {
    descriptor.validate()?;
    if descriptor.media_type != OCI_IMAGE_INDEX {
        return Err(TreeError::IntegrityError);
    }
    let (first, last) = descriptor.range().ok_or(TreeError::IntegrityError)?;
    let expected_range = Some((first.to_owned(), last.to_owned()));
    let descriptor = OciDescriptor::new(
        &descriptor.media_type,
        &descriptor.digest()?,
        descriptor.size,
    );
    Ok(PageRef {
        descriptor,
        expected_range,
    })
}

fn descriptor_as_registry(descriptor: &OciDescriptor) -> Result<Descriptor, TreeError> {
    descriptor.validate()?;
    Ok(Descriptor {
        media_type: descriptor.media_type.clone(),
        digest: descriptor.digest()?,
        size: descriptor.size,
    })
}

pub(super) fn map_reachable_error(error: RegistryError, stale_cursor: bool) -> TreeError {
    match error.kind {
        ErrorKind::NotFound if stale_cursor => TreeError::StaleCursor,
        ErrorKind::NotFound
        | ErrorKind::InvalidReference
        | ErrorKind::InvalidResponse
        | ErrorKind::ResponseTooLarge
        | ErrorKind::DigestMismatch
        | ErrorKind::SizeMismatch => TreeError::IntegrityError,
        _ => TreeError::Registry(error),
    }
}
