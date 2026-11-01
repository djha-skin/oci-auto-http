use std::{collections::HashSet, future::Future, io, pin::Pin};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use reqwest::header::HeaderValue;
use sha2::{Digest as _, Sha256};

use crate::registry::{Digest, ErrorKind, ManifestReference, OCI_IMAGE_INDEX, OCI_IMAGE_MANIFEST};

use super::{
    MAX_INDEX_BYTES, MAX_INDEX_DESCRIPTORS, PageRef, StoragePath, TreeError, TreeStore,
    format::{
        FILE_CONFIG_MEDIA_TYPE, FILE_DATA_MEDIA_TYPE, FileConfig, FileManifest, IndexKind,
        OciDescriptor, OciIndex,
    },
    read::{map_reachable_error, page_ref_from_branch_child, page_ref_from_directory_entry},
};

#[derive(Clone)]
enum EntryOperation {
    Put {
        descriptor: OciDescriptor,
        require_absent: bool,
    },
    Remove,
}

#[derive(Clone, Copy)]
enum WriteMode {
    CreateOnly,
    Upsert,
    Replace,
}

impl TreeStore {
    /// Create or replace a file, creating missing parents in the same root-tag
    /// publication. Identical content is a no-op that preserves the root digest.
    pub async fn write_file(
        &self,
        path: &StoragePath,
        bytes: Bytes,
        overwrite: bool,
        authorization: Option<&HeaderValue>,
    ) -> Result<Digest, TreeError> {
        let digest = Digest::from_bytes(&bytes);
        let size = bytes.len() as u64;
        let source = futures_util::stream::once(async move { Ok::<Bytes, io::Error>(bytes) });
        self.write_file_stream(path, digest, size, source, overwrite, authorization)
            .await
    }

    /// Stream a file body into OCI without storing it in a local catalog or
    /// buffering the full content. The caller supplies the SHA-256 digest and
    /// byte size; the source is checked against both before commit. For a
    /// no-op, it is verified locally without uploading a second blob.
    pub async fn write_file_stream<S>(
        &self,
        path: &StoragePath,
        content_digest: Digest,
        size: u64,
        source: S,
        overwrite: bool,
        authorization: Option<&HeaderValue>,
    ) -> Result<Digest, TreeError>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
    {
        self.write_file_stream_with_requirement(
            path,
            content_digest,
            size,
            source,
            if overwrite {
                WriteMode::Upsert
            } else {
                WriteMode::CreateOnly
            },
            authorization,
        )
        .await
    }

    /// Replace an existing file without creating a new path if it is absent.
    /// The existence check and replacement are serialized under the tree writer lock.
    pub async fn replace_file_stream<S>(
        &self,
        path: &StoragePath,
        content_digest: Digest,
        size: u64,
        source: S,
        authorization: Option<&HeaderValue>,
    ) -> Result<Digest, TreeError>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
    {
        self.write_file_stream_with_requirement(
            path,
            content_digest,
            size,
            source,
            WriteMode::Replace,
            authorization,
        )
        .await
    }

    async fn write_file_stream_with_requirement<S>(
        &self,
        path: &StoragePath,
        content_digest: Digest,
        size: u64,
        source: S,
        mode: WriteMode,
        authorization: Option<&HeaderValue>,
    ) -> Result<Digest, TreeError>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
    {
        if size > i64::MAX as u64 {
            return Err(TreeError::InvalidFileSize);
        }
        if path.is_root() {
            return Err(TreeError::InvalidPath);
        }
        let _guard = self.writer.lock().await;
        let old_root = self.ensure_root(authorization).await?;
        let old_digest = old_root.descriptor.digest()?;
        let name = path.name().ok_or(TreeError::InvalidPath)?;

        match self
            .lookup_entry(&old_root, path.components(), false, authorization)
            .await
        {
            Ok(existing) if existing.media_type == OCI_IMAGE_INDEX => {
                return Err(TreeError::IsDirectory);
            }
            Ok(existing) if existing.media_type == OCI_IMAGE_MANIFEST => {
                if matches!(mode, WriteMode::CreateOnly) {
                    return Err(TreeError::AlreadyExists);
                }
                let current = self.file_layer_descriptor(&existing, authorization).await?;
                if current.digest == content_digest && current.size == size {
                    let blob = self
                        .registry
                        .head_blob(&current.digest, authorization)
                        .await
                        .map_err(|error| map_reachable_error(error, false))?;
                    if blob.size != current.size || blob.digest != current.digest {
                        return Err(TreeError::IntegrityError);
                    }
                    verify_source_stream(source, &content_digest, size).await?;
                    return Ok(old_digest);
                }
            }
            Ok(_) => return Err(TreeError::IntegrityError),
            Err(TreeError::NotFound) if matches!(mode, WriteMode::Replace) => {
                return Err(TreeError::NotFound);
            }
            Err(TreeError::NotFound) => {}
            Err(error) => return Err(error),
        }

        let layer_blob = match self
            .registry
            .put_blob_stream(content_digest.clone(), size, source, authorization)
            .await
        {
            Ok(descriptor) => descriptor,
            Err(error) => match error.kind {
                ErrorKind::DigestMismatch | ErrorKind::SizeMismatch => {
                    return Err(TreeError::InvalidFileContent);
                }
                ErrorKind::UploadSource => return Err(TreeError::InputStream),
                _ => return Err(TreeError::Registry(error)),
            },
        };
        if layer_blob.digest != content_digest || layer_blob.size != size {
            return Err(TreeError::IntegrityError);
        }

        let config_bytes = FileConfig::new(size).serialize()?;
        let config_blob = self
            .registry
            .put_blob(Bytes::from(config_bytes), authorization)
            .await?;
        let config = OciDescriptor::new(
            FILE_CONFIG_MEDIA_TYPE,
            &config_blob.digest,
            config_blob.size,
        );
        let layer = OciDescriptor::new(FILE_DATA_MEDIA_TYPE, &layer_blob.digest, layer_blob.size);
        let file_manifest = FileManifest::new(config, layer);
        let manifest_bytes = file_manifest.serialize()?;
        let manifest_digest = Digest::from_bytes(&manifest_bytes);
        let published = self
            .registry
            .put_manifest(
                &ManifestReference::Digest(manifest_digest.clone()),
                OCI_IMAGE_MANIFEST,
                Bytes::from(manifest_bytes),
                authorization,
            )
            .await?;
        if published.digest != manifest_digest || published.media_type != OCI_IMAGE_MANIFEST {
            return Err(TreeError::IntegrityError);
        }

        let entry = OciDescriptor::new(OCI_IMAGE_MANIFEST, &published.digest, published.size)
            .with_entry_name(name);
        let new_root = self
            .update_directory(
                &old_root,
                &path.components()[..path.components().len() - 1],
                name,
                &EntryOperation::Put {
                    descriptor: entry,
                    require_absent: matches!(mode, WriteMode::CreateOnly),
                },
                true,
                authorization,
            )
            .await?;
        let new_digest = new_root.descriptor.digest()?;
        if new_digest == old_digest {
            return Ok(old_digest);
        }
        self.publish_root(&new_root, Some(&old_digest), authorization)
            .await
    }

    /// Create an empty directory and missing ancestors in one root publication.
    /// Existing paths are not silently reinterpreted as directories.
    pub async fn create_directory(
        &self,
        path: &StoragePath,
        authorization: Option<&HeaderValue>,
    ) -> Result<Digest, TreeError> {
        if path.is_root() {
            return Err(TreeError::AlreadyExists);
        }
        let _guard = self.writer.lock().await;
        let root = self.ensure_root(authorization).await?;
        let old_digest = root.descriptor.digest()?;
        match self
            .lookup_entry(&root, path.components(), false, authorization)
            .await
        {
            Ok(_) => return Err(TreeError::AlreadyExists),
            Err(TreeError::NotFound) => {}
            Err(error) => return Err(error),
        }

        let name = path.name().ok_or(TreeError::InvalidPath)?;
        let empty_directory = self
            .upload_index(&OciIndex::new(IndexKind::Leaf, Vec::new()), authorization)
            .await?;
        let entry = OciDescriptor::new(
            OCI_IMAGE_INDEX,
            &empty_directory.descriptor.digest()?,
            empty_directory.descriptor.size,
        )
        .with_entry_name(name);
        let new_root = self
            .update_directory(
                &root,
                &path.components()[..path.components().len() - 1],
                name,
                &EntryOperation::Put {
                    descriptor: entry,
                    require_absent: true,
                },
                true,
                authorization,
            )
            .await?;
        self.publish_root(&new_root, Some(&old_digest), authorization)
            .await
    }

    /// Remove a file or directory in one root-tag publication. Nonempty
    /// directories require `recursive`; orphaned OCI objects are left for GC.
    pub async fn remove(
        &self,
        path: &StoragePath,
        recursive: bool,
        authorization: Option<&HeaderValue>,
    ) -> Result<Digest, TreeError> {
        if path.is_root() {
            return Err(TreeError::InvalidPath);
        }
        let _guard = self.writer.lock().await;
        let root = self.resolve_root(authorization).await?;
        let old_digest = root.descriptor.digest()?;
        let entry = self
            .lookup_entry(&root, path.components(), false, authorization)
            .await?;
        if entry.media_type == OCI_IMAGE_INDEX && !recursive {
            let directory = page_ref_from_directory_entry(entry.clone())?;
            if !self.directory_is_empty(&directory, authorization).await? {
                return Err(TreeError::DirectoryNotEmpty);
            }
        } else if entry.media_type != OCI_IMAGE_INDEX && entry.media_type != OCI_IMAGE_MANIFEST {
            return Err(TreeError::IntegrityError);
        }

        let name = path.name().ok_or(TreeError::InvalidPath)?;
        let new_root = self
            .update_directory(
                &root,
                &path.components()[..path.components().len() - 1],
                name,
                &EntryOperation::Remove,
                false,
                authorization,
            )
            .await?;
        self.publish_root(&new_root, Some(&old_digest), authorization)
            .await
    }

    async fn ensure_root(&self, authorization: Option<&HeaderValue>) -> Result<PageRef, TreeError> {
        match self.resolve_root(authorization).await {
            Ok(root) => Ok(root),
            Err(TreeError::NotFound) => {
                let root = self
                    .upload_index(&OciIndex::new(IndexKind::Leaf, Vec::new()), authorization)
                    .await?;
                self.publish_root(&root, None, authorization).await?;
                self.resolve_root(authorization).await
            }
            Err(error) => Err(error),
        }
    }

    async fn directory_is_empty(
        &self,
        directory: &PageRef,
        authorization: Option<&HeaderValue>,
    ) -> Result<bool, TreeError> {
        let mut listing = super::read::ListingCollector {
            after: None,
            limit: 1,
            stale_cursor: false,
            authorization,
            visited: HashSet::new(),
            entries: Vec::with_capacity(1),
        };
        self.collect_entries(directory, &mut listing).await?;
        Ok(listing.entries.is_empty())
    }

    fn update_directory<'a>(
        &'a self,
        current_root: &'a PageRef,
        parent_components: &'a [String],
        name: &'a str,
        operation: &'a EntryOperation,
        create_parents: bool,
        authorization: Option<&'a HeaderValue>,
    ) -> Pin<Box<dyn Future<Output = Result<PageRef, TreeError>> + Send + 'a>> {
        Box::pin(async move {
            if parent_components.is_empty() {
                let pages = self
                    .mutate_page(
                        current_root,
                        name,
                        operation,
                        true,
                        authorization,
                        &mut HashSet::new(),
                    )
                    .await?;
                return self.promote_root(pages, authorization).await;
            }

            let parent_name = &parent_components[0];
            let existing = self
                .find_page_entry(
                    current_root,
                    parent_name,
                    false,
                    authorization,
                    &mut HashSet::new(),
                )
                .await?;
            let child_root = match existing.as_ref() {
                Some(descriptor) if descriptor.media_type == OCI_IMAGE_INDEX => {
                    page_ref_from_directory_entry(descriptor.clone())?
                }
                Some(_) => return Err(TreeError::NotDirectory),
                None if create_parents => {
                    self.upload_index(&OciIndex::new(IndexKind::Leaf, Vec::new()), authorization)
                        .await?
                }
                None => return Err(TreeError::NotFound),
            };
            let updated_child = self
                .update_directory(
                    &child_root,
                    &parent_components[1..],
                    name,
                    operation,
                    create_parents,
                    authorization,
                )
                .await?;
            let child_entry = OciDescriptor::new(
                OCI_IMAGE_INDEX,
                &updated_child.descriptor.digest()?,
                updated_child.descriptor.size,
            )
            .with_entry_name(parent_name);
            let pages = self
                .mutate_page(
                    current_root,
                    parent_name,
                    &EntryOperation::Put {
                        descriptor: child_entry,
                        require_absent: existing.is_none(),
                    },
                    true,
                    authorization,
                    &mut HashSet::new(),
                )
                .await?;
            self.promote_root(pages, authorization).await
        })
    }

    fn mutate_page<'a>(
        &'a self,
        page: &'a PageRef,
        name: &'a str,
        operation: &'a EntryOperation,
        is_directory_root: bool,
        authorization: Option<&'a HeaderValue>,
        ancestors: &'a mut HashSet<Digest>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PageRef>, TreeError>> + Send + 'a>> {
        Box::pin(async move {
            let digest = page.descriptor.digest()?;
            if !ancestors.insert(digest.clone()) {
                return Err(TreeError::IntegrityError);
            }
            let result = self
                .mutate_page_inner(
                    page,
                    name,
                    operation,
                    is_directory_root,
                    authorization,
                    ancestors,
                )
                .await;
            ancestors.remove(&digest);
            result
        })
    }

    async fn mutate_page_inner(
        &self,
        page: &PageRef,
        name: &str,
        operation: &EntryOperation,
        is_directory_root: bool,
        authorization: Option<&HeaderValue>,
        ancestors: &mut HashSet<Digest>,
    ) -> Result<Vec<PageRef>, TreeError> {
        let index = self.load_index(page, false, authorization).await?;
        match index.kind()? {
            IndexKind::Leaf => {
                let mut entries = index.manifests;
                let position = entries.binary_search_by(|descriptor| {
                    descriptor
                        .entry_name()
                        .unwrap_or_default()
                        .as_bytes()
                        .cmp(name.as_bytes())
                });
                match operation {
                    EntryOperation::Put {
                        descriptor,
                        require_absent,
                    } => {
                        descriptor.validate()?;
                        if descriptor.entry_name() != Some(name)
                            || (descriptor.media_type != OCI_IMAGE_INDEX
                                && descriptor.media_type != OCI_IMAGE_MANIFEST)
                        {
                            return Err(TreeError::IntegrityError);
                        }
                        match position {
                            Ok(_) if *require_absent => return Err(TreeError::AlreadyExists),
                            Ok(index) if entries[index] == *descriptor => {
                                return Ok(vec![page.clone()]);
                            }
                            Ok(index) => entries[index] = descriptor.clone(),
                            Err(index) => entries.insert(index, descriptor.clone()),
                        }
                    }
                    EntryOperation::Remove => match position {
                        Ok(index) => {
                            entries.remove(index);
                        }
                        Err(_) => return Err(TreeError::NotFound),
                    },
                }

                if entries.is_empty() && !is_directory_root {
                    return Ok(Vec::new());
                }
                self.upload_page_group(IndexKind::Leaf, entries, authorization)
                    .await
            }
            IndexKind::Branch => {
                let mut children = index
                    .manifests
                    .into_iter()
                    .map(page_ref_from_branch_child)
                    .collect::<Result<Vec<_>, _>>()?;
                let Some(child_index) = choose_branch_child(&children, name, operation) else {
                    return match operation {
                        EntryOperation::Remove => Err(TreeError::NotFound),
                        EntryOperation::Put { .. } => Err(TreeError::IntegrityError),
                    };
                };
                let child_pages = self
                    .mutate_page(
                        &children[child_index],
                        name,
                        operation,
                        false,
                        authorization,
                        ancestors,
                    )
                    .await?;
                children.splice(child_index..=child_index, child_pages);

                if children.is_empty() {
                    if is_directory_root {
                        return self
                            .upload_page_group(IndexKind::Leaf, Vec::new(), authorization)
                            .await;
                    }
                    return Ok(Vec::new());
                }
                if children.len() == 1 {
                    return Ok(children);
                }
                let descriptors = children
                    .iter()
                    .map(branch_descriptor)
                    .collect::<Result<Vec<_>, _>>()?;
                self.upload_page_group(IndexKind::Branch, descriptors, authorization)
                    .await
            }
        }
    }

    async fn upload_page_group(
        &self,
        kind: IndexKind,
        descriptors: Vec<OciDescriptor>,
        authorization: Option<&HeaderValue>,
    ) -> Result<Vec<PageRef>, TreeError> {
        let partitions = partition_descriptors(kind, descriptors)?;
        if partitions.is_empty() {
            if kind == IndexKind::Leaf {
                return Ok(vec![
                    self.upload_index(&OciIndex::new(IndexKind::Leaf, Vec::new()), authorization)
                        .await?,
                ]);
            }
            return Ok(Vec::new());
        }
        let mut pages = Vec::with_capacity(partitions.len());
        for manifests in partitions {
            pages.push(
                self.upload_index(&OciIndex::new(kind, manifests), authorization)
                    .await?,
            );
        }
        Ok(pages)
    }

    async fn promote_root(
        &self,
        mut pages: Vec<PageRef>,
        authorization: Option<&HeaderValue>,
    ) -> Result<PageRef, TreeError> {
        if pages.is_empty() {
            return self
                .upload_index(&OciIndex::new(IndexKind::Leaf, Vec::new()), authorization)
                .await;
        }
        while pages.len() > 1 {
            let descriptors = pages
                .iter()
                .map(branch_descriptor)
                .collect::<Result<Vec<_>, _>>()?;
            pages = self
                .upload_page_group(IndexKind::Branch, descriptors, authorization)
                .await?;
        }
        Ok(pages.remove(0))
    }

    pub(super) async fn upload_index(
        &self,
        index: &OciIndex,
        authorization: Option<&HeaderValue>,
    ) -> Result<PageRef, TreeError> {
        let bytes = index.serialize()?;
        let digest = Digest::from_bytes(&bytes);
        let published = self
            .registry
            .put_manifest(
                &ManifestReference::Digest(digest.clone()),
                OCI_IMAGE_INDEX,
                Bytes::from(bytes),
                authorization,
            )
            .await?;
        if published.digest != digest
            || published.media_type != OCI_IMAGE_INDEX
            || published.size > MAX_INDEX_BYTES as u64
        {
            return Err(TreeError::IntegrityError);
        }
        let expected_range = index
            .bounds()?
            .map(|(first, last)| (first.to_owned(), last.to_owned()));
        Ok(PageRef {
            descriptor: OciDescriptor::new(OCI_IMAGE_INDEX, &published.digest, published.size),
            expected_range,
        })
    }

    pub(super) async fn publish_root(
        &self,
        root: &PageRef,
        previous: Option<&Digest>,
        authorization: Option<&HeaderValue>,
    ) -> Result<Digest, TreeError> {
        let digest = root.descriptor.digest()?;
        let manifest = self
            .registry
            .get_manifest(
                &ManifestReference::Digest(digest.clone()),
                &[OCI_IMAGE_INDEX],
                authorization,
            )
            .await
            .map_err(|error| map_reachable_error(error, false))?;
        if manifest.descriptor.size != root.descriptor.size
            || manifest.descriptor.digest != digest
            || manifest.descriptor.media_type != OCI_IMAGE_INDEX
        {
            return Err(TreeError::IntegrityError);
        }
        let tag =
            ManifestReference::parse(&self.root_tag).map_err(|_| TreeError::InvalidMountTarget)?;
        let first = self
            .registry
            .put_manifest(&tag, OCI_IMAGE_INDEX, manifest.bytes.clone(), authorization)
            .await;
        match first {
            Ok(published) => {
                if published.digest != digest {
                    return Err(TreeError::CommitOutcomeUnknown);
                }
                match self.current_root_digest(authorization).await {
                    Ok(Some(current)) if current == digest => Ok(digest),
                    _ => Err(TreeError::CommitOutcomeUnknown),
                }
            }
            Err(error) => {
                let current = self.current_root_digest(authorization).await?;
                if current.as_ref() == Some(&digest) {
                    return Ok(digest);
                }
                if !matches_previous(current.as_ref(), previous) {
                    return Err(TreeError::CommitOutcomeUnknown);
                }
                if !ambiguous_root_error(&error) {
                    return Err(TreeError::Registry(error));
                }
                let retry = self
                    .registry
                    .put_manifest(&tag, OCI_IMAGE_INDEX, manifest.bytes, authorization)
                    .await;
                match retry {
                    Ok(published) if published.digest == digest => {}
                    Ok(_) => return Err(TreeError::CommitOutcomeUnknown),
                    Err(error) => {
                        let current = self.current_root_digest(authorization).await?;
                        if current.as_ref() == Some(&digest) {
                            return Ok(digest);
                        }
                        if matches_previous(current.as_ref(), previous)
                            && !ambiguous_root_error(&error)
                        {
                            return Err(TreeError::Registry(error));
                        }
                        return Err(TreeError::CommitOutcomeUnknown);
                    }
                }
                match self.current_root_digest(authorization).await {
                    Ok(Some(current)) if current == digest => Ok(digest),
                    _ => Err(TreeError::CommitOutcomeUnknown),
                }
            }
        }
    }

    async fn current_root_digest(
        &self,
        authorization: Option<&HeaderValue>,
    ) -> Result<Option<Digest>, TreeError> {
        match self
            .registry
            .resolve_tag(&self.root_tag, &[OCI_IMAGE_INDEX], authorization)
            .await
        {
            Ok(descriptor) if descriptor.media_type == OCI_IMAGE_INDEX => {
                Ok(Some(descriptor.digest))
            }
            Ok(_) => Err(TreeError::CommitOutcomeUnknown),
            Err(error) if error.kind == ErrorKind::NotFound => Ok(None),
            Err(_) => Err(TreeError::CommitOutcomeUnknown),
        }
    }
}

fn choose_branch_child(
    children: &[PageRef],
    name: &str,
    operation: &EntryOperation,
) -> Option<usize> {
    let insert = matches!(operation, EntryOperation::Put { .. });
    for (index, child) in children.iter().enumerate() {
        let (first, last) = child.expected_range.as_ref()?;
        if first.as_bytes() <= name.as_bytes() && name.as_bytes() <= last.as_bytes() {
            return Some(index);
        }
        if insert && name.as_bytes() < first.as_bytes() {
            return Some(index);
        }
    }
    if insert {
        children.len().checked_sub(1)
    } else {
        None
    }
}

fn branch_descriptor(page: &PageRef) -> Result<OciDescriptor, TreeError> {
    let (first, last) = page
        .expected_range
        .as_ref()
        .ok_or(TreeError::IntegrityError)?;
    Ok(OciDescriptor::new(
        OCI_IMAGE_INDEX,
        &page.descriptor.digest()?,
        page.descriptor.size,
    )
    .with_range(first, last))
}

fn partition_descriptors(
    kind: IndexKind,
    descriptors: Vec<OciDescriptor>,
) -> Result<Vec<Vec<OciDescriptor>>, TreeError> {
    let mut pages = Vec::new();
    let mut current = Vec::new();
    for descriptor in descriptors {
        let mut candidate = current.clone();
        candidate.push(descriptor.clone());
        let encoded_size = serde_json::to_vec(&OciIndex::new(kind, candidate.clone()))
            .map_err(|_| TreeError::IntegrityError)?
            .len();
        if candidate.len() <= MAX_INDEX_DESCRIPTORS && encoded_size <= MAX_INDEX_BYTES {
            current = candidate;
            continue;
        }
        if current.is_empty() {
            return Err(TreeError::IntegrityError);
        }
        pages.push(std::mem::take(&mut current));
        let single_size = serde_json::to_vec(&OciIndex::new(kind, vec![descriptor.clone()]))
            .map_err(|_| TreeError::IntegrityError)?
            .len();
        if single_size > MAX_INDEX_BYTES {
            return Err(TreeError::IntegrityError);
        }
        current.push(descriptor);
    }
    if !current.is_empty() {
        pages.push(current);
    }
    if kind == IndexKind::Branch
        && pages.len() > 1
        && pages.last().is_some_and(|page| page.len() == 1)
    {
        let last = pages.pop().ok_or(TreeError::IntegrityError)?;
        let previous = pages.last_mut().ok_or(TreeError::IntegrityError)?;
        let moved = previous.pop().ok_or(TreeError::IntegrityError)?;
        let mut balanced = vec![moved];
        balanced.extend(last);
        if previous.len() < 2
            || serde_json::to_vec(&OciIndex::new(IndexKind::Branch, balanced.clone()))
                .map_err(|_| TreeError::IntegrityError)?
                .len()
                > MAX_INDEX_BYTES
        {
            return Err(TreeError::IntegrityError);
        }
        pages.push(balanced);
    }
    Ok(pages)
}

fn matches_previous(current: Option<&Digest>, previous: Option<&Digest>) -> bool {
    current == previous
}

async fn verify_source_stream<S>(
    source: S,
    expected_digest: &Digest,
    expected_size: u64,
) -> Result<(), TreeError>
where
    S: Stream<Item = Result<Bytes, io::Error>>,
{
    let mut source = Box::pin(source);
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    while let Some(chunk) = source.next().await {
        let chunk = chunk.map_err(|_| TreeError::InputStream)?;
        let length = u64::try_from(chunk.len()).map_err(|_| TreeError::InvalidFileSize)?;
        size = size.checked_add(length).ok_or(TreeError::InvalidFileSize)?;
        if size > expected_size {
            return Err(TreeError::InvalidFileContent);
        }
        hasher.update(&chunk);
    }
    if size != expected_size {
        return Err(TreeError::InvalidFileContent);
    }
    let mut actual = String::from("sha256:");
    for byte in hasher.finalize() {
        use std::fmt::Write as _;
        write!(actual, "{byte:02x}").map_err(|_| TreeError::IntegrityError)?;
    }
    if Digest::parse(&actual).map_err(|_| TreeError::IntegrityError)? != *expected_digest {
        return Err(TreeError::InvalidFileContent);
    }
    Ok(())
}

fn ambiguous_root_error(error: &crate::registry::RegistryError) -> bool {
    matches!(
        error.kind,
        ErrorKind::Timeout | ErrorKind::Transport | ErrorKind::Server | ErrorKind::DigestMismatch
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::OCI_IMAGE_MANIFEST;

    #[tokio::test]
    async fn file_stream_verification_checks_declared_digest_and_size() {
        let bytes = Bytes::from_static(b"streamed file bytes");
        let digest = Digest::from_bytes(&bytes);
        let source = futures_util::stream::iter(vec![Ok::<_, io::Error>(bytes.clone())]);
        verify_source_stream(source, &digest, bytes.len() as u64)
            .await
            .unwrap();

        let wrong_digest = Digest::from_bytes(b"not the stream");
        let source = futures_util::stream::iter(vec![Ok::<_, io::Error>(bytes.clone())]);
        assert!(matches!(
            verify_source_stream(source, &wrong_digest, bytes.len() as u64).await,
            Err(TreeError::InvalidFileContent)
        ));

        let source = futures_util::stream::iter(vec![Ok::<_, io::Error>(bytes.clone())]);
        assert!(matches!(
            verify_source_stream(source, &digest, (bytes.len() - 1) as u64).await,
            Err(TreeError::InvalidFileContent)
        ));

        let failed_source =
            futures_util::stream::iter(vec![Err::<Bytes, _>(io::Error::other("input failed"))]);
        assert!(matches!(
            verify_source_stream(failed_source, &digest, bytes.len() as u64).await,
            Err(TreeError::InputStream)
        ));
    }

    #[test]
    fn leaf_pages_split_at_the_descriptor_limit() {
        let entries = (0..129)
            .map(|index| {
                let name = format!("f{index:03}");
                OciDescriptor::new(OCI_IMAGE_MANIFEST, &Digest::from_bytes(name.as_bytes()), 10)
                    .with_entry_name(&name)
            })
            .collect();
        let pages = partition_descriptors(IndexKind::Leaf, entries).unwrap();
        assert_eq!(pages.iter().map(Vec::len).collect::<Vec<_>>(), [128, 1]);
        for page in pages {
            OciIndex::new(IndexKind::Leaf, page).validate().unwrap();
        }
    }

    #[test]
    fn branch_pages_never_leave_a_single_child_page() {
        let pages = (0..129)
            .map(|index| {
                let name = format!("p{index:03}");
                OciDescriptor::new(OCI_IMAGE_INDEX, &Digest::from_bytes(name.as_bytes()), 10)
                    .with_range(&name, &name)
            })
            .collect();
        let partitions = partition_descriptors(IndexKind::Branch, pages).unwrap();
        assert_eq!(
            partitions.iter().map(Vec::len).collect::<Vec<_>>(),
            [127, 2]
        );
        for partition in partitions {
            OciIndex::new(IndexKind::Branch, partition)
                .validate()
                .unwrap();
        }
    }
}
