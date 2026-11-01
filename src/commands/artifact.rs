use std::{
    fs::File,
    io::{self, IsTerminal, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use bytes::Bytes;
use futures_util::{TryStreamExt, stream};
use oci_auto_http::{
    config::{Config, Mount},
    error::AppError,
    registry::{ClientPolicy, Digest, RegistryClient},
    tree::{DirectoryEntry, EntryKind, StoragePath, TreeError, TreeStore},
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;

use crate::cli::{ArtifactArgs, ArtifactCommand};

const INPUT_CHUNK_BYTES: usize = 64 * 1024;
const MAX_CA_CERT_BYTES: u64 = 1024 * 1024;

pub async fn run(args: ArtifactArgs) -> Result<(), AppError> {
    let config = Config::load(&args.config)?;
    let mut policy = ClientPolicy::default();
    if let Some(certificate_path) = args.ca_cert {
        let certificate = read_ca_certificate(&certificate_path)?;
        policy = policy.with_root_certificate_pem(&certificate);
    }

    match args.command {
        ArtifactCommand::Create {
            path,
            input,
            directory,
        } => {
            let (store, storage_path) = store_for_path(&config, &policy, &path, false)?;
            let root_digest = if directory {
                store.create_directory(&storage_path, None).await?
            } else {
                if storage_path.is_root() {
                    return Err(TreeError::InvalidPath.into());
                }
                match store.stat_file(&storage_path, None).await {
                    Ok(_) => return Err(TreeError::AlreadyExists.into()),
                    Err(TreeError::NotFound | TreeError::MissingRoot) => {}
                    Err(error) => return Err(error.into()),
                }
                let input = prepare_input(input).await?;
                store
                    .write_file_stream(
                        &storage_path,
                        input.digest,
                        input.size,
                        file_stream(input.file),
                        false,
                        None,
                    )
                    .await?
            };
            write_json(&MutationOutput {
                path: &path,
                root_digest: root_digest.as_str(),
            })
        }
        ArtifactCommand::Read { path, output } => {
            let (store, storage_path) = store_for_path(&config, &policy, &path, false)?;
            let file = store.read_file(&storage_path, None).await?;
            write_download(file.body, &output).await
        }
        ArtifactCommand::List { path, limit } => {
            let (store, storage_path) = if let Some(path) = path.as_deref() {
                store_for_path(&config, &policy, path, true)?
            } else {
                if config.mounts.len() != 1 {
                    return Err(AppError::ListPathRequired);
                }
                (
                    store_for_mount(&config.mounts[0], &policy)?,
                    StoragePath::root(),
                )
            };
            write_listing(&store, &storage_path, limit).await
        }
        ArtifactCommand::Update { path, input } => {
            let (store, storage_path) = store_for_path(&config, &policy, &path, false)?;
            store.stat_file(&storage_path, None).await?;
            let input = prepare_input(input).await?;
            let root_digest = store
                .replace_file_stream(
                    &storage_path,
                    input.digest,
                    input.size,
                    file_stream(input.file),
                    None,
                )
                .await?;
            write_json(&MutationOutput {
                path: &path,
                root_digest: root_digest.as_str(),
            })
        }
        ArtifactCommand::Delete { path, recursive } => {
            let (store, storage_path) = store_for_path(&config, &policy, &path, false)?;
            let root_digest = store.remove(&storage_path, recursive, None).await?;
            write_json(&MutationOutput {
                path: &path,
                root_digest: root_digest.as_str(),
            })
        }
    }
}

fn store_for_path(
    config: &Config,
    policy: &ClientPolicy,
    served_path: &str,
    allow_trailing_slash: bool,
) -> Result<(TreeStore, StoragePath), AppError> {
    let (mount, path) = resolve_served_path(config, served_path, allow_trailing_slash)?;
    Ok((store_for_mount(mount, policy)?, path))
}

fn store_for_mount(mount: &Mount, policy: &ClientPolicy) -> Result<TreeStore, AppError> {
    let registry = RegistryClient::new(mount.clone(), policy.clone())?;
    Ok(TreeStore::new(registry)?)
}

fn resolve_served_path<'a>(
    config: &'a Config,
    served_path: &str,
    allow_trailing_slash: bool,
) -> Result<(&'a Mount, StoragePath), AppError> {
    let resolved = config
        .resolve_served_path(served_path)?
        .ok_or(AppError::NoMountForPath)?;
    if resolved.trailing_slash && !allow_trailing_slash {
        return Err(AppError::InvalidServedPath);
    }
    Ok((resolved.mount, resolved.storage_path))
}

async fn prepare_input(input: Option<PathBuf>) -> Result<PreparedInput, AppError> {
    tokio::task::spawn_blocking(move || prepare_input_blocking(input))
        .await
        .map_err(AppError::InputTask)?
}

struct PreparedInput {
    file: File,
    digest: Digest,
    size: u64,
}

fn prepare_input_blocking(input: Option<PathBuf>) -> Result<PreparedInput, AppError> {
    let from_stdin = input.as_deref().is_none_or(|path| path.as_os_str() == "-");
    if from_stdin && io::stdin().is_terminal() {
        return Err(AppError::InteractiveInput);
    }

    let (mut file, digest, size) = if from_stdin {
        let mut spool = tempfile::tempfile().map_err(AppError::ArtifactInput)?;
        let stdin = io::stdin();
        let mut reader = stdin.lock();
        let (digest, size) = hash_reader(&mut reader, Some(&mut spool))?;
        (spool, digest, size)
    } else {
        let mut file = File::open(input.as_deref().expect("non-stdin input has a path"))
            .map_err(AppError::ArtifactInput)?;
        let (digest, size) = hash_reader(&mut file, None)?;
        (file, digest, size)
    };
    file.seek(SeekFrom::Start(0))
        .map_err(AppError::ArtifactInput)?;
    Ok(PreparedInput { file, digest, size })
}

fn hash_reader(
    reader: &mut impl Read,
    mut spool: Option<&mut File>,
) -> Result<(Digest, u64), AppError> {
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; INPUT_CHUNK_BYTES];
    loop {
        let read = reader.read(&mut buffer).map_err(AppError::ArtifactInput)?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| AppError::ArtifactInput(io::Error::other("input size overflow")))?;
        if size > i64::MAX as u64 {
            return Err(AppError::Tree(
                oci_auto_http::tree::TreeError::InvalidFileSize,
            ));
        }
        hasher.update(&buffer[..read]);
        if let Some(spool) = spool.as_deref_mut() {
            spool
                .write_all(&buffer[..read])
                .map_err(AppError::ArtifactOutput)?;
        }
    }

    let digest = Digest::parse(&format!("sha256:{:x}", hasher.finalize()))?;
    Ok((digest, size))
}

fn file_stream(
    file: File,
) -> impl futures_util::Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
    let file = tokio::fs::File::from_std(file);
    stream::try_unfold(file, |mut file| async move {
        use tokio::io::AsyncReadExt;

        let mut buffer = vec![0_u8; INPUT_CHUNK_BYTES];
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            Ok(None)
        } else {
            buffer.truncate(read);
            Ok(Some((Bytes::from(buffer), file)))
        }
    })
}

async fn write_download(
    mut body: impl futures_util::Stream<Item = Result<Bytes, oci_auto_http::tree::TreeError>> + Unpin,
    output: &Path,
) -> Result<(), AppError> {
    if output.as_os_str() == "-" {
        let mut stdout = tokio::io::stdout();
        while let Some(chunk) = body.try_next().await? {
            stdout
                .write_all(&chunk)
                .await
                .map_err(AppError::ArtifactOutput)?;
        }
        stdout.flush().await.map_err(AppError::ArtifactOutput)?;
        return Ok(());
    }

    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = NamedTempFile::new_in(parent).map_err(AppError::ArtifactOutput)?;
    while let Some(chunk) = body.try_next().await? {
        temporary
            .write_all(&chunk)
            .map_err(AppError::ArtifactOutput)?;
    }
    temporary.flush().map_err(AppError::ArtifactOutput)?;
    temporary
        .as_file()
        .sync_all()
        .map_err(AppError::ArtifactOutput)?;
    temporary
        .persist(output)
        .map_err(|error| AppError::ArtifactOutput(error.error))?;
    Ok(())
}

async fn write_listing(
    store: &TreeStore,
    path: &StoragePath,
    limit: usize,
) -> Result<(), AppError> {
    let mut cursor = None;
    let mut header_written = false;
    loop {
        let page = store
            .list_page(path, limit, cursor.as_deref(), None)
            .await?;
        if !header_written {
            write_json_line(&ListingHeader {
                record_type: "listing",
                root_digest: page.root_digest.to_string(),
            })?;
            header_written = true;
        }
        for entry in &page.entries {
            write_json_line(&ListingEntry::from(entry))?;
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(()),
        }
    }
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

fn write_json(value: &impl Serialize) -> Result<(), AppError> {
    write_json_line(value)
}

fn write_json_line(value: &impl Serialize) -> Result<(), AppError> {
    let mut encoded = serde_json::to_vec(value)?;
    encoded.push(b'\n');
    io::stdout()
        .lock()
        .write_all(&encoded)
        .map_err(AppError::ArtifactOutput)
}

#[derive(Serialize)]
struct MutationOutput<'a> {
    path: &'a str,
    root_digest: &'a str,
}

#[derive(Serialize)]
struct ListingHeader {
    #[serde(rename = "type")]
    record_type: &'static str,
    root_digest: String,
}

#[derive(Serialize)]
struct ListingEntry {
    #[serde(rename = "type")]
    record_type: &'static str,
    name: String,
    kind: &'static str,
    digest: String,
    size: u64,
}

impl From<&DirectoryEntry> for ListingEntry {
    fn from(entry: &DirectoryEntry) -> Self {
        Self {
            record_type: "entry",
            name: entry.name.clone(),
            kind: match entry.kind {
                EntryKind::File => "file",
                EntryKind::Directory => "directory",
            },
            digest: entry.digest.to_string(),
            size: entry.size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config::parse(
            Path::new("test-config.toml"),
            r#"
version = 1

[[mounts]]
name = "packages"
prefix = "/packages"
registry = "https://registry.example"
repository = "team/packages"

[[mounts]]
name = "mirror"
prefix = "/mirror%20releases"
registry = "https://registry.example"
repository = "team/mirror"
"#,
        )
        .unwrap()
    }

    #[test]
    fn served_paths_select_a_mount_and_decode_once() {
        let config = test_config();
        let (mount, path) = resolve_served_path(&config, "/packages/a%25b/+meta", false).unwrap();
        assert_eq!(mount.name, "packages");
        assert_eq!(path.components(), &["a%b", "+meta"]);

        let (mount, path) =
            resolve_served_path(&config, "/mirror%20releases/%E2%98%83", false).unwrap();
        assert_eq!(mount.name, "mirror");
        assert_eq!(path.components(), &["☃"]);
    }

    #[test]
    fn served_paths_reject_ambiguous_or_unsafe_components() {
        let config = test_config();
        for path in [
            "packages/file",
            "/packages//file",
            "/packages/a%2Fb",
            "/packages/%2e%2e/secret",
            "/packages/%",
            "/packages/a?query",
            "/packages/a#fragment",
            "/packages/file/",
        ] {
            assert!(
                resolve_served_path(&config, path, false).is_err(),
                "accepted {path}"
            );
        }
        assert!(resolve_served_path(&config, "/packages/file/", true).is_ok());
    }
}
