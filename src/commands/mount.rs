use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use fs2::FileExt;

use oci_auto_http::{
    config::{Config, Mount},
    error::AppError,
};
use serde::Serialize;
use tempfile::NamedTempFile;
use toml_edit::{Array, ArrayOfTables, DocumentMut, Item, Table, value};

use crate::cli::{MountArgs, MountCommand};

pub fn run(args: MountArgs) -> Result<(), AppError> {
    match args.command {
        MountCommand::Create {
            name,
            prefix,
            registry,
            repository,
            root_tag,
            redirect_allowlist,
        } => {
            let output = create(
                &args.config,
                &name,
                &prefix,
                &registry,
                &repository,
                root_tag.as_deref(),
                &redirect_allowlist,
            )?;
            write_json(&output)
        }
        MountCommand::List => {
            let config = Config::load(&args.config)?;
            write_json(
                &config
                    .mounts
                    .iter()
                    .map(MountOutput::from)
                    .collect::<Vec<_>>(),
            )
        }
        MountCommand::Show { name } => {
            let config = Config::load(&args.config)?;
            let mount = find_mount(&config, &name)?;
            write_json(&MountOutput::from(mount))
        }
        MountCommand::Update {
            name,
            prefix,
            registry,
            repository,
            root_tag,
            reset_root_tag,
            redirect_allowlist,
            clear_redirect_allowlist,
        } => {
            let output = update(
                &args.config,
                MountUpdate {
                    name: &name,
                    prefix: prefix.as_deref(),
                    registry: registry.as_deref(),
                    repository: repository.as_deref(),
                    root_tag: root_tag.as_deref(),
                    reset_root_tag,
                    redirect_allowlist: redirect_allowlist.as_deref(),
                    clear_redirect_allowlist,
                },
            )?;
            write_json(&output)
        }
        MountCommand::Delete { name } => {
            let output = delete(&args.config, &name)?;
            write_json(&output)
        }
    }
}

fn create(
    path: &Path,
    name: &str,
    prefix: &str,
    registry: &str,
    repository: &str,
    root_tag: Option<&str>,
    redirect_allowlist: &[String],
) -> Result<MountOutput, AppError> {
    let _lock = MountConfigLock::acquire(path, true)?;
    let (mut document, original) = read_document(path, true)?;
    let mounts = mounts_mut(path, &mut document, true)?;
    if mounts
        .iter()
        .any(|mount| mount.get("name").and_then(Item::as_str) == Some(name))
    {
        return Err(AppError::MountAlreadyExists {
            name: name.to_owned(),
        });
    }

    let mut mount = Table::new();
    mount["name"] = value(name);
    mount["prefix"] = value(prefix);
    mount["registry"] = value(registry);
    mount["repository"] = value(repository);
    if let Some(root_tag) = root_tag {
        mount["root_tag"] = value(root_tag);
    }
    if !redirect_allowlist.is_empty() {
        mount["redirect_allowlist"] = value(string_array(redirect_allowlist));
    }
    mounts.push(mount);

    let config = validate_and_write(path, &document, original.as_deref())?;
    Ok(MountOutput::from(find_mount(&config, name)?))
}

struct MountUpdate<'a> {
    name: &'a str,
    prefix: Option<&'a str>,
    registry: Option<&'a str>,
    repository: Option<&'a str>,
    root_tag: Option<&'a str>,
    reset_root_tag: bool,
    redirect_allowlist: Option<&'a [String]>,
    clear_redirect_allowlist: bool,
}

fn update(path: &Path, update: MountUpdate<'_>) -> Result<MountOutput, AppError> {
    let _lock = MountConfigLock::acquire(path, false)?;
    let MountUpdate {
        name,
        prefix,
        registry,
        repository,
        root_tag,
        reset_root_tag,
        redirect_allowlist,
        clear_redirect_allowlist,
    } = update;
    if prefix.is_none()
        && registry.is_none()
        && repository.is_none()
        && root_tag.is_none()
        && !reset_root_tag
        && redirect_allowlist.is_none()
        && !clear_redirect_allowlist
    {
        return Err(AppError::EmptyMountUpdate);
    }

    let (mut document, original) = read_document(path, false)?;
    let config = Config::parse(path, original.as_deref().expect("existing config was read"))?;
    let mount_index = config
        .mounts
        .iter()
        .position(|mount| mount.name == name)
        .ok_or_else(|| AppError::MountNotFound {
            name: name.to_owned(),
        })?;
    let mounts = mounts_mut(path, &mut document, false)?;
    let mount = mounts
        .get_mut(mount_index)
        .expect("validated config and TOML document have the same mounts");

    if let Some(prefix) = prefix {
        mount["prefix"] = value(prefix);
    }
    if let Some(registry) = registry {
        mount["registry"] = value(registry);
    }
    if let Some(repository) = repository {
        mount["repository"] = value(repository);
    }
    if let Some(root_tag) = root_tag {
        mount["root_tag"] = value(root_tag);
    } else if reset_root_tag {
        mount.remove("root_tag");
    }
    if let Some(redirect_allowlist) = redirect_allowlist {
        if redirect_allowlist.is_empty() {
            mount.remove("redirect_allowlist");
        } else {
            mount["redirect_allowlist"] = value(string_array(redirect_allowlist));
        }
    } else if clear_redirect_allowlist {
        mount.remove("redirect_allowlist");
    }

    let config = validate_and_write(path, &document, original.as_deref())?;
    Ok(MountOutput::from(find_mount(&config, name)?))
}

fn delete(path: &Path, name: &str) -> Result<DeleteOutput, AppError> {
    let _lock = MountConfigLock::acquire(path, false)?;
    let (mut document, original) = read_document(path, false)?;
    let config = Config::parse(path, original.as_deref().expect("existing config was read"))?;
    let mount_index = config
        .mounts
        .iter()
        .position(|mount| mount.name == name)
        .ok_or_else(|| AppError::MountNotFound {
            name: name.to_owned(),
        })?;
    if config.mounts.len() == 1 {
        return Err(AppError::CannotDeleteLastMount);
    }

    mounts_mut(path, &mut document, false)?.remove(mount_index);
    let _ = validate_and_write(path, &document, original.as_deref())?;
    Ok(DeleteOutput {
        deleted: name.to_owned(),
    })
}

fn find_mount<'a>(config: &'a Config, name: &str) -> Result<&'a Mount, AppError> {
    config
        .mounts
        .iter()
        .find(|mount| mount.name == name)
        .ok_or_else(|| AppError::MountNotFound {
            name: name.to_owned(),
        })
}

struct MountConfigLock {
    _file: File,
}

impl MountConfigLock {
    fn acquire(config_path: &Path, allow_missing: bool) -> Result<Self, AppError> {
        match fs::symlink_metadata(config_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(mount_io_error(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "mount configuration must not be a symlink",
                )));
            }
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(mount_io_error(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "mount configuration must be a regular file",
                )));
            }
            Ok(_) => {}
            Err(error) if allow_missing && error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(mount_io_error(error)),
        }

        let lock_path = lock_path(config_path)?;
        match fs::symlink_metadata(&lock_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(mount_io_error(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "mount configuration lock must not be a symlink",
                )));
            }
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(mount_io_error(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "mount configuration lock must be a regular file",
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(mount_io_error(error)),
        }

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(lock_path).map_err(mount_io_error)?;
        if !file.metadata().map_err(mount_io_error)?.is_file() {
            return Err(mount_io_error(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mount configuration lock must be a regular file",
            )));
        }
        file.lock_exclusive().map_err(mount_io_error)?;
        Ok(Self { _file: file })
    }
}

fn lock_path(config_path: &Path) -> Result<PathBuf, AppError> {
    let file_name = config_path.file_name().ok_or_else(|| {
        mount_io_error(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mount configuration path must name a file",
        ))
    })?;
    let parent = config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent).map_err(mount_io_error)?;
    let mut lock_name = std::ffi::OsString::from(".");
    lock_name.push(file_name);
    lock_name.push(".oci-auto-http.lock");
    Ok(parent.join(lock_name))
}

fn read_document(
    path: &Path,
    allow_missing: bool,
) -> Result<(DocumentMut, Option<String>), AppError> {
    let source = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(mount_io_error(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mount configuration must not be a symlink",
            )));
        }
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(mount_io_error(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mount configuration must be a regular file",
            )));
        }
        Ok(_) => Some(fs::read_to_string(path).map_err(mount_io_error)?),
        Err(error) if allow_missing && error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(mount_io_error(error)),
    };

    match source {
        Some(source) => {
            let document = source
                .parse::<DocumentMut>()
                .map_err(|_| AppError::InvalidConfig {
                    path: path.to_path_buf(),
                    reason: "invalid TOML syntax".to_owned(),
                })?;
            Ok((document, Some(source)))
        }
        None => {
            let mut document = DocumentMut::new();
            document["version"] = value(1);
            Ok((document, None))
        }
    }
}

fn mounts_mut<'a>(
    path: &Path,
    document: &'a mut DocumentMut,
    create_if_absent: bool,
) -> Result<&'a mut ArrayOfTables, AppError> {
    let missing_mounts = document.get("mounts").is_none();
    let empty_mount_array = document
        .get("mounts")
        .and_then(Item::as_array)
        .is_some_and(Array::is_empty);
    if create_if_absent && (missing_mounts || empty_mount_array) {
        document["mounts"] = Item::ArrayOfTables(ArrayOfTables::new());
    }
    document
        .get_mut("mounts")
        .and_then(Item::as_array_of_tables_mut)
        .ok_or_else(|| AppError::InvalidConfig {
            path: path.to_path_buf(),
            reason: "mounts must be an array of tables".to_owned(),
        })
}

fn string_array(values: &[String]) -> Array {
    let mut array = Array::new();
    for value in values {
        array.push(value.as_str());
    }
    array
}

fn validate_and_write(
    path: &Path,
    document: &DocumentMut,
    original: Option<&str>,
) -> Result<Config, AppError> {
    let rendered = document.to_string();
    let config = Config::parse(path, &rendered)?;
    atomic_write(path, rendered.as_bytes(), original)?;
    Ok(config)
}

fn atomic_write(path: &Path, contents: &[u8], original: Option<&str>) -> Result<(), AppError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let existing_permissions = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(mount_io_error(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mount configuration must not be a symlink",
            )));
        }
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(mount_io_error(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mount configuration must be a regular file",
            )));
        }
        Ok(metadata) => Some(metadata.permissions()),
        Err(error) if error.kind() == io::ErrorKind::NotFound && original.is_none() => None,
        Err(error) => return Err(mount_io_error(error)),
    };

    let mut temporary = NamedTempFile::new_in(parent).map_err(mount_io_error)?;
    temporary.write_all(contents).map_err(mount_io_error)?;
    if let Some(permissions) = existing_permissions {
        temporary
            .as_file()
            .set_permissions(permissions)
            .map_err(mount_io_error)?;
    }
    temporary.as_file().sync_all().map_err(mount_io_error)?;

    match original {
        Some(original) => {
            let current = fs::read_to_string(path).map_err(mount_io_error)?;
            if current != original {
                return Err(AppError::MountConfigChanged);
            }
        }
        None => match fs::symlink_metadata(path) {
            Ok(_) => return Err(AppError::MountConfigChanged),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(mount_io_error(error)),
        },
    }

    temporary
        .persist(path)
        .map_err(|error| mount_io_error(error.error))?;
    Ok(())
}

fn mount_io_error(error: io::Error) -> AppError {
    AppError::MountConfigIo(error)
}

fn write_json<T: Serialize>(value: &T) -> Result<(), AppError> {
    let mut output = serde_json::to_vec(value)?;
    output.push(b'\n');
    io::stdout()
        .lock()
        .write_all(&output)
        .map_err(AppError::ArtifactOutput)
}

#[derive(Serialize)]
struct MountOutput {
    name: String,
    prefix: String,
    registry: String,
    repository: String,
    root_tag: String,
    redirect_allowlist: Vec<String>,
}

impl From<&Mount> for MountOutput {
    fn from(mount: &Mount) -> Self {
        Self {
            name: mount.name.clone(),
            prefix: mount.prefix.clone(),
            registry: mount.registry.as_str().trim_end_matches('/').to_owned(),
            repository: mount.repository.clone(),
            root_tag: mount.root_tag.clone(),
            redirect_allowlist: mount.redirect_allowlist.clone(),
        }
    }
}

#[derive(Serialize)]
struct DeleteOutput {
    deleted: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const MOUNT_CONFIG: &str = r#"# Keep this header comment.
version = 1

[server]
listen = "127.0.0.1:9090" # Preserve this inline comment.

[[mounts]]
# Keep this mount comment.
name = "packages"
prefix = "/packages"
registry = "https://registry.example"
repository = "team/packages"
root_tag = "packages-v2"
redirect_allowlist = ["cdn.example:443"]

[[mounts]]
name = "mirror"
prefix = "/mirror"
registry = "https://registry.example"
repository = "team/mirror"
"#;

    #[test]
    fn create_initializes_config_and_preserves_toml_comments() {
        let directory = tempfile::tempdir().unwrap();
        let new_config_path = directory.path().join("new.toml");
        create(
            &new_config_path,
            "packages",
            "/packages",
            "https://registry.example",
            "team/packages",
            None,
            &[],
        )
        .unwrap();
        let new_config = Config::load(&new_config_path).unwrap();
        assert_eq!(new_config.mounts.len(), 1);
        assert_eq!(new_config.mounts[0].root_tag, "oci-auto-http-tree-v1");

        let empty_array_path = directory.path().join("empty-array.toml");
        fs::write(&empty_array_path, "version = 1\nmounts = []\n").unwrap();
        create(
            &empty_array_path,
            "empty-seed",
            "/seed",
            "https://registry.example",
            "team/seed",
            None,
            &[],
        )
        .unwrap();
        assert_eq!(Config::load(&empty_array_path).unwrap().mounts.len(), 1);

        let path = directory.path().join("existing.toml");
        fs::write(&path, MOUNT_CONFIG).unwrap();
        create(
            &path,
            "staging",
            "/staging",
            "https://registry.example",
            "team/staging",
            Some("stage-v1"),
            &["cdn2.example:443".to_owned()],
        )
        .unwrap();
        let rendered = fs::read_to_string(&path).unwrap();
        assert!(rendered.contains("# Keep this header comment."));
        assert!(rendered.contains("# Preserve this inline comment."));
        assert!(rendered.contains("# Keep this mount comment."));
        let config = Config::load(&path).unwrap();
        assert_eq!(config.mounts.len(), 3);
        assert_eq!(config.mounts[2].root_tag, "stage-v1");
        assert_eq!(config.mounts[2].redirect_allowlist, ["cdn2.example:443"]);
    }

    #[test]
    fn updates_validate_before_persisting_and_deletes_protect_the_final_mount() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, MOUNT_CONFIG).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        }

        let original = fs::read_to_string(&path).unwrap();
        let invalid = update(
            &path,
            MountUpdate {
                name: "mirror",
                prefix: Some("/packages/child"),
                registry: None,
                repository: None,
                root_tag: None,
                reset_root_tag: false,
                redirect_allowlist: None,
                clear_redirect_allowlist: false,
            },
        );
        assert!(matches!(invalid, Err(AppError::InvalidConfig { .. })));
        assert_eq!(fs::read_to_string(&path).unwrap(), original);

        update(
            &path,
            MountUpdate {
                name: "mirror",
                prefix: Some("/mirror-v2"),
                registry: None,
                repository: Some("team/new-mirror"),
                root_tag: Some("mirror-v2"),
                reset_root_tag: false,
                redirect_allowlist: Some(&["cdn2.example:8443".to_owned()]),
                clear_redirect_allowlist: false,
            },
        )
        .unwrap();
        let config = Config::load(&path).unwrap();
        let mirror = find_mount(&config, "mirror").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o640
            );
        }
        assert_eq!(mirror.prefix, "/mirror-v2");
        assert_eq!(mirror.repository, "team/new-mirror");
        assert_eq!(mirror.root_tag, "mirror-v2");
        assert_eq!(mirror.redirect_allowlist, ["cdn2.example:8443"]);

        delete(&path, "mirror").unwrap();
        assert_eq!(Config::load(&path).unwrap().mounts.len(), 1);
        let remaining = fs::read_to_string(&path).unwrap();
        assert!(matches!(
            delete(&path, "packages"),
            Err(AppError::CannotDeleteLastMount)
        ));
        assert_eq!(fs::read_to_string(&path).unwrap(), remaining);
    }

    #[test]
    fn concurrent_mount_updates_are_serialized_and_both_changes_survive() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, MOUNT_CONFIG).unwrap();

        let first_lock = MountConfigLock::acquire(&path, false).unwrap();
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock_path(&path).unwrap())
            .unwrap();
        assert_eq!(
            lock_file.try_lock_exclusive().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(lock_file);
        drop(first_lock);

        let barrier = Arc::new(Barrier::new(3));
        let package_path = path.clone();
        let package_barrier = Arc::clone(&barrier);
        let package_update = thread::spawn(move || {
            package_barrier.wait();
            update(
                &package_path,
                MountUpdate {
                    name: "packages",
                    prefix: Some("/packages-v2"),
                    registry: None,
                    repository: None,
                    root_tag: None,
                    reset_root_tag: false,
                    redirect_allowlist: None,
                    clear_redirect_allowlist: false,
                },
            )
        });
        let mirror_path = path.clone();
        let mirror_barrier = Arc::clone(&barrier);
        let mirror_update = thread::spawn(move || {
            mirror_barrier.wait();
            update(
                &mirror_path,
                MountUpdate {
                    name: "mirror",
                    prefix: None,
                    registry: None,
                    repository: Some("team/mirror-v2"),
                    root_tag: None,
                    reset_root_tag: false,
                    redirect_allowlist: None,
                    clear_redirect_allowlist: false,
                },
            )
        });
        barrier.wait();
        package_update.join().unwrap().unwrap();
        mirror_update.join().unwrap().unwrap();

        let config = Config::load(&path).unwrap();
        assert_eq!(
            find_mount(&config, "packages").unwrap().prefix,
            "/packages-v2"
        );
        assert_eq!(
            find_mount(&config, "mirror").unwrap().repository,
            "team/mirror-v2"
        );
    }

    #[test]
    fn atomic_write_detects_concurrent_changes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        fs::write(&path, MOUNT_CONFIG).unwrap();
        let (document, original) = read_document(&path, false).unwrap();
        fs::write(&path, format!("{MOUNT_CONFIG}\n# concurrent edit\n")).unwrap();
        let error = validate_and_write(&path, &document, original.as_deref()).unwrap_err();
        assert!(matches!(error, AppError::MountConfigChanged));
        assert!(
            fs::read_to_string(&path)
                .unwrap()
                .ends_with("# concurrent edit\n")
        );
    }

    #[cfg(unix)]
    #[test]
    fn config_mutations_refuse_symlink_targets() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target.toml");
        let link = directory.path().join("link.toml");
        fs::write(&target, MOUNT_CONFIG).unwrap();
        symlink(&target, &link).unwrap();
        assert!(matches!(
            read_document(&link, false),
            Err(AppError::MountConfigIo(_))
        ));
    }
}
