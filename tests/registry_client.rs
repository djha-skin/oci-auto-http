#![cfg(unix)]

use std::{
    fs,
    io::{self, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{StreamExt, TryStreamExt};
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use oci_auto_http::{
    config::{Config, Mount},
    registry::{
        Bytes, ClientPolicy, Digest, ErrorKind, HeaderValue, ManifestReference, OCI_EMPTY_CONFIG,
        OCI_IMAGE_INDEX, OCI_IMAGE_MANIFEST, RegistryClient,
    },
    tree::{EntryKind, StoragePath, TreeError, TreeStore},
};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_rustls::{
    TlsAcceptor,
    rustls::pki_types::{CertificateDer, PrivateKeyDer},
};
use url::Url;

const REGISTRY_IMAGE: &str = "registry:2";
const TEST_PASSWORD: &str = "test";
const BASIC_AUTH: &str = "Basic dGVzdDp0ZXN0";

#[tokio::test]
async fn distribution_round_trip_matches_independent_registry_reads_and_tag_listing() {
    let registry = TestRegistry::new(false);
    let client = registry.client(ClientPolicy::default());

    let missing = Digest::from_bytes(b"not stored");
    let error = client.head_blob(&missing, None).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::NotFound);
    assert_eq!(error.status.unwrap().as_u16(), 404);

    let file_bytes = Bytes::from_static(b"file bytes: nul=\0 utf8=\xe2\x98\x83 opaque=\xff");
    let layer = client.put_blob(file_bytes.clone(), None).await.unwrap();
    assert_eq!(layer.digest, Digest::from_bytes(&file_bytes));
    assert_eq!(
        client.head_blob(&layer.digest, None).await.unwrap().size,
        file_bytes.len() as u64
    );
    let chunks: Vec<Bytes> = client
        .get_blob(&layer, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let fetched: Vec<u8> = chunks
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    assert_eq!(fetched, file_bytes);
    assert_eq!(
        registry.curl_api(&format!("blobs/{}", layer.digest), None),
        file_bytes
    );

    let empty = client.put_blob(Bytes::new(), None).await.unwrap();
    assert_eq!(empty.size, 0);
    let empty_chunks: Vec<Bytes> = client
        .get_blob(&empty, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(empty_chunks.is_empty());

    let wrong_digest = Digest::from_bytes(b"expected but not sent");
    let actual_bytes = Bytes::from_static(b"actual stream bytes");
    let source = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(actual_bytes.clone())]);
    let error = client
        .put_blob_stream(
            wrong_digest.clone(),
            actual_bytes.len() as u64,
            source,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::DigestMismatch);
    assert_eq!(
        client
            .head_blob(&wrong_digest, None)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound,
        "a digest-mismatched upload must not reach the final commit PUT"
    );
    assert_eq!(
        client
            .head_blob(&Digest::from_bytes(&actual_bytes), None)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound,
        "a rejected upload must not become visible under its actual digest"
    );

    let config_bytes = Bytes::from_static(b"{}");
    let config_blob = client.put_blob(config_bytes, None).await.unwrap();
    let manifest_bytes = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "mediaType": OCI_IMAGE_MANIFEST,
            "config": {
                "mediaType": OCI_EMPTY_CONFIG,
                "digest": config_blob.digest.to_string(),
                "size": config_blob.size,
            },
            "layers": [{
                "mediaType": "application/octet-stream",
                "digest": layer.digest.to_string(),
                "size": layer.size,
            }],
        }))
        .unwrap(),
    );
    let reference = ManifestReference::parse("tree-smoke").unwrap();
    let published = client
        .put_manifest(&reference, OCI_IMAGE_MANIFEST, manifest_bytes.clone(), None)
        .await
        .unwrap();
    assert_eq!(published.digest, Digest::from_bytes(&manifest_bytes));
    assert_eq!(
        client
            .resolve_tag("tree-smoke", &[OCI_IMAGE_MANIFEST], None)
            .await
            .unwrap(),
        published
    );
    let fetched = client
        .get_manifest(
            &ManifestReference::Digest(published.digest.clone()),
            &[OCI_IMAGE_MANIFEST],
            None,
        )
        .await
        .unwrap();
    assert_eq!(fetched.descriptor, published);
    assert_eq!(fetched.bytes, manifest_bytes);
    assert_eq!(
        registry.curl_api(
            &format!("manifests/{}", published.digest),
            Some(OCI_IMAGE_MANIFEST)
        ),
        manifest_bytes
    );
    assert_eq!(
        registry.curl_api("manifests/tree-smoke", Some(OCI_IMAGE_MANIFEST)),
        manifest_bytes
    );

    for index in 0..101 {
        let tag = format!("page-{index:03}");
        client
            .put_manifest(
                &ManifestReference::parse(&tag).unwrap(),
                OCI_IMAGE_MANIFEST,
                manifest_bytes.clone(),
                None,
            )
            .await
            .unwrap();
    }
    let tags = client.list_tags(None).await.unwrap();
    assert_eq!(tags.len(), 102);
    assert!(tags.contains(&"page-000".to_owned()));
    assert!(tags.contains(&"page-100".to_owned()));
    let independent_tags: serde_json::Value =
        serde_json::from_slice(&registry.curl_api("tags/list?n=100", Some("application/json")))
            .unwrap();
    assert_eq!(independent_tags["tags"].as_array().unwrap().len(), 102);

    client
        .delete_manifest(&published.digest, None)
        .await
        .unwrap();
    client.delete_blob(&layer.digest, None).await.unwrap();
    client.delete_blob(&config_blob.digest, None).await.unwrap();
    client.delete_blob(&empty.digest, None).await.unwrap();
    assert_eq!(
        client
            .head_blob(&layer.digest, None)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        client
            .resolve_tag("tree-smoke", &[OCI_IMAGE_MANIFEST], None)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn basic_auth_challenges_and_not_found_are_preserved_without_leaking_credentials() {
    let registry = TestRegistry::new(true);
    let client = registry.client(ClientPolicy::default());

    let unauthorized = client.list_tags(None).await.unwrap_err();
    assert_eq!(unauthorized.kind, ErrorKind::Unauthorized);
    assert_eq!(unauthorized.status.unwrap().as_u16(), 401);
    assert!(unauthorized.challenges.iter().any(|challenge| {
        challenge
            .to_str()
            .is_ok_and(|value| value.starts_with("Basic "))
    }));
    assert!(!unauthorized.to_string().contains(TEST_PASSWORD));
    assert!(!format!("{unauthorized:?}").contains(TEST_PASSWORD));

    let authorization = HeaderValue::from_static(BASIC_AUTH);
    let authorized_blob = client
        .put_blob(
            Bytes::from_static(b"authenticated registry write"),
            Some(&authorization),
        )
        .await
        .unwrap();
    let authorized_body: Vec<Bytes> = client
        .get_blob(&authorized_blob, Some(&authorization))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        authorized_body
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect::<Vec<_>>(),
        b"authenticated registry write"
    );
    let missing = Digest::from_bytes(b"authenticated missing blob");
    let not_found = client
        .head_blob(&missing, Some(&authorization))
        .await
        .unwrap_err();
    assert_eq!(not_found.kind, ErrorKind::NotFound);
    assert_eq!(not_found.status.unwrap().as_u16(), 404);

    let wrong_authorization = HeaderValue::from_static("Basic dGVzdDpiYWQ=");
    let denied = client
        .list_tags(Some(&wrong_authorization))
        .await
        .unwrap_err();
    assert_eq!(denied.kind, ErrorKind::Unauthorized);
    assert!(!format!("{denied:?}").contains("test:bad"));
}

#[tokio::test]
async fn configured_request_timeout_is_reported_against_a_real_registry() {
    let registry = TestRegistry::new(false);
    let policy = ClientPolicy {
        request_timeout: Duration::from_nanos(1),
        max_retries: 0,
        ..ClientPolicy::default()
    };
    let error = registry.client(policy).list_tags(None).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
}

#[tokio::test]
async fn interrupting_a_real_blob_transfer_surfaces_a_stream_error() {
    let registry = TestRegistry::new(false);
    let policy = ClientPolicy {
        request_timeout: Duration::from_secs(120),
        ..ClientPolicy::default()
    };
    let client = registry.client(policy);
    let bytes = Bytes::from(vec![0x5a; 128 * 1024 * 1024]);
    let descriptor = client.put_blob(bytes, None).await.unwrap();
    let mut body = client.get_blob(&descriptor, None).await.unwrap();
    let first = body.next().await.unwrap().unwrap();
    assert!(!first.is_empty());

    let container = registry.container.clone();
    let killed = tokio::task::spawn_blocking(move || {
        Command::new("docker")
            .args(["kill", &container])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        killed.status.success(),
        "docker kill failed: {:?}",
        killed.stderr
    );

    let mut received = first.len() as u64;
    let mut stream_error = None;
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(bytes) => received += bytes.len() as u64,
            Err(error) => {
                stream_error = Some(error);
                break;
            }
        }
    }
    let error = stream_error.expect("killing registry should interrupt the blob stream");
    assert!(matches!(
        error.kind,
        ErrorKind::Transport | ErrorKind::Timeout
    ));
    assert!(
        received < descriptor.size,
        "received the full blob after killing registry"
    );
}

#[tokio::test]
async fn tree_storage_survives_client_reconstruction_and_matches_independent_registry_reads() {
    let registry = TestRegistry::new(false);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.initialize(None).await.unwrap();

    let bytes = Bytes::from_static(b"nested tree bytes\0\xff\xe2\x98\x83");
    let path = StoragePath::parse_relative("nested/snow/data.bin").unwrap();
    tree.write_file(&path, bytes.clone(), false, None)
        .await
        .unwrap();
    tree.write_file(
        &StoragePath::parse_relative("nested/snow/empty.bin").unwrap(),
        Bytes::new(),
        false,
        None,
    )
    .await
    .unwrap();
    tree.create_directory(&StoragePath::parse_relative("empty/deep").unwrap(), None)
        .await
        .unwrap();

    let initial_listing = tree
        .list_page(&StoragePath::root(), 10, None, None)
        .await
        .unwrap();
    assert_eq!(
        initial_listing
            .entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry.kind))
            .collect::<Vec<_>>(),
        [
            ("empty", EntryKind::Directory),
            ("nested", EntryKind::Directory)
        ]
    );

    // Reconstruct both client and store; the registry root tag is the only
    // durable state required to recover the entire tree.
    let restarted = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    let file = restarted.read_file(&path, None).await.unwrap();
    let received: Vec<Bytes> = file.body.try_collect().await.unwrap();
    assert_eq!(received.concat(), bytes);
    assert_eq!(
        restarted.stat_file(&path, None).await.unwrap().digest,
        Digest::from_bytes(&bytes)
    );
    let nested = restarted
        .list_page(
            &StoragePath::parse_relative("nested/snow").unwrap(),
            10,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(nested.entries.len(), 2);
    assert_eq!(nested.entries[0].name, "data.bin");
    assert_eq!(nested.entries[1].name, "empty.bin");

    // Inspect the root index, nested directory indexes, file manifest, and
    // content blob using independent curl requests to the registry API.
    const ENTRY_NAME: &str = "io.github.djha-skin.oci-auto-http.entry-name";
    const NODE_KIND: &str = "io.github.djha-skin.oci-auto-http.node-kind";
    let root_bytes = registry.curl_api(
        &format!("manifests/{}", registry.mount.root_tag),
        Some(OCI_IMAGE_INDEX),
    );
    let root: serde_json::Value = serde_json::from_slice(&root_bytes).unwrap();
    assert_eq!(root["annotations"][NODE_KIND], "directory-leaf");
    let nested_ref = descriptor_named(&root, "nested", ENTRY_NAME);
    let nested_bytes = registry.curl_api(
        &format!("manifests/{}", nested_ref["digest"].as_str().unwrap()),
        Some(OCI_IMAGE_INDEX),
    );
    let nested_index: serde_json::Value = serde_json::from_slice(&nested_bytes).unwrap();
    let snow_ref = descriptor_named(&nested_index, "snow", ENTRY_NAME);
    let snow_bytes = registry.curl_api(
        &format!("manifests/{}", snow_ref["digest"].as_str().unwrap()),
        Some(OCI_IMAGE_INDEX),
    );
    let snow_index: serde_json::Value = serde_json::from_slice(&snow_bytes).unwrap();
    let file_ref = descriptor_named(&snow_index, "data.bin", ENTRY_NAME);
    let file_manifest_bytes = registry.curl_api(
        &format!("manifests/{}", file_ref["digest"].as_str().unwrap()),
        Some(OCI_IMAGE_MANIFEST),
    );
    let file_manifest: serde_json::Value = serde_json::from_slice(&file_manifest_bytes).unwrap();
    let layer_digest = file_manifest["layers"][0]["digest"].as_str().unwrap();
    assert_eq!(layer_digest, Digest::from_bytes(&bytes).to_string());
    assert_eq!(
        registry.curl_api(&format!("blobs/{layer_digest}"), None),
        bytes
    );
}

#[tokio::test]
async fn artifact_cli_crud_streams_and_persists_against_a_real_registry() {
    let registry = TestRegistry::new(false);
    let scratch = TempDir::new();
    let config = scratch.path.join("artifact-cli.toml");
    fs::write(
        &config,
        format!(
            r#"version = 1

[[mounts]]
name = "packages"
prefix = "/packages"
registry = "{}"
repository = "team/releases"
"#,
            registry.base_url
        ),
    )
    .unwrap();

    let file_bytes = b"file input\0 opaque=\xff";
    let file_input = scratch.path.join("file-input.bin");
    fs::write(&file_input, file_bytes).unwrap();
    let file_path = "/packages/nested/dir/from-file.bin";
    let created = artifact_cli(&registry, &config)
        .args(["create", file_path, "--input"])
        .arg(&file_input)
        .output()
        .unwrap();
    let created = assert_cli_success(created);
    let create_result: serde_json::Value = serde_json::from_slice(&created.stdout).unwrap();
    assert_eq!(create_result["path"], file_path);
    assert!(
        create_result["root_digest"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );

    let stdin_bytes = b"stdin input\0 opaque=\xfe";
    let stdin_path = "/packages/nested/dir/from-stdin.bin";
    let mut child = artifact_cli(&registry, &config)
        .args(["create", stdin_path])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin_bytes).unwrap();
    let created_from_stdin = assert_cli_success(child.wait_with_output().unwrap());
    let stdin_result: serde_json::Value =
        serde_json::from_slice(&created_from_stdin.stdout).unwrap();
    assert_eq!(stdin_result["path"], stdin_path);

    let listing = artifact_cli(&registry, &config)
        .args(["list", "/packages/nested/dir", "--limit", "1"])
        .output()
        .unwrap();
    let listing = assert_cli_success(listing);
    let records: Vec<serde_json::Value> = listing
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(records[0]["type"], "listing");
    let listing_root = records[0]["root_digest"].as_str().unwrap();
    assert!(listing_root.starts_with("sha256:"));
    let entries: Vec<_> = records
        .iter()
        .filter(|record| record["type"] == "entry")
        .collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["name"], "from-file.bin");
    assert_eq!(entries[1]["name"], "from-stdin.bin");

    let read_stdout = artifact_cli(&registry, &config)
        .args(["read", file_path])
        .output()
        .unwrap();
    let read_stdout = assert_cli_success(read_stdout);
    assert_eq!(read_stdout.stdout, file_bytes);

    let download = scratch.path.join("downloaded.bin");
    let read_file = artifact_cli(&registry, &config)
        .args(["read", stdin_path, "--output"])
        .arg(&download)
        .output()
        .unwrap();
    assert_cli_success(read_file);
    assert_eq!(fs::read(&download).unwrap(), stdin_bytes);

    let replacement_bytes = b"replacement from a regular file\0\xff";
    let replacement = scratch.path.join("replacement.bin");
    fs::write(&replacement, replacement_bytes).unwrap();
    let update = artifact_cli(&registry, &config)
        .args(["update", file_path, "--input"])
        .arg(&replacement)
        .output()
        .unwrap();
    assert_cli_success(update);
    assert_eq!(
        curl_tree_file(&registry, &["nested", "dir", "from-file.bin"]),
        replacement_bytes
    );

    let missing_update = artifact_cli(&registry, &config)
        .args(["update", "/packages/missing.bin", "--input"])
        .arg(&replacement)
        .output()
        .unwrap();
    assert_eq!(missing_update.status.code(), Some(66));

    let root_before_conflict = parse_listing(
        artifact_cli(&registry, &config)
            .args(["list", "/packages/nested/dir"])
            .output()
            .unwrap(),
    )[0]["root_digest"]
        .as_str()
        .unwrap()
        .to_owned();
    let conflict = artifact_cli(&registry, &config)
        .args(["create", file_path, "--input"])
        .arg(&file_input)
        .output()
        .unwrap();
    assert_eq!(conflict.status.code(), Some(73));
    let root_after_conflict = parse_listing(
        artifact_cli(&registry, &config)
            .args(["list", "/packages/nested/dir"])
            .output()
            .unwrap(),
    )[0]["root_digest"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(root_after_conflict, root_before_conflict);

    let empty_directory = "/packages/nested/empty";
    let mkdir = artifact_cli(&registry, &config)
        .args(["create", empty_directory, "--directory"])
        .output()
        .unwrap();
    assert_cli_success(mkdir);
    let empty_listing = artifact_cli(&registry, &config)
        .args(["list", empty_directory])
        .output()
        .unwrap();
    let empty_listing = parse_listing(empty_listing);
    assert_eq!(
        empty_listing.len(),
        1,
        "empty listing emits just its header"
    );
    assert_eq!(empty_listing[0]["type"], "listing");

    let delete_file = artifact_cli(&registry, &config)
        .args(["delete", stdin_path])
        .output()
        .unwrap();
    assert_cli_success(delete_file);
    let deleted_read = artifact_cli(&registry, &config)
        .args(["read", stdin_path])
        .output()
        .unwrap();
    assert_eq!(deleted_read.status.code(), Some(66));

    let nonrecursive = artifact_cli(&registry, &config)
        .args(["delete", "/packages/nested/dir"])
        .output()
        .unwrap();
    assert_eq!(nonrecursive.status.code(), Some(73));
    let delete_directory = artifact_cli(&registry, &config)
        .args(["delete", "/packages/nested", "--recursive"])
        .output()
        .unwrap();
    assert_cli_success(delete_directory);
    let deleted_directory = artifact_cli(&registry, &config)
        .args(["list", "/packages/nested"])
        .output()
        .unwrap();
    assert_eq!(deleted_directory.status.code(), Some(66));

    let root_bytes = registry.curl_api(
        &format!("manifests/{}", registry.mount.root_tag),
        Some(OCI_IMAGE_INDEX),
    );
    let root: serde_json::Value = serde_json::from_slice(&root_bytes).unwrap();
    assert_eq!(root["mediaType"], OCI_IMAGE_INDEX);
}

#[tokio::test]
async fn tree_concurrent_writers_and_pinned_listing_cursor_preserve_snapshots() {
    let registry = TestRegistry::new(false);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.initialize(None).await.unwrap();

    let alpha = StoragePath::parse_relative("alpha/one.bin").unwrap();
    let beta = StoragePath::parse_relative("beta/two.bin").unwrap();
    let (alpha_result, beta_result) = tokio::join!(
        tree.write_file(&alpha, Bytes::from_static(b"alpha contents"), false, None,),
        tree.write_file(&beta, Bytes::from_static(b"beta contents"), false, None,),
    );
    alpha_result.unwrap();
    beta_result.unwrap();

    let first = tree
        .list_page(&StoragePath::root(), 1, None, None)
        .await
        .unwrap();
    assert_eq!(first.entries[0].name, "alpha");
    let continuation = first
        .next_cursor
        .expect("a second root entry must paginate");

    tree.write_file(
        &StoragePath::parse_relative("charlie/three.bin").unwrap(),
        Bytes::from_static(b"charlie contents"),
        false,
        None,
    )
    .await
    .unwrap();
    let pinned = tree
        .list_page(&StoragePath::root(), 1, Some(&continuation), None)
        .await
        .unwrap();
    assert_eq!(pinned.root_digest, first.root_digest);
    assert_eq!(pinned.entries.len(), 1);
    assert_eq!(pinned.entries[0].name, "beta");
    assert!(pinned.next_cursor.is_none());

    let current = tree
        .list_page(&StoragePath::root(), 10, None, None)
        .await
        .unwrap();
    assert_eq!(
        current
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta", "charlie"]
    );
    for (path, expected) in [
        (&alpha, b"alpha contents".as_slice()),
        (&beta, b"beta contents"),
    ] {
        let file = tree.read_file(path, None).await.unwrap();
        let chunks: Vec<Bytes> = file.body.try_collect().await.unwrap();
        assert_eq!(chunks.concat(), expected);
    }
}

#[tokio::test]
async fn tree_page_overflow_builds_ordered_bounded_branch_pages() {
    let registry = TestRegistry::new(false);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.initialize(None).await.unwrap();
    let directory = StoragePath::parse_relative("items").unwrap();
    for index in 0..129 {
        let path = directory.child(format!("{index:03}.txt")).unwrap();
        tree.write_file(&path, Bytes::from(format!("content {index}")), false, None)
            .await
            .unwrap();
    }

    let mut names = Vec::new();
    let mut continuation = None;
    loop {
        let page = tree
            .list_page(&directory, 17, continuation.as_deref(), None)
            .await
            .unwrap();
        names.extend(page.entries.into_iter().map(|entry| entry.name));
        continuation = page.next_cursor;
        if continuation.is_none() {
            break;
        }
    }
    let expected = (0..129)
        .map(|index| format!("{index:03}.txt"))
        .collect::<Vec<_>>();
    assert_eq!(names, expected);

    const ENTRY_NAME: &str = "io.github.djha-skin.oci-auto-http.entry-name";
    const NODE_KIND: &str = "io.github.djha-skin.oci-auto-http.node-kind";
    const FIRST_NAME: &str = "io.github.djha-skin.oci-auto-http.first-name";
    const LAST_NAME: &str = "io.github.djha-skin.oci-auto-http.last-name";
    let root_bytes = registry.curl_api(
        &format!("manifests/{}", registry.mount.root_tag),
        Some(OCI_IMAGE_INDEX),
    );
    let root: serde_json::Value = serde_json::from_slice(&root_bytes).unwrap();
    let directory_ref = descriptor_named(&root, "items", ENTRY_NAME);
    let page_bytes = registry.curl_api(
        &format!("manifests/{}", directory_ref["digest"].as_str().unwrap()),
        Some(OCI_IMAGE_INDEX),
    );
    let branch: serde_json::Value = serde_json::from_slice(&page_bytes).unwrap();
    assert_eq!(branch["annotations"][NODE_KIND], "directory-branch");
    let children = branch["manifests"].as_array().unwrap();
    assert_eq!(children.len(), 2);
    assert!(
        children
            .iter()
            .all(|child| child["mediaType"] == OCI_IMAGE_INDEX)
    );
    assert!(children.iter().all(|child| {
        child["annotations"][FIRST_NAME].as_str().is_some()
            && child["annotations"][LAST_NAME].as_str().is_some()
    }));
    assert!(
        children[0]["annotations"][LAST_NAME].as_str().unwrap()
            < children[1]["annotations"][FIRST_NAME].as_str().unwrap()
    );

    tree.remove(&directory, true, None).await.unwrap();
    assert!(matches!(
        tree.list_page(&directory, 17, None, None).await,
        Err(TreeError::NotFound)
    ));
}

#[tokio::test]
async fn tree_failed_content_stream_does_not_publish_a_new_root() {
    let registry = TestRegistry::new(false);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    let before = tree.initialize(None).await.unwrap();
    let intended = Bytes::from_static(b"valid first chunk and a missing second chunk");
    let source = futures_util::stream::iter(vec![
        Ok::<_, io::Error>(Bytes::from_static(b"valid first chunk and ")),
        Err(io::Error::other("simulated local input interruption")),
    ]);
    let error = tree
        .write_file_stream(
            &StoragePath::parse_relative("interrupted.bin").unwrap(),
            Digest::from_bytes(&intended),
            intended.len() as u64,
            source,
            false,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, TreeError::InputStream));

    let client = registry.client(ClientPolicy::default());
    let after = client
        .resolve_tag(&registry.mount.root_tag, &[OCI_IMAGE_INDEX], None)
        .await
        .unwrap();
    assert_eq!(after.digest, before);
    assert!(matches!(
        tree.stat_file(
            &StoragePath::parse_relative("interrupted.bin").unwrap(),
            None
        )
        .await,
        Err(TreeError::NotFound)
    ));
}

#[tokio::test]
async fn tree_root_tag_write_retries_after_a_real_precommit_connection_drop() {
    run_root_tag_fault(CommitFault::BeforeCommit).await;
}

#[tokio::test]
async fn tree_root_tag_write_confirms_a_commit_after_losing_the_response() {
    run_root_tag_fault(CommitFault::AfterCommit).await;
}

async fn run_root_tag_fault(mode: CommitFault) {
    let registry = TestRegistry::new(false);
    let initial_tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    let before = initial_tree.initialize(None).await.unwrap();
    drop(initial_tree);

    let proxy = FaultProxy::new(&registry, mode).await;
    let proxy_probe = proxy.registry_client(&registry);
    proxy_probe
        .resolve_tag(&registry.mount.root_tag, &[OCI_IMAGE_INDEX], None)
        .await
        .unwrap_or_else(|error| panic!("proxy root probe failed: {error:?}"));
    let tree = TreeStore::new(proxy.registry_client(&registry)).unwrap();
    let bytes = Bytes::from_static(b"root commit fault injection data");
    let path = StoragePath::parse_relative("commit/fault.bin").unwrap();
    let published = tree
        .write_file(&path, bytes.clone(), false, None)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "tree write failed: {error:?}; attempts={}, forwarded={}, faulted={}",
                proxy.state.root_put_attempts.load(Ordering::SeqCst),
                proxy.state.root_put_forwarded.load(Ordering::SeqCst),
                proxy.state.fault_fired.load(Ordering::SeqCst)
            )
        });
    assert_ne!(published, before);
    assert!(proxy.state.fault_fired.load(Ordering::SeqCst));
    assert_eq!(proxy.state.root_put_forwarded.load(Ordering::SeqCst), 1);
    match mode {
        CommitFault::BeforeCommit => {
            assert_eq!(proxy.state.root_put_attempts.load(Ordering::SeqCst), 2);
        }
        CommitFault::AfterCommit => {
            assert_eq!(proxy.state.root_put_attempts.load(Ordering::SeqCst), 1);
        }
    }

    let direct_client = registry.client(ClientPolicy::default());
    let current = direct_client
        .resolve_tag(&registry.mount.root_tag, &[OCI_IMAGE_INDEX], None)
        .await
        .unwrap();
    assert_eq!(current.digest, published);
    let direct_tree = TreeStore::new(direct_client).unwrap();
    let file = direct_tree.read_file(&path, None).await.unwrap();
    let received: Vec<Bytes> = file.body.try_collect().await.unwrap();
    assert_eq!(received.concat(), bytes);
}

fn descriptor_named<'a>(
    index: &'a serde_json::Value,
    name: &str,
    annotation: &str,
) -> &'a serde_json::Value {
    index["manifests"]
        .as_array()
        .unwrap()
        .iter()
        .find(|descriptor| descriptor["annotations"][annotation] == name)
        .unwrap_or_else(|| panic!("descriptor {name:?} not found in OCI index"))
}

fn cli_binary() -> PathBuf {
    ["CARGO_BIN_EXE_oci-auto-http", "CARGO_BIN_EXE_oci_auto_http"]
        .into_iter()
        .find_map(std::env::var_os)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let suffix = std::env::consts::EXE_SUFFIX;
            std::env::current_exe()
                .expect("test executable path is available")
                .parent()
                .and_then(Path::parent)
                .expect("test executable is under target/<profile>/deps")
                .join(format!("oci-auto-http{suffix}"))
        })
}

fn artifact_cli(registry: &TestRegistry, config: &Path) -> Command {
    let mut command = Command::new(cli_binary());
    command
        .args(["artifact", "--config"])
        .arg(config)
        .args(["--ca-cert"])
        .arg(&registry.certificate);
    command
}

fn assert_cli_success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "CLI command failed: status={:?}, stdout={:?}, stderr={}",
        output.status.code(),
        output.stdout,
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn parse_listing(output: Output) -> Vec<serde_json::Value> {
    assert_cli_success(output)
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

fn curl_tree_file(registry: &TestRegistry, path: &[&str]) -> Vec<u8> {
    const ENTRY_NAME: &str = "io.github.djha-skin.oci-auto-http.entry-name";
    let root_bytes = registry.curl_api(
        &format!("manifests/{}", registry.mount.root_tag),
        Some(OCI_IMAGE_INDEX),
    );
    let mut index: serde_json::Value = serde_json::from_slice(&root_bytes).unwrap();
    for component in &path[..path.len() - 1] {
        let descriptor = descriptor_named(&index, component, ENTRY_NAME);
        assert_eq!(descriptor["mediaType"], OCI_IMAGE_INDEX);
        let bytes = registry.curl_api(
            &format!("manifests/{}", descriptor["digest"].as_str().unwrap()),
            Some(OCI_IMAGE_INDEX),
        );
        index = serde_json::from_slice(&bytes).unwrap();
    }
    let descriptor = descriptor_named(&index, path[path.len() - 1], ENTRY_NAME);
    assert_eq!(descriptor["mediaType"], OCI_IMAGE_MANIFEST);
    let bytes = registry.curl_api(
        &format!("manifests/{}", descriptor["digest"].as_str().unwrap()),
        Some(OCI_IMAGE_MANIFEST),
    );
    let file_manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let layer_digest = file_manifest["layers"][0]["digest"].as_str().unwrap();
    registry.curl_api(&format!("blobs/{layer_digest}"), None)
}

struct TestRegistry {
    container: String,
    _cleanup: ContainerGuard,
    base_url: String,
    certificate: PathBuf,
    mount: Mount,
    _temp: TempDir,
}

impl TestRegistry {
    fn new(authenticated: bool) -> Self {
        assert_command("docker", &["info"], "Docker daemon must be running");
        assert_command(
            "docker",
            &["image", "inspect", REGISTRY_IMAGE],
            "pull registry:2 before running registry-tests",
        );
        assert_command("openssl", &["version"], "OpenSSL is required for local TLS");
        assert_command(
            "curl",
            &["--version"],
            "curl is required for independent API reads",
        );

        let temp = TempDir::new();
        let certificates = temp.path.join("certificates");
        fs::create_dir_all(&certificates).unwrap();
        generate_certificates(&certificates);
        let certificate = certificates.join("ca.crt");

        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let container = format!("oci-auto-http-registry-{}-{sequence}", std::process::id());
        let certificate_volume = format!("{}:/certs:ro,z", certificates.display());
        let mut command = Command::new("docker");
        command.args([
            "run",
            "--detach",
            "--name",
            &container,
            "--publish",
            "127.0.0.1::5000",
            "--volume",
            &certificate_volume,
            "--env",
            "REGISTRY_HTTP_TLS_CERTIFICATE=/certs/localhost.crt",
            "--env",
            "REGISTRY_HTTP_TLS_KEY=/certs/localhost.key",
            "--env",
            "REGISTRY_STORAGE_DELETE_ENABLED=true",
        ]);
        if authenticated {
            let authentication = temp.path.join("authentication");
            fs::create_dir_all(&authentication).unwrap();
            fs::write(
                authentication.join("htpasswd"),
                "test:$2a$10$abcdefghijklmnopqrstuuNYVhuCzN8W/N3q6oBTpBoHaLLh6DgBG\n",
            )
            .unwrap();
            let auth_volume = format!("{}:/auth:ro,z", authentication.display());
            command.args(["--volume", &auth_volume]).args([
                "--env",
                "REGISTRY_AUTH=htpasswd",
                "--env",
                "REGISTRY_AUTH_HTPASSWD_REALM=Test Registry",
                "--env",
                "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd",
            ]);
        }
        command.arg(REGISTRY_IMAGE);
        let output = command
            .output()
            .expect("could not start local registry container");
        assert!(
            output.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let cleanup = ContainerGuard(container.clone());

        let port_output = Command::new("docker")
            .args(["port", &container, "5000/tcp"])
            .output()
            .unwrap();
        assert!(
            port_output.status.success(),
            "docker port failed: {}",
            String::from_utf8_lossy(&port_output.stderr)
        );
        let port = String::from_utf8(port_output.stdout)
            .unwrap()
            .lines()
            .next()
            .and_then(|mapping| mapping.rsplit_once(':'))
            .map(|(_, port)| port.parse::<u16>().unwrap())
            .expect("Docker did not report a mapped registry port");
        let base_url = format!("https://127.0.0.1:{port}");
        wait_until_ready(&container, &base_url, &certificate, authenticated);
        let config = Config::parse(
            Path::new("registry-test.toml"),
            &format!(
                "version = 1\n[[mounts]]\nname = \"test\"\nprefix = \"/test\"\nregistry = \"{base_url}\"\nrepository = \"team/releases\"\n"
            ),
        )
        .unwrap();

        Self {
            container,
            _cleanup: cleanup,
            base_url,
            certificate,
            mount: config.mounts.into_iter().next().unwrap(),
            _temp: temp,
        }
    }

    fn client(&self, policy: ClientPolicy) -> RegistryClient {
        let pem = fs::read(&self.certificate).unwrap();
        RegistryClient::new(self.mount.clone(), policy.with_root_certificate_pem(&pem)).unwrap()
    }

    fn curl_api(&self, path: &str, accept: Option<&str>) -> Vec<u8> {
        let url = format!("{}/v2/team/releases/{path}", self.base_url);
        let mut command = Command::new("curl");
        command.args([
            "--silent",
            "--show-error",
            "--fail",
            "--cacert",
            self.certificate.to_str().unwrap(),
            "--user",
            "test:test",
        ]);
        if let Some(accept) = accept {
            command.args(["--header", &format!("Accept: {accept}")]);
        }
        let output = command.arg(url).output().unwrap();
        assert!(
            output.status.success(),
            "independent curl read failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }
}

#[derive(Clone, Copy)]
enum CommitFault {
    BeforeCommit,
    AfterCommit,
}

struct FaultState {
    mode: CommitFault,
    root_path: String,
    fault_fired: AtomicBool,
    root_put_attempts: AtomicUsize,
    root_put_forwarded: AtomicUsize,
}

struct FaultProxy {
    base_url: String,
    state: Arc<FaultState>,
    task: JoinHandle<()>,
}

impl FaultProxy {
    async fn new(registry: &TestRegistry, mode: CommitFault) -> Self {
        let certificates = registry.certificate.parent().unwrap();
        let certificate_der = certificates.join("localhost.der");
        let key_der = certificates.join("localhost-key.der");
        let certificate_output = Command::new("openssl")
            .args([
                "x509",
                "-in",
                certificates.join("localhost.crt").to_str().unwrap(),
                "-outform",
                "DER",
                "-out",
                certificate_der.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(certificate_output.status.success());
        let key_output = Command::new("openssl")
            .args([
                "pkcs8",
                "-topk8",
                "-nocrypt",
                "-in",
                certificates.join("localhost.key").to_str().unwrap(),
                "-outform",
                "DER",
                "-out",
                key_der.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(key_output.status.success());

        let certificate = CertificateDer::from(fs::read(certificate_der).unwrap());
        let key = PrivateKeyDer::try_from(fs::read(key_der).unwrap()).unwrap();
        let tls_config = tokio_rustls::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate], key)
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(tls_config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let base_url = format!("https://{address}");
        let ca = reqwest::Certificate::from_pem(&fs::read(&registry.certificate).unwrap()).unwrap();
        let upstream_client = reqwest::Client::builder()
            .add_root_certificate(ca)
            .build()
            .unwrap();
        let state = Arc::new(FaultState {
            mode,
            root_path: format!(
                "/v2/{}/manifests/{}",
                registry.mount.repository, registry.mount.root_tag
            ),
            fault_fired: AtomicBool::new(false),
            root_put_attempts: AtomicUsize::new(0),
            root_put_forwarded: AtomicUsize::new(0),
        });
        let task = {
            let upstream = registry.base_url.clone();
            let proxy_origin = base_url.clone();
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let acceptor = acceptor.clone();
                    let upstream = upstream.clone();
                    let proxy_origin = proxy_origin.clone();
                    let client = upstream_client.clone();
                    let state = Arc::clone(&state);
                    tokio::spawn(async move {
                        let Ok(stream) = acceptor.accept(stream).await else {
                            return;
                        };
                        let service = service_fn(move |request| {
                            let upstream = upstream.clone();
                            let proxy_origin = proxy_origin.clone();
                            let client = client.clone();
                            let state = Arc::clone(&state);
                            async move {
                                proxy_registry_request(
                                    request,
                                    &upstream,
                                    &proxy_origin,
                                    &client,
                                    &state,
                                )
                                .await
                            }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    });
                }
            })
        };
        Self {
            base_url,
            state,
            task,
        }
    }

    fn registry_client(&self, registry: &TestRegistry) -> RegistryClient {
        let mut mount: Mount = registry.mount.clone();
        mount.registry = Url::parse(&self.base_url).unwrap();
        let ca = fs::read(&registry.certificate).unwrap();
        RegistryClient::new(
            mount,
            ClientPolicy::default().with_root_certificate_pem(&ca),
        )
        .unwrap()
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn proxy_registry_request(
    request: Request<Incoming>,
    upstream: &str,
    proxy_origin: &str,
    client: &reqwest::Client,
    state: &FaultState,
) -> Result<Response<Full<Bytes>>, io::Error> {
    let is_root_put =
        request.method() == reqwest::Method::PUT && request.uri().path() == state.root_path;
    let mut inject_fault = false;
    if is_root_put {
        state.root_put_attempts.fetch_add(1, Ordering::SeqCst);
        inject_fault = !state.fault_fired.swap(true, Ordering::SeqCst);
        if inject_fault && matches!(state.mode, CommitFault::BeforeCommit) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "injected connection drop before root-tag commit",
            ));
        }
    }

    let (parts, body) = request.into_parts();
    let body = body
        .collect()
        .await
        .map_err(|error| io::Error::other(error.to_string()))?
        .to_bytes();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| "/".to_owned(), |value| value.as_str().to_owned());
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in &parts.headers {
        if !matches!(
            name.as_str(),
            "host" | "connection" | "content-length" | "transfer-encoding"
        ) {
            headers.append(name.clone(), value.clone());
        }
    }
    let response = client
        .request(
            parts.method,
            format!("{}{path_and_query}", upstream.trim_end_matches('/')),
        )
        .headers(headers)
        .body(body)
        .send()
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
    if is_root_put {
        state.root_put_forwarded.fetch_add(1, Ordering::SeqCst);
    }
    let status = response.status();
    let response_headers = response.headers().clone();
    let response_body = response
        .bytes()
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;

    if inject_fault && matches!(state.mode, CommitFault::AfterCommit) && status.is_success() {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "injected connection drop after root-tag commit",
        ));
    }

    let mut output = Response::builder().status(status);
    for (name, value) in &response_headers {
        if matches!(name.as_str(), "connection" | "transfer-encoding") {
            continue;
        }
        if name == reqwest::header::LOCATION
            && let Ok(location) = value.to_str()
            && let Ok(location_url) = Url::parse(location)
        {
            let mut local_location = format!("{proxy_origin}{}", location_url.path());
            if let Some(query) = location_url.query() {
                local_location.push('?');
                local_location.push_str(query);
            }
            output = output.header(name, local_location);
            continue;
        }
        output = output.header(name, value);
    }
    output
        .body(Full::new(response_body))
        .map_err(|error| io::Error::other(error.to_string()))
}

struct ContainerGuard(String);

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.0])
            .output();
    }
}

static TEMP_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "oci-auto-http-registry-test-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn generate_certificates(directory: &Path) {
    let ca_key = directory.join("ca.key");
    let ca_cert = directory.join("ca.crt");
    let server_key = directory.join("localhost.key");
    let server_csr = directory.join("localhost.csr");
    let server_cert = directory.join("localhost.crt");
    let extension_file = directory.join("server.ext");
    let output = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            ca_key.to_str().unwrap(),
            "-out",
            ca_cert.to_str().unwrap(),
            "-days",
            "3",
            "-sha256",
            "-subj",
            "/CN=oci-auto-http registry test CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "could not generate test CA: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = Command::new("openssl")
        .args([
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            server_key.to_str().unwrap(),
            "-out",
            server_csr.to_str().unwrap(),
            "-subj",
            "/CN=127.0.0.1",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "could not generate registry key: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::write(
        &extension_file,
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1\n",
    )
    .unwrap();
    let output = Command::new("openssl")
        .args([
            "x509",
            "-req",
            "-in",
            server_csr.to_str().unwrap(),
            "-CA",
            ca_cert.to_str().unwrap(),
            "-CAkey",
            ca_key.to_str().unwrap(),
            "-CAcreateserial",
            "-out",
            server_cert.to_str().unwrap(),
            "-days",
            "3",
            "-sha256",
            "-extfile",
            extension_file.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "could not sign registry certificate: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    for key in [&ca_key, &server_key] {
        let mut permissions = fs::metadata(key).unwrap().permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(key, permissions).unwrap();
    }
}

fn wait_until_ready(container: &str, base_url: &str, certificate: &Path, authenticated: bool) {
    let mut last_error = String::new();
    for _ in 0..80 {
        let mut command = Command::new("curl");
        command.args([
            "--silent",
            "--show-error",
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code}",
            "--cacert",
            certificate.to_str().unwrap(),
        ]);
        if authenticated {
            command.args(["--user", "test:test"]);
        }
        let output = command.arg(format!("{base_url}/v2/")).output().unwrap();
        if output.status.success() && output.stdout == b"200" {
            return;
        }
        last_error = String::from_utf8_lossy(&output.stderr).into_owned();
        thread::sleep(Duration::from_millis(100));
    }
    let logs = Command::new("docker")
        .args(["logs", "--tail", "40", container])
        .output()
        .unwrap();
    panic!(
        "local registry did not become ready: {last_error}\n{}",
        String::from_utf8_lossy(&logs.stderr)
    );
}

fn assert_command(program: &str, args: &[&str], prerequisite: &str) {
    let output: Output = Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|_| panic!("{prerequisite}"));
    assert!(output.status.success(), "{prerequisite}");
}
