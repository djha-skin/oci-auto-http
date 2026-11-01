#![cfg(unix)]

use std::{
    collections::HashMap,
    fs,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener as StdTcpListener, TcpStream},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::body::Body as AxumBody;
use futures_util::{StreamExt, TryStreamExt};
use http_body_util::BodyExt;
use hyper::{Request, Response, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use oci_auto_http::{
    config::{Config, Mount},
    registry::{
        Bytes, ClientPolicy, Digest, ErrorKind, HeaderValue, ManifestReference, OCI_EMPTY_CONFIG,
        OCI_IMAGE_INDEX, OCI_IMAGE_MANIFEST, RegistryClient,
    },
    tree::{DirectoryEntry, EntryKind, StoragePath, TreeError, TreeStore},
};
#[cfg(target_os = "linux")]
use sha2::{Digest as _, Sha256};
#[cfg(target_os = "linux")]
use std::io::{BufRead, BufReader};
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
async fn mount_artifact_and_serve_cli_complete_a_real_registry_demo_flow() {
    let registry = TestRegistry::new(false);
    let scratch = TempDir::new();
    let config_path = scratch.path.join("demo.toml");
    let requests_before_mount = registry.request_count("team/releases");
    let mount = Command::new(cli_binary())
        .args(["mount", "--config"])
        .arg(&config_path)
        .args([
            "create",
            "demo",
            "--prefix",
            "/demo",
            "--registry",
            &registry.base_url,
            "--repository",
            "team/releases",
        ])
        .output()
        .unwrap();
    let mount = assert_cli_success(mount);
    let mount_json: serde_json::Value = serde_json::from_slice(&mount.stdout).unwrap();
    assert_eq!(mount_json["name"], "demo");
    assert_eq!(mount_json["registry"], registry.base_url);
    assert_eq!(
        registry.request_count("team/releases"),
        requests_before_mount,
        "mount CRUD must not contact the registry"
    );

    let input_path = scratch.path.join("payload.bin");
    let expected = b"published by the CLI\0opaque=\xff";
    fs::write(&input_path, expected).unwrap();
    let create = artifact_cli(&registry, &config_path)
        .args(["create", "/demo/releases/v1.bin", "--input"])
        .arg(&input_path)
        .output()
        .unwrap();
    let create = assert_cli_success(create);
    let created: serde_json::Value = serde_json::from_slice(&create.stdout).unwrap();
    assert_eq!(created["path"], "/demo/releases/v1.bin");
    assert_eq!(
        curl_tree_file(&registry, &["releases", "v1.bin"]),
        expected,
        "independent registry reads must observe the CLI-published bytes"
    );

    let read = artifact_cli(&registry, &config_path)
        .args(["read", "/demo/releases/v1.bin"])
        .output()
        .unwrap();
    assert_eq!(assert_cli_success(read).stdout, expected);
    let listing = parse_listing(
        artifact_cli(&registry, &config_path)
            .args(["list", "/demo/releases"])
            .output()
            .unwrap(),
    );
    assert_eq!(listing[1]["name"], "v1.bin");

    let address = unused_http_address();
    let mut server = spawn_http_server(&config_path, &registry.certificate, address);
    wait_for_http_server(&mut server.child, address);
    let response = http_request(address, "GET", "/demo/releases/v1.bin", &[]).unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body, expected);
    let head = http_request(address, "HEAD", "/demo/releases/v1.bin", &[]).unwrap();
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty());
    assert_eq!(
        head.headers.get("content-length").unwrap(),
        &expected.len().to_string()
    );
    let directory = http_request(address, "GET", "/demo/releases/", &[]).unwrap();
    assert_eq!(directory.status, 200);
    assert!(
        String::from_utf8_lossy(&directory.body)
            .contains("href=\"/demo/releases/v1.bin\">v1.bin</a>")
    );

    let delete = artifact_cli(&registry, &config_path)
        .args(["delete", "/demo/releases/v1.bin"])
        .output()
        .unwrap();
    assert_cli_success(delete);
    let deleted_http = http_request(address, "GET", "/demo/releases/v1.bin", &[]).unwrap();
    assert_eq!(deleted_http.status, 404);
    let missing_cli = artifact_cli(&registry, &config_path)
        .args(["read", "/demo/releases/v1.bin"])
        .output()
        .unwrap();
    assert_eq!(missing_cli.status.code(), Some(66));
    stop_http_server(&mut server);

    let local_files = fs::read_dir(&scratch.path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        local_files,
        [".demo.toml.oci-auto-http.lock", "demo.toml", "payload.bin"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
        "the application must not create a local durable content catalog"
    );
}

#[tokio::test]
async fn serve_routes_real_requests_to_the_selected_mount_only() {
    let registry = TestRegistry::new(false);
    let scratch = TempDir::new();
    let config_path = scratch.path.join("serve.toml");
    let config_text = format!(
        r#"version = 1

[[mounts]]
name = "packages"
prefix = "/packages"
registry = "{}"
repository = "team/releases"

[[mounts]]
name = "mirror"
prefix = "/mirror"
registry = "{}"
repository = "team/mirror"

[[mounts]]
name = "empty"
prefix = "/empty"
registry = "{}"
repository = "team/empty"
"#,
        registry.base_url, registry.base_url, registry.base_url
    );
    fs::write(&config_path, &config_text).unwrap();
    let config = Config::parse(Path::new("serve-test.toml"), &config_text).unwrap();
    let certificate = fs::read(&registry.certificate).unwrap();
    let policy = ClientPolicy::default().with_root_certificate_pem(&certificate);

    let package_canary = b"packages repository canary\0\xff";
    let mirror_canary = b"mirror repository canary\0\xfe";
    let package_only = b"packages-only file";
    let mirror_only = b"mirror-only file";
    let encoded_name_bytes = b"encoded path canary";
    for (mount_name, path, contents) in [
        ("packages", "nested/canary.bin", package_canary.as_slice()),
        (
            "packages",
            "nested/packages-only.bin",
            package_only.as_slice(),
        ),
        (
            "packages",
            "nested/literal%2F+space ☃.bin",
            encoded_name_bytes.as_slice(),
        ),
        ("mirror", "nested/canary.bin", mirror_canary.as_slice()),
        ("mirror", "nested/mirror-only.bin", mirror_only.as_slice()),
    ] {
        let mount = config
            .mounts
            .iter()
            .find(|mount| mount.name == mount_name)
            .unwrap();
        let client = RegistryClient::new(mount.clone(), policy.clone()).unwrap();
        let tree = TreeStore::new(client).unwrap();
        tree.write_file(
            &StoragePath::parse_relative(path).unwrap(),
            Bytes::copy_from_slice(contents),
            false,
            None,
        )
        .await
        .unwrap();
    }

    let address = unused_http_address();
    let mut server = spawn_http_server(&config_path, &registry.certificate, address);
    wait_for_http_server(&mut server.child, address);

    let package_before = registry.request_count("team/releases");
    let mirror_before = registry.request_count("team/mirror");
    let package_response =
        http_request(address, "GET", "/packages/nested/canary.bin", &[]).unwrap();
    assert_eq!(package_response.status, 200);
    assert_eq!(package_response.body, package_canary);
    assert_eq!(
        package_response.headers.get("content-length").unwrap(),
        &package_canary.len().to_string()
    );
    wait_for_registry_requests(&registry, "team/releases", package_before + 1);
    assert_eq!(registry.request_count("team/mirror"), mirror_before);

    let head = http_request(address, "HEAD", "/packages/nested/canary.bin", &[]).unwrap();
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty());
    assert_eq!(
        head.headers.get("content-length").unwrap(),
        &package_canary.len().to_string()
    );

    let mirror_before = registry.request_count("team/mirror");
    let package_before = registry.request_count("team/releases");
    let mirror_response = http_request(address, "GET", "/mirror/nested/canary.bin", &[]).unwrap();
    assert_eq!(mirror_response.status, 200);
    assert_eq!(mirror_response.body, mirror_canary);
    wait_for_registry_requests(&registry, "team/mirror", mirror_before + 1);
    assert_eq!(registry.request_count("team/releases"), package_before);

    let package_before_missing = registry.request_count("team/releases");
    let mirror_before_missing = registry.request_count("team/mirror");
    let package_only_path =
        http_request(address, "GET", "/packages/nested/mirror-only.bin", &[]).unwrap();
    assert_eq!(package_only_path.status, 404);
    wait_for_registry_requests(&registry, "team/releases", package_before_missing + 1);
    assert_eq!(registry.request_count("team/mirror"), mirror_before_missing);

    let package_before_mirror_missing = registry.request_count("team/releases");
    let mirror_before_mirror_missing = registry.request_count("team/mirror");
    let mirror_only_path =
        http_request(address, "GET", "/mirror/nested/packages-only.bin", &[]).unwrap();
    assert_eq!(mirror_only_path.status, 404);
    wait_for_registry_requests(&registry, "team/mirror", mirror_before_mirror_missing + 1);
    assert_eq!(
        registry.request_count("team/releases"),
        package_before_mirror_missing
    );

    let encoded = http_request(
        address,
        "GET",
        "/packages/nested/literal%252F+space%20%E2%98%83.bin",
        &[],
    )
    .unwrap();
    assert_eq!(encoded.status, 200);
    assert_eq!(encoded.body, encoded_name_bytes);

    let directory_redirect =
        http_request(address, "GET", "/packages/nested?do-not-copy=private", &[]).unwrap();
    assert_eq!(directory_redirect.status, 301);
    assert_eq!(
        directory_redirect.headers.get("location").unwrap(),
        "/packages/nested/"
    );
    let directory_page = http_request(address, "GET", "/packages/nested/", &[]).unwrap();
    assert_eq!(directory_page.status, 200);
    assert_eq!(
        directory_page.headers.get("content-type").unwrap(),
        "text/html; charset=utf-8"
    );
    let html = String::from_utf8_lossy(&directory_page.body);
    assert!(html.contains("Index of /packages/nested/"));
    assert!(html.contains("href=\"/packages/\">../</a>"));
    assert!(html.contains("href=\"/packages/nested/canary.bin\">canary.bin</a>"));
    assert!(!html.contains("<script"));
    let file_with_slash =
        http_request(address, "GET", "/packages/nested/canary.bin/", &[]).unwrap();
    assert_eq!(file_with_slash.status, 404);

    let package_before_invalid = registry.request_count("team/releases");
    let mirror_before_invalid = registry.request_count("team/mirror");
    for path in [
        "/packages/nested/a%2Fb",
        "/packages/%2e%2e/secret",
        "/packages/%FF",
        "/packages/nested//file",
        "/packages/nested/%5Cfile",
        "/packages/%",
    ] {
        let response = http_request(address, "GET", path, &[]).unwrap();
        assert_eq!(response.status, 400, "path {path}: {response:?}");
    }
    for (method, path) in [
        ("GET", "/packages-old/nested/canary.bin"),
        ("GET", "/unmounted/file"),
    ] {
        let response = http_request(address, method, path, &[]).unwrap();
        assert_eq!(response.status, 404, "{method} {path}: {response:?}");
    }
    let unsupported = http_request(address, "POST", "/packages/nested/canary.bin", &[]).unwrap();
    assert_eq!(unsupported.status, 405);
    assert_eq!(unsupported.headers.get("allow").unwrap(), "GET, HEAD");

    let unauthorized = http_request(
        address,
        "GET",
        "/packages/nested/canary.bin",
        &[("Authorization", "Bearer do-not-forward-this")],
    )
    .unwrap();
    assert_eq!(unauthorized.status, 403);
    assert!(!String::from_utf8_lossy(&unauthorized.body).contains("do-not-forward-this"));
    let duplicate_authorization = http_request(
        address,
        "GET",
        "/packages/nested/canary.bin",
        &[
            ("Authorization", "Bearer first-secret"),
            ("Authorization", "Bearer second-secret"),
        ],
    )
    .unwrap();
    assert_eq!(duplicate_authorization.status, 400);
    assert_eq!(
        registry.request_count("team/releases"),
        package_before_invalid
    );
    assert_eq!(registry.request_count("team/mirror"), mirror_before_invalid);

    let empty_before = registry.request_count("team/empty");
    let empty_root = http_request(address, "GET", "/empty/missing", &[]).unwrap();
    assert_eq!(empty_root.status, 503);
    wait_for_registry_requests(&registry, "team/empty", empty_before + 1);
    let empty_mount = config
        .mounts
        .iter()
        .find(|mount| mount.name == "empty")
        .unwrap();
    let empty_client = RegistryClient::new(empty_mount.clone(), policy).unwrap();
    assert_eq!(
        empty_client
            .resolve_tag(&empty_mount.root_tag, &[OCI_IMAGE_INDEX], None)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound,
        "GET on an absent root must not initialize the tree"
    );

    let stderr = stop_http_server(&mut server);
    assert!(!stderr.contains("do-not-forward-this"));
    assert!(!stderr.contains("first-secret"));
    assert!(!stderr.contains("second-secret"));
}

#[tokio::test]
async fn serve_relays_credentials_and_challenges_only_over_tls_to_the_selected_registry() {
    let primary = TestRegistry::new(true);
    let secondary = TestRegistry::new(true);
    let scratch = TempDir::new();
    let config_path = scratch.path.join("authenticated.toml");
    let primary_mount = Command::new(cli_binary())
        .args(["mount", "--config"])
        .arg(&config_path)
        .args([
            "create",
            "primary",
            "--prefix",
            "/primary",
            "--registry",
            &primary.base_url,
            "--repository",
            "team/releases",
        ])
        .output()
        .unwrap();
    assert_cli_success(primary_mount);
    let secondary_mount = Command::new(cli_binary())
        .args(["mount", "--config"])
        .arg(&config_path)
        .args([
            "create",
            "secondary",
            "--prefix",
            "/secondary",
            "--registry",
            &secondary.base_url,
            "--repository",
            "team/releases",
        ])
        .output()
        .unwrap();
    assert_cli_success(secondary_mount);

    let authorization = HeaderValue::from_static(BASIC_AUTH);
    for (registry, contents) in [
        (&primary, b"primary private bytes".as_slice()),
        (&secondary, b"secondary private bytes".as_slice()),
    ] {
        let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
        tree.write_file(
            &StoragePath::parse_relative("private.txt").unwrap(),
            Bytes::copy_from_slice(contents),
            false,
            Some(&authorization),
        )
        .await
        .unwrap();
    }

    let registry_ca_bundle = scratch.path.join("registry-ca-bundle.pem");
    let mut registry_ca_bytes = fs::read(&primary.certificate).unwrap();
    registry_ca_bytes.extend_from_slice(&fs::read(&secondary.certificate).unwrap());
    fs::write(&registry_ca_bundle, registry_ca_bytes).unwrap();
    let primary_certificate_dir = primary.certificate.parent().unwrap();
    let listener_certificate = primary_certificate_dir.join("localhost.crt");
    let listener_key = primary_certificate_dir.join("localhost.key");
    let tls_client = https_test_client(&primary.certificate);

    let tls_address = unused_http_address();
    let mut tls_server = spawn_https_http_server(
        &config_path,
        &registry_ca_bundle,
        &listener_certificate,
        &listener_key,
        tls_address,
    );
    wait_for_https_http_server(&mut tls_server.child, tls_address, &tls_client).await;

    let primary_url = format!("https://{tls_address}/primary/private.txt");
    let secondary_url = format!("https://{tls_address}/secondary/private.txt");
    let primary_before = primary.request_count("team/releases");
    let secondary_before = secondary.request_count("team/releases");

    let unauthenticated = tls_client.get(&primary_url).send().await.unwrap();
    assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);
    let challenges: Vec<_> = unauthenticated
        .headers()
        .get_all(reqwest::header::WWW_AUTHENTICATE)
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect();
    assert_eq!(challenges.len(), 1);
    assert!(challenges[0].starts_with("Basic realm="));
    assert_eq!(
        unauthenticated.bytes().await.unwrap().as_ref(),
        b"unauthorized\n"
    );
    wait_for_registry_requests(&primary, "team/releases", primary_before + 1);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(secondary.request_count("team/releases"), secondary_before);

    let wrong_credential = tls_client
        .get(&primary_url)
        .header(reqwest::header::AUTHORIZATION, "Basic d3Jvbmc6cGFzcw==")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_credential.status(), reqwest::StatusCode::UNAUTHORIZED);
    let rejected_challenges: Vec<_> = wrong_credential
        .headers()
        .get_all(reqwest::header::WWW_AUTHENTICATE)
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect();
    assert_eq!(rejected_challenges, challenges);
    assert_eq!(
        wrong_credential.bytes().await.unwrap().as_ref(),
        b"unauthorized\n"
    );
    wait_for_registry_requests(&primary, "team/releases", primary_before + 2);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(secondary.request_count("team/releases"), secondary_before);

    let accepted = tls_client
        .get(&primary_url)
        .header(reqwest::header::AUTHORIZATION, BASIC_AUTH)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), reqwest::StatusCode::OK);
    assert_eq!(
        accepted.bytes().await.unwrap().as_ref(),
        b"primary private bytes"
    );
    wait_for_registry_requests(&primary, "team/releases", primary_before + 3);
    let primary_after = primary.request_count("team/releases");
    assert!(primary_after > primary_before);
    assert_eq!(secondary.request_count("team/releases"), secondary_before);

    let accepted_secondary = tls_client
        .get(&secondary_url)
        .header(reqwest::header::AUTHORIZATION, BASIC_AUTH)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted_secondary.status(), reqwest::StatusCode::OK);
    assert_eq!(
        accepted_secondary.bytes().await.unwrap().as_ref(),
        b"secondary private bytes"
    );
    wait_for_registry_requests(&secondary, "team/releases", secondary_before + 1);
    let secondary_after = secondary.request_count("team/releases");
    assert!(secondary_after > secondary_before);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        primary.request_count("team/releases"),
        primary_after,
        "a request selected for the secondary mount must not reach the primary registry"
    );

    let mut duplicate_authorization = reqwest::header::HeaderMap::new();
    duplicate_authorization.append(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_static(BASIC_AUTH),
    );
    duplicate_authorization.append(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_static("Basic d3Jvbmc6cGFzcw=="),
    );
    let before_duplicate = primary.request_count("team/releases");
    let duplicate = tls_client
        .get(&primary_url)
        .headers(duplicate_authorization)
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(
        !duplicate
            .headers()
            .contains_key(reqwest::header::WWW_AUTHENTICATE)
    );
    let _ = duplicate.bytes().await.unwrap();
    thread::sleep(Duration::from_millis(100));
    assert_eq!(primary.request_count("team/releases"), before_duplicate);

    let outside = tls_client
        .get(format!("https://{tls_address}/unmounted/private.txt"))
        .header(reqwest::header::AUTHORIZATION, BASIC_AUTH)
        .send()
        .await
        .unwrap();
    assert_eq!(outside.status(), reqwest::StatusCode::NOT_FOUND);
    let _ = outside.bytes().await.unwrap();
    thread::sleep(Duration::from_millis(100));
    assert_eq!(primary.request_count("team/releases"), before_duplicate);
    assert_eq!(secondary.request_count("team/releases"), secondary_after);

    let tls_stderr = stop_http_server(&mut tls_server);
    assert!(!tls_stderr.contains(BASIC_AUTH));
    assert!(!tls_stderr.contains("d3Jvbmc6cGFzcw=="));

    let plain_address = unused_http_address();
    let mut plain_server = spawn_http_server(&config_path, &registry_ca_bundle, plain_address);
    wait_for_http_server(&mut plain_server.child, plain_address);
    let primary_before_plain = primary.request_count("team/releases");
    let secondary_before_plain = secondary.request_count("team/releases");
    let cleartext_credential = http_request(
        plain_address,
        "GET",
        "/primary/private.txt",
        &[("Authorization", BASIC_AUTH)],
    )
    .unwrap();
    assert_eq!(cleartext_credential.status, 403);
    assert_eq!(primary.request_count("team/releases"), primary_before_plain);
    assert_eq!(
        secondary.request_count("team/releases"),
        secondary_before_plain
    );

    let cleartext_challenge =
        http_request(plain_address, "GET", "/primary/private.txt", &[]).unwrap();
    assert_eq!(cleartext_challenge.status, 403);
    assert!(!cleartext_challenge.headers.contains_key("www-authenticate"));
    wait_for_registry_requests(&primary, "team/releases", primary_before_plain + 1);
    assert_eq!(
        secondary.request_count("team/releases"),
        secondary_before_plain
    );
    let plain_stderr = stop_http_server(&mut plain_server);
    assert!(!plain_stderr.contains(BASIC_AUTH));
    assert!(!plain_stderr.contains("d3Jvbmc6cGFzcw=="));
}

#[tokio::test]
async fn mount_cli_changes_take_effect_after_restarting_the_server() {
    let registry = TestRegistry::new(false);
    let scratch = TempDir::new();
    let config_path = scratch.path.join("serve-restart.toml");
    fs::write(
        &config_path,
        format!(
            r#"version = 1

[[mounts]]
name = "packages"
prefix = "/packages"
registry = "{}"
repository = "team/packages"
"#,
            registry.base_url
        ),
    )
    .unwrap();

    let initial_address = unused_http_address();
    let mut initial_server =
        spawn_http_server(&config_path, &registry.certificate, initial_address);
    wait_for_http_server(&mut initial_server.child, initial_address);

    let created = Command::new(cli_binary())
        .args(["mount", "--config"])
        .arg(&config_path)
        .args([
            "create",
            "mirror",
            "--prefix",
            "/mirror",
            "--registry",
            &registry.base_url,
            "--repository",
            "team/mirror",
        ])
        .output()
        .unwrap();
    assert_cli_success(created);

    let config = Config::load(&config_path).unwrap();
    let mirror = config
        .mounts
        .iter()
        .find(|mount| mount.name == "mirror")
        .unwrap()
        .clone();
    let certificate = fs::read(&registry.certificate).unwrap();
    let client = RegistryClient::new(
        mirror,
        ClientPolicy::default().with_root_certificate_pem(&certificate),
    )
    .unwrap();
    let tree = TreeStore::new(client).unwrap();
    tree.write_file(
        &StoragePath::parse_relative("welcome.txt").unwrap(),
        Bytes::from_static(b"mount changes need a server restart"),
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);

    let mirror_requests = registry.request_count("team/mirror");
    let before_restart = http_request(initial_address, "GET", "/mirror/welcome.txt", &[]).unwrap();
    assert_eq!(before_restart.status, 404);
    assert_eq!(registry.request_count("team/mirror"), mirror_requests);
    stop_http_server(&mut initial_server);

    let restarted_address = unused_http_address();
    let mut restarted_server =
        spawn_http_server(&config_path, &registry.certificate, restarted_address);
    wait_for_http_server(&mut restarted_server.child, restarted_address);
    let after_restart = http_request(restarted_address, "GET", "/mirror/welcome.txt", &[]).unwrap();
    assert_eq!(after_restart.status, 200);
    assert_eq!(after_restart.body, b"mount changes need a server restart");
    stop_http_server(&mut restarted_server);
}

#[tokio::test]
async fn serve_renders_safe_sorted_paginated_autoindexes_for_real_tree_data() {
    let registry = TestRegistry::new(false);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    for path in ["b-dir", "docs", "empty", "items/z-dir", "deep/a/b"] {
        tree.create_directory(&StoragePath::parse_relative(path).unwrap(), None)
            .await
            .unwrap();
    }
    tree.write_file(
        &StoragePath::parse_relative("a-file.txt").unwrap(),
        Bytes::from_static(b"root file"),
        false,
        None,
    )
    .await
    .unwrap();
    tree.write_file(
        &StoragePath::parse_relative("docs/index.html").unwrap(),
        Bytes::from_static(b"this document is not an implicit index"),
        false,
        None,
    )
    .await
    .unwrap();
    tree.write_file(
        &StoragePath::parse_relative("deep/a/b/leaf.txt").unwrap(),
        Bytes::from_static(b"deep file"),
        false,
        None,
    )
    .await
    .unwrap();
    for index in 0..127 {
        tree.write_file(
            &StoragePath::parse_relative(&format!("items/f{index:03}.txt")).unwrap(),
            Bytes::from(format!("item {index}")),
            false,
            None,
        )
        .await
        .unwrap();
    }
    let hostile_name = r#"z<script>&" +☃%2F.txt"#;
    tree.write_file(
        &StoragePath::parse_relative(&format!("items/{hostile_name}")).unwrap(),
        Bytes::from_static(b"hostile name content"),
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);

    let scratch = TempDir::new();
    let config_path = scratch.path.join("autoindex.toml");
    fs::write(
        &config_path,
        format!(
            r#"version = 1

[[mounts]]
name = "files"
prefix = "/files"
registry = "{}"
repository = "team/releases"
"#,
            registry.base_url
        ),
    )
    .unwrap();
    let address = unused_http_address();
    let mut server = spawn_http_server(&config_path, &registry.certificate, address);
    wait_for_http_server(&mut server.child, address);

    let root = http_request(
        address,
        "GET",
        "/files/?private-query=must-not-appear",
        &[("Host", "attacker.example")],
    )
    .unwrap();
    assert_eq!(root.status, 200);
    let root_html = String::from_utf8(root.body).unwrap();
    assert!(root_html.contains("Index of /files/"));
    assert!(!root_html.contains("attacker.example"));
    assert!(!root_html.contains("private-query=must-not-appear"));
    assert!(
        !root_html.contains("href=\"../\""),
        "mount root must not link to a parent"
    );
    let root_positions = ["b-dir/", "deep/", "docs/", "empty/", "items/", "a-file.txt"]
        .map(|name| root_html.find(name).expect("root listing omitted entry"));
    assert!(root_positions.windows(2).all(|pair| pair[0] < pair[1]));

    let items = http_request(address, "GET", "/files/items/", &[]).unwrap();
    assert_eq!(items.status, 200);
    assert_eq!(
        items.headers.get("content-type").unwrap(),
        "text/html; charset=utf-8"
    );
    let items_headers = items.headers.clone();
    let items_content_length = items.headers.get("content-length").cloned();
    assert_eq!(
        items.headers.get("content-security-policy").unwrap(),
        "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    );
    assert_eq!(items.headers.get("referrer-policy").unwrap(), "no-referrer");
    let html = String::from_utf8(items.body).unwrap();
    assert_eq!(
        html.matches("<li><a href=").count(),
        130,
        "parent plus 129 entries"
    );
    assert!(!html.contains("<script>"));
    assert!(html.contains("&lt;script&gt;&amp;&quot; +☃%2F.txt"));
    assert!(html.contains("href=\"/files/items/z%3Cscript%3E%26%22%20%2B%E2%98%83%252F.txt\""));
    let directory_position = html.find("z-dir/</a>").unwrap();
    let first_file_position = html.find("f000.txt</a>").unwrap();
    assert!(
        directory_position < first_file_position,
        "directories precede files"
    );
    let mut previous_position = first_file_position;
    for index in 1..127 {
        let name = format!("f{index:03}.txt</a>");
        let position = html.find(&name).unwrap();
        assert!(
            previous_position < position,
            "file entries are not bytewise sorted"
        );
        previous_position = position;
    }
    let hostile_position = html
        .find("&lt;script&gt;&amp;&quot; +☃%2F.txt</a>")
        .unwrap();
    assert!(previous_position < hostile_position);

    let empty = http_request(address, "GET", "/files/empty/", &[]).unwrap();
    assert_eq!(empty.status, 200);
    let empty_html = String::from_utf8(empty.body).unwrap();
    assert!(empty_html.contains("href=\"/files/\">../</a>"));
    assert_eq!(empty_html.matches("<li><a href=").count(), 1);

    let docs = http_request(address, "GET", "/files/docs/", &[]).unwrap();
    assert_eq!(docs.status, 200);
    let docs_html = String::from_utf8(docs.body).unwrap();
    assert!(docs_html.contains("href=\"/files/docs/index.html\">index.html</a>"));
    assert!(!docs_html.contains("this document is not an implicit index"));

    let deep = http_request(address, "GET", "/files/deep/a/b/", &[]).unwrap();
    assert_eq!(deep.status, 200);
    let deep_html = String::from_utf8(deep.body).unwrap();
    assert!(deep_html.contains("href=\"/files/deep/a/b/leaf.txt\">leaf.txt</a>"));

    let head = http_request(address, "HEAD", "/files/items/", &[]).unwrap();
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty());
    assert_eq!(
        head.headers.get("content-type").unwrap(),
        "text/html; charset=utf-8"
    );
    assert_eq!(
        head.headers.get("content-length").cloned(),
        items_content_length
    );
    for name in [
        "x-content-type-options",
        "cache-control",
        "vary",
        "content-security-policy",
        "referrer-policy",
    ] {
        assert_eq!(
            head.headers.get(name),
            items_headers.get(name),
            "HEAD header {name}"
        );
    }
    let ranged = http_request(address, "GET", "/files/items/", &[("Range", "bytes=0-1")]).unwrap();
    assert_eq!(ranged.status, 200, "Range is ignored for autoindexes");
    assert!(!ranged.headers.contains_key("content-range"));

    let before_accept_reject = registry.request_count("team/releases");
    for accept in ["application/json", "text/html;q=0, */*;q=1"] {
        let response =
            http_request(address, "GET", "/files/items/", &[("Accept", accept)]).unwrap();
        assert_eq!(response.status, 406, "Accept {accept}");
    }
    assert_eq!(
        registry.request_count("team/releases"),
        before_accept_reject
    );

    let stderr = stop_http_server(&mut server);
    assert!(!stderr.contains("<script>"));
}

#[tokio::test]
async fn serve_file_mime_etag_conditionals_and_ranges_match_the_contract() {
    let registry = TestRegistry::new(false);
    let scratch = TempDir::new();
    let config_path = scratch.path.join("static-files.toml");
    let config_text = format!(
        r#"version = 1

[[mounts]]
name = "packages"
prefix = "/files"
registry = "{}"
repository = "team/releases"
"#,
        registry.base_url
    );
    fs::write(&config_path, &config_text).unwrap();
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    let sample = b"0123456789";
    let sample_path = StoragePath::parse_relative("sample.TXT").unwrap();
    tree.write_file(&sample_path, Bytes::from_static(sample), false, None)
        .await
        .unwrap();
    tree.write_file(
        &StoragePath::parse_relative("empty.bin").unwrap(),
        Bytes::new(),
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);

    let address = unused_http_address();
    let mut server = spawn_http_server(&config_path, &registry.certificate, address);
    wait_for_http_server(&mut server.child, address);
    let url_path = "/files/sample.TXT";
    let etag = format!(
        "\"sha256-{}\"",
        Digest::from_bytes(sample)
            .as_str()
            .strip_prefix("sha256:")
            .unwrap()
    );

    let get = http_request(address, "GET", url_path, &[]).unwrap();
    assert_eq!(get.status, 200);
    assert_eq!(get.body, sample);
    assert_eq!(
        get.headers.get("content-type").unwrap(),
        "text/plain; charset=utf-8"
    );
    assert_eq!(get.headers.get("content-length").unwrap(), "10");
    assert_eq!(get.headers.get("etag").unwrap(), &etag);
    assert_eq!(get.headers.get("accept-ranges").unwrap(), "bytes");
    assert_eq!(
        get.headers.get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(
        get.headers.get("cache-control").unwrap(),
        "private, no-store"
    );
    assert_eq!(get.headers.get("vary").unwrap(), "Authorization");
    assert!(!get.headers.contains_key("content-encoding"));

    let head = http_request(address, "HEAD", url_path, &[]).unwrap();
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty());
    assert_eq!(head.headers.get("content-length").unwrap(), "10");
    assert_eq!(
        head.headers.get("content-type").unwrap(),
        "text/plain; charset=utf-8"
    );
    assert_eq!(head.headers.get("etag").unwrap(), &etag);
    let head_not_modified =
        http_request(address, "HEAD", url_path, &[("If-None-Match", &etag)]).unwrap();
    assert_eq!(head_not_modified.status, 304);
    assert!(head_not_modified.body.is_empty());
    let head_precondition_failed = http_request(
        address,
        "HEAD",
        url_path,
        &[("If-Match", "\"not-current\"")],
    )
    .unwrap();
    assert_eq!(head_precondition_failed.status, 412);
    assert!(head_precondition_failed.body.is_empty());

    for (range, expected, content_range) in [
        ("bytes=2-5", &b"2345"[..], "bytes 2-5/10"),
        ("bytes=7-", &b"789"[..], "bytes 7-9/10"),
        ("bytes=-3", &b"789"[..], "bytes 7-9/10"),
    ] {
        let response = http_request(address, "GET", url_path, &[("Range", range)]).unwrap();
        assert_eq!(response.status, 206, "range {range}");
        assert_eq!(response.body, expected, "range {range}");
        assert_eq!(
            response.headers.get("content-range").unwrap(),
            content_range
        );
        assert_eq!(
            response.headers.get("content-length").unwrap(),
            &expected.len().to_string()
        );
        assert_eq!(response.headers.get("etag").unwrap(), &etag);
    }
    let head_range = http_request(address, "HEAD", url_path, &[("Range", "bytes=2-5")]).unwrap();
    assert_eq!(head_range.status, 206);
    assert!(head_range.body.is_empty());
    assert_eq!(
        head_range.headers.get("content-range").unwrap(),
        "bytes 2-5/10"
    );
    assert_eq!(head_range.headers.get("content-length").unwrap(), "4");

    let unsatisfiable = http_request(address, "GET", url_path, &[("Range", "bytes=99-")]).unwrap();
    assert_eq!(unsatisfiable.status, 416);
    assert_eq!(
        unsatisfiable.headers.get("content-range").unwrap(),
        "bytes */10"
    );
    let head_unsatisfiable =
        http_request(address, "HEAD", url_path, &[("Range", "bytes=99-")]).unwrap();
    assert_eq!(head_unsatisfiable.status, 416);
    assert!(head_unsatisfiable.body.is_empty());
    assert_eq!(
        head_unsatisfiable.headers.get("content-range").unwrap(),
        "bytes */10"
    );

    for malformed in [
        "items=0-1",
        "bytes=0-1,4-5",
        "bytes=bad",
        "bytes=5-2",
        "bytes=0 -1",
    ] {
        let response = http_request(address, "GET", url_path, &[("Range", malformed)]).unwrap();
        assert_eq!(response.status, 200, "malformed range {malformed}");
        assert_eq!(response.body, sample);
    }

    let if_match = http_request(address, "GET", url_path, &[("If-Match", &etag)]).unwrap();
    assert_eq!(if_match.status, 200);
    let if_match_list = http_request(
        address,
        "GET",
        url_path,
        &[("If-Match", &format!("\"other\", {etag}"))],
    )
    .unwrap();
    assert_eq!(if_match_list.status, 200);
    let if_match_wildcard = http_request(address, "GET", url_path, &[("If-Match", "*")]).unwrap();
    assert_eq!(if_match_wildcard.status, 200);
    let if_match_wrong =
        http_request(address, "GET", url_path, &[("If-Match", "\"not-current\"")]).unwrap();
    assert_eq!(if_match_wrong.status, 412);
    let if_match_weak = http_request(
        address,
        "GET",
        url_path,
        &[("If-Match", &format!("W/{etag}"))],
    )
    .unwrap();
    assert_eq!(if_match_weak.status, 412);

    for value in [
        etag.as_str(),
        &format!("W/{etag}"),
        &format!("\"other\", W/{etag}"),
        "*",
    ] {
        let response = http_request(address, "GET", url_path, &[("If-None-Match", value)]).unwrap();
        assert_eq!(response.status, 304, "If-None-Match {value}");
        assert!(response.body.is_empty());
        assert_eq!(response.headers.get("etag").unwrap(), &etag);
    }
    let if_none_then_range = http_request(
        address,
        "GET",
        url_path,
        &[("If-None-Match", &etag), ("Range", "bytes=99-")],
    )
    .unwrap();
    assert_eq!(if_none_then_range.status, 304);
    let if_match_before_range = http_request(
        address,
        "GET",
        url_path,
        &[("If-Match", "\"not-current\""), ("Range", "bytes=99-")],
    )
    .unwrap();
    assert_eq!(if_match_before_range.status, 412);

    let matching_if_range = http_request(
        address,
        "GET",
        url_path,
        &[("Range", "bytes=2-5"), ("If-Range", etag.as_str())],
    )
    .unwrap();
    assert_eq!(matching_if_range.status, 206);
    assert_eq!(matching_if_range.body, b"2345");
    for if_range in [
        "\"not-current\"",
        &format!("W/{etag}"),
        "Wed, 21 Oct 2015 07:28:00 GMT",
    ] {
        let response = http_request(
            address,
            "GET",
            url_path,
            &[("Range", "bytes=2-5"), ("If-Range", if_range)],
        )
        .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, sample);
    }

    for (name, value) in [
        ("If-Modified-Since", "Wed, 21 Oct 2099 07:28:00 GMT"),
        ("If-Unmodified-Since", "Wed, 21 Oct 1999 07:28:00 GMT"),
    ] {
        let response = http_request(address, "GET", url_path, &[(name, value)]).unwrap();
        assert_eq!(response.status, 200, "date condition {name}");
        assert_eq!(response.body, sample);
    }

    let empty_get = http_request(address, "GET", "/files/empty.bin", &[]).unwrap();
    assert_eq!(empty_get.status, 200);
    assert!(empty_get.body.is_empty());
    assert_eq!(empty_get.headers.get("content-length").unwrap(), "0");
    let empty_head = http_request(address, "HEAD", "/files/empty.bin", &[]).unwrap();
    assert_eq!(empty_head.status, 200);
    assert!(empty_head.body.is_empty());
    let empty_range = http_request(
        address,
        "GET",
        "/files/empty.bin",
        &[("Range", "bytes=0-0")],
    )
    .unwrap();
    assert_eq!(empty_range.status, 416);

    let stderr = stop_http_server(&mut server);
    assert!(!stderr.contains("registry.example"));
}

#[tokio::test]
async fn serve_withholds_the_final_byte_when_registry_blob_digest_fails() {
    let registry = TestRegistry::new(false);
    let contents = Bytes::from(vec![0x35; 1024 * 1024]);
    let digest = Digest::from_bytes(&contents);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.write_file(
        &StoragePath::parse_relative("corrupt.bin").unwrap(),
        contents.clone(),
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);

    let proxy = FaultProxy::corrupting_blob(&registry, &digest).await;
    let scratch = TempDir::new();
    let config_path = scratch.path.join("corrupt-stream.toml");
    fs::write(
        &config_path,
        format!(
            r#"version = 1

[[mounts]]
name = "files"
prefix = "/files"
registry = "{}"
repository = "team/releases"
"#,
            proxy.base_url
        ),
    )
    .unwrap();
    let address = unused_http_address();
    let mut server = spawn_http_server(&config_path, &registry.certificate, address);
    wait_for_http_server(&mut server.child, address);

    let response = tokio::task::spawn_blocking(move || {
        read_http_response_partial(address, "/files/corrupt.bin")
    })
    .await
    .unwrap()
    .unwrap();
    let status = response.status;
    let headers = response.headers;
    let received = response.body;
    assert_eq!(status, 200);
    assert_eq!(
        headers.get("content-length").unwrap(),
        &contents.len().to_string()
    );
    assert!(proxy.corruption_fired());
    assert!(
        received.len() < contents.len(),
        "corrupt file response satisfied its declared Content-Length"
    );
    let stderr = stop_http_server(&mut server);
    assert!(stderr.contains("OCI file stream failed digest/size verification or transport"));
    assert!(!stderr.contains("555555555"));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn serve_relays_only_valid_retry_after_values_on_rate_limits() {
    let registry = TestRegistry::new(false);
    for (retry_after, expected) in [
        (Some("120"), Some("120")),
        (
            Some("Wed, 21 Oct 2015 07:28:00 GMT"),
            Some("Wed, 21 Oct 2015 07:28:00 GMT"),
        ),
        (Some("not-a-date"), None),
        (None, None),
    ] {
        let proxy = FaultProxy::rate_limited(&registry, retry_after).await;
        let scratch = TempDir::new();
        let config_path = scratch.path.join("rate-limit.toml");
        fs::write(
            &config_path,
            format!(
                r#"version = 1

[[mounts]]
name = "files"
prefix = "/files"
registry = "{}"
repository = "team/releases"
"#,
                proxy.base_url
            ),
        )
        .unwrap();
        let address = unused_http_address();
        let mut server = spawn_http_server(&config_path, &registry.certificate, address);
        wait_for_http_server(&mut server.child, address);

        let response = tokio::task::spawn_blocking(move || {
            http_request(address, "GET", "/files/anything", &[])
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status, 503);
        assert_eq!(response.body, b"service unavailable\n");
        assert_eq!(
            response.headers.get("retry-after").map(String::as_str),
            expected
        );
        assert!(!String::from_utf8_lossy(&response.body).contains("127.0.0.1"));
        stop_http_server(&mut server);
        drop(proxy);
    }
}

#[tokio::test]
async fn serve_bounds_total_and_upstream_registry_request_timeouts() {
    let registry = TestRegistry::new(false);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.write_file(
        &StoragePath::parse_relative("timeout.txt").unwrap(),
        Bytes::from_static(b"timeout fixture"),
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);
    let delayed_path = format!(
        "/v2/{}/manifests/{}",
        registry.mount.repository, registry.mount.root_tag
    );
    let proxy = FaultProxy::delaying_request(&registry, delayed_path, Duration::from_secs(2)).await;
    let scratch = TempDir::new();
    let config_path = scratch.path.join("timeouts.toml");

    for (request_timeout, registry_timeout) in [(1, 10), (10, 1)] {
        fs::write(
            &config_path,
            format!(
                "version = 1\n[server]\nrequest_timeout_seconds = {request_timeout}\nregistry_request_timeout_seconds = {registry_timeout}\n[[mounts]]\nname = \"files\"\nprefix = \"/files\"\nregistry = \"{}\"\nrepository = \"team/releases\"\n",
                proxy.base_url
            ),
        )
        .unwrap();
        let address = unused_http_address();
        let mut server = spawn_http_server(&config_path, &registry.certificate, address);
        wait_for_http_server(&mut server.child, address);

        let response = tokio::task::spawn_blocking(move || {
            http_request(address, "GET", "/files/timeout.txt", &[])
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status, 504);
        assert_eq!(response.body, b"gateway timeout\n");
        assert!(
            proxy.delay_fired(),
            "timeout case request_timeout={request_timeout}, registry_timeout={registry_timeout} reached {} proxy requests without matching the delay path",
            proxy.total_requests()
        );
        stop_http_server(&mut server);
        tokio::time::sleep(Duration::from_millis(2100)).await;
        proxy.state.delay_fired.store(false, Ordering::SeqCst);
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn serve_enforces_idle_timeout_after_stream_headers_are_sent() {
    let registry = TestRegistry::new(false);
    let file_bytes = Bytes::from_static(b"idle timeout fixture");
    let expected_size = file_bytes.len();
    let digest = Digest::from_bytes(&file_bytes);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.write_file(
        &StoragePath::parse_relative("slow.bin").unwrap(),
        file_bytes,
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);

    let proxy = FaultProxy::slowly_tracking_blob(&registry, &digest, Duration::from_secs(2)).await;
    let scratch = TempDir::new();
    let config_path = scratch.path.join("idle-timeout.toml");
    fs::write(
        &config_path,
        format!(
            "version = 1\n[server]\nrequest_timeout_seconds = 10\nregistry_request_timeout_seconds = 10\nstream_idle_timeout_seconds = 1\n[[mounts]]\nname = \"files\"\nprefix = \"/files\"\nregistry = \"{}\"\nrepository = \"team/releases\"\n",
            proxy.base_url
        ),
    )
    .unwrap();
    let address = unused_http_address();
    let mut server = spawn_http_server(&config_path, &registry.certificate, address);
    wait_for_http_server(&mut server.child, address);

    let partial =
        tokio::task::spawn_blocking(move || read_http_response_partial(address, "/files/slow.bin"))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(partial.status, 200);
    assert_eq!(
        partial.headers.get("content-length").unwrap(),
        &expected_size.to_string()
    );
    assert!(
        partial.body.len() < expected_size,
        "an idle timeout after headers must terminate the representation as incomplete"
    );
    assert!(proxy.tracked_blob_requests() >= 1);
    let health = http_request(address, "GET", "/healthz", &[]).unwrap();
    assert_eq!(health.status, 204);
    stop_http_server(&mut server);
}

#[tokio::test]
async fn serve_rejects_autoindexes_that_exceed_the_configured_spool_limit() {
    let registry = TestRegistry::new(false);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.write_file(
        &StoragePath::parse_relative("long-entry-name.txt").unwrap(),
        Bytes::from_static(b"listed"),
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);

    let scratch = TempDir::new();
    let config_path = scratch.path.join("small-spool.toml");
    fs::write(
        &config_path,
        format!(
            "version = 1\n[server]\nmax_autoindex_spool_bytes = 128\nmax_autoindex_spool_total_bytes = 128\n[[mounts]]\nname = \"files\"\nprefix = \"/files\"\nregistry = \"{}\"\nrepository = \"team/releases\"\n",
            registry.base_url
        ),
    )
    .unwrap();
    let address = unused_http_address();
    let mut server = spawn_http_server(&config_path, &registry.certificate, address);
    wait_for_http_server(&mut server.child, address);

    let response = http_request(address, "GET", "/files/", &[]).unwrap();
    assert_eq!(response.status, 503);
    assert_eq!(response.body, b"service unavailable\n");
    assert_eq!(
        response.headers.get("cache-control").unwrap(),
        "private, no-store"
    );
    let stderr = stop_http_server(&mut server);
    assert!(!stderr.contains("registry.example"));
}

#[tokio::test]
async fn serve_maps_a_late_autoindex_page_challenge_before_committing_success_headers() {
    let registry = TestRegistry::new(false);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    for index in 0..129 {
        tree.write_file(
            &StoragePath::parse_relative(&format!("items/entry-{index:03}.txt")).unwrap(),
            Bytes::from_static(b"listing entry"),
            false,
            None,
        )
        .await
        .unwrap();
    }
    drop(tree);

    let root = registry
        .client(ClientPolicy::default())
        .resolve_tag(&registry.mount.root_tag, &[OCI_IMAGE_INDEX], None)
        .await
        .unwrap();
    let proxy = FaultProxy::challenge_after_manifest_gets(&registry, &root.digest, 5).await;
    let scratch = TempDir::new();
    let config_path = scratch.path.join("late-challenge.toml");
    fs::write(
        &config_path,
        format!(
            "version = 1\n[[mounts]]\nname = \"files\"\nprefix = \"/files\"\nregistry = \"{}\"\nrepository = \"team/releases\"\n",
            proxy.base_url
        ),
    )
    .unwrap();

    let certificates = registry.certificate.parent().unwrap();
    let listener_certificate = certificates.join("localhost.crt");
    let listener_key = certificates.join("localhost.key");
    let client = https_test_client(&registry.certificate);
    let address = unused_http_address();
    let mut server = spawn_https_http_server(
        &config_path,
        &registry.certificate,
        &listener_certificate,
        &listener_key,
        address,
    );
    wait_for_https_http_server(&mut server.child, address, &client).await;

    let response = client
        .get(format!("https://{address}/files/items/"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    let challenges: Vec<_> = response
        .headers()
        .get_all(reqwest::header::WWW_AUTHENTICATE)
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        challenges,
        ["Bearer realm=\"https://auth.example/token\",service=\"registry-test\""]
    );
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"unauthorized\n");
    assert!(proxy.auth_challenge_fired());
    assert_eq!(proxy.auth_challenge_matches(), 5);

    let stderr = stop_http_server(&mut server);
    assert!(!stderr.contains("auth.example/token"));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn serve_streams_large_files_with_bounded_rss_and_cancels_after_disconnect() {
    let registry = TestRegistry::new(false);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    let total_size = 64 * 1024 * 1024_u64;
    let chunk = Bytes::from(vec![0x5a; 64 * 1024]);
    let chunk_count = (total_size as usize) / chunk.len();
    let mut content_hasher = Sha256::new();
    for _ in 0..chunk_count {
        content_hasher.update(&chunk);
    }
    let digest_string = format!("sha256:{:x}", content_hasher.finalize());
    let digest = Digest::parse(&digest_string).unwrap();
    let source_chunk = chunk.clone();
    let source = futures_util::stream::iter(
        (0..chunk_count).map(move |_| Ok::<_, io::Error>(source_chunk.clone())),
    );
    tree.write_file_stream(
        &StoragePath::parse_relative("large.bin").unwrap(),
        digest.clone(),
        total_size,
        source,
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);

    let scratch = TempDir::new();
    let direct_config = scratch.path.join("direct.toml");
    fs::write(
        &direct_config,
        format!(
            r#"version = 1

[[mounts]]
name = "files"
prefix = "/files"
registry = "{}"
repository = "team/releases"
"#,
            registry.base_url
        ),
    )
    .unwrap();
    let direct_address = unused_http_address();
    let mut direct_server =
        spawn_http_server(&direct_config, &registry.certificate, direct_address);
    wait_for_http_server(&mut direct_server.child, direct_address);
    let baseline_rss = process_rss_bytes(direct_server.child.id());
    let download =
        stream_http_response(direct_address, "/files/large.bin", direct_server.child.id()).unwrap();
    assert_eq!(download.status, 200);
    assert_eq!(download.bytes_read, total_size);
    assert_eq!(download.digest, digest_string);
    assert_eq!(
        download.headers.get("content-length").unwrap(),
        &total_size.to_string()
    );
    assert!(
        download.peak_rss_bytes.saturating_sub(baseline_rss) < total_size / 2,
        "server RSS grew by {} bytes while serving a {} byte blob",
        download.peak_rss_bytes.saturating_sub(baseline_rss),
        total_size
    );
    stop_http_server(&mut direct_server);

    let proxy = FaultProxy::tracking_blob(&registry, &digest).await;
    let descriptor = registry
        .client(ClientPolicy::default())
        .head_blob(&digest, None)
        .await
        .unwrap();
    let proxy_client = proxy.registry_client(&registry);
    let mut proxy_body = proxy_client.get_blob(&descriptor, None).await.unwrap();
    let first_proxy_chunk = proxy_body.next().await.unwrap().unwrap();
    assert!(!first_proxy_chunk.is_empty());
    drop(proxy_body);
    let proxy_probe_bytes = proxy.tracked_blob_bytes();
    assert!(
        proxy_probe_bytes > 0,
        "streaming proxy did not forward blob bytes"
    );

    let proxied_config = scratch.path.join("proxied.toml");
    fs::write(
        &proxied_config,
        format!(
            r#"version = 1

[[mounts]]
name = "files"
prefix = "/files"
registry = "{}"
repository = "team/releases"
"#,
            proxy.base_url
        ),
    )
    .unwrap();
    let proxied_address = unused_http_address();
    let mut proxied_server =
        spawn_http_server(&proxied_config, &registry.certificate, proxied_address);
    wait_for_http_server(&mut proxied_server.child, proxied_address);
    let before = proxy.tracked_blob_bytes();
    let proxy_requests_before = proxy.total_requests();
    let proxy_accepted_before = proxy.accepted_connections();
    let proxy_tls_before = proxy.tls_connections();
    let tracked_requests_before = proxy.tracked_blob_requests();
    let upstream_requests_before = registry.request_count("team/releases");
    let disconnect_result = tokio::task::spawn_blocking(move || {
        disconnect_http_response(proxied_address, "/files/large.bin")
    })
    .await
    .unwrap();
    let (status, headers, client_received) = match disconnect_result {
        Ok(response) => response,
        Err(error) => {
            let server_logs = stop_http_server(&mut proxied_server);
            panic!(
                "could not receive first proxied response body chunk: {error}; proxy accepted={} TLS={} requests={} (tracked={}); proxy forwarded {} bytes; upstream registry requests delta={}; server logs: {server_logs}",
                proxy
                    .accepted_connections()
                    .saturating_sub(proxy_accepted_before),
                proxy.tls_connections().saturating_sub(proxy_tls_before),
                proxy.total_requests().saturating_sub(proxy_requests_before),
                proxy
                    .tracked_blob_requests()
                    .saturating_sub(tracked_requests_before),
                proxy.tracked_blob_bytes().saturating_sub(before),
                registry
                    .request_count("team/releases")
                    .saturating_sub(upstream_requests_before)
            );
        }
    };
    assert_eq!(status, 200);
    assert_eq!(
        headers.get("content-length").unwrap(),
        &total_size.to_string()
    );
    assert!(client_received > 0 && (client_received as u64) < total_size);
    wait_for_proxy_blob_bytes(&proxy, before + client_received as u64).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let settled = proxy.tracked_blob_bytes();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let after = proxy.tracked_blob_bytes();
    assert!(
        after.saturating_sub(settled) < 1024 * 1024,
        "upstream blob continued streaming after the client disconnected"
    );
    assert!(
        after.saturating_sub(before) < total_size / 2,
        "disconnect consumed too much of the upstream blob"
    );
    let stderr = stop_http_server(&mut proxied_server);
    assert!(!stderr.contains("registry.example"));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn serve_bounds_inflight_streams_and_releases_request_permits() {
    let registry = TestRegistry::new(false);
    let total_size = 32 * 1024 * 1024_u64;
    let large_bytes = Bytes::from(vec![0x5a; total_size as usize]);
    let digest = Digest::from_bytes(&large_bytes);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.write_file(
        &StoragePath::parse_relative("large.bin").unwrap(),
        large_bytes,
        false,
        None,
    )
    .await
    .unwrap();
    tree.write_file(
        &StoragePath::parse_relative("small.txt").unwrap(),
        Bytes::from_static(b"small complete response"),
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);

    let proxy = FaultProxy::tracking_blob(&registry, &digest).await;
    let scratch = TempDir::new();
    let config_path = scratch.path.join("concurrency.toml");
    fs::write(
        &config_path,
        format!(
            "version = 1\n[server]\nmax_concurrent_requests = 1\n[[mounts]]\nname = \"files\"\nprefix = \"/files\"\nregistry = \"{}\"\nrepository = \"team/releases\"\n",
            proxy.base_url
        ),
    )
    .unwrap();
    let address = unused_http_address();
    let mut server = spawn_http_server(&config_path, &registry.certificate, address);
    wait_for_http_server(&mut server.child, address);

    let (open_response, status, headers) =
        tokio::task::spawn_blocking(move || open_http_response_head(address, "/files/large.bin"))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(status, 200);
    assert_eq!(
        headers.get("content-length").unwrap(),
        &total_size.to_string()
    );
    wait_for_proxy_blob_bytes(&proxy, 256 * 1024).await;
    assert!(proxy.tracked_blob_bytes() < total_size);

    let proxy_requests_before_saturated = proxy.total_requests();
    let saturated = http_request(address, "GET", "/files/large.bin", &[]).unwrap();
    assert_eq!(saturated.status, 503);
    assert_eq!(saturated.body, b"service unavailable\n");
    assert_eq!(
        proxy.total_requests(),
        proxy_requests_before_saturated,
        "a saturated request must be rejected before registry work"
    );
    let cleartext_credential = http_request(
        address,
        "GET",
        "/files/large.bin",
        &[("Authorization", BASIC_AUTH)],
    )
    .unwrap();
    assert_eq!(cleartext_credential.status, 403);
    let duplicate_credentials = http_request(
        address,
        "GET",
        "/files/large.bin",
        &[
            ("Authorization", BASIC_AUTH),
            ("Authorization", "Basic d3Jvbmc6cGFzcw=="),
        ],
    )
    .unwrap();
    assert_eq!(duplicate_credentials.status, 400);
    assert_eq!(proxy.total_requests(), proxy_requests_before_saturated);
    drop(open_response);
    let release_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let health = http_request(address, "GET", "/healthz", &[]).unwrap();
        if health.status == 204 {
            break;
        }
        assert_eq!(
            health.status, 503,
            "unexpected response while awaiting disconnect cleanup"
        );
        assert!(
            Instant::now() < release_deadline,
            "request permit was not released after the client disconnected"
        );
        thread::sleep(Duration::from_millis(50));
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let completed = client
        .get(format!("http://{address}/files/small.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(completed.status(), reqwest::StatusCode::OK);
    assert_eq!(
        completed.bytes().await.unwrap().as_ref(),
        b"small complete response"
    );
    let health = http_request(address, "GET", "/healthz", &[]).unwrap();
    assert_eq!(
        health.status, 204,
        "the permit must be released after body EOF"
    );
    let stderr = stop_http_server(&mut server);
    assert!(!stderr.contains("registry.example"));
}

#[tokio::test]
async fn serve_maps_a_stopped_registry_to_service_unavailable() {
    let registry = TestRegistry::new(false);
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.write_file(
        &StoragePath::parse_relative("available.txt").unwrap(),
        Bytes::from_static(b"stored before registry stopped"),
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);

    let scratch = TempDir::new();
    let config_path = scratch.path.join("stopped-registry.toml");
    fs::write(
        &config_path,
        format!(
            r#"version = 1

[[mounts]]
name = "files"
prefix = "/files"
registry = "{}"
repository = "team/releases"
"#,
            registry.base_url
        ),
    )
    .unwrap();
    let address = unused_http_address();
    let mut server = spawn_http_server(&config_path, &registry.certificate, address);
    wait_for_http_server(&mut server.child, address);

    let stopped = Command::new("docker")
        .args(["stop", "--time", "1", &registry.container])
        .output()
        .unwrap();
    assert!(stopped.status.success(), "could not stop test registry");
    let response = http_request(address, "GET", "/files/available.txt", &[]).unwrap();
    assert_eq!(response.status, 503);
    assert_eq!(response.body, b"service unavailable\n");
    assert!(!String::from_utf8_lossy(&response.body).contains("127.0.0.1"));
    stop_http_server(&mut server);
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
async fn tree_rejects_branch_chains_beyond_the_documented_depth_limit() {
    const MAX_BRANCH_DEPTH: usize = 64;
    let registry = TestRegistry::new(false);
    let path = StoragePath::parse_relative("entry.txt").unwrap();
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.write_file(&path, Bytes::from_static(b"depth fixture"), false, None)
        .await
        .unwrap();
    drop(tree);

    let client = registry.client(ClientPolicy::default());
    let leaf = client
        .resolve_tag(&registry.mount.root_tag, &[OCI_IMAGE_INDEX], None)
        .await
        .unwrap();
    let valid_root = install_single_child_branch_chain(
        &registry,
        &client,
        leaf.digest.clone(),
        leaf.size,
        MAX_BRANCH_DEPTH,
    )
    .await;
    let valid_tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    let stat = valid_tree.stat_file(&path, None).await.unwrap();
    assert_eq!(stat.size, b"depth fixture".len() as u64);
    let page = valid_tree
        .list_page(&StoragePath::root(), 10, None, None)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "entry.txt");
    valid_tree
        .write_file(
            &path,
            Bytes::from_static(b"updated at max depth"),
            true,
            None,
        )
        .await
        .unwrap();
    assert_ne!(
        client
            .resolve_tag(&registry.mount.root_tag, &[OCI_IMAGE_INDEX], None)
            .await
            .unwrap()
            .digest,
        valid_root
    );
    drop(valid_tree);

    let too_deep_root = install_single_child_branch_chain(
        &registry,
        &client,
        leaf.digest.clone(),
        leaf.size,
        MAX_BRANCH_DEPTH + 1,
    )
    .await;
    let too_deep_tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    let leaf_manifest_path = format!(
        "/v2/{}/manifests/{}",
        registry.mount.repository, leaf.digest
    );
    let leaf_reads_before = registry.request_count_for_path(&leaf_manifest_path);

    assert!(matches!(
        too_deep_tree.stat_file(&path, None).await,
        Err(TreeError::IndexDepthLimit)
    ));
    assert!(matches!(
        too_deep_tree
            .list_page(&StoragePath::root(), 10, None, None)
            .await,
        Err(TreeError::IndexDepthLimit)
    ));
    assert!(matches!(
        too_deep_tree
            .write_file(&path, Bytes::from_static(b"must not publish"), true, None)
            .await,
        Err(TreeError::IndexDepthLimit)
    ));
    assert_eq!(
        registry.request_count_for_path(&leaf_manifest_path),
        leaf_reads_before,
        "an over-depth traversal must reject before fetching the leaf beyond the bound"
    );
    assert_eq!(
        client
            .resolve_tag(&registry.mount.root_tag, &[OCI_IMAGE_INDEX], None)
            .await
            .unwrap()
            .digest,
        too_deep_root,
        "a rejected mutation must preserve the root tag"
    );
}

#[tokio::test]
async fn tree_refuses_root_promotion_that_would_deepen_an_untouched_sibling() {
    let registry = TestRegistry::new(false);
    let client = registry.client(ClientPolicy::default());
    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    tree.write_file(
        &StoragePath::parse_relative("a").unwrap(),
        Bytes::from_static(b"shared valid file manifest"),
        false,
        None,
    )
    .await
    .unwrap();
    drop(tree);

    let root_leaf = client
        .get_manifest(
            &ManifestReference::Tag(registry.mount.root_tag.clone()),
            &[OCI_IMAGE_INDEX],
            None,
        )
        .await
        .unwrap();
    let leaf_json: serde_json::Value = serde_json::from_slice(&root_leaf.bytes).unwrap();
    let shared_file_descriptor = &leaf_json["manifests"][0];
    let shared_file_digest =
        Digest::parse(shared_file_descriptor["digest"].as_str().unwrap()).unwrap();
    let shared_file_size = shared_file_descriptor["size"].as_u64().unwrap();

    let leaf_bytes = test_leaf_index(vec![shared_file_descriptor.clone()]);
    let (mut deep_digest, mut deep_size) = put_test_index(&client, leaf_bytes).await;
    for _ in 0..63 {
        let branch = test_branch_index(vec![test_branch_descriptor(
            &deep_digest,
            deep_size,
            "a",
            "a",
        )]);
        (deep_digest, deep_size) = put_test_index(&client, branch).await;
    }
    let mut root_children = vec![test_branch_descriptor(&deep_digest, deep_size, "a", "a")];

    let mut overflowing_leaf = Vec::new();
    for index in 0..128 {
        let name = format!("b000-{index:03}");
        overflowing_leaf.push(test_file_descriptor(&DirectoryEntry {
            name,
            kind: EntryKind::File,
            digest: shared_file_digest.clone(),
            size: shared_file_size,
        }));
    }
    let (digest, size) = put_test_index(&client, test_leaf_index(overflowing_leaf)).await;
    root_children.push(test_branch_descriptor(
        &digest, size, "b000-000", "b000-127",
    ));

    for index in 1..=126 {
        let name = format!("b{index:03}");
        let descriptor = test_file_descriptor(&DirectoryEntry {
            name: name.clone(),
            kind: EntryKind::File,
            digest: shared_file_digest.clone(),
            size: shared_file_size,
        });
        let (digest, size) = put_test_index(&client, test_leaf_index(vec![descriptor])).await;
        root_children.push(test_branch_descriptor(&digest, size, &name, &name));
    }
    assert_eq!(root_children.len(), 128);

    let root_bytes = test_branch_index(root_children);
    let root_digest = Digest::from_bytes(&root_bytes);
    client
        .put_manifest(
            &ManifestReference::Tag(registry.mount.root_tag.clone()),
            OCI_IMAGE_INDEX,
            Bytes::from(root_bytes),
            None,
        )
        .await
        .unwrap();

    let tree = TreeStore::new(registry.client(ClientPolicy::default())).unwrap();
    let error = tree
        .write_file(
            &StoragePath::parse_relative("b000-050a").unwrap(),
            Bytes::from_static(b"must not publish"),
            false,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, TreeError::IndexDepthLimit));
    assert_eq!(
        client
            .resolve_tag(&registry.mount.root_tag, &[OCI_IMAGE_INDEX], None)
            .await
            .unwrap()
            .digest,
        root_digest,
        "a failed root promotion must not publish an over-depth sibling"
    );
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

fn test_leaf_index(descriptors: Vec<serde_json::Value>) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": OCI_IMAGE_INDEX,
        "manifests": descriptors,
        "annotations": {
            "io.github.djha-skin.oci-auto-http.node-kind": "directory-leaf",
            "io.github.djha-skin.oci-auto-http.format-version": "1"
        }
    }))
    .unwrap()
}

fn test_branch_index(descriptors: Vec<serde_json::Value>) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": OCI_IMAGE_INDEX,
        "manifests": descriptors,
        "annotations": {
            "io.github.djha-skin.oci-auto-http.node-kind": "directory-branch",
            "io.github.djha-skin.oci-auto-http.format-version": "1"
        }
    }))
    .unwrap()
}

fn test_file_descriptor(entry: &DirectoryEntry) -> serde_json::Value {
    serde_json::json!({
        "mediaType": OCI_IMAGE_MANIFEST,
        "digest": entry.digest.to_string(),
        "size": entry.size,
        "annotations": {
            "io.github.djha-skin.oci-auto-http.entry-name": entry.name
        }
    })
}

fn test_branch_descriptor(
    digest: &Digest,
    size: u64,
    first_name: &str,
    last_name: &str,
) -> serde_json::Value {
    serde_json::json!({
        "mediaType": OCI_IMAGE_INDEX,
        "digest": digest.to_string(),
        "size": size,
        "annotations": {
            "io.github.djha-skin.oci-auto-http.first-name": first_name,
            "io.github.djha-skin.oci-auto-http.last-name": last_name
        }
    })
}

async fn put_test_index(client: &RegistryClient, bytes: Vec<u8>) -> (Digest, u64) {
    let digest = Digest::from_bytes(&bytes);
    let size = bytes.len() as u64;
    client
        .put_manifest(
            &ManifestReference::Digest(digest.clone()),
            OCI_IMAGE_INDEX,
            Bytes::from(bytes),
            None,
        )
        .await
        .unwrap();
    (digest, size)
}

async fn install_single_child_branch_chain(
    registry: &TestRegistry,
    client: &RegistryClient,
    child_digest: Digest,
    child_size: u64,
    branch_edges: usize,
) -> Digest {
    const NODE_KIND_ANNOTATION: &str = "io.github.djha-skin.oci-auto-http.node-kind";
    const FORMAT_VERSION_ANNOTATION: &str = "io.github.djha-skin.oci-auto-http.format-version";
    const FIRST_NAME_ANNOTATION: &str = "io.github.djha-skin.oci-auto-http.first-name";
    const LAST_NAME_ANNOTATION: &str = "io.github.djha-skin.oci-auto-http.last-name";

    assert!(branch_edges > 0);
    let mut child_digest = child_digest;
    let mut child_size = child_size;
    let mut top_manifest = None;
    for _ in 0..branch_edges {
        let bytes = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "schemaVersion": 2,
                "mediaType": OCI_IMAGE_INDEX,
                "manifests": [{
                    "mediaType": OCI_IMAGE_INDEX,
                    "digest": child_digest.to_string(),
                    "size": child_size,
                    "annotations": {
                        (FIRST_NAME_ANNOTATION): "entry.txt",
                        (LAST_NAME_ANNOTATION): "entry.txt"
                    }
                }],
                "annotations": {
                    (NODE_KIND_ANNOTATION): "directory-branch",
                    (FORMAT_VERSION_ANNOTATION): "1"
                }
            }))
            .unwrap(),
        );
        child_digest = Digest::from_bytes(&bytes);
        child_size = bytes.len() as u64;
        client
            .put_manifest(
                &ManifestReference::Digest(child_digest.clone()),
                OCI_IMAGE_INDEX,
                bytes.clone(),
                None,
            )
            .await
            .unwrap();
        top_manifest = Some(bytes);
    }
    client
        .put_manifest(
            &ManifestReference::Tag(registry.mount.root_tag.clone()),
            OCI_IMAGE_INDEX,
            top_manifest.unwrap(),
            None,
        )
        .await
        .unwrap();
    child_digest
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

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn http_request(
    address: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
) -> io::Result<HttpResponse> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let host = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("host"))
        .map_or("localhost", |(_, value)| *value);
    write!(stream, "{method} {target} HTTP/1.1\r\nHost: {host}\r\n")?;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("host") {
            continue;
        }
        write!(stream, "{name}: {value}\r\n")?;
    }
    stream.write_all(b"Connection: close\r\n\r\n")?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let header_end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| io::Error::other("HTTP response omitted its header terminator"))?;
    let header_text = std::str::from_utf8(&raw[..header_end])
        .map_err(|_| io::Error::other("HTTP response headers were not UTF-8"))?;
    let mut lines = header_text.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .ok_or_else(|| io::Error::other("HTTP response status was malformed"))?;
    let mut response_headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            response_headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    let raw_body = &raw[header_end + 4..];
    let body = if response_headers
        .get("transfer-encoding")
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        decode_chunked_body(raw_body)?
    } else {
        raw_body.to_vec()
    };
    Ok(HttpResponse {
        status,
        headers: response_headers,
        body,
    })
}

fn decode_chunked_body(body: &[u8]) -> io::Result<Vec<u8>> {
    let mut position = 0;
    let mut decoded = Vec::new();
    loop {
        let line_end = body[position..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| io::Error::other("malformed chunked HTTP body"))?;
        let size_line = std::str::from_utf8(&body[position..position + line_end])
            .map_err(|_| io::Error::other("invalid HTTP chunk size"))?;
        let size = usize::from_str_radix(size_line.split(';').next().unwrap_or_default(), 16)
            .map_err(|_| io::Error::other("invalid HTTP chunk size"))?;
        position += line_end + 2;
        if size == 0 {
            return Ok(decoded);
        }
        let end = position
            .checked_add(size)
            .filter(|end| end.checked_add(2).is_some_and(|end| end <= body.len()))
            .ok_or_else(|| io::Error::other("truncated chunked HTTP body"))?;
        decoded.extend_from_slice(&body[position..end]);
        if &body[end..end + 2] != b"\r\n" {
            return Err(io::Error::other("malformed HTTP chunk terminator"));
        }
        position = end + 2;
    }
}

#[cfg(target_os = "linux")]
struct PartialHttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn read_http_response_partial(
    address: SocketAddr,
    target: &str,
) -> io::Result<PartialHttpResponse> {
    let mut socket = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        socket,
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )?;
    let mut reader = BufReader::new(socket);
    let (status, headers) = read_http_response_head(&mut reader)?;
    let mut body = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(count) => body.extend_from_slice(&buffer[..count]),
        }
    }
    Ok(PartialHttpResponse {
        status,
        headers,
        body,
    })
}

fn read_http_response_head(
    reader: &mut BufReader<TcpStream>,
) -> io::Result<(u16, HashMap<String, String>)> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let status = line
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .ok_or_else(|| io::Error::other("HTTP response status was malformed"))?;
    let mut headers = HashMap::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Err(io::Error::other("HTTP response ended before its headers"));
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    Ok((status, headers))
}

#[cfg(target_os = "linux")]
struct StreamedHttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    bytes_read: u64,
    digest: String,
    peak_rss_bytes: u64,
}

#[cfg(target_os = "linux")]
fn process_rss_bytes(pid: u32) -> u64 {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let resident = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    resident * 1024
}

#[cfg(target_os = "linux")]
fn stream_http_response(
    address: SocketAddr,
    target: &str,
    pid: u32,
) -> io::Result<StreamedHttpResponse> {
    let mut socket = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    socket.set_read_timeout(Some(Duration::from_secs(30)))?;
    write!(
        socket,
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )?;
    let mut reader = BufReader::new(socket);
    let (status, headers) = read_http_response_head(&mut reader)?;
    let mut remaining = headers
        .get("content-length")
        .ok_or_else(|| io::Error::other("streamed HTTP response omitted Content-Length"))?
        .parse::<u64>()
        .map_err(|_| io::Error::other("streamed HTTP Content-Length was invalid"))?;
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut received = 0_u64;
    let mut peak_rss_bytes = process_rss_bytes(pid);
    let mut hasher = Sha256::new();
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        let count = reader.read(&mut buffer[..limit])?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        received += count as u64;
        remaining -= count as u64;
        peak_rss_bytes = peak_rss_bytes.max(process_rss_bytes(pid));
        thread::sleep(Duration::from_millis(1));
    }
    Ok(StreamedHttpResponse {
        status,
        headers,
        bytes_read: received,
        digest: format!("sha256:{:x}", hasher.finalize()),
        peak_rss_bytes,
    })
}

#[cfg(target_os = "linux")]
fn open_http_response_head(
    address: SocketAddr,
    target: &str,
) -> io::Result<(BufReader<TcpStream>, u16, HashMap<String, String>)> {
    let mut socket = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        socket,
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )?;
    let mut reader = BufReader::new(socket);
    let (status, headers) = read_http_response_head(&mut reader)?;
    Ok((reader, status, headers))
}

#[cfg(target_os = "linux")]
fn disconnect_http_response(
    address: SocketAddr,
    target: &str,
) -> io::Result<(u16, HashMap<String, String>, usize)> {
    let mut socket = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        socket,
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )?;
    let mut reader = BufReader::new(socket);
    let (status, headers) = read_http_response_head(&mut reader)
        .map_err(|error| io::Error::other(format!("reading response headers: {error}")))?;
    let mut first_chunk = vec![0_u8; 64 * 1024];
    let count = reader
        .read(&mut first_chunk)
        .map_err(|error| io::Error::other(format!("reading first body chunk: {error}")))?;
    Ok((status, headers, count))
}

struct RunningHttpServer {
    child: Child,
    stopped: bool,
}

impl Drop for RunningHttpServer {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn spawn_http_server(config: &Path, certificate: &Path, address: SocketAddr) -> RunningHttpServer {
    let child = Command::new(cli_binary())
        .args(["--log-format", "json", "serve", "--config"])
        .arg(config)
        .args(["--ca-cert"])
        .arg(certificate)
        .args(["--listen", &address.to_string()])
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    RunningHttpServer {
        child,
        stopped: false,
    }
}

fn spawn_https_http_server(
    config: &Path,
    registry_ca: &Path,
    listener_certificate: &Path,
    listener_key: &Path,
    address: SocketAddr,
) -> RunningHttpServer {
    let child = Command::new(cli_binary())
        .args(["--log-format", "json", "serve", "--config"])
        .arg(config)
        .args(["--ca-cert"])
        .arg(registry_ca)
        .args(["--tls-cert"])
        .arg(listener_certificate)
        .args(["--tls-key"])
        .arg(listener_key)
        .args(["--listen", &address.to_string()])
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    RunningHttpServer {
        child,
        stopped: false,
    }
}

fn https_test_client(certificate: &Path) -> reqwest::Client {
    let root = reqwest::Certificate::from_pem(&fs::read(certificate).unwrap()).unwrap();
    reqwest::Client::builder()
        .add_root_certificate(root)
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

async fn wait_for_https_http_server(
    child: &mut Child,
    address: SocketAddr,
    client: &reqwest::Client,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                pipe.read_to_string(&mut stderr).unwrap();
            }
            panic!("HTTPS server exited before readiness: {status}; stderr: {stderr}");
        }
        if client
            .get(format!("https://{address}/healthz"))
            .send()
            .await
            .is_ok_and(|response| response.status() == reqwest::StatusCode::NO_CONTENT)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "HTTPS server did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn wait_for_http_server(child: &mut Child, address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                pipe.read_to_string(&mut stderr).unwrap();
            }
            panic!("HTTP server exited before readiness: {status}; stderr: {stderr}");
        }
        if http_request(address, "GET", "/healthz", &[])
            .is_ok_and(|response| response.status == 204)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "HTTP server did not become ready"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn stop_http_server(server: &mut RunningHttpServer) -> String {
    let signal = Command::new("kill")
        .args(["-TERM", &server.child.id().to_string()])
        .status()
        .unwrap();
    assert!(
        signal.success(),
        "could not send SIGTERM to HTTP test server"
    );
    let status = server.child.wait().unwrap();
    server.stopped = true;
    let mut stderr = String::new();
    server
        .child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(status.success(), "HTTP test server exited unsuccessfully");
    stderr
}

fn unused_http_address() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn wait_for_registry_requests(registry: &TestRegistry, repository: &str, minimum: usize) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let count = registry.request_count(repository);
        if count >= minimum {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "registry {repository} logged {count} matching requests; expected at least {minimum}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

async fn wait_for_proxy_blob_bytes(proxy: &FaultProxy, minimum: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let count = proxy.tracked_blob_bytes();
        if count >= minimum {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "proxy forwarded {count} tracked blob bytes; expected at least {minimum}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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

    fn request_count(&self, repository: &str) -> usize {
        let output = Command::new("docker")
            .args(["logs", &self.container])
            .output()
            .unwrap();
        assert!(output.status.success(), "docker logs failed");
        let logs = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        logs.matches(&format!("/v2/{repository}/")).count()
    }

    fn request_count_for_path(&self, path: &str) -> usize {
        let output = Command::new("docker")
            .args(["logs", &self.container])
            .output()
            .unwrap();
        assert!(output.status.success(), "docker logs failed");
        let logs = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        logs.matches(path).count()
    }
}

#[derive(Clone, Copy)]
enum CommitFault {
    BeforeCommit,
    AfterCommit,
}

struct FaultProxyOptions {
    mode: CommitFault,
    tracked_blob_path: Option<String>,
    corrupt_tracked_blob: bool,
    rate_limit_path: Option<String>,
    rate_limit_after: Option<HeaderValue>,
    auth_challenge_path: Option<String>,
    auth_challenge_after: usize,
    delayed_request_path: Option<String>,
    delayed_request: Duration,
    tracked_blob_delay: Duration,
}

impl Default for FaultProxyOptions {
    fn default() -> Self {
        Self {
            mode: CommitFault::BeforeCommit,
            tracked_blob_path: None,
            corrupt_tracked_blob: false,
            rate_limit_path: None,
            rate_limit_after: None,
            auth_challenge_path: None,
            auth_challenge_after: 0,
            delayed_request_path: None,
            delayed_request: Duration::ZERO,
            tracked_blob_delay: Duration::from_millis(2),
        }
    }
}

struct FaultState {
    mode: CommitFault,
    root_path: String,
    tracked_blob_path: Option<String>,
    corrupt_tracked_blob: bool,
    corruption_fired: AtomicBool,
    rate_limit_path: Option<String>,
    rate_limit_after: Option<HeaderValue>,
    accepted_connections: AtomicUsize,
    tls_connections: AtomicUsize,
    tracked_blob_requests: AtomicUsize,
    tracked_blob_bytes: AtomicU64,
    total_requests: AtomicUsize,
    fault_fired: AtomicBool,
    root_put_attempts: AtomicUsize,
    root_put_forwarded: AtomicUsize,
    auth_challenge_path: Option<String>,
    auth_challenge_after: usize,
    auth_challenge_matches: AtomicUsize,
    auth_challenge_fired: AtomicBool,
    delayed_request_path: Option<String>,
    delayed_request: Duration,
    delay_fired: AtomicBool,
    tracked_blob_delay: Duration,
}

struct FaultProxy {
    base_url: String,
    state: Arc<FaultState>,
    task: JoinHandle<()>,
}

impl FaultProxy {
    async fn new(registry: &TestRegistry, mode: CommitFault) -> Self {
        Self::new_with_tracked_blob(
            registry,
            FaultProxyOptions {
                mode,
                ..FaultProxyOptions::default()
            },
        )
        .await
    }

    async fn tracking_blob(registry: &TestRegistry, digest: &Digest) -> Self {
        let path = format!("/v2/{}/blobs/{digest}", registry.mount.repository);
        Self::new_with_tracked_blob(
            registry,
            FaultProxyOptions {
                tracked_blob_path: Some(path),
                ..FaultProxyOptions::default()
            },
        )
        .await
    }

    async fn corrupting_blob(registry: &TestRegistry, digest: &Digest) -> Self {
        let path = format!("/v2/{}/blobs/{digest}", registry.mount.repository);
        Self::new_with_tracked_blob(
            registry,
            FaultProxyOptions {
                tracked_blob_path: Some(path),
                corrupt_tracked_blob: true,
                ..FaultProxyOptions::default()
            },
        )
        .await
    }

    async fn rate_limited(registry: &TestRegistry, retry_after: Option<&'static str>) -> Self {
        let path = format!(
            "/v2/{}/manifests/{}",
            registry.mount.repository, registry.mount.root_tag
        );
        Self::new_with_tracked_blob(
            registry,
            FaultProxyOptions {
                rate_limit_path: Some(path),
                rate_limit_after: retry_after.map(HeaderValue::from_static),
                ..FaultProxyOptions::default()
            },
        )
        .await
    }

    async fn challenge_after_manifest_gets(
        registry: &TestRegistry,
        digest: &Digest,
        after: usize,
    ) -> Self {
        let path = format!("/v2/{}/manifests/{digest}", registry.mount.repository);
        Self::new_with_tracked_blob(
            registry,
            FaultProxyOptions {
                auth_challenge_path: Some(path),
                auth_challenge_after: after,
                ..FaultProxyOptions::default()
            },
        )
        .await
    }

    async fn delaying_request(registry: &TestRegistry, path: String, delay: Duration) -> Self {
        Self::new_with_tracked_blob(
            registry,
            FaultProxyOptions {
                delayed_request_path: Some(path),
                delayed_request: delay,
                ..FaultProxyOptions::default()
            },
        )
        .await
    }

    async fn slowly_tracking_blob(
        registry: &TestRegistry,
        digest: &Digest,
        delay: Duration,
    ) -> Self {
        let path = format!("/v2/{}/blobs/{digest}", registry.mount.repository);
        Self::new_with_tracked_blob(
            registry,
            FaultProxyOptions {
                tracked_blob_path: Some(path),
                tracked_blob_delay: delay,
                ..FaultProxyOptions::default()
            },
        )
        .await
    }

    async fn new_with_tracked_blob(registry: &TestRegistry, options: FaultProxyOptions) -> Self {
        let FaultProxyOptions {
            mode,
            tracked_blob_path,
            corrupt_tracked_blob,
            rate_limit_path,
            rate_limit_after,
            auth_challenge_path,
            auth_challenge_after,
            delayed_request_path,
            delayed_request,
            tracked_blob_delay,
        } = options;
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
            tracked_blob_path,
            corrupt_tracked_blob,
            corruption_fired: AtomicBool::new(false),
            rate_limit_path,
            rate_limit_after,
            accepted_connections: AtomicUsize::new(0),
            tls_connections: AtomicUsize::new(0),
            tracked_blob_requests: AtomicUsize::new(0),
            tracked_blob_bytes: AtomicU64::new(0),
            total_requests: AtomicUsize::new(0),
            fault_fired: AtomicBool::new(false),
            root_put_attempts: AtomicUsize::new(0),
            root_put_forwarded: AtomicUsize::new(0),
            auth_challenge_path,
            auth_challenge_after,
            auth_challenge_matches: AtomicUsize::new(0),
            auth_challenge_fired: AtomicBool::new(false),
            delayed_request_path,
            delayed_request,
            delay_fired: AtomicBool::new(false),
            tracked_blob_delay,
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
                    state.accepted_connections.fetch_add(1, Ordering::SeqCst);
                    let acceptor = acceptor.clone();
                    let upstream = upstream.clone();
                    let proxy_origin = proxy_origin.clone();
                    let client = upstream_client.clone();
                    let state = Arc::clone(&state);
                    tokio::spawn(async move {
                        let Ok(stream) = acceptor.accept(stream).await else {
                            return;
                        };
                        state.tls_connections.fetch_add(1, Ordering::SeqCst);
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
                                    state,
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

    fn tracked_blob_bytes(&self) -> u64 {
        self.state.tracked_blob_bytes.load(Ordering::SeqCst)
    }

    fn accepted_connections(&self) -> usize {
        self.state.accepted_connections.load(Ordering::SeqCst)
    }

    fn tls_connections(&self) -> usize {
        self.state.tls_connections.load(Ordering::SeqCst)
    }

    fn total_requests(&self) -> usize {
        self.state.total_requests.load(Ordering::SeqCst)
    }

    fn tracked_blob_requests(&self) -> usize {
        self.state.tracked_blob_requests.load(Ordering::SeqCst)
    }

    fn corruption_fired(&self) -> bool {
        self.state.corruption_fired.load(Ordering::SeqCst)
    }

    fn auth_challenge_matches(&self) -> usize {
        self.state.auth_challenge_matches.load(Ordering::SeqCst)
    }

    fn auth_challenge_fired(&self) -> bool {
        self.state.auth_challenge_fired.load(Ordering::SeqCst)
    }

    fn delay_fired(&self) -> bool {
        self.state.delay_fired.load(Ordering::SeqCst)
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
    state: Arc<FaultState>,
) -> Result<Response<AxumBody>, io::Error> {
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
    state.total_requests.fetch_add(1, Ordering::SeqCst);
    if state.delayed_request_path.as_deref() == Some(parts.uri.path()) {
        state.delay_fired.store(true, Ordering::SeqCst);
        tokio::time::sleep(state.delayed_request).await;
    }
    if parts.method == reqwest::Method::GET
        && state.auth_challenge_path.as_deref() == Some(parts.uri.path())
    {
        let matched = state.auth_challenge_matches.fetch_add(1, Ordering::SeqCst) + 1;
        if matched == state.auth_challenge_after {
            state.auth_challenge_fired.store(true, Ordering::SeqCst);
            return Response::builder()
                .status(reqwest::StatusCode::UNAUTHORIZED)
                .header(
                    reqwest::header::WWW_AUTHENTICATE,
                    "Bearer realm=\"https://auth.example/token\",service=\"registry-test\"",
                )
                .body(AxumBody::empty())
                .map_err(|error| io::Error::other(error.to_string()));
        }
    }
    if parts.method == reqwest::Method::HEAD
        && state.rate_limit_path.as_deref() == Some(parts.uri.path())
    {
        let mut response = Response::builder().status(reqwest::StatusCode::TOO_MANY_REQUESTS);
        if let Some(retry_after) = &state.rate_limit_after {
            response = response.header(reqwest::header::RETRY_AFTER, retry_after.clone());
        }
        return response
            .body(AxumBody::from("rate limited\\n"))
            .map_err(|error| io::Error::other(error.to_string()));
    }
    let tracked_blob = parts.method == reqwest::Method::GET
        && state.tracked_blob_path.as_deref() == Some(parts.uri.path());
    if tracked_blob {
        state.tracked_blob_requests.fetch_add(1, Ordering::SeqCst);
    }
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

    if inject_fault && matches!(state.mode, CommitFault::AfterCommit) && status.is_success() {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "injected connection drop after root-tag commit",
        ));
    }

    let metrics = Arc::clone(&state);
    let corrupt_tracked_blob = tracked_blob && state.corrupt_tracked_blob;
    let tracked_blob_delay = state.tracked_blob_delay;
    let response_body = response
        .bytes_stream()
        .map(move |result| match result {
            Ok(mut bytes) => {
                if tracked_blob {
                    metrics
                        .tracked_blob_bytes
                        .fetch_add(bytes.len() as u64, Ordering::SeqCst);
                }
                if corrupt_tracked_blob
                    && !metrics.corruption_fired.swap(true, Ordering::SeqCst)
                    && !bytes.is_empty()
                {
                    let mut corrupted = bytes.to_vec();
                    corrupted[0] ^= 0x01;
                    bytes = Bytes::from(corrupted);
                }
                Ok(bytes)
            }
            Err(error) => Err(io::Error::other(error.to_string())),
        })
        .then(move |chunk| async move {
            if tracked_blob && chunk.is_ok() {
                tokio::time::sleep(tracked_blob_delay).await;
            }
            chunk
        });
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
        .body(AxumBody::from_stream(response_body))
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
