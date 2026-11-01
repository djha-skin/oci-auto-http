#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{StreamExt, TryStreamExt};
use oci_auto_http::{
    config::{Config, Mount},
    registry::{
        Bytes, ClientPolicy, Digest, ErrorKind, HeaderValue, ManifestReference, OCI_EMPTY_CONFIG,
        OCI_IMAGE_MANIFEST, RegistryClient,
    },
};

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
