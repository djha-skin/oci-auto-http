use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[cfg(unix)]
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    process::{Child, Stdio},
    thread,
    time::{Duration, Instant},
};

fn config_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.toml")
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "oci-auto-http-test-{}-{unique}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self { path }
    }

    fn write_config(&self, contents: &str) -> PathBuf {
        let path = self.path.join("config.toml");
        fs::write(&path, contents).unwrap();
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn valid_two_mount_config() -> String {
    r#"
version = 1

[server]
listen = "127.0.0.1:9999"

[[mounts]]
name = "packages"
prefix = "/packages"
registry = "https://reg-a.example"
repository = "team/packages"

[[mounts]]
name = "mirror"
prefix = "/mirror"
registry = "https://reg-b.example:5443"
repository = "team/mirror"
root_tag = "mirror-v2"
redirect_allowlist = ["cdn.example:443"]
"#
    .to_owned()
}

fn spawn_mount(config: &Path, args: &[&str]) -> std::process::Child {
    Command::new(binary())
        .args(["mount", "--config"])
        .arg(config)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
}

fn run_mount(config: &Path, args: &[&str]) -> std::process::Output {
    spawn_mount(config, args).wait_with_output().unwrap()
}

fn binary() -> PathBuf {
    ["CARGO_BIN_EXE_oci-auto-http", "CARGO_BIN_EXE_oci_auto_http"]
        .into_iter()
        .find_map(std::env::var_os)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let suffix = std::env::consts::EXE_SUFFIX;
            std::env::current_exe()
                .expect("test executable path is available")
                .parent()
                .and_then(std::path::Path::parent)
                .expect("test executable is under target/<profile>/deps")
                .join(format!("oci-auto-http{suffix}"))
        })
}

#[test]
fn help_exposes_stable_command_groups_and_crud_verbs() {
    let output = Command::new(binary()).arg("--help").output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for command in ["serve", "artifact", "mount"] {
        assert!(
            help.contains(command),
            "top-level help omitted {command}: {help}"
        );
    }

    for (group, verbs) in [
        (
            "artifact",
            ["create", "read", "list", "update", "delete"].as_slice(),
        ),
        (
            "mount",
            ["create", "list", "show", "update", "delete"].as_slice(),
        ),
    ] {
        let output = Command::new(binary())
            .args([group, "--help"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let help = String::from_utf8(output.stdout).unwrap();
        for verb in verbs {
            assert!(help.contains(verb), "{group} help omitted {verb}: {help}");
        }
    }
}

#[test]
fn invalid_arguments_and_mount_usage_fail_actionably() {
    let invalid = Command::new(binary())
        .arg("--not-a-real-option")
        .output()
        .unwrap();
    assert_eq!(invalid.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("unexpected argument"));

    let missing_mount_options = Command::new(binary())
        .args(["mount", "create", "example"])
        .output()
        .unwrap();
    assert_eq!(missing_mount_options.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&missing_mount_options.stderr).contains("required"));

    let missing_mount_config = Command::new(binary())
        .args(["mount", "list"])
        .output()
        .unwrap();
    assert_eq!(missing_mount_config.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&missing_mount_config.stderr).contains("--config"));

    let missing_config = Command::new(binary())
        .args(["artifact", "create", "/packages/file"])
        .output()
        .unwrap();
    assert_eq!(missing_config.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&missing_config.stderr).contains("--config"));

    let create_help = Command::new(binary())
        .args(["artifact", "create", "--help"])
        .output()
        .unwrap();
    assert!(create_help.status.success());
    let help = String::from_utf8(create_help.stdout).unwrap();
    for option in ["--config", "--input", "--directory"] {
        assert!(
            help.contains(option),
            "artifact create help omitted {option}: {help}"
        );
    }
}

#[test]
fn mount_create_cli_initializes_a_missing_config_without_registry_access() {
    let temp = TempDir::new();
    let config_path = temp.path.join("new-mounts.toml");
    let created = run_mount(
        &config_path,
        &[
            "create",
            "packages",
            "--prefix",
            "/packages",
            "--registry",
            "https://registry.example",
            "--repository",
            "team/packages",
        ],
    );
    assert!(
        created.status.success(),
        "mount create failed: {}",
        String::from_utf8_lossy(&created.stderr)
    );
    let output: serde_json::Value = serde_json::from_slice(&created.stdout).unwrap();
    assert_eq!(output["name"], "packages");
    assert_eq!(output["registry"], "https://registry.example");
    assert_eq!(output["prefix"], "/packages");
    assert_eq!(output["root_tag"], "oci-auto-http-tree-v1");
    assert_eq!(output["redirect_allowlist"], serde_json::json!([]));
    assert!(config_path.is_file());

    let config = oci_auto_http::config::Config::load(&config_path).unwrap();
    assert_eq!(config.mounts.len(), 1);
    assert_eq!(config.mounts[0].repository, "team/packages");

    let version_only_path = temp.path.join("version-only.toml");
    fs::write(&version_only_path, "version = 1\n").unwrap();
    let version_only = run_mount(
        &version_only_path,
        &[
            "create",
            "seed",
            "--prefix",
            "/seed",
            "--registry",
            "https://version-only.example",
            "--repository",
            "team/seed",
        ],
    );
    assert!(
        version_only.status.success(),
        "version-only initialization failed: {}",
        String::from_utf8_lossy(&version_only.stderr)
    );
    let version_only_json: serde_json::Value =
        serde_json::from_slice(&version_only.stdout).unwrap();
    assert_eq!(
        version_only_json["registry"],
        "https://version-only.example"
    );
    assert_eq!(
        oci_auto_http::config::Config::load(&version_only_path)
            .unwrap()
            .mounts
            .len(),
        1
    );
}

#[test]
fn mount_crud_cli_preserves_comments_and_rejects_invalid_changes_atomically() {
    let temp = TempDir::new();
    let config_path = temp.write_config(
        r#"# Keep the operator header.
version = 1

[server]
listen = "127.0.0.1:9999" # Keep listen.

[[mounts]]
# Keep the package comment.
name = "packages"
prefix = "/packages" # Keep prefix.
registry = "https://reg-a.example"
repository = "team/packages"
root_tag = "packages-v2"
redirect_allowlist = ["cdn-a.example:443"]
"#,
    );

    let created = run_mount(
        &config_path,
        &[
            "create",
            "mirror",
            "--prefix",
            "/mirror",
            "--registry",
            "https://reg-b.example:5443",
            "--repository",
            "team/mirror",
            "--root-tag",
            "mirror-v1",
            "--redirect-allowlist",
            "cdn-b.example:443",
            "--redirect-allowlist",
            "cdn-c.example:8443",
        ],
    );
    assert!(
        created.status.success(),
        "mount create failed: {}",
        String::from_utf8_lossy(&created.stderr)
    );
    let created_json: serde_json::Value = serde_json::from_slice(&created.stdout).unwrap();
    assert_eq!(created_json["root_tag"], "mirror-v1");
    assert_eq!(
        created_json["redirect_allowlist"],
        serde_json::json!(["cdn-b.example:443", "cdn-c.example:8443"])
    );

    let listing = run_mount(&config_path, &["list"]);
    assert!(listing.status.success());
    let mounts: Vec<serde_json::Value> = serde_json::from_slice(&listing.stdout).unwrap();
    assert_eq!(mounts.len(), 2);
    assert_eq!(mounts[0]["name"], "packages");
    assert_eq!(mounts[1]["name"], "mirror");

    let shown = run_mount(&config_path, &["show", "packages"]);
    assert!(shown.status.success());
    let shown: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
    assert_eq!(shown["repository"], "team/packages");

    let config_before_duplicate = fs::read(&config_path).unwrap();
    let duplicate = run_mount(
        &config_path,
        &[
            "create",
            "packages",
            "--prefix",
            "/another",
            "--registry",
            "https://reg-a.example",
            "--repository",
            "team/another",
        ],
    );
    assert_eq!(duplicate.status.code(), Some(73));
    assert_eq!(fs::read(&config_path).unwrap(), config_before_duplicate);

    let updated = run_mount(
        &config_path,
        &[
            "update",
            "mirror",
            "--prefix",
            "/mirror-v2",
            "--registry",
            "https://reg-c.example:5443",
            "--repository",
            "team/new-mirror",
            "--root-tag",
            "mirror-v2",
        ],
    );
    assert!(
        updated.status.success(),
        "mount update failed: {}",
        String::from_utf8_lossy(&updated.stderr)
    );
    let updated: serde_json::Value = serde_json::from_slice(&updated.stdout).unwrap();
    assert_eq!(updated["prefix"], "/mirror-v2");
    assert_eq!(updated["registry"], "https://reg-c.example:5443");
    assert_eq!(updated["repository"], "team/new-mirror");
    assert_eq!(updated["root_tag"], "mirror-v2");
    assert_eq!(updated["redirect_allowlist"].as_array().unwrap().len(), 2);

    let replaced_redirects = run_mount(
        &config_path,
        &[
            "update",
            "mirror",
            "--redirect-allowlist",
            "cdn-d.example:443",
            "--redirect-allowlist",
            "cdn-e.example:8443",
        ],
    );
    assert!(replaced_redirects.status.success());
    let replaced_redirects: serde_json::Value =
        serde_json::from_slice(&replaced_redirects.stdout).unwrap();
    assert_eq!(
        replaced_redirects["redirect_allowlist"],
        serde_json::json!(["cdn-d.example:443", "cdn-e.example:8443"])
    );

    let reset = run_mount(
        &config_path,
        &[
            "update",
            "mirror",
            "--reset-root-tag",
            "--clear-redirect-allowlist",
        ],
    );
    assert!(reset.status.success());
    let reset: serde_json::Value = serde_json::from_slice(&reset.stdout).unwrap();
    assert_eq!(reset["root_tag"], "oci-auto-http-tree-v1");
    assert_eq!(reset["redirect_allowlist"], serde_json::json!([]));

    let config_before_invalid_update = fs::read(&config_path).unwrap();
    let invalid_update = run_mount(
        &config_path,
        &["update", "mirror", "--prefix", "/packages/nested"],
    );
    assert_eq!(invalid_update.status.code(), Some(2));
    assert_eq!(
        fs::read(&config_path).unwrap(),
        config_before_invalid_update
    );
    let empty_update = run_mount(&config_path, &["update", "mirror"]);
    assert_eq!(empty_update.status.code(), Some(2));

    let rendered = fs::read_to_string(&config_path).unwrap();
    for comment in [
        "# Keep the operator header.",
        "# Keep listen.",
        "# Keep the package comment.",
        "# Keep prefix.",
    ] {
        assert!(rendered.contains(comment), "lost TOML comment {comment:?}");
    }
    assert!(rendered.contains("listen = \"127.0.0.1:9999\""));

    let deleted = run_mount(&config_path, &["delete", "mirror"]);
    assert!(deleted.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&deleted.stdout).unwrap(),
        serde_json::json!({"deleted": "mirror"})
    );
    let config_before_last_delete = fs::read(&config_path).unwrap();
    let last_mount = run_mount(&config_path, &["delete", "packages"]);
    assert_eq!(last_mount.status.code(), Some(73));
    assert_eq!(fs::read(&config_path).unwrap(), config_before_last_delete);

    for args in [
        ["show", "missing"].as_slice(),
        ["delete", "missing"].as_slice(),
    ] {
        let missing = run_mount(&config_path, args);
        assert_eq!(missing.status.code(), Some(66));
    }
    let missing_update = run_mount(
        &config_path,
        &["update", "missing", "--repository", "team/missing"],
    );
    assert_eq!(missing_update.status.code(), Some(66));
}

#[test]
fn concurrent_mount_process_updates_preserve_both_changes() {
    let temp = TempDir::new();
    let config_path = temp.write_config(&valid_two_mount_config());
    let lock_path = temp.path.join(".config.toml.oci-auto-http.lock");
    let lock_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .unwrap();
    fs2::FileExt::lock_exclusive(&lock_file).unwrap();

    let mut packages = spawn_mount(
        &config_path,
        &["update", "packages", "--prefix", "/packages-v2"],
    );
    let mut mirror = spawn_mount(
        &config_path,
        &["update", "mirror", "--repository", "team/mirror-v2"],
    );
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(packages.try_wait().unwrap().is_none());
    assert!(mirror.try_wait().unwrap().is_none());
    drop(lock_file);

    let packages = packages.wait_with_output().unwrap();
    let mirror = mirror.wait_with_output().unwrap();
    assert!(
        packages.status.success(),
        "{}",
        String::from_utf8_lossy(&packages.stderr)
    );
    assert!(
        mirror.status.success(),
        "{}",
        String::from_utf8_lossy(&mirror.stderr)
    );
    let config = oci_auto_http::config::Config::load(&config_path).unwrap();
    assert_eq!(
        config
            .mounts
            .iter()
            .find(|mount| mount.name == "packages")
            .unwrap()
            .prefix,
        "/packages-v2"
    );
    assert_eq!(
        config
            .mounts
            .iter()
            .find(|mount| mount.name == "mirror")
            .unwrap()
            .repository,
        "team/mirror-v2"
    );
}

#[test]
fn mount_cli_refuses_non_regular_and_symlink_config_targets() {
    let temp = TempDir::new();
    let directory_path = temp.path.join("config-directory");
    fs::create_dir(&directory_path).unwrap();
    let directory = run_mount(
        &directory_path,
        &[
            "create",
            "packages",
            "--prefix",
            "/packages",
            "--registry",
            "https://registry.example",
            "--repository",
            "team/packages",
        ],
    );
    assert_eq!(directory.status.code(), Some(74));

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let target = temp.write_config(&valid_two_mount_config());
        let symlink_path = temp.path.join("mounts-link.toml");
        symlink(&target, &symlink_path).unwrap();
        let original = fs::read(&target).unwrap();
        let linked = run_mount(
            &symlink_path,
            &["update", "packages", "--prefix", "/changed"],
        );
        assert_eq!(linked.status.code(), Some(74));
        assert!(
            symlink_path
                .symlink_metadata()
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&target).unwrap(), original);
    }
}

#[test]
fn malformed_ambiguous_and_unsafe_configs_fail_before_listening() {
    let temp = TempDir::new();
    let base = valid_two_mount_config();
    let invalid = [
        ("malformed TOML", "version = [".to_owned()),
        (
            "unsupported version",
            base.replace("version = 1", "version = 2"),
        ),
        (
            "overlapping mounts",
            base.replace("prefix = \"/mirror\"", "prefix = \"/packages/child\""),
        ),
        (
            "encoded separator",
            base.replace("prefix = \"/mirror\"", "prefix = \"/safe/%2Fsecret\""),
        ),
        (
            "insecure registry",
            base.replace("https://reg-b.example:5443", "http://reg-b.example:5443"),
        ),
        (
            "normalized dot-segment registry path",
            base.replace(
                "https://reg-b.example:5443",
                "https://reg-b.example:5443/a/..",
            ),
        ),
        (
            "registry userinfo",
            base.replace(
                "https://reg-b.example:5443",
                "https://user:supersecret@reg-b.example:5443",
            ),
        ),
        (
            "unknown option",
            base.replace(
                "repository = \"team/mirror\"",
                "repository = \"team/mirror\"\nunsupported_option = \"do-not-leak\"",
            ),
        ),
    ];

    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    for (case, contents) in invalid {
        let config = temp.write_config(&contents);
        let output = Command::new(binary())
            .args(["serve", "--config"])
            .arg(config)
            .args(["--listen", &address.to_string()])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{case}: {:?}", output.stderr);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("invalid configuration"), "{case}: {stderr}");
        assert!(
            !stderr.contains("supersecret"),
            "{case} leaked credentials: {stderr}"
        );
        assert!(
            !stderr.contains("do-not-leak"),
            "{case} echoed config data: {stderr}"
        );
        assert!(
            !stderr.contains("could not bind HTTP listener"),
            "{case} attempted bind before validation: {stderr}"
        );
    }
}

#[cfg(unix)]
#[test]
fn valid_multiple_mounts_load_without_local_content_state() {
    let temp = TempDir::new();
    let config = temp.write_config(&valid_two_mount_config());
    let address = unused_loopback_address();
    let mut server = spawn_server_with_config(address, &config, Some(&temp.path));

    let health = request_until_ready(&mut server.child, address, "GET", "/healthz");
    assert!(health.starts_with("HTTP/1.1 204"), "{health}");

    let stderr = stop_server(&mut server);
    let startup = stderr
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find(|record| {
            record["fields"]["message"]
                .as_str()
                .unwrap()
                .contains("HTTP listener started")
        })
        .expect("valid multi-mount config starts the listener");
    assert_eq!(startup["fields"]["mounts"], 2);
    let entries: Vec<_> = fs::read_dir(&temp.path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(entries, [std::ffi::OsString::from("config.toml")]);
}

#[test]
fn invalid_listen_address_and_bind_failure_are_actionable() {
    let invalid = Command::new(binary())
        .args(["serve", "--config"])
        .arg(config_file())
        .args(["--listen", "not-a-socket-address"])
        .output()
        .unwrap();
    assert_eq!(invalid.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("invalid value"));

    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let output = Command::new(binary())
        .args(["serve", "--config"])
        .arg(config_file())
        .args(["--listen", &address.to_string()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("could not bind HTTP listener"));
}

#[cfg(unix)]
#[test]
fn serve_runs_a_real_listener_and_shuts_down_on_sigterm() {
    let address = unused_loopback_address();
    let mut server = spawn_server(address);

    let get_health = request_until_ready(&mut server.child, address, "GET", "/healthz");
    assert!(get_health.starts_with("HTTP/1.1 204"), "{get_health}");

    let head_health = request_until_ready(&mut server.child, address, "HEAD", "/healthz");
    assert!(head_health.starts_with("HTTP/1.1 204"), "{head_health}");
    assert!(
        head_health.ends_with("\r\n\r\n"),
        "HEAD returned a body: {head_health}"
    );

    let fallback = request_until_ready(&mut server.child, address, "GET", "/");
    assert!(fallback.starts_with("HTTP/1.1 404"), "{fallback}");
    assert!(fallback.contains("not found"));

    let invalid = request_until_ready(&mut server.child, address, "GET", "/packages/%2Fsecret");
    assert!(invalid.starts_with("HTTP/1.1 400"), "{invalid}");
    let unsupported = request_until_ready(&mut server.child, address, "POST", "/packages/file");
    assert!(unsupported.starts_with("HTTP/1.1 405"), "{unsupported}");
    assert!(unsupported.contains("allow: GET, HEAD"), "{unsupported}");
    let credential = request_until_ready_with_headers(
        &mut server.child,
        address,
        "GET",
        "/packages/file",
        &[("Authorization", "Bearer do-not-log-this")],
    );
    assert!(credential.starts_with("HTTP/1.1 403"), "{credential}");
    assert!(!credential.contains("do-not-log-this"), "{credential}");
    let health_credential = request_until_ready_with_headers(
        &mut server.child,
        address,
        "GET",
        "/healthz",
        &[("Authorization", "Bearer health-secret")],
    );
    assert!(
        health_credential.starts_with("HTTP/1.1 403"),
        "{health_credential}"
    );
    let unsupported_credential = request_until_ready_with_headers(
        &mut server.child,
        address,
        "POST",
        "/healthz",
        &[("Authorization", "Bearer method-secret")],
    );
    assert!(
        unsupported_credential.starts_with("HTTP/1.1 403"),
        "{unsupported_credential}"
    );
    let duplicate_credentials = request_until_ready_with_headers(
        &mut server.child,
        address,
        "GET",
        "/healthz",
        &[
            ("Authorization", "Bearer first-secret"),
            ("Authorization", "Bearer second-secret"),
        ],
    );
    assert!(
        duplicate_credentials.starts_with("HTTP/1.1 400"),
        "{duplicate_credentials}"
    );

    let stderr = stop_server(&mut server);
    assert!(
        stderr.contains("HTTP listener stopped"),
        "server stderr: {stderr}"
    );
    for secret in [
        "do-not-log-this",
        "health-secret",
        "method-secret",
        "first-secret",
        "second-secret",
    ] {
        assert!(
            !stderr.contains(secret),
            "server logs leaked Authorization value {secret}: {stderr}"
        );
    }
}

#[cfg(unix)]
struct RunningServer {
    child: Child,
    stopped: bool,
}

#[cfg(unix)]
impl Drop for RunningServer {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[cfg(unix)]
fn spawn_server(address: SocketAddr) -> RunningServer {
    spawn_server_with_config(address, &config_file(), None)
}

#[cfg(unix)]
fn spawn_server_with_config(
    address: SocketAddr,
    config: &Path,
    working_directory: Option<&Path>,
) -> RunningServer {
    let mut command = Command::new(binary());
    command
        .args(["--log-format", "json", "serve", "--config"])
        .arg(config)
        .args(["--listen", &address.to_string()])
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(directory) = working_directory {
        command
            .current_dir(directory)
            .env("HOME", directory)
            .env("XDG_CACHE_HOME", directory);
    }
    let child = command.spawn().unwrap();
    RunningServer {
        child,
        stopped: false,
    }
}

#[cfg(unix)]
fn stop_server(server: &mut RunningServer) -> String {
    let signal = Command::new("kill")
        .args(["-TERM", &server.child.id().to_string()])
        .status()
        .unwrap();
    assert!(signal.success(), "could not send SIGTERM to test server");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if server.child.try_wait().unwrap().is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "server did not stop after SIGTERM"
        );
        thread::sleep(Duration::from_millis(20));
    }

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
    assert!(status.success());
    stderr
}

#[cfg(unix)]
fn request_until_ready(child: &mut Child, address: SocketAddr, method: &str, path: &str) -> String {
    request_until_ready_with_headers(child, address, method, path, &[])
}

#[cfg(unix)]
fn request_until_ready_with_headers(
    child: &mut Child,
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                pipe.read_to_string(&mut stderr).unwrap();
            }
            panic!("server exited before serving request: {status}; stderr: {stderr}");
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(100)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            write!(stream, "{method} {path} HTTP/1.1\r\nHost: localhost\r\n").unwrap();
            for (name, value) in headers {
                write!(stream, "{name}: {value}\r\n").unwrap();
            }
            stream.write_all(b"Connection: close\r\n\r\n").unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            return response;
        }
        assert!(Instant::now() < deadline, "server did not start listening");
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(unix)]
fn unused_loopback_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}
