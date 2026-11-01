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
fn invalid_arguments_and_reserved_commands_fail_actionably() {
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

    let reserved = Command::new(binary())
        .args(["mount", "list"])
        .output()
        .unwrap();
    assert_eq!(reserved.status.code(), Some(69));
    let diagnostic = String::from_utf8_lossy(&reserved.stderr);
    assert!(
        diagnostic.contains("mount list is not available yet"),
        "unexpected reserved-command diagnostic: {diagnostic}"
    );
    assert!(
        diagnostic.contains("no registry request was made"),
        "{diagnostic}"
    );

    let filtered = Command::new(binary())
        .args(["mount", "list"])
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    assert_eq!(filtered.status.code(), Some(69));
    assert!(String::from_utf8_lossy(&filtered.stderr).contains("mount list is not available yet"));

    let json = Command::new(binary())
        .args(["--log-format", "json", "mount", "list"])
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    assert_eq!(json.status.code(), Some(69));
    let stderr = String::from_utf8(json.stderr).unwrap();
    let lines: Vec<_> = stderr.lines().collect();
    assert_eq!(lines.len(), 1, "JSON mode must emit one record: {stderr}");
    let record: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(record["level"], "ERROR");
    assert_eq!(record["fields"]["exit_code"], 69);
    assert!(
        record["fields"]["error"]
            .as_str()
            .unwrap()
            .contains("no registry request was made")
    );

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
    assert!(fallback.starts_with("HTTP/1.1 503"), "{fallback}");
    assert!(fallback.contains("mounted storage serving is not implemented"));

    let stderr = stop_server(&mut server);
    assert!(
        stderr.contains("HTTP listener stopped"),
        "server stderr: {stderr}"
    );
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
            write!(
                stream,
                "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
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
