use std::{net::TcpListener, path::PathBuf, process::Command};

#[cfg(unix)]
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    process::{Child, Stdio},
    thread,
    time::{Duration, Instant},
};

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
        .args(["artifact", "create", "sample"])
        .output()
        .unwrap();
    assert_eq!(reserved.status.code(), Some(69));
    let diagnostic = String::from_utf8_lossy(&reserved.stderr);
    assert!(
        diagnostic.contains("artifact create is not available yet"),
        "unexpected reserved-command diagnostic: {diagnostic}"
    );
    assert!(
        diagnostic.contains("no registry request was made"),
        "{diagnostic}"
    );

    let filtered = Command::new(binary())
        .args(["artifact", "create", "sample"])
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    assert_eq!(filtered.status.code(), Some(69));
    assert!(
        String::from_utf8_lossy(&filtered.stderr)
            .contains("error: artifact create is not available yet")
    );

    let json = Command::new(binary())
        .args(["--log-format", "json", "artifact", "create", "sample"])
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
}

#[test]
fn invalid_listen_address_and_bind_failure_are_actionable() {
    let invalid = Command::new(binary())
        .args(["serve", "--listen", "not-a-socket-address"])
        .output()
        .unwrap();
    assert_eq!(invalid.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("invalid value"));

    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let output = Command::new(binary())
        .args(["serve", "--listen", &address.to_string()])
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
    assert!(fallback.contains("mounted storage is not configured"));

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
    let child = Command::new(binary())
        .args(["serve", "--listen", &address.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    RunningServer {
        child,
        stopped: false,
    }
}

#[cfg(unix)]
fn request_until_ready(child: &mut Child, address: SocketAddr, method: &str, path: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited before serving request: {status}");
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
