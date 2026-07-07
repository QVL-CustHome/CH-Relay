use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;
use rmqtt_codec::v5::{Codec, Connect, Packet};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const SECRET: &str = "scrum170-secret";

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn base_config(tcp_port: u16, ws_port: u16, http_line: &str, stem: &str) -> std::path::PathBuf {
    let data_dir = std::env::temp_dir().join(format!("relay-{stem}-data"));
    let _ = std::fs::remove_dir_all(&data_dir);
    let cfg = std::env::temp_dir().join(format!("relay-{stem}.toml"));
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\nws_addr = \"127.0.0.1:{ws_port}\"\n\
             {http_line}data_dir = '{}'\n\
             \n\
             [auth]\n\
             jwt_secret = \"{SECRET}\"\n\
             \n\
             [[auth.acl]]\n\
             role = \"*\"\n\
             publish = [\"sensors/#\"]\n\
             subscribe = [\"sensors/#\"]\n",
            data_dir.display()
        ),
    )
    .expect("write test config");
    cfg
}

fn spawn_relay(
    cfg: &std::path::Path,
    allow_external: Option<&str>,
) -> (ChildGuard, Receiver<String>) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_relay"));
    command
        .env("RELAY_CONFIG", cfg)
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(value) = allow_external {
        command.env("RELAY_HTTP_ALLOW_EXTERNAL", value);
    }
    let mut child = command.spawn().expect("spawn relay binary");

    let (tx, rx) = channel();
    let stderr = child.stderr.take().expect("capture stderr");
    let tx_err = tx.clone();
    thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            let _ = tx_err.send(line);
        }
    });
    let stdout = child.stdout.take().expect("capture stdout");
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            let _ = tx.send(line);
        }
    });

    (ChildGuard(child), rx)
}

fn collect_logs(rx: &Receiver<String>, window: Duration) -> Vec<String> {
    let mut lines = Vec::new();
    let deadline = std::time::Instant::now() + window;
    while std::time::Instant::now() < deadline {
        if let Ok(line) = rx.recv_timeout(Duration::from_millis(100)) {
            lines.push(line)
        }
    }
    lines
}

fn connect_packet(client_id: &str, password: &str) -> Connect {
    Connect {
        clean_start: true,
        keep_alive: 0,
        session_expiry_interval_secs: 0,
        auth_method: None,
        auth_data: None,
        request_problem_info: true,
        request_response_info: false,
        receive_max: None,
        topic_alias_max: 0,
        user_properties: Vec::new(),
        max_packet_size: None,
        last_will: None,
        client_id: client_id.into(),
        username: None,
        password: Some(Bytes::from(password.to_string())),
        cert: None,
    }
}

async fn wait_broker_ready(tcp_port: u16) {
    let addr = format!("127.0.0.1:{tcp_port}");
    let deadline = Instant::now() + Duration::from_secs(10);
    let stream = loop {
        match TcpStream::connect(&addr).await {
            Ok(s) => break s,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("broker never accepted MQTT connections on {addr}: {e}"),
        }
    };
    let mut framed = Framed::new(stream, Codec::new(256 * 1024, 0));
    framed
        .send(Packet::from(connect_packet("scrum170-probe", "ignored")))
        .await
        .expect("send CONNECT");
    let _ = timeout(Duration::from_secs(5), framed.next()).await;
}

async fn http_get(addr: &str, path: &str) -> Option<(String, String)> {
    let mut socket = match timeout(Duration::from_secs(2), TcpStream::connect(addr)).await {
        Ok(Ok(s)) => s,
        _ => return None,
    };
    let request = format!("GET {path} HTTP/1.1\r\nHost: relay\r\nConnection: close\r\n\r\n");
    socket.write_all(request.as_bytes()).await.ok()?;
    let mut raw = String::new();
    timeout(Duration::from_secs(2), socket.read_to_string(&mut raw))
        .await
        .ok()?
        .ok()?;
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((raw.as_str(), ""));
    let status = head.lines().next().unwrap_or("").to_string();
    Some((status, body.to_string()))
}

async fn http_listening(addr: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if timeout(Duration::from_millis(300), TcpStream::connect(addr))
            .await
            .is_ok_and(|r| r.is_ok())
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn ac1_scenario1_no_http_addr_dashboard_disabled() {
    let tcp_port = 21910;
    let ws_port = 28110;
    let probe_http_port = 21911;
    let cfg = base_config(tcp_port, ws_port, "", "scrum170-s1");
    let (_guard, _rx) = spawn_relay(&cfg, None);

    wait_broker_ready(tcp_port).await;

    let probe = format!("127.0.0.1:{probe_http_port}");
    assert!(
        !http_listening(&probe).await,
        "no http_addr configured: no HTTP listener expected"
    );
}

#[tokio::test]
async fn ac1_scenario2_loopback_dashboard_accessible() {
    let tcp_port = 21912;
    let ws_port = 28112;
    let http_port = 21913;
    let http_line = format!("http_addr = \"127.0.0.1:{http_port}\"\n");
    let cfg = base_config(tcp_port, ws_port, &http_line, "scrum170-s2");
    let (_guard, _rx) = spawn_relay(&cfg, None);

    wait_broker_ready(tcp_port).await;
    let http_addr = format!("127.0.0.1:{http_port}");

    assert!(
        http_listening(&http_addr).await,
        "loopback dashboard must listen"
    );

    let root = http_get(&http_addr, "/").await.expect("GET / must respond");
    assert!(root.0.contains("200"), "GET / status: {}", root.0);

    let stats = http_get(&http_addr, "/stats")
        .await
        .expect("GET /stats must respond");
    assert!(stats.0.contains("200"), "GET /stats status: {}", stats.0);
}

#[tokio::test]
async fn ac1_scenario3_external_without_flag_failsafe_no_listen_and_warns() {
    let tcp_port = 21914;
    let ws_port = 28114;
    let http_port = 21915;
    let http_line = format!("http_addr = \"0.0.0.0:{http_port}\"\n");
    let cfg = base_config(tcp_port, ws_port, &http_line, "scrum170-s3");
    let (guard, rx) = spawn_relay(&cfg, None);

    let booted = std::panic::AssertUnwindSafe(wait_broker_ready(tcp_port));
    let _ = timeout(Duration::from_secs(8), booted).await;

    let loopback = format!("127.0.0.1:{http_port}");
    let external = format!("0.0.0.0:{http_port}");
    let listens_loopback = http_listening(&loopback).await;
    let listens_external = http_listening(&external).await;

    let logs = collect_logs(&rx, Duration::from_secs(2));
    let joined = logs.join("\n").to_lowercase();
    drop(guard);

    assert!(
        !listens_loopback && !listens_external,
        "fail-safe: external bind without flag must NOT start the dashboard (loopback={listens_loopback}, external={listens_external})"
    );
    assert!(
        joined.contains("warn") && joined.contains("dashboard") && joined.contains("relay_http_allow_external"),
        "a security warning about the refused external bind, mentioning how to allow it, must be logged, got:\n{}",
        logs.join("\n")
    );
}

#[tokio::test]
async fn ac1_scenario4_external_with_flag_binds_and_warns() {
    let tcp_port = 21916;
    let ws_port = 28116;
    let http_port = 21917;
    let http_line = format!("http_addr = \"0.0.0.0:{http_port}\"\n");
    let cfg = base_config(tcp_port, ws_port, &http_line, "scrum170-s4");
    let (guard, rx) = spawn_relay(&cfg, Some("true"));

    wait_broker_ready(tcp_port).await;

    let probe = format!("127.0.0.1:{http_port}");
    let listens = http_listening(&probe).await;

    let logs = collect_logs(&rx, Duration::from_secs(2));
    let joined = logs.join("\n").to_lowercase();
    drop(guard);

    assert!(
        listens,
        "with RELAY_HTTP_ALLOW_EXTERNAL=true the external bind must start the dashboard"
    );
    assert!(
        joined.contains("warn")
            && joined.contains("exposed")
            && joined.contains("without authentication"),
        "a security warning must be logged when exposing the dashboard externally, got:\n{}",
        logs.join("\n")
    );
}

#[tokio::test]
async fn ac2_exposure_decision_traced_by_flag_effect() {
    let tcp_port = 21918;
    let ws_port = 28118;
    let http_port = 21919;
    let http_line = format!("http_addr = \"0.0.0.0:{http_port}\"\n");

    let cfg_default = base_config(tcp_port, ws_port, &http_line, "scrum170-ac2-default");
    let (guard_default, _) = spawn_relay(&cfg_default, None);
    let _ = timeout(Duration::from_secs(8), wait_broker_ready(tcp_port)).await;
    let probe = format!("127.0.0.1:{http_port}");
    let exposed_by_default = http_listening(&probe).await;
    drop(guard_default);
    sleep(Duration::from_millis(500)).await;

    let cfg_flag = base_config(tcp_port, ws_port, &http_line, "scrum170-ac2-flag");
    let (guard_flag, _) = spawn_relay(&cfg_flag, Some("true"));
    wait_broker_ready(tcp_port).await;
    let exposed_with_flag = http_listening(&probe).await;
    drop(guard_flag);

    assert!(
        !exposed_by_default,
        "AC2: default (no flag) must not expose the dashboard; the flag is the traceable decision"
    );
    assert!(
        exposed_with_flag,
        "AC2: setting RELAY_HTTP_ALLOW_EXTERNAL=true is the explicit, traceable exposure decision and must take effect"
    );
}
