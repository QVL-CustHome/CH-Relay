//! End-to-end dashboard test (V2): the embedded HTTP monitoring endpoint
//! reflects live broker state. With one MQTT subscriber connected, `/stats`
//! reports it, and `/` serves the HTML page.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::v5::{Codec, Connect, Packet, Subscribe, SubscriptionOptions};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const TCP_PORT: u16 = 21903;
const WS_PORT: u16 = 28103;
const HTTP_PORT: u16 = 21904;
const SECRET: &str = "e2e-dashboard-secret";
const EXP: i64 = 4_102_444_800;

fn jwt(sub: &str, roles: &[&str]) -> String {
    let claims = serde_json::json!({ "sub": sub, "roles": roles, "exp": EXP });
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .expect("encode jwt")
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

type Client = Framed<TcpStream, Codec>;

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

async fn connect(addr: &str, client_id: &str, password: &str) -> Client {
    let deadline = Instant::now() + Duration::from_secs(5);
    let stream = loop {
        match TcpStream::connect(addr).await {
            Ok(s) => break s,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("broker never accepted connections: {e}"),
        }
    };
    let mut framed = Framed::new(stream, Codec::new(256 * 1024, 0));
    framed
        .send(Packet::from(connect_packet(client_id, password)))
        .await
        .expect("send CONNECT");
    match timeout(Duration::from_secs(5), framed.next()).await {
        Ok(Some(Ok((Packet::ConnectAck(_), _)))) => framed,
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

/// Issue a GET and return (status line, body).
async fn http_get(addr: &str, path: &str) -> (String, String) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut socket = loop {
        match TcpStream::connect(addr).await {
            Ok(s) => break s,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("dashboard never accepted connections: {e}"),
        }
    };
    let request = format!("GET {path} HTTP/1.1\r\nHost: relay\r\nConnection: close\r\n\r\n");
    socket
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    let mut raw = String::new();
    socket
        .read_to_string(&mut raw)
        .await
        .expect("read response");
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((raw.as_str(), ""));
    let status = head.lines().next().unwrap_or("").to_string();
    (status, body.to_string())
}

#[tokio::test]
async fn dashboard_reports_live_state() {
    let data_dir = std::env::temp_dir().join("relay-dashboard-test");
    let _ = std::fs::remove_dir_all(&data_dir);

    let cfg = std::env::temp_dir().join("relay-dashboard-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{TCP_PORT}\"\nws_addr = \"127.0.0.1:{WS_PORT}\"\n\
             http_addr = \"127.0.0.1:{HTTP_PORT}\"\ndata_dir = '{}'\n\
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

    let child = Command::new(env!("CARGO_BIN_EXE_relay"))
        .env("RELAY_CONFIG", &cfg)
        .env("RUST_LOG", "off")
        .spawn()
        .expect("spawn relay binary");
    let _guard = ChildGuard(child);

    let mqtt_addr = format!("127.0.0.1:{TCP_PORT}");
    let http_addr = format!("127.0.0.1:{HTTP_PORT}");

    // One subscriber online with one subscription.
    let token = jwt("watcher", &["*"]);
    let mut sub = connect(&mqtt_addr, "watcher", &token).await;
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1.try_into().unwrap(),
        id: None,
        user_properties: Vec::new(),
        topic_filters: vec![("sensors/#".into(), SubscriptionOptions::default())],
    }))
    .await
    .expect("send SUBSCRIBE");
    match timeout(Duration::from_secs(5), sub.next()).await {
        Ok(Some(Ok((Packet::SubscribeAck(_), _)))) => {}
        other => panic!("expected SUBACK, got {other:?}"),
    }

    // The JSON endpoint reflects it.
    let (status, body) = http_get(&http_addr, "/stats").await;
    assert!(status.contains("200"), "stats status: {status}");
    assert!(
        body.contains("\"clients_online\":1"),
        "expected one online client in stats: {body}"
    );
    assert!(
        body.contains("\"subscriptions\":1"),
        "expected one subscription in stats: {body}"
    );

    // The dashboard page is served.
    let (status, body) = http_get(&http_addr, "/").await;
    assert!(status.contains("200"), "index status: {status}");
    assert!(body.contains("Relay"), "index page should mention Relay");

    // Unknown paths 404.
    let (status, _) = http_get(&http_addr, "/nope").await;
    assert!(status.contains("404"), "unknown path should 404: {status}");

    // Keep the subscriber alive until the assertions ran.
    drop(sub);
}
