//! End-to-end handshake test: launch the real `relay` binary, connect over TCP
//! using the same MQTT v5 codec, send a genuine CONNECT, and assert the broker
//! replies with a successful CONNACK.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::v5::{Codec, Connect, ConnectAckReason, Packet};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const SECRET: &str = "e2e-handshake-secret";
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

/// Kills the spawned broker when the test ends (even on panic).
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn test_connect() -> Connect {
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
        client_id: "relay-test-client".into(),
        username: None,
        password: Some(Bytes::from(jwt("relay-test-client", &["*"]))),
        cert: None,
    }
}

#[tokio::test]
async fn connect_gets_connack() {
    let tcp_port = 21883;

    let cfg = std::env::temp_dir().join("relay-handshake-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\nws_addr = \"127.0.0.1:28083\"\n\
             \n\
             [auth]\n\
             jwt_secret = \"{SECRET}\"\n\
             \n\
             [[auth.acl]]\n\
             role = \"*\"\n\
             publish = [\"#\"]\n\
             subscribe = [\"#\"]\n"
        ),
    )
    .expect("write test config");

    let child = Command::new(env!("CARGO_BIN_EXE_relay"))
        .env("RELAY_CONFIG", &cfg)
        .env("RUST_LOG", "off")
        .spawn()
        .expect("spawn relay binary");
    let _guard = ChildGuard(child);

    // Wait for the listener (retry connect for up to 5s).
    let addr = format!("127.0.0.1:{tcp_port}");
    let deadline = Instant::now() + Duration::from_secs(5);
    let stream = loop {
        match TcpStream::connect(&addr).await {
            Ok(s) => break s,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("broker never accepted connections: {e}"),
        }
    };

    let mut framed = Framed::new(stream, Codec::new(256 * 1024, 0));
    framed
        .send(Packet::from(test_connect()))
        .await
        .expect("send CONNECT");

    let (reply, _) = timeout(Duration::from_secs(5), framed.next())
        .await
        .expect("timed out waiting for CONNACK")
        .expect("connection closed without reply")
        .expect("decode reply");

    match reply {
        Packet::ConnectAck(ack) => {
            assert_eq!(
                ack.reason_code,
                ConnectAckReason::Success,
                "expected success CONNACK"
            );
        }
        other => panic!("expected CONNACK, got {other:?}"),
    }
}
