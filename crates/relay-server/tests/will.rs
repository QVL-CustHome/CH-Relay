//! End-to-end Will (Last Will & Testament) test against the real `relay` binary.
//!
//! A client registers a Will in CONNECT. When its connection drops *abnormally*
//! (here: the socket is dropped without a DISCONNECT), the broker must publish
//! the Will to its subscribers. When the client disconnects *cleanly* (a normal
//! DISCONNECT), the Will must be discarded.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::v5::{
    Codec, Connect, Disconnect, DisconnectReasonCode, LastWill, Packet, QoS, Subscribe,
    SubscriptionOptions,
};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const WILL_TOPIC: &str = "clients/agent-1/status";
const SECRET: &str = "e2e-will-secret";
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

fn connect_packet(client_id: &str, will: Option<LastWill>, token: &str) -> Connect {
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
        last_will: will,
        client_id: client_id.into(),
        username: None,
        password: Some(Bytes::from(token.to_string())),
        cert: None,
    }
}

fn will(topic: &str, payload: &'static [u8]) -> LastWill {
    LastWill {
        qos: QoS::AtMostOnce,
        retain: false,
        topic: topic.into(),
        message: Bytes::from_static(payload),
        will_delay_interval_sec: None,
        correlation_data: None,
        message_expiry_interval: None,
        content_type: None,
        user_properties: Vec::new(),
        is_utf8_payload: None,
        response_topic: None,
    }
}

async fn connect(addr: &str, client_id: &str, will: Option<LastWill>) -> Client {
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
        .send(Packet::from(connect_packet(
            client_id,
            will,
            &jwt(client_id, &["*"]),
        )))
        .await
        .expect("send CONNECT");
    match next_packet(&mut framed).await {
        Packet::ConnectAck(_) => framed,
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

async fn next_packet(framed: &mut Client) -> Packet {
    timeout(Duration::from_secs(5), framed.next())
        .await
        .expect("timed out waiting for a packet")
        .expect("connection closed unexpectedly")
        .expect("decode error")
        .0
}

async fn subscribe(client: &mut Client, topic: &str) {
    client
        .send(Packet::Subscribe(Subscribe {
            packet_id: 1.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(topic.into(), SubscriptionOptions::default())],
        }))
        .await
        .expect("send SUBSCRIBE");
    match next_packet(client).await {
        Packet::SubscribeAck(_) => {}
        other => panic!("expected SUBACK, got {other:?}"),
    }
}

async fn try_next_payload(client: &mut Client) -> Option<String> {
    match timeout(Duration::from_millis(600), client.next()).await {
        Ok(Some(Ok((Packet::Publish(p), _)))) => {
            Some(String::from_utf8_lossy(&p.payload).into_owned())
        }
        Ok(other) => panic!("unexpected frame: {other:?}"),
        Err(_) => None,
    }
}

fn spawn_broker(tcp_port: u16, ws_port: u16, tag: &str) -> (ChildGuard, String) {
    let cfg = std::env::temp_dir().join(format!("relay-will-{tag}.toml"));
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\nws_addr = \"127.0.0.1:{ws_port}\"\n\
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
    (ChildGuard(child), format!("127.0.0.1:{tcp_port}"))
}

#[tokio::test]
async fn abnormal_disconnect_publishes_the_will() {
    let (_guard, addr) = spawn_broker(21888, 28088, "abnormal");

    // Subscriber watches the Will topic.
    let mut watcher = connect(&addr, "watcher", None).await;
    subscribe(&mut watcher, WILL_TOPIC).await;

    // Agent connects with a Will, then its socket is dropped (no DISCONNECT).
    let agent = connect(&addr, "agent-1", Some(will(WILL_TOPIC, b"offline"))).await;
    drop(agent); // abnormal termination

    // The broker must publish the Will to the watcher.
    assert_eq!(
        try_next_payload(&mut watcher).await.as_deref(),
        Some("offline"),
        "watcher should receive the Will after the agent's abnormal disconnect"
    );
}

#[tokio::test]
async fn clean_disconnect_discards_the_will() {
    let (_guard, addr) = spawn_broker(21889, 28089, "clean");

    let mut watcher = connect(&addr, "watcher", None).await;
    subscribe(&mut watcher, WILL_TOPIC).await;

    // Agent connects with a Will, then disconnects cleanly.
    let mut agent = connect(&addr, "agent-1", Some(will(WILL_TOPIC, b"offline"))).await;
    agent
        .send(Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::NormalDisconnection,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Vec::new(),
        }))
        .await
        .expect("send DISCONNECT");
    drop(agent);

    // No Will should be published after a clean disconnect.
    assert_eq!(
        try_next_payload(&mut watcher).await,
        None,
        "a clean DISCONNECT must discard the Will"
    );
}
