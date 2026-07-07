//! End-to-end retained-message test against the real `relay` binary.
//!
//! A publisher sends a *retained* message to a topic **before** anyone is
//! subscribed. A subscriber that connects afterwards must immediately receive
//! that message, flagged as retained. Then a zero-length retained publish must
//! clear it, so a second late subscriber gets nothing.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, Connect, Packet, PublishProperties, QoS, Subscribe, SubscriptionOptions,
};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const TOPIC: &str = "devices/thermostat/state";
const SECRET: &str = "e2e-retained-secret";
const EXP: i64 = 4_102_444_800;

fn jwt(sub: &str, roles: &[&str]) -> String {
    let claims = serde_json::json!({ "sub": sub, "roles": roles, "exp": EXP, "iss": "ch-api-authenticator", "aud": "ch-relay" });
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

fn connect_packet(client_id: &str, token: &str) -> Connect {
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
        password: Some(Bytes::from(token.to_string())),
        cert: None,
    }
}

fn retained_publish(topic: &str, payload: &'static [u8]) -> Publish {
    Publish {
        dup: false,
        retain: true,
        qos: QoS::AtMostOnce,
        topic: topic.into(),
        packet_id: None,
        payload: Bytes::from_static(payload),
        properties: Some(PublishProperties::default()),
    }
}

async fn connect(addr: &str, client_id: &str) -> Client {
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

/// PINGREQ→PINGRESP round-trip. Because a single connection is processed in
/// order, this guarantees everything sent before the PINGREQ has been handled.
async fn ping(client: &mut Client) {
    client
        .send(Packet::PingRequest)
        .await
        .expect("send PINGREQ");
    match next_packet(client).await {
        Packet::PingResponse => {}
        other => panic!("expected PINGRESP, got {other:?}"),
    }
}

async fn subscribe(client: &mut Client, topic: &str, packet_id: u16) {
    client
        .send(Packet::Subscribe(Subscribe {
            packet_id: packet_id.try_into().unwrap(),
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

/// Read the next PUBLISH payload, or `None` if nothing arrives shortly.
async fn try_next_payload(client: &mut Client) -> Option<(String, bool)> {
    match timeout(Duration::from_millis(400), client.next()).await {
        Ok(Some(Ok((Packet::Publish(p), _)))) => {
            Some((String::from_utf8_lossy(&p.payload).into_owned(), p.retain))
        }
        Ok(other) => panic!("unexpected frame: {other:?}"),
        Err(_) => None,
    }
}

#[tokio::test]
async fn retained_message_is_replayed_to_late_subscriber_and_cleared() {
    let tcp_port = 21887;

    let cfg = std::env::temp_dir().join("relay-retained-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\nws_addr = \"127.0.0.1:28087\"\n\
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

    let addr = format!("127.0.0.1:{tcp_port}");

    // Publisher retains a value with NO subscriber present yet.
    let mut publisher = connect(&addr, "publisher").await;
    publisher
        .send(Packet::from(retained_publish(TOPIC, b"22.5C")))
        .await
        .expect("send retained PUBLISH");
    ping(&mut publisher).await; // ensure the broker stored it

    // A late subscriber must immediately get the retained message, flagged retained.
    let mut late = connect(&addr, "late").await;
    subscribe(&mut late, TOPIC, 1).await;
    let (payload, retain) = try_next_payload(&mut late)
        .await
        .expect("late subscriber should receive the retained message");
    assert_eq!(payload, "22.5C", "retained payload mismatch");
    assert!(
        retain,
        "replayed retained message must have the retain flag set"
    );

    // Clearing: a zero-length retained publish removes it.
    publisher
        .send(Packet::from(retained_publish(TOPIC, b"")))
        .await
        .expect("send clearing retained PUBLISH");
    ping(&mut publisher).await;

    // A second late subscriber should now get nothing on subscribe.
    let mut later = connect(&addr, "later").await;
    subscribe(&mut later, TOPIC, 1).await;
    assert_eq!(
        try_next_payload(&mut later).await,
        None,
        "retained message should have been cleared by the empty payload"
    );
}
