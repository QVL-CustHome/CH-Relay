//! End-to-end shared-subscription test: two workers join `$share/workers/jobs`,
//! two messages are published, and each worker receives exactly one (round-robin),
//! NOT both — that's the difference between a shared subscription (queue) and a
//! normal fan-out subscription.

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

const SHARED_FILTER: &str = "$share/workers/jobs";
const TOPIC: &str = "jobs";
const SECRET: &str = "e2e-shared-secret";
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

fn publish_packet(topic: &str, payload: &'static [u8]) -> Publish {
    Publish {
        dup: false,
        retain: false,
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

/// Subscribe to the shared filter and wait for the SUBACK (so ordering of group
/// membership is deterministic).
async fn subscribe_shared(client: &mut Client, packet_id: u16) {
    client
        .send(Packet::Subscribe(Subscribe {
            packet_id: packet_id.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(SHARED_FILTER.into(), SubscriptionOptions::default())],
        }))
        .await
        .expect("send SUBSCRIBE");
    match next_packet(client).await {
        Packet::SubscribeAck(_) => {}
        other => panic!("expected SUBACK, got {other:?}"),
    }
}

async fn next_packet(client: &mut Client) -> Packet {
    timeout(Duration::from_secs(5), client.next())
        .await
        .expect("timed out waiting for a packet")
        .expect("connection closed unexpectedly")
        .expect("decode error")
        .0
}

/// Read the payload of the next PUBLISH, or `None` if nothing arrives shortly.
async fn try_next_payload(client: &mut Client) -> Option<String> {
    match timeout(Duration::from_millis(400), client.next()).await {
        Ok(Some(Ok((Packet::Publish(p), _)))) => {
            Some(String::from_utf8_lossy(&p.payload).into_owned())
        }
        Ok(other) => panic!("unexpected frame: {other:?}"),
        Err(_) => None, // timed out: no message pending
    }
}

#[tokio::test]
async fn shared_subscription_distributes_one_message_per_worker() {
    let tcp_port = 21885;

    let cfg = std::env::temp_dir().join("relay-shared-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\nws_addr = \"127.0.0.1:28085\"\n\
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

    // Two workers join the same share group, in a deterministic order.
    let mut worker1 = connect(&addr, "worker1").await;
    subscribe_shared(&mut worker1, 1).await;
    let mut worker2 = connect(&addr, "worker2").await;
    subscribe_shared(&mut worker2, 1).await;

    // Publisher sends two messages.
    let mut publisher = connect(&addr, "publisher").await;
    publisher
        .send(Packet::from(publish_packet(TOPIC, b"m1")))
        .await
        .expect("publish m1");
    publisher
        .send(Packet::from(publish_packet(TOPIC, b"m2")))
        .await
        .expect("publish m2");

    // Each worker should get exactly ONE message (round-robin), not both.
    let w1_first = try_next_payload(&mut worker1).await;
    let w2_first = try_next_payload(&mut worker2).await;

    assert_eq!(
        w1_first.as_deref(),
        Some("m1"),
        "worker1 should get the 1st message"
    );
    assert_eq!(
        w2_first.as_deref(),
        Some("m2"),
        "worker2 should get the 2nd message"
    );

    // And neither should have a second message waiting (proves it's a queue,
    // not a fan-out where both would receive both messages).
    assert_eq!(
        try_next_payload(&mut worker1).await,
        None,
        "worker1 got an extra message"
    );
    assert_eq!(
        try_next_payload(&mut worker2).await,
        None,
        "worker2 got an extra message"
    );
}
