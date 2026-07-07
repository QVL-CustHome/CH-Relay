//! End-to-end pub/sub test: one subscriber, one publisher, both talking to the
//! real `relay` binary. Asserts a published message is routed to the subscriber.

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

const TOPIC: &str = "test/topic";
const SECRET: &str = "e2e-pubsub-secret";
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

type Client = Framed<TcpStream, Codec>;

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

#[tokio::test]
async fn published_message_reaches_subscriber() {
    let tcp_port = 21884;

    let cfg = std::env::temp_dir().join("relay-pubsub-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\nws_addr = \"127.0.0.1:28084\"\n\
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

    // Subscriber connects and subscribes, then waits for its SUBACK so we know
    // the subscription is registered before anyone publishes.
    let mut subscriber = connect(&addr, "subscriber").await;
    subscriber
        .send(Packet::Subscribe(Subscribe {
            packet_id: 1.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(TOPIC.into(), SubscriptionOptions::default())],
        }))
        .await
        .expect("send SUBSCRIBE");
    match next_packet(&mut subscriber).await {
        Packet::SubscribeAck(_) => {}
        other => panic!("expected SUBACK, got {other:?}"),
    }

    // Publisher connects and publishes.
    let mut publisher = connect(&addr, "publisher").await;
    publisher
        .send(Packet::from(publish_packet(TOPIC, b"hello")))
        .await
        .expect("send PUBLISH");

    // Subscriber should receive the forwarded message.
    match next_packet(&mut subscriber).await {
        Packet::Publish(p) => {
            assert_eq!(&*p.topic, TOPIC, "topic mismatch");
            assert_eq!(p.payload.as_ref(), b"hello".as_ref(), "payload mismatch");
        }
        other => panic!("expected forwarded PUBLISH, got {other:?}"),
    }
}
