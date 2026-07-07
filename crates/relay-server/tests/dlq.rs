//! End-to-end dead-letter test (V2): a QoS 1 message that a connected
//! subscriber never acknowledges is, after the configured number of attempts,
//! republished on `$dlq/{client}/{topic}` where a dead-letter consumer
//! (subscribed to `$dlq/#`) picks it up.

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

const TOPIC: &str = "orders/42/created";
const CLIENT: &str = "order-consumer";
const TCP_PORT: u16 = 21899;
const WS_PORT: u16 = 28099;
const SECRET: &str = "e2e-dlq-secret";
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

fn publish_qos1(topic: &str, payload: &'static [u8], packet_id: u16) -> Publish {
    Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: topic.into(),
        packet_id: Some(packet_id.try_into().unwrap()),
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
    timeout(Duration::from_secs(8), framed.next())
        .await
        .expect("timed out waiting for a packet")
        .expect("connection closed unexpectedly")
        .expect("decode error")
        .0
}

async fn subscribe(client: &mut Client, topic: &str, packet_id: u16, qos: QoS) {
    let options = SubscriptionOptions {
        qos,
        ..Default::default()
    };
    client
        .send(Packet::Subscribe(Subscribe {
            packet_id: packet_id.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(topic.into(), options)],
        }))
        .await
        .expect("send SUBSCRIBE");
    match next_packet(client).await {
        Packet::SubscribeAck(_) => {}
        other => panic!("expected SUBACK, got {other:?}"),
    }
}

#[tokio::test]
async fn undelivered_qos1_message_is_dead_lettered() {
    let cfg = std::env::temp_dir().join("relay-dlq-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{TCP_PORT}\"\nws_addr = \"127.0.0.1:{WS_PORT}\"\n\
             max_delivery_attempts = 2\nretry_base_secs = 1\n\
             \n\
             [auth]\n\
             jwt_secret = \"{SECRET}\"\n\
             \n\
             [[auth.acl]]\n\
             role = \"*\"\n\
             publish = [\"orders/#\", \"$dlq/#\"]\n\
             subscribe = [\"orders/#\", \"$dlq/#\"]\n"
        ),
    )
    .expect("write test config");

    let child = Command::new(env!("CARGO_BIN_EXE_relay"))
        .env("RELAY_CONFIG", &cfg)
        .env("RUST_LOG", "off")
        .spawn()
        .expect("spawn relay binary");
    let _guard = ChildGuard(child);

    let addr = format!("127.0.0.1:{TCP_PORT}");

    // A dead-letter consumer watches the whole `$dlq/` namespace.
    let mut dlq_watcher = connect(&addr, "dlq-ops").await;
    subscribe(&mut dlq_watcher, "$dlq/#", 1, QoS::AtMostOnce).await;

    // The real consumer subscribes at QoS 1 but will never acknowledge anything.
    let mut consumer = connect(&addr, CLIENT).await;
    subscribe(&mut consumer, TOPIC, 1, QoS::AtLeastOnce).await;

    // Publish a QoS 1 message. The consumer receives it (and ignores it); after
    // `max_delivery_attempts` redeliveries it is dead-lettered.
    let mut publisher = connect(&addr, "shop").await;
    publisher
        .send(Packet::from(publish_qos1(TOPIC, b"order#42", 1)))
        .await
        .expect("publish");
    match next_packet(&mut publisher).await {
        Packet::PublishAck(_) => {}
        other => panic!("expected PUBACK for the publisher, got {other:?}"),
    }

    // The dead-letter consumer eventually receives the message on its $dlq topic.
    match next_packet(&mut dlq_watcher).await {
        Packet::Publish(p) => {
            assert_eq!(
                &*p.topic,
                format!("$dlq/{CLIENT}/{TOPIC}"),
                "dead-letter topic must carry the client and original topic"
            );
            assert_eq!(
                p.payload.as_ref(),
                b"order#42".as_ref(),
                "payload preserved"
            );
        }
        other => panic!("expected a dead-lettered message on $dlq/#, got {other:?}"),
    }
}
