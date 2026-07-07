//! End-to-end QoS 1 (at-least-once) test against the real `relay` binary.
//!
//! Covers both halves of the QoS 1 handshake:
//! - **publisher side** — a QoS 1 PUBLISH is acknowledged by the broker with a
//!   PUBACK carrying the same packet id;
//! - **subscriber side** — a client that subscribed at QoS 1 is granted QoS 1
//!   (SUBACK) and receives the message *at QoS 1 with a packet id*, which it
//!   then PUBACKs back to the broker.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, Connect, Packet, PublishAck, PublishAckReason, PublishProperties, QoS, Subscribe,
    SubscribeAckReason, SubscriptionOptions,
};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const TOPIC: &str = "orders/created";
const SECRET: &str = "e2e-qos1-secret";
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
async fn qos1_publish_is_acked_and_delivered_with_packet_id() {
    let tcp_port = 21886;

    let cfg = std::env::temp_dir().join("relay-qos1-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\nws_addr = \"127.0.0.1:28086\"\n\
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

    // Subscriber subscribes at QoS 1 and must be GRANTED QoS 1.
    let mut subscriber = connect(&addr, "subscriber").await;
    subscriber
        .send(Packet::Subscribe(Subscribe {
            packet_id: 1.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(
                TOPIC.into(),
                SubscriptionOptions {
                    qos: QoS::AtLeastOnce,
                    ..Default::default()
                },
            )],
        }))
        .await
        .expect("send SUBSCRIBE");
    match next_packet(&mut subscriber).await {
        Packet::SubscribeAck(ack) => {
            assert_eq!(
                ack.status.first(),
                Some(&SubscribeAckReason::GrantedQos1),
                "subscriber asked for QoS 1, broker should grant it"
            );
        }
        other => panic!("expected SUBACK, got {other:?}"),
    }

    // Publisher sends a QoS 1 PUBLISH with packet id 7.
    let mut publisher = connect(&addr, "publisher").await;
    publisher
        .send(Packet::Publish(Box::new(Publish {
            dup: false,
            retain: false,
            qos: QoS::AtLeastOnce,
            topic: TOPIC.into(),
            packet_id: Some(7.try_into().unwrap()),
            payload: Bytes::from_static(b"order-42"),
            properties: Some(PublishProperties::default()),
        })))
        .await
        .expect("send PUBLISH");

    // The broker must PUBACK the publisher with the same packet id.
    match next_packet(&mut publisher).await {
        Packet::PublishAck(ack) => {
            assert_eq!(
                ack.packet_id.get(),
                7,
                "PUBACK packet id must echo the PUBLISH"
            );
            assert_eq!(
                ack.reason_code,
                PublishAckReason::Success,
                "PUBACK should be Success"
            );
        }
        other => panic!("expected PUBACK to publisher, got {other:?}"),
    }

    // The subscriber receives the message at QoS 1, with a broker-assigned id.
    let delivered_id = match next_packet(&mut subscriber).await {
        Packet::Publish(p) => {
            assert_eq!(&*p.topic, TOPIC, "topic mismatch");
            assert_eq!(p.payload.as_ref(), b"order-42".as_ref(), "payload mismatch");
            assert_eq!(p.qos, QoS::AtLeastOnce, "delivery should be at QoS 1");
            p.packet_id.expect("QoS 1 delivery must carry a packet id")
        }
        other => panic!("expected forwarded PUBLISH, got {other:?}"),
    };

    // Subscriber acknowledges the delivery; the broker must accept it silently.
    subscriber
        .send(Packet::PublishAck(PublishAck {
            packet_id: delivered_id,
            reason_code: PublishAckReason::Success,
            properties: Vec::new(),
            reason_string: None,
        }))
        .await
        .expect("send PUBACK back to broker");

    // Give the broker a moment to process the PUBACK; it should not drop us or
    // send anything unexpected. A PINGREQ round-trip proves the link is healthy.
    sleep(Duration::from_millis(100)).await;
    subscriber
        .send(Packet::PingRequest)
        .await
        .expect("send PINGREQ");
    match next_packet(&mut subscriber).await {
        Packet::PingResponse => {}
        other => panic!("connection unhealthy after PUBACK, got {other:?}"),
    }
}
