//! End-to-end QoS 2 (exactly-once) test against the real `relay` binary.
//!
//! Exercises the full four-packet handshake on both sides:
//! - **publisher side** — PUBLISH(QoS2) → PUBREC ← PUBREL → PUBCOMP;
//! - **subscriber side** — the broker delivers PUBLISH(QoS2), then
//!   PUBREC ← (from subscriber) → PUBREL → PUBCOMP completes it.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, Connect, Packet, PublishAck, PublishAck2, PublishAck2Reason, PublishAckReason,
    PublishProperties, QoS, Subscribe, SubscribeAckReason, SubscriptionOptions,
};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const TOPIC: &str = "payments/settled";
const SECRET: &str = "e2e-qos2-secret";
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

fn ack2(packet_id: u16) -> PublishAck2 {
    PublishAck2 {
        packet_id: packet_id.try_into().unwrap(),
        reason_code: PublishAck2Reason::Success,
        properties: Vec::new(),
        reason_string: None,
    }
}

#[tokio::test]
async fn qos2_runs_the_full_handshake_both_ways() {
    let tcp_port = 21891;

    let cfg = std::env::temp_dir().join("relay-qos2-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\nws_addr = \"127.0.0.1:28091\"\n\
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

    // Subscriber subscribes at QoS 2 and must be granted QoS 2.
    let mut subscriber = connect(&addr, "subscriber").await;
    subscriber
        .send(Packet::Subscribe(Subscribe {
            packet_id: 1.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(
                TOPIC.into(),
                SubscriptionOptions {
                    qos: QoS::ExactlyOnce,
                    ..Default::default()
                },
            )],
        }))
        .await
        .expect("send SUBSCRIBE");
    match next_packet(&mut subscriber).await {
        Packet::SubscribeAck(ack) => assert_eq!(
            ack.status.first(),
            Some(&SubscribeAckReason::GrantedQos2),
            "should be granted QoS 2"
        ),
        other => panic!("expected SUBACK, got {other:?}"),
    }

    // Publisher sends a QoS 2 PUBLISH with packet id 9.
    let mut publisher = connect(&addr, "publisher").await;
    publisher
        .send(Packet::Publish(Box::new(Publish {
            dup: false,
            retain: false,
            qos: QoS::ExactlyOnce,
            topic: TOPIC.into(),
            packet_id: Some(9.try_into().unwrap()),
            payload: Bytes::from_static(b"txn-1001"),
            properties: Some(PublishProperties::default()),
        })))
        .await
        .expect("send PUBLISH");

    // Publisher side: PUBREC, then we PUBREL, then PUBCOMP.
    match next_packet(&mut publisher).await {
        Packet::PublishReceived(rec) => assert_eq!(rec.packet_id.get(), 9, "PUBREC id"),
        other => panic!("expected PUBREC, got {other:?}"),
    }
    publisher
        .send(Packet::PublishRelease(ack2(9)))
        .await
        .expect("send PUBREL");
    match next_packet(&mut publisher).await {
        Packet::PublishComplete(comp) => assert_eq!(comp.packet_id.get(), 9, "PUBCOMP id"),
        other => panic!("expected PUBCOMP, got {other:?}"),
    }

    // Subscriber side: receive the QoS 2 PUBLISH, run the handshake to completion.
    let delivered_id = match next_packet(&mut subscriber).await {
        Packet::Publish(p) => {
            assert_eq!(&*p.topic, TOPIC, "topic mismatch");
            assert_eq!(p.payload.as_ref(), b"txn-1001".as_ref(), "payload mismatch");
            assert_eq!(p.qos, QoS::ExactlyOnce, "delivery should be QoS 2");
            p.packet_id.expect("QoS 2 delivery must carry a packet id")
        }
        other => panic!("expected forwarded PUBLISH, got {other:?}"),
    };

    // Subscriber answers PUBREC; broker must reply PUBREL.
    subscriber
        .send(Packet::PublishReceived(PublishAck {
            packet_id: delivered_id,
            reason_code: PublishAckReason::Success,
            properties: Vec::new(),
            reason_string: None,
        }))
        .await
        .expect("send PUBREC");
    match next_packet(&mut subscriber).await {
        Packet::PublishRelease(rel) => {
            assert_eq!(rel.packet_id, delivered_id, "PUBREL id should match")
        }
        other => panic!("expected PUBREL, got {other:?}"),
    }

    // Subscriber completes with PUBCOMP; the link should stay healthy.
    subscriber
        .send(Packet::PublishComplete(ack2(delivered_id.get())))
        .await
        .expect("send PUBCOMP");

    sleep(Duration::from_millis(100)).await;
    subscriber
        .send(Packet::PingRequest)
        .await
        .expect("send PINGREQ");
    match next_packet(&mut subscriber).await {
        Packet::PingResponse => {}
        other => panic!("connection unhealthy after QoS 2 handshake, got {other:?}"),
    }
}
