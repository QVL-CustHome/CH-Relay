//! End-to-end in-flight-persistence test (V2): a QoS 1 message queued for an
//! *offline* durable session survives a broker restart. The subscriber is
//! offline when the message is published (so it lands in the session's in-flight
//! queue), the broker is killed and restarted, and on reconnect the message is
//! retransmitted — proving the at-least-once guarantee crosses a restart.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, Connect, Packet, PublishAck, PublishAckReason, PublishProperties, QoS, Subscribe,
    SubscriptionOptions,
};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const TOPIC: &str = "orders/+/created";
const PUB_TOPIC: &str = "orders/42/created";
const TCP_PORT: u16 = 21897;
const WS_PORT: u16 = 28097;
const SECRET: &str = "e2e-inflight-persist-secret";
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

fn connect_packet(client_id: &str, clean_start: bool, expiry: u32, token: &str) -> Connect {
    Connect {
        clean_start,
        keep_alive: 0,
        session_expiry_interval_secs: expiry,
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

async fn connect(addr: &str, client_id: &str, clean_start: bool, expiry: u32) -> (Client, bool) {
    let deadline = Instant::now() + Duration::from_secs(8);
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
            clean_start,
            expiry,
            &jwt(client_id, &["*"]),
        )))
        .await
        .expect("send CONNECT");
    match next_packet(&mut framed).await {
        Packet::ConnectAck(ack) => (framed, ack.session_present),
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

/// Subscribe at QoS 1 so deliveries are at-least-once (and thus queued offline).
async fn subscribe_qos1(client: &mut Client, topic: &str) {
    let mut options = SubscriptionOptions::default();
    options.qos = QoS::AtLeastOnce;
    client
        .send(Packet::Subscribe(Subscribe {
            packet_id: 1.try_into().unwrap(),
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

fn spawn_broker(cfg_path: &PathBuf) -> ChildGuard {
    let child = Command::new(env!("CARGO_BIN_EXE_relay"))
        .env("RELAY_CONFIG", cfg_path)
        .env("RUST_LOG", "off")
        .spawn()
        .expect("spawn relay binary");
    ChildGuard(child)
}

#[tokio::test]
async fn queued_qos1_message_survives_a_restart() {
    let data_dir = std::env::temp_dir().join("relay-inflight-persist-test");
    let _ = std::fs::remove_dir_all(&data_dir);

    let cfg_path = std::env::temp_dir().join("relay-inflight-persist-test.toml");
    std::fs::write(
        &cfg_path,
        format!(
            "tcp_addr = \"127.0.0.1:{TCP_PORT}\"\nws_addr = \"127.0.0.1:{WS_PORT}\"\ndata_dir = '{}'\n\
             \n\
             [auth]\n\
             jwt_secret = \"{SECRET}\"\n\
             \n\
             [[auth.acl]]\n\
             role = \"*\"\n\
             publish = [\"#\"]\n\
             subscribe = [\"#\"]\n",
            data_dir.display()
        ),
    )
    .expect("write test config");

    let addr = format!("127.0.0.1:{TCP_PORT}");

    // --- First run: durable client subscribes (QoS 1) then goes offline; while
    //     it is offline a QoS 1 message is published and must be queued. ---
    {
        let _broker = spawn_broker(&cfg_path);

        let (mut sub, present) = connect(&addr, "order-consumer", false, 3600).await;
        assert!(!present, "first connect should have no prior session");
        subscribe_qos1(&mut sub, TOPIC).await;
        drop(sub); // go offline (session is durable, so it is kept)
        sleep(Duration::from_millis(200)).await; // let the broker detach (offline)

        // Publish while the subscriber is offline: the QoS 1 message is enqueued
        // in the session's (now persisted) in-flight queue.
        let (mut publisher, _) = connect(&addr, "publisher", true, 0).await;
        publisher
            .send(Packet::from(publish_qos1(PUB_TOPIC, b"order#42", 1)))
            .await
            .expect("send PUBLISH");
        match next_packet(&mut publisher).await {
            Packet::PublishAck(_) => {} // publisher's own QoS 1 handshake
            other => panic!("expected PUBACK for the publish, got {other:?}"),
        }
        drop(publisher);
        sleep(Duration::from_millis(200)).await; // let the in-flight blob hit disk

        let mut broker = _broker;
        let _ = broker.0.kill();
        let _ = broker.0.wait();
    }

    sleep(Duration::from_millis(400)).await; // let the OS release the port

    // --- Second run: reconnect resumes the session and retransmits the queued
    //     QoS 1 message — without anyone re-publishing it. ---
    let _broker = spawn_broker(&cfg_path);
    let (mut sub, present) = connect(&addr, "order-consumer", false, 3600).await;
    assert!(
        present,
        "the durable session should be restored after restart"
    );

    match next_packet(&mut sub).await {
        Packet::Publish(p) => {
            assert_eq!(&*p.topic, PUB_TOPIC, "topic mismatch");
            assert_eq!(p.payload.as_ref(), b"order#42".as_ref(), "payload mismatch");
            assert_eq!(p.qos, QoS::AtLeastOnce, "should be redelivered at QoS 1");
            let pid = p.packet_id.expect("a QoS 1 delivery carries a packet id");
            // Acknowledge it so the broker can clear the in-flight entry.
            sub.send(Packet::PublishAck(PublishAck {
                packet_id: pid,
                reason_code: PublishAckReason::Success,
                properties: Vec::new(),
                reason_string: None,
            }))
            .await
            .expect("send PUBACK");
        }
        other => panic!("expected the retransmitted queued message, got {other:?}"),
    }
}
