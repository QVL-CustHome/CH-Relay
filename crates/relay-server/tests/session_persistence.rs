//! End-to-end session-persistence test (V2): a durable session's subscription
//! survives a broker restart. After restart, the client reconnects with
//! `clean_start=false`, the broker reports `session_present`, and — without
//! re-subscribing — a freshly published message is still delivered.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, Connect, Packet, PublishProperties, QoS, Subscribe, SubscriptionOptions,
};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const TOPIC: &str = "fleet/+/telemetry";
const PUB_TOPIC: &str = "fleet/truck7/telemetry";
const TCP_PORT: u16 = 21895;
const WS_PORT: u16 = 28095;

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

type Client = Framed<TcpStream, Codec>;

fn connect_packet(client_id: &str, clean_start: bool, expiry: u32) -> Connect {
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
        password: None,
        cert: None,
    }
}

fn publish(topic: &str, payload: &'static [u8]) -> Publish {
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
        .send(Packet::from(connect_packet(client_id, clean_start, expiry)))
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

fn spawn_broker(cfg_path: &PathBuf) -> ChildGuard {
    let child = Command::new(env!("CARGO_BIN_EXE_relay"))
        .env("RELAY_CONFIG", cfg_path)
        .env("RUST_LOG", "off")
        .spawn()
        .expect("spawn relay binary");
    ChildGuard(child)
}

#[tokio::test]
async fn durable_subscription_survives_a_restart() {
    let data_dir = std::env::temp_dir().join("relay-session-persist-test");
    let _ = std::fs::remove_dir_all(&data_dir);

    let cfg_path = std::env::temp_dir().join("relay-session-persist-test.toml");
    std::fs::write(
        &cfg_path,
        format!(
            "tcp_addr = \"127.0.0.1:{TCP_PORT}\"\nws_addr = \"127.0.0.1:{WS_PORT}\"\ndata_dir = '{}'\n",
            data_dir.display()
        ),
    )
    .expect("write test config");

    let addr = format!("127.0.0.1:{TCP_PORT}");

    // --- First run: a durable client subscribes, then the broker stops. ---
    {
        let mut broker = spawn_broker(&cfg_path);
        let (mut sub, present) = connect(&addr, "fleet-consumer", false, 3600).await;
        assert!(!present, "first connect should have no prior session");
        subscribe(&mut sub, TOPIC).await;
        drop(sub);
        sleep(Duration::from_millis(150)).await; // let the broker persist + detach
        let _ = broker.0.kill();
        let _ = broker.0.wait();
    }

    sleep(Duration::from_millis(400)).await; // let the OS release the port

    // --- Second run: reconnect resumes the persisted session. ---
    let _broker = spawn_broker(&cfg_path);
    let (mut sub, present) = connect(&addr, "fleet-consumer", false, 3600).await;
    assert!(present, "the durable session should be restored after restart");

    // Without re-subscribing, a published message must reach the resumed session.
    let (mut publisher, _) = connect(&addr, "publisher", true, 0).await;
    publisher
        .send(Packet::from(publish(PUB_TOPIC, b"speed=72")))
        .await
        .expect("send PUBLISH");

    match next_packet(&mut sub).await {
        Packet::Publish(p) => {
            assert_eq!(&*p.topic, PUB_TOPIC, "topic mismatch");
            assert_eq!(p.payload.as_ref(), b"speed=72".as_ref(), "payload mismatch");
        }
        other => panic!("expected delivery via the restored subscription, got {other:?}"),
    }
}
