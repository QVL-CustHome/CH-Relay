//! End-to-end replay test (V2): every published message is journalled with a
//! global offset, and a client can replay the log from an offset by publishing
//! a `$replay/{from}/{filter}` control request. Replayed messages arrive on the
//! requester's session, in offset order, tagged with their offset.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{Codec, Connect, Packet, PublishProperties, QoS};
use std::time::Duration;
use std::process::{Child, Command};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const TOPIC: &str = "sensors/a/temp";
const OTHER: &str = "other/x";
const TCP_PORT: u16 = 21900;
const WS_PORT: u16 = 28100;

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

type Client = Framed<TcpStream, Codec>;

fn connect_packet(client_id: &str) -> Connect {
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
        password: None,
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
        .send(Packet::from(connect_packet(client_id)))
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

/// Publish a QoS 1 message and wait for the PUBACK — which guarantees the broker
/// has logged the event before we move on.
async fn publish_and_ack(client: &mut Client, topic: &str, payload: &'static [u8], packet_id: u16) {
    client
        .send(Packet::from(publish_qos1(topic, payload, packet_id)))
        .await
        .expect("publish");
    match next_packet(client).await {
        Packet::PublishAck(_) => {}
        other => panic!("expected PUBACK, got {other:?}"),
    }
}

fn replay_request(topic: &str) -> Publish {
    Publish {
        dup: false,
        retain: false,
        qos: QoS::AtMostOnce,
        topic: topic.into(),
        packet_id: None,
        payload: Bytes::new(),
        properties: Some(PublishProperties::default()),
    }
}

async fn try_next(client: &mut Client) -> Option<Packet> {
    match timeout(Duration::from_millis(700), client.next()).await {
        Ok(Some(Ok((p, _)))) => Some(p),
        Ok(_) => None,
        Err(_) => None,
    }
}

fn replay_offset(p: &Publish) -> Option<u64> {
    let props = p.properties.as_ref()?;
    props
        .user_properties
        .iter()
        .find(|(k, _)| &**k == "x-replay-offset")
        .and_then(|(_, v)| v.parse::<u64>().ok())
}

#[tokio::test]
async fn replay_streams_logged_events_from_an_offset() {
    let data_dir = std::env::temp_dir().join("relay-replay-test");
    let _ = std::fs::remove_dir_all(&data_dir);

    let cfg = std::env::temp_dir().join("relay-replay-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{TCP_PORT}\"\nws_addr = \"127.0.0.1:{WS_PORT}\"\n\
             data_dir = '{}'\nevent_log_max = 1000\n",
            data_dir.display()
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

    // Produce a log: offsets 0,1 on TOPIC and offset 2 on OTHER.
    let mut producer = connect(&addr, "producer").await;
    publish_and_ack(&mut producer, TOPIC, b"21", 1).await;
    publish_and_ack(&mut producer, TOPIC, b"22", 2).await;
    publish_and_ack(&mut producer, OTHER, b"99", 3).await;

    // A fresh consumer asks to replay TOPIC from offset 0 (QoS 0 request, so no
    // PUBACK clutters the stream). It need not even be subscribed: replay streams
    // straight to the requester's session.
    let mut consumer = connect(&addr, "replayer").await;
    consumer
        .send(Packet::from(replay_request(&format!("$replay/0/{TOPIC}"))))
        .await
        .expect("send replay request");

    // First replayed event: offset 0, payload "21".
    match next_packet(&mut consumer).await {
        Packet::Publish(p) => {
            assert_eq!(&*p.topic, TOPIC);
            assert_eq!(p.payload.as_ref(), b"21".as_ref());
            assert_eq!(replay_offset(&p), Some(0), "first event carries offset 0");
        }
        other => panic!("expected first replayed event, got {other:?}"),
    }

    // Second replayed event: offset 1, payload "22".
    match next_packet(&mut consumer).await {
        Packet::Publish(p) => {
            assert_eq!(p.payload.as_ref(), b"22".as_ref());
            assert_eq!(replay_offset(&p), Some(1), "second event carries offset 1");
        }
        other => panic!("expected second replayed event, got {other:?}"),
    }

    // OTHER's event (offset 2) must NOT be replayed (it doesn't match the filter).
    assert!(
        try_next(&mut consumer).await.is_none(),
        "only matching events should be replayed"
    );
}
