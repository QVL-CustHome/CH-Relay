//! End-to-end persistence test (V2): a retained message published before the
//! broker is restarted is still delivered to a subscriber that connects after
//! the restart — proving it was written to and reloaded from disk.

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

const TOPIC: &str = "devices/door/state";
const TCP_PORT: u16 = 21894;
const WS_PORT: u16 = 28094;

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

async fn ping(client: &mut Client) {
    client.send(Packet::PingRequest).await.expect("send PINGREQ");
    match next_packet(client).await {
        Packet::PingResponse => {}
        other => panic!("expected PINGRESP, got {other:?}"),
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
async fn retained_message_survives_a_restart() {
    let data_dir = std::env::temp_dir().join("relay-persist-test");
    let _ = std::fs::remove_dir_all(&data_dir); // start from a clean slate

    let cfg_path = std::env::temp_dir().join("relay-persist-test.toml");
    // TOML literal string ('...') so Windows backslashes need no escaping.
    std::fs::write(
        &cfg_path,
        format!(
            "tcp_addr = \"127.0.0.1:{TCP_PORT}\"\nws_addr = \"127.0.0.1:{WS_PORT}\"\ndata_dir = '{}'\n",
            data_dir.display()
        ),
    )
    .expect("write test config");

    let addr = format!("127.0.0.1:{TCP_PORT}");

    // --- First run: publish a retained message, then stop the broker. ---
    {
        let mut broker = spawn_broker(&cfg_path);
        let mut publisher = connect(&addr, "publisher").await;
        publisher
            .send(Packet::from(retained_publish(TOPIC, b"open")))
            .await
            .expect("send retained PUBLISH");
        ping(&mut publisher).await; // ensure the broker stored (and persisted) it
        drop(publisher);
        // Stop the broker and wait for it to fully exit.
        let _ = broker.0.kill();
        let _ = broker.0.wait();
    }

    // Give the OS a moment to release the listening port.
    sleep(Duration::from_millis(400)).await;

    // --- Second run: a fresh broker must reload the retained message. ---
    let _broker = spawn_broker(&cfg_path);
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

    match next_packet(&mut subscriber).await {
        Packet::Publish(p) => {
            assert_eq!(&*p.topic, TOPIC, "topic mismatch after restart");
            assert_eq!(p.payload.as_ref(), b"open".as_ref(), "payload lost across restart");
            assert!(p.retain, "reloaded message should carry the retain flag");
        }
        other => panic!("expected the persisted retained PUBLISH, got {other:?}"),
    }
}
