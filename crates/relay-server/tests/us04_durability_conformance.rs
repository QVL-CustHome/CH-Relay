use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, Connect, Packet, PublishProperties, QoS, Subscribe, SubscriptionOptions,
};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const SECRET: &str = "e2e-us04-secret";
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

type Client = Framed<TcpStream, Codec>;

static PORT_SEQ: AtomicU16 = AtomicU16::new(0);

struct Broker {
    child: Option<Child>,
    addr: String,
    data_dir: std::path::PathBuf,
    cfg_path: std::path::PathBuf,
    tcp_port: u16,
}

impl Broker {
    fn start_fresh() -> Broker {
        let nonce = PORT_SEQ.fetch_add(1, Ordering::SeqCst);
        let tcp_port = 24000 + nonce;
        let ws_port = 25000 + nonce;
        let unique = format!(
            "{}-{}",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        );
        let data_dir =
            std::path::PathBuf::from(format!("C:/CustHome/CH-Relay/target/us04-{unique}-{nonce}"));
        let _ = std::fs::remove_dir_all(&data_dir);
        let cfg_path = std::env::temp_dir().join(format!("relay-us04-{unique}-{nonce}.toml"));
        let mut broker = Broker {
            child: None,
            addr: format!("127.0.0.1:{tcp_port}"),
            data_dir,
            cfg_path,
            tcp_port,
        };
        broker.spawn(ws_port);
        broker
    }

    fn spawn(&mut self, ws_port: u16) {
        let data_dir_literal = self.data_dir.display().to_string().replace('\\', "/");
        let cfg = format!(
            "tcp_addr = \"127.0.0.1:{}\"\nws_addr = \"127.0.0.1:{}\"\ndata_dir = \"{}\"\nevent_log_max = 100000\n\
             \n\
             [auth]\n\
             jwt_secret = \"{SECRET}\"\n\
             \n\
             [[auth.acl]]\n\
             role = \"*\"\n\
             publish = [\"#\"]\n\
             subscribe = [\"#\"]\n",
            self.tcp_port, ws_port, data_dir_literal
        );
        std::fs::write(&self.cfg_path, cfg).expect("write broker config");
        let child = Command::new(env!("CARGO_BIN_EXE_relay"))
            .env("RELAY_CONFIG", &self.cfg_path)
            .env("RUST_LOG", "off")
            .spawn()
            .expect("spawn relay binary");
        self.child = Some(child);
    }

    async fn restart(&mut self) {
        sleep(Duration::from_millis(250)).await;
        self.stop().await;
        let ws_port = 25000 + self.tcp_port - 24000;
        self.spawn(ws_port);
    }

    async fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        sleep(Duration::from_millis(700)).await;
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
        let _ = std::fs::remove_file(&self.cfg_path);
    }
}

fn connect_packet(client_id: &str, clean_start: bool, token: &str) -> Connect {
    Connect {
        clean_start,
        keep_alive: 0,
        session_expiry_interval_secs: if clean_start { 0 } else { 3600 },
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

fn publish_packet(
    topic: &str,
    payload: &[u8],
    qos: QoS,
    retain: bool,
    packet_id: Option<u16>,
) -> Publish {
    Publish {
        dup: false,
        retain,
        qos,
        topic: topic.into(),
        packet_id: packet_id.map(|id| id.try_into().unwrap()),
        payload: Bytes::copy_from_slice(payload),
        properties: Some(PublishProperties::default()),
    }
}

async fn connect(addr: &str, client_id: &str, clean_start: bool) -> Client {
    let deadline = Instant::now() + Duration::from_secs(10);
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
            &jwt(client_id, &["*"]),
        )))
        .await
        .expect("send CONNECT");
    match next_packet(&mut framed).await {
        Packet::ConnectAck(_) => framed,
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

async fn subscribe(client: &mut Client, filter: &str, packet_id: u16) {
    subscribe_qos(client, filter, packet_id, QoS::AtMostOnce).await;
}

async fn subscribe_qos(client: &mut Client, filter: &str, packet_id: u16, qos: QoS) {
    client
        .send(Packet::Subscribe(Subscribe {
            packet_id: packet_id.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(
                filter.into(),
                SubscriptionOptions {
                    qos,
                    ..Default::default()
                },
            )],
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

async fn publish_qos1_await_puback(
    client: &mut Client,
    topic: &str,
    payload: &[u8],
    packet_id: u16,
) {
    client
        .send(Packet::from(publish_packet(
            topic,
            payload,
            QoS::AtLeastOnce,
            false,
            Some(packet_id),
        )))
        .await
        .expect("send QoS1 PUBLISH");
    loop {
        match next_packet(client).await {
            Packet::PublishAck(_) => break,
            Packet::Publish(_) => continue,
            other => panic!("expected PUBACK, got {other:?}"),
        }
    }
}

async fn collect_publishes(
    client: &mut Client,
    window: Duration,
) -> Vec<(String, String, Option<String>)> {
    let mut out = Vec::new();
    loop {
        match timeout(window, client.next()).await {
            Ok(Some(Ok((Packet::Publish(p), _)))) => {
                let topic = p.topic.to_string();
                let payload = String::from_utf8_lossy(&p.payload).into_owned();
                let offset = p.properties.as_ref().and_then(|props| {
                    props
                        .user_properties
                        .iter()
                        .find(|(k, _)| &k[..] == "x-replay-offset")
                        .map(|(_, v)| v.to_string())
                });
                out.push((topic, payload, offset));
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => panic!("decode error during collect: {e}"),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    out
}

#[tokio::test]
async fn ac3_retained_message_survives_broker_restart() {
    let mut broker = Broker::start_fresh();
    let topic = "sensors/temp";
    let payload = b"21.5";

    let mut publisher = connect(&broker.addr, "us04-retain-pub", true).await;
    publisher
        .send(Packet::from(publish_packet(
            topic,
            payload,
            QoS::AtLeastOnce,
            true,
            Some(1),
        )))
        .await
        .expect("send retained QoS1 PUBLISH");
    match next_packet(&mut publisher).await {
        Packet::PublishAck(_) => {}
        other => panic!("expected PUBACK for retained publish, got {other:?}"),
    }

    broker.restart().await;

    let mut subscriber = connect(&broker.addr, "us04-retain-sub", true).await;
    subscribe(&mut subscriber, topic, 1).await;

    let received = collect_publishes(&mut subscriber, Duration::from_millis(800)).await;

    assert_eq!(
        received.len(),
        1,
        "exactly one retained message expected after restart, got {received:?}"
    );
    assert_eq!(received[0].0, topic, "retained message on wrong topic");
    assert_eq!(
        received[0].1,
        String::from_utf8_lossy(payload),
        "retained payload did not survive restart"
    );
}

#[tokio::test]
async fn ac3_event_replay_after_qos1_publishes_survives_restart() {
    let mut broker = Broker::start_fresh();
    let topic = "orders/created";

    let mut publisher = connect(&broker.addr, "us04-replay-pub", true).await;
    publish_qos1_await_puback(&mut publisher, topic, b"order-1", 1).await;
    publish_qos1_await_puback(&mut publisher, topic, b"order-2", 2).await;
    publish_qos1_await_puback(&mut publisher, topic, b"order-3", 3).await;

    broker.restart().await;

    let mut replayer = connect(&broker.addr, "us04-replay-consumer", true).await;
    replayer
        .send(Packet::from(publish_packet(
            "$replay/0/orders/#",
            b"",
            QoS::AtMostOnce,
            false,
            None,
        )))
        .await
        .expect("send replay request");

    let events = collect_publishes(&mut replayer, Duration::from_millis(1000)).await;
    let payloads: Vec<&str> = events.iter().map(|(_, p, _)| p.as_str()).collect();

    assert_eq!(
        payloads,
        vec!["order-1", "order-2", "order-3"],
        "all QoS1 events must be persisted and replayable after restart, got {events:?}"
    );
    for (recv_topic, _, offset) in &events {
        assert_eq!(recv_topic, topic, "replayed event on wrong topic");
        assert!(
            offset.is_some(),
            "replayed event missing x-replay-offset property"
        );
    }
}

#[tokio::test]
async fn ac3_event_replay_from_nonzero_offset_returns_tail() {
    let broker = Broker::start_fresh();
    let topic = "orders/created";

    let mut publisher = connect(&broker.addr, "us04-offset-pub", true).await;
    publish_qos1_await_puback(&mut publisher, topic, b"a", 1).await;
    publish_qos1_await_puback(&mut publisher, topic, b"b", 2).await;
    publish_qos1_await_puback(&mut publisher, topic, b"c", 3).await;

    let mut probe = connect(&broker.addr, "us04-offset-probe", true).await;
    probe
        .send(Packet::from(publish_packet(
            "$replay/0/orders/#",
            b"",
            QoS::AtMostOnce,
            false,
            None,
        )))
        .await
        .expect("send full replay");
    let full = collect_publishes(&mut probe, Duration::from_millis(1000)).await;
    assert_eq!(
        full.len(),
        3,
        "expected 3 events in full replay, got {full:?}"
    );

    let second_offset = full[1]
        .2
        .clone()
        .expect("second event must carry an offset");

    let mut tail_consumer = connect(&broker.addr, "us04-offset-tail", true).await;
    tail_consumer
        .send(Packet::from(publish_packet(
            &format!("$replay/{second_offset}/orders/#"),
            b"",
            QoS::AtMostOnce,
            false,
            None,
        )))
        .await
        .expect("send tail replay");
    let tail = collect_publishes(&mut tail_consumer, Duration::from_millis(1000)).await;
    let tail_payloads: Vec<&str> = tail.iter().map(|(_, p, _)| p.as_str()).collect();

    assert_eq!(
        tail_payloads,
        vec!["b", "c"],
        "replay from a non-zero offset must return only events at or after that offset, got {tail:?}"
    );
}

#[tokio::test]
async fn ac3_durable_session_and_inflight_survive_restart() {
    let mut broker = Broker::start_fresh();
    let topic = "tasks/queue";

    let mut subscriber = connect(&broker.addr, "us04-durable-sub", false).await;
    subscribe_qos(&mut subscriber, topic, 1, QoS::AtLeastOnce).await;
    drop(subscriber);
    sleep(Duration::from_millis(150)).await;

    let mut publisher = connect(&broker.addr, "us04-durable-pub", true).await;
    publish_qos1_await_puback(&mut publisher, topic, b"queued-while-offline", 1).await;

    broker.restart().await;

    let mut resumed = connect(&broker.addr, "us04-durable-sub", false).await;
    let received = collect_publishes(&mut resumed, Duration::from_millis(1200)).await;
    let payloads: Vec<&str> = received.iter().map(|(_, p, _)| p.as_str()).collect();

    assert!(
        payloads.contains(&"queued-while-offline"),
        "QoS1 message queued for an offline durable session must survive restart and be delivered on resume, got {received:?}"
    );
}
