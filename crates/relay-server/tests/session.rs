//! End-to-end session tests against the real `relay` binary.
//!
//! - **resume**: a subscriber with `clean_start=false` and a non-zero expiry
//!   keeps its subscription across a disconnect; a QoS 1 message published while
//!   it is offline is queued and delivered when it reconnects (without
//!   re-subscribing). The CONNACK reports `session_present`.
//! - **clean start**: reconnecting with `clean_start=true` wipes the prior
//!   session, so the old subscription no longer delivers.

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

const SECRET: &str = "e2e-session-secret";
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

fn qos1_publish(topic: &str, payload: &'static [u8]) -> Publish {
    Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: topic.into(),
        packet_id: Some(1.try_into().unwrap()),
        payload: Bytes::from_static(payload),
        properties: Some(PublishProperties::default()),
    }
}

/// Connect and return the framed client plus the CONNACK's `session_present`.
async fn connect(addr: &str, client_id: &str, clean_start: bool, expiry: u32) -> (Client, bool) {
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

async fn subscribe_qos1(client: &mut Client, topic: &str) {
    client
        .send(Packet::Subscribe(Subscribe {
            packet_id: 1.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(
                topic.into(),
                SubscriptionOptions {
                    qos: QoS::AtLeastOnce,
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

async fn try_next_payload(client: &mut Client) -> Option<String> {
    match timeout(Duration::from_millis(600), client.next()).await {
        Ok(Some(Ok((Packet::Publish(p), _)))) => {
            Some(String::from_utf8_lossy(&p.payload).into_owned())
        }
        Ok(other) => panic!("unexpected frame: {other:?}"),
        Err(_) => None,
    }
}

fn spawn_broker(tcp_port: u16, ws_port: u16, tag: &str) -> (ChildGuard, String) {
    let cfg = std::env::temp_dir().join(format!("relay-session-{tag}.toml"));
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\nws_addr = \"127.0.0.1:{ws_port}\"\n\
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
    (ChildGuard(child), format!("127.0.0.1:{tcp_port}"))
}

#[tokio::test]
async fn session_resume_keeps_subscription_and_queues_offline_messages() {
    let (_guard, addr) = spawn_broker(21892, 28092, "resume");

    // First connection: fresh session, subscribe at QoS 1, then drop abruptly.
    let (mut sub, present) = connect(&addr, "durable", false, 60).await;
    assert!(!present, "first connect should report no prior session");
    subscribe_qos1(&mut sub, "events").await;
    drop(sub); // abnormal disconnect; session persists (expiry = 60s)
    sleep(Duration::from_millis(200)).await; // let the broker detach the session

    // While the subscriber is offline, publish a QoS 1 message.
    let (mut publisher, _) = connect(&addr, "publisher", true, 0).await;
    publisher
        .send(Packet::Publish(Box::new(qos1_publish(
            "events",
            b"queued-while-offline",
        ))))
        .await
        .expect("send PUBLISH");
    match next_packet(&mut publisher).await {
        Packet::PublishAck(_) => {}
        other => panic!("expected PUBACK to publisher, got {other:?}"),
    }

    // Reconnect with the same client id: the session resumes (no re-subscribe)
    // and the queued message is delivered.
    let (mut sub, present) = connect(&addr, "durable", false, 60).await;
    assert!(present, "reconnect should resume the existing session");
    assert_eq!(
        try_next_payload(&mut sub).await.as_deref(),
        Some("queued-while-offline"),
        "the message queued while offline should be delivered on reconnect"
    );
}

#[tokio::test]
async fn clean_start_wipes_the_previous_session() {
    let (_guard, addr) = spawn_broker(21893, 28093, "clean");

    // Subscribe under a durable session, then drop.
    let (mut sub, _) = connect(&addr, "ephemeral", false, 60).await;
    subscribe_qos1(&mut sub, "events").await;
    drop(sub);
    sleep(Duration::from_millis(200)).await;

    // Reconnect with clean_start = true: the old session (and its subscription)
    // must be discarded.
    let (mut sub, present) = connect(&addr, "ephemeral", true, 0).await;
    assert!(!present, "clean_start must not resume a session");

    // Publish to the old topic; the cleaned client should NOT receive it.
    let (mut publisher, _) = connect(&addr, "publisher", true, 0).await;
    publisher
        .send(Packet::Publish(Box::new(qos1_publish(
            "events",
            b"should-not-arrive",
        ))))
        .await
        .expect("send PUBLISH");
    match next_packet(&mut publisher).await {
        Packet::PublishAck(_) => {}
        other => panic!("expected PUBACK, got {other:?}"),
    }

    assert_eq!(
        try_next_payload(&mut sub).await,
        None,
        "a clean-started session must not inherit the old subscription"
    );
}
