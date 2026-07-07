//! End-to-end UNSUBSCRIBE test: after a client unsubscribes (and gets its
//! UNSUBACK), messages on that topic must no longer be delivered to it.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, Connect, Packet, PublishProperties, QoS, Subscribe, SubscriptionOptions, Unsubscribe,
    UnsubscribeAckReason,
};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const TOPIC: &str = "news/sports";
const SECRET: &str = "e2e-unsub-secret";
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

async fn try_next_payload(client: &mut Client) -> Option<String> {
    match timeout(Duration::from_millis(500), client.next()).await {
        Ok(Some(Ok((Packet::Publish(p), _)))) => {
            Some(String::from_utf8_lossy(&p.payload).into_owned())
        }
        Ok(other) => panic!("unexpected frame: {other:?}"),
        Err(_) => None,
    }
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let tcp_port = 21896;

    let cfg = std::env::temp_dir().join("relay-unsub-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\nws_addr = \"127.0.0.1:28096\"\n\
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

    let mut publisher = connect(&addr, "publisher").await;

    // While subscribed: the message arrives.
    publisher
        .send(Packet::from(publish(TOPIC, b"first")))
        .await
        .expect("publish first");
    assert_eq!(
        try_next_payload(&mut subscriber).await.as_deref(),
        Some("first"),
        "message should arrive while subscribed"
    );

    // Unsubscribe and wait for the UNSUBACK.
    subscriber
        .send(Packet::Unsubscribe(Unsubscribe {
            packet_id: 2.try_into().unwrap(),
            user_properties: Vec::new(),
            topic_filters: vec![TOPIC.into()],
        }))
        .await
        .expect("send UNSUBSCRIBE");
    match next_packet(&mut subscriber).await {
        Packet::UnsubscribeAck(ack) => assert_eq!(
            ack.status.first(),
            Some(&UnsubscribeAckReason::Success),
            "unsubscribing an existing subscription should succeed"
        ),
        other => panic!("expected UNSUBACK, got {other:?}"),
    }

    // After unsubscribe: the next message must NOT arrive.
    publisher
        .send(Packet::from(publish(TOPIC, b"second")))
        .await
        .expect("publish second");
    assert_eq!(
        try_next_payload(&mut subscriber).await,
        None,
        "no message should arrive after unsubscribe"
    );
}
