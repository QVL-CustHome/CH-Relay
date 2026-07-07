//! End-to-end MQTT-over-WebSocket test against the real `relay` binary.
//!
//! A subscriber and a publisher both connect over **WebSocket** (HTTP upgrade,
//! MQTT packets in binary frames) and a published message is routed between
//! them — proving the WS transport runs the same broker loop as TCP.

use bytes::{Bytes, BytesMut};
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
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_util::codec::{Decoder, Encoder};

const TOPIC: &str = "ws/test/topic";
const SECRET: &str = "e2e-ws-secret";
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

/// A minimal MQTT client speaking the codec over WebSocket binary frames.
struct WsClient {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    codec: Codec,
    rbuf: BytesMut,
}

impl WsClient {
    async fn connect(url: &str, client_id: &str) -> Self {
        // Retry until the broker's WS listener is up.
        let deadline = Instant::now() + Duration::from_secs(5);
        let ws = loop {
            match connect_async(url).await {
                Ok((ws, _resp)) => break ws,
                Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
                Err(e) => panic!("WebSocket never connected: {e}"),
            }
        };
        let mut client = WsClient {
            ws,
            codec: Codec::new(256 * 1024, 0),
            rbuf: BytesMut::new(),
        };
        client
            .send(Packet::from(connect_packet(
                client_id,
                &jwt(client_id, &["*"]),
            )))
            .await;
        match client.recv().await {
            Packet::ConnectAck(_) => {}
            other => panic!("expected CONNACK, got {other:?}"),
        }
        client
    }

    async fn send(&mut self, packet: Packet) {
        let mut buf = BytesMut::new();
        self.codec.encode(packet, &mut buf).expect("encode packet");
        self.ws
            .send(Message::Binary(buf.freeze()))
            .await
            .expect("send WS binary frame");
    }

    async fn recv(&mut self) -> Packet {
        loop {
            if let Some((packet, _)) = self.codec.decode(&mut self.rbuf).expect("decode") {
                return packet;
            }
            match timeout(Duration::from_secs(5), self.ws.next()).await {
                Ok(Some(Ok(Message::Binary(data)))) => self.rbuf.extend_from_slice(&data),
                Ok(Some(Ok(_))) => {} // ping/pong/text: keep reading
                Ok(Some(Err(e))) => panic!("WS error: {e}"),
                Ok(None) => panic!("WS closed unexpectedly"),
                Err(_) => panic!("timed out waiting for a WS frame"),
            }
        }
    }
}

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

#[tokio::test]
async fn mqtt_over_websocket_routes_a_message() {
    let tcp_port = 21890;
    let ws_port = 28090;

    let cfg = std::env::temp_dir().join("relay-ws-test.toml");
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
    let _guard = ChildGuard(child);

    let url = format!("ws://127.0.0.1:{ws_port}");

    // Subscriber over WebSocket.
    let mut subscriber = WsClient::connect(&url, "ws-subscriber").await;
    subscriber
        .send(Packet::Subscribe(Subscribe {
            packet_id: 1.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(TOPIC.into(), SubscriptionOptions::default())],
        }))
        .await;
    match subscriber.recv().await {
        Packet::SubscribeAck(_) => {}
        other => panic!("expected SUBACK, got {other:?}"),
    }

    // Publisher over WebSocket.
    let mut publisher = WsClient::connect(&url, "ws-publisher").await;
    publisher
        .send(Packet::Publish(Box::new(Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: TOPIC.into(),
            packet_id: None,
            payload: Bytes::from_static(b"over-websocket"),
            properties: Some(PublishProperties::default()),
        })))
        .await;

    // The subscriber should receive the forwarded message over WebSocket.
    match subscriber.recv().await {
        Packet::Publish(p) => {
            assert_eq!(&*p.topic, TOPIC, "topic mismatch");
            assert_eq!(
                p.payload.as_ref(),
                b"over-websocket".as_ref(),
                "payload mismatch"
            );
        }
        other => panic!("expected forwarded PUBLISH, got {other:?}"),
    }
}
