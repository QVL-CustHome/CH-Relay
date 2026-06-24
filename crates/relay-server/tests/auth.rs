//! End-to-end auth + ACL test against the real `relay` binary, configured with
//! an `[auth]` block. Asserts: a CONNECT without a valid JWT is refused; a valid
//! JWT connects; and the per-role ACL (templated with `{sub}`) scopes which
//! topics the client may subscribe to.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::v5::{
    Codec, Connect, ConnectAckReason, Packet, Subscribe, SubscribeAckReason, SubscriptionOptions,
};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const SECRET: &str = "e2e-auth-secret";
const EXP: i64 = 4_102_444_800; // 2100-01-01, far future

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn jwt(sub: &str, roles: &[&str]) -> String {
    let claims = serde_json::json!({ "sub": sub, "roles": roles, "exp": EXP });
    encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(SECRET.as_bytes()))
        .expect("encode jwt")
}

fn connect_packet(client_id: &str, password: Option<&str>) -> Connect {
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
        password: password.map(|p| Bytes::from(p.to_string())),
        cert: None,
    }
}

type Client = Framed<TcpStream, Codec>;

async fn raw_connect(addr: &str) -> Client {
    let deadline = Instant::now() + Duration::from_secs(5);
    let stream = loop {
        match TcpStream::connect(addr).await {
            Ok(s) => break s,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("broker never accepted connections: {e}"),
        }
    };
    Framed::new(stream, Codec::new(256 * 1024, 0))
}

async fn next_packet(framed: &mut Client) -> Packet {
    timeout(Duration::from_secs(5), framed.next())
        .await
        .expect("timed out waiting for a packet")
        .expect("connection closed unexpectedly")
        .expect("decode error")
        .0
}

async fn subscribe(client: &mut Client, packet_id: u16, filter: &str) -> SubscribeAckReason {
    client
        .send(Packet::Subscribe(Subscribe {
            packet_id: packet_id.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(filter.into(), SubscriptionOptions::default())],
        }))
        .await
        .expect("send SUBSCRIBE");
    match next_packet(client).await {
        Packet::SubscribeAck(ack) => ack.status[0],
        other => panic!("expected SUBACK, got {other:?}"),
    }
}

#[tokio::test]
async fn jwt_required_and_acl_scopes_topics() {
    let tcp_port = 21899;
    let cfg = std::env::temp_dir().join("relay-auth-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{tcp_port}\"\n\
             ws_addr = \"127.0.0.1:28099\"\n\
             \n\
             [auth]\n\
             jwt_secret = \"{SECRET}\"\n\
             \n\
             [[auth.acl]]\n\
             role = \"drive\"\n\
             publish = [\"drive/{{sub}}/#\"]\n\
             subscribe = [\"drive/{{sub}}/#\"]\n"
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

    // 1) No token → CONNECT refused with NotAuthorized.
    let mut anon = raw_connect(&addr).await;
    anon.send(Packet::from(connect_packet("anon", None))).await.expect("send CONNECT");
    match next_packet(&mut anon).await {
        Packet::ConnectAck(ack) => {
            assert_eq!(ack.reason_code, ConnectAckReason::NotAuthorized, "expected rejection");
        }
        other => panic!("expected CONNACK, got {other:?}"),
    }

    // 2) Valid token → CONNECT accepted.
    let token = jwt("u1", &["drive"]);
    let mut user = raw_connect(&addr).await;
    user.send(Packet::from(connect_packet("u1-dev", Some(&token)))).await.expect("send CONNECT");
    match next_packet(&mut user).await {
        Packet::ConnectAck(ack) => {
            assert_eq!(ack.reason_code, ConnectAckReason::Success, "expected success");
        }
        other => panic!("expected CONNACK, got {other:?}"),
    }

    // 3) ACL: own subtree granted, another user's subtree refused.
    let granted = subscribe(&mut user, 1, "drive/u1/files").await;
    assert_eq!(granted, SubscribeAckReason::GrantedQos0, "own subtree should be granted");

    let denied = subscribe(&mut user, 2, "drive/u2/files").await;
    assert_eq!(denied, SubscribeAckReason::NotAuthorized, "another user's subtree must be refused");

    let no_role_token = jwt("u3", &["other"]);
    let mut no_role = raw_connect(&addr).await;
    no_role
        .send(Packet::from(connect_packet("u3-dev", Some(&no_role_token))))
        .await
        .expect("send CONNECT");
    match next_packet(&mut no_role).await {
        Packet::ConnectAck(ack) => {
            assert_eq!(ack.reason_code, ConnectAckReason::Success, "valid jwt connects");
        }
        other => panic!("expected CONNACK, got {other:?}"),
    }

    let no_role_denied = subscribe(&mut no_role, 1, "drive/u3/files").await;
    assert_eq!(
        no_role_denied,
        SubscribeAckReason::NotAuthorized,
        "client without drive role must be refused its own subtree"
    );
}
