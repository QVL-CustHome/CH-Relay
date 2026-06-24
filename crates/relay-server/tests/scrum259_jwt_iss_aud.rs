use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::v5::{
    Codec, Connect, ConnectAckReason, Packet, Subscribe, SubscribeAckReason, SubscriptionOptions,
};
use serde_json::{json, Value};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const SECRET: &str = "scrum259-shared-secret-min-32-bytes-long";
const OTHER_SECRET: &str = "scrum259-attacker-secret-32-bytes-long";
const ISS: &str = "ch-api-authenticator";
const FUTURE_EXP: i64 = 4_102_444_800;
const PAST_EXP: i64 = 1_000_000_000;

type Client = Framed<TcpStream, Codec>;

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn sign(secret: &str, claims: Value) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
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

async fn connect_reason(addr: &str, client_id: &str, password: Option<&str>) -> ConnectAckReason {
    let mut framed = raw_connect(addr).await;
    framed
        .send(Packet::from(connect_packet(client_id, password)))
        .await
        .expect("send CONNECT");
    match next_packet(&mut framed).await {
        Packet::ConnectAck(ack) => ack.reason_code,
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

async fn connect_then_subscribe(
    addr: &str,
    client_id: &str,
    token: &str,
    filter: &str,
) -> (ConnectAckReason, Option<SubscribeAckReason>) {
    let mut framed = raw_connect(addr).await;
    framed
        .send(Packet::from(connect_packet(client_id, Some(token))))
        .await
        .expect("send CONNECT");
    let reason = match next_packet(&mut framed).await {
        Packet::ConnectAck(ack) => ack.reason_code,
        other => panic!("expected CONNACK, got {other:?}"),
    };
    if reason != ConnectAckReason::Success {
        return (reason, None);
    }
    framed
        .send(Packet::Subscribe(Subscribe {
            packet_id: 1u16.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(filter.into(), SubscriptionOptions::default())],
        }))
        .await
        .expect("send SUBSCRIBE");
    let sub = match next_packet(&mut framed).await {
        Packet::SubscribeAck(ack) => ack.status[0],
        other => panic!("expected SUBACK, got {other:?}"),
    };
    (reason, Some(sub))
}

fn boot(
    tcp_port: u16,
    ws_port: u16,
    allowed_audiences: Option<&str>,
    env: &[(&str, &str)],
) -> ChildGuard {
    let audiences_line = match allowed_audiences {
        Some(list) => format!("allowed_audiences = [{list}]\n"),
        None => String::new(),
    };
    let cfg_body = format!(
        "tcp_addr = \"127.0.0.1:{tcp_port}\"\n\
         ws_addr = \"127.0.0.1:{ws_port}\"\n\
         \n\
         [auth]\n\
         jwt_secret = \"{SECRET}\"\n\
         {audiences_line}\
         \n\
         [[auth.acl]]\n\
         role = \"drive\"\n\
         publish = [\"drive/{{sub}}/#\"]\n\
         subscribe = [\"drive/{{sub}}/#\"]\n\
         \n\
         [[auth.acl]]\n\
         role = \"drive_admin\"\n\
         publish = [\"drive/#\"]\n\
         subscribe = [\"drive/#\"]\n"
    );
    let cfg = std::env::temp_dir().join(format!("relay-scrum259-{tcp_port}.toml"));
    std::fs::write(&cfg, cfg_body).expect("write test config");

    let mut command = Command::new(env!("CARGO_BIN_EXE_relay"));
    command.env("RELAY_CONFIG", &cfg).env("RUST_LOG", "off");
    command.env_remove("RELAY_ALLOWED_AUDIENCES");
    for (k, v) in env {
        command.env(k, v);
    }
    ChildGuard(command.spawn().expect("spawn relay binary"))
}

#[tokio::test]
async fn ac1_valid_string_and_array_audiences_are_accepted() {
    let _guard = boot(22101, 32101, None, &[]);
    let addr = "127.0.0.1:22101";

    let string_aud = sign(
        SECRET,
        json!({"sub": "u1", "roles": ["drive"], "iss": ISS, "exp": FUTURE_EXP, "aud": "ch-api-drive"}),
    );
    let array_aud = sign(
        SECRET,
        json!({"sub": "u2", "roles": ["drive"], "iss": ISS, "exp": FUTURE_EXP, "aud": ["ch-api-budgy", "ch-api-drive"]}),
    );

    assert_eq!(
        connect_reason(addr, "c-str", Some(&string_aud)).await,
        ConnectAckReason::Success
    );
    assert_eq!(
        connect_reason(addr, "c-arr", Some(&array_aud)).await,
        ConnectAckReason::Success
    );
}

#[tokio::test]
async fn ac2_issuer_missing_or_wrong_is_rejected() {
    let _guard = boot(22102, 32102, None, &[]);
    let addr = "127.0.0.1:22102";

    let no_iss = sign(
        SECRET,
        json!({"sub": "u1", "exp": FUTURE_EXP, "aud": "ch-api-drive"}),
    );
    let wrong_iss = sign(
        SECRET,
        json!({"sub": "u1", "iss": "evil-issuer", "exp": FUTURE_EXP, "aud": "ch-api-drive"}),
    );

    assert_eq!(
        connect_reason(addr, "no-iss", Some(&no_iss)).await,
        ConnectAckReason::NotAuthorized
    );
    assert_eq!(
        connect_reason(addr, "bad-iss", Some(&wrong_iss)).await,
        ConnectAckReason::NotAuthorized
    );
}

#[tokio::test]
async fn ac3_audience_missing_empty_unknown_or_mistyped_is_rejected() {
    let _guard = boot(22103, 32103, None, &[]);
    let addr = "127.0.0.1:22103";

    let no_aud = sign(SECRET, json!({"sub": "u1", "iss": ISS, "exp": FUTURE_EXP}));
    let empty_string = sign(
        SECRET,
        json!({"sub": "u1", "iss": ISS, "exp": FUTURE_EXP, "aud": ""}),
    );
    let empty_array = sign(
        SECRET,
        json!({"sub": "u1", "iss": ISS, "exp": FUTURE_EXP, "aud": []}),
    );
    let unknown = sign(
        SECRET,
        json!({"sub": "u1", "iss": ISS, "exp": FUTURE_EXP, "aud": "ch-api-other"}),
    );
    let mistyped = sign(
        SECRET,
        json!({"sub": "u1", "iss": ISS, "exp": FUTURE_EXP, "aud": 1234}),
    );
    let mistyped_array = sign(
        SECRET,
        json!({"sub": "u1", "iss": ISS, "exp": FUTURE_EXP, "aud": [42, true]}),
    );

    assert_eq!(
        connect_reason(addr, "no-aud", Some(&no_aud)).await,
        ConnectAckReason::NotAuthorized
    );
    assert_eq!(
        connect_reason(addr, "empty-str", Some(&empty_string)).await,
        ConnectAckReason::NotAuthorized
    );
    assert_eq!(
        connect_reason(addr, "empty-arr", Some(&empty_array)).await,
        ConnectAckReason::NotAuthorized
    );
    assert_eq!(
        connect_reason(addr, "unknown", Some(&unknown)).await,
        ConnectAckReason::NotAuthorized
    );
    assert_eq!(
        connect_reason(addr, "mistyped", Some(&mistyped)).await,
        ConnectAckReason::NotAuthorized
    );
    assert_eq!(
        connect_reason(addr, "mistyped-arr", Some(&mistyped_array)).await,
        ConnectAckReason::NotAuthorized
    );
}

#[tokio::test]
async fn ac4_signature_and_expiry_failures_are_rejected() {
    let _guard = boot(22104, 32104, None, &[]);
    let addr = "127.0.0.1:22104";

    let wrong_secret = sign(
        OTHER_SECRET,
        json!({"sub": "u1", "iss": ISS, "exp": FUTURE_EXP, "aud": "ch-api-drive"}),
    );
    let no_exp = sign(
        SECRET,
        json!({"sub": "u1", "iss": ISS, "aud": "ch-api-drive"}),
    );
    let expired = sign(
        SECRET,
        json!({"sub": "u1", "iss": ISS, "exp": PAST_EXP, "aud": "ch-api-drive"}),
    );

    assert_eq!(
        connect_reason(addr, "no-token", None).await,
        ConnectAckReason::NotAuthorized
    );
    assert_eq!(
        connect_reason(addr, "garbage", Some("not-a-jwt")).await,
        ConnectAckReason::NotAuthorized
    );
    assert_eq!(
        connect_reason(addr, "wrong-sig", Some(&wrong_secret)).await,
        ConnectAckReason::NotAuthorized
    );
    assert_eq!(
        connect_reason(addr, "no-exp", Some(&no_exp)).await,
        ConnectAckReason::NotAuthorized
    );
    assert_eq!(
        connect_reason(addr, "expired", Some(&expired)).await,
        ConnectAckReason::NotAuthorized
    );
}

#[tokio::test]
async fn ac5_env_override_restricts_whitelist_to_drive_only() {
    let _guard = boot(
        22105,
        32105,
        None,
        &[("RELAY_ALLOWED_AUDIENCES", "ch-api-drive")],
    );
    let addr = "127.0.0.1:22105";

    let drive = sign(
        SECRET,
        json!({"sub": "u1", "roles": ["drive"], "iss": ISS, "exp": FUTURE_EXP, "aud": "ch-api-drive"}),
    );
    let budgy = sign(
        SECRET,
        json!({"sub": "u2", "roles": ["drive"], "iss": ISS, "exp": FUTURE_EXP, "aud": "ch-api-budgy"}),
    );

    assert_eq!(
        connect_reason(addr, "drive-ok", Some(&drive)).await,
        ConnectAckReason::Success
    );
    assert_eq!(
        connect_reason(addr, "budgy-ko", Some(&budgy)).await,
        ConnectAckReason::NotAuthorized
    );
}

#[tokio::test]
async fn ac5_default_whitelist_accepts_drive_and_budgy() {
    let _guard = boot(22106, 32106, None, &[]);
    let addr = "127.0.0.1:22106";

    let drive = sign(
        SECRET,
        json!({"sub": "u1", "roles": ["drive"], "iss": ISS, "exp": FUTURE_EXP, "aud": "ch-api-drive"}),
    );
    let budgy = sign(
        SECRET,
        json!({"sub": "u2", "roles": ["drive"], "iss": ISS, "exp": FUTURE_EXP, "aud": "ch-api-budgy"}),
    );

    assert_eq!(
        connect_reason(addr, "drive-ok", Some(&drive)).await,
        ConnectAckReason::Success
    );
    assert_eq!(
        connect_reason(addr, "budgy-ok", Some(&budgy)).await,
        ConnectAckReason::Success
    );
}

#[tokio::test]
async fn ac6_legitimate_drive_user_connects_and_acl_by_role_is_unchanged() {
    let _guard = boot(22107, 32107, None, &[]);
    let addr = "127.0.0.1:22107";

    let token = sign(
        SECRET,
        json!({"sub": "alice", "roles": ["drive"], "iss": ISS, "exp": FUTURE_EXP, "aud": "ch-api-drive"}),
    );

    let (conn, own) = connect_then_subscribe(addr, "alice-dev", &token, "drive/alice/files").await;
    assert_eq!(conn, ConnectAckReason::Success);
    assert_eq!(own, Some(SubscribeAckReason::GrantedQos0));

    let (_conn2, other) =
        connect_then_subscribe(addr, "alice-dev2", &token, "drive/bob/files").await;
    assert_eq!(other, Some(SubscribeAckReason::NotAuthorized));
}
