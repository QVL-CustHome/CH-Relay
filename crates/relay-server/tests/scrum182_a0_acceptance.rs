use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, Connect, ConnectAckReason, Packet, PublishAckReason, PublishProperties, QoS, Subscribe,
    SubscribeAckReason, SubscriptionOptions,
};
use serde_json::{json, Value};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const SECRET: &str = "scrum182-shared-secret-min-32-bytes-long";
const ISS: &str = "ch-api-authenticator";
const SERVICE_AUD: &str = "ch-relay";
const FUTURE_EXP: i64 = 4_102_444_800;

const UPLOAD_TOPIC: &str = "drive/owner-opaque-1/files/file-42/uploaded";
const OTHER_EVENT_TOPIC: &str = "drive/owner-opaque-1/files/file-42/created";
const OTHER_OWNER_UPLOAD: &str = "drive/owner-opaque-2/files/file-99/uploaded";
const NON_BUSINESS_TREE: &str = "events/owner-opaque-1/files/file-42/uploaded";

type Client = Framed<TcpStream, Codec>;

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn sign(claims: Value) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .expect("encode jwt")
}

fn service_token() -> String {
    sign(json!({
        "sub": "svc-drive",
        "roles": ["drive_service"],
        "iss": ISS,
        "exp": FUTURE_EXP,
        "aud": SERVICE_AUD,
    }))
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

fn publish_qos1(topic: &str, packet_id: u16) -> Publish {
    Publish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: topic.into(),
        packet_id: Some(packet_id.try_into().unwrap()),
        payload: Bytes::from_static(b"{}"),
        properties: Some(PublishProperties::default()),
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

async fn connect_as_service(addr: &str, client_id: &str, token: &str) -> (Client, ConnectAckReason) {
    let mut framed = raw_connect(addr).await;
    framed
        .send(Packet::from(connect_packet(client_id, token)))
        .await
        .expect("send CONNECT");
    match next_packet(&mut framed).await {
        Packet::ConnectAck(ack) => (framed, ack.reason_code),
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

async fn publish_result(client: &mut Client, topic: &str, packet_id: u16) -> PublishAckReason {
    client
        .send(Packet::from(publish_qos1(topic, packet_id)))
        .await
        .expect("send PUBLISH");
    match next_packet(client).await {
        Packet::PublishAck(ack) => ack.reason_code,
        other => panic!("expected PUBACK, got {other:?}"),
    }
}

async fn subscribe_result(client: &mut Client, topic: &str, packet_id: u16) -> SubscribeAckReason {
    client
        .send(Packet::Subscribe(Subscribe {
            packet_id: packet_id.try_into().unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(topic.into(), SubscriptionOptions::default())],
        }))
        .await
        .expect("send SUBSCRIBE");
    match next_packet(client).await {
        Packet::SubscribeAck(ack) => ack.status.into_iter().next().expect("at least one status"),
        other => panic!("expected SUBACK, got {other:?}"),
    }
}

fn boot_loopback_relay(tcp_port: u16) -> ChildGuard {
    let cfg_body = format!(
        "tcp_addr = \"127.0.0.1:{tcp_port}\"\n\
         ws_addr = \"127.0.0.1:{}\"\n\
         http_addr = \"127.0.0.1:{}\"\n\
         [auth]\n\
         jwt_secret = \"{SECRET}\"\n\
         identity_claim = \"sub\"\n\
         roles_claim = \"roles\"\n\
         allowed_audiences = [\"ch-api-drive\", \"ch-api-budgy\", \"ch-relay\"]\n\
         [[auth.acl]]\n\
         role = \"drive\"\n\
         publish = [\"drive/{{sub}}/#\"]\n\
         subscribe = [\"drive/{{sub}}/#\"]\n\
         [[auth.acl]]\n\
         role = \"drive_service\"\n\
         publish = [\"drive/+/files/+/uploaded\"]\n\
         subscribe = []\n\
         [[auth.acl]]\n\
         role = \"drive_admin\"\n\
         publish = [\"drive/#\"]\n\
         subscribe = [\"drive/#\", \"$dlq/#\"]\n",
        tcp_port + 1000,
        tcp_port + 2000,
    );

    let cfg = std::env::temp_dir().join(format!("relay-scrum182-{tcp_port}.toml"));
    std::fs::write(&cfg, cfg_body).expect("write per-test config");

    let child = Command::new(env!("CARGO_BIN_EXE_relay"))
        .env("RELAY_CONFIG", &cfg)
        .env("RUST_LOG", "off")
        .spawn()
        .expect("spawn relay binary against loopback config");
    ChildGuard(child)
}

#[tokio::test]
async fn ac2_service_identity_connects_with_service_token() {
    let port = 21982;
    let _guard = boot_loopback_relay(port);
    let addr = format!("127.0.0.1:{port}");

    let (_client, reason) = connect_as_service(&addr, "svc-drive", &service_token()).await;

    assert_eq!(
        reason,
        ConnectAckReason::Success,
        "AC2: svc-drive must authenticate successfully with the service JWT"
    );
}

#[tokio::test]
async fn ac3_publish_allowed_on_upload_event_topic() {
    let port = 21983;
    let _guard = boot_loopback_relay(port);
    let addr = format!("127.0.0.1:{port}");

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &service_token()).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let reason = publish_result(&mut client, UPLOAD_TOPIC, 1).await;

    assert_eq!(
        reason,
        PublishAckReason::Success,
        "AC3: publish on drive/{{owner_id}}/files/{{file_id}}/uploaded must be authorized"
    );
}

#[tokio::test]
async fn ac3_publish_rejected_on_other_event_same_owner() {
    let port = 21984;
    let _guard = boot_loopback_relay(port);
    let addr = format!("127.0.0.1:{port}");

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &service_token()).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let reason = publish_result(&mut client, OTHER_EVENT_TOPIC, 1).await;

    assert_eq!(
        reason,
        PublishAckReason::NotAuthorized,
        "AC3: publish on a non-uploaded event must be rejected"
    );
}

#[tokio::test]
async fn ac3_publish_allowed_on_any_owner_upload_via_wildcard() {
    let port = 21985;
    let _guard = boot_loopback_relay(port);
    let addr = format!("127.0.0.1:{port}");

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &service_token()).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let reason = publish_result(&mut client, OTHER_OWNER_UPLOAD, 1).await;

    assert_eq!(
        reason,
        PublishAckReason::Success,
        "AC3: drive/+/files/+/uploaded grants the service any owner's uploaded topic"
    );
}

#[tokio::test]
async fn ac3_publish_rejected_outside_drive_tree() {
    let port = 21986;
    let _guard = boot_loopback_relay(port);
    let addr = format!("127.0.0.1:{port}");

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &service_token()).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let reason = publish_result(&mut client, NON_BUSINESS_TREE, 1).await;

    assert_eq!(
        reason,
        PublishAckReason::NotAuthorized,
        "AC3: publish outside the drive/... business tree must be rejected"
    );
}

#[tokio::test]
async fn ac3_subscribe_rejected_on_upload_topic() {
    let port = 21987;
    let _guard = boot_loopback_relay(port);
    let addr = format!("127.0.0.1:{port}");

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &service_token()).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let reason = subscribe_result(&mut client, UPLOAD_TOPIC, 1).await;

    assert_eq!(
        reason,
        SubscribeAckReason::NotAuthorized,
        "AC3: the service is publish-only, every subscribe must be rejected"
    );
}

#[tokio::test]
async fn ac2_connect_refused_with_tampered_token() {
    let port = 21988;
    let _guard = boot_loopback_relay(port);
    let addr = format!("127.0.0.1:{port}");
    let bad = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJzdmMtZHJpdmUiLCJyb2xlcyI6WyJkcml2ZV9zZXJ2aWNlIl19.tampered";

    let (_client, reason) = connect_as_service(&addr, "svc-drive", bad).await;

    assert_eq!(
        reason,
        ConnectAckReason::NotAuthorized,
        "AC2 sentinel: a tampered service token must be refused at CONNECT"
    );
}

#[tokio::test]
async fn residual_wildcard_no_longer_grants_drive_subtree() {
    let port = 21989;
    let _guard = boot_loopback_relay(port);
    let addr = format!("127.0.0.1:{port}");

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &service_token()).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let pub_reason = publish_result(&mut client, "drive/svc-drive/anything", 1).await;
    let sub_reason = subscribe_result(&mut client, "drive/svc-drive/#", 2).await;

    assert_eq!(
        pub_reason,
        PublishAckReason::NotAuthorized,
        "the drive_service role only grants drive/+/files/+/uploaded, not the whole drive subtree"
    );
    assert_eq!(
        sub_reason,
        SubscribeAckReason::NotAuthorized,
        "the drive_service role is publish-only and scoped to the uploaded topic"
    );
}
