use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, Connect, ConnectAckReason, Packet, PublishAckReason, PublishProperties, QoS, Subscribe,
    SubscribeAckReason, SubscriptionOptions,
};
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;

const REAL_CONFIG: &str = r"C:\CustHome\CH-Relay\config.toml";
const REAL_DRIVE_ENV: &str = r"C:\CustHome\CH-Api-Drive\.env";
const UPLOAD_TOPIC: &str = "users/u-owner-1/files/file-42/uploaded";
const OTHER_EVENT_TOPIC: &str = "users/u-owner-1/files/file-42/created";
const OTHER_USER_TREE: &str = "users/u-owner-2/files/file-99/uploaded";
const NON_BUSINESS_TREE: &str = "events/u-owner-1/files/file-42/uploaded";

type Client = Framed<TcpStream, Codec>;

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn read_service_token() -> String {
    let content = std::fs::read_to_string(REAL_DRIVE_ENV).expect("read CH-Api-Drive/.env");
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("RELAY_SERVICE_TOKEN=") {
            return rest.trim().to_string();
        }
    }
    panic!("RELAY_SERVICE_TOKEN not found in CH-Api-Drive/.env");
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

fn boot_real_relay(tcp_port: u16) -> ChildGuard {
    let real = std::fs::read_to_string(REAL_CONFIG).expect("read real config.toml");
    let mut rebound = String::new();
    for line in real.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("tcp_addr") {
            rebound.push_str(&format!("tcp_addr = \"127.0.0.1:{tcp_port}\"\n"));
        } else if trimmed.starts_with("ws_addr") {
            rebound.push_str(&format!("ws_addr = \"127.0.0.1:{}\"\n", tcp_port + 1000));
        } else if trimmed.starts_with("http_addr") {
            rebound.push_str(&format!("http_addr = \"127.0.0.1:{}\"\n", tcp_port + 2000));
        } else {
            rebound.push_str(line);
            rebound.push('\n');
        }
    }

    let cfg = std::env::temp_dir().join(format!("relay-scrum182-{tcp_port}.toml"));
    std::fs::write(&cfg, rebound).expect("write per-test config");

    let child = Command::new(env!("CARGO_BIN_EXE_relay"))
        .env("RELAY_CONFIG", &cfg)
        .env("RUST_LOG", "off")
        .spawn()
        .expect("spawn relay binary against rebound real config");
    ChildGuard(child)
}

#[tokio::test]
async fn ac2_service_identity_connects_with_real_token() {
    let port = 21982;
    let _guard = boot_real_relay(port);
    let addr = format!("127.0.0.1:{port}");
    let token = read_service_token();

    let (_client, reason) = connect_as_service(&addr, "svc-drive", &token).await;

    assert_eq!(
        reason,
        ConnectAckReason::Success,
        "AC2: svc-drive must authenticate successfully with the provisioned service JWT"
    );
}

#[tokio::test]
async fn ac3_publish_allowed_on_upload_event_topic() {
    let port = 21983;
    let _guard = boot_real_relay(port);
    let addr = format!("127.0.0.1:{port}");
    let token = read_service_token();

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &token).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let reason = publish_result(&mut client, UPLOAD_TOPIC, 1).await;

    assert_eq!(
        reason,
        PublishAckReason::Success,
        "AC3: publish on users/{{owner}}/files/{{file}}/uploaded must be authorized"
    );
}

#[tokio::test]
async fn ac3_publish_rejected_on_other_event_same_user() {
    let port = 21984;
    let _guard = boot_real_relay(port);
    let addr = format!("127.0.0.1:{port}");
    let token = read_service_token();

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &token).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let reason = publish_result(&mut client, OTHER_EVENT_TOPIC, 1).await;

    assert_eq!(
        reason,
        PublishAckReason::NotAuthorized,
        "AC3: publish on a non-uploaded event must be rejected"
    );
}

#[tokio::test]
async fn ac3_publish_rejected_on_other_user_tree() {
    let port = 21985;
    let _guard = boot_real_relay(port);
    let addr = format!("127.0.0.1:{port}");
    let token = read_service_token();

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &token).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let reason = publish_result(&mut client, OTHER_USER_TREE, 1).await;

    assert_eq!(
        reason,
        PublishAckReason::Success,
        "AC3 sentinel: wildcard owner means any user's uploaded topic is publishable by the service"
    );
}

#[tokio::test]
async fn ac3_publish_rejected_outside_business_tree() {
    let port = 21986;
    let _guard = boot_real_relay(port);
    let addr = format!("127.0.0.1:{port}");
    let token = read_service_token();

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &token).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let reason = publish_result(&mut client, NON_BUSINESS_TREE, 1).await;

    assert_eq!(
        reason,
        PublishAckReason::NotAuthorized,
        "AC3: publish outside the users/... business tree must be rejected"
    );
}

#[tokio::test]
async fn ac3_subscribe_rejected_on_upload_topic() {
    let port = 21987;
    let _guard = boot_real_relay(port);
    let addr = format!("127.0.0.1:{port}");
    let token = read_service_token();

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &token).await;
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
    let _guard = boot_real_relay(port);
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
    let _guard = boot_real_relay(port);
    let addr = format!("127.0.0.1:{port}");
    let token = read_service_token();

    let (mut client, conn) = connect_as_service(&addr, "svc-drive", &token).await;
    assert_eq!(conn, ConnectAckReason::Success);

    let pub_reason = publish_result(&mut client, "drive/svc-drive/anything", 1).await;
    let sub_reason = subscribe_result(&mut client, "drive/svc-drive/#", 2).await;

    assert_eq!(
        pub_reason,
        PublishAckReason::NotAuthorized,
        "SCRUM-265: drive/{{sub}}/# now requires the drive role, the residual wildcard lane must not grant it to the drive service"
    );
    assert_eq!(
        sub_reason,
        SubscribeAckReason::NotAuthorized,
        "SCRUM-265: drive/{{sub}}/# now requires the drive role, the residual wildcard lane must not grant it to the drive service"
    );
}
