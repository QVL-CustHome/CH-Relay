//! End-to-end TLS test (V2): the broker exposes a secure mqtts listener; a
//! rustls client that trusts the broker's self-signed certificate completes the
//! MQTT CONNECT/CONNACK handshake over the encrypted channel.

use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rmqtt_codec::v5::{Codec, Connect, Packet};
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Instant};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use tokio_util::codec::Framed;

const TCP_PORT: u16 = 21901;
const WS_PORT: u16 = 28101;
const TLS_PORT: u16 = 21902;
const SECRET: &str = "e2e-tls-secret";
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

type Client = Framed<TlsStream<TcpStream>, Codec>;

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
async fn mqtts_handshake_over_tls() {
    // Generate a self-signed certificate for "localhost".
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("generate self-signed cert");
    let cert_der = certified.cert.der().clone();

    let dir = std::env::temp_dir().join("relay-tls-test");
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, certified.cert.pem()).expect("write cert");
    std::fs::write(&key_path, certified.key_pair.serialize_pem()).expect("write key");

    let cfg = dir.join("relay-tls-test.toml");
    std::fs::write(
        &cfg,
        format!(
            "tcp_addr = \"127.0.0.1:{TCP_PORT}\"\nws_addr = \"127.0.0.1:{WS_PORT}\"\n\
             tls_addr = \"127.0.0.1:{TLS_PORT}\"\ntls_cert = '{}'\ntls_key = '{}'\n\
             \n\
             [auth]\n\
             jwt_secret = \"{SECRET}\"\n\
             \n\
             [[auth.acl]]\n\
             role = \"*\"\n\
             publish = [\"#\"]\n\
             subscribe = [\"#\"]\n",
            cert_path.display(),
            key_path.display()
        ),
    )
    .expect("write test config");

    let child = Command::new(env!("CARGO_BIN_EXE_relay"))
        .env("RELAY_CONFIG", &cfg)
        .env("RUST_LOG", "off")
        .spawn()
        .expect("spawn relay binary");
    let _guard = ChildGuard(child);

    // Client trusts exactly the broker's self-signed cert.
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der).expect("add cert to root store");
    let client_config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("default protocol versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_config));

    let addr = format!("127.0.0.1:{TLS_PORT}");
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut framed: Client = loop {
        let attempt = async {
            let tcp = TcpStream::connect(&addr).await.ok()?;
            let domain = ServerName::try_from("localhost").ok()?;
            let tls = connector.connect(domain, tcp).await.ok()?;
            Some(Framed::new(tls, Codec::new(256 * 1024, 0)))
        };
        match attempt.await {
            Some(f) => break f,
            None if Instant::now() < deadline => sleep(Duration::from_millis(100)).await,
            None => panic!("could not establish a TLS connection to the broker"),
        }
    };

    framed
        .send(Packet::from(connect_packet(
            "secure-client",
            &jwt("secure-client", &["*"]),
        )))
        .await
        .expect("send CONNECT over TLS");

    let packet = timeout(Duration::from_secs(5), framed.next())
        .await
        .expect("timed out waiting for CONNACK")
        .expect("connection closed unexpectedly")
        .expect("decode error")
        .0;

    match packet {
        Packet::ConnectAck(ack) => {
            assert!(!ack.session_present, "fresh clean-start session");
        }
        other => panic!("expected CONNACK over TLS, got {other:?}"),
    }
}
