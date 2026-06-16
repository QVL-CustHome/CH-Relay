//! Relay daemon entry point.
//!
//! V1 wiring status:
//! - [x] config load (TOML)
//! - [x] TCP + WebSocket listeners accepting connections
//! - [ ] MQTT 5.0 packet codec (CONNECT/PUBLISH/SUBSCRIBE/…)  — next step
//! - [ ] broker routing via `relay-core` (subscriptions, retained, shared subs)
//! - [ ] QoS 1/2 acknowledgement flows
//!
//! For now the listeners accept sockets and log them, so the daemon runs and the
//! plumbing is in place; the codec + broker loop plug into `handle_connection`.

mod config;
mod connection;
mod hub;

use config::Config;
use hub::Hub;
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "relay=info".into()),
        )
        .init();

    let config_path =
        std::env::var("RELAY_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
    let config = Config::load(&config_path)?;

    info!(tcp = %config.tcp_addr, ws = %config.ws_addr, "Relay starting");

    let tcp = TcpListener::bind(config.tcp_addr).await?;
    info!("relay listening on tcp://{}", config.tcp_addr);

    // The WebSocket listener will upgrade HTTP -> WS and run MQTT-over-WebSocket.
    // For V1 scaffolding it binds and accepts; the upgrade is wired with the codec.
    let ws = TcpListener::bind(config.ws_addr).await?;
    info!("relay listening on ws://{} (WebSocket upgrade TODO)", config.ws_addr);

    let hub = Hub::new();

    loop {
        tokio::select! {
            res = tcp.accept() => {
                match res {
                    Ok((socket, peer)) => {
                        info!(%peer, "accepted TCP connection");
                        tokio::spawn(connection::handle(socket, peer.to_string(), hub.clone()));
                    }
                    Err(e) => warn!(error = %e, "TCP accept failed"),
                }
            }
            res = ws.accept() => {
                match res {
                    // WebSocket upgrade + MQTT-over-WS is the next transport to wire.
                    Ok((_socket, peer)) => {
                        warn!(%peer, "WebSocket transport not implemented yet (upgrade TODO)");
                    }
                    Err(e) => warn!(error = %e, "WS accept failed"),
                }
            }
        }
    }
}
