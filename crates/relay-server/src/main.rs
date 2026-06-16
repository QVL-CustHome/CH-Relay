//! Relay daemon entry point.
//!
//! Binds two listeners and runs the same MQTT broker loop over both:
//! - **tcp://** — raw MQTT for backends and native clients;
//! - **ws://**  — MQTT-over-WebSocket for browsers and mobile (HTTP upgrade,
//!   `mqtt` subprotocol), bridged to bytes by [`ws::WsByteStream`].

mod config;
mod connection;
mod hub;
mod storage;
mod ws;

use config::Config;
use hub::{Hub, RetryConfig};
use std::time::Duration;
use storage::Storage;
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

    // Open the on-disk store if a data directory is configured; otherwise run
    // fully in-memory (V1 behaviour).
    let storage = match &config.data_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            let path = dir.join("relay.redb");
            info!(path = %path.display(), "persistence enabled");
            Some(Storage::open(&path)?)
        }
        None => {
            info!("persistence disabled (in-memory); set `data_dir` to enable");
            None
        }
    };

    let tcp = TcpListener::bind(config.tcp_addr).await?;
    info!("relay listening on tcp://{}", config.tcp_addr);

    // The WebSocket listener upgrades HTTP -> WS (mqtt subprotocol) and runs the
    // same MQTT broker loop over the WebSocket byte stream.
    let ws_listener = TcpListener::bind(config.ws_addr).await?;
    info!("relay listening on ws://{}", config.ws_addr);

    let retry = RetryConfig {
        max_attempts: config.max_delivery_attempts,
        base: Duration::from_secs(config.retry_base_secs.max(1)),
        cap: Duration::from_secs(config.retry_max_secs.max(1)),
    };
    let hub = Hub::new(storage, retry, config.event_log_max);

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
            res = ws_listener.accept() => {
                match res {
                    Ok((socket, peer)) => {
                        info!(%peer, "accepted WebSocket connection");
                        let hub = hub.clone();
                        tokio::spawn(async move {
                            match tokio_tungstenite::accept_hdr_async(socket, ws::upgrade_callback).await {
                                Ok(stream) => {
                                    let io = ws::WsByteStream::new(stream);
                                    connection::handle(io, format!("ws://{peer}"), hub).await;
                                }
                                Err(e) => warn!(%peer, error = %e, "WebSocket handshake failed"),
                            }
                        });
                    }
                    Err(e) => warn!(error = %e, "WS accept failed"),
                }
            }
        }
    }
}
