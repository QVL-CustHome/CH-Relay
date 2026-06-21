mod auth;
mod config;
mod connection;
mod dashboard;
mod hub;
mod storage;
mod tls;
mod ws;

use config::Config;
use connection::Limits;
use hub::{Hub, RetryConfig};
use std::sync::Arc;
use std::time::Duration;
use storage::Storage;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
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

    let ws_listener = TcpListener::bind(config.ws_addr).await?;
    info!("relay listening on ws://{}", config.ws_addr);

    let retry = RetryConfig {
        max_attempts: config.max_delivery_attempts,
        base: Duration::from_secs(config.retry_base_secs.max(1)),
        cap: Duration::from_secs(config.retry_max_secs.max(1)),
    };
    let hub = Hub::new(storage, retry, config.event_log_max);

    let auth = Arc::new(config.auth.clone());
    info!("authentication enabled (JWT required at CONNECT)");

    let limits = Limits {
        connect_timeout: Duration::from_millis(config.connect_timeout_ms),
        max_subscriptions_per_client: config.max_subscriptions_per_client,
    };
    let connections = Arc::new(Semaphore::new(config.max_connections));
    info!(
        max_connections = config.max_connections,
        connect_timeout_ms = config.connect_timeout_ms,
        max_subscriptions_per_client = config.max_subscriptions_per_client,
        "connection limits enabled"
    );

    if let (Some(cert), Some(key)) = (&config.tls_cert, &config.tls_key) {
        let acceptor = tls::acceptor(cert, key)?;
        let tls_addr = config.tls_addr.unwrap_or_else(|| "0.0.0.0:8883".parse().unwrap());
        let tls_listener = TcpListener::bind(tls_addr).await?;
        info!("relay listening on mqtts://{tls_addr} (TLS)");
        let hub = hub.clone();
        let auth = auth.clone();
        let connections = connections.clone();
        tokio::spawn(async move {
            loop {
                match tls_listener.accept().await {
                    Ok((socket, peer)) => {
                        let Ok(permit) = connections.clone().try_acquire_owned() else {
                            warn!(%peer, "connection limit reached, refusing TLS connection");
                            continue;
                        };
                        info!(%peer, "accepted TLS connection");
                        let acceptor = acceptor.clone();
                        let hub = hub.clone();
                        let auth = auth.clone();
                        tokio::spawn(async move {
                            match acceptor.accept(socket).await {
                                Ok(stream) => {
                                    connection::handle(stream, format!("tls://{peer}"), hub, auth, limits, permit).await;
                                }
                                Err(e) => warn!(%peer, error = %e, "TLS handshake failed"),
                            }
                        });
                    }
                    Err(e) => warn!(error = %e, "TLS accept failed"),
                }
            }
        });
    } else {
        info!("TLS disabled (set tls_cert + tls_key to enable mqtts)");
    }

    if let Some(http_addr) = config.http_addr {
        let http_listener = TcpListener::bind(http_addr).await?;
        info!("relay dashboard on http://{http_addr}");
        let hub = hub.clone();
        tokio::spawn(dashboard::serve(http_listener, hub));
    } else {
        info!("dashboard disabled (set http_addr to enable)");
    }

    loop {
        tokio::select! {
            res = tcp.accept() => {
                match res {
                    Ok((socket, peer)) => {
                        let Ok(permit) = connections.clone().try_acquire_owned() else {
                            warn!(%peer, "connection limit reached, refusing TCP connection");
                            continue;
                        };
                        info!(%peer, "accepted TCP connection");
                        tokio::spawn(connection::handle(socket, peer.to_string(), hub.clone(), auth.clone(), limits, permit));
                    }
                    Err(e) => warn!(error = %e, "TCP accept failed"),
                }
            }
            res = ws_listener.accept() => {
                match res {
                    Ok((socket, peer)) => {
                        let Ok(permit) = connections.clone().try_acquire_owned() else {
                            warn!(%peer, "connection limit reached, refusing WebSocket connection");
                            continue;
                        };
                        info!(%peer, "accepted WebSocket connection");
                        let hub = hub.clone();
                        let auth = auth.clone();
                        tokio::spawn(async move {
                            match tokio_tungstenite::accept_hdr_async(socket, ws::upgrade_callback).await {
                                Ok(stream) => {
                                    let io = ws::WsByteStream::new(stream);
                                    connection::handle(io, format!("ws://{peer}"), hub, auth, limits, permit).await;
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
