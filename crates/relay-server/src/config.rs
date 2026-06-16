//! Daemon configuration, loaded from a TOML file (consistent with the rest of
//! the QVL-ToolBox: AIGate, HealthServ, MasterEnv all use TOML).

use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Top-level Relay configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// TCP listener for native MQTT clients (backs: Rust, Go, Java…).
    pub tcp_addr: SocketAddr,
    /// WebSocket listener for browser / mobile clients (MQTT-over-WebSocket).
    pub ws_addr: SocketAddr,
    /// Directory for the on-disk store. When set, retained messages survive
    /// restarts; when absent, Relay runs fully in-memory (V1 behaviour).
    pub data_dir: Option<PathBuf>,
    /// Total delivery attempts for an unacknowledged QoS 1/2 message before it is
    /// dead-lettered (`1` = no retry, straight to DLQ on the first failure).
    pub max_delivery_attempts: u32,
    /// Base back-off between redelivery attempts, in seconds (doubles each
    /// attempt, capped at [`retry_max_secs`](Config::retry_max_secs)).
    pub retry_base_secs: u64,
    /// Upper bound on the redelivery back-off, in seconds.
    pub retry_max_secs: u64,
    /// Maximum number of messages kept in the replayable event log (oldest are
    /// pruned beyond this). `0` disables the event log (no replay). Requires
    /// `data_dir`.
    pub event_log_max: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            // 1883 is the IANA-registered MQTT port; 8083 is the de-facto MQTT-over-WS port.
            tcp_addr: "0.0.0.0:1883".parse().unwrap(),
            ws_addr: "0.0.0.0:8083".parse().unwrap(),
            data_dir: None,
            max_delivery_attempts: 5,
            retry_base_secs: 5,
            retry_max_secs: 60,
            event_log_max: 100_000,
        }
    }
}

impl Config {
    /// Load configuration from a TOML file. Falls back to defaults if the file is absent.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        match std::fs::read_to_string(path) {
            Ok(text) => Ok(toml::from_str(&text)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
            Err(e) => Err(ConfigError::Io(e)),
        }
    }
}

/// Errors that can occur while loading configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
}
