//! Daemon configuration, loaded from a TOML file (consistent with the rest of
//! the QVL-ToolBox: AIGate, HealthServ, MasterEnv all use TOML).

use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

/// Top-level Relay configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// TCP listener for native MQTT clients (backs: Rust, Go, Java…).
    pub tcp_addr: SocketAddr,
    /// WebSocket listener for browser / mobile clients (MQTT-over-WebSocket).
    pub ws_addr: SocketAddr,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            // 1883 is the IANA-registered MQTT port; 8083 is the de-facto MQTT-over-WS port.
            tcp_addr: "0.0.0.0:1883".parse().unwrap(),
            ws_addr: "0.0.0.0:8083".parse().unwrap(),
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
