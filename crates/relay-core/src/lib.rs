//! `relay-core` — the transport-agnostic broker engine for Relay (MQTT 5.0).
//!
//! This crate contains **no I/O**: no sockets, no tokio, no WebSocket. It is a
//! pure state machine that `relay-server` drives. Everything here is unit-testable
//! without spinning up a network.
//!
//! Module map (V1):
//! - [`topic`]   — topic filter matching (`+`, `#`) and shared-subscription parsing.
//! - [`message`]  — the in-flight message model (payload, QoS, retain).
//! - [`qos`]      — quality-of-service levels.
//! - [`router`]   — subscription table and topic→subscribers routing.
//! - [`retained`] — last-value-per-topic store, replayed to new subscribers.
//! - `session`    — per-client session state and QoS in-flight tracking *(TODO V1)*.

pub mod acl;
pub mod message;
pub mod qos;
pub mod retained;
pub mod router;
pub mod topic;

pub use acl::{filter_subsumes, Acl};
pub use message::Message;
pub use qos::QoS;
pub use retained::RetainedStore;
pub use router::{ClientId, Router};
pub use topic::{topic_matches, SharedSubscription, TopicFilter};
