//! The in-flight message model.

use crate::qos::QoS;
use bytes::Bytes;

/// A message as it flows through the broker: a topic name, an opaque payload,
/// and the delivery flags carried by a PUBLISH packet.
///
/// The `topic` here is always a concrete topic **name** (no wildcards) — wildcards
/// only ever appear in subscription *filters*, never in a published message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Concrete topic name this message was published to, e.g. `orders/eu/created`.
    pub topic: String,
    /// Opaque payload bytes — Relay never interprets them.
    pub payload: Bytes,
    /// Requested quality of service.
    pub qos: QoS,
    /// If true, the broker keeps this as the retained message for `topic`.
    pub retain: bool,
}

impl Message {
    /// Build a QoS 0, non-retained message from a topic and payload.
    pub fn new(topic: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Message {
            topic: topic.into(),
            payload: payload.into(),
            qos: QoS::AtMostOnce,
            retain: false,
        }
    }

    /// Builder: set the QoS.
    pub fn with_qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    /// Builder: mark the message as retained.
    pub fn retained(mut self) -> Self {
        self.retain = true;
        self
    }
}
