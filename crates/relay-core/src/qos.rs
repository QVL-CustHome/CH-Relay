//! MQTT Quality of Service levels.

/// Delivery guarantee requested for a PUBLISH, per the MQTT 5.0 spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QoS {
    /// At most once — fire and forget. No acknowledgement.
    AtMostOnce = 0,
    /// At least once — acknowledged with PUBACK; may be redelivered.
    AtLeastOnce = 1,
    /// Exactly once — 4-packet handshake (PUBREC/PUBREL/PUBCOMP).
    ExactlyOnce = 2,
}

impl QoS {
    /// Parse the 2-bit QoS field from the wire. Returns `None` for the reserved value 3.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(QoS::AtMostOnce),
            1 => Some(QoS::AtLeastOnce),
            2 => Some(QoS::ExactlyOnce),
            _ => None,
        }
    }
}
