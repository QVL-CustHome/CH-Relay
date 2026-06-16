//! Per-connection MQTT loop, built on the `rmqtt-codec` v5 tokio codec.
//!
//! The `Framed` stream is split into a reader (`stream`) and a writer (`sink`).
//! A `tokio::select!` interleaves two sources:
//! - **incoming**: packets from this client (CONNECT, SUBSCRIBE, PUBLISH, …);
//! - **outgoing**: [`Delivery`]s the [`Hub`] routes *to* this client because it
//!   subscribed to a topic someone else published on.
//!
//! ## QoS
//!
//! The broker supports QoS 0, 1 (at-least-once) and 2 (exactly-once).
//!
//! - **Inbound** PUBLISH at QoS 1 is PUBACK'd; at QoS 2 it gets the
//!   PUBREC→PUBREL→PUBCOMP handshake, and is routed only on first receipt (the
//!   packet id is remembered until PUBREL so a retransmitted PUBLISH is not
//!   delivered twice).
//! - **Outbound** delivery at QoS ≥ 1 carries a packet id drawn from this
//!   connection's own counter, tracked until the matching PUBACK (QoS 1) or
//!   PUBREC/PUBCOMP (QoS 2) returns. (Retransmission across reconnects needs
//!   persistent sessions — a later step; the in-memory tracking drives the live
//!   handshake correctly.)

use std::collections::HashSet;
use std::num::NonZeroU16;

use futures::{SinkExt, StreamExt};
use relay_core::{Message, QoS, SharedSubscription, TopicFilter};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Codec, ConnectAck, ConnectAckReason, DisconnectReasonCode, Packet, PublishAck, PublishAck2,
    PublishAck2Reason, PublishAckReason, PublishProperties, QoS as WireQoS, SubscribeAck,
    SubscribeAckReason,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::hub::{Delivery, Hub};

/// Maximum inbound packet size we accept (256 KiB); 0 outbound = unlimited.
const MAX_INBOUND_SIZE: u32 = 256 * 1024;

/// Highest QoS the broker grants/delivers.
const MAX_QOS: QoS = QoS::ExactlyOnce;

/// Per-connection state for QoS > 0 delivery *to* this client.
///
/// QoS 1 awaits a single PUBACK. QoS 2 is the four-packet handshake: we send
/// PUBLISH and await PUBREC; on PUBREC we send PUBREL and await PUBCOMP.
struct OutboundQos {
    /// Next packet identifier to hand out (1..=65535, never 0).
    next_id: u16,
    /// QoS 1 ids delivered and not yet PUBACK'd by this client.
    qos1_unacked: HashSet<u16>,
    /// QoS 2 ids delivered, awaiting PUBREC.
    qos2_pubrec: HashSet<u16>,
    /// QoS 2 ids we've sent PUBREL for, awaiting PUBCOMP.
    qos2_pubcomp: HashSet<u16>,
}

impl OutboundQos {
    fn new() -> Self {
        Self {
            next_id: 1,
            qos1_unacked: HashSet::new(),
            qos2_pubrec: HashSet::new(),
            qos2_pubcomp: HashSet::new(),
        }
    }

    /// Allocate the next packet id, maintaining the "never 0" invariant.
    fn allocate(&mut self) -> NonZeroU16 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        if self.next_id == 0 {
            self.next_id = 1;
        }
        // `id` is non-zero by the invariant we maintain on `next_id`.
        NonZeroU16::new(id).expect("packet id invariant: never 0")
    }
}

fn wire_to_core(q: WireQoS) -> QoS {
    match q {
        WireQoS::AtMostOnce => QoS::AtMostOnce,
        WireQoS::AtLeastOnce => QoS::AtLeastOnce,
        WireQoS::ExactlyOnce => QoS::ExactlyOnce,
    }
}

fn core_to_wire(q: QoS) -> WireQoS {
    match q {
        QoS::AtMostOnce => WireQoS::AtMostOnce,
        QoS::AtLeastOnce => WireQoS::AtLeastOnce,
        QoS::ExactlyOnce => WireQoS::ExactlyOnce,
    }
}

fn granted_reason(q: QoS) -> SubscribeAckReason {
    match q {
        QoS::AtMostOnce => SubscribeAckReason::GrantedQos0,
        QoS::AtLeastOnce => SubscribeAckReason::GrantedQos1,
        QoS::ExactlyOnce => SubscribeAckReason::GrantedQos2,
    }
}

/// PUBREC — acknowledges receipt of a QoS 2 PUBLISH (handshake step 2).
fn pubrec(packet_id: NonZeroU16) -> Packet {
    Packet::PublishReceived(PublishAck {
        packet_id,
        reason_code: PublishAckReason::Success,
        properties: Vec::new(),
        reason_string: None,
    })
}

/// PUBREL — releases a QoS 2 message (handshake step 3).
fn pubrel(packet_id: NonZeroU16) -> Packet {
    Packet::PublishRelease(PublishAck2 {
        packet_id,
        reason_code: PublishAck2Reason::Success,
        properties: Vec::new(),
        reason_string: None,
    })
}

/// PUBCOMP — completes a QoS 2 handshake (step 4).
fn pubcomp(packet_id: NonZeroU16) -> Packet {
    Packet::PublishComplete(PublishAck2 {
        packet_id,
        reason_code: PublishAck2Reason::Success,
        properties: Vec::new(),
        reason_string: None,
    })
}

/// Turn a routed [`Delivery`] into a PUBLISH packet for this connection,
/// stamping a packet id (and starting the QoS 1/2 tracking) as needed.
fn build_publish(delivery: Delivery, qos_state: &mut OutboundQos) -> Packet {
    let qos = delivery.qos.min(MAX_QOS);
    let packet_id = match qos {
        QoS::AtMostOnce => None,
        QoS::AtLeastOnce => {
            let id = qos_state.allocate();
            qos_state.qos1_unacked.insert(id.get());
            Some(id)
        }
        QoS::ExactlyOnce => {
            let id = qos_state.allocate();
            qos_state.qos2_pubrec.insert(id.get());
            Some(id)
        }
    };

    Packet::Publish(Box::new(Publish {
        dup: false,
        retain: delivery.retain,
        qos: core_to_wire(qos),
        topic: delivery.topic.into(),
        packet_id,
        payload: delivery.payload,
        properties: Some(PublishProperties::default()),
    }))
}

/// Drive a single client connection until it disconnects or errors.
///
/// Transport-agnostic: `io` is any byte stream — a raw [`TcpStream`] or the
/// WebSocket byte adapter ([`crate::ws::WsByteStream`]) — so the exact same MQTT
/// loop serves both the TCP and the WebSocket listener.
///
/// [`TcpStream`]: tokio::net::TcpStream
pub async fn handle<S>(io: S, peer: String, hub: Hub)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (id, mut rx) = hub.register();
    let (mut sink, mut stream) = Framed::new(io, Codec::new(MAX_INBOUND_SIZE, 0)).split();
    let mut connected = false;
    let mut qos_state = OutboundQos::new();
    // QoS 2 packet ids received from this client (as a publisher), awaiting
    // PUBREL — used to deliver the message exactly once despite retransmits.
    let mut incoming_qos2: HashSet<u16> = HashSet::new();
    // The client's Will, published if the connection ends *without* a clean
    // DISCONNECT (network drop, protocol error, or DISCONNECT-with-Will).
    let mut will: Option<Message> = None;
    let mut clean_disconnect = false;

    loop {
        tokio::select! {
            // ---- A packet arrived from this client ----
            incoming = stream.next() => {
                let packet = match incoming {
                    Some(Ok((p, _))) => p,
                    Some(Err(e)) => { warn!(%peer, error = ?e, "protocol error, dropping"); break; }
                    None => { info!(%peer, "client closed connection"); break; }
                };

                match packet {
                    Packet::Connect(connect) => {
                        info!(%peer, client_id = %connect.client_id, "CONNECT");
                        connected = true;
                        // Remember the Will (if any) to publish on abnormal exit.
                        will = connect.last_will.as_ref().map(|w| Message {
                            topic: w.topic.to_string(),
                            payload: w.message.clone(),
                            qos: wire_to_core(w.qos),
                            retain: w.retain,
                        });
                        let ack = ConnectAck {
                            reason_code: ConnectAckReason::Success,
                            ..ConnectAck::default()
                        };
                        if sink.send(Packet::from(ack)).await.is_err() { break; }
                    }

                    Packet::Subscribe(sub) => {
                        if !connected { warn!(%peer, "SUBSCRIBE before CONNECT, dropping"); break; }
                        let mut status = Vec::with_capacity(sub.topic_filters.len());
                        // Retained messages matching a fresh subscription are sent
                        // *after* the SUBACK, marked retained.
                        let mut retained_to_send: Vec<Delivery> = Vec::new();
                        for (filter, opts) in &sub.topic_filters {
                            // Grant the requested QoS, capped at what we support.
                            let granted = wire_to_core(opts.qos).min(MAX_QOS);
                            // `$share/{group}/{filter}` is a shared subscription
                            // (competing consumers); anything else is normal fan-out.
                            if let Some(shared) = SharedSubscription::parse(filter) {
                                info!(%peer, group = %shared.group, filter = %shared.filter.as_str(), ?granted, "SUBSCRIBE (shared)");
                                // Retained messages are not replayed to shared subs.
                                hub.subscribe_shared(shared.group, id, shared.filter, granted);
                                status.push(granted_reason(granted));
                            } else if let Some(tf) = TopicFilter::parse(filter) {
                                info!(%peer, %filter, ?granted, "SUBSCRIBE");
                                for msg in hub.retained_matching(&tf) {
                                    retained_to_send.push(Delivery {
                                        topic: msg.topic,
                                        payload: msg.payload,
                                        qos: msg.qos.min(granted),
                                        retain: true,
                                    });
                                }
                                hub.subscribe(id, tf, granted);
                                status.push(granted_reason(granted));
                            } else {
                                warn!(%peer, %filter, "invalid topic filter");
                                status.push(SubscribeAckReason::TopicFilterInvalid);
                            }
                        }
                        let ack = SubscribeAck {
                            packet_id: sub.packet_id,
                            properties: Vec::new(),
                            reason_string: None,
                            status,
                        };
                        if sink.send(Packet::from(ack)).await.is_err() { break; }

                        // Replay retained messages now that the SUBACK is out.
                        let mut send_failed = false;
                        for delivery in retained_to_send {
                            let packet = build_publish(delivery, &mut qos_state);
                            if sink.send(packet).await.is_err() { send_failed = true; break; }
                        }
                        if send_failed { break; }
                    }

                    Packet::Publish(p) => {
                        if !connected { warn!(%peer, "PUBLISH before CONNECT, dropping"); break; }
                        let topic = p.topic.to_string();
                        let msg_qos = wire_to_core(p.qos);

                        match msg_qos {
                            // Fire-and-forget: just route it.
                            QoS::AtMostOnce => {
                                let n = hub.publish(&topic, &p.payload, msg_qos, p.retain);
                                debug!(%peer, %topic, subscribers = n, "PUBLISH routed (QoS 0)");
                            }

                            // At-least-once: route, then PUBACK the publisher.
                            QoS::AtLeastOnce => {
                                let packet_id = match p.packet_id {
                                    Some(pid) => pid,
                                    None => { warn!(%peer, "QoS 1 PUBLISH without packet id, dropping"); break; }
                                };
                                let n = hub.publish(&topic, &p.payload, msg_qos, p.retain);
                                debug!(%peer, %topic, subscribers = n, "PUBLISH routed (QoS 1)");
                                let ack = PublishAck {
                                    packet_id,
                                    reason_code: PublishAckReason::Success,
                                    properties: Vec::new(),
                                    reason_string: None,
                                };
                                if sink.send(Packet::PublishAck(ack)).await.is_err() { break; }
                            }

                            // Exactly-once, handshake step 1: route on first sight
                            // (dedup retransmits), then PUBREC. Delivery completes
                            // on PUBREL.
                            QoS::ExactlyOnce => {
                                let packet_id = match p.packet_id {
                                    Some(pid) => pid,
                                    None => { warn!(%peer, "QoS 2 PUBLISH without packet id, dropping"); break; }
                                };
                                if incoming_qos2.insert(packet_id.get()) {
                                    let n = hub.publish(&topic, &p.payload, msg_qos, p.retain);
                                    debug!(%peer, %topic, subscribers = n, "PUBLISH routed (QoS 2)");
                                } else {
                                    debug!(%peer, packet_id = packet_id.get(), "duplicate QoS 2 PUBLISH, not re-routed");
                                }
                                if sink.send(pubrec(packet_id)).await.is_err() { break; }
                            }
                        }
                    }

                    // A subscriber acknowledged a QoS 1 delivery we sent it.
                    Packet::PublishAck(ack) => {
                        if qos_state.qos1_unacked.remove(&ack.packet_id.get()) {
                            debug!(%peer, packet_id = ack.packet_id.get(), "PUBACK (QoS 1 confirmed)");
                        } else {
                            debug!(%peer, packet_id = ack.packet_id.get(), "PUBACK for unknown packet id");
                        }
                    }

                    // PUBREC: subscriber received our QoS 2 PUBLISH -> send PUBREL.
                    Packet::PublishReceived(rec) => {
                        let pid = rec.packet_id;
                        if qos_state.qos2_pubrec.remove(&pid.get()) {
                            qos_state.qos2_pubcomp.insert(pid.get());
                        }
                        debug!(%peer, packet_id = pid.get(), "PUBREC -> PUBREL");
                        if sink.send(pubrel(pid)).await.is_err() { break; }
                    }

                    // PUBREL: publisher releases its QoS 2 message -> PUBCOMP.
                    Packet::PublishRelease(rel) => {
                        let pid = rel.packet_id;
                        incoming_qos2.remove(&pid.get());
                        debug!(%peer, packet_id = pid.get(), "PUBREL -> PUBCOMP");
                        if sink.send(pubcomp(pid)).await.is_err() { break; }
                    }

                    // PUBCOMP: subscriber completed the QoS 2 handshake.
                    Packet::PublishComplete(comp) => {
                        if qos_state.qos2_pubcomp.remove(&comp.packet_id.get()) {
                            debug!(%peer, packet_id = comp.packet_id.get(), "PUBCOMP (QoS 2 confirmed)");
                        } else {
                            debug!(%peer, packet_id = comp.packet_id.get(), "PUBCOMP for unknown packet id");
                        }
                    }

                    Packet::PingRequest => {
                        debug!(%peer, "PINGREQ");
                        if sink.send(Packet::PingResponse).await.is_err() { break; }
                    }

                    Packet::Disconnect(d) => {
                        // A normal DISCONNECT discards the Will; any other reason
                        // code means "publish my Will" (MQTT 5 §3.14).
                        if d.reason_code == DisconnectReasonCode::NormalDisconnection {
                            clean_disconnect = true;
                        }
                        info!(%peer, reason = ?d.reason_code, "DISCONNECT");
                        break;
                    }

                    other => {
                        debug!(%peer, kind = other.packet_type(), "unhandled packet (TODO)");
                    }
                }
            }

            // ---- The hub routed a message to us ----
            outgoing = rx.recv() => {
                match outgoing {
                    Some(delivery) => {
                        let packet = build_publish(delivery, &mut qos_state);
                        if sink.send(packet).await.is_err() { break; }
                    }
                    None => break, // our sender was dropped (hub deregistered us)
                }
            }
        }
    }

    // Publish the Will unless the client disconnected cleanly.
    if !clean_disconnect {
        if let Some(w) = will.take() {
            info!(%peer, topic = %w.topic, "publishing Will");
            hub.publish(&w.topic, &w.payload, w.qos, w.retain);
        }
    }

    hub.deregister(id);
    info!(%peer, "connection closed");
}
