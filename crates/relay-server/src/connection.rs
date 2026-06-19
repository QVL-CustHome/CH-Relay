//! Per-connection MQTT loop, built on the `rmqtt-codec` v5 tokio codec.
//!
//! The `Framed` stream is split into a reader (`stream`) and a writer (`sink`).
//! A `tokio::select!` interleaves two sources:
//! - **incoming**: packets from this client (CONNECT, SUBSCRIBE, PUBLISH, acks…);
//! - **outgoing**: ready-to-write [`Packet`]s the client's [`Session`] pushes
//!   onto its channel — outbound PUBLISHes, retransmits, PUBRELs, retained.
//!
//! The connection itself is thin: all durable QoS state (packet-id counter,
//! in-flight queue, inbound QoS 2 dedup, subscriptions) lives in the [`Hub`]'s
//! session so it survives reconnects. The connection registers its session on
//! CONNECT, forwards acknowledgements to the hub, writes its own immediate
//! responses (CONNACK/SUBACK/PUBACK/PINGRESP and the inbound QoS 2 PUBREC/
//! PUBCOMP) directly, and detaches the session on exit.
//!
//! Transport-agnostic: `io` is any byte stream — a raw `TcpStream` or the
//! WebSocket byte adapter ([`crate::ws::WsByteStream`]) — so the same loop serves
//! the TCP and the WebSocket listener.
//!
//! [`Session`]: crate::hub
//! [`Hub`]: crate::hub::Hub

use std::num::NonZeroU16;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use relay_core::{Acl, ClientId, Message, QoS, SharedSubscription, TopicFilter};
use rmqtt_codec::v5::{
    Codec, ConnectAck, ConnectAckReason, DisconnectReasonCode, Packet, PublishAck, PublishAck2,
    PublishAck2Reason, PublishAckReason, SubscribeAck, SubscribeAckReason, UnsubscribeAck,
    UnsubscribeAckReason,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::auth::AuthConfig;
use crate::hub::{self, Hub};

/// A connection's authorization context, established at CONNECT.
enum Access {
    /// Auth disabled — every topic is permitted (legacy open broker).
    Open,
    /// Authenticated client, limited to its templated topic ACL.
    Authed(Acl),
}

impl Access {
    fn can_publish(&self, topic: &str) -> bool {
        match self {
            Access::Open => true,
            Access::Authed(acl) => acl.can_publish(topic),
        }
    }

    fn can_subscribe(&self, filter: &str) -> bool {
        match self {
            Access::Open => true,
            Access::Authed(acl) => acl.can_subscribe(filter),
        }
    }
}

/// Maximum inbound packet size we accept (256 KiB); 0 outbound = unlimited.
const MAX_INBOUND_SIZE: u32 = 256 * 1024;

/// Highest QoS the broker grants/delivers.
const MAX_QOS: QoS = QoS::ExactlyOnce;

fn granted_reason(q: QoS) -> SubscribeAckReason {
    match q {
        QoS::AtMostOnce => SubscribeAckReason::GrantedQos0,
        QoS::AtLeastOnce => SubscribeAckReason::GrantedQos1,
        QoS::ExactlyOnce => SubscribeAckReason::GrantedQos2,
    }
}

/// PUBREC — we (as receiver) acknowledge a QoS 2 PUBLISH (handshake step 2).
fn pubrec(packet_id: NonZeroU16) -> Packet {
    Packet::PublishReceived(PublishAck {
        packet_id,
        reason_code: PublishAckReason::Success,
        properties: Vec::new(),
        reason_string: None,
    })
}

/// PUBCOMP — we (as receiver) complete a QoS 2 handshake (step 4).
fn pubcomp(packet_id: NonZeroU16) -> Packet {
    Packet::PublishComplete(PublishAck2 {
        packet_id,
        reason_code: PublishAck2Reason::Success,
        properties: Vec::new(),
        reason_string: None,
    })
}

/// Parse a `$replay/{from_offset}/{filter}` control topic (the `$replay/`
/// prefix already stripped) into a starting offset and a validated topic filter.
fn parse_replay(rest: &str) -> Option<(u64, TopicFilter)> {
    let (from, filter) = rest.split_once('/')?;
    let from = from.parse::<u64>().ok()?;
    let filter = TopicFilter::parse(filter)?;
    Some((from, filter))
}

/// Await the next outbound packet, or never resolve if not yet connected (no
/// session channel exists before CONNECT).
async fn next_outbound(rx: &mut Option<mpsc::UnboundedReceiver<Packet>>) -> Option<Packet> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

/// Drive a single client connection until it disconnects or errors.
pub async fn handle<S>(io: S, peer: String, hub: Hub, auth: Option<Arc<AuthConfig>>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut sink, mut stream) = Framed::new(io, Codec::new(MAX_INBOUND_SIZE, 0)).split();

    let mut session_id: Option<ClientId> = None;
    let mut generation: u64 = 0;
    let mut rx: Option<mpsc::UnboundedReceiver<Packet>> = None;
    // Authorization context, set on a successful CONNECT.
    let mut access = Access::Open;
    // The client's Will, published if the connection ends without a clean DISCONNECT.
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
                        // An empty client id can't address a durable session, so
                        // give it a unique, clean one tied to this connection.
                        let provided = connect.client_id.to_string();
                        let (client_id, clean_start) = if provided.is_empty() {
                            (format!("anon:{peer}"), true)
                        } else {
                            (provided, connect.clean_start)
                        };
                        info!(%peer, %client_id, clean_start, "CONNECT");

                        // Authenticate before creating any session state. The JWT
                        // is carried as the MQTT password; with no [auth] config the
                        // broker stays open.
                        if let Some(cfg) = &auth {
                            match cfg.authenticate(connect.password.as_deref()) {
                                Ok(principal) => {
                                    info!(%peer, %client_id, identity = %principal.identity, "authenticated");
                                    access = Access::Authed(principal.acl);
                                }
                                Err(e) => {
                                    warn!(%peer, %client_id, ?e, "authentication failed, rejecting CONNECT");
                                    let ack = ConnectAck {
                                        session_present: false,
                                        reason_code: ConnectAckReason::NotAuthorized,
                                        ..ConnectAck::default()
                                    };
                                    let _ = sink.send(Packet::from(ack)).await;
                                    break;
                                }
                            }
                        }

                        let conn = hub.connect(&client_id, clean_start, connect.session_expiry_interval_secs);
                        session_id = Some(conn.id);
                        generation = conn.generation;
                        rx = Some(conn.rx);

                        will = connect.last_will.as_ref().map(|w| Message {
                            topic: w.topic.to_string(),
                            payload: w.message.clone(),
                            qos: hub::to_core_qos(w.qos),
                            retain: w.retain,
                        });

                        let ack = ConnectAck {
                            session_present: conn.session_present,
                            reason_code: ConnectAckReason::Success,
                            ..ConnectAck::default()
                        };
                        if sink.send(Packet::from(ack)).await.is_err() { break; }
                    }

                    Packet::Subscribe(sub) => {
                        let Some(id) = session_id else { warn!(%peer, "SUBSCRIBE before CONNECT, dropping"); break; };
                        let mut status = Vec::with_capacity(sub.topic_filters.len());
                        // Retained replay happens *after* the SUBACK, via the session.
                        let mut retained_jobs: Vec<(TopicFilter, QoS)> = Vec::new();
                        for (filter, opts) in &sub.topic_filters {
                            let granted = hub::to_core_qos(opts.qos).min(MAX_QOS);
                            // ACL: a shared subscription is checked on its inner filter.
                            let effective = SharedSubscription::parse(filter)
                                .map(|s| s.filter.as_str().to_string())
                                .unwrap_or_else(|| filter.to_string());
                            if !access.can_subscribe(&effective) {
                                warn!(%peer, %filter, "SUBSCRIBE denied by ACL");
                                status.push(SubscribeAckReason::NotAuthorized);
                                continue;
                            }
                            if let Some(shared) = SharedSubscription::parse(filter) {
                                info!(%peer, group = %shared.group, filter = %shared.filter.as_str(), ?granted, "SUBSCRIBE (shared)");
                                hub.subscribe_shared(shared.group, id, shared.filter, granted, filter);
                                status.push(granted_reason(granted));
                            } else if let Some(tf) = TopicFilter::parse(filter) {
                                info!(%peer, %filter, ?granted, "SUBSCRIBE");
                                hub.subscribe(id, tf.clone(), granted, filter);
                                retained_jobs.push((tf, granted));
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

                        // Now that the SUBACK is out, replay retained messages
                        // (they flow through the session's channel).
                        for (tf, granted) in retained_jobs {
                            hub.deliver_retained(id, &tf, granted);
                        }
                    }

                    Packet::Publish(p) => {
                        let Some(id) = session_id else { warn!(%peer, "PUBLISH before CONNECT, dropping"); break; };
                        let topic = p.topic.to_string();
                        let msg_qos = hub::to_core_qos(p.qos);

                        // `$replay/{from}/{filter}` is a control request: stream
                        // logged events back to this client instead of routing.
                        if let Some(rest) = topic.strip_prefix("$replay/") {
                            match parse_replay(rest) {
                                Some((from, filter)) => {
                                    // Replay reads topics back to the client: gate it on
                                    // the subscribe ACL for the requested filter.
                                    if access.can_subscribe(filter.as_str()) {
                                        hub.flush().await;
                                        let n = hub.replay(id, from, &filter);
                                        info!(%peer, from, filter = %filter.as_str(), replayed = n, "REPLAY");
                                    } else {
                                        warn!(%peer, filter = %filter.as_str(), "REPLAY denied by ACL");
                                    }
                                }
                                None => warn!(%peer, %topic, "invalid $replay request"),
                            }
                            // Acknowledge the control publish so its QoS handshake completes.
                            match (msg_qos, p.packet_id) {
                                (QoS::AtLeastOnce, Some(packet_id)) => {
                                    let ack = PublishAck {
                                        packet_id,
                                        reason_code: PublishAckReason::Success,
                                        properties: Vec::new(),
                                        reason_string: None,
                                    };
                                    if sink.send(Packet::PublishAck(ack)).await.is_err() { break; }
                                }
                                (QoS::ExactlyOnce, Some(packet_id)) => {
                                    if sink.send(pubrec(packet_id)).await.is_err() { break; }
                                }
                                _ => {}
                            }
                        } else if !access.can_publish(&topic) {
                            warn!(%peer, %topic, "PUBLISH denied by ACL");
                            match (msg_qos, p.packet_id) {
                                (QoS::AtLeastOnce, Some(packet_id)) => {
                                    let ack = PublishAck {
                                        packet_id,
                                        reason_code: PublishAckReason::NotAuthorized,
                                        properties: Vec::new(),
                                        reason_string: None,
                                    };
                                    if sink.send(Packet::PublishAck(ack)).await.is_err() { break; }
                                }
                                (QoS::ExactlyOnce, Some(packet_id)) => {
                                    let rec = PublishAck {
                                        packet_id,
                                        reason_code: PublishAckReason::NotAuthorized,
                                        properties: Vec::new(),
                                        reason_string: None,
                                    };
                                    if sink.send(Packet::PublishReceived(rec)).await.is_err() { break; }
                                }
                                _ => {}
                            }
                        } else {
                            match msg_qos {
                                QoS::AtMostOnce => {
                                    let n = hub.publish(&topic, &p.payload, msg_qos, p.retain);
                                    debug!(%peer, %topic, recipients = n, "PUBLISH routed (QoS 0)");
                                }
                                QoS::AtLeastOnce => {
                                    let Some(packet_id) = p.packet_id else { warn!(%peer, "QoS 1 PUBLISH without packet id"); break; };
                                    let n = hub.publish(&topic, &p.payload, msg_qos, p.retain);
                                    debug!(%peer, %topic, recipients = n, "PUBLISH routed (QoS 1)");
                                    let ack = PublishAck {
                                        packet_id,
                                        reason_code: PublishAckReason::Success,
                                        properties: Vec::new(),
                                        reason_string: None,
                                    };
                                    if sink.send(Packet::PublishAck(ack)).await.is_err() { break; }
                                }
                                QoS::ExactlyOnce => {
                                    let Some(packet_id) = p.packet_id else { warn!(%peer, "QoS 2 PUBLISH without packet id"); break; };
                                    if hub.inbound_qos2_seen(id, packet_id.get()) {
                                        let n = hub.publish(&topic, &p.payload, msg_qos, p.retain);
                                        debug!(%peer, %topic, recipients = n, "PUBLISH routed (QoS 2)");
                                    } else {
                                        debug!(%peer, packet_id = packet_id.get(), "duplicate QoS 2 PUBLISH, not re-routed");
                                    }
                                    if sink.send(pubrec(packet_id)).await.is_err() { break; }
                                }
                            }
                        }
                    }

                    // Acknowledgements for messages we (the broker) sent out.
                    Packet::PublishAck(ack) => {
                        if let Some(id) = session_id { hub.on_puback(id, ack.packet_id.get()); }
                    }
                    Packet::PublishReceived(rec) => {
                        // Subscriber received our QoS 2 PUBLISH; the session emits PUBREL.
                        if let Some(id) = session_id { hub.on_pubrec(id, rec.packet_id.get()); }
                    }
                    Packet::PublishComplete(comp) => {
                        if let Some(id) = session_id { hub.on_pubcomp(id, comp.packet_id.get()); }
                    }

                    // A publisher releasing its inbound QoS 2 message.
                    Packet::PublishRelease(rel) => {
                        if let Some(id) = session_id {
                            hub.inbound_qos2_release(id, rel.packet_id.get());
                        }
                        if sink.send(pubcomp(rel.packet_id)).await.is_err() { break; }
                    }

                    Packet::Unsubscribe(unsub) => {
                        let Some(id) = session_id else { warn!(%peer, "UNSUBSCRIBE before CONNECT, dropping"); break; };
                        let mut status = Vec::with_capacity(unsub.topic_filters.len());
                        for filter in &unsub.topic_filters {
                            let existed = hub.unsubscribe(id, filter);
                            info!(%peer, %filter, existed, "UNSUBSCRIBE");
                            status.push(if existed {
                                UnsubscribeAckReason::Success
                            } else {
                                UnsubscribeAckReason::NoSubscriptionExisted
                            });
                        }
                        let ack = UnsubscribeAck {
                            packet_id: unsub.packet_id,
                            properties: Vec::new(),
                            reason_string: None,
                            status,
                        };
                        if sink.send(Packet::UnsubscribeAck(ack)).await.is_err() { break; }
                    }

                    Packet::PingRequest => {
                        debug!(%peer, "PINGREQ");
                        hub.flush().await;
                        if sink.send(Packet::PingResponse).await.is_err() { break; }
                    }

                    Packet::Disconnect(d) => {
                        // A normal DISCONNECT discards the Will; any other reason
                        // means "publish my Will" (MQTT 5 §3.14).
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

            // ---- The session pushed a packet for us to write ----
            outgoing = next_outbound(&mut rx) => {
                match outgoing {
                    Some(packet) => { if sink.send(packet).await.is_err() { break; } }
                    None => break, // our session channel closed (taken over / purged)
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

    // Detach the session (keeps or discards it per its expiry interval).
    if let Some(id) = session_id {
        hub.detach(id, generation);
    }
    info!(%peer, "connection closed");
}
