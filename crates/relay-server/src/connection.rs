use std::collections::HashSet;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use relay_core::{Acl, ClientId, Message, QoS, SharedSubscription, TopicFilter};
use rmqtt_codec::v5::{
    Codec, ConnectAck, ConnectAckReason, DisconnectReasonCode, Packet, PublishAck, PublishAck2,
    PublishAck2Reason, PublishAckReason, SubscribeAck, SubscribeAckReason, UnsubscribeAck,
    UnsubscribeAckReason,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::auth::AuthConfig;
use crate::hub::{self, Hub};

const MAX_INBOUND_SIZE: u32 = 256 * 1024;

const MAX_QOS: QoS = QoS::ExactlyOnce;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub connect_timeout: Duration,
    pub max_subscriptions_per_client: usize,
}

fn granted_reason(q: QoS) -> SubscribeAckReason {
    match q {
        QoS::AtMostOnce => SubscribeAckReason::GrantedQos0,
        QoS::AtLeastOnce => SubscribeAckReason::GrantedQos1,
        QoS::ExactlyOnce => SubscribeAckReason::GrantedQos2,
    }
}

fn pubrec(packet_id: NonZeroU16) -> Packet {
    Packet::PublishReceived(PublishAck {
        packet_id,
        reason_code: PublishAckReason::Success,
        properties: Vec::new(),
        reason_string: None,
    })
}

fn pubcomp(packet_id: NonZeroU16) -> Packet {
    Packet::PublishComplete(PublishAck2 {
        packet_id,
        reason_code: PublishAck2Reason::Success,
        properties: Vec::new(),
        reason_string: None,
    })
}

fn parse_replay(rest: &str) -> Option<(u64, TopicFilter)> {
    let (from, filter) = rest.split_once('/')?;
    let from = from.parse::<u64>().ok()?;
    let filter = TopicFilter::parse(filter)?;
    Some((from, filter))
}

async fn next_outbound(rx: &mut Option<mpsc::UnboundedReceiver<Packet>>) -> Option<Packet> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

enum Inbound {
    Packet(Packet),
    ProtocolError,
    Closed,
}

async fn next_inbound<St, T, E>(buffered: &mut Option<Packet>, stream: &mut St) -> Inbound
where
    St: StreamExt<Item = Result<(Packet, T), E>> + Unpin,
    E: std::fmt::Debug,
{
    if let Some(packet) = buffered.take() {
        return Inbound::Packet(packet);
    }
    match stream.next().await {
        Some(Ok((packet, _))) => Inbound::Packet(packet),
        Some(Err(e)) => {
            warn!(error = ?e, "decode error");
            Inbound::ProtocolError
        }
        None => Inbound::Closed,
    }
}

pub async fn handle<S>(
    io: S,
    peer: String,
    hub: Hub,
    auth: Arc<AuthConfig>,
    limits: Limits,
    permit: OwnedSemaphorePermit,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _permit = permit;
    let (mut sink, mut stream) = Framed::new(io, Codec::new(MAX_INBOUND_SIZE, 0)).split();

    let mut buffered: Option<Packet> = None;
    let first = match timeout(limits.connect_timeout, next_inbound(&mut buffered, &mut stream)).await {
        Ok(Inbound::Packet(p)) => p,
        Ok(Inbound::ProtocolError) => { debug!(%peer, "protocol error before CONNECT, dropping"); return; }
        Ok(Inbound::Closed) => { info!(%peer, "client closed connection before CONNECT"); return; }
        Err(_) => { warn!(%peer, "no CONNECT before timeout, dropping"); return; }
    };

    let mut session_id: Option<ClientId> = None;
    let mut generation: u64 = 0;
    let mut rx: Option<mpsc::UnboundedReceiver<Packet>> = None;
    let mut access = Acl::default();
    let mut will: Option<Message> = None;
    let mut clean_disconnect = false;
    let mut active_filters: HashSet<String> = HashSet::new();

    buffered = Some(first);

    loop {
        tokio::select! {
            incoming = next_inbound(&mut buffered, &mut stream) => {
                let packet = match incoming {
                    Inbound::Packet(p) => p,
                    Inbound::ProtocolError => { debug!(%peer, "protocol error, dropping"); break; }
                    Inbound::Closed => { info!(%peer, "client closed connection"); break; }
                };

                match packet {
                    Packet::Connect(connect) => {
                        let provided = connect.client_id.to_string();
                        let (client_id, clean_start) = if provided.is_empty() {
                            (format!("anon:{peer}"), true)
                        } else {
                            (provided, connect.clean_start)
                        };
                        info!(%peer, %client_id, clean_start, "CONNECT");

                        match auth.authenticate(connect.password.as_deref()) {
                            Ok(principal) => {
                                info!(%peer, %client_id, identity = %principal.identity, "authenticated");
                                access = principal.acl;
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

                        if let Some(w) = connect.last_will.as_ref() {
                            let will_topic = w.topic.to_string();
                            if !access.can_publish(&will_topic) {
                                warn!(%peer, %client_id, topic = %will_topic, "Will denied by ACL, rejecting CONNECT");
                                let ack = ConnectAck {
                                    session_present: false,
                                    reason_code: ConnectAckReason::NotAuthorized,
                                    ..ConnectAck::default()
                                };
                                let _ = sink.send(Packet::from(ack)).await;
                                break;
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
                        let mut retained_jobs: Vec<(TopicFilter, QoS)> = Vec::new();
                        for (filter, opts) in &sub.topic_filters {
                            let granted = hub::to_core_qos(opts.qos).min(MAX_QOS);
                            let effective = SharedSubscription::parse(filter)
                                .map(|s| s.filter.as_str().to_string())
                                .unwrap_or_else(|| filter.to_string());
                            if !access.can_subscribe(&effective) {
                                warn!(%peer, %filter, "SUBSCRIBE denied by ACL");
                                status.push(SubscribeAckReason::NotAuthorized);
                                continue;
                            }
                            let key = filter.to_string();
                            let is_new = !active_filters.contains(&key);
                            if is_new && active_filters.len() >= limits.max_subscriptions_per_client {
                                warn!(%peer, %filter, limit = limits.max_subscriptions_per_client, "SUBSCRIBE rejected, subscription quota exceeded");
                                status.push(SubscribeAckReason::QuotaExceeded);
                                continue;
                            }
                            if let Some(shared) = SharedSubscription::parse(filter) {
                                info!(%peer, group = %shared.group, filter = %shared.filter.as_str(), ?granted, "SUBSCRIBE (shared)");
                                hub.subscribe_shared(shared.group, id, shared.filter, granted, filter);
                                active_filters.insert(key);
                                status.push(granted_reason(granted));
                            } else if let Some(tf) = TopicFilter::parse(filter) {
                                info!(%peer, %filter, ?granted, "SUBSCRIBE");
                                hub.subscribe(id, tf.clone(), granted, filter);
                                active_filters.insert(key);
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

                        for (tf, granted) in retained_jobs {
                            hub.deliver_retained(id, &tf, granted);
                        }
                    }

                    Packet::Publish(p) => {
                        let Some(id) = session_id else { warn!(%peer, "PUBLISH before CONNECT, dropping"); break; };
                        let topic = p.topic.to_string();
                        let msg_qos = hub::to_core_qos(p.qos);

                        if let Some(rest) = topic.strip_prefix("$replay/") {
                            match parse_replay(rest) {
                                Some((from, filter)) => {
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

                    Packet::PublishAck(ack) => {
                        if let Some(id) = session_id { hub.on_puback(id, ack.packet_id.get()); }
                    }
                    Packet::PublishReceived(rec) => {
                        if let Some(id) = session_id { hub.on_pubrec(id, rec.packet_id.get()); }
                    }
                    Packet::PublishComplete(comp) => {
                        if let Some(id) = session_id { hub.on_pubcomp(id, comp.packet_id.get()); }
                    }

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
                            active_filters.remove(&filter.to_string());
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
                        if d.reason_code == DisconnectReasonCode::NormalDisconnection {
                            clean_disconnect = true;
                        }
                        info!(%peer, reason = ?d.reason_code, "DISCONNECT");
                        break;
                    }

                    other => {
                        debug!(%peer, kind = other.packet_type(), "unhandled packet");
                    }
                }
            }

            outgoing = next_outbound(&mut rx) => {
                match outgoing {
                    Some(packet) => { if sink.send(packet).await.is_err() { break; } }
                    None => break,
                }
            }
        }
    }

    if !clean_disconnect {
        if let Some(w) = will.take() {
            info!(%peer, topic = %w.topic, "publishing Will");
            hub.publish(&w.topic, &w.payload, w.qos, w.retain);
        }
    }

    if let Some(id) = session_id {
        hub.detach(id, generation);
    }
    info!(%peer, "connection closed");
}
