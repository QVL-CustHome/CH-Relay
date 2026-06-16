//! Per-connection MQTT loop, built on the `rmqtt-codec` v5 tokio codec.
//!
//! The `Framed` stream is split into a reader (`stream`) and a writer (`sink`).
//! A `tokio::select!` interleaves two sources:
//! - **incoming**: packets from this client (CONNECT, SUBSCRIBE, PUBLISH, …);
//! - **outgoing**: packets the [`Hub`] routes *to* this client because it
//!   subscribed to a topic someone else published on.
//!
//! V1 status: CONNECT/CONNACK, PINGREQ/PINGRESP, DISCONNECT, SUBSCRIBE/SUBACK,
//! and **QoS 0 pub/sub fan-out** are wired. QoS 1/2 acks, retained, and shared
//! subscriptions are the next steps.

use futures::{SinkExt, StreamExt};
use relay_core::{SharedSubscription, TopicFilter};
use rmqtt_codec::v5::{
    Codec, ConnectAck, ConnectAckReason, Packet, SubscribeAck, SubscribeAckReason,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::hub::Hub;

/// Maximum inbound packet size we accept (256 KiB); 0 outbound = unlimited.
const MAX_INBOUND_SIZE: u32 = 256 * 1024;

/// Drive a single TCP client connection until it disconnects or errors.
pub async fn handle(socket: TcpStream, peer: String, hub: Hub) {
    let (id, mut rx) = hub.register();
    let (mut sink, mut stream) = Framed::new(socket, Codec::new(MAX_INBOUND_SIZE, 0)).split();
    let mut connected = false;

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
                        let ack = ConnectAck {
                            reason_code: ConnectAckReason::Success,
                            ..ConnectAck::default()
                        };
                        if sink.send(Packet::from(ack)).await.is_err() { break; }
                    }

                    Packet::Subscribe(sub) => {
                        if !connected { warn!(%peer, "SUBSCRIBE before CONNECT, dropping"); break; }
                        let mut status = Vec::with_capacity(sub.topic_filters.len());
                        for (filter, _opts) in &sub.topic_filters {
                            // `$share/{group}/{filter}` is a shared subscription
                            // (competing consumers); anything else is normal fan-out.
                            if let Some(shared) = SharedSubscription::parse(filter) {
                                info!(%peer, group = %shared.group, filter = %shared.filter.as_str(), "SUBSCRIBE (shared)");
                                hub.subscribe_shared(shared.group, id, shared.filter);
                                status.push(SubscribeAckReason::GrantedQos0);
                            } else if let Some(tf) = TopicFilter::parse(filter) {
                                info!(%peer, %filter, "SUBSCRIBE");
                                hub.subscribe(id, tf);
                                status.push(SubscribeAckReason::GrantedQos0);
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
                    }

                    Packet::Publish(p) => {
                        if !connected { warn!(%peer, "PUBLISH before CONNECT, dropping"); break; }
                        let topic = p.topic.to_string();
                        // QoS 0 fan-out: forward the packet as-is to matching subscribers.
                        let n = hub.publish(&topic, &Packet::Publish(p));
                        debug!(%peer, %topic, subscribers = n, "PUBLISH routed");
                        // TODO: QoS 1/2 acknowledgement back to the publisher.
                    }

                    Packet::PingRequest => {
                        debug!(%peer, "PINGREQ");
                        if sink.send(Packet::PingResponse).await.is_err() { break; }
                    }

                    Packet::Disconnect(_) => { info!(%peer, "DISCONNECT"); break; }

                    other => {
                        debug!(%peer, kind = other.packet_type(), "unhandled packet (TODO)");
                    }
                }
            }

            // ---- The hub routed a message to us ----
            outgoing = rx.recv() => {
                match outgoing {
                    Some(packet) => { if sink.send(packet).await.is_err() { break; } }
                    None => break, // our sender was dropped (hub deregistered us)
                }
            }
        }
    }

    hub.deregister(id);
    info!(%peer, "connection closed");
}
