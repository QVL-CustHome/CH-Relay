//! Shared broker state: the persistent **session** layer over `relay-core`'s pure
//! [`Router`] and [`RetainedStore`].
//!
//! `relay-core` stays I/O-free; the hub owns everything that must survive — or
//! interact with — live connections:
//!
//! - a [`Session`] per MQTT `client_id`, holding its delivery channel (when
//!   online), its outbound QoS in-flight queue, its inbound QoS 2 dedup set, and
//!   its packet-id counter. Sessions **outlive** their connection: on a clean
//!   reconnect (`clean_start = false`) the subscriptions and unacknowledged
//!   messages are still there, and the messages are retransmitted.
//! - the [`Router`] (keyed by the stable per-session [`ClientId`]) and the
//!   [`RetainedStore`].
//!
//! All outbound traffic to a client flows through its session's MPSC channel of
//! ready-to-write [`Packet`]s; the session stamps packet ids and records
//! in-flight state so it can retransmit after a reconnect.

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use relay_core::{ClientId, Message, QoS, RetainedStore, Router, SharedSubscription, TopicFilter};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{
    Packet, PublishAck2, PublishAck2Reason, PublishProperties, QoS as WireQoS,
};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::storage::Storage;

/// "Never expire" sentinel for the session-expiry interval (MQTT 5).
const NO_EXPIRY: u32 = u32::MAX;

pub(crate) fn to_core_qos(q: WireQoS) -> QoS {
    match q {
        WireQoS::AtMostOnce => QoS::AtMostOnce,
        WireQoS::AtLeastOnce => QoS::AtLeastOnce,
        WireQoS::ExactlyOnce => QoS::ExactlyOnce,
    }
}

/// PUBREL — releases a QoS 2 message we sent (handshake step 3).
pub(crate) fn pubrel(packet_id: NonZeroU16) -> Packet {
    Packet::PublishRelease(PublishAck2 {
        packet_id,
        reason_code: PublishAck2Reason::Success,
        properties: Vec::new(),
        reason_string: None,
    })
}

fn make_publish(
    topic: &str,
    payload: &Bytes,
    qos: WireQoS,
    packet_id: Option<NonZeroU16>,
    retain: bool,
    dup: bool,
) -> Publish {
    Publish {
        dup,
        retain,
        qos,
        topic: topic.into(),
        packet_id,
        payload: payload.clone(),
        properties: Some(PublishProperties::default()),
    }
}

/// One outbound QoS > 0 message awaiting acknowledgement — kept so it can be
/// retransmitted after a reconnect.
enum Inflight {
    /// QoS 1 PUBLISH sent, awaiting PUBACK.
    Qos1(Publish),
    /// QoS 2 PUBLISH sent, awaiting PUBREC.
    Qos2AwaitRec(Publish),
    /// QoS 2 PUBREL sent, awaiting PUBCOMP.
    Qos2AwaitComp(NonZeroU16),
}

// ---- in-flight queue persistence (opaque blob stored by `storage`) ----
//
// Layout: `next_id (u16 BE)` then, per entry, a tag byte followed by its body:
//   1 = Qos1, 2 = Qos2AwaitRec — `pid(u16) flags(u8) topic_len(u16) topic
//       payload_len(u32) payload` (flags bit 0 = retain)
//   3 = Qos2AwaitComp          — `pid(u16)`

fn encode_publish_entry(out: &mut Vec<u8>, tag: u8, p: &Publish) {
    out.push(tag);
    let pid = p.packet_id.map(|x| x.get()).unwrap_or(0);
    out.extend_from_slice(&pid.to_be_bytes());
    out.push(if p.retain { 1 } else { 0 });
    let topic = p.topic.as_bytes();
    out.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    out.extend_from_slice(topic);
    out.extend_from_slice(&(p.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&p.payload);
}

/// Decode a persisted in-flight blob into `(next_id, queue)`. Returns `None` on
/// any malformed input (the session then simply starts with an empty queue).
fn decode_inflight(blob: &[u8]) -> Option<(u16, VecDeque<Inflight>)> {
    let mut c = std::io::Cursor::new(blob);
    let next_id = read_u16(&mut c)?;
    let mut queue = VecDeque::new();
    while (c.position() as usize) < blob.len() {
        let tag = read_u8(&mut c)?;
        match tag {
            1 | 2 => {
                let pid = NonZeroU16::new(read_u16(&mut c)?)?;
                let retain = read_u8(&mut c)? != 0;
                let topic_len = read_u16(&mut c)? as usize;
                let topic = read_bytes(&mut c, topic_len)?;
                let payload_len = read_u32(&mut c)? as usize;
                let payload = read_bytes(&mut c, payload_len)?;
                let wire = if tag == 1 { WireQoS::AtLeastOnce } else { WireQoS::ExactlyOnce };
                let p = make_publish(
                    &String::from_utf8_lossy(&topic),
                    &Bytes::from(payload),
                    wire,
                    Some(pid),
                    retain,
                    false,
                );
                queue.push_back(if tag == 1 {
                    Inflight::Qos1(p)
                } else {
                    Inflight::Qos2AwaitRec(p)
                });
            }
            3 => {
                let pid = NonZeroU16::new(read_u16(&mut c)?)?;
                queue.push_back(Inflight::Qos2AwaitComp(pid));
            }
            _ => return None,
        }
    }
    Some((next_id, queue))
}

fn read_u8(c: &mut std::io::Cursor<&[u8]>) -> Option<u8> {
    let pos = c.position() as usize;
    let b = c.get_ref().get(pos).copied()?;
    c.set_position((pos + 1) as u64);
    Some(b)
}

fn read_u16(c: &mut std::io::Cursor<&[u8]>) -> Option<u16> {
    Some(u16::from_be_bytes(read_bytes(c, 2)?.try_into().ok()?))
}

fn read_u32(c: &mut std::io::Cursor<&[u8]>) -> Option<u32> {
    Some(u32::from_be_bytes(read_bytes(c, 4)?.try_into().ok()?))
}

fn read_bytes(c: &mut std::io::Cursor<&[u8]>, n: usize) -> Option<Vec<u8>> {
    let pos = c.position() as usize;
    let end = pos.checked_add(n)?;
    let slice = c.get_ref().get(pos..end)?;
    c.set_position(end as u64);
    Some(slice.to_vec())
}

/// Per-`client_id` session. Survives disconnection (subject to expiry).
struct Session {
    /// The MQTT client identifier — the persistence key.
    client_id: String,
    /// Live delivery channel while online; `None` while disconnected.
    tx: Option<mpsc::UnboundedSender<Packet>>,
    /// Next packet identifier to hand out (1..=65535, never 0).
    next_id: u16,
    /// Outbound QoS > 0 messages awaiting acknowledgement, in send order.
    inflight: VecDeque<Inflight>,
    /// Inbound QoS 2 packet ids received and awaiting PUBREL (dedup).
    incoming_qos2: HashSet<u16>,
    /// Session-expiry interval from the latest CONNECT (seconds; 0 = discard on
    /// disconnect, [`NO_EXPIRY`] = keep forever).
    expiry_secs: u32,
    /// Bumped on every (re)connect; guards stale detach/purge of a session that
    /// has since been taken over.
    generation: u64,
}

impl Session {
    fn new(
        client_id: String,
        tx: Option<mpsc::UnboundedSender<Packet>>,
        expiry_secs: u32,
        generation: u64,
    ) -> Self {
        Session {
            client_id,
            tx,
            next_id: 1,
            inflight: VecDeque::new(),
            incoming_qos2: HashSet::new(),
            expiry_secs,
            generation,
        }
    }

    /// Serialize the packet-id counter and the in-flight queue for persistence
    /// (see [`decode_inflight`]). An empty queue still records `next_id` so
    /// packet ids stay monotonic across a restart.
    fn encode_inflight(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.next_id.to_be_bytes());
        for entry in &self.inflight {
            match entry {
                Inflight::Qos1(p) => encode_publish_entry(&mut out, 1, p),
                Inflight::Qos2AwaitRec(p) => encode_publish_entry(&mut out, 2, p),
                Inflight::Qos2AwaitComp(pid) => {
                    out.push(3);
                    out.extend_from_slice(&pid.get().to_be_bytes());
                }
            }
        }
        out
    }

    fn allocate_id(&mut self) -> NonZeroU16 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        if self.next_id == 0 {
            self.next_id = 1;
        }
        NonZeroU16::new(id).expect("packet id invariant: never 0")
    }

    fn send(&self, packet: Packet) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(packet);
        }
    }

    /// Deliver a message to this session at the given (already effective) QoS.
    /// QoS 0 is dropped while offline; QoS 1/2 are recorded as in-flight and
    /// transmitted when online (or held for retransmit on reconnect).
    fn deliver(&mut self, topic: &str, payload: &Bytes, qos: QoS, retain: bool) {
        match qos {
            QoS::AtMostOnce => {
                let p = make_publish(topic, payload, WireQoS::AtMostOnce, None, retain, false);
                self.send(Packet::Publish(Box::new(p)));
            }
            QoS::AtLeastOnce => {
                let pid = self.allocate_id();
                let p = make_publish(topic, payload, WireQoS::AtLeastOnce, Some(pid), retain, false);
                self.inflight.push_back(Inflight::Qos1(p.clone()));
                self.send(Packet::Publish(Box::new(p)));
            }
            QoS::ExactlyOnce => {
                let pid = self.allocate_id();
                let p = make_publish(topic, payload, WireQoS::ExactlyOnce, Some(pid), retain, false);
                self.inflight.push_back(Inflight::Qos2AwaitRec(p.clone()));
                self.send(Packet::Publish(Box::new(p)));
            }
        }
    }

    /// Resend every in-flight message after a reconnect, marked as duplicates.
    fn retransmit(&self) {
        for entry in &self.inflight {
            match entry {
                Inflight::Qos1(p) | Inflight::Qos2AwaitRec(p) => {
                    let mut p = p.clone();
                    p.dup = true;
                    self.send(Packet::Publish(Box::new(p)));
                }
                Inflight::Qos2AwaitComp(pid) => self.send(pubrel(*pid)),
            }
        }
    }

    fn on_puback(&mut self, pid: u16) {
        self.inflight.retain(|e| !matches!(e, Inflight::Qos1(p) if p.packet_id.map(|x| x.get()) == Some(pid)));
    }

    /// PUBREC for one of our QoS 2 PUBLISHes: move it to "awaiting PUBCOMP" and
    /// send the PUBREL.
    fn on_pubrec(&mut self, pid: u16) {
        for entry in self.inflight.iter_mut() {
            if let Inflight::Qos2AwaitRec(p) = entry {
                if p.packet_id.map(|x| x.get()) == Some(pid) {
                    let nz = p.packet_id.expect("qos2 publish has a packet id");
                    *entry = Inflight::Qos2AwaitComp(nz);
                    self.send(pubrel(nz));
                    return;
                }
            }
        }
        // Unknown id: still answer with PUBREL so the peer can complete.
        if let Some(nz) = NonZeroU16::new(pid) {
            self.send(pubrel(nz));
        }
    }

    fn on_pubcomp(&mut self, pid: u16) {
        self.inflight
            .retain(|e| !matches!(e, Inflight::Qos2AwaitComp(x) if x.get() == pid));
    }
}

/// Session table, indexed both by stable [`ClientId`] and by MQTT `client_id`.
#[derive(Default)]
struct SessionTable {
    by_id: HashMap<ClientId, Session>,
    id_of: HashMap<String, ClientId>,
    generation: u64,
}

/// Outcome of [`Hub::connect`]: the connection's handles plus whether a previous
/// session was resumed.
pub struct Connected {
    pub id: ClientId,
    pub generation: u64,
    pub rx: mpsc::UnboundedReceiver<Packet>,
    pub session_present: bool,
}

/// Cloneable handle to the shared broker state.
#[derive(Clone)]
pub struct Hub {
    inner: Arc<Inner>,
}

struct Inner {
    next_id: AtomicU64,
    router: Mutex<Router>,
    retained: Mutex<RetainedStore>,
    sessions: Mutex<SessionTable>,
    storage: Option<Storage>,
}

impl Hub {
    /// Build the broker state. With a [`Storage`], retained messages are loaded
    /// from disk at startup and persisted on change; without one, Relay is fully
    /// in-memory.
    pub fn new(storage: Option<Storage>) -> Self {
        let mut retained = RetainedStore::new();
        let mut router = Router::new();
        let mut table = SessionTable::default();
        let mut next_raw = 1u64;
        // Durable sessions to expire (treating startup as their detach time).
        let mut to_expire: Vec<(ClientId, u32)> = Vec::new();

        if let Some(s) = &storage {
            match s.load_retained() {
                Ok(messages) => {
                    let n = messages.len();
                    for msg in messages {
                        retained.apply(msg);
                    }
                    debug!(retained = n, "loaded retained messages from disk");
                }
                Err(e) => warn!(error = %e, "failed to load retained messages from disk"),
            }

            // In-flight queues, keyed by client_id (decoded per session below).
            let inflight = s.load_inflight().unwrap_or_else(|e| {
                warn!(error = %e, "failed to load in-flight queues from disk");
                Default::default()
            });

            match s.load_sessions() {
                Ok(sessions) => {
                    let n = sessions.len();
                    for ps in sessions {
                        let id = ClientId(next_raw);
                        next_raw += 1;
                        // Rebuild the subscriptions in the router.
                        for (raw, qos) in ps.subscriptions {
                            if let Some(shared) = SharedSubscription::parse(&raw) {
                                router.subscribe_shared(shared.group, id, shared.filter, qos);
                            } else if let Some(tf) = TopicFilter::parse(&raw) {
                                router.subscribe(id, tf, qos);
                            }
                        }
                        // Re-create the session offline (generation 0), restoring
                        // its in-flight queue + packet-id counter if any.
                        let mut session = Session::new(ps.client_id.clone(), None, ps.expiry_secs, 0);
                        if let Some((next_id, queue)) =
                            inflight.get(&ps.client_id).and_then(|b| decode_inflight(b))
                        {
                            session.next_id = next_id;
                            session.inflight = queue;
                        }
                        table.id_of.insert(ps.client_id, id);
                        table.by_id.insert(id, session);
                        if ps.expiry_secs != NO_EXPIRY {
                            to_expire.push((id, ps.expiry_secs));
                        }
                    }
                    debug!(sessions = n, "loaded durable sessions from disk");
                }
                Err(e) => warn!(error = %e, "failed to load sessions from disk"),
            }
        }

        let hub = Hub {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(next_raw),
                router: Mutex::new(router),
                retained: Mutex::new(retained),
                sessions: Mutex::new(table),
                storage,
            }),
        };

        // Schedule expiry for reloaded sessions still offline (generation 0).
        for (id, expiry) in to_expire {
            let hub = hub.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(expiry as u64)).await;
                hub.purge_if_idle(id, 0);
            });
        }

        hub
    }

    /// Attach a connection for `client_id`. Resumes an existing session unless
    /// `clean_start` is set (or none exists). Returns the routing id, the
    /// delivery receiver the connection drains to its socket, and whether a
    /// session was resumed (for CONNACK's `session_present`).
    pub fn connect(&self, client_id: &str, clean_start: bool, expiry_secs: u32) -> Connected {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut table = self.inner.sessions.lock().unwrap();
        table.generation += 1;
        let generation = table.generation;

        let existing = table.id_of.get(client_id).copied();

        if let Some(id) = existing {
            if clean_start {
                // Drop the old session and its subscriptions, start fresh.
                table.by_id.remove(&id);
                self.inner.router.lock().unwrap().remove_client(id);
                self.forget_persisted(client_id);
                let new_id = ClientId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
                table.id_of.insert(client_id.to_string(), new_id);
                table.by_id.insert(
                    new_id,
                    Session::new(client_id.to_string(), Some(tx), expiry_secs, generation),
                );
                self.persist_meta(client_id, expiry_secs);
                return Connected { id: new_id, generation, rx, session_present: false };
            }
            // Resume: re-attach the channel, refresh expiry, retransmit in-flight.
            let session = table.by_id.get_mut(&id).expect("index/table consistency");
            session.tx = Some(tx);
            session.expiry_secs = expiry_secs;
            session.generation = generation;
            session.retransmit();
            self.persist_meta(client_id, expiry_secs);
            return Connected { id, generation, rx, session_present: true };
        }

        // Brand-new session.
        let id = ClientId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        table.id_of.insert(client_id.to_string(), id);
        table.by_id.insert(
            id,
            Session::new(client_id.to_string(), Some(tx), expiry_secs, generation),
        );
        self.persist_meta(client_id, expiry_secs);
        Connected { id, generation, rx, session_present: false }
    }

    /// Persist (expiry > 0) or clear (expiry == 0) a session's durable marker.
    fn persist_meta(&self, client_id: &str, expiry_secs: u32) {
        if let Some(storage) = &self.inner.storage {
            let r = if expiry_secs > 0 {
                storage.put_session(client_id, expiry_secs)
            } else {
                storage.remove_session(client_id)
            };
            if let Err(e) = r {
                warn!(%client_id, error = %e, "failed to persist session");
            }
        }
    }

    /// If `session` is durable and persistence is on, snapshot its in-flight
    /// queue for writing (cheap, CPU-only — meant to be called under the
    /// sessions lock, with the actual disk write deferred until after unlock).
    fn snapshot_inflight(&self, session: &Session) -> Option<(String, Vec<u8>)> {
        if self.inner.storage.is_some() && session.expiry_secs > 0 {
            Some((session.client_id.clone(), session.encode_inflight()))
        } else {
            None
        }
    }

    /// Write a previously-snapshotted in-flight blob to disk.
    fn write_inflight(&self, client_id: &str, blob: &[u8]) {
        if let Some(storage) = &self.inner.storage {
            if let Err(e) = storage.put_inflight(client_id, blob) {
                warn!(%client_id, error = %e, "failed to persist in-flight queue");
            }
        }
    }

    /// Forget a persisted session and its subscriptions (e.g. on clean start).
    fn forget_persisted(&self, client_id: &str) {
        if let Some(storage) = &self.inner.storage {
            if let Err(e) = storage.remove_session(client_id) {
                warn!(%client_id, error = %e, "failed to forget persisted session");
            }
        }
    }

    /// Persist one subscription if the session is durable (expiry > 0).
    fn persist_subscription(&self, id: ClientId, raw: &str, qos: QoS) {
        let Some(storage) = &self.inner.storage else { return };
        let client_id = {
            let table = self.inner.sessions.lock().unwrap();
            table
                .by_id
                .get(&id)
                .filter(|s| s.expiry_secs > 0)
                .map(|s| s.client_id.clone())
        };
        if let Some(client_id) = client_id {
            if let Err(e) = storage.put_subscription(&client_id, raw, qos) {
                warn!(%client_id, error = %e, "failed to persist subscription");
            }
        }
    }

    /// Detach a connection (disconnect/error). If the session-expiry interval is
    /// 0 the session is discarded immediately; otherwise it is kept (offline)
    /// and a purge is scheduled. A no-op if the session was already taken over
    /// by a newer connection (`generation` mismatch).
    pub fn detach(&self, id: ClientId, generation: u64) {
        let mut table = self.inner.sessions.lock().unwrap();
        let session = match table.by_id.get_mut(&id) {
            Some(s) if s.generation == generation => s,
            _ => return, // superseded or already gone
        };
        session.tx = None;
        let expiry = session.expiry_secs;

        if expiry == 0 {
            self.discard(&mut table, id);
        } else if expiry != NO_EXPIRY {
            // Schedule a purge if still idle after the expiry interval.
            let hub = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(expiry as u64)).await;
                hub.purge_if_idle(id, generation);
            });
        }
    }

    /// Purge a session that is still offline and at the expected generation.
    fn purge_if_idle(&self, id: ClientId, generation: u64) {
        let mut table = self.inner.sessions.lock().unwrap();
        let drop_it = matches!(
            table.by_id.get(&id),
            Some(s) if s.generation == generation && s.tx.is_none()
        );
        if drop_it {
            debug!(client = id.0, "session expired, purging");
            self.discard(&mut table, id);
        }
    }

    /// Remove a session entirely: table entries, subscriptions, and disk record.
    fn discard(&self, table: &mut SessionTable, id: ClientId) {
        let client_id = table.by_id.remove(&id).map(|s| s.client_id);
        table.id_of.retain(|_, v| *v != id);
        self.inner.router.lock().unwrap().remove_client(id);
        if let (Some(storage), Some(client_id)) = (&self.inner.storage, client_id) {
            if let Err(e) = storage.remove_session(&client_id) {
                warn!(%client_id, error = %e, "failed to remove persisted session");
            }
        }
    }

    /// Register a normal (fan-out) subscription at granted `qos`. `raw` is the
    /// filter string as sent, persisted for durable sessions.
    pub fn subscribe(&self, id: ClientId, filter: TopicFilter, qos: QoS, raw: &str) {
        self.inner.router.lock().unwrap().subscribe(id, filter, qos);
        self.persist_subscription(id, raw, qos);
    }

    /// Register a shared subscription: `id` joins `group` with `filter` at `qos`.
    /// `raw` is the `$share/...` string as sent, persisted for durable sessions.
    pub fn subscribe_shared(&self, group: String, id: ClientId, filter: TopicFilter, qos: QoS, raw: &str) {
        self.inner
            .router
            .lock()
            .unwrap()
            .subscribe_shared(group, id, filter, qos);
        self.persist_subscription(id, raw, qos);
    }

    /// Remove a subscription (`raw` is the filter string as sent). Returns
    /// whether it existed, and clears it from disk for durable sessions.
    pub fn unsubscribe(&self, id: ClientId, raw: &str) -> bool {
        let removed = {
            let mut router = self.inner.router.lock().unwrap();
            if let Some(shared) = SharedSubscription::parse(raw) {
                router.unsubscribe_shared(&shared.group, id, shared.filter.as_str())
            } else {
                router.unsubscribe(id, raw)
            }
        };

        if let Some(storage) = &self.inner.storage {
            let client_id = {
                let table = self.inner.sessions.lock().unwrap();
                table
                    .by_id
                    .get(&id)
                    .filter(|s| s.expiry_secs > 0)
                    .map(|s| s.client_id.clone())
            };
            if let Some(client_id) = client_id {
                if let Err(e) = storage.remove_subscription(&client_id, raw) {
                    warn!(%client_id, error = %e, "failed to remove persisted subscription");
                }
            }
        }
        removed
    }

    /// Replay retained messages matching `filter` to a freshly-subscribed
    /// session, capped at its granted QoS and flagged retained.
    pub fn deliver_retained(&self, id: ClientId, filter: &TopicFilter, granted: QoS) {
        let retained = self.inner.retained.lock().unwrap().matching(filter);
        if retained.is_empty() {
            return;
        }
        let snapshot = {
            let mut table = self.inner.sessions.lock().unwrap();
            match table.by_id.get_mut(&id) {
                Some(session) => {
                    let mut any_qos_gt0 = false;
                    for msg in retained {
                        let effective = msg.qos.min(granted);
                        any_qos_gt0 |= effective != QoS::AtMostOnce;
                        session.deliver(&msg.topic, &msg.payload, effective, true);
                    }
                    if any_qos_gt0 {
                        self.snapshot_inflight(session)
                    } else {
                        None
                    }
                }
                None => None,
            }
        };
        if let Some((client_id, blob)) = snapshot {
            self.write_inflight(&client_id, &blob);
        }
    }

    /// Deliver a PUBLISH to its recipients: every matching normal subscriber,
    /// plus one member per matching share group (round-robin). If `retain` is
    /// set, updates the retained store first. Returns the number of recipient
    /// sessions.
    pub fn publish(&self, topic: &str, payload: &Bytes, msg_qos: QoS, retain: bool) -> usize {
        if retain {
            self.inner.retained.lock().unwrap().apply(Message {
                topic: topic.to_string(),
                payload: payload.clone(),
                qos: msg_qos,
                retain: true,
            });
            // Persist (or clear) the retained message so it survives a restart.
            if let Some(storage) = &self.inner.storage {
                if let Err(e) = storage.put_retained(topic, payload, msg_qos) {
                    warn!(%topic, error = %e, "failed to persist retained message");
                }
            }
        }

        // Resolve targets, releasing the router lock before touching sessions.
        let targets = { self.inner.router.lock().unwrap().route(topic) };
        if targets.is_empty() {
            return 0;
        }
        let mut to_persist: Vec<(String, Vec<u8>)> = Vec::new();
        let mut delivered = 0;
        {
            let mut table = self.inner.sessions.lock().unwrap();
            for (id, granted) in targets {
                if let Some(session) = table.by_id.get_mut(&id) {
                    let effective = msg_qos.min(granted);
                    session.deliver(topic, payload, effective, false);
                    delivered += 1;
                    // Only QoS > 0 changes the in-flight queue.
                    if effective != QoS::AtMostOnce {
                        to_persist.extend(self.snapshot_inflight(session));
                    }
                }
            }
        }
        for (client_id, blob) in to_persist {
            self.write_inflight(&client_id, &blob);
        }
        delivered
    }

    // ---- acknowledgements for messages we sent (outbound QoS) ----

    pub fn on_puback(&self, id: ClientId, pid: u16) {
        let snapshot = {
            let mut table = self.inner.sessions.lock().unwrap();
            table.by_id.get_mut(&id).and_then(|s| {
                s.on_puback(pid);
                self.snapshot_inflight(s)
            })
        };
        if let Some((client_id, blob)) = snapshot {
            self.write_inflight(&client_id, &blob);
        }
    }

    pub fn on_pubrec(&self, id: ClientId, pid: u16) {
        let snapshot = {
            let mut table = self.inner.sessions.lock().unwrap();
            table.by_id.get_mut(&id).and_then(|s| {
                s.on_pubrec(pid);
                self.snapshot_inflight(s)
            })
        };
        if let Some((client_id, blob)) = snapshot {
            self.write_inflight(&client_id, &blob);
        }
    }

    pub fn on_pubcomp(&self, id: ClientId, pid: u16) {
        let snapshot = {
            let mut table = self.inner.sessions.lock().unwrap();
            table.by_id.get_mut(&id).and_then(|s| {
                s.on_pubcomp(pid);
                self.snapshot_inflight(s)
            })
        };
        if let Some((client_id, blob)) = snapshot {
            self.write_inflight(&client_id, &blob);
        }
    }

    // ---- inbound QoS 2 dedup (we are the receiver) ----

    /// Record an inbound QoS 2 packet id; returns `true` the first time (so the
    /// message is routed exactly once despite retransmits).
    pub fn inbound_qos2_seen(&self, id: ClientId, pid: u16) -> bool {
        match self.inner.sessions.lock().unwrap().by_id.get_mut(&id) {
            Some(s) => s.incoming_qos2.insert(pid),
            None => true,
        }
    }

    /// Clear an inbound QoS 2 packet id on PUBREL.
    pub fn inbound_qos2_release(&self, id: ClientId, pid: u16) {
        if let Some(s) = self.inner.sessions.lock().unwrap().by_id.get_mut(&id) {
            s.incoming_qos2.remove(&pid);
        }
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new(None)
    }
}
