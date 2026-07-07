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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use relay_core::{ClientId, Message, QoS, RetainedStore, Router, SharedSubscription, TopicFilter};
use rmqtt_codec::types::Publish;
use rmqtt_codec::v5::{Packet, PublishAck2, PublishAck2Reason, PublishProperties, QoS as WireQoS};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::storage::{spawn_persist_worker, PersistHandle, PersistOp, Storage};

/// "Never expire" sentinel for the session-expiry interval (MQTT 5).
const NO_EXPIRY: u32 = u32::MAX;

/// Redelivery / dead-letter policy for unacknowledged QoS 1/2 messages.
#[derive(Clone, Copy)]
pub struct RetryConfig {
    /// Total delivery attempts before dead-lettering (1 = no retry).
    pub max_attempts: u32,
    /// Base back-off; doubles each attempt, capped at `cap`.
    pub base: Duration,
    /// Upper bound on the back-off.
    pub cap: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            max_attempts: 5,
            base: Duration::from_secs(5),
            cap: Duration::from_secs(60),
        }
    }
}

/// Back-off before the `attempt`-th delivery (1-based): `base * 2^(attempt-1)`,
/// capped at `cap`.
fn backoff(cfg: &RetryConfig, attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(20);
    cfg.base.saturating_mul(1u32 << shift).min(cfg.cap)
}

/// `$dlq/...` is Relay's dead-letter namespace; such messages are never
/// themselves dead-lettered (no infinite recursion).
fn is_dlq_topic(topic: &str) -> bool {
    topic.starts_with("$dlq/")
}

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

/// An in-flight message plus its redelivery bookkeeping.
struct InflightEntry {
    state: Inflight,
    /// Delivery attempts made so far (initial send counts as 1).
    attempts: u32,
    /// Earliest instant the next redelivery is due.
    next_due: Instant,
}

/// A message removed from an in-flight queue because delivery ultimately failed
/// — handed to the dead-letter path.
struct DeadMsg {
    publish: Publish,
    attempts: u32,
}

// ---- in-flight queue persistence (opaque blob stored by `storage`) ----
//
// Layout: `next_id (u16 BE)` then, per entry, `tag(u8) attempts(u32 BE)` and:
//   1 = Qos1, 2 = Qos2AwaitRec — `pid(u16) flags(u8) topic_len(u16) topic
//       payload_len(u32) payload` (flags bit 0 = retain)
//   3 = Qos2AwaitComp          — `pid(u16)`
// `next_due` is not persisted; it is recomputed from `attempts` on reload.

fn encode_publish_entry(out: &mut Vec<u8>, tag: u8, attempts: u32, p: &Publish) {
    out.push(tag);
    out.extend_from_slice(&attempts.to_be_bytes());
    let pid = p.packet_id.map(|x| x.get()).unwrap_or(0);
    out.extend_from_slice(&pid.to_be_bytes());
    out.push(if p.retain { 1 } else { 0 });
    let topic = p.topic.as_bytes();
    out.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    out.extend_from_slice(topic);
    out.extend_from_slice(&(p.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&p.payload);
}

/// Decode a persisted in-flight blob into `(next_id, queue)`, recomputing each
/// entry's `next_due` from its attempt count. Returns `None` on any malformed
/// input (the session then simply starts with an empty queue).
fn decode_inflight(
    blob: &[u8],
    now: Instant,
    cfg: &RetryConfig,
) -> Option<(u16, VecDeque<InflightEntry>)> {
    let mut c = std::io::Cursor::new(blob);
    let next_id = read_u16(&mut c)?;
    let mut queue = VecDeque::new();
    while (c.position() as usize) < blob.len() {
        let tag = read_u8(&mut c)?;
        let attempts = read_u32(&mut c)?;
        let next_due = now + backoff(cfg, attempts.max(1));
        let state = match tag {
            1 | 2 => {
                let pid = NonZeroU16::new(read_u16(&mut c)?)?;
                let retain = read_u8(&mut c)? != 0;
                let topic_len = read_u16(&mut c)? as usize;
                let topic = read_bytes(&mut c, topic_len)?;
                let payload_len = read_u32(&mut c)? as usize;
                let payload = read_bytes(&mut c, payload_len)?;
                let wire = if tag == 1 {
                    WireQoS::AtLeastOnce
                } else {
                    WireQoS::ExactlyOnce
                };
                let p = make_publish(
                    &String::from_utf8_lossy(&topic),
                    &Bytes::from(payload),
                    wire,
                    Some(pid),
                    retain,
                    false,
                );
                if tag == 1 {
                    Inflight::Qos1(p)
                } else {
                    Inflight::Qos2AwaitRec(p)
                }
            }
            3 => Inflight::Qos2AwaitComp(NonZeroU16::new(read_u16(&mut c)?)?),
            _ => return None,
        };
        queue.push_back(InflightEntry {
            state,
            attempts,
            next_due,
        });
    }
    Some((next_id, queue))
}

/// Serialize a dead-lettered message for persistence/replay. Layout:
/// `ts(u64) attempts(u32) reason_len(u16) reason client_len(u16) client
/// topic_len(u16) topic qos(u8) payload`.
fn encode_dead_letter(
    client_id: &str,
    topic: &str,
    reason: &str,
    attempts: u32,
    qos: u8,
    payload: &Bytes,
) -> Vec<u8> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut out = Vec::new();
    out.extend_from_slice(&ts.to_be_bytes());
    out.extend_from_slice(&attempts.to_be_bytes());
    out.extend_from_slice(&(reason.len() as u16).to_be_bytes());
    out.extend_from_slice(reason.as_bytes());
    out.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    out.extend_from_slice(client_id.as_bytes());
    out.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    out.extend_from_slice(topic.as_bytes());
    out.push(qos);
    out.extend_from_slice(payload);
    out
}

/// Serialize a logged event. Layout: `ts(u64) qos(u8) topic_len(u16) topic
/// payload`. The offset is the storage key, not part of the blob.
fn encode_event(topic: &str, qos: u8, payload: &Bytes) -> Vec<u8> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut out = Vec::new();
    out.extend_from_slice(&ts.to_be_bytes());
    out.push(qos);
    out.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    out.extend_from_slice(topic.as_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode a logged event blob into `(topic, payload, qos)`.
fn decode_event(blob: &[u8]) -> Option<(String, Bytes, QoS)> {
    let mut c = std::io::Cursor::new(blob);
    let _ts = read_bytes(&mut c, 8)?;
    let qos = QoS::from_u8(read_u8(&mut c)?).unwrap_or(QoS::AtMostOnce);
    let topic_len = read_u16(&mut c)? as usize;
    let topic = read_bytes(&mut c, topic_len)?;
    let pos = c.position() as usize;
    let payload = Bytes::copy_from_slice(c.get_ref().get(pos..)?);
    Some((String::from_utf8_lossy(&topic).into_owned(), payload, qos))
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
    inflight: VecDeque<InflightEntry>,
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
            match &entry.state {
                Inflight::Qos1(p) => encode_publish_entry(&mut out, 1, entry.attempts, p),
                Inflight::Qos2AwaitRec(p) => encode_publish_entry(&mut out, 2, entry.attempts, p),
                Inflight::Qos2AwaitComp(pid) => {
                    out.push(3);
                    out.extend_from_slice(&entry.attempts.to_be_bytes());
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
    /// QoS 0 is dropped while offline; QoS 1/2 are recorded as in-flight (with
    /// the first retry due after one back-off) and transmitted when online.
    fn deliver(
        &mut self,
        topic: &str,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        now: Instant,
        cfg: &RetryConfig,
    ) {
        match qos {
            QoS::AtMostOnce => {
                let p = make_publish(topic, payload, WireQoS::AtMostOnce, None, retain, false);
                self.send(Packet::Publish(Box::new(p)));
            }
            QoS::AtLeastOnce | QoS::ExactlyOnce => {
                let pid = self.allocate_id();
                let wire = if qos == QoS::AtLeastOnce {
                    WireQoS::AtLeastOnce
                } else {
                    WireQoS::ExactlyOnce
                };
                let p = make_publish(topic, payload, wire, Some(pid), retain, false);
                let state = if qos == QoS::AtLeastOnce {
                    Inflight::Qos1(p.clone())
                } else {
                    Inflight::Qos2AwaitRec(p.clone())
                };
                self.inflight.push_back(InflightEntry {
                    state,
                    attempts: 1,
                    next_due: now + backoff(cfg, 1),
                });
                self.send(Packet::Publish(Box::new(p)));
            }
        }
    }

    /// Resend every in-flight message after a reconnect (marked as duplicates)
    /// and re-arm each entry's retry clock so the timer takes over.
    fn retransmit(&mut self, now: Instant, cfg: &RetryConfig) {
        for entry in self.inflight.iter_mut() {
            entry.next_due = now + backoff(cfg, entry.attempts.max(1));
            let pkt = match &entry.state {
                Inflight::Qos1(p) | Inflight::Qos2AwaitRec(p) => {
                    let mut p = p.clone();
                    p.dup = true;
                    Packet::Publish(Box::new(p))
                }
                Inflight::Qos2AwaitComp(pid) => pubrel(*pid),
            };
            if let Some(tx) = &self.tx {
                let _ = tx.send(pkt);
            }
        }
    }

    fn on_puback(&mut self, pid: u16) {
        self.inflight.retain(
            |e| !matches!(&e.state, Inflight::Qos1(p) if p.packet_id.map(|x| x.get()) == Some(pid)),
        );
    }

    /// PUBREC for one of our QoS 2 PUBLISHes: move it to "awaiting PUBCOMP" and
    /// send the PUBREL.
    fn on_pubrec(&mut self, pid: u16) {
        for entry in self.inflight.iter_mut() {
            if let Inflight::Qos2AwaitRec(p) = &entry.state {
                if p.packet_id.map(|x| x.get()) == Some(pid) {
                    let nz = p.packet_id.expect("qos2 publish has a packet id");
                    entry.state = Inflight::Qos2AwaitComp(nz);
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
            .retain(|e| !matches!(&e.state, Inflight::Qos2AwaitComp(x) if x.get() == pid));
    }

    /// Drive redelivery for this (online) session: resend due, unacknowledged
    /// messages as duplicates with back-off, and remove those that have run out
    /// of attempts (returned for dead-lettering). A QoS 2 message past PUBREC was
    /// already delivered, so it is never dead-lettered — its PUBREL is just
    /// nudged again. Returns the dead messages and whether the queue changed.
    fn tick_retries(&mut self, now: Instant, cfg: &RetryConfig) -> (Vec<DeadMsg>, bool) {
        let mut dead = Vec::new();
        let mut to_send: Vec<Packet> = Vec::new();
        let mut changed = false;
        let mut i = 0;
        while i < self.inflight.len() {
            if now < self.inflight[i].next_due {
                i += 1;
                continue;
            }
            let attempts = self.inflight[i].attempts;
            let dead_now = match &self.inflight[i].state {
                Inflight::Qos1(p) | Inflight::Qos2AwaitRec(p) => {
                    attempts >= cfg.max_attempts && !is_dlq_topic(&p.topic)
                }
                Inflight::Qos2AwaitComp(_) => false,
            };
            if dead_now {
                if let Some(entry) = self.inflight.remove(i) {
                    if let Inflight::Qos1(p) | Inflight::Qos2AwaitRec(p) = entry.state {
                        dead.push(DeadMsg {
                            publish: p,
                            attempts,
                        });
                    }
                }
                changed = true;
                // do not advance `i`: the next entry shifted into this slot
                continue;
            }
            let pkt = match &self.inflight[i].state {
                Inflight::Qos1(p) | Inflight::Qos2AwaitRec(p) => {
                    let mut p = p.clone();
                    p.dup = true;
                    Packet::Publish(Box::new(p))
                }
                Inflight::Qos2AwaitComp(pid) => pubrel(*pid),
            };
            self.inflight[i].attempts = attempts + 1;
            self.inflight[i].next_due = now + backoff(cfg, attempts + 1);
            to_send.push(pkt);
            changed = true;
            i += 1;
        }
        for pkt in to_send {
            self.send(pkt);
        }
        (dead, changed)
    }

    /// Remove every undelivered QoS 1/2 message (those still awaiting the first
    /// confirmation) for dead-lettering when the session is torn down on expiry.
    fn drain_undelivered(&mut self) -> Vec<DeadMsg> {
        let mut dead = Vec::new();
        self.inflight.retain(|e| match &e.state {
            Inflight::Qos1(p) | Inflight::Qos2AwaitRec(p) if !is_dlq_topic(&p.topic) => {
                dead.push(DeadMsg {
                    publish: p.clone(),
                    attempts: e.attempts,
                });
                false
            }
            _ => true,
        });
        dead
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

/// A point-in-time snapshot of broker activity, for the monitoring dashboard.
pub struct Stats {
    pub clients_online: usize,
    pub clients_total: usize,
    pub subscriptions: usize,
    pub retained: usize,
    pub dead_letters: u64,
    pub events: u64,
    pub next_offset: u64,
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
    /// Read-only access to the on-disk store (startup loads + monitoring/replay).
    /// All durable writes go through `persist` so no redb I/O runs on the runtime.
    storage: Option<Arc<Storage>>,
    /// Queues durable writes onto the dedicated persistence worker thread.
    persist: Option<PersistHandle>,
    retry: RetryConfig,
    /// Event-log retention (max rows; 0 disables logging). Replay reads this log.
    event_log_max: u64,
}

impl Hub {
    /// Build the broker state. With a [`Storage`], retained messages are loaded
    /// from disk at startup and persisted on change; without one, Relay is fully
    /// in-memory. `retry` governs redelivery back-off and dead-lettering;
    /// `event_log_max` bounds the replayable event log (0 disables it).
    pub fn new(storage: Option<Storage>, retry: RetryConfig, event_log_max: u64) -> Self {
        let storage = storage.map(Arc::new);
        let now = Instant::now();
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
                        let mut session =
                            Session::new(ps.client_id.clone(), None, ps.expiry_secs, 0);
                        if let Some((next_id, queue)) = inflight
                            .get(&ps.client_id)
                            .and_then(|b| decode_inflight(b, now, &retry))
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

        let persist = storage.clone().map(spawn_persist_worker);

        let hub = Hub {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(next_raw),
                router: Mutex::new(router),
                retained: Mutex::new(retained),
                sessions: Mutex::new(table),
                storage,
                persist,
                retry,
                event_log_max,
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

        // Redelivery timer: periodically resend due, unacknowledged messages
        // (with back-off) and dead-letter those that run out of attempts.
        {
            let hub = hub.clone();
            let period = retry.base.max(Duration::from_millis(100));
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(period);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    hub.run_retries();
                }
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
                return Connected {
                    id: new_id,
                    generation,
                    rx,
                    session_present: false,
                };
            }
            // Resume: re-attach the channel, refresh expiry, retransmit in-flight.
            let session = table.by_id.get_mut(&id).expect("index/table consistency");
            session.tx = Some(tx);
            session.expiry_secs = expiry_secs;
            session.generation = generation;
            session.retransmit(Instant::now(), &self.inner.retry);
            self.persist_meta(client_id, expiry_secs);
            return Connected {
                id,
                generation,
                rx,
                session_present: true,
            };
        }

        // Brand-new session.
        let id = ClientId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        table.id_of.insert(client_id.to_string(), id);
        table.by_id.insert(
            id,
            Session::new(client_id.to_string(), Some(tx), expiry_secs, generation),
        );
        self.persist_meta(client_id, expiry_secs);
        Connected {
            id,
            generation,
            rx,
            session_present: false,
        }
    }

    /// Queue a durable write onto the persistence worker. A no-op when running
    /// in-memory. Never performs disk I/O on the caller's thread.
    fn enqueue(&self, op: PersistOp) {
        if let Some(persist) = &self.inner.persist {
            persist.enqueue(op);
        }
    }

    pub async fn flush(&self) {
        if let Some(persist) = &self.inner.persist {
            persist.flush().await;
        }
    }

    /// Persist (expiry > 0) or clear (expiry == 0) a session's durable marker.
    fn persist_meta(&self, client_id: &str, expiry_secs: u32) {
        if expiry_secs > 0 {
            self.enqueue(PersistOp::PutSession {
                client_id: client_id.to_string(),
                expiry_secs,
            });
        } else {
            self.enqueue(PersistOp::RemoveSession {
                client_id: client_id.to_string(),
            });
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

    /// Queue a previously-snapshotted in-flight blob for writing.
    fn write_inflight(&self, client_id: String, blob: Vec<u8>) {
        self.enqueue(PersistOp::PutInflight { client_id, blob });
    }

    /// Forget a persisted session and its subscriptions (e.g. on clean start).
    fn forget_persisted(&self, client_id: &str) {
        self.enqueue(PersistOp::RemoveSession {
            client_id: client_id.to_string(),
        });
    }

    /// Persist one subscription if the session is durable (expiry > 0).
    fn persist_subscription(&self, id: ClientId, raw: &str, qos: QoS) {
        if self.inner.persist.is_none() {
            return;
        }
        let client_id = {
            let table = self.inner.sessions.lock().unwrap();
            table
                .by_id
                .get(&id)
                .filter(|s| s.expiry_secs > 0)
                .map(|s| s.client_id.clone())
        };
        if let Some(client_id) = client_id {
            self.enqueue(PersistOp::PutSubscription {
                client_id,
                raw: raw.to_string(),
                qos,
            });
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
    /// Any messages it never managed to deliver are dead-lettered first.
    fn purge_if_idle(&self, id: ClientId, generation: u64) {
        let dead: Vec<(String, DeadMsg)> = {
            let mut table = self.inner.sessions.lock().unwrap();
            let drop_it = matches!(
                table.by_id.get(&id),
                Some(s) if s.generation == generation && s.tx.is_none()
            );
            if !drop_it {
                return;
            }
            debug!(client = id.0, "session expired, purging");
            let dead = table
                .by_id
                .get_mut(&id)
                .map(|s| {
                    let cid = s.client_id.clone();
                    s.drain_undelivered()
                        .into_iter()
                        .map(move |d| (cid.clone(), d))
                        .collect()
                })
                .unwrap_or_else(Vec::new);
            self.discard(&mut table, id);
            dead
        };
        for (client_id, msg) in dead {
            self.dead_letter(&client_id, &msg, "session_expired");
        }
    }

    /// Drive the redelivery timer over every online session: resend due
    /// unacknowledged messages with back-off, dead-letter those out of attempts,
    /// and persist the queues that changed (durable sessions only).
    fn run_retries(&self) {
        let now = Instant::now();
        let mut dead: Vec<(String, DeadMsg)> = Vec::new();
        let mut to_persist: Vec<(String, Vec<u8>)> = Vec::new();
        {
            let mut table = self.inner.sessions.lock().unwrap();
            for session in table.by_id.values_mut() {
                if session.tx.is_none() {
                    continue; // offline: wait for reconnect, don't retry
                }
                let (dead_msgs, changed) = session.tick_retries(now, &self.inner.retry);
                for d in dead_msgs {
                    dead.push((session.client_id.clone(), d));
                }
                if changed {
                    to_persist.extend(self.snapshot_inflight(session));
                }
            }
        }
        for (client_id, blob) in to_persist {
            self.write_inflight(client_id, blob);
        }
        for (client_id, msg) in dead {
            self.dead_letter(&client_id, &msg, "max_delivery_attempts_exceeded");
        }
    }

    /// Dead-letter a message that could not be delivered: persist it (for later
    /// replay) and republish it on `$dlq/{client_id}/{original_topic}` so an
    /// operator subscribed to `$dlq/#` sees it in real time. Never recurses on a
    /// message already in the `$dlq/` namespace.
    fn dead_letter(&self, client_id: &str, msg: &DeadMsg, reason: &str) {
        let original_topic = msg.publish.topic.as_ref();
        if is_dlq_topic(original_topic) {
            return;
        }
        let qos = to_core_qos(msg.publish.qos);
        warn!(%client_id, topic = original_topic, attempts = msg.attempts, reason, "dead-lettering message");

        if self.inner.persist.is_some() {
            let blob = encode_dead_letter(
                client_id,
                original_topic,
                reason,
                msg.attempts,
                qos as u8,
                &msg.publish.payload,
            );
            self.enqueue(PersistOp::AppendDeadLetter { blob });
        }

        let dlq_topic = format!("$dlq/{client_id}/{original_topic}");
        // Republish at the original QoS so the dead-letter consumer gets the same
        // delivery guarantee. Detailed metadata lives in the persisted record.
        self.publish(&dlq_topic, &msg.publish.payload, qos, false);
    }

    /// Remove a session entirely: table entries, subscriptions, and disk record.
    fn discard(&self, table: &mut SessionTable, id: ClientId) {
        let client_id = table.by_id.remove(&id).map(|s| s.client_id);
        table.id_of.retain(|_, v| *v != id);
        self.inner.router.lock().unwrap().remove_client(id);
        if let Some(client_id) = client_id {
            self.enqueue(PersistOp::RemoveSession { client_id });
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
    pub fn subscribe_shared(
        &self,
        group: String,
        id: ClientId,
        filter: TopicFilter,
        qos: QoS,
        raw: &str,
    ) {
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

        if self.inner.persist.is_some() {
            let client_id = {
                let table = self.inner.sessions.lock().unwrap();
                table
                    .by_id
                    .get(&id)
                    .filter(|s| s.expiry_secs > 0)
                    .map(|s| s.client_id.clone())
            };
            if let Some(client_id) = client_id {
                self.enqueue(PersistOp::RemoveSubscription {
                    client_id,
                    raw: raw.to_string(),
                });
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
                    let now = Instant::now();
                    let mut any_qos_gt0 = false;
                    for msg in retained {
                        let effective = msg.qos.min(granted);
                        any_qos_gt0 |= effective != QoS::AtMostOnce;
                        session.deliver(
                            &msg.topic,
                            &msg.payload,
                            effective,
                            true,
                            now,
                            &self.inner.retry,
                        );
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
            self.write_inflight(client_id, blob);
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
            self.enqueue(PersistOp::PutRetained {
                topic: topic.to_string(),
                payload: payload.clone(),
                qos: msg_qos,
            });
        }

        // Append to the replayable event log (application topics only; `$`
        // control/system topics such as `$dlq/...` are not journalled).
        if self.inner.event_log_max > 0 && !topic.starts_with('$') {
            let blob = encode_event(topic, msg_qos as u8, payload);
            self.enqueue(PersistOp::AppendEvent {
                blob,
                retention: self.inner.event_log_max,
            });
        }

        // Resolve targets, releasing the router lock before touching sessions.
        let targets = { self.inner.router.lock().unwrap().route(topic) };
        if targets.is_empty() {
            return 0;
        }
        let mut to_persist: Vec<(String, Vec<u8>)> = Vec::new();
        let mut delivered = 0;
        {
            let now = Instant::now();
            let mut table = self.inner.sessions.lock().unwrap();
            for (id, granted) in targets {
                if let Some(session) = table.by_id.get_mut(&id) {
                    let effective = msg_qos.min(granted);
                    session.deliver(topic, payload, effective, false, now, &self.inner.retry);
                    delivered += 1;
                    // Only QoS > 0 changes the in-flight queue.
                    if effective != QoS::AtMostOnce {
                        to_persist.extend(self.snapshot_inflight(session));
                    }
                }
            }
        }
        for (client_id, blob) in to_persist {
            self.write_inflight(client_id, blob);
        }
        delivered
    }

    /// Replay logged events whose offset is >= `from` and whose topic matches
    /// `filter`, to the requesting session. Each replayed PUBLISH is sent at
    /// QoS 0 (a bulk read, outside the live delivery guarantees) and tagged with
    /// its offset in the `x-replay-offset` user property. Returns how many were
    /// replayed. A no-op without persistence (the event log lives on disk).
    pub fn replay(&self, id: ClientId, from: u64, filter: &TopicFilter) -> usize {
        let Some(storage) = &self.inner.storage else {
            return 0;
        };
        let events = match storage.load_events(from) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "failed to read event log for replay");
                return 0;
            }
        };
        let mut table = self.inner.sessions.lock().unwrap();
        let Some(session) = table.by_id.get_mut(&id) else {
            return 0;
        };
        let mut replayed = 0;
        for (offset, blob) in events {
            let Some((topic, payload, _qos)) = decode_event(&blob) else {
                continue;
            };
            if !filter.matches(&topic) {
                continue;
            }
            let mut properties = PublishProperties::default();
            properties.user_properties =
                vec![("x-replay-offset".into(), offset.to_string().into())];
            let p = Publish {
                dup: false,
                retain: false,
                qos: WireQoS::AtMostOnce,
                topic: topic.into(),
                packet_id: None,
                payload,
                properties: Some(properties),
            };
            session.send(Packet::Publish(Box::new(p)));
            replayed += 1;
        }
        replayed
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
            self.write_inflight(client_id, blob);
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
            self.write_inflight(client_id, blob);
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
            self.write_inflight(client_id, blob);
        }
    }

    /// Snapshot current broker activity for the monitoring dashboard.
    pub fn stats(&self) -> Stats {
        let (clients_online, clients_total) = {
            let table = self.inner.sessions.lock().unwrap();
            let online = table.by_id.values().filter(|s| s.tx.is_some()).count();
            (online, table.by_id.len())
        };
        let subscriptions = self.inner.router.lock().unwrap().subscription_count();
        let retained = self.inner.retained.lock().unwrap().len();
        let (dead_letters, events, next_offset) = match &self.inner.storage {
            Some(s) => (
                s.dead_letter_count().unwrap_or(0),
                s.event_count().unwrap_or(0),
                s.next_offset().unwrap_or(0),
            ),
            None => (0, 0, 0),
        };
        Stats {
            clients_online,
            clients_total,
            subscriptions,
            retained,
            dead_letters,
            events,
            next_offset,
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
        Self::new(None, RetryConfig::default(), 0)
    }
}
