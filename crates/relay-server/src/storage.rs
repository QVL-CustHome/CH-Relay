//! On-disk persistence (V2).
//!
//! A thin wrapper over a [`redb`] embedded key-value store — pure Rust, a single
//! file, no native dependencies. For now it persists **retained messages** so a
//! topic's last known value survives a broker restart; sessions and in-flight
//! queues are the next things to persist.
//!
//! Tables:
//! - `retained` — key = topic, value = `[qos_byte] ++ payload`.
//! - `sessions` — key = `client_id`, value = the session-expiry interval. Marks
//!   a durable session so a `clean_start = false` reconnect after a restart
//!   still finds it (`session_present`).
//! - `subscriptions` — key = `client_id ++ '\0' ++ raw_filter`, value =
//!   `[granted_qos]`. `raw_filter` is the subscription string exactly as the
//!   client sent it (a topic filter, or a `$share/group/filter`), so it is
//!   re-parsed on load the same way the live path parses it.
//! - `inflight` — key = `client_id`, value = an opaque blob produced by the hub
//!   (its outbound QoS 1/2 in-flight queue plus its packet-id counter). The
//!   storage layer stores and returns the bytes verbatim; only the hub knows the
//!   encoding. Restored on reconnect so unacknowledged messages survive a
//!   broker restart.
//! - `dead_letters` — key = auto-incrementing sequence, value = an opaque hub
//!   blob. Append-only record of undeliverable messages, for replay.
//! - `events` — key = global monotonic offset, value = an opaque hub blob (one
//!   per published message). Append-only with bounded retention; the source of
//!   truth for offset-based replay.
//!
//! Writes are durable (each is its own committed transaction); the store is
//! loaded back into memory at startup.
//!
//! [`RetainedStore`]: relay_core::RetainedStore

use std::collections::HashMap;
use std::path::Path;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread;

use bytes::Bytes;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use relay_core::{Message, QoS};
use tokio::sync::oneshot;
use tracing::warn;

const RETAINED: TableDefinition<&str, &[u8]> = TableDefinition::new("retained");
const SESSIONS: TableDefinition<&str, u32> = TableDefinition::new("sessions");
const SUBSCRIPTIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("subscriptions");
const INFLIGHT: TableDefinition<&str, &[u8]> = TableDefinition::new("inflight");
/// Dead-lettered messages, keyed by an auto-incrementing sequence (insertion
/// order) so they can be replayed later. Value is an opaque blob from the hub.
const DEAD_LETTERS: TableDefinition<u64, &[u8]> = TableDefinition::new("dead_letters");
/// The event log: every published message keyed by a global, monotonic offset.
/// Append-only with bounded retention (oldest offsets pruned). Value is an
/// opaque blob from the hub. Replay reads a range from a starting offset.
const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("events");

/// Separator between `client_id` and the raw filter in a subscription key.
const SEP: char = '\u{0}';

/// A durable session reloaded from disk at startup.
pub struct PersistedSession {
    pub client_id: String,
    pub expiry_secs: u32,
    /// `(raw subscription string, granted QoS)` pairs.
    pub subscriptions: Vec<(String, QoS)>,
}

/// Handle to the on-disk store.
pub struct Storage {
    db: Database,
}

impl Storage {
    /// Open (creating if needed) the store at `path`, ensuring its tables exist.
    pub fn open(path: &Path) -> Result<Self, redb::Error> {
        let db = Database::create(path)?;
        // Materialise the tables so later read-only transactions never fail.
        let txn = db.begin_write()?;
        txn.open_table(RETAINED)?;
        txn.open_table(SESSIONS)?;
        txn.open_table(SUBSCRIPTIONS)?;
        txn.open_table(INFLIGHT)?;
        txn.open_table(DEAD_LETTERS)?;
        txn.open_table(EVENTS)?;
        txn.commit()?;
        Ok(Storage { db })
    }

    /// Persist (or, for an empty payload, clear) the retained message for `topic`.
    pub fn put_retained(&self, topic: &str, payload: &Bytes, qos: QoS) -> Result<(), redb::Error> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(RETAINED)?;
            if payload.is_empty() {
                table.remove(topic)?;
            } else {
                let mut value = Vec::with_capacity(1 + payload.len());
                value.push(qos as u8);
                value.extend_from_slice(payload);
                table.insert(topic, value.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Load every retained message back into memory (called at startup).
    pub fn load_retained(&self) -> Result<Vec<Message>, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(RETAINED)?;
        let mut out = Vec::new();
        for row in table.iter()? {
            let (key, value) = row?;
            let bytes = value.value();
            if bytes.is_empty() {
                continue;
            }
            let qos = QoS::from_u8(bytes[0]).unwrap_or(QoS::AtMostOnce);
            out.push(Message {
                topic: key.value().to_string(),
                payload: Bytes::copy_from_slice(&bytes[1..]),
                qos,
                retain: true,
            });
        }
        Ok(out)
    }

    /// Mark a durable session (its expiry interval) as present.
    pub fn put_session(&self, client_id: &str, expiry_secs: u32) -> Result<(), redb::Error> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(SESSIONS)?;
            table.insert(client_id, expiry_secs)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Drop a session and all of its subscriptions.
    pub fn remove_session(&self, client_id: &str) -> Result<(), redb::Error> {
        let prefix = format!("{client_id}{SEP}");
        let txn = self.db.begin_write()?;
        {
            let mut sessions = txn.open_table(SESSIONS)?;
            sessions.remove(client_id)?;

            let mut subs = txn.open_table(SUBSCRIPTIONS)?;
            let keys: Vec<String> = subs
                .iter()?
                .filter_map(|row| row.ok())
                .map(|(k, _)| k.value().to_string())
                .filter(|k| k.starts_with(&prefix))
                .collect();
            for key in keys {
                subs.remove(key.as_str())?;
            }

            let mut inflight = txn.open_table(INFLIGHT)?;
            inflight.remove(client_id)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Persist a single subscription of `client_id` (`raw` is the filter string
    /// as the client sent it).
    pub fn put_subscription(&self, client_id: &str, raw: &str, qos: QoS) -> Result<(), redb::Error> {
        let key = format!("{client_id}{SEP}{raw}");
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(SUBSCRIPTIONS)?;
            table.insert(key.as_str(), [qos as u8].as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Remove a single persisted subscription of `client_id`.
    pub fn remove_subscription(&self, client_id: &str, raw: &str) -> Result<(), redb::Error> {
        let key = format!("{client_id}{SEP}{raw}");
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(SUBSCRIPTIONS)?;
            table.remove(key.as_str())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Persist a session's in-flight queue blob (opaque to storage). An empty
    /// blob clears the row.
    pub fn put_inflight(&self, client_id: &str, blob: &[u8]) -> Result<(), redb::Error> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(INFLIGHT)?;
            if blob.is_empty() {
                table.remove(client_id)?;
            } else {
                table.insert(client_id, blob)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Load every persisted in-flight blob, keyed by `client_id` (called at
    /// startup). The bytes are returned verbatim for the hub to decode.
    pub fn load_inflight(&self) -> Result<HashMap<String, Vec<u8>>, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(INFLIGHT)?;
        let mut out = HashMap::new();
        for row in table.iter()? {
            let (key, value) = row?;
            out.insert(key.value().to_string(), value.value().to_vec());
        }
        Ok(out)
    }

    /// Append a dead-lettered message (opaque blob from the hub), assigning it
    /// the next sequence number so insertion order — hence replay order — is
    /// preserved.
    pub fn append_dead_letter(&self, blob: &[u8]) -> Result<(), redb::Error> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(DEAD_LETTERS)?;
            let next = match table.last()? {
                Some((k, _)) => k.value().wrapping_add(1),
                None => 0,
            };
            table.insert(next, blob)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Append a published message to the event log, returning its global offset.
    /// `retention` bounds the log: once it would exceed `retention` rows, the
    /// oldest offsets are pruned (0 = unbounded). Offsets are never reused.
    pub fn append_event(&self, blob: &[u8], retention: u64) -> Result<u64, redb::Error> {
        let txn = self.db.begin_write()?;
        let offset;
        {
            let mut table = txn.open_table(EVENTS)?;
            offset = match table.last()? {
                Some((k, _)) => k.value().wrapping_add(1),
                None => 0,
            };
            table.insert(offset, blob)?;
            if retention > 0 {
                while table.len()? > retention {
                    let Some(oldest) = table.first()?.map(|(k, _)| k.value()) else { break };
                    table.remove(oldest)?;
                }
            }
        }
        txn.commit()?;
        Ok(offset)
    }

    /// Read logged events with offset >= `from`, in offset order, for replay.
    /// Returns `(offset, opaque blob)` pairs for the hub to decode and filter.
    pub fn load_events(&self, from: u64) -> Result<Vec<(u64, Vec<u8>)>, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(EVENTS)?;
        let mut out = Vec::new();
        for row in table.range(from..)? {
            let (k, v) = row?;
            out.push((k.value(), v.value().to_vec()));
        }
        Ok(out)
    }

    /// Number of dead-lettered messages currently stored (for monitoring).
    pub fn dead_letter_count(&self) -> Result<u64, redb::Error> {
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(DEAD_LETTERS)?.len()?)
    }

    /// Number of events currently retained in the log (for monitoring).
    pub fn event_count(&self) -> Result<u64, redb::Error> {
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(EVENTS)?.len()?)
    }

    /// The next offset that would be assigned (i.e. one past the newest), for
    /// monitoring the log's high-water mark.
    pub fn next_offset(&self) -> Result<u64, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(EVENTS)?;
        let next = match table.last()? {
            Some((k, _)) => k.value().wrapping_add(1),
            None => 0,
        };
        Ok(next)
    }

    /// Load every durable session with its subscriptions (called at startup).
    pub fn load_sessions(&self) -> Result<Vec<PersistedSession>, redb::Error> {
        let txn = self.db.begin_read()?;

        // Group subscriptions by client_id.
        let mut subs_by_client: HashMap<String, Vec<(String, QoS)>> = HashMap::new();
        let subs = txn.open_table(SUBSCRIPTIONS)?;
        for row in subs.iter()? {
            let (key, value) = row?;
            let key = key.value();
            let bytes = value.value();
            if let Some((client_id, raw)) = key.split_once(SEP) {
                let qos = bytes.first().and_then(|b| QoS::from_u8(*b)).unwrap_or(QoS::AtMostOnce);
                subs_by_client
                    .entry(client_id.to_string())
                    .or_default()
                    .push((raw.to_string(), qos));
            }
        }

        let sessions = txn.open_table(SESSIONS)?;
        let mut out = Vec::new();
        for row in sessions.iter()? {
            let (key, value) = row?;
            let client_id = key.value().to_string();
            let subscriptions = subs_by_client.remove(&client_id).unwrap_or_default();
            out.push(PersistedSession {
                expiry_secs: value.value(),
                client_id,
                subscriptions,
            });
        }
        Ok(out)
    }
}

/// A single durable write to apply against [`Storage`]. Queued by the hub and
/// drained, in order, by the persistence worker thread.
pub enum PersistOp {
    PutSession { client_id: String, expiry_secs: u32 },
    RemoveSession { client_id: String },
    PutSubscription { client_id: String, raw: String, qos: QoS },
    RemoveSubscription { client_id: String, raw: String },
    PutInflight { client_id: String, blob: Vec<u8> },
    PutRetained { topic: String, payload: Bytes, qos: QoS },
    AppendDeadLetter { blob: Vec<u8> },
    AppendEvent { blob: Vec<u8>, retention: u64 },
    Flush { ack: oneshot::Sender<()> },
}

/// Hub-side handle to the persistence worker: enqueues [`PersistOp`]s without
/// blocking the async runtime. Cloneable; the worker stops once every handle is
/// dropped and the channel drains.
#[derive(Clone)]
pub struct PersistHandle {
    tx: Sender<PersistOp>,
}

impl PersistHandle {
    /// Queue a durable write. Never blocks on disk I/O. A send failure means the
    /// worker thread is gone (shutdown); the op is dropped after a warning.
    pub fn enqueue(&self, op: PersistOp) {
        if self.tx.send(op).is_err() {
            warn!("persistence worker is gone; dropping write");
        }
    }

    pub async fn flush(&self) {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(PersistOp::Flush { ack: ack_tx }).is_err() {
            warn!("persistence worker is gone; flush is a no-op");
            return;
        }
        let _ = ack_rx.await;
    }
}

/// Own a [`Storage`] on a dedicated OS thread and apply queued [`PersistOp`]s in
/// FIFO order, keeping all redb I/O off the Tokio runtime while preserving write
/// ordering (a single sequential writer). Returns the hub-side handle.
pub fn spawn_persist_worker(storage: Arc<Storage>) -> PersistHandle {
    let (tx, rx) = std::sync::mpsc::channel::<PersistOp>();
    thread::Builder::new()
        .name("relay-persist".into())
        .spawn(move || run_persist_worker(storage, rx))
        .expect("spawn persistence worker thread");
    PersistHandle { tx }
}

fn run_persist_worker(storage: Arc<Storage>, rx: Receiver<PersistOp>) {
    while let Ok(op) = rx.recv() {
        apply_persist_op(&storage, op);
    }
}

fn apply_persist_op(storage: &Storage, op: PersistOp) {
    let name = persist_op_name(&op);
    let result = match op {
        PersistOp::Flush { ack } => {
            let _ = ack.send(());
            return;
        }
        PersistOp::PutSession { client_id, expiry_secs } => {
            storage.put_session(&client_id, expiry_secs)
        }
        PersistOp::RemoveSession { client_id } => storage.remove_session(&client_id),
        PersistOp::PutSubscription { client_id, raw, qos } => {
            storage.put_subscription(&client_id, &raw, qos)
        }
        PersistOp::RemoveSubscription { client_id, raw } => {
            storage.remove_subscription(&client_id, &raw)
        }
        PersistOp::PutInflight { client_id, blob } => storage.put_inflight(&client_id, &blob),
        PersistOp::PutRetained { topic, payload, qos } => {
            storage.put_retained(&topic, &payload, qos)
        }
        PersistOp::AppendDeadLetter { blob } => storage.append_dead_letter(&blob),
        PersistOp::AppendEvent { blob, retention } => {
            storage.append_event(&blob, retention).map(|_| ())
        }
    };
    if let Err(e) = result {
        warn!(op = name, error = %e, "persistence write failed");
    }
}

fn persist_op_name(op: &PersistOp) -> &'static str {
    match op {
        PersistOp::PutSession { .. } => "put_session",
        PersistOp::RemoveSession { .. } => "remove_session",
        PersistOp::PutSubscription { .. } => "put_subscription",
        PersistOp::RemoveSubscription { .. } => "remove_subscription",
        PersistOp::PutInflight { .. } => "put_inflight",
        PersistOp::PutRetained { .. } => "put_retained",
        PersistOp::AppendDeadLetter { .. } => "append_dead_letter",
        PersistOp::AppendEvent { .. } => "append_event",
        PersistOp::Flush { .. } => "flush",
    }
}
