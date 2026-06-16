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
//!
//! Writes are durable (each is its own committed transaction); the store is
//! loaded back into memory at startup. In-flight QoS 1/2 queues are **not** yet
//! persisted — they are lost on a broker restart.
//!
//! [`RetainedStore`]: relay_core::RetainedStore

use std::collections::HashMap;
use std::path::Path;

use bytes::Bytes;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use relay_core::{Message, QoS};

const RETAINED: TableDefinition<&str, &[u8]> = TableDefinition::new("retained");
const SESSIONS: TableDefinition<&str, u32> = TableDefinition::new("sessions");
const SUBSCRIPTIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("subscriptions");

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
