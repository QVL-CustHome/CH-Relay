//! On-disk persistence (V2).
//!
//! A thin wrapper over a [`redb`] embedded key-value store — pure Rust, a single
//! file, no native dependencies. For now it persists **retained messages** so a
//! topic's last known value survives a broker restart; sessions and in-flight
//! queues are the next things to persist.
//!
//! Layout — one table `retained`: key = topic name, value = `[qos_byte] ++
//! payload`. Retained writes are durable (each is its own committed
//! transaction); the store is loaded back into the in-memory [`RetainedStore`]
//! at startup.
//!
//! [`RetainedStore`]: relay_core::RetainedStore

use std::path::Path;

use bytes::Bytes;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use relay_core::{Message, QoS};

const RETAINED: TableDefinition<&str, &[u8]> = TableDefinition::new("retained");

/// Handle to the on-disk store.
pub struct Storage {
    db: Database,
}

impl Storage {
    /// Open (creating if needed) the store at `path`, ensuring its tables exist.
    pub fn open(path: &Path) -> Result<Self, redb::Error> {
        let db = Database::create(path)?;
        // Materialise the table so later read-only transactions never fail.
        let txn = db.begin_write()?;
        txn.open_table(RETAINED)?;
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
}
