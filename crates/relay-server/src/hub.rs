//! Shared broker state tying `relay-core`'s pure [`Router`] to the live delivery
//! channels.
//!
//! `relay-core` stays I/O-free: it only answers "which clients match this topic?".
//! The hub owns, per connected client, an unbounded MPSC sender that the client's
//! writer task drains to its socket. Publishing = ask the router for the matching
//! [`ClientId`]s, then push a clone of the packet onto each one's sender.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use relay_core::{ClientId, Router, TopicFilter};
use rmqtt_codec::v5::Packet;
use tokio::sync::mpsc;
use tracing::debug;

/// Cloneable handle to the shared broker state.
#[derive(Clone)]
pub struct Hub {
    inner: Arc<Inner>,
}

struct Inner {
    next_id: AtomicU64,
    router: Mutex<Router>,
    clients: Mutex<HashMap<ClientId, mpsc::UnboundedSender<Packet>>>,
}

impl Hub {
    pub fn new() -> Self {
        Hub {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(1),
                router: Mutex::new(Router::new()),
                clients: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Register a new connection: assign a [`ClientId`] and return the receiver
    /// that the connection's writer drains to the socket.
    pub fn register(&self) -> (ClientId, mpsc::UnboundedReceiver<Packet>) {
        let id = ClientId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner.clients.lock().unwrap().insert(id, tx);
        (id, rx)
    }

    /// Tear a connection down on disconnect/error.
    pub fn deregister(&self, id: ClientId) {
        self.inner.clients.lock().unwrap().remove(&id);
        self.inner.router.lock().unwrap().remove_client(id);
    }

    /// Register a normal (fan-out) subscription for a client.
    pub fn subscribe(&self, id: ClientId, filter: TopicFilter) {
        self.inner.router.lock().unwrap().subscribe(id, filter);
    }

    /// Register a shared subscription: `id` joins `group` with `filter`.
    pub fn subscribe_shared(&self, group: String, id: ClientId, filter: TopicFilter) {
        self.inner
            .router
            .lock()
            .unwrap()
            .subscribe_shared(group, id, filter);
    }

    /// Deliver a PUBLISH to its recipients: every matching normal subscriber,
    /// plus one member per matching share group (round-robin).
    /// Returns how many recipients the packet was queued for.
    pub fn publish(&self, topic: &str, packet: &Packet) -> usize {
        // Resolve targets under the router lock, then release it before sending.
        let targets = self.inner.router.lock().unwrap().route(topic);
        if targets.is_empty() {
            return 0;
        }
        let clients = self.inner.clients.lock().unwrap();
        let mut delivered = 0;
        for id in targets {
            if let Some(tx) = clients.get(&id) {
                if tx.send(packet.clone()).is_ok() {
                    delivered += 1;
                } else {
                    debug!(client = id.0, "delivery channel closed, skipping");
                }
            }
        }
        delivered
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}
