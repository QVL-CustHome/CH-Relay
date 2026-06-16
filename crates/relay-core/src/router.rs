//! Subscription routing — pure, I/O-free.
//!
//! The router answers one question: *given a published topic, which subscribers
//! should receive it?* It does not own sockets or channels; `relay-server` keeps
//! the actual delivery handles in a separate map keyed by [`ClientId`] and asks
//! the router for the matching ids.
//!
//! Two kinds of subscription:
//! - **normal** — every matching subscriber receives a copy (pub/sub fan-out);
//! - **shared** (`$share/{group}/{filter}`) — the members of a share group
//!   *compete* for messages: each matching message goes to exactly **one**
//!   member, picked round-robin. This is the "queue" / work-distribution mode.

use crate::topic::TopicFilter;
use std::collections::HashMap;

/// Opaque, broker-assigned identifier for a connected client/session.
///
/// This is the broker's own connection handle, not the MQTT client identifier
/// (which may be empty or duplicated). `relay-server` assigns it on accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(pub u64);

/// A share group: its members and a round-robin cursor.
#[derive(Debug, Default)]
struct SharedGroup {
    /// Members in subscription order: `(client, filter)`.
    members: Vec<(ClientId, TopicFilter)>,
    /// Round-robin position, advanced each time the group is selected.
    cursor: usize,
}

/// Tracks subscriptions and routes published topics to subscribers.
#[derive(Debug, Default)]
pub struct Router {
    /// Normal (fan-out) subscriptions: client → its filters.
    normal: HashMap<ClientId, Vec<TopicFilter>>,
    /// Shared subscriptions, keyed by share-group name.
    shared: HashMap<String, SharedGroup>,
}

impl Router {
    /// Create an empty router.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a normal (fan-out) subscription. Subscribing twice is a no-op.
    pub fn subscribe(&mut self, client: ClientId, filter: TopicFilter) {
        let filters = self.normal.entry(client).or_default();
        if !filters.iter().any(|f| f.as_str() == filter.as_str()) {
            filters.push(filter);
        }
    }

    /// Add a shared subscription: `client` joins `group` with `filter`.
    /// Joining the same group with the same filter twice is a no-op.
    pub fn subscribe_shared(&mut self, group: String, client: ClientId, filter: TopicFilter) {
        let g = self.shared.entry(group).or_default();
        if !g
            .members
            .iter()
            .any(|(c, f)| *c == client && f.as_str() == filter.as_str())
        {
            g.members.push((client, filter));
        }
    }

    /// Remove a single normal subscription. No-op if it wasn't there.
    pub fn unsubscribe(&mut self, client: ClientId, filter: &str) {
        if let Some(filters) = self.normal.get_mut(&client) {
            filters.retain(|f| f.as_str() != filter);
            if filters.is_empty() {
                self.normal.remove(&client);
            }
        }
    }

    /// Drop a client entirely (on disconnect): from normal subs and every group.
    pub fn remove_client(&mut self, client: ClientId) {
        self.normal.remove(&client);
        for group in self.shared.values_mut() {
            group.members.retain(|(c, _)| *c != client);
        }
        self.shared.retain(|_, g| !g.members.is_empty());
    }

    /// The distinct **normal** subscribers matching `topic`, sorted.
    pub fn matching_subscribers(&self, topic: &str) -> Vec<ClientId> {
        let mut matched: Vec<ClientId> = self
            .normal
            .iter()
            .filter(|(_, filters)| filters.iter().any(|f| f.matches(topic)))
            .map(|(client, _)| *client)
            .collect();
        matched.sort();
        matched
    }

    /// Resolve every recipient for a message published on `topic`:
    /// all matching normal subscribers, plus exactly one matching member per
    /// matching share group (round-robin). Mutates the round-robin cursors.
    ///
    /// The returned vec is sorted for deterministic delivery order; it may
    /// contain a client more than once if it matches via several distinct
    /// subscriptions (e.g. a normal sub and a share group).
    pub fn route(&mut self, topic: &str) -> Vec<ClientId> {
        let mut recipients = self.matching_subscribers(topic);

        for group in self.shared.values_mut() {
            let matching: Vec<ClientId> = group
                .members
                .iter()
                .filter(|(_, f)| f.matches(topic))
                .map(|(c, _)| *c)
                .collect();
            if matching.is_empty() {
                continue;
            }
            let pick = group.cursor % matching.len();
            group.cursor = group.cursor.wrapping_add(1);
            recipients.push(matching[pick]);
        }

        recipients.sort();
        recipients
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(s: &str) -> TopicFilter {
        TopicFilter::parse(s).unwrap()
    }

    #[test]
    fn fan_out_to_matching_clients() {
        let mut r = Router::new();
        r.subscribe(ClientId(1), filter("sensors/+/temp"));
        r.subscribe(ClientId(2), filter("sensors/#"));
        r.subscribe(ClientId(3), filter("orders/created"));

        assert_eq!(
            r.matching_subscribers("sensors/eu/temp"),
            vec![ClientId(1), ClientId(2)]
        );
        assert_eq!(r.matching_subscribers("orders/created"), vec![ClientId(3)]);
        assert!(r.matching_subscribers("nothing/here").is_empty());
    }

    #[test]
    fn a_client_matches_once_even_with_overlapping_filters() {
        let mut r = Router::new();
        r.subscribe(ClientId(1), filter("a/+"));
        r.subscribe(ClientId(1), filter("a/#"));
        assert_eq!(r.matching_subscribers("a/b"), vec![ClientId(1)]);
    }

    #[test]
    fn duplicate_subscribe_is_noop() {
        let mut r = Router::new();
        r.subscribe(ClientId(1), filter("a/b"));
        r.subscribe(ClientId(1), filter("a/b"));
        assert_eq!(r.matching_subscribers("a/b"), vec![ClientId(1)]);
    }

    #[test]
    fn unsubscribe_and_remove() {
        let mut r = Router::new();
        r.subscribe(ClientId(1), filter("a/b"));
        r.subscribe(ClientId(2), filter("a/b"));

        r.unsubscribe(ClientId(1), "a/b");
        assert_eq!(r.matching_subscribers("a/b"), vec![ClientId(2)]);

        r.remove_client(ClientId(2));
        assert!(r.matching_subscribers("a/b").is_empty());
    }

    #[test]
    fn shared_group_distributes_round_robin() {
        let mut r = Router::new();
        // Three workers competing on the same shared filter.
        r.subscribe_shared("workers".into(), ClientId(1), filter("jobs"));
        r.subscribe_shared("workers".into(), ClientId(2), filter("jobs"));
        r.subscribe_shared("workers".into(), ClientId(3), filter("jobs"));

        // Each published message goes to exactly one worker, rotating.
        assert_eq!(r.route("jobs"), vec![ClientId(1)]);
        assert_eq!(r.route("jobs"), vec![ClientId(2)]);
        assert_eq!(r.route("jobs"), vec![ClientId(3)]);
        assert_eq!(r.route("jobs"), vec![ClientId(1)]); // wraps around
    }

    #[test]
    fn shared_and_normal_coexist() {
        let mut r = Router::new();
        r.subscribe(ClientId(9), filter("jobs")); // normal: gets every message
        r.subscribe_shared("workers".into(), ClientId(1), filter("jobs"));
        r.subscribe_shared("workers".into(), ClientId(2), filter("jobs"));

        // Normal subscriber (9) always present; the group contributes one worker.
        assert_eq!(r.route("jobs"), vec![ClientId(1), ClientId(9)]);
        assert_eq!(r.route("jobs"), vec![ClientId(2), ClientId(9)]);
        assert_eq!(r.route("jobs"), vec![ClientId(1), ClientId(9)]);
    }

    #[test]
    fn two_groups_each_pick_one_member() {
        let mut r = Router::new();
        r.subscribe_shared("a".into(), ClientId(1), filter("t"));
        r.subscribe_shared("a".into(), ClientId(2), filter("t"));
        r.subscribe_shared("b".into(), ClientId(3), filter("t"));
        r.subscribe_shared("b".into(), ClientId(4), filter("t"));

        // One member from group "a" and one from group "b" each time.
        let first = r.route("t");
        assert_eq!(first.len(), 2);
        assert!(first.contains(&ClientId(1))); // a's first pick
        assert!(first.contains(&ClientId(3))); // b's first pick
    }

    #[test]
    fn removing_client_cleans_shared_group() {
        let mut r = Router::new();
        r.subscribe_shared("workers".into(), ClientId(1), filter("jobs"));
        r.subscribe_shared("workers".into(), ClientId(2), filter("jobs"));

        r.remove_client(ClientId(1));
        // Only worker 2 remains, so it gets everything.
        assert_eq!(r.route("jobs"), vec![ClientId(2)]);
        assert_eq!(r.route("jobs"), vec![ClientId(2)]);
    }
}
