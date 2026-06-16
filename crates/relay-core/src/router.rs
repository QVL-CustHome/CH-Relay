//! Subscription routing — pure, I/O-free.
//!
//! The router answers one question: *given a published topic, which subscribers
//! should receive it, and at which granted QoS?* It does not own sockets or
//! channels; `relay-server` keeps the actual delivery handles in a separate map
//! keyed by [`ClientId`] and asks the router for the matching ids.
//!
//! Two kinds of subscription:
//! - **normal** — every matching subscriber receives a copy (pub/sub fan-out);
//! - **shared** (`$share/{group}/{filter}`) — the members of a share group
//!   *compete* for messages: each matching message goes to exactly **one**
//!   member, picked round-robin. This is the "queue" / work-distribution mode.
//!
//! Each subscription carries the **granted QoS** (the maximum QoS at which the
//! broker will deliver to it). The effective QoS of a delivery is
//! `min(publish QoS, granted QoS)` — but that minimum is computed by the server
//! at delivery time, since the router does not see the published message's QoS.

use crate::qos::QoS;
use crate::topic::TopicFilter;
use std::collections::HashMap;

/// Opaque, broker-assigned identifier for a connected client/session.
///
/// This is the broker's own connection handle, not the MQTT client identifier
/// (which may be empty or duplicated). `relay-server` assigns it on accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(pub u64);

/// A single subscription: the topic filter and the granted QoS.
#[derive(Debug, Clone)]
struct Sub {
    filter: TopicFilter,
    qos: QoS,
}

/// A share group: its members and a round-robin cursor.
#[derive(Debug, Default)]
struct SharedGroup {
    /// Members in subscription order: `(client, subscription)`.
    members: Vec<(ClientId, Sub)>,
    /// Round-robin position, advanced each time the group is selected.
    cursor: usize,
}

/// Tracks subscriptions and routes published topics to subscribers.
#[derive(Debug, Default)]
pub struct Router {
    /// Normal (fan-out) subscriptions: client → its subscriptions.
    normal: HashMap<ClientId, Vec<Sub>>,
    /// Shared subscriptions, keyed by share-group name.
    shared: HashMap<String, SharedGroup>,
}

impl Router {
    /// Create an empty router.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a normal (fan-out) subscription at granted `qos`. Re-subscribing to
    /// the same filter updates its granted QoS (MQTT replaces the subscription).
    pub fn subscribe(&mut self, client: ClientId, filter: TopicFilter, qos: QoS) {
        let subs = self.normal.entry(client).or_default();
        match subs.iter_mut().find(|s| s.filter.as_str() == filter.as_str()) {
            Some(existing) => existing.qos = qos,
            None => subs.push(Sub { filter, qos }),
        }
    }

    /// Add a shared subscription: `client` joins `group` with `filter` at `qos`.
    /// Re-joining with the same filter updates its granted QoS.
    pub fn subscribe_shared(
        &mut self,
        group: String,
        client: ClientId,
        filter: TopicFilter,
        qos: QoS,
    ) {
        let g = self.shared.entry(group).or_default();
        match g
            .members
            .iter_mut()
            .find(|(c, s)| *c == client && s.filter.as_str() == filter.as_str())
        {
            Some((_, existing)) => existing.qos = qos,
            None => g.members.push((client, Sub { filter, qos })),
        }
    }

    /// Remove a single normal subscription. Returns whether it existed.
    pub fn unsubscribe(&mut self, client: ClientId, filter: &str) -> bool {
        if let Some(subs) = self.normal.get_mut(&client) {
            let before = subs.len();
            subs.retain(|s| s.filter.as_str() != filter);
            let removed = subs.len() != before;
            if subs.is_empty() {
                self.normal.remove(&client);
            }
            removed
        } else {
            false
        }
    }

    /// Remove a client's membership of a share group for `filter`. Returns
    /// whether it existed.
    pub fn unsubscribe_shared(&mut self, group: &str, client: ClientId, filter: &str) -> bool {
        if let Some(g) = self.shared.get_mut(group) {
            let before = g.members.len();
            g.members
                .retain(|(c, s)| !(*c == client && s.filter.as_str() == filter));
            let removed = g.members.len() != before;
            if g.members.is_empty() {
                self.shared.remove(group);
            }
            removed
        } else {
            false
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

    /// Total active subscriptions: normal subscriptions plus shared-group
    /// memberships. For monitoring.
    pub fn subscription_count(&self) -> usize {
        let normal: usize = self.normal.values().map(|v| v.len()).sum();
        let shared: usize = self.shared.values().map(|g| g.members.len()).sum();
        normal + shared
    }

    /// The distinct **normal** subscribers matching `topic`, with the granted
    /// QoS of their matching subscription (the maximum granted QoS if several of
    /// a client's filters match). Sorted by [`ClientId`].
    pub fn matching_subscribers(&self, topic: &str) -> Vec<(ClientId, QoS)> {
        let mut matched: Vec<(ClientId, QoS)> = self
            .normal
            .iter()
            .filter_map(|(client, subs)| {
                subs.iter()
                    .filter(|s| s.filter.matches(topic))
                    .map(|s| s.qos)
                    .max()
                    .map(|qos| (*client, qos))
            })
            .collect();
        matched.sort_by_key(|(c, _)| *c);
        matched
    }

    /// Resolve every recipient for a message published on `topic`:
    /// all matching normal subscribers, plus exactly one matching member per
    /// matching share group (round-robin). Each recipient is paired with the
    /// granted QoS of the subscription it matched on. Mutates the round-robin
    /// cursors.
    ///
    /// The returned vec is sorted by [`ClientId`] for deterministic delivery
    /// order; it may contain a client more than once if it matches via several
    /// distinct subscriptions (e.g. a normal sub and a share group).
    pub fn route(&mut self, topic: &str) -> Vec<(ClientId, QoS)> {
        let mut recipients = self.matching_subscribers(topic);

        for group in self.shared.values_mut() {
            let matching: Vec<(ClientId, QoS)> = group
                .members
                .iter()
                .filter(|(_, s)| s.filter.matches(topic))
                .map(|(c, s)| (*c, s.qos))
                .collect();
            if matching.is_empty() {
                continue;
            }
            let pick = group.cursor % matching.len();
            group.cursor = group.cursor.wrapping_add(1);
            recipients.push(matching[pick]);
        }

        recipients.sort_by_key(|(c, _)| *c);
        recipients
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(s: &str) -> TopicFilter {
        TopicFilter::parse(s).unwrap()
    }

    /// The client ids of normal subscribers matching `topic` (QoS dropped),
    /// for the tests that only care about *who* matches.
    fn ids(matched: Vec<(ClientId, QoS)>) -> Vec<ClientId> {
        matched.into_iter().map(|(c, _)| c).collect()
    }

    #[test]
    fn fan_out_to_matching_clients() {
        let mut r = Router::new();
        r.subscribe(ClientId(1), filter("sensors/+/temp"), QoS::AtMostOnce);
        r.subscribe(ClientId(2), filter("sensors/#"), QoS::AtMostOnce);
        r.subscribe(ClientId(3), filter("orders/created"), QoS::AtMostOnce);

        assert_eq!(
            ids(r.matching_subscribers("sensors/eu/temp")),
            vec![ClientId(1), ClientId(2)]
        );
        assert_eq!(ids(r.matching_subscribers("orders/created")), vec![ClientId(3)]);
        assert!(r.matching_subscribers("nothing/here").is_empty());
    }

    #[test]
    fn a_client_matches_once_even_with_overlapping_filters() {
        let mut r = Router::new();
        r.subscribe(ClientId(1), filter("a/+"), QoS::AtMostOnce);
        r.subscribe(ClientId(1), filter("a/#"), QoS::AtMostOnce);
        assert_eq!(ids(r.matching_subscribers("a/b")), vec![ClientId(1)]);
    }

    #[test]
    fn overlapping_filters_yield_the_max_granted_qos() {
        let mut r = Router::new();
        r.subscribe(ClientId(1), filter("a/+"), QoS::AtMostOnce);
        r.subscribe(ClientId(1), filter("a/#"), QoS::AtLeastOnce);
        // Both filters match `a/b`; the client should be offered the higher QoS.
        assert_eq!(
            r.matching_subscribers("a/b"),
            vec![(ClientId(1), QoS::AtLeastOnce)]
        );
    }

    #[test]
    fn re_subscribe_updates_granted_qos() {
        let mut r = Router::new();
        r.subscribe(ClientId(1), filter("a/b"), QoS::AtMostOnce);
        r.subscribe(ClientId(1), filter("a/b"), QoS::AtLeastOnce);
        // One subscription, upgraded — not two.
        assert_eq!(
            r.matching_subscribers("a/b"),
            vec![(ClientId(1), QoS::AtLeastOnce)]
        );
    }

    #[test]
    fn subscription_count_tracks_normal_and_shared() {
        let mut r = Router::new();
        assert_eq!(r.subscription_count(), 0);
        r.subscribe(ClientId(1), filter("a/b"), QoS::AtMostOnce);
        r.subscribe(ClientId(2), filter("a/#"), QoS::AtMostOnce);
        r.subscribe_shared("g".into(), ClientId(3), filter("jobs"), QoS::AtMostOnce);
        assert_eq!(r.subscription_count(), 3);
        r.unsubscribe(ClientId(1), "a/b");
        assert_eq!(r.subscription_count(), 2);
    }

    #[test]
    fn unsubscribe_and_remove() {
        let mut r = Router::new();
        r.subscribe(ClientId(1), filter("a/b"), QoS::AtMostOnce);
        r.subscribe(ClientId(2), filter("a/b"), QoS::AtMostOnce);

        assert!(r.unsubscribe(ClientId(1), "a/b"), "should report the sub existed");
        assert!(!r.unsubscribe(ClientId(1), "a/b"), "second time it's gone");
        assert_eq!(ids(r.matching_subscribers("a/b")), vec![ClientId(2)]);

        r.remove_client(ClientId(2));
        assert!(r.matching_subscribers("a/b").is_empty());
    }

    #[test]
    fn unsubscribe_shared_removes_one_member() {
        let mut r = Router::new();
        r.subscribe_shared("workers".into(), ClientId(1), filter("jobs"), QoS::AtMostOnce);
        r.subscribe_shared("workers".into(), ClientId(2), filter("jobs"), QoS::AtMostOnce);

        assert!(r.unsubscribe_shared("workers", ClientId(1), "jobs"));
        assert!(!r.unsubscribe_shared("workers", ClientId(1), "jobs"));
        // Only worker 2 remains in the group.
        assert_eq!(ids(r.route("jobs")), vec![ClientId(2)]);
        assert_eq!(ids(r.route("jobs")), vec![ClientId(2)]);
    }

    #[test]
    fn shared_group_distributes_round_robin() {
        let mut r = Router::new();
        // Three workers competing on the same shared filter.
        r.subscribe_shared("workers".into(), ClientId(1), filter("jobs"), QoS::AtMostOnce);
        r.subscribe_shared("workers".into(), ClientId(2), filter("jobs"), QoS::AtMostOnce);
        r.subscribe_shared("workers".into(), ClientId(3), filter("jobs"), QoS::AtMostOnce);

        // Each published message goes to exactly one worker, rotating.
        assert_eq!(ids(r.route("jobs")), vec![ClientId(1)]);
        assert_eq!(ids(r.route("jobs")), vec![ClientId(2)]);
        assert_eq!(ids(r.route("jobs")), vec![ClientId(3)]);
        assert_eq!(ids(r.route("jobs")), vec![ClientId(1)]); // wraps around
    }

    #[test]
    fn shared_and_normal_coexist() {
        let mut r = Router::new();
        r.subscribe(ClientId(9), filter("jobs"), QoS::AtMostOnce); // normal: gets every message
        r.subscribe_shared("workers".into(), ClientId(1), filter("jobs"), QoS::AtMostOnce);
        r.subscribe_shared("workers".into(), ClientId(2), filter("jobs"), QoS::AtMostOnce);

        // Normal subscriber (9) always present; the group contributes one worker.
        assert_eq!(ids(r.route("jobs")), vec![ClientId(1), ClientId(9)]);
        assert_eq!(ids(r.route("jobs")), vec![ClientId(2), ClientId(9)]);
        assert_eq!(ids(r.route("jobs")), vec![ClientId(1), ClientId(9)]);
    }

    #[test]
    fn two_groups_each_pick_one_member() {
        let mut r = Router::new();
        r.subscribe_shared("a".into(), ClientId(1), filter("t"), QoS::AtMostOnce);
        r.subscribe_shared("a".into(), ClientId(2), filter("t"), QoS::AtMostOnce);
        r.subscribe_shared("b".into(), ClientId(3), filter("t"), QoS::AtMostOnce);
        r.subscribe_shared("b".into(), ClientId(4), filter("t"), QoS::AtMostOnce);

        // One member from group "a" and one from group "b" each time.
        let first = ids(r.route("t"));
        assert_eq!(first.len(), 2);
        assert!(first.contains(&ClientId(1))); // a's first pick
        assert!(first.contains(&ClientId(3))); // b's first pick
    }

    #[test]
    fn removing_client_cleans_shared_group() {
        let mut r = Router::new();
        r.subscribe_shared("workers".into(), ClientId(1), filter("jobs"), QoS::AtMostOnce);
        r.subscribe_shared("workers".into(), ClientId(2), filter("jobs"), QoS::AtMostOnce);

        r.remove_client(ClientId(1));
        // Only worker 2 remains, so it gets everything.
        assert_eq!(ids(r.route("jobs")), vec![ClientId(2)]);
        assert_eq!(ids(r.route("jobs")), vec![ClientId(2)]);
    }
}
