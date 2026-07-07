//! Topic filter matching and shared-subscription parsing (MQTT 5.0, §4.7).
//!
//! Two distinct notions:
//! - a **topic name** is what a message is published to: `orders/eu/created`
//!   (no wildcards allowed);
//! - a **topic filter** is what a client subscribes with: it may contain the
//!   single-level wildcard `+` and the multi-level wildcard `#`.
//!
//! Wildcard rules implemented here:
//! - `+` matches exactly one topic level.
//! - `#` matches the parent level and any number of child levels; it must be the
//!   last character of the filter and occupy a level on its own.
//! - A filter whose first level is a wildcard (`+` or `#`) does **not** match topic
//!   names that begin with `$` (e.g. `$SYS/...`), per §4.7.2.

/// A parsed subscription filter. Thin wrapper that validates the filter once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicFilter(String);

impl TopicFilter {
    /// Validate and wrap a topic filter. Returns `None` if the filter is malformed
    /// (e.g. `#` not last, or `#`/`+` not alone in their level).
    pub fn parse(filter: &str) -> Option<Self> {
        if filter.is_empty() {
            return None;
        }
        let levels: Vec<&str> = filter.split('/').collect();
        for (i, level) in levels.iter().enumerate() {
            if level.contains('#') {
                // '#' must be the whole level AND the last level.
                if *level != "#" || i != levels.len() - 1 {
                    return None;
                }
            }
            if level.contains('+') && *level != "+" {
                // '+' must occupy the whole level.
                return None;
            }
        }
        Some(TopicFilter(filter.to_string()))
    }

    /// The underlying filter string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Does this filter match the given concrete topic name?
    pub fn matches(&self, topic: &str) -> bool {
        topic_matches(&self.0, topic)
    }
}

/// A subscription that may be a shared subscription (`$share/{group}/{filter}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedSubscription {
    /// The share group name. Subscribers in the same group share delivery
    /// (competing consumers — this is the "queue" behaviour).
    pub group: String,
    /// The effective topic filter, with the `$share/{group}/` prefix stripped.
    pub filter: TopicFilter,
}

impl SharedSubscription {
    /// Parse a subscription string. If it begins with `$share/`, returns the group
    /// and the inner filter; otherwise returns `None` (it's an ordinary subscription).
    ///
    /// Format: `$share/{ShareName}/{filter}` where `{ShareName}` is non-empty and
    /// contains no `/`, `+`, or `#`.
    pub fn parse(subscription: &str) -> Option<Self> {
        let rest = subscription.strip_prefix("$share/")?;
        let (group, filter) = rest.split_once('/')?;
        if group.is_empty() || group.contains('+') || group.contains('#') {
            return None;
        }
        let filter = TopicFilter::parse(filter)?;
        Some(SharedSubscription {
            group: group.to_string(),
            filter,
        })
    }
}

/// Return `true` if `filter` matches the concrete `topic` name.
///
/// `filter` may contain wildcards; `topic` must not. A malformed `filter` never
/// matches.
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    // The leading-`$` rule: a wildcard first level must not match `$`-topics.
    if topic.starts_with('$') {
        let first = filter.split('/').next().unwrap_or("");
        if first == "+" || first == "#" {
            return false;
        }
    }

    let mut f = filter.split('/');
    let mut t = topic.split('/');

    loop {
        match (f.next(), t.next()) {
            // Multi-level wildcard: matches the rest of the topic, whatever it is.
            (Some("#"), _) => return true,
            // Single-level wildcard: consume exactly one topic level (any value).
            (Some("+"), Some(_)) => continue,
            // Literal levels must be equal.
            (Some(fl), Some(tl)) if fl == tl => continue,
            // Both exhausted at the same time: exact match.
            // (Note: `#` matching the parent level — filter `a/#` vs topic `a` — is
            // already handled by the `(Some("#"), _)` arm above.)
            (None, None) => return true,
            // Any mismatch or length difference: no match.
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(topic_matches("a/b/c", "a/b/c"));
        assert!(!topic_matches("a/b/c", "a/b/d"));
        assert!(!topic_matches("a/b", "a/b/c"));
        assert!(!topic_matches("a/b/c", "a/b"));
    }

    #[test]
    fn single_level_wildcard() {
        assert!(topic_matches("a/+/c", "a/b/c"));
        assert!(topic_matches("a/+/c", "a/zzz/c"));
        assert!(!topic_matches("a/+/c", "a/b/d"));
        assert!(!topic_matches("a/+/c", "a/b/c/d"));
        assert!(!topic_matches("a/+", "a/b/c"));
        assert!(topic_matches("+/+/+", "a/b/c"));
    }

    #[test]
    fn multi_level_wildcard() {
        assert!(topic_matches("a/#", "a/b"));
        assert!(topic_matches("a/#", "a/b/c/d"));
        // '#' matches the parent level too.
        assert!(topic_matches("a/#", "a"));
        assert!(topic_matches("#", "a/b/c"));
        assert!(!topic_matches("a/#", "b/c"));
    }

    #[test]
    fn dollar_topics_are_shielded_from_wildcards() {
        assert!(!topic_matches("#", "$SYS/broker/uptime"));
        assert!(!topic_matches("+/broker/uptime", "$SYS/broker/uptime"));
        // But an explicit $ filter matches.
        assert!(topic_matches("$SYS/#", "$SYS/broker/uptime"));
    }

    #[test]
    fn filter_validation() {
        assert!(TopicFilter::parse("a/b/c").is_some());
        assert!(TopicFilter::parse("a/+/c").is_some());
        assert!(TopicFilter::parse("a/#").is_some());
        // '#' must be last and alone.
        assert!(TopicFilter::parse("a/#/c").is_none());
        assert!(TopicFilter::parse("a/b#").is_none());
        // '+' must be alone in its level.
        assert!(TopicFilter::parse("a/b+/c").is_none());
        assert!(TopicFilter::parse("").is_none());
    }

    #[test]
    fn shared_subscription_parsing() {
        let s = SharedSubscription::parse("$share/workers/orders/+/created").unwrap();
        assert_eq!(s.group, "workers");
        assert_eq!(s.filter.as_str(), "orders/+/created");
        assert!(s.filter.matches("orders/eu/created"));

        // Ordinary subscriptions are not shared.
        assert!(SharedSubscription::parse("orders/+/created").is_none());
        // Malformed: empty group.
        assert!(SharedSubscription::parse("$share//orders").is_none());
    }
}
