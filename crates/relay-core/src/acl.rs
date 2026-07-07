//! Topic access-control primitives (authorization).
//!
//! `relay-core` stays I/O- and policy-free: this module only answers
//! "does this set of allowed patterns permit this publish topic / this
//! subscription filter?". The *source* of the patterns (a JWT, a config file,
//! an external hook) and how they are templated per-principal live in the
//! server. An empty allow-list permits nothing.
//!
//! - **Publish** targets a concrete topic → an allowed pattern is a topic filter
//!   that must [`topic_matches`](crate::topic::topic_matches) the topic.
//! - **Subscribe** targets a filter (which may itself contain `+`/`#`) → an
//!   allowed pattern must *subsume* the requested filter: every concrete topic
//!   the request could match must also be matched by the allowed pattern. This
//!   stops a client from widening its reach with `#`.

use crate::topic::topic_matches;

/// The effective, already-templated topic permissions of one principal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Acl {
    /// Topic-filter patterns this principal may publish to.
    pub publish: Vec<String>,
    /// Topic-filter patterns this principal may subscribe to.
    pub subscribe: Vec<String>,
}

impl Acl {
    /// May this principal publish to the concrete `topic`?
    pub fn can_publish(&self, topic: &str) -> bool {
        self.publish.iter().any(|pat| topic_matches(pat, topic))
    }

    /// May this principal subscribe with `filter`? True only if some allowed
    /// pattern subsumes the requested filter (the request cannot be broader).
    pub fn can_subscribe(&self, filter: &str) -> bool {
        self.subscribe
            .iter()
            .any(|pat| filter_subsumes(pat, filter))
    }
}

/// Does `allowed` permit the subscription `requested`? I.e. is every concrete
/// topic matched by `requested` also matched by `allowed`?
///
/// Both are MQTT topic filters. The check is structural, level by level:
/// `allowed` may be broader (a `#`/`+` covering the request) but never narrower.
pub fn filter_subsumes(allowed: &str, requested: &str) -> bool {
    let a: Vec<&str> = allowed.split('/').collect();
    let r: Vec<&str> = requested.split('/').collect();
    let mut i = 0;
    loop {
        match (a.get(i), r.get(i)) {
            // `allowed` ends in `#`: it covers everything from here on.
            (Some(&"#"), _) => return true,
            // Both filters consumed entirely with no mismatch.
            (None, None) => return true,
            (Some(&al), Some(&rl)) => {
                // The request widens with `#`/`+` but `allowed` is not as broad.
                if rl == "#" {
                    return false; // only an `allowed` `#` (handled above) covers `#`
                }
                if al == "+" {
                    // `+` covers one concrete level or a request `+`; not `#` (above).
                    i += 1;
                    continue;
                }
                if rl == "+" {
                    return false; // request asks for any value, literal `allowed` doesn't grant it
                }
                if al == rl {
                    i += 1;
                    continue;
                }
                return false;
            }
            // Differing lengths with no covering `#`.
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subsumes_basics() {
        assert!(filter_subsumes("drive/u1/#", "drive/u1/files"));
        assert!(filter_subsumes("drive/u1/#", "drive/u1/#"));
        assert!(filter_subsumes("drive/u1/#", "drive/u1/+"));
        assert!(filter_subsumes("drive/#", "drive/u1/files"));
        assert!(filter_subsumes("drive/+/created", "drive/42/created"));
        assert!(filter_subsumes("drive/+/created", "drive/+/created"));
        // exact
        assert!(filter_subsumes("a/b/c", "a/b/c"));
    }

    #[test]
    fn subsumes_rejects_widening() {
        // Can't grab the whole tree when only your subtree is allowed.
        assert!(!filter_subsumes("drive/u1/#", "drive/#"));
        assert!(!filter_subsumes("drive/u1/#", "drive/u2/files"));
        assert!(!filter_subsumes("drive/u1/files", "drive/u1/#"));
        assert!(!filter_subsumes("drive/+/created", "drive/#"));
        // literal can't cover a `+` request
        assert!(!filter_subsumes("drive/u1/files", "drive/u1/+"));
        // length mismatch
        assert!(!filter_subsumes("a/b", "a/b/c"));
        assert!(!filter_subsumes("a/b/c", "a/b"));
    }

    #[test]
    fn acl_publish_and_subscribe() {
        let acl = Acl {
            publish: vec!["drive/u1/#".into()],
            subscribe: vec!["drive/u1/#".into(), "config/+".into()],
        };
        assert!(acl.can_publish("drive/u1/files/42"));
        assert!(!acl.can_publish("drive/u2/files/42"));
        assert!(acl.can_subscribe("drive/u1/files"));
        assert!(acl.can_subscribe("config/feature"));
        assert!(!acl.can_subscribe("drive/#"));
        assert!(!acl.can_subscribe("config/a/b"));

        // Empty ACL grants nothing.
        let empty = Acl::default();
        assert!(!empty.can_publish("x"));
        assert!(!empty.can_subscribe("x"));
    }
}
