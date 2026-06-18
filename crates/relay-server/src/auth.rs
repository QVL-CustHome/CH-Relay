//! Optional JWT authentication + topic ACL (authorization).
//!
//! Auth is **opt-in**: with no `[auth]` section in the config the broker stays
//! open (its historical behaviour). When `[auth]` is present, every CONNECT must
//! carry a valid JWT (sent as the MQTT *password*); the broker validates it
//! (HS256, shared secret) and derives the client's topic permissions from the
//! ACL rules, matched by the token's roles and templated with the token's claims
//! (e.g. `{sub}`).
//!
//! The design is **generic** — it is not tied to any one project: the signing
//! secret, the identity claim and the roles claim are all configurable, and ACL
//! patterns may reference any string claim via `{claim}` placeholders.

use std::collections::HashMap;

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use relay_core::Acl;
use serde::Deserialize;
use serde_json::Value;

/// `[auth]` configuration block. Absent ⇒ authentication disabled.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    /// HS256 shared secret used to verify the JWT signature.
    pub jwt_secret: String,
    /// Claim used as the principal identity (and as `{sub}` in ACL templates).
    #[serde(default = "default_identity_claim")]
    pub identity_claim: String,
    /// Claim holding the principal's roles (a JSON array of strings).
    #[serde(default = "default_roles_claim")]
    pub roles_claim: String,
    /// ACL rules. A client gets the union of every rule whose `role` matches.
    #[serde(default)]
    pub acl: Vec<AclRule>,
}

fn default_identity_claim() -> String {
    "sub".into()
}
fn default_roles_claim() -> String {
    "roles".into()
}
fn default_role() -> String {
    "*".into()
}

/// One ACL rule: grants the listed publish/subscribe topic patterns to clients
/// holding `role` (`"*"` = any authenticated client). Patterns may contain
/// `{claim}` placeholders substituted from the token (e.g. `drive/{sub}/#`).
#[derive(Debug, Clone, Deserialize)]
pub struct AclRule {
    #[serde(default = "default_role")]
    pub role: String,
    #[serde(default)]
    pub publish: Vec<String>,
    #[serde(default)]
    pub subscribe: Vec<String>,
}

/// An authenticated client: its identity (for logging) and effective topic ACL.
pub struct Principal {
    pub identity: String,
    pub acl: Acl,
}

/// Why a CONNECT was rejected (mapped to a CONNACK reason by the caller).
#[derive(Debug)]
pub enum AuthError {
    /// No token, malformed token, bad signature, or expired.
    InvalidToken,
    /// Token verified but carries no usable identity claim.
    NoIdentity,
}

impl AuthConfig {
    /// Verify the JWT carried in the MQTT password and build the client's
    /// principal (identity + templated ACL). The username is not required.
    pub fn authenticate(&self, password: Option<&[u8]>) -> Result<Principal, AuthError> {
        let raw = password.ok_or(AuthError::InvalidToken)?;
        let token = std::str::from_utf8(raw).map_err(|_| AuthError::InvalidToken)?;

        let mut validation = Validation::new(Algorithm::HS256);
        // Identity/roles are app-defined claims; we don't constrain the audience.
        validation.validate_aud = false;
        let data = decode::<Value>(
            token,
            &DecodingKey::from_secret(self.jwt_secret.as_bytes()),
            &validation,
        )
        .map_err(|_| AuthError::InvalidToken)?;

        let claims = data.claims.as_object().ok_or(AuthError::InvalidToken)?;
        let identity = claims
            .get(&self.identity_claim)
            .and_then(Value::as_str)
            .ok_or(AuthError::NoIdentity)?
            .to_string();

        let roles: Vec<String> = claims
            .get(&self.roles_claim)
            .and_then(Value::as_array)
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();

        // Variables available to ACL templates: every string claim, plus `{sub}`.
        let mut vars: HashMap<&str, &str> = claims
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.as_str(), s)))
            .collect();
        vars.insert("sub", identity.as_str());

        let mut acl = Acl::default();
        for rule in &self.acl {
            if rule.role != "*" && !roles.iter().any(|r| r == &rule.role) {
                continue;
            }
            for pat in &rule.publish {
                if let Some(p) = substitute(pat, &vars) {
                    acl.publish.push(p);
                }
            }
            for pat in &rule.subscribe {
                if let Some(p) = substitute(pat, &vars) {
                    acl.subscribe.push(p);
                }
            }
        }

        Ok(Principal { identity, acl })
    }
}

/// Replace every `{key}` in `pattern` with its claim value. Returns `None` if a
/// referenced claim is missing — that pattern then grants nothing (fail closed).
fn substitute(pattern: &str, vars: &HashMap<&str, &str>) -> Option<String> {
    let mut out = String::with_capacity(pattern.len());
    let mut rest = pattern;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let end = after.find('}')?;
        let key = &after[..end];
        out.push_str(vars.get(key)?);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    fn token(secret: &str, claims: Value) -> String {
        encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(secret.as_bytes())).unwrap()
    }

    fn cfg() -> AuthConfig {
        AuthConfig {
            jwt_secret: "test-secret".into(),
            identity_claim: "sub".into(),
            roles_claim: "roles".into(),
            acl: vec![
                AclRule { role: "*".into(), publish: vec!["drive/{sub}/#".into()], subscribe: vec!["drive/{sub}/#".into()] },
                AclRule { role: "drive_admin".into(), publish: vec!["drive/#".into()], subscribe: vec!["drive/#".into()] },
            ],
        }
    }

    // exp far in the future
    const EXP: i64 = 4_102_444_800; // 2100-01-01

    #[test]
    fn rejects_missing_or_bad_token() {
        let c = cfg();
        assert!(matches!(c.authenticate(None), Err(AuthError::InvalidToken)));
        assert!(matches!(c.authenticate(Some(b"not-a-jwt")), Err(AuthError::InvalidToken)));
        let wrong = token("other-secret", serde_json::json!({"sub": "u1", "exp": EXP}));
        assert!(matches!(c.authenticate(Some(wrong.as_bytes())), Err(AuthError::InvalidToken)));
    }

    #[test]
    fn user_gets_own_subtree() {
        let c = cfg();
        let t = token("test-secret", serde_json::json!({"sub": "u1", "roles": ["drive"], "exp": EXP}));
        let p = c.authenticate(Some(t.as_bytes())).unwrap();
        assert_eq!(p.identity, "u1");
        assert!(p.acl.can_publish("drive/u1/files/1"));
        assert!(p.acl.can_subscribe("drive/u1/#"));
        assert!(!p.acl.can_publish("drive/u2/files/1"));
        assert!(!p.acl.can_subscribe("drive/#"));
    }

    #[test]
    fn admin_gets_whole_tree() {
        let c = cfg();
        let t = token("test-secret", serde_json::json!({"sub": "boss", "roles": ["drive_admin"], "exp": EXP}));
        let p = c.authenticate(Some(t.as_bytes())).unwrap();
        assert!(p.acl.can_subscribe("drive/#"));
        assert!(p.acl.can_publish("drive/anyone/x"));
    }
}
