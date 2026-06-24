use std::collections::HashMap;

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use relay_core::Acl;
use serde::Deserialize;
use serde_json::Value;

const EXPECTED_ISSUER: &str = "ch-api-authenticator";

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    pub jwt_secret: String,
    #[serde(default = "default_identity_claim")]
    pub identity_claim: String,
    #[serde(default = "default_roles_claim")]
    pub roles_claim: String,
    #[serde(default = "default_allowed_audiences")]
    pub allowed_audiences: Vec<String>,
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
fn default_allowed_audiences() -> Vec<String> {
    match std::env::var("RELAY_ALLOWED_AUDIENCES") {
        Ok(raw) => raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
        Err(_) => vec!["ch-api-drive".into(), "ch-api-budgy".into(), "ch-relay".into()],
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AclRule {
    #[serde(default = "default_role")]
    pub role: String,
    #[serde(default)]
    pub publish: Vec<String>,
    #[serde(default)]
    pub subscribe: Vec<String>,
}

pub struct Principal {
    pub identity: String,
    pub acl: Acl,
}

#[derive(Debug)]
pub enum AuthError {
    InvalidToken,
    NoIdentity,
    InvalidIssuer,
    InvalidAudience,
}

impl AuthConfig {
    pub fn authenticate(&self, password: Option<&[u8]>) -> Result<Principal, AuthError> {
        let raw = password.ok_or(AuthError::InvalidToken)?;
        let token = std::str::from_utf8(raw).map_err(|_| AuthError::InvalidToken)?;

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_aud = false;
        validation.set_required_spec_claims(&["exp", "iss"]);
        validation.set_issuer(&[EXPECTED_ISSUER]);
        let data = decode::<Value>(
            token,
            &DecodingKey::from_secret(self.jwt_secret.as_bytes()),
            &validation,
        )
        .map_err(map_decode_error)?;

        let claims = data.claims.as_object().ok_or(AuthError::InvalidToken)?;

        if !self.audience_allowed(claims.get("aud")) {
            return Err(AuthError::InvalidAudience);
        }

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

    fn audience_allowed(&self, aud: Option<&Value>) -> bool {
        let presented = match aud {
            Some(Value::String(s)) => vec![s.as_str()],
            Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).collect(),
            _ => return false,
        };
        if presented.is_empty() {
            return false;
        }
        presented
            .iter()
            .any(|a| self.allowed_audiences.iter().any(|allowed| allowed == a))
    }
}

fn map_decode_error(error: jsonwebtoken::errors::Error) -> AuthError {
    use jsonwebtoken::errors::ErrorKind;
    match error.kind() {
        ErrorKind::InvalidIssuer => AuthError::InvalidIssuer,
        _ => AuthError::InvalidToken,
    }
}

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
            allowed_audiences: vec!["ch-api-drive".into(), "ch-api-budgy".into()],
            acl: vec![
                AclRule { role: "drive".into(), publish: vec!["drive/{sub}/#".into()], subscribe: vec!["drive/{sub}/#".into()] },
                AclRule { role: "drive_admin".into(), publish: vec!["drive/#".into()], subscribe: vec!["drive/#".into()] },
            ],
        }
    }

    const EXP: i64 = 4_102_444_800;
    const ISS: &str = "ch-api-authenticator";

    #[test]
    fn rejects_missing_or_bad_token() {
        let c = cfg();
        assert!(matches!(c.authenticate(None), Err(AuthError::InvalidToken)));
        assert!(matches!(c.authenticate(Some(b"not-a-jwt")), Err(AuthError::InvalidToken)));
        let wrong = token("other-secret", serde_json::json!({"sub": "u1", "exp": EXP, "iss": ISS, "aud": "ch-api-drive"}));
        assert!(matches!(c.authenticate(Some(wrong.as_bytes())), Err(AuthError::InvalidToken)));
    }

    #[test]
    fn user_gets_own_subtree() {
        let c = cfg();
        let t = token("test-secret", serde_json::json!({"sub": "u1", "roles": ["drive"], "exp": EXP, "iss": ISS, "aud": "ch-api-drive"}));
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
        let t = token("test-secret", serde_json::json!({"sub": "boss", "roles": ["drive_admin"], "exp": EXP, "iss": ISS, "aud": ["ch-api-budgy", "ch-api-drive"]}));
        let p = c.authenticate(Some(t.as_bytes())).unwrap();
        assert!(p.acl.can_subscribe("drive/#"));
        assert!(p.acl.can_publish("drive/anyone/x"));
    }
}
