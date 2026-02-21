//! Authentication for cloud tunnel connections.
//!
//! Supports JWT tokens from hanzo.id and plain API keys.

use serde::{Deserialize, Serialize};

/// Auth credential for the tunnel connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthToken {
    /// A JWT from hanzo.id (Casdoor IAM).
    Jwt { token: String },
    /// A Hanzo API key (sk-hanzo-...).
    ApiKey { key: String },
}

impl AuthToken {
    /// Create from a raw token string. If it looks like a JWT (has dots), treat
    /// it as JWT; otherwise treat as API key.
    pub fn from_string(s: impl Into<String>) -> Self {
        let s = s.into();
        if s.matches('.').count() >= 2 {
            AuthToken::Jwt { token: s }
        } else {
            AuthToken::ApiKey { key: s }
        }
    }

    /// The bearer value to send in the WebSocket upgrade request.
    pub fn bearer(&self) -> &str {
        match self {
            AuthToken::Jwt { token } => token,
            AuthToken::ApiKey { key } => key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_detection() {
        let token = AuthToken::from_string("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature");
        assert!(matches!(token, AuthToken::Jwt { .. }));
    }

    #[test]
    fn api_key_detection() {
        let token = AuthToken::from_string("sk-hanzo-abc123");
        assert!(matches!(token, AuthToken::ApiKey { .. }));
    }

    #[test]
    fn bearer_returns_value() {
        let jwt = AuthToken::Jwt {
            token: "abc.def.ghi".into(),
        };
        assert_eq!(jwt.bearer(), "abc.def.ghi");

        let key = AuthToken::ApiKey {
            key: "sk-test".into(),
        };
        assert_eq!(key.bearer(), "sk-test");
    }
}
