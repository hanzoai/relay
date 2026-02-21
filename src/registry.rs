//! Instance registry — tracks connected Hanzo apps.
//!
//! The cloud relay (or local bot gateway) maintains a registry of all connected
//! instances. This module provides the client-side types for querying and
//! interacting with registered instances.

use crate::protocol::AppKind;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A registered instance visible from the cloud.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    /// Unique instance ID.
    pub instance_id: String,
    /// What kind of app.
    pub app_kind: AppKind,
    /// Human-readable name.
    pub display_name: String,
    /// Capabilities this instance advertises.
    pub capabilities: Vec<String>,
    /// Commands this instance supports (for invocation).
    pub commands: Vec<String>,
    /// App version.
    pub version: String,
    /// Platform string (e.g., "darwin-arm64").
    pub platform: String,
    /// Working directory (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Whether the instance is currently connected.
    pub connected: bool,
    /// Unix timestamp (ms) when the instance connected.
    pub connected_at_ms: u64,
    /// Management URL (e.g., hanzo.app/i/<id>).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_url: Option<String>,
    /// Exposed services (local ports made reachable via tunnel).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exposed: Vec<crate::expose::ExposedUrl>,
    /// Arbitrary metadata.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Result of invoking a command on a remote instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvokeResult {
    /// Whether the invocation succeeded.
    pub ok: bool,
    /// Response payload (method-specific).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    /// Error message if not ok.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Parameters for invoking a command on a remote instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvokeParams {
    /// Target instance ID.
    pub instance_id: String,
    /// Command to invoke (must be in the instance's `commands` list).
    pub command: String,
    /// Command-specific parameters.
    #[serde(default)]
    pub params: serde_json::Value,
    /// Timeout in milliseconds (default: 30000).
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

fn default_timeout() -> u64 {
    30_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_serialization() {
        let inst = Instance {
            instance_id: "abc-123".into(),
            app_kind: AppKind::Dev,
            display_name: "z-macbook".into(),
            capabilities: vec!["chat".into(), "exec".into()],
            commands: vec!["chat.send".into(), "exec.run".into()],
            version: "0.6.74".into(),
            platform: "darwin-arm64".into(),
            cwd: Some("/Users/z/work".into()),
            connected: true,
            connected_at_ms: 1700000000000,
            session_url: Some("https://hanzo.app/i/abc-123".into()),
            exposed: vec![],
            metadata: HashMap::new(),
        };

        let json = serde_json::to_string(&inst).unwrap();
        let parsed: Instance = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.instance_id, "abc-123");
        assert_eq!(parsed.app_kind, AppKind::Dev);
        assert!(parsed.connected);
    }

    #[test]
    fn invoke_params_default_timeout() {
        let json = r#"{"instance_id":"abc","command":"chat.send","params":{}}"#;
        let params: InvokeParams = serde_json::from_str(json).unwrap();
        assert_eq!(params.timeout_ms, 30_000);
    }

    #[test]
    fn invoke_result_ok() {
        let result = InvokeResult {
            ok: true,
            payload: Some(serde_json::json!({"turn_id": "t1"})),
            error: None,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("turn_id"));
        assert!(!json.contains("error"));
    }
}
