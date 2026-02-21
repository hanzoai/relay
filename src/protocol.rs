//! Wire protocol for cloud ↔ instance communication.
//!
//! All frames are JSON over WebSocket text messages.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Envelope for all frames sent over the tunnel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    // ── Instance → Cloud ──────────────────────────────
    /// Register this instance with the cloud.
    Register(RegisterPayload),

    /// Stream an event to the cloud (chat delta, exec output, etc.).
    Event(EventPayload),

    /// Response to a command received from the cloud.
    Response(ResponsePayload),

    // ── Cloud → Instance ──────────────────────────────
    /// Acknowledge registration.
    Registered(RegisteredPayload),

    /// A command from the cloud (chat message, approval, config change, etc.).
    Command(CommandPayload),

    // ── Bidirectional ─────────────────────────────────
    /// Keep-alive ping.
    Ping,

    /// Keep-alive pong.
    Pong,
}

/// What kind of app is connecting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppKind {
    Dev,
    Node,
    Desktop,
    Bot,
    Extension,
}

impl std::fmt::Display for AppKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppKind::Dev => write!(f, "dev"),
            AppKind::Node => write!(f, "node"),
            AppKind::Desktop => write!(f, "desktop"),
            AppKind::Bot => write!(f, "bot"),
            AppKind::Extension => write!(f, "extension"),
        }
    }
}

/// Instance → Cloud: registration payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterPayload {
    pub instance_id: String,
    pub app_kind: AppKind,
    pub display_name: String,
    pub capabilities: Vec<String>,
    pub version: String,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Arbitrary metadata (hostname, GPU count, model list, etc.)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Cloud → Instance: registration acknowledged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredPayload {
    pub instance_id: String,
    /// URL where this instance can be managed (e.g. app.hanzo.bot/i/<id>).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_url: Option<String>,
}

/// Instance → Cloud: event stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPayload {
    pub event: String,
    #[serde(default)]
    pub data: serde_json::Value,
}

/// Cloud → Instance: a command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandPayload {
    pub id: String,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

/// Instance → Cloud: response to a command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsePayload {
    pub id: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_roundtrip() {
        let frame = Frame::Register(RegisterPayload {
            instance_id: "abc-123".into(),
            app_kind: AppKind::Dev,
            display_name: "z-macbook".into(),
            capabilities: vec!["chat".into(), "exec".into()],
            version: "0.6.74".into(),
            platform: "darwin-arm64".into(),
            cwd: Some("/Users/z/work".into()),
            metadata: HashMap::new(),
        });

        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""type":"register""#));
        assert!(json.contains(r#""app_kind":"dev""#));

        let parsed: Frame = serde_json::from_str(&json).unwrap();
        match parsed {
            Frame::Register(p) => {
                assert_eq!(p.instance_id, "abc-123");
                assert_eq!(p.app_kind, AppKind::Dev);
            }
            _ => panic!("expected Register"),
        }
    }

    #[test]
    fn command_roundtrip() {
        let frame = Frame::Command(CommandPayload {
            id: "req-1".into(),
            method: "chat.send".into(),
            params: serde_json::json!({"message": "fix the bug"}),
        });

        let json = serde_json::to_string(&frame).unwrap();
        let parsed: Frame = serde_json::from_str(&json).unwrap();
        match parsed {
            Frame::Command(c) => {
                assert_eq!(c.id, "req-1");
                assert_eq!(c.method, "chat.send");
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn response_roundtrip() {
        let frame = Frame::Response(ResponsePayload {
            id: "req-1".into(),
            ok: true,
            data: Some(serde_json::json!({"turn_id": "t1"})),
            error: None,
        });

        let json = serde_json::to_string(&frame).unwrap();
        let parsed: Frame = serde_json::from_str(&json).unwrap();
        match parsed {
            Frame::Response(r) => {
                assert!(r.ok);
                assert_eq!(r.id, "req-1");
            }
            _ => panic!("expected Response"),
        }
    }

    #[test]
    fn ping_pong_roundtrip() {
        let ping_json = serde_json::to_string(&Frame::Ping).unwrap();
        assert!(ping_json.contains(r#""type":"ping""#));
        let parsed: Frame = serde_json::from_str(&ping_json).unwrap();
        assert!(matches!(parsed, Frame::Ping));

        let pong_json = serde_json::to_string(&Frame::Pong).unwrap();
        let parsed: Frame = serde_json::from_str(&pong_json).unwrap();
        assert!(matches!(parsed, Frame::Pong));
    }

    #[test]
    fn event_roundtrip() {
        let frame = Frame::Event(EventPayload {
            event: "chat.delta".into(),
            data: serde_json::json!({"text": "hello"}),
        });

        let json = serde_json::to_string(&frame).unwrap();
        let parsed: Frame = serde_json::from_str(&json).unwrap();
        match parsed {
            Frame::Event(e) => {
                assert_eq!(e.event, "chat.delta");
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn registered_roundtrip() {
        let frame = Frame::Registered(RegisteredPayload {
            instance_id: "abc-123".into(),
            session_url: Some("https://app.hanzo.bot/i/abc-123".into()),
        });

        let json = serde_json::to_string(&frame).unwrap();
        let parsed: Frame = serde_json::from_str(&json).unwrap();
        match parsed {
            Frame::Registered(r) => {
                assert_eq!(r.instance_id, "abc-123");
                assert!(r.session_url.unwrap().contains("hanzo.bot"));
            }
            _ => panic!("expected Registered"),
        }
    }

    #[test]
    fn app_kind_display() {
        assert_eq!(AppKind::Dev.to_string(), "dev");
        assert_eq!(AppKind::Node.to_string(), "node");
        assert_eq!(AppKind::Desktop.to_string(), "desktop");
        assert_eq!(AppKind::Bot.to_string(), "bot");
        assert_eq!(AppKind::Extension.to_string(), "extension");
    }
}
