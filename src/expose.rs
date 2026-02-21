//! Expose local services through the cloud tunnel.
//!
//! This enables any Hanzo app to make local HTTP/WebSocket endpoints reachable
//! from the cloud (e.g., `hanzo.app/i/<instance_id>` proxies to local service).
//!
//! Think of it like ngrok/cloudflared but built into every Hanzo app.

use crate::protocol::{EventPayload, Frame};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// A local service to expose through the tunnel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExposedService {
    /// Unique name for this service (e.g., "app-server", "mcp", "api").
    pub name: String,
    /// Local address (e.g., "127.0.0.1:3000").
    pub local_addr: String,
    /// Protocol: "http", "ws", or "tcp".
    pub protocol: ExposeProtocol,
    /// Optional subdomain hint (cloud assigns the actual URL).
    pub subdomain: Option<String>,
}

/// Protocol for exposed services.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExposeProtocol {
    Http,
    WebSocket,
    Tcp,
}

/// Cloud-assigned URL for an exposed service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExposedUrl {
    /// The service name that was exposed.
    pub name: String,
    /// The public URL assigned by the cloud.
    pub url: String,
    /// How long this URL is valid (in seconds, 0 = permanent while connected).
    pub ttl: u64,
}

/// Request to expose a local service through the tunnel.
pub async fn expose(
    events_tx: &mpsc::Sender<Frame>,
    service: ExposedService,
) -> Result<(), crate::TunnelError> {
    let frame = Frame::Event(EventPayload {
        event: "expose.request".into(),
        data: serde_json::to_value(&service)
            .map_err(|e| crate::TunnelError::Protocol(e.to_string()))?,
    });
    events_tx
        .send(frame)
        .await
        .map_err(|_| crate::TunnelError::ChannelClosed)
}

/// Request to stop exposing a service.
pub async fn unexpose(
    events_tx: &mpsc::Sender<Frame>,
    service_name: &str,
) -> Result<(), crate::TunnelError> {
    let frame = Frame::Event(EventPayload {
        event: "expose.stop".into(),
        data: serde_json::json!({ "name": service_name }),
    });
    events_tx
        .send(frame)
        .await
        .map_err(|_| crate::TunnelError::ChannelClosed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposed_service_serialization() {
        let svc = ExposedService {
            name: "app-server".into(),
            local_addr: "127.0.0.1:3000".into(),
            protocol: ExposeProtocol::Http,
            subdomain: Some("my-dev".into()),
        };
        let json = serde_json::to_string(&svc).unwrap();
        assert!(json.contains("app-server"));
        assert!(json.contains("http"));

        let parsed: ExposedService = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "app-server");
        assert_eq!(parsed.protocol, ExposeProtocol::Http);
    }

    #[test]
    fn exposed_url_deserialization() {
        let json = r#"{"name":"api","url":"https://abc123.hanzo.app","ttl":0}"#;
        let url: ExposedUrl = serde_json::from_str(json).unwrap();
        assert_eq!(url.name, "api");
        assert!(url.url.contains("hanzo.app"));
        assert_eq!(url.ttl, 0);
    }
}
