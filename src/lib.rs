//! # hanzo-tunnel
//!
//! Cloud tunnel client for Hanzo apps. One crate, one way to connect any
//! local Hanzo app (dev, node, desktop, bot, extension) to the cloud relay
//! at `app.hanzo.bot` for remote management and multiplayer.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use hanzo_tunnel::{connect, TunnelConfig, AppKind};
//!
//! # async fn example() -> Result<(), hanzo_tunnel::TunnelError> {
//! let conn = connect(TunnelConfig {
//!     relay_url: "wss://api.hanzo.ai/v1/relay".into(),
//!     auth_token: "sk-hanzo-...".into(),
//!     app_kind: AppKind::Dev,
//!     display_name: "z-macbook".into(),
//!     capabilities: vec!["chat".into(), "exec".into()],
//!     ..Default::default()
//! }).await?;
//!
//! // Send events to cloud
//! conn.send_event("chat.delta", serde_json::json!({"text": "hello"})).await?;
//!
//! // Receive commands from cloud
//! while let Some(cmd) = conn.recv_command().await {
//!     println!("got command: {} {}", cmd.method, cmd.params);
//!     conn.respond(cmd.id, true, None).await?;
//! }
//! # Ok(())
//! # }
//! ```

pub mod auth;
pub mod discovery;
pub mod expose;
pub mod protocol;
pub mod registry;
pub(crate) mod transport;

pub use expose::{ExposeProtocol, ExposedService, ExposedUrl};
pub use protocol::{AppKind, CommandPayload, EventPayload, Frame};
pub use registry::{Instance, InvokeParams, InvokeResult};

use protocol::{RegisterPayload, ResponsePayload};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info};
use transport::TransportConfig;

/// Error type for tunnel operations.
#[derive(Debug, thiserror::Error)]
pub enum TunnelError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("disconnected")]
    Disconnected,

    #[error("discovery error: {0}")]
    Discovery(String),

    #[error("channel closed")]
    ChannelClosed,

    #[error("not registered")]
    NotRegistered,
}

/// Configuration for connecting to the cloud.
#[derive(Debug, Clone)]
pub struct TunnelConfig {
    /// Cloud relay URL (default: wss://api.hanzo.ai/v1/relay).
    pub relay_url: String,
    /// Auth token (hanzo.id JWT or API key).
    pub auth_token: String,
    /// What kind of app this is.
    pub app_kind: AppKind,
    /// Display name (e.g., hostname, custom label).
    pub display_name: String,
    /// Capabilities this instance advertises.
    pub capabilities: Vec<String>,
    /// Working directory (for dev/desktop).
    pub cwd: Option<PathBuf>,
    /// App version string.
    pub version: String,
    /// Arbitrary metadata.
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Default for TunnelConfig {
    fn default() -> Self {
        Self {
            relay_url: "wss://api.hanzo.ai/v1/relay".into(),
            auth_token: String::new(),
            app_kind: AppKind::Dev,
            display_name: hostname(),
            capabilities: Vec::new(),
            cwd: None,
            version: String::new(),
            metadata: HashMap::new(),
        }
    }
}

/// A live connection to the cloud relay.
pub struct TunnelConnection {
    /// Unique instance ID for this connection.
    pub instance_id: String,
    /// URL where this instance can be managed (set after registration).
    pub session_url: Option<String>,

    outgoing_tx: mpsc::Sender<Frame>,
    incoming_rx: tokio::sync::Mutex<mpsc::Receiver<Frame>>,
    shutdown_tx: watch::Sender<bool>,
    _task: tokio::task::JoinHandle<()>,
}

impl TunnelConnection {
    /// Send an event to the cloud.
    pub async fn send_event(
        &self,
        event: impl Into<String>,
        data: serde_json::Value,
    ) -> Result<(), TunnelError> {
        let frame = Frame::Event(EventPayload {
            event: event.into(),
            data,
        });
        self.outgoing_tx
            .send(frame)
            .await
            .map_err(|_| TunnelError::ChannelClosed)
    }

    /// Send a raw frame to the cloud.
    pub async fn send_frame(&self, frame: Frame) -> Result<(), TunnelError> {
        self.outgoing_tx
            .send(frame)
            .await
            .map_err(|_| TunnelError::ChannelClosed)
    }

    /// Receive the next command from the cloud. Returns None when the connection is closed.
    pub async fn recv_command(&self) -> Option<CommandPayload> {
        let mut rx = self.incoming_rx.lock().await;
        loop {
            match rx.recv().await {
                Some(Frame::Command(cmd)) => return Some(cmd),
                Some(Frame::Registered(reg)) => {
                    debug!(session_url = ?reg.session_url, "re-registered with cloud");
                    continue;
                }
                Some(_) => continue,
                None => return None,
            }
        }
    }

    /// Respond to a command from the cloud.
    pub async fn respond(
        &self,
        command_id: impl Into<String>,
        ok: bool,
        data: Option<serde_json::Value>,
    ) -> Result<(), TunnelError> {
        let frame = Frame::Response(ResponsePayload {
            id: command_id.into(),
            ok,
            data,
            error: None,
        });
        self.outgoing_tx
            .send(frame)
            .await
            .map_err(|_| TunnelError::ChannelClosed)
    }

    /// Respond to a command with an error.
    pub async fn respond_error(
        &self,
        command_id: impl Into<String>,
        error: impl Into<String>,
    ) -> Result<(), TunnelError> {
        let frame = Frame::Response(ResponsePayload {
            id: command_id.into(),
            ok: false,
            data: None,
            error: Some(error.into()),
        });
        self.outgoing_tx
            .send(frame)
            .await
            .map_err(|_| TunnelError::ChannelClosed)
    }

    /// Get a clone of the outgoing sender for use in spawned tasks.
    pub fn event_sender(&self) -> mpsc::Sender<Frame> {
        self.outgoing_tx.clone()
    }

    /// Shut down the tunnel connection.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Check if the connection is still alive.
    pub fn is_connected(&self) -> bool {
        !self.outgoing_tx.is_closed()
    }
}

impl Drop for TunnelConnection {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

const CHANNEL_CAPACITY: usize = 256;

/// Connect to the cloud relay.
///
/// This is the primary entry point. It establishes a WebSocket connection,
/// sends the registration frame, and returns a `TunnelConnection` that can
/// be used to send events and receive commands.
pub async fn connect(config: TunnelConfig) -> Result<TunnelConnection, TunnelError> {
    let instance_id = uuid::Uuid::new_v4().to_string();
    let platform = current_platform();

    let transport_config = TransportConfig {
        url: config.relay_url.clone(),
        auth: auth::AuthToken::from_string(&config.auth_token),
        ..Default::default()
    };

    // Channels for app ↔ transport communication.
    let (outgoing_tx, outgoing_rx) = mpsc::channel::<Frame>(CHANNEL_CAPACITY);
    let (incoming_tx, incoming_rx) = mpsc::channel::<Frame>(CHANNEL_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Build registration frame.
    let register_frame = Frame::Register(RegisterPayload {
        instance_id: instance_id.clone(),
        app_kind: config.app_kind,
        display_name: config.display_name.clone(),
        capabilities: config.capabilities.clone(),
        version: config.version.clone(),
        platform: platform.clone(),
        cwd: config.cwd.as_ref().map(|p| p.display().to_string()),
        metadata: config.metadata.clone(),
    });

    // Send registration as the first outgoing frame.
    outgoing_tx
        .send(register_frame)
        .await
        .map_err(|_| TunnelError::ChannelClosed)?;

    // Spawn the transport task.
    let task = tokio::spawn(transport::run_with_reconnect(
        transport_config,
        outgoing_rx,
        incoming_tx,
        shutdown_rx,
    ));

    info!(
        instance_id = %instance_id,
        app_kind = %config.app_kind,
        display_name = %config.display_name,
        relay_url = %config.relay_url,
        "tunnel connection initiated"
    );

    Ok(TunnelConnection {
        instance_id,
        session_url: None,
        outgoing_tx,
        incoming_rx: tokio::sync::Mutex::new(incoming_rx),
        shutdown_tx,
        _task: task,
    })
}

/// Convenience: connect and wait for the `Registered` acknowledgment.
///
/// Use this when you want to block until the cloud confirms registration.
pub async fn connect_and_register(
    config: TunnelConfig,
) -> Result<TunnelConnection, TunnelError> {
    let mut conn = connect(config).await?;

    // Wait for the Registered response (with timeout).
    let mut rx = conn.incoming_rx.lock().await;
    let registered = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match rx.recv().await {
                Some(Frame::Registered(reg)) => return Ok(reg),
                Some(_) => continue,
                None => return Err(TunnelError::Disconnected),
            }
        }
    })
    .await
    .map_err(|_| TunnelError::Connection("registration timed out".into()))??;

    conn.session_url = registered.session_url;
    drop(rx);

    info!(
        instance_id = %conn.instance_id,
        session_url = ?conn.session_url,
        "registered with cloud"
    );

    Ok(conn)
}

/// Get the current platform string (e.g., "darwin-arm64").
fn current_platform() -> String {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    format!("{os}-{arch}")
}

/// Get the local hostname.
fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = TunnelConfig::default();
        assert_eq!(config.relay_url, "wss://api.hanzo.ai/v1/relay");
        assert_eq!(config.app_kind, AppKind::Dev);
        assert!(config.capabilities.is_empty());
    }

    #[test]
    fn platform_string_format() {
        let platform = current_platform();
        assert!(platform.contains('-'), "platform should be os-arch: {platform}");
    }

    #[test]
    fn tunnel_error_display() {
        let err = TunnelError::Connection("refused".into());
        assert_eq!(err.to_string(), "connection error: refused");

        let err = TunnelError::Disconnected;
        assert_eq!(err.to_string(), "disconnected");
    }
}
