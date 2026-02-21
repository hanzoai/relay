//! WebSocket transport with automatic reconnection and exponential backoff.

use crate::auth::AuthToken;
use crate::protocol::Frame;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, info, warn};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;
type WsSource = SplitStream<WsStream>;

/// Configuration for the WebSocket transport.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// WebSocket URL to connect to.
    pub url: String,
    /// Auth token for the connection.
    pub auth: AuthToken,
    /// Initial reconnect delay (doubles on each failure, capped at max).
    pub initial_backoff: Duration,
    /// Maximum reconnect delay.
    pub max_backoff: Duration,
    /// Heartbeat interval — send ping every N seconds.
    pub heartbeat_interval: Duration,
    /// Timeout for the initial WebSocket handshake.
    pub connect_timeout: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            url: "wss://api.hanzo.ai/v1/relay".into(),
            auth: AuthToken::ApiKey {
                key: String::new(),
            },
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

/// Establish a WebSocket connection with auth header.
async fn ws_connect(config: &TransportConfig) -> Result<(WsSink, WsSource), crate::TunnelError> {
    let mut request = config
        .url
        .as_str()
        .into_client_request()
        .map_err(|e| crate::TunnelError::Connection(e.to_string()))?;

    // Add auth header.
    let bearer = format!("Bearer {}", config.auth.bearer());
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&bearer)
            .map_err(|e| crate::TunnelError::Connection(e.to_string()))?,
    );

    let connect_fut = connect_async(request);
    let (ws_stream, _response) = tokio::time::timeout(config.connect_timeout, connect_fut)
        .await
        .map_err(|_| crate::TunnelError::Connection("connection timed out".into()))?
        .map_err(|e| crate::TunnelError::Connection(e.to_string()))?;

    info!(url = %config.url, "tunnel connected");
    Ok(ws_stream.split())
}

/// Run the transport loop: read from WS → commands channel, write from events channel → WS.
///
/// Returns when the connection drops or shutdown is signaled.
/// If `reconnect` feature is enabled, the caller should call this again with backoff.
pub(crate) async fn run_transport(
    config: &TransportConfig,
    outgoing_rx: &mut mpsc::Receiver<Frame>,
    incoming_tx: &mpsc::Sender<Frame>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), crate::TunnelError> {
    let (mut sink, mut stream) = ws_connect(config).await?;

    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    // Don't send heartbeat immediately on connect.
    heartbeat.tick().await;

    loop {
        tokio::select! {
            // Shutdown signal.
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    debug!("tunnel shutdown requested");
                    let _ = sink.close().await;
                    return Ok(());
                }
            }

            // Outgoing frame from the app.
            Some(frame) = outgoing_rx.recv() => {
                let json = serde_json::to_string(&frame)
                    .map_err(|e| crate::TunnelError::Protocol(e.to_string()))?;
                sink.send(Message::Text(json.into())).await
                    .map_err(|e| crate::TunnelError::Connection(e.to_string()))?;
            }

            // Incoming message from the cloud.
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<Frame>(&text) {
                            Ok(Frame::Ping) => {
                                let pong = serde_json::to_string(&Frame::Pong)
                                    .map_err(|e| crate::TunnelError::Protocol(e.to_string()))?;
                                sink.send(Message::Text(pong.into())).await
                                    .map_err(|e| crate::TunnelError::Connection(e.to_string()))?;
                            }
                            Ok(Frame::Pong) => {
                                // Heartbeat acknowledged, nothing to do.
                            }
                            Ok(frame) => {
                                if incoming_tx.send(frame).await.is_err() {
                                    debug!("incoming channel closed, shutting down transport");
                                    return Ok(());
                                }
                            }
                            Err(e) => {
                                warn!(text = %text, error = %e, "ignoring malformed frame");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        // WebSocket-level ping — respond with pong.
                        let _ = sink.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        info!("tunnel connection closed by remote");
                        return Err(crate::TunnelError::Disconnected);
                    }
                    Some(Err(e)) => {
                        error!(error = %e, "tunnel read error");
                        return Err(crate::TunnelError::Connection(e.to_string()));
                    }
                    Some(Ok(_)) => {
                        // Binary or other frame — ignore.
                    }
                }
            }

            // Heartbeat timer.
            _ = heartbeat.tick() => {
                let ping = serde_json::to_string(&Frame::Ping)
                    .map_err(|e| crate::TunnelError::Protocol(e.to_string()))?;
                if let Err(e) = sink.send(Message::Text(ping.into())).await {
                    warn!(error = %e, "heartbeat send failed");
                    return Err(crate::TunnelError::Connection(e.to_string()));
                }
            }
        }
    }
}

/// Run the transport with automatic reconnection.
///
/// Keeps reconnecting until shutdown is signaled.
pub(crate) async fn run_with_reconnect(
    config: TransportConfig,
    mut outgoing_rx: mpsc::Receiver<Frame>,
    incoming_tx: mpsc::Sender<Frame>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut backoff = config.initial_backoff;

    loop {
        if *shutdown.borrow() {
            break;
        }

        match run_transport(&config, &mut outgoing_rx, &incoming_tx, shutdown.clone()).await {
            Ok(()) => {
                // Clean shutdown.
                break;
            }
            Err(crate::TunnelError::Disconnected) => {
                info!(backoff_ms = backoff.as_millis(), "reconnecting after disconnect");
            }
            Err(e) => {
                warn!(error = %e, backoff_ms = backoff.as_millis(), "reconnecting after error");
            }
        }

        if *shutdown.borrow() {
            break;
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(config.max_backoff);
    }

    debug!("transport loop exited");
}
