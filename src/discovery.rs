//! Local LAN discovery via mDNS (optional, behind `mdns` feature).
//!
//! When enabled, instances advertise themselves on the local network so
//! other Hanzo apps can discover them without cloud relay.

use crate::protocol::AppKind;

/// Service type for mDNS advertisement.
pub const MDNS_SERVICE_TYPE: &str = "_hanzo._tcp.local.";

/// Metadata for a discovered local instance.
#[derive(Debug, Clone)]
pub struct LocalInstance {
    pub instance_id: String,
    pub app_kind: AppKind,
    pub display_name: String,
    pub host: String,
    pub port: u16,
}

/// Advertise this instance on the local network.
///
/// Returns a handle that stops advertisement when dropped.
#[cfg(feature = "mdns")]
pub async fn advertise(
    instance_id: &str,
    app_kind: AppKind,
    display_name: &str,
    port: u16,
) -> Result<mdns_sd::ServiceDaemon, crate::TunnelError> {
    use std::collections::HashMap;

    let mdns = mdns_sd::ServiceDaemon::new()
        .map_err(|e| crate::TunnelError::Discovery(e.to_string()))?;

    let mut properties = HashMap::new();
    properties.insert("id".to_string(), instance_id.to_string());
    properties.insert("kind".to_string(), app_kind.to_string());
    properties.insert("name".to_string(), display_name.to_string());

    let service_name = format!("{}-{}", app_kind, &instance_id[..8.min(instance_id.len())]);
    let service = mdns_sd::ServiceInfo::new(
        MDNS_SERVICE_TYPE,
        &service_name,
        &format!("{service_name}.local."),
        "",
        port,
        properties,
    )
    .map_err(|e| crate::TunnelError::Discovery(e.to_string()))?;

    mdns.register(service)
        .map_err(|e| crate::TunnelError::Discovery(e.to_string()))?;

    tracing::info!(instance_id, %app_kind, port, "mDNS advertisement started");
    Ok(mdns)
}

/// Stub when mDNS feature is not enabled.
#[cfg(not(feature = "mdns"))]
pub async fn advertise(
    _instance_id: &str,
    _app_kind: AppKind,
    _display_name: &str,
    _port: u16,
) -> Result<(), crate::TunnelError> {
    tracing::debug!("mDNS discovery not enabled (compile with `mdns` feature)");
    Ok(())
}
