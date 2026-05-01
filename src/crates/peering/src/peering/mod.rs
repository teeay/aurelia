// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::time::timeout;
use tracing::{debug, warn};

use crate::address::DomusAddr;
use crate::config::DomusConfigAccess;
use crate::routing::RouteResolver;
use crate::taberna::{TabernaInbox, TabernaRegistry};
use crate::transport::{BlobBufferTracker, Transport, TransportBackend, TransportBackendImpl};
use aurelia_ids::{AureliaError, ErrorId, MessageType, TabernaId};

mod builder;
mod dispatch;

pub use builder::RouteLocalRemoteBuilder;

#[derive(Clone)]
pub struct RouteLocalRemote<RR, B = TransportBackendImpl>
where
    RR: RouteResolver,
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    pub(crate) registry: Arc<TabernaRegistry>,
    pub(crate) resolver: Arc<RR>,
    pub(crate) transport: Arc<Transport<B>>,
    pub(crate) blob_buffers: Arc<BlobBufferTracker>,
    pub(crate) config: DomusConfigAccess,
}

pub(super) struct DispatchLogs {
    pub(super) local: &'static str,
    pub(super) resolve_failed: &'static str,
    pub(super) resolve_timeout: &'static str,
    pub(super) remote: &'static str,
}

pub(super) enum DispatchTarget {
    Local(Arc<dyn TabernaInbox>),
    Remote(DomusAddr),
}

pub(super) async fn resolve_dispatch_target<RR>(
    registry: &TabernaRegistry,
    resolver: &RR,
    config: &DomusConfigAccess,
    taberna_id: TabernaId,
    msg_type: MessageType,
    logs: DispatchLogs,
) -> Result<DispatchTarget, AureliaError>
where
    RR: RouteResolver + ?Sized,
{
    if let Some(sink) = registry.resolve_local(taberna_id).await {
        debug!(taberna_id, msg_type, "{}", logs.local);
        return Ok(DispatchTarget::Local(sink));
    }

    let config = config.snapshot().await;
    let resolve = resolver.resolve(taberna_id);
    let peer = match timeout(config.send_timeout, resolve).await {
        Ok(Ok(peer)) => peer,
        Ok(Err(err)) => {
            warn!(taberna_id, msg_type, error = %err, "{}", logs.resolve_failed);
            return Err(err);
        }
        Err(_) => {
            warn!(taberna_id, msg_type, "{}", logs.resolve_timeout);
            return Err(AureliaError::new(ErrorId::SendTimeout));
        }
    };
    debug!(taberna_id, msg_type, peer = %peer, "{}", logs.remote);
    Ok(DispatchTarget::Remote(peer))
}
