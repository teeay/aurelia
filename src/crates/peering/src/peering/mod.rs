// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::config::DomusConfigAccess;
use crate::taberna::TabernaRegistry;
use crate::transport::{BlobBufferTracker, Transport, TransportBackend, TransportBackendImpl};
use aurelia_data::DomusAddr;
use aurelia_data::RouteResolver;

mod dispatch;

#[cfg(test)]
pub(crate) use dispatch::validate_app_send;

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

impl<RR, B> RouteLocalRemote<RR, B>
where
    RR: RouteResolver,
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    pub(crate) fn new(
        config: DomusConfigAccess,
        registry: Arc<TabernaRegistry>,
        resolver: Arc<RR>,
        transport: Arc<Transport<B>>,
    ) -> Self {
        let blob_buffers = transport.blob_buffers();
        Self {
            registry,
            resolver,
            transport,
            blob_buffers,
            config,
        }
    }
}
