// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::address::DomusAddr;
use crate::config::DomusConfigAccess;
use crate::routing::RouteResolver;
use crate::taberna::TabernaRegistry;
use crate::transport::{BlobBufferTracker, Transport, TransportBackend, TransportBackendImpl};

use super::RouteLocalRemote;

pub struct RouteLocalRemoteBuilder<RR, B = TransportBackendImpl>
where
    RR: RouteResolver,
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    registry: Arc<TabernaRegistry>,
    resolver: Arc<RR>,
    transport: Arc<Transport<B>>,
    blob_buffers: Arc<BlobBufferTracker>,
    config: DomusConfigAccess,
}

impl<RR, B> RouteLocalRemoteBuilder<RR, B>
where
    RR: RouteResolver,
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    pub fn new(
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

    pub fn build(self) -> RouteLocalRemote<RR, B> {
        RouteLocalRemote {
            registry: self.registry,
            resolver: self.resolver,
            transport: self.transport,
            blob_buffers: self.blob_buffers,
            config: self.config,
        }
    }
}
