// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::super::backend::AuthenticatedStream;
use super::*;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::SendOptions;

const BACKEND_TEST_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Clone, Default)]
struct MockBackend {
    dial_calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl TransportBackend for MockBackend {
    type Addr = DomusAddr;
    type Listener = ();
    type Stream = tokio::io::DuplexStream;

    async fn bind(&self, _local: &Self::Addr) -> Result<Self::Listener, AureliaError> {
        Ok(())
    }

    async fn accept(
        &self,
        _listener: &mut Self::Listener,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        Err(AureliaError::new(ErrorId::PeerUnavailable))
    }

    async fn dial(
        &self,
        _peer: &Self::Addr,
    ) -> Result<AuthenticatedStream<Self::Stream, Self::Addr>, AureliaError> {
        self.dial_calls.fetch_add(1, Ordering::SeqCst);
        Err(AureliaError::new(ErrorId::PeerUnavailable))
    }
}

#[tokio::test]
async fn mismatched_peer_addr_is_rejected_before_backend_dial() {
    tokio::time::timeout(BACKEND_TEST_TIMEOUT, async {
        let backend = Arc::new(MockBackend::default());
        let registry = Arc::new(TabernaRegistry::new());
        let config: DomusConfigAccess = DomusConfigAccess::from_config(DomusConfig::default());

        let transport = Transport::bind_with_backend(
            DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
            registry,
            config,
            crate::observability::new_observability(tokio::runtime::Handle::current()).1,
            tokio::runtime::Handle::current(),
            Arc::clone(&backend),
        )
        .await
        .expect("bind transport");

        let err = transport
            .send_remote(
                DomusAddr::Socket(PathBuf::from("/tmp/aurelia.sock")),
                1,
                42,
                Bytes::new(),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect_err("expected mismatch error");

        assert_eq!(err.kind, ErrorId::AddressMismatch);
        assert_eq!(backend.dial_calls.load(Ordering::SeqCst), 0);
    })
    .await
    .expect("async test timed out");
}
