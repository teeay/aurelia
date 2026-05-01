// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use crate::message_id::PeerMessageIdAllocator;
use crate::session::{BackpressureConfig, PeerSession};
use crate::taberna::{Taberna, TabernaInboxHandle, TabernaRegistry};
use crate::{DomusConfigAccess, DomusConfigBuilder};
use aurelia_ids::ErrorId;
use caducus::MpscBuilder;

use super::TestCodec;

#[test]
fn taberna_drop_unregisters_without_runtime() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let registry = Arc::new(TabernaRegistry::new());
    let taberna_id = 7u64;
    let taberna = runtime.block_on(async {
        let config = DomusConfigBuilder::new()
            .build()
            .expect("valid domus config");
        let config_access = DomusConfigAccess::from_config(config.clone());
        let (sender, receiver) =
            MpscBuilder::new(config.taberna_accept_queue_size, config.accept_timeout)
                .runtime(tokio::runtime::Handle::current())
                .build()
                .expect("caducus build");
        let inbox = Arc::new(TabernaInboxHandle::new(
            TestCodec,
            sender,
            config_access.clone(),
            config.taberna_accept_queue_size,
            config.accept_timeout,
        ));
        registry
            .register(taberna_id, inbox)
            .await
            .expect("register");
        Taberna::new(
            taberna_id,
            receiver,
            Arc::clone(&registry),
            tokio::runtime::Handle::current(),
        )
    });

    drop(taberna);

    runtime.block_on(async {
        let mut cleared = false;
        for _ in 0..20 {
            if registry.resolve_local(taberna_id).await.is_none() {
                cleared = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(cleared, "taberna should unregister on drop");
    });
}

#[test]
fn ack_waiter_drop_releases_queue_without_runtime() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let session = runtime.block_on(async {
        let config = BackpressureConfig {
            send_queue_size: 1,
            inflight_window: 1,
            send_timeout: Duration::from_millis(25),
        };
        let domus_config = DomusConfigBuilder::new()
            .send_queue_size(config.send_queue_size)
            .inflight_window(config.inflight_window)
            .send_timeout(config.send_timeout)
            .accept_timeout(config.send_timeout)
            .build()
            .expect("valid domus config");
        let store: DomusConfigAccess = DomusConfigAccess::from_config(domus_config);
        PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            store,
            tokio::runtime::Handle::current(),
        )
    });

    let waiter = runtime.block_on(async {
        let (_message, waiter) = session
            .create_outgoing(1u64, 2u64, 1u32, 0, bytes::Bytes::from_static(b"a"))
            .await
            .expect("enqueue");
        waiter
    });

    drop(waiter);

    runtime.block_on(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _next = session
            .create_outgoing(1u64, 2u64, 1u32, 0, bytes::Bytes::from_static(b"b"))
            .await
            .expect("queue released after drop");
    });
}

#[tokio::test]
async fn taberna_next_timeout_returns_receive_timeout() {
    let registry = Arc::new(TabernaRegistry::new());
    let taberna_id = 77u64;
    let domus_config = DomusConfigBuilder::new()
        .accept_timeout(Duration::from_secs(1))
        .taberna_accept_queue_size(1)
        .build()
        .expect("valid domus config");
    let config_access = DomusConfigAccess::from_config(domus_config.clone());
    let (sender, receiver) = MpscBuilder::new(
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    )
    .runtime(tokio::runtime::Handle::current())
    .build()
    .expect("caducus build");
    let inbox = Arc::new(TabernaInboxHandle::new(
        TestCodec,
        sender,
        config_access.clone(),
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    ));
    registry
        .register(taberna_id, inbox)
        .await
        .expect("register");
    let taberna = Taberna::new(
        taberna_id,
        receiver,
        Arc::clone(&registry),
        tokio::runtime::Handle::current(),
    );

    let err = match taberna.next(Some(Duration::from_millis(20))).await {
        Ok(_) => panic!("expected receive-timeout"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::ReceiveTimeout);
}

#[tokio::test]
async fn taberna_next_shutdown_returns_domus_closed() {
    let registry = Arc::new(TabernaRegistry::new());
    let taberna_id = 78u64;
    let domus_config = DomusConfigBuilder::new()
        .accept_timeout(Duration::from_secs(1))
        .taberna_accept_queue_size(1)
        .build()
        .expect("valid domus config");
    let config_access = DomusConfigAccess::from_config(domus_config.clone());
    let (sender, receiver) = MpscBuilder::new(
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    )
    .runtime(tokio::runtime::Handle::current())
    .build()
    .expect("caducus build");
    let inbox = Arc::new(TabernaInboxHandle::new(
        TestCodec,
        sender.clone(),
        config_access.clone(),
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    ));
    registry
        .register(taberna_id, inbox)
        .await
        .expect("register");
    let taberna = Taberna::new(
        taberna_id,
        receiver,
        Arc::clone(&registry),
        tokio::runtime::Handle::current(),
    );

    sender.shutdown();
    let err = match taberna.next(Some(Duration::from_millis(20))).await {
        Ok(_) => panic!("expected domus-closed"),
        Err(err) => err,
    };
    assert_eq!(err.kind, ErrorId::DomusClosed);
}
