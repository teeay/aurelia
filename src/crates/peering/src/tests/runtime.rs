// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use crate::message_id::PeerMessageIdAllocator;
use crate::session::PeerSession;
use crate::taberna::{Taberna, TabernaInboxHandle, TabernaRegistry};
use crate::transport::primary_dispatch::PrimaryDispatchManager;
use crate::{DomusConfigAccess, DomusConfigBuilder};
use aurelia_ids::ErrorId;
use caducus::{CaducusErrorKind, MpscBuilder};

use super::TestCodec;

const RUNTIME_ASYNC_TEST_TIMEOUT: Duration = Duration::from_millis(500);

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
fn ack_waiter_drop_does_not_release_queue_without_runtime() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let session = runtime.block_on(async {
        let domus_config = DomusConfigBuilder::new()
            .send_queue_size(1)
            .send_timeout(Duration::from_millis(25))
            .callis_connect_timeout(Duration::from_millis(25))
            .accept_timeout(Duration::from_millis(25))
            .build()
            .expect("valid domus config");
        let store: DomusConfigAccess = DomusConfigAccess::from_config(domus_config);
        PeerSession::new(
            Arc::new(PeerMessageIdAllocator::default()),
            store,
            tokio::runtime::Handle::current(),
            PrimaryDispatchManager::new_for_tests(tokio::runtime::Handle::current()),
        )
    });

    let waiter = runtime.block_on(async {
        let (_message, waiter) = session
            .create_outgoing(
                1u64,
                2u64,
                crate::a3_message_type(0),
                0,
                bytes::Bytes::from_static(b"a"),
            )
            .await
            .expect("enqueue");
        waiter
    });

    drop(waiter);

    runtime.block_on(async {
        let err = match session
            .create_outgoing(
                1u64,
                2u64,
                crate::a3_message_type(0),
                0,
                bytes::Bytes::from_static(b"b"),
            )
            .await
        {
            Ok(_) => panic!("dropped waiter must not recall queued work"),
            Err(err) => err,
        };
        assert_eq!(err.kind, ErrorId::LocalQueueFull);
    });
}

#[tokio::test]
async fn caducus_send_with_deadline_accepts_future_deadline() {
    tokio::time::timeout(RUNTIME_ASYNC_TEST_TIMEOUT, async {
        let (sender, receiver) = MpscBuilder::<u64>::new(1, Duration::from_secs(1))
            .runtime(tokio::runtime::Handle::current())
            .build()
            .expect("caducus build");

        sender
            .send_with_deadline(7, std::time::Instant::now() + Duration::from_secs(1))
            .expect("future deadline accepted");

        let item = receiver
            .next(Some(std::time::Instant::now() + Duration::from_millis(50)))
            .await
            .expect("item received");
        assert_eq!(item, 7);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn caducus_send_with_deadline_rejects_expired_deadline() {
    tokio::time::timeout(RUNTIME_ASYNC_TEST_TIMEOUT, async {
        let (sender, _receiver) = MpscBuilder::<u64>::new(1, Duration::from_secs(1))
            .runtime(tokio::runtime::Handle::current())
            .build()
            .expect("caducus build");

        let err = sender
            .send_with_deadline(7, std::time::Instant::now())
            .expect_err("expired deadline rejected");
        assert!(matches!(err.kind, CaducusErrorKind::InvalidTTL(7)));
    })
    .await
    .expect("async test timed out");
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
