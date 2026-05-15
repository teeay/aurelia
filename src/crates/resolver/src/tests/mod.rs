// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::time::{timeout, Duration};

use super::*;

const RESOLVER_TEST_TIMEOUT: Duration = Duration::from_millis(100);

async fn run_resolver_test(test: impl Future<Output = ()>) {
    timeout(RESOLVER_TEST_TIMEOUT, test)
        .await
        .expect("resolver test timed out");
}

fn tcp_addr(port: u16) -> DomusAddr {
    DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
}

fn socket_addr(name: &str) -> DomusAddr {
    DomusAddr::Socket(PathBuf::from(name))
}

#[tokio::test]
async fn resolves_inserted_route() {
    run_resolver_test(async {
        let resolver = SimpleResolver::new();
        let taberna_id = 42;
        let addr = tcp_addr(5555);

        resolver.insert(taberna_id, addr.clone()).await;

        let resolved = resolver.resolve(taberna_id).await.expect("resolve");
        assert_eq!(resolved, addr);
    })
    .await;
}

#[tokio::test]
async fn insert_replaces_route() {
    run_resolver_test(async {
        let resolver = SimpleResolver::new();
        let taberna_id = 42;
        let first = tcp_addr(5555);
        let second = tcp_addr(5556);

        resolver.insert(taberna_id, first).await;
        resolver.insert(taberna_id, second.clone()).await;

        let resolved = resolver.resolve(taberna_id).await.expect("resolve");
        assert_eq!(resolved, second);
    })
    .await;
}

#[tokio::test]
async fn remove_deletes_route() {
    run_resolver_test(async {
        let resolver = SimpleResolver::new();
        let taberna_id = 42;

        resolver.insert(taberna_id, tcp_addr(5555)).await;
        resolver.remove(taberna_id).await;

        let err = resolver
            .resolve(taberna_id)
            .await
            .expect_err("unknown taberna");
        assert_eq!(err.kind, ErrorId::UnknownTaberna);
    })
    .await;
}

#[tokio::test]
async fn clear_all_deletes_routes() {
    run_resolver_test(async {
        let resolver = SimpleResolver::new();

        resolver.insert(1, tcp_addr(5555)).await;
        resolver.insert(2, tcp_addr(5556)).await;
        resolver.clear_all().await;

        let err = resolver.resolve(1).await.expect_err("unknown taberna 1");
        assert_eq!(err.kind, ErrorId::UnknownTaberna);
        let err = resolver.resolve(2).await.expect_err("unknown taberna 2");
        assert_eq!(err.kind, ErrorId::UnknownTaberna);
    })
    .await;
}

#[tokio::test]
async fn unknown_taberna_maps_to_unknown_taberna_error() {
    run_resolver_test(async {
        let resolver = SimpleResolver::new();

        let err = resolver.resolve(42).await.expect_err("unknown taberna");

        assert_eq!(err.kind, ErrorId::UnknownTaberna);
    })
    .await;
}

#[tokio::test]
async fn resolves_socket_route() {
    run_resolver_test(async {
        let resolver = SimpleResolver::new();
        let taberna_id = 42;
        let addr = socket_addr("/tmp/aurelia-resolver-test.sock");

        resolver.insert(taberna_id, addr.clone()).await;

        let resolved = resolver.resolve(taberna_id).await.expect("resolve");
        assert_eq!(resolved, addr);
    })
    .await;
}

#[tokio::test]
async fn remove_missing_route_is_noop() {
    run_resolver_test(async {
        let resolver = SimpleResolver::new();
        let installed_id = 42;
        let missing_id = 43;
        let addr = tcp_addr(5555);

        resolver.insert(installed_id, addr.clone()).await;
        resolver.remove(missing_id).await;

        let resolved = resolver.resolve(installed_id).await.expect("resolve");
        assert_eq!(resolved, addr);

        let err = resolver
            .resolve(missing_id)
            .await
            .expect_err("unknown taberna");
        assert_eq!(err.kind, ErrorId::UnknownTaberna);
    })
    .await;
}

#[tokio::test]
async fn clear_all_empty_is_noop() {
    run_resolver_test(async {
        let resolver = SimpleResolver::new();

        resolver.clear_all().await;

        let err = resolver.resolve(42).await.expect_err("unknown taberna");
        assert_eq!(err.kind, ErrorId::UnknownTaberna);
    })
    .await;
}

#[tokio::test]
async fn concurrent_resolve_observes_route_updates() {
    run_resolver_test(async {
        let resolver = Arc::new(SimpleResolver::new());
        let taberna_id = 42;
        let first = tcp_addr(5555);
        let second = tcp_addr(5556);

        resolver.insert(taberna_id, first.clone()).await;

        let reader = {
            let resolver = Arc::clone(&resolver);
            let expected_second = second.clone();
            tokio::spawn(async move {
                let mut observed_first = false;
                let mut observed_second = false;
                for _ in 0..16 {
                    let resolved = resolver.resolve(taberna_id).await.expect("resolve");
                    observed_first |= resolved == first;
                    observed_second |= resolved == expected_second;
                    if observed_first && observed_second {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
                panic!("resolver did not expose both route values");
            })
        };

        tokio::task::yield_now().await;
        resolver.insert(taberna_id, second.clone()).await;
        reader.await.expect("reader task");
    })
    .await;
}
