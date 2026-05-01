// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::oob_control::{dispatch, process_request, ConnectionOutcome, DispatchOutcome};
use crate::{Behavior, BehaviorState, RuntimeCommand};

fn behavior_state() -> Arc<BehaviorState> {
    Arc::new(BehaviorState::new())
}

#[tokio::test]
async fn dispatch_ready_returns_ok() {
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);
    let outcome = dispatch("ready", &app, &tx).await.expect("ready ok");
    assert_eq!(outcome, DispatchOutcome::Ok);
}

#[tokio::test]
async fn dispatch_ready_rejects_arguments() {
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("ready now", &app, &tx)
        .await
        .expect_err("ready arg rejected");
    assert!(err.contains("ready"));
}

#[tokio::test]
async fn dispatch_set_app_block_changes_behavior() {
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("set app block", &app, &tx).await.expect("set ok");
    assert_eq!(app.current().await, Behavior::Block);
}

#[tokio::test]
async fn dispatch_set_app_block_with_duration_restores_normal() {
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("set app block 50", &app, &tx)
        .await
        .expect("set ok");
    assert_eq!(app.current().await, Behavior::Block);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(app.current().await, Behavior::Normal);
}

#[tokio::test]
async fn dispatch_set_unknown_behavior_errors() {
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("set app weird", &app, &tx)
        .await
        .expect_err("unknown behavior");
    assert!(err.contains("weird"));
}

#[tokio::test]
async fn dispatch_unblock_app_sets_normal() {
    let app = behavior_state();
    app.set(Behavior::Block).await;
    let (tx, _rx) = mpsc::channel(1);
    dispatch("unblock app", &app, &tx)
        .await
        .expect("unblock ok");
    assert_eq!(app.current().await, Behavior::Normal);
}

#[tokio::test]
async fn dispatch_shutdown_signals_runtime() {
    let app = behavior_state();
    let (tx, mut rx) = mpsc::channel(1);
    dispatch("shutdown 5000", &app, &tx)
        .await
        .expect("shutdown ok");
    let cmd = rx.recv().await.expect("runtime cmd");
    match cmd {
        RuntimeCommand::Shutdown { downtime } => {
            assert_eq!(downtime, Duration::from_millis(5000));
        }
        other => panic!("unexpected runtime command: {other:?}"),
    }
}

#[tokio::test]
async fn dispatch_shutdown_clamps_to_minimum() {
    let app = behavior_state();
    let (tx, mut rx) = mpsc::channel(1);
    dispatch("shutdown 100", &app, &tx)
        .await
        .expect("shutdown ok");
    let cmd = rx.recv().await.expect("runtime cmd");
    match cmd {
        RuntimeCommand::Shutdown { downtime } => {
            assert!(downtime >= Duration::from_millis(crate::MIN_SHUTDOWN_DOWNTIME_MS));
        }
        other => panic!("unexpected runtime command: {other:?}"),
    }
}

#[tokio::test]
async fn dispatch_crash_returns_crash_outcome_without_runtime_signal() {
    let app = behavior_state();
    let (tx, mut rx) = mpsc::channel(1);
    let outcome = dispatch("crash", &app, &tx).await.expect("crash ok");
    assert_eq!(outcome, DispatchOutcome::Crash);
    // Crash does not signal the runtime channel; the connection task drives the exit.
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn dispatch_reload_auth_signals_runtime() {
    let app = behavior_state();
    let (tx, mut rx) = mpsc::channel(1);
    dispatch("reload-auth", &app, &tx)
        .await
        .expect("reload-auth ok");
    let cmd = rx.recv().await.expect("runtime cmd");
    assert!(matches!(cmd, RuntimeCommand::ReloadAuth));
}

#[tokio::test]
async fn dispatch_unknown_command_errors() {
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("netem apply partition", &app, &tx)
        .await
        .expect_err("unknown command");
    assert!(err.contains("netem"));
}

#[tokio::test]
async fn dispatch_empty_command_errors() {
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("", &app, &tx).await.expect_err("empty");
    assert!(err.contains("empty"));
}

async fn loopback_pair() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    (listener, addr)
}

#[tokio::test]
async fn process_request_ok_returns_done_and_writes_ok() {
    let (listener, addr) = loopback_pair().await;
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);

    let app_clone = Arc::clone(&app);
    let server = tokio::spawn(async move {
        let (server_stream, _) = listener.accept().await.expect("accept");
        process_request(server_stream, &app_clone, &tx).await
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    client.write_all(b"ready\n").await.expect("write");
    let mut reader = BufReader::new(&mut client);
    let mut response = String::new();
    reader.read_line(&mut response).await.expect("read");
    drop(client);

    let outcome = server.await.expect("join");
    assert_eq!(outcome, ConnectionOutcome::Done);
    assert_eq!(response, "OK\n");
}

#[tokio::test]
async fn process_request_crash_waits_for_client_eof() {
    let (listener, addr) = loopback_pair().await;
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);

    let app_clone = Arc::clone(&app);
    let server = tokio::spawn(async move {
        let (server_stream, _) = listener.accept().await.expect("accept");
        process_request(server_stream, &app_clone, &tx).await
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    client.write_all(b"crash\n").await.expect("write");

    // Read OK without closing.
    let mut response = String::new();
    {
        let mut reader = BufReader::new(&mut client);
        reader.read_line(&mut response).await.expect("read");
    }
    assert_eq!(response, "OK\n");

    // Server should still be waiting on EOF — it must not have returned yet.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!server.is_finished(), "server returned before client EOF");

    // Close the client; server now observes EOF and returns Crash.
    drop(client);
    let outcome = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server timeout")
        .expect("join");
    assert_eq!(outcome, ConnectionOutcome::Crash);
}

#[tokio::test]
async fn process_request_oversize_returns_err() {
    let (listener, addr) = loopback_pair().await;
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);

    let app_clone = Arc::clone(&app);
    let server = tokio::spawn(async move {
        let (server_stream, _) = listener.accept().await.expect("accept");
        process_request(server_stream, &app_clone, &tx).await
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    let big = vec![b'a'; 2048];
    client.write_all(&big).await.expect("write");
    client.shutdown().await.expect("shutdown write");

    let mut response = String::new();
    let mut reader = BufReader::new(&mut client);
    reader.read_line(&mut response).await.expect("read");
    drop(client);

    let outcome = server.await.expect("join");
    assert_eq!(outcome, ConnectionOutcome::Done);
    assert!(response.starts_with("ERR oversize"), "got: {response}");
}

#[tokio::test]
async fn process_request_unknown_command_returns_err() {
    let (listener, addr) = loopback_pair().await;
    let app = behavior_state();
    let (tx, _rx) = mpsc::channel(1);

    let app_clone = Arc::clone(&app);
    let server = tokio::spawn(async move {
        let (server_stream, _) = listener.accept().await.expect("accept");
        process_request(server_stream, &app_clone, &tx).await
    });

    let mut client = TcpStream::connect(addr).await.expect("connect");
    client.write_all(b"foobar\n").await.expect("write");
    let mut response = String::new();
    let mut reader = BufReader::new(&mut client);
    reader.read_line(&mut response).await.expect("read");
    drop(client);

    let outcome = server.await.expect("join");
    assert_eq!(outcome, ConnectionOutcome::Done);
    assert!(
        response.starts_with("ERR unknown command"),
        "got: {response}"
    );
}
