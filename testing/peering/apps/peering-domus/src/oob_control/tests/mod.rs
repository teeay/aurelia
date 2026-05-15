// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::oob_control::{dispatch, process_request, ConnectionOutcome, DispatchOutcome};
use crate::{Behavior, ControlState, RuntimeCommand};

const OOB_CONTROL_UNIT_TEST_TIMEOUT: Duration = Duration::from_secs(1);

fn control_state() -> Arc<ControlState> {
    Arc::new(ControlState::new())
}

#[tokio::test]
async fn dispatch_ready_returns_ok() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    let outcome = dispatch("ready", &app, &tx).await.expect("ready ok");
    assert_eq!(outcome, DispatchOutcome::Ok);

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_ready_rejects_arguments() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("ready now", &app, &tx)
        .await
        .expect_err("ready arg rejected");
    assert!(err.contains("ready"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_set_app_block_changes_behavior() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("set app block", &app, &tx).await.expect("set ok");
    assert_eq!(app.app.current().await, Behavior::Block);

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_set_app_block_with_duration_restores_normal() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("set app block 50", &app, &tx)
        .await
        .expect("set ok");
    assert_eq!(app.app.current().await, Behavior::Block);
    // aurelia-test-allow-sleep: behavior-duration; the test validates automatic restoration
    // after the requested app block duration.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(app.app.current().await, Behavior::Normal);

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_set_unknown_behavior_errors() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("set app weird", &app, &tx)
        .await
        .expect_err("unknown behavior");
    assert!(err.contains("weird"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_unblock_app_sets_normal() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    app.app.set(Behavior::Block).await;
    let (tx, _rx) = mpsc::channel(1);
    dispatch("unblock app", &app, &tx)
        .await
        .expect("unblock ok");
    assert_eq!(app.app.current().await, Behavior::Normal);

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_wait_app_blocked_observes_reported_handler() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("set app block", &app, &tx).await.expect("set ok");
    app.app
        .report_blocked()
        .await
        .expect("report blocked generation");
    let outcome = dispatch("wait app blocked 100", &app, &tx)
        .await
        .expect("wait ok");
    assert_eq!(outcome, DispatchOutcome::Ok);

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_wait_app_blocked_wrong_state_errors() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("wait app blocked 100", &app, &tx)
        .await
        .expect_err("wrong state");
    assert!(err.contains("app is not in block behavior"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_wait_app_blocked_times_out() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("set app block", &app, &tx).await.expect("set ok");
    let err = dispatch("wait app blocked 5", &app, &tx)
        .await
        .expect_err("timeout");
    assert!(err.contains("app blocked timeout"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_arm_and_wait_app_blob_started_observes_reported_blob() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("arm app blob-started", &app, &tx)
        .await
        .expect("arm ok");
    app.app.report_blob_started();
    let outcome = dispatch("wait app blob-started 100", &app, &tx)
        .await
        .expect("wait ok");
    assert_eq!(outcome, DispatchOutcome::Ok);

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_wait_app_blob_started_requires_arm() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("wait app blob-started 100", &app, &tx)
        .await
        .expect_err("not armed");
    assert!(err.contains("not armed"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_wait_app_blob_started_times_out() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("arm app blob-started", &app, &tx)
        .await
        .expect("arm ok");
    let err = dispatch("wait app blob-started 5", &app, &tx)
        .await
        .expect_err("timeout");
    assert!(err.contains("app blob-started timeout"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_set_actix_block_changes_behavior() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("set actix block", &app, &tx)
        .await
        .expect("set ok");
    assert_eq!(app.actix.current(), Behavior::Block);

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_unblock_actix_sets_normal() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    app.actix.set(Behavior::Block).await;
    let (tx, _rx) = mpsc::channel(1);
    dispatch("unblock actix", &app, &tx)
        .await
        .expect("unblock ok");
    assert_eq!(app.actix.current(), Behavior::Normal);

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_wait_actix_blocked_observes_reported_handler() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("set actix block", &app, &tx)
        .await
        .expect("set ok");
    app.actix.report_blocked();
    let outcome = dispatch("wait actix blocked 100", &app, &tx)
        .await
        .expect("wait ok");
    assert_eq!(outcome, DispatchOutcome::Ok);

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_wait_actix_blocked_wrong_state_errors() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("wait actix blocked 100", &app, &tx)
        .await
        .expect_err("wrong state");
    assert!(err.contains("actix is not in block behavior"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_wait_actix_blocked_times_out() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    dispatch("set actix block", &app, &tx)
        .await
        .expect("set ok");
    let err = dispatch("wait actix blocked 5", &app, &tx)
        .await
        .expect_err("timeout");
    assert!(err.contains("actix blocked timeout"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_wait_rejects_malformed_command() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("wait app blocked", &app, &tx)
        .await
        .expect_err("missing timeout");
    assert!(err.contains("missing timeout"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_stop_actix_without_actor_errors() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("stop actix", &app, &tx)
        .await
        .expect_err("missing actor");
    assert!(err.contains("actix actor missing"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_shutdown_signals_runtime() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
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

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_shutdown_clamps_to_minimum() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
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

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_crash_returns_crash_outcome_without_runtime_signal() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, mut rx) = mpsc::channel(1);
    let outcome = dispatch("crash", &app, &tx).await.expect("crash ok");
    assert_eq!(outcome, DispatchOutcome::Crash);
    // Crash does not signal the runtime channel; the connection task drives the exit.
    assert!(rx.try_recv().is_err());

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_reload_auth_signals_runtime() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, mut rx) = mpsc::channel(1);
    dispatch("reload-auth", &app, &tx)
        .await
        .expect("reload-auth ok");
    let cmd = rx.recv().await.expect("runtime cmd");
    assert!(matches!(cmd, RuntimeCommand::ReloadAuth));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_unknown_command_errors() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("netem apply partition", &app, &tx)
        .await
        .expect_err("unknown command");
    assert!(err.contains("netem"));

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn dispatch_empty_command_errors() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let app = control_state();
    let (tx, _rx) = mpsc::channel(1);
    let err = dispatch("", &app, &tx).await.expect_err("empty");
    assert!(err.contains("empty"));

    })
    .await
    .expect("async test timed out");
}

async fn loopback_pair() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    (listener, addr)
}

#[tokio::test]
async fn process_request_ok_returns_done_and_writes_ok() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let (listener, addr) = loopback_pair().await;
    let app = control_state();
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

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn process_request_crash_waits_for_client_eof() {
    let (listener, addr) = loopback_pair().await;
    let app = control_state();
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

    // Negative assertion: no completion event should exist until the client drops its half, so this
    // bounded wait is the observation window for "must not have returned yet."
    // aurelia-test-allow-sleep: negative-assertion; there is no positive event for "server has not
    // returned before client EOF".
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
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let (listener, addr) = loopback_pair().await;
    let app = control_state();
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

    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn process_request_unknown_command_returns_err() {
    tokio::time::timeout(OOB_CONTROL_UNIT_TEST_TIMEOUT, async {
    let (listener, addr) = loopback_pair().await;
    let app = control_state();
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

    })
    .await
    .expect("async test timed out");
}
