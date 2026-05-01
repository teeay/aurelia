// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};

use crate::{
    parse_behavior, Behavior, BehaviorState, RuntimeCommand, DEFAULT_SHUTDOWN_DOWNTIME_MS,
    MIN_SHUTDOWN_DOWNTIME_MS,
};

const MAX_REQUEST_BYTES: u64 = 1024;
const READ_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ConnectionOutcome {
    Done,
    Crash,
}

/// Bind the OOB control listener.
///
/// Caller must guarantee that the peer's A1 layer (Domus) is fully built and the response
/// taberna has been registered before calling this. The OOB plane's `ready` command relies on
/// this ordering: a successful TCP connect to `addr` is a strict precondition that A1 is up.
pub async fn serve(
    addr: SocketAddr,
    app: Arc<BehaviorState>,
    runtime_tx: mpsc::Sender<RuntimeCommand>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), io::Error> {
    let listener = TcpListener::bind(addr).await?;
    accept_loop(listener, app, runtime_tx, shutdown_rx).await;
    Ok(())
}

async fn accept_loop(
    listener: TcpListener,
    app: Arc<BehaviorState>,
    runtime_tx: mpsc::Sender<RuntimeCommand>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        tokio::select! {
            _ = shutdown_rx.changed() => {
                continue;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer)) => {
                        let app = Arc::clone(&app);
                        let runtime_tx = runtime_tx.clone();
                        tokio::spawn(async move {
                            if matches!(
                                process_request(stream, &app, &runtime_tx).await,
                                ConnectionOutcome::Crash
                            ) {
                                std::process::exit(2);
                            }
                        });
                    }
                    Err(err) => {
                        eprintln!("oob accept failed: {err}");
                    }
                }
            }
        }
    }
}

/// Read one command from the connection, dispatch it, write the response, and return the
/// connection outcome.
///
/// For `crash`, this writes `OK`, half-closes the write side, drains reads until EOF (the
/// driver closes after observing `OK`), and returns `ConnectionOutcome::Crash`. The caller
/// is responsible for calling `process::exit`. The EOF read is the synchronisation event;
/// no timer participates in this handshake.
pub(crate) async fn process_request(
    stream: TcpStream,
    app: &Arc<BehaviorState>,
    runtime_tx: &mpsc::Sender<RuntimeCommand>,
) -> ConnectionOutcome {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).take(MAX_REQUEST_BYTES + 1);
    let mut buf = String::new();

    let read_result = tokio::time::timeout(READ_DEADLINE, reader.read_line(&mut buf)).await;

    let dispatch_result = match read_result {
        Err(_) => Err("read timeout".to_string()),
        Ok(Err(err)) => Err(format!("read error: {err}")),
        Ok(Ok(0)) => Err("empty request".to_string()),
        Ok(Ok(_)) => {
            if !buf.ends_with('\n') {
                Err("oversize".to_string())
            } else {
                dispatch(buf.trim_end_matches(&['\r', '\n'][..]), app, runtime_tx).await
            }
        }
    };

    match dispatch_result {
        Ok(DispatchOutcome::Ok) => {
            let _ = write_half.write_all(b"OK\n").await;
            let _ = write_half.flush().await;
            ConnectionOutcome::Done
        }
        Ok(DispatchOutcome::Crash) => {
            let _ = write_half.write_all(b"OK\n").await;
            let _ = write_half.flush().await;
            let _ = write_half.shutdown().await;
            // Drain reads until the driver closes its end. EOF is the synchronisation event
            // proving the driver received OK.
            let read_half = reader.into_inner().into_inner();
            drain_until_eof(read_half).await;
            ConnectionOutcome::Crash
        }
        Err(message) => {
            let payload = format!("ERR {message}\n");
            let _ = write_half.write_all(payload.as_bytes()).await;
            let _ = write_half.flush().await;
            ConnectionOutcome::Done
        }
    }
}

async fn drain_until_eof(mut read_half: tokio::net::tcp::OwnedReadHalf) {
    let mut sink = [0u8; 64];
    loop {
        match read_half.read(&mut sink).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DispatchOutcome {
    Ok,
    Crash,
}

pub(crate) async fn dispatch(
    line: &str,
    app: &Arc<BehaviorState>,
    runtime_tx: &mpsc::Sender<RuntimeCommand>,
) -> Result<DispatchOutcome, String> {
    let mut parts = line.split_whitespace();
    let cmd = parts.next().ok_or_else(|| "empty command".to_string())?;
    match cmd {
        "ready" => {
            if parts.next().is_some() {
                return Err("ready takes no arguments".to_string());
            }
            Ok(DispatchOutcome::Ok)
        }
        "set" => {
            let target = parts.next().ok_or_else(|| "missing target".to_string())?;
            if target != "app" {
                return Err(format!("unknown target {target}"));
            }
            let behavior_token = parts.next().ok_or_else(|| "missing behavior".to_string())?;
            let behavior = parse_behavior(behavior_token)
                .ok_or_else(|| format!("unknown behavior {behavior_token}"))?;
            app.set(behavior).await;
            if behavior == Behavior::Block {
                if let Some(duration_token) = parts.next() {
                    let duration_ms: u64 = duration_token
                        .parse()
                        .map_err(|_| format!("invalid duration {duration_token}"))?;
                    if parts.next().is_some() {
                        return Err("set takes at most three arguments".to_string());
                    }
                    if duration_ms > 0 {
                        let app_for_timer = Arc::clone(app);
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
                            app_for_timer.set(Behavior::Normal).await;
                        });
                    }
                }
            } else if parts.next().is_some() {
                return Err("set takes at most three arguments".to_string());
            }
            Ok(DispatchOutcome::Ok)
        }
        "unblock" => {
            let target = parts.next().ok_or_else(|| "missing target".to_string())?;
            if target != "app" {
                return Err(format!("unknown target {target}"));
            }
            if parts.next().is_some() {
                return Err("unblock takes no extra arguments".to_string());
            }
            app.set(Behavior::Normal).await;
            Ok(DispatchOutcome::Ok)
        }
        "shutdown" => {
            let mut downtime_ms = DEFAULT_SHUTDOWN_DOWNTIME_MS;
            if let Some(duration_token) = parts.next() {
                downtime_ms = duration_token
                    .parse()
                    .map_err(|_| format!("invalid duration {duration_token}"))?;
            }
            if parts.next().is_some() {
                return Err("shutdown takes at most one argument".to_string());
            }
            if downtime_ms < MIN_SHUTDOWN_DOWNTIME_MS {
                downtime_ms = MIN_SHUTDOWN_DOWNTIME_MS;
            }
            runtime_tx
                .send(RuntimeCommand::Shutdown {
                    downtime: Duration::from_millis(downtime_ms),
                })
                .await
                .map_err(|_| "runtime channel closed".to_string())?;
            Ok(DispatchOutcome::Ok)
        }
        "crash" => {
            if parts.next().is_some() {
                return Err("crash takes no arguments".to_string());
            }
            Ok(DispatchOutcome::Crash)
        }
        "reload-auth" => {
            if parts.next().is_some() {
                return Err("reload-auth takes no arguments".to_string());
            }
            runtime_tx
                .send(RuntimeCommand::ReloadAuth)
                .await
                .map_err(|_| "runtime channel closed".to_string())?;
            Ok(DispatchOutcome::Ok)
        }
        other => Err(format!("unknown command {other}")),
    }
}

#[cfg(test)]
mod tests;
