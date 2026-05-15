// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::process::Command;
use std::str::FromStr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tracing_subscriber::EnvFilter;

use aurelia::{
    a3_message_type, Aurelia, AureliaError, Domus, DomusAddr, DomusConfig, DomusConfigAccess,
    EncodedMessage, ErrorId, MessageCodec, MessageType, Pkcs8AuthConfig, Pkcs8PemConfig,
    SendOptions, SendOutcome, SimpleResolver, Taberna, TabernaRequest,
};

const MSG_PING: MessageType = a3_message_type(100);
const MSG_PONG: MessageType = a3_message_type(101);
const MSG_DECODE_FAIL: MessageType = a3_message_type(102);
const MSG_ENCODE_FAIL: MessageType = a3_message_type(103);
const APP_BASE: u64 = 2000;
const ACTIX_APP_OFFSET: u64 = 100;
const DRIVER_TABERNA: u64 = 9000;
const UNAVAILABLE_ENDPOINT: u64 = 9100;
const UNAVAILABLE_PORT_OFFSET: u16 = 999;
const WIRE_HEADER_LEN: usize = 32;
const DEFAULT_OOB_CONTROL_PORT: u16 = 5001;
const OOB_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const OOB_READ_TIMEOUT: Duration = Duration::from_secs(5);
const OOB_READY_POLL_INTERVAL: Duration = Duration::from_millis(200);
const RECONNECT_REPLAY_JOIN_TIMEOUT: Duration = Duration::from_secs(10);
const GRACEFUL_SHUTDOWN_DOWNTIME_MS: u64 = 8000;
const GRACEFUL_SHUTDOWN_PROBE_INTERVAL_MS: u64 = 500;
const GRACEFUL_SHUTDOWN_PROBE_ATTEMPTS: usize = 10;

#[derive(Clone, Debug)]
struct WireMessage {
    msg_type: MessageType,
    payload: Bytes,
}

#[derive(Clone, Copy)]
struct WireCodec;

impl MessageCodec for WireCodec {
    type AppMessage = WireMessage;

    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
        if msg.msg_type == MSG_ENCODE_FAIL {
            return Err(AureliaError::with_message(
                ErrorId::RemoteTabernaRejected,
                "requested encode failure",
            ));
        }
        Ok(EncodedMessage::new(msg.msg_type, msg.payload.clone()))
    }

    fn decode_app(
        &self,
        msg_type: MessageType,
        payload: &[u8],
    ) -> Result<Self::AppMessage, AureliaError> {
        Ok(WireMessage {
            msg_type,
            payload: Bytes::copy_from_slice(payload),
        })
    }
}

#[derive(Clone)]
struct DomusInfo {
    name: String,
    primary: SocketAddr,
    app_taberna: u64,
    actix_taberna: u64,
}

struct Response {
    msg_type: MessageType,
    payload: Bytes,
}

struct ResponseHub {
    waiters: tokio::sync::Mutex<HashMap<String, oneshot::Sender<Response>>>,
}

impl ResponseHub {
    fn new() -> Self {
        Self {
            waiters: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn register(&self, id: String) -> oneshot::Receiver<Response> {
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().await.insert(id, tx);
        rx
    }

    async fn unregister(&self, id: &str) {
        self.waiters.lock().await.remove(id);
    }

    async fn deliver(&self, response: Response) {
        if let Some(id) = parse_id(&response.payload) {
            if let Some(tx) = self.waiters.lock().await.remove(&id) {
                let _ = tx.send(response);
            }
        }
    }
}

struct ResponseSink {
    hub: Arc<ResponseHub>,
    expected_msg_types: Vec<u32>,
}

impl ResponseSink {
    async fn handle(&self, request: TabernaRequest<WireCodec>) {
        if !self.expected_msg_types.contains(&request.message.msg_type) {
            request.reject();
            return;
        }
        self.hub
            .deliver(Response {
                msg_type: request.message.msg_type,
                payload: request.message.payload.clone(),
            })
            .await;
        request.accept();
    }
}

async fn run_response_loop(taberna: Taberna<WireCodec>, sink: Arc<ResponseSink>) {
    loop {
        match taberna.next(None).await {
            Ok(request) => {
                sink.handle(request).await;
            }
            Err(err) if err.kind == ErrorId::ReceiveTimeout => {}
            Err(err) if err.kind == ErrorId::DomusClosed => {
                break;
            }
            Err(err) => {
                eprintln!("response taberna receive failed: {err}");
            }
        }
    }
}

struct DriverContext {
    config: DomusConfigAccess,
    domus: Arc<Domus<SimpleResolver>>,
    hub: Arc<ResponseHub>,
    domus_entries: Vec<DomusInfo>,
    connector: TlsConnector,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let ctx = DriverContext::new().await?;
    if let Err(err) = ctx.run().await {
        eprintln!("E2E FAILED: {err}");
        std::process::exit(1);
    }
    println!("E2E OK");
    Ok(())
}

impl DriverContext {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let primary_addr = env_socket("PRIMARY_ADDR");
        let primary_port = env_u16("PRIMARY_PORT", primary_addr.port());
        let domus_list = env::var("DOMUS_LIST").expect("DOMUS_LIST missing");

        let ca_path = env::var("CA_CERT").expect("CA_CERT missing");
        let cert_path = env::var("DOMUS_CERT").expect("DOMUS_CERT missing");
        let key_path = env::var("DOMUS_KEY").expect("DOMUS_KEY missing");

        let (auth, client) = build_auth_and_client(&ca_path, &cert_path, &key_path);
        let connector = TlsConnector::from(Arc::new(client));

        let config = DomusConfig {
            listener_delay: Duration::from_millis(0),
            listener_reconnect_timeout: Duration::from_secs(1),
            taberna_accept_queue_size: 16,
            ..DomusConfig::default()
        };

        let hub = Arc::new(ResponseHub::new());

        let mut domus_entries = Vec::new();
        for (idx, entry) in domus_list.split(',').enumerate() {
            let mut parts = entry.split('=');
            let name = parts.next().unwrap_or("domus").to_string();
            let ip: IpAddr = parts
                .next()
                .ok_or("missing domus ip")
                .and_then(|value| value.parse().map_err(|_| "invalid ip"))?;
            let index = (idx + 1) as u64;
            let primary = SocketAddr::new(ip, primary_port);
            domus_entries.push(DomusInfo {
                name,
                primary,
                app_taberna: APP_BASE + index,
                actix_taberna: APP_BASE + index + ACTIX_APP_OFFSET,
            });
        }

        let resolver = Arc::new(SimpleResolver::new());
        for domus in &domus_entries {
            resolver
                .insert(domus.app_taberna, DomusAddr::Tcp(domus.primary))
                .await;
            resolver
                .insert(domus.actix_taberna, DomusAddr::Tcp(domus.primary))
                .await;
        }
        if let Some(domus) = domus_entries.first() {
            let unavailable_port = primary_port.saturating_add(UNAVAILABLE_PORT_OFFSET);
            let unavailable = SocketAddr::new(domus.primary.ip(), unavailable_port);
            resolver
                .insert(UNAVAILABLE_ENDPOINT, DomusAddr::Tcp(unavailable))
                .await;
        }

        let aurelia = Aurelia::new();
        let domus = aurelia
            .domus_builder(config, DomusAddr::Tcp(primary_addr), auth, resolver)
            .build()
            .await?;
        let response_sink = Arc::new(ResponseSink {
            hub: Arc::clone(&hub),
            expected_msg_types: vec![MSG_PONG],
        });
        let response_taberna = domus.taberna(DRIVER_TABERNA, WireCodec).await?;
        tokio::spawn(run_response_loop(response_taberna, response_sink));
        let domus = Arc::new(domus);
        let config = domus.config();

        Ok(Self {
            config,
            domus,
            hub,
            domus_entries,
            connector,
        })
    }

    async fn run(&self) -> Result<(), String> {
        // Compose starts containers before the processes are necessarily ready to accept TLS traffic.
        // Preflight against peers other than the listener/originator scenario target so that the
        // initial domus-1 connection still exercises the first-send listener delay.
        self.wait_for_cluster_ready().await?;

        self.scenario_listener_originator()
            .await
            .map_err(|err| format!("listener-originator: {err}"))?;
        self.scenario_concurrent_primary_sends()
            .await
            .map_err(|err| format!("concurrent-primary-sends: {err}"))?;
        self.scenario_concurrent_blob_streams()
            .await
            .map_err(|err| format!("concurrent-blob-streams: {err}"))?;
        self.scenario_primary_while_blobs_active()
            .await
            .map_err(|err| format!("primary-while-blobs-active: {err}"))?;
        self.scenario_reconnect_replay()
            .await
            .map_err(|err| format!("reconnect-replay: {err}"))?;
        self.scenario_blob_reconnect_replay()
            .await
            .map_err(|err| format!("blob-reconnect-replay: {err}"))?;
        self.scenario_smooth_rotation()
            .await
            .map_err(|err| format!("smooth-rotation: {err}"))?;
        self.scenario_peer_restart()
            .await
            .map_err(|err| format!("peer-restart: {err}"))?;
        self.scenario_graceful_close()
            .await
            .map_err(|err| format!("graceful-close: {err}"))?;
        self.scenario_backpressure()
            .await
            .map_err(|err| format!("backpressure: {err}"))?;
        self.scenario_actix_taberna()
            .await
            .map_err(|err| format!("actix-taberna: {err}"))?;
        self.scenario_taberna_errors()
            .await
            .map_err(|err| format!("taberna-errors: {err}"))?;
        self.scenario_unknown_taberna()
            .await
            .map_err(|err| format!("unknown-taberna: {err}"))?;
        self.scenario_peer_unreachable()
            .await
            .map_err(|err| format!("peer-unreachable: {err}"))?;
        self.scenario_protocol_mismatch()
            .await
            .map_err(|err| format!("protocol-mismatch: {err}"))?;
        self.scenario_half_open_keepalive()
            .await
            .map_err(|err| format!("half-open-keepalive: {err}"))?;
        self.scenario_receive_timeout()
            .await
            .map_err(|err| format!("receive-timeout: {err}"))?;
        Ok(())
    }

    async fn wait_for_cluster_ready(&self) -> Result<(), String> {
        update_config(&self.config, |cfg| {
            cfg.listener_delay = Duration::from_millis(0);
            cfg.send_timeout = Duration::from_secs(10);
        })
        .await?;

        let targets = [self.domus(1)?.clone(), self.domus(2)?.clone()];
        for domus in targets {
            let mut last_error = None;
            for _ in 0..10 {
                match self
                    .send_ping(&domus, MSG_PING, MSG_PONG, "ready", "pong")
                    .await
                {
                    Ok(()) => {
                        last_error = None;
                        break;
                    }
                    Err(err) => {
                        last_error = Some(err);
                        // aurelia-test-allow-sleep: poll-interval; cluster readiness is probed
                        // through bounded A1 pings until all peers respond.
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
            if let Some(err) = last_error {
                return Err(format!("cluster-ready {}: {err}", domus.name));
            }
        }

        update_config(&self.config, |cfg| {
            *cfg = DomusConfig::default();
            cfg.listener_delay = Duration::from_millis(0);
            cfg.listener_reconnect_timeout = Duration::from_secs(1);
        })
        .await?;
        Ok(())
    }

    async fn scenario_listener_originator(&self) -> Result<(), String> {
        let domus = self.domus(0)?;
        update_config(&self.config, |cfg| {
            cfg.listener_delay = Duration::from_secs(1);
            cfg.send_timeout = Duration::from_secs(15);
        })
        .await?;
        let start = Instant::now();
        self.send_ping(domus, MSG_PING, MSG_PONG, "ping", "pong")
            .await?;
        let elapsed = start.elapsed();
        if elapsed < Duration::from_millis(900) {
            return Err("listener delay not observed".into());
        }
        update_config(&self.config, |cfg| {
            *cfg = DomusConfig::default();
            cfg.listener_delay = Duration::from_millis(0);
            cfg.listener_reconnect_timeout = Duration::from_secs(1);
        })
        .await?;
        Ok(())
    }

    async fn scenario_concurrent_primary_sends(&self) -> Result<(), String> {
        update_config(&self.config, |cfg| {
            cfg.listener_delay = Duration::from_millis(0);
            cfg.listener_reconnect_timeout = Duration::from_millis(500);
            cfg.send_timeout = Duration::from_millis(1500);
            cfg.accept_timeout = Duration::from_millis(250);
            cfg.taberna_accept_queue_size = 16;
        })
        .await?;
        let mut tasks = Vec::new();
        for (idx, domus) in self.domus_entries.iter().cloned().enumerate() {
            for n in 0..3 {
                let runtime = Arc::clone(&self.domus);
                let hub = Arc::clone(&self.hub);
                let domus = domus.clone();
                tasks.push(tokio::spawn(async move {
                    send_ping_with(
                        runtime,
                        hub,
                        domus,
                        MSG_PING,
                        MSG_PONG,
                        &format!("primary-{idx}-{n}"),
                        "pong",
                    )
                    .await
                }));
            }
        }
        for task in tasks {
            timeout(Duration::from_secs(2), task)
                .await
                .map_err(|_| "concurrent primary send timeout".to_string())?
                .map_err(|err| format!("join error: {err}"))??;
        }
        Ok(())
    }

    async fn scenario_concurrent_blob_streams(&self) -> Result<(), String> {
        let domus = self.domus(0)?.clone();
        update_config(&self.config, |cfg| {
            cfg.send_timeout = Duration::from_secs(2);
            cfg.accept_timeout = Duration::from_millis(250);
        })
        .await?;
        let mut tasks = Vec::new();
        for n in 0..3 {
            let runtime = Arc::clone(&self.domus);
            let hub = Arc::clone(&self.hub);
            let domus = domus.clone();
            let payload = Bytes::from(vec![n as u8; 64 * 1024]);
            tasks.push(tokio::spawn(async move {
                send_blob_ping_with(runtime, hub, domus, &format!("blob-{n}"), payload).await
            }));
        }
        for task in tasks {
            timeout(Duration::from_secs(3), task)
                .await
                .map_err(|_| "concurrent blob send timeout".to_string())?
                .map_err(|err| format!("join error: {err}"))??;
        }
        Ok(())
    }

    async fn scenario_primary_while_blobs_active(&self) -> Result<(), String> {
        let domus = self.domus(1)?.clone();
        update_config(&self.config, |cfg| {
            cfg.send_timeout = Duration::from_secs(2);
            cfg.accept_timeout = Duration::from_millis(250);
        })
        .await?;
        let mut blob_tasks = Vec::new();
        for n in 0..2 {
            let runtime = Arc::clone(&self.domus);
            let hub = Arc::clone(&self.hub);
            let domus = domus.clone();
            let payload = Bytes::from(vec![0xA0 + n as u8; 128 * 1024]);
            blob_tasks.push(tokio::spawn(async move {
                send_blob_ping_with(runtime, hub, domus, &format!("mixed-blob-{n}"), payload).await
            }));
        }
        self.send_ping(&domus, MSG_PING, MSG_PONG, "mixed-primary", "pong")
            .await?;
        for task in blob_tasks {
            timeout(Duration::from_secs(3), task)
                .await
                .map_err(|_| "mixed blob timeout".to_string())?
                .map_err(|err| format!("join error: {err}"))??;
        }
        Ok(())
    }

    async fn scenario_reconnect_replay(&self) -> Result<(), String> {
        let domus_info = self.domus(0)?;
        oob_control(domus_info, "set app block 1000").await?;
        let domus_runtime = Arc::clone(&self.domus);
        let hub = Arc::clone(&self.hub);
        let domus_clone = domus_info.clone();
        let send_task = tokio::spawn(async move {
            send_ping_with(
                domus_runtime,
                hub,
                domus_clone,
                MSG_PING,
                MSG_PONG,
                "ping",
                "pong",
            )
            .await
        });
        oob_control(domus_info, "wait app blocked 2000").await?;
        // Driver-side netem: apply 100% loss on the driver's eth0 for 400ms then auto-clear.
        // Driver-side avoids any chicken-and-egg with the OOB plane (which would otherwise
        // share the same partitioned interface as the system-under-test).
        apply_local_netem_temporary("partition", Duration::from_millis(400))?;
        let result = timeout(RECONNECT_REPLAY_JOIN_TIMEOUT, send_task)
            .await
            .map_err(|_| "replay timeout")?;
        let result = result.map_err(|err| format!("join error: {err}"))?;
        result?;
        Ok(())
    }

    async fn scenario_blob_reconnect_replay(&self) -> Result<(), String> {
        let domus = self.domus(0)?.clone();
        update_config(&self.config, |cfg| {
            cfg.send_timeout = Duration::from_secs(3);
            cfg.accept_timeout = Duration::from_millis(500);
            cfg.listener_reconnect_timeout = Duration::from_millis(500);
        })
        .await?;
        let runtime = Arc::clone(&self.domus);
        let hub = Arc::clone(&self.hub);
        oob_control(&domus, "arm app blob-started").await?;
        let wait_domus = domus.clone();
        let send_task = tokio::spawn(async move {
            let payload = Bytes::from(vec![0x5A; 512 * 1024]);
            send_blob_ping_with(runtime, hub, domus, "blob-reconnect", payload).await
        });
        oob_control(&wait_domus, "wait app blob-started 2000").await?;
        apply_local_netem_temporary("partition", Duration::from_millis(400))?;
        timeout(Duration::from_secs(4), send_task)
            .await
            .map_err(|_| "blob reconnect timeout".to_string())?
            .map_err(|err| format!("join error: {err}"))??;
        Ok(())
    }

    /// Peer dies and is restarted by Docker as a fresh process. The driver:
    ///   1. Confirms baseline: ping the peer, expect pong.
    ///   2. Crashes the peer via OOB (process exits).
    ///   3. Waits for the new peer instance to bind OOB (proof that A1 is up).
    ///   4. Confirms recovery: ping the peer, expect pong.
    ///
    /// No "Block mode" contrivance, no inflight-during-crash race. Pure
    /// recovery test on the cleanly-bracketed restart boundary.
    async fn scenario_peer_restart(&self) -> Result<(), String> {
        let domus_info = self.domus(1)?;

        // 1. baseline
        self.send_ping(domus_info, MSG_PING, MSG_PONG, "pre-restart", "pong")
            .await?;

        // 2. crash
        oob_control(domus_info, "crash").await?;

        // 3. wait for new instance
        wait_for_peer_ready_oob(domus_info, Instant::now() + Duration::from_secs(30)).await?;

        // 4. recovery. The first send may fail transiently because aurelia is still
        //    re-establishing the peer handle to the fresh session; retry briefly.
        let mut last_error = None;
        for _ in 0..6 {
            match self
                .send_ping(domus_info, MSG_PING, MSG_PONG, "post-restart", "pong")
                .await
            {
                Ok(()) => return Ok(()),
                Err(err) => {
                    last_error = Some(err);
                    // aurelia-test-allow-sleep: poll-interval; the restarted peer is observed by
                    // retrying bounded pings until the fresh session accepts traffic.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        Err(format!(
            "post-restart ping never succeeded: {}",
            last_error.unwrap_or_else(|| "unknown error".into())
        ))
    }

    async fn wait_for_restart(&self, domus_info: &DomusInfo) -> Result<(), String> {
        // Calibrate to loopback per docs/testing.md: send/accept must be short
        // enough that a stuck send fails the scenario quickly, not after the
        // 30s production default. The cold dial + TLS + handshake on the
        // freshly-restarted peer fits comfortably inside 2s on loopback.
        update_config(&self.config, |cfg| {
            cfg.send_timeout = Duration::from_secs(2);
            cfg.accept_timeout = Duration::from_secs(2);
            cfg.listener_reconnect_timeout = Duration::from_secs(1);
        })
        .await?;
        let mut errors: Vec<String> = Vec::new();
        for attempt in 0..6 {
            match self
                .send_ping(domus_info, MSG_PING, MSG_PONG, "restart", "pong")
                .await
            {
                Ok(()) => {
                    return Ok(());
                }
                Err(err) => {
                    errors.push(format!("attempt {}: {err}", attempt + 1));
                    // aurelia-test-allow-sleep: poll-interval; restart readiness is observed by
                    // retrying bounded pings against the peer.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        Err(format!("restart ping failed: [{}]", errors.join("; ")))
    }

    /// `reload_auth` is non-disruptive (smooth rotation): the existing primary callis stays up,
    /// sends straddling the reload all succeed without any reconnect or impaired window.
    async fn scenario_smooth_rotation(&self) -> Result<(), String> {
        let domus = self.domus(0)?;
        self.send_ping(domus, MSG_PING, MSG_PONG, "rotation", "pong")
            .await?;
        oob_control(domus, "set app block 300").await?;
        let runtime = Arc::clone(&self.domus);
        let hub = Arc::clone(&self.hub);
        let live_domus = domus.clone();
        let live_send = tokio::spawn(async move {
            send_ping_with(
                runtime,
                hub,
                live_domus,
                MSG_PING,
                MSG_PONG,
                "rotation-live",
                "pong",
            )
            .await
        });
        oob_control(domus, "wait app blocked 1000").await?;
        oob_control(domus, "reload-auth").await?;
        timeout(Duration::from_secs(2), live_send)
            .await
            .map_err(|_| "live rotation send timeout".to_string())?
            .map_err(|err| format!("join error: {err}"))??;
        // Immediately exercise the same callis after the rotation. No retry loop, no
        // partition: if the breaker had ever activated, this would time out.
        self.send_ping(domus, MSG_PING, MSG_PONG, "rotation", "pong")
            .await?;
        self.send_ping(domus, MSG_PING, MSG_PONG, "rotation", "pong")
            .await?;
        Ok(())
    }

    /// Peer is asked to shut down gracefully (OOB `shutdown <ms>`). During the downtime
    /// window, the listener is closed and the process eventually exits. Driver verifies:
    ///   1. During downtime, raw TCP connects to the peer primary port fail; this proves
    ///      the peer listener is unavailable without creating a second domus with the
    ///      driver's fixed certificate identity on an ephemeral callback port.
    ///   2. After Docker restarts the peer, OOB `ready` succeeds and a fresh ping works.
    async fn scenario_graceful_close(&self) -> Result<(), String> {
        let domus = self.domus(2)?;
        update_config(&self.config, |cfg| {
            cfg.send_timeout = Duration::from_millis(500);
            cfg.accept_timeout = Duration::from_millis(500);
        })
        .await?;
        oob_control(domus, "set app block").await?;
        let runtime = Arc::clone(&self.domus);
        let taberna = domus.app_taberna;
        let queued = tokio::spawn(async move {
            send_wire(
                &runtime,
                taberna,
                MSG_PING,
                Bytes::from_static(b"ping:queued-close"),
            )
            .await
        });
        oob_control(domus, "wait app blocked 1000").await?;
        oob_control(domus, &format!("shutdown {GRACEFUL_SHUTDOWN_DOWNTIME_MS}")).await?;
        let queued_result = timeout(Duration::from_secs(2), queued)
            .await
            .map_err(|_| "queued graceful-close send timeout".to_string())?
            .map_err(|err| format!("join error: {err}"))?;
        match queued_result {
            Ok(()) => return Err("queued graceful-close send unexpectedly succeeded".into()),
            Err(err)
                if matches!(
                    err.kind,
                    ErrorId::PeerUnavailable
                        | ErrorId::SendTimeout
                        | ErrorId::ConnectionLost
                        | ErrorId::RemoteTabernaRejected
                ) => {}
            Err(err) => return Err(format!("unexpected queued graceful-close error: {err}")),
        }
        let mut saw_unavailable = false;
        for attempt in 0..GRACEFUL_SHUTDOWN_PROBE_ATTEMPTS {
            match timeout(
                Duration::from_millis(500),
                tokio::net::TcpStream::connect(domus.primary),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => {
                    saw_unavailable = true;
                    break;
                }
            }
            if attempt + 1 < GRACEFUL_SHUTDOWN_PROBE_ATTEMPTS {
                // aurelia-test-allow-sleep: poll-interval; each probe actively attempts a TCP
                // connection and the sleep is only the bounded interval between probes.
                tokio::time::sleep(Duration::from_millis(GRACEFUL_SHUTDOWN_PROBE_INTERVAL_MS))
                    .await;
            }
        }
        if !saw_unavailable {
            update_config(&self.config, |cfg| {
                cfg.send_timeout = DomusConfig::default().send_timeout;
                cfg.accept_timeout = DomusConfig::default().accept_timeout;
                cfg.listener_reconnect_timeout = Duration::from_secs(1);
            })
            .await?;
            return Err("probes never observed unavailability during shutdown window".into());
        }
        wait_for_peer_ready_oob(domus, Instant::now() + Duration::from_secs(30)).await?;
        let result = self.wait_for_restart(domus).await;
        // Restore both send_timeout and accept_timeout in one update so the
        // accept_timeout <= send_timeout invariant holds at all times.
        update_config(&self.config, |cfg| {
            cfg.send_timeout = DomusConfig::default().send_timeout;
            cfg.accept_timeout = DomusConfig::default().accept_timeout;
        })
        .await?;
        result
    }

    async fn scenario_backpressure(&self) -> Result<(), String> {
        let domus_info = self.domus(0)?;
        oob_control(domus_info, "set app block").await?;
        update_config(&self.config, |cfg| {
            cfg.send_queue_size = 1;
            cfg.send_timeout = Duration::from_millis(500);
            cfg.accept_timeout = Duration::from_millis(500);
        })
        .await?;
        let domus_runtime = Arc::clone(&self.domus);
        let taberna = domus_info.app_taberna;
        let first = tokio::spawn(async move {
            send_wire(
                &domus_runtime,
                taberna,
                MSG_PING,
                Bytes::from_static(b"ping:queue"),
            )
            .await
        });
        oob_control(domus_info, "wait app blocked 1000").await?;
        let err = send_wire(
            &self.domus,
            domus_info.app_taberna,
            MSG_PING,
            Bytes::from_static(b"ping:queue"),
        )
        .await
        .expect_err("expected local queue full");
        if err.kind != ErrorId::LocalQueueFull {
            return Err(format!("expected local queue full, got {:?}", err.kind));
        }
        oob_control(domus_info, "unblock app").await?;
        let _ = timeout(Duration::from_secs(2), first).await;

        update_config(&self.config, |cfg| {
            cfg.send_queue_size = 2;
            cfg.send_timeout = Duration::from_millis(500);
            cfg.accept_timeout = Duration::from_millis(500);
        })
        .await?;
        oob_control(domus_info, "set app block").await?;
        let domus_runtime = Arc::clone(&self.domus);
        let taberna = domus_info.app_taberna;
        let first = tokio::spawn(async move {
            send_wire(
                &domus_runtime,
                taberna,
                MSG_PING,
                Bytes::from_static(b"ping:inflight"),
            )
            .await
        });
        oob_control(domus_info, "wait app blocked 1000").await?;
        let err = send_wire(
            &self.domus,
            domus_info.app_taberna,
            MSG_PING,
            Bytes::from_static(b"ping:inflight"),
        )
        .await
        .expect_err("expected inflight timeout");
        if err.kind != ErrorId::SendTimeout {
            return Err("expected inflight timeout".into());
        }
        oob_control(domus_info, "unblock app").await?;
        let _ = timeout(Duration::from_secs(2), first).await;

        update_config(&self.config, |cfg| {
            *cfg = DomusConfig::default();
            cfg.listener_delay = Duration::from_millis(0);
            cfg.listener_reconnect_timeout = Duration::from_secs(1);
        })
        .await?;
        Ok(())
    }

    async fn scenario_actix_taberna(&self) -> Result<(), String> {
        let domus = self.domus(0)?;
        update_config(&self.config, |cfg| {
            cfg.send_timeout = Duration::from_millis(1000);
            cfg.accept_timeout = Duration::from_millis(500);
            cfg.send_queue_size = 16;
        })
        .await?;

        send_ping_to_taberna_with(
            Arc::clone(&self.domus),
            Arc::clone(&self.hub),
            domus.actix_taberna,
            MSG_PING,
            MSG_PONG,
            "actix",
            "pong",
        )
        .await?;

        oob_control(domus, "set actix block").await?;
        send_wire(
            &self.domus,
            domus.actix_taberna,
            MSG_PING,
            Bytes::from_static(b"ping:actix-held"),
        )
        .await
        .map_err(|err| format!("first actix blocked send failed: {err}"))?;
        oob_control(domus, "wait actix blocked 1000").await?;

        let mut saw_busy = false;
        // Actix can hold the active handler plus one queued mailbox item; busy is expected
        // once both slots are occupied.
        for n in 0..4 {
            match send_wire(
                &self.domus,
                domus.actix_taberna,
                MSG_PING,
                Bytes::from(format!("ping:actix-full-{n}")),
            )
            .await
            {
                Ok(()) => {}
                Err(err) if err.kind == ErrorId::TabernaBusy => {
                    saw_busy = true;
                    break;
                }
                Err(err) => return Err(format!("unexpected actix full error: {err}")),
            }
        }
        if !saw_busy {
            return Err("actix mailbox-full scenario did not observe taberna-busy".into());
        }

        oob_control(domus, "unblock actix").await?;
        send_ping_to_taberna_with(
            Arc::clone(&self.domus),
            Arc::clone(&self.hub),
            domus.actix_taberna,
            MSG_PING,
            MSG_PONG,
            "actix-after-full",
            "pong",
        )
        .await?;

        oob_control(domus, "stop actix").await?;
        let err = send_wire(
            &self.domus,
            domus.actix_taberna,
            MSG_PING,
            Bytes::from_static(b"ping:actix-stopped"),
        )
        .await
        .expect_err("expected actix taberna shutdown");
        if err.kind != ErrorId::TabernaShutdown {
            return Err(format!("expected taberna-shutdown, got {err:?}"));
        }

        update_config(&self.config, |cfg| {
            *cfg = DomusConfig::default();
            cfg.listener_delay = Duration::from_millis(0);
            cfg.listener_reconnect_timeout = Duration::from_secs(1);
        })
        .await?;
        Ok(())
    }

    async fn scenario_taberna_errors(&self) -> Result<(), String> {
        let domus = self.domus(1)?;
        oob_control(domus, "set app reject").await?;
        expect_error(
            &self.domus,
            domus.app_taberna,
            ErrorId::RemoteTabernaRejected,
        )
        .await?;

        oob_control(domus, "set app normal").await?;
        expect_error_with_msg_type(
            &self.domus,
            domus.app_taberna,
            MSG_DECODE_FAIL,
            ErrorId::DecodeFailure,
        )
        .await?;

        expect_error_with_msg_type(
            &self.domus,
            domus.app_taberna,
            MSG_ENCODE_FAIL,
            ErrorId::EncodeFailure,
        )
        .await?;

        oob_control(domus, "set app normal").await?;
        Ok(())
    }

    async fn scenario_unknown_taberna(&self) -> Result<(), String> {
        let err = send_wire(
            &self.domus,
            9999,
            MSG_PING,
            Bytes::from_static(b"ping:unknown"),
        )
        .await
        .expect_err("expected unknown taberna");
        if err.kind != ErrorId::UnknownTaberna {
            return Err("expected UnknownTaberna".into());
        }
        Ok(())
    }

    async fn scenario_peer_unreachable(&self) -> Result<(), String> {
        let err = send_wire(
            &self.domus,
            UNAVAILABLE_ENDPOINT,
            MSG_PING,
            Bytes::from_static(b"ping:unavailable"),
        )
        .await
        .expect_err("expected send timeout");
        if err.kind != ErrorId::SendTimeout {
            return Err("expected SendTimeout".into());
        }
        Ok(())
    }

    async fn scenario_protocol_mismatch(&self) -> Result<(), String> {
        let domus = self.domus(0)?;
        let mut raw_header = [0u8; WIRE_HEADER_LEN];
        raw_header[0..2].copy_from_slice(&99u16.to_be_bytes());
        raw_header[2..4].copy_from_slice(&0u16.to_be_bytes());
        raw_header[4..8].copy_from_slice(&MSG_PING.to_be_bytes());

        let stream = self
            .connector
            .connect(
                tokio_rustls::rustls::pki_types::ServerName::IpAddress(domus.primary.ip().into()),
                tokio::net::TcpStream::connect(domus.primary)
                    .await
                    .map_err(|_| "tcp connect failed")?,
            )
            .await
            .map_err(|_| "tls connect failed")?;

        let (mut reader, mut writer) = tokio::io::split(stream);
        tokio::io::AsyncWriteExt::write_all(&mut writer, &raw_header)
            .await
            .map_err(|_| "write failed")?;
        tokio::io::AsyncWriteExt::flush(&mut writer)
            .await
            .map_err(|_| "flush failed")?;

        let mut buf = [0u8; 1];
        let result = timeout(
            Duration::from_secs(1),
            tokio::io::AsyncReadExt::read(&mut reader, &mut buf),
        )
        .await;
        match result {
            Ok(Ok(0)) => Ok(()),
            Ok(Ok(_)) => Err("unexpected data for invalid protocol".into()),
            Ok(Err(_)) => Ok(()),
            Err(_) => Ok(()),
        }
    }

    async fn scenario_half_open_keepalive(&self) -> Result<(), String> {
        let domus = self.domus(0)?;
        apply_local_netem_temporary("partition", Duration::from_millis(1500))?;
        update_config(&self.config, |cfg| {
            cfg.keepalive_interval = Duration::from_millis(200);
            cfg.send_timeout = Duration::from_millis(500);
            cfg.accept_timeout = Duration::from_millis(500);
        })
        .await?;
        let err = send_wire(
            &self.domus,
            domus.app_taberna,
            MSG_PING,
            Bytes::from_static(b"ping:half-open"),
        )
        .await
        .expect_err("expected half-open timeout");
        if err.kind != ErrorId::SendTimeout {
            return Err("expected SendTimeout for half-open".into());
        }
        update_config(&self.config, |cfg| {
            cfg.keepalive_interval = Duration::from_secs(15);
            cfg.send_timeout = Duration::from_secs(30);
            cfg.accept_timeout = Duration::from_secs(5);
        })
        .await?;
        Ok(())
    }

    async fn scenario_receive_timeout(&self) -> Result<(), String> {
        let taberna_id = DRIVER_TABERNA.saturating_add(1);
        let taberna = self
            .domus
            .taberna(taberna_id, WireCodec)
            .await
            .map_err(|err| format!("taberna open failed: {err}"))?;

        let err = match taberna.next(Some(Duration::from_millis(50))).await {
            Ok(_) => return Err("expected receive-timeout, got message".into()),
            Err(err) => err,
        };
        if err.kind != ErrorId::ReceiveTimeout {
            return Err(format!(
                "expected receive-timeout, got {}",
                err.kind.as_u32()
            ));
        }

        self.domus.shutdown().await;
        let err = match taberna.next(Some(Duration::from_millis(50))).await {
            Ok(_) => return Err("expected domus-closed, got message".into()),
            Err(err) => err,
        };
        if err.kind != ErrorId::DomusClosed {
            return Err(format!("expected domus-closed, got {}", err.kind.as_u32()));
        }
        Ok(())
    }

    async fn send_ping(
        &self,
        domus: &DomusInfo,
        msg_type: u32,
        expected_type: u32,
        prefix: &str,
        response_prefix: &str,
    ) -> Result<(), String> {
        send_ping_with(
            Arc::clone(&self.domus),
            Arc::clone(&self.hub),
            domus.clone(),
            msg_type,
            expected_type,
            prefix,
            response_prefix,
        )
        .await
    }

    fn domus(&self, index: usize) -> Result<&DomusInfo, String> {
        self.domus_entries.get(index).ok_or("missing domus".into())
    }
}

async fn send_wire(
    domus_runtime: &Arc<Domus<SimpleResolver>>,
    taberna_id: u64,
    msg_type: u32,
    payload: Bytes,
) -> Result<(), AureliaError> {
    let message = WireMessage { msg_type, payload };
    let outcome = domus_runtime
        .send(&WireCodec, taberna_id, &message, SendOptions::MESSAGE_ONLY)
        .await?;
    match outcome {
        SendOutcome::MessageOnly => Ok(()),
        SendOutcome::Blob { .. } => Err(AureliaError::new(ErrorId::ProtocolViolation)),
    }
}

async fn send_ping_with(
    domus_runtime: Arc<Domus<SimpleResolver>>,
    hub: Arc<ResponseHub>,
    domus: DomusInfo,
    msg_type: u32,
    expected_type: u32,
    prefix: &str,
    response_prefix: &str,
) -> Result<(), String> {
    send_ping_to_taberna_with(
        domus_runtime,
        hub,
        domus.app_taberna,
        msg_type,
        expected_type,
        prefix,
        response_prefix,
    )
    .await
}

async fn send_ping_to_taberna_with(
    domus_runtime: Arc<Domus<SimpleResolver>>,
    hub: Arc<ResponseHub>,
    taberna_id: u64,
    msg_type: u32,
    expected_type: u32,
    prefix: &str,
    response_prefix: &str,
) -> Result<(), String> {
    let id = next_id();
    let rx = hub.register(id.clone()).await;
    let payload = Bytes::from(format!("{}:{}", prefix, id));
    let send_result = send_wire(&domus_runtime, taberna_id, msg_type, payload).await;
    if let Err(err) = send_result {
        return Err(format!("send failed: {err}"));
    }
    let response = match timeout(Duration::from_secs(5), rx).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => return Err("response channel closed".into()),
        Err(_) => {
            hub.unregister(&id).await;
            return Err("response timeout".into());
        }
    };
    if response.msg_type != expected_type {
        return Err("unexpected response type".into());
    }
    let expected = format!("{}:{}", response_prefix, id);
    if response.payload.as_ref() != expected.as_bytes() {
        return Err("unexpected response payload".into());
    }
    Ok(())
}

async fn send_blob_ping_with(
    domus_runtime: Arc<Domus<SimpleResolver>>,
    hub: Arc<ResponseHub>,
    domus: DomusInfo,
    prefix: &str,
    blob_payload: Bytes,
) -> Result<(), String> {
    let id = next_id();
    let expected_len = blob_payload.len();
    let rx = hub.register(id.clone()).await;
    let message = WireMessage {
        msg_type: MSG_PING,
        payload: Bytes::from(format!("{prefix}:{id}")),
    };
    let outcome = domus_runtime
        .send(&WireCodec, domus.app_taberna, &message, SendOptions::BLOB)
        .await
        .map_err(|err| format!("blob request failed: {err}"))?;
    let SendOutcome::Blob { mut sender } = outcome else {
        return Err("expected blob sender".into());
    };
    tokio::io::AsyncWriteExt::write_all(&mut sender, &blob_payload)
        .await
        .map_err(|err| format!("blob write failed: {err}"))?;
    tokio::io::AsyncWriteExt::shutdown(&mut sender)
        .await
        .map_err(|err| format!("blob shutdown failed: {err}"))?;
    let response = match timeout(Duration::from_secs(3), rx).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => return Err("blob response channel closed".into()),
        Err(_) => {
            hub.unregister(&id).await;
            return Err("blob response timeout".into());
        }
    };
    if response.msg_type != MSG_PONG {
        return Err("unexpected blob response type".into());
    }
    let expected = format!("blob-pong:{id}:{expected_len}");
    if response.payload.as_ref() != expected.as_bytes() {
        return Err(format!(
            "unexpected blob response payload: {:?}",
            String::from_utf8_lossy(&response.payload)
        ));
    }
    Ok(())
}

fn next_id() -> String {
    static COUNTER: StdMutex<u64> = StdMutex::new(0);
    let mut guard = COUNTER.lock().expect("counter lock");
    *guard += 1;
    guard.to_string()
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

async fn update_config<F>(config: &DomusConfigAccess, update: F) -> Result<(), String>
where
    F: FnOnce(&mut DomusConfig),
{
    let mut next = config.snapshot().await;
    update(&mut next);
    if next.callis_connect_timeout > next.send_timeout {
        next.callis_connect_timeout = next.send_timeout;
    }
    config
        .update(next)
        .await
        .map(|_| ())
        .map_err(|err| format!("config update failed: {err}"))
}

async fn expect_error(
    domus: &Arc<Domus<SimpleResolver>>,
    taberna_id: u64,
    kind: ErrorId,
) -> Result<(), String> {
    expect_error_with_msg_type(domus, taberna_id, MSG_PING, kind).await
}

async fn expect_error_with_msg_type(
    domus: &Arc<Domus<SimpleResolver>>,
    taberna_id: u64,
    msg_type: u32,
    kind: ErrorId,
) -> Result<(), String> {
    let err = send_wire(
        domus,
        taberna_id,
        msg_type,
        Bytes::from_static(b"ping:error"),
    )
    .await
    .expect_err("expected error");
    if err.kind != kind {
        return Err(format!("expected {kind:?}, got {err:?}"));
    }
    Ok(())
}

fn env_u16(name: &str, default: u16) -> u16 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_socket(name: &str) -> SocketAddr {
    SocketAddr::from_str(&env::var(name).unwrap_or_else(|_| panic!("{name} missing")))
        .expect("invalid socket addr")
}

fn parse_id(payload: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(payload);
    text.split(':').nth(1).map(|value| value.to_string())
}

fn oob_control_port() -> u16 {
    env::var("OOB_CONTROL_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_OOB_CONTROL_PORT)
}

async fn oob_control(domus_info: &DomusInfo, command: &str) -> Result<(), String> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let addr = SocketAddr::new(domus_info.primary.ip(), oob_control_port());
    let mut stream = timeout(OOB_CONNECT_TIMEOUT, tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| "oob control connect timeout".to_string())?
        .map_err(|err| format!("oob control connect: {err}"))?;
    stream
        .write_all(format!("{command}\n").as_bytes())
        .await
        .map_err(|err| format!("oob control write: {err}"))?;
    let mut buf = String::new();
    timeout(
        OOB_READ_TIMEOUT,
        tokio::io::BufReader::new(&mut stream).read_line(&mut buf),
    )
    .await
    .map_err(|_| "oob control response timeout".to_string())?
    .map_err(|err| format!("oob control read: {err}"))?;
    let trimmed = buf.trim_end_matches(&['\r', '\n'][..]);
    if trimmed == "OK" {
        Ok(())
    } else if let Some(rest) = trimmed.strip_prefix("ERR ") {
        Err(rest.to_string())
    } else {
        Err(format!("oob control malformed response: {trimmed}"))
    }
}

async fn wait_for_peer_ready_oob(domus_info: &DomusInfo, deadline: Instant) -> Result<(), String> {
    loop {
        if oob_control(domus_info, "ready").await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "peer {} never became ready via OOB",
                domus_info.name
            ));
        }
        // Polling cadence, not synchronisation delay: there is no peer-side event the driver
        // can subscribe to before the peer's listener is bound.
        // aurelia-test-allow-sleep: poll-interval; OOB readiness is observed through bounded
        // ready commands until the listener accepts control traffic.
        tokio::time::sleep(OOB_READY_POLL_INTERVAL).await;
    }
}

fn apply_local_netem_temporary(profile: &str, duration: Duration) -> Result<(), String> {
    apply_local_netem(profile)?;
    tokio::spawn(async move {
        // aurelia-test-allow-sleep: behavior-duration; this task holds the requested local netem
        // profile duration before clearing it.
        tokio::time::sleep(duration).await;
        let _ = clear_local_netem();
    });
    Ok(())
}

fn apply_local_netem(profile: &str) -> Result<(), String> {
    let args: &[&str] = match profile {
        "partition" => &[
            "qdisc", "replace", "dev", "eth0", "root", "netem", "loss", "100%",
        ],
        _ => return Err(format!("unknown netem profile: {profile}")),
    };
    run_local_tc(args)
}

fn clear_local_netem() -> Result<(), String> {
    run_local_tc(&["qdisc", "del", "dev", "eth0", "root"])
}

fn run_local_tc(args: &[&str]) -> Result<(), String> {
    let status = Command::new("tc")
        .args(args)
        .status()
        .map_err(|err| format!("tc {:?} failed: {err}", args))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("tc {:?} exited with {status}", args))
    }
}

fn build_auth_and_client(
    ca_path: &str,
    cert_path: &str,
    key_path: &str,
) -> (Pkcs8AuthConfig, ClientConfig) {
    let ca_pem = std::fs::read(ca_path).expect("read ca");
    let cert_pem = std::fs::read(cert_path).expect("read cert");
    let pkcs8_key_pem = std::fs::read(key_path).expect("read key");

    let auth = Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
        ca_pem,
        cert_pem,
        pkcs8_key_pem: pkcs8_key_pem.into(),
    });

    let ca_cert = load_certs(ca_path);
    let domus_cert = load_certs(cert_path);
    let domus_key = load_key(key_path);

    let mut roots = RootCertStore::empty();
    roots.add(ca_cert[0].clone()).expect("add ca");

    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(domus_cert, domus_key.clone_key())
        .expect("client config");

    (auth, client_config)
}

fn load_certs(path: &str) -> Vec<CertificateDer<'static>> {
    let pem = std::fs::read(path).expect("cert file");
    CertificateDer::pem_slice_iter(&pem)
        .map(|result| result.expect("cert parse"))
        .collect()
}

fn load_key(path: &str) -> PrivateKeyDer<'static> {
    let pem = std::fs::read(path).expect("key file");
    let keys: Vec<PrivatePkcs8KeyDer<'static>> = PrivatePkcs8KeyDer::pem_slice_iter(&pem)
        .map(|result| result.expect("key parse"))
        .collect();
    let key = keys.into_iter().next().expect("missing private key");
    PrivateKeyDer::from(key)
}
