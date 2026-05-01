// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

mod oob_control;

use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use aurelia::{
    Aurelia, AureliaError, Domus, DomusAddr, DomusAuthConfig, DomusConfig, EncodedMessage, ErrorId,
    MessageCodec, Pkcs8AuthConfig, Pkcs8PemConfig, RouteResolver, SendOptions, Taberna,
    TabernaRequest,
};
use bytes::Bytes;
use tokio::sync::{mpsc, watch, Mutex, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

const MSG_PING: u32 = 100;
const MSG_PONG: u32 = 101;
const APP_BASE: u64 = 2000;
const DEFAULT_OOB_CONTROL_PORT: u16 = 5001;
pub(crate) const MIN_SHUTDOWN_DOWNTIME_MS: u64 = 2000;
pub(crate) const DEFAULT_SHUTDOWN_DOWNTIME_MS: u64 = 3000;

#[derive(Clone, Debug)]
struct WireMessage {
    msg_type: u32,
    payload: Bytes,
}

#[derive(Clone, Copy)]
struct WireCodec;

impl MessageCodec for WireCodec {
    type AppMessage = WireMessage;

    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
        Ok(EncodedMessage::new(msg.msg_type, msg.payload.clone()))
    }

    fn decode_app(&self, msg_type: u32, payload: &[u8]) -> Result<Self::AppMessage, AureliaError> {
        Ok(WireMessage {
            msg_type,
            payload: Bytes::copy_from_slice(payload),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Behavior {
    Normal,
    Reject,
    IngressFull,
    Block,
    Busy,
    DecodeFailure,
}

pub(crate) struct BehaviorState {
    behavior: Mutex<Behavior>,
    notify: Notify,
}

impl BehaviorState {
    pub(crate) fn new() -> Self {
        Self {
            behavior: Mutex::new(Behavior::Normal),
            notify: Notify::new(),
        }
    }

    pub(crate) async fn set(&self, behavior: Behavior) {
        let mut guard = self.behavior.lock().await;
        *guard = behavior;
        if behavior != Behavior::Block {
            self.notify.notify_waiters();
        }
    }

    pub(crate) async fn current(&self) -> Behavior {
        *self.behavior.lock().await
    }

    async fn wait_if_blocked(&self) {
        loop {
            if self.current().await != Behavior::Block {
                break;
            }
            self.notify.notified().await;
        }
    }
}

struct DomusState {
    app: Arc<BehaviorState>,
    domus: Arc<RwLock<Arc<Domus<DomusResolver>>>>,
    driver_taberna: u64,
}

async fn build_domus(
    aurelia: &Aurelia,
    primary_addr: SocketAddr,
    config: DomusConfig,
    resolver: Arc<DomusResolver>,
    auth: DomusAuthConfig,
) -> Result<Arc<Domus<DomusResolver>>, AureliaError> {
    let domus = aurelia
        .domus_builder(config, DomusAddr::Tcp(primary_addr), auth, resolver)
        .build()
        .await?;
    Ok(Arc::new(domus))
}

#[derive(Clone)]
struct DomusResolver {
    driver_taberna: u64,
    driver_primary: SocketAddr,
}

#[async_trait::async_trait]
impl RouteResolver for DomusResolver {
    async fn resolve(&self, taberna_id: u64) -> Result<DomusAddr, AureliaError> {
        if taberna_id == self.driver_taberna {
            Ok(DomusAddr::Tcp(self.driver_primary))
        } else {
            Err(AureliaError::new(ErrorId::UnknownTaberna))
        }
    }
}

#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    ReloadAuth,
    Shutdown { downtime: Duration },
}

struct TabernaTasks {
    shutdown: watch::Sender<bool>,
    handles: Vec<JoinHandle<()>>,
}

impl TabernaTasks {
    fn stop(&mut self) {
        let _ = self.shutdown.send(true);
        for handle in self.handles.drain(..) {
            handle.abort();
        }
    }
}

async fn handle_app_request(request: TabernaRequest<WireCodec>, state: Arc<DomusState>) {
    state.app.wait_if_blocked().await;
    if request.message.msg_type != MSG_PING {
        request
            .reject(AureliaError::new(ErrorId::RemoteTabernaRejected))
            .await;
        return;
    }
    match state.app.current().await {
        Behavior::Normal => {
            if let Some(id) = parse_id(&request.message.payload) {
                let response = WireMessage {
                    msg_type: MSG_PONG,
                    payload: Bytes::from(format!("pong:{}", id)),
                };
                let domus = state.domus.read().await.clone();
                let driver_taberna = state.driver_taberna;
                tokio::spawn(async move {
                    let _ = domus
                        .send(
                            &WireCodec,
                            driver_taberna,
                            &response,
                            SendOptions::MESSAGE_ONLY,
                        )
                        .await;
                });
            }
            let _ = request.accept().await;
        }
        Behavior::Reject => {
            request
                .reject(AureliaError::new(ErrorId::RemoteTabernaRejected))
                .await;
        }
        Behavior::IngressFull => {
            request
                .reject(AureliaError::new(ErrorId::LocalQueueFull))
                .await;
        }
        Behavior::Busy => {
            request
                .reject(AureliaError::new(ErrorId::TabernaBusy))
                .await;
        }
        Behavior::DecodeFailure => {
            request
                .reject(AureliaError::with_message(
                    ErrorId::DecodeFailure,
                    "decode-failure",
                ))
                .await;
        }
        Behavior::Block => {
            let _ = request.accept().await;
        }
    }
}

async fn run_app_loop(
    taberna: Taberna<WireCodec>,
    state: Arc<DomusState>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            _ = shutdown.changed() => {
                continue;
            }
            request = taberna.next(None) => {
                match request {
                    Ok(request) => {
                        handle_app_request(request, Arc::clone(&state)).await;
                    }
                    Err(err) if err.kind == ErrorId::ReceiveTimeout => {}
                    Err(err) if err.kind == ErrorId::DomusClosed => {
                        break;
                    }
                    Err(err) => {
                        eprintln!("app taberna receive failed: {err}");
                    }
                }
            }
        }
    }
}

async fn spawn_taberna_tasks(
    domus: Arc<Domus<DomusResolver>>,
    state: Arc<DomusState>,
    app_taberna: u64,
) -> Result<TabernaTasks, Box<dyn std::error::Error>> {
    let app = domus.taberna(app_taberna, WireCodec).await?;
    let (shutdown, shutdown_rx) = watch::channel(false);
    let app_handle = tokio::spawn(run_app_loop(app, state, shutdown_rx));
    Ok(TabernaTasks {
        shutdown,
        handles: vec![app_handle],
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let domus_index = env_u64("DOMUS_INDEX", 1);
    let app_taberna = env_u64("APP_TABERNA", APP_BASE + domus_index);
    let driver_taberna = env_u64("DRIVER_TABERNA", 9000);

    let primary_addr = env_socket("PRIMARY_ADDR");
    let driver_primary = env_socket("DRIVER_ADDR");
    let oob_port = env_u16("OOB_CONTROL_PORT", DEFAULT_OOB_CONTROL_PORT);
    let oob_addr = SocketAddr::new("0.0.0.0".parse().expect("ipv4 any"), oob_port);

    let ca_path = env::var("CA_CERT").expect("CA_CERT missing");
    let cert_path = env::var("DOMUS_CERT").expect("DOMUS_CERT missing");
    let key_path = env::var("DOMUS_KEY").expect("DOMUS_KEY missing");

    let auth = build_auth(&ca_path, &cert_path, &key_path);

    let mut config = DomusConfig::default();
    config.send_timeout = env_duration("SEND_TIMEOUT_MS", config.send_timeout);
    config.accept_timeout = env_duration("ACCEPT_TIMEOUT_MS", config.accept_timeout);
    config.listener_delay = env_duration("LISTENER_DELAY_MS", config.listener_delay);
    config.listener_reconnect_timeout = env_duration(
        "LISTENER_RECONNECT_TIMEOUT_MS",
        config.listener_reconnect_timeout,
    );
    config.keepalive_interval = env_duration("KEEPALIVE_MS", config.keepalive_interval);
    config.send_queue_size = env_usize("SEND_QUEUE_SIZE", config.send_queue_size);
    config.inflight_window = env_usize("INFLIGHT_WINDOW", config.inflight_window);

    let resolver = Arc::new(DomusResolver {
        driver_taberna,
        driver_primary,
    });
    let aurelia = Aurelia::init();
    let domus = build_domus(
        &aurelia,
        primary_addr,
        config.clone(),
        resolver.clone(),
        auth.clone(),
    )
    .await?;
    let domus_handle: Arc<RwLock<Arc<Domus<DomusResolver>>>> = Arc::new(RwLock::new(domus));

    let state = Arc::new(DomusState {
        app: Arc::new(BehaviorState::new()),
        domus: Arc::clone(&domus_handle),
        driver_taberna,
    });

    let (runtime_tx, mut runtime_rx) = mpsc::channel(8);
    let mut taberna_tasks = spawn_taberna_tasks(
        domus_handle.read().await.clone(),
        Arc::clone(&state),
        app_taberna,
    )
    .await?;

    // OOB control listener. Bind ordering is strict: the listener is spawned only after
    // build_domus has returned and the response/app tabernas have been registered. A
    // successful TCP connect on `oob_addr` is therefore a strict precondition that A1 is
    // fully initialised. The listener has its own shutdown channel scoped to the process
    // so it survives in-process restarts and only stops when the process is on its way out.
    let (oob_shutdown_tx, oob_shutdown_rx) = watch::channel(false);
    let oob_app = Arc::clone(&state.app);
    let oob_runtime_tx = runtime_tx.clone();
    tokio::spawn(async move {
        if let Err(err) =
            oob_control::serve(oob_addr, oob_app, oob_runtime_tx, oob_shutdown_rx).await
        {
            eprintln!("oob control listener exited: {err}");
        }
    });

    loop {
        tokio::select! {
            cmd = runtime_rx.recv() => {
                let Some(cmd) = cmd else { break; };
                match cmd {
                    RuntimeCommand::Shutdown { downtime } => {
                        let _ = oob_shutdown_tx.send(true);
                        taberna_tasks.stop();
                        let current = domus_handle.read().await.clone();
                        // Spawn the shutdown: graceful_close (which closes the listener)
                        // runs synchronously at the start; wait_for_callis_zero can take up
                        // to 2 * send_timeout. We don't want the downtime sleep to block on
                        // the drain — the listener is closed by the time we sleep, and the
                        // process is about to exit anyway.
                        tokio::spawn(async move {
                            current.shutdown().await;
                        });
                        tokio::time::sleep(downtime).await;
                        std::process::exit(2);
                    }
                    RuntimeCommand::ReloadAuth => {
                        let current = domus_handle.read().await.clone();
                        current
                            .reload_auth(build_auth(&ca_path, &cert_path, &key_path))
                            .await?;
                    }
                }
            }
        }
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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

fn env_duration(name: &str, default: std::time::Duration) -> std::time::Duration {
    match env::var(name) {
        Ok(value) => {
            let millis: u64 = value.parse().expect("invalid duration ms");
            std::time::Duration::from_millis(millis)
        }
        Err(_) => default,
    }
}

pub(crate) fn parse_behavior(value: &str) -> Option<Behavior> {
    match value {
        "normal" => Some(Behavior::Normal),
        "reject" => Some(Behavior::Reject),
        "ingress_full" => Some(Behavior::IngressFull),
        "block" => Some(Behavior::Block),
        "busy" => Some(Behavior::Busy),
        "decode_fail" => Some(Behavior::DecodeFailure),
        _ => None,
    }
}

fn parse_id(payload: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(payload);
    text.split(':').nth(1).map(|value| value.to_string())
}

fn build_auth(ca_path: &str, cert_path: &str, key_path: &str) -> DomusAuthConfig {
    let ca_pem = std::fs::read(ca_path).expect("read ca");
    let cert_pem = std::fs::read(cert_path).expect("read cert");
    let pkcs8_key_pem = std::fs::read(key_path).expect("read key");

    DomusAuthConfig::Pkcs8(Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
        ca_pem,
        cert_pem,
        pkcs8_key_pem,
    }))
}
