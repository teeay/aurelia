// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

mod oob_control;

use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix::prelude::{
    Actor, ActorContext, ActorFutureExt, Context, ContextFutureSpawner, Handler, WrapFuture,
};
use aurelia::{
    ActixTaberna, ActixTabernaDelivery, Aurelia, AureliaError, Domus, DomusAddr, DomusConfig,
    EncodedMessage, ErrorId, MessageCodec, MessageType, Pkcs8AuthConfig, Pkcs8PemConfig,
    RouteResolver, SendOptions, Taberna, TabernaRequest,
};
use bytes::Bytes;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, watch, Mutex, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

use aurelia::a3_message_type;

const MSG_PING: MessageType = a3_message_type(100);
const MSG_PONG: MessageType = a3_message_type(101);
const MSG_DECODE_FAIL: MessageType = a3_message_type(102);
const APP_BASE: u64 = 2000;
const ACTIX_APP_OFFSET: u64 = 100;
const DEFAULT_OOB_CONTROL_PORT: u16 = 5001;
pub(crate) const MIN_SHUTDOWN_DOWNTIME_MS: u64 = 2000;
pub(crate) const DEFAULT_SHUTDOWN_DOWNTIME_MS: u64 = 3000;

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
        Ok(EncodedMessage::new(msg.msg_type, msg.payload.clone()))
    }

    fn decode_app(
        &self,
        msg_type: MessageType,
        payload: &[u8],
    ) -> Result<Self::AppMessage, AureliaError> {
        if msg_type == MSG_DECODE_FAIL {
            return Err(AureliaError::with_message(
                ErrorId::DecodeFailure,
                "requested decode failure",
            ));
        }
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
    Block,
}

fn encode_behavior(behavior: Behavior) -> u8 {
    match behavior {
        Behavior::Normal => 0,
        Behavior::Reject => 1,
        Behavior::Block => 2,
    }
}

fn decode_behavior(value: u8) -> Behavior {
    match value {
        1 => Behavior::Reject,
        2 => Behavior::Block,
        _ => Behavior::Normal,
    }
}

pub(crate) struct BehaviorState {
    inner: Mutex<BehaviorInner>,
    notify: Notify,
    blocked_tx: watch::Sender<u64>,
    blob_started_tx: watch::Sender<u64>,
    armed_blob_target: AtomicU64,
}

struct BehaviorInner {
    behavior: Behavior,
    block_generation: u64,
}

impl BehaviorState {
    pub(crate) fn new() -> Self {
        let (blocked_tx, _) = watch::channel(0);
        let (blob_started_tx, _) = watch::channel(0);
        Self {
            inner: Mutex::new(BehaviorInner {
                behavior: Behavior::Normal,
                block_generation: 0,
            }),
            notify: Notify::new(),
            blocked_tx,
            blob_started_tx,
            armed_blob_target: AtomicU64::new(0),
        }
    }

    pub(crate) async fn set(&self, behavior: Behavior) {
        let mut guard = self.inner.lock().await;
        guard.behavior = behavior;
        if behavior == Behavior::Block {
            guard.block_generation = guard.block_generation.saturating_add(1);
        }
        if behavior != Behavior::Block {
            self.notify.notify_waiters();
        }
    }

    pub(crate) async fn current(&self) -> Behavior {
        self.inner.lock().await.behavior
    }

    pub(crate) async fn wait_blocked(&self, timeout_duration: Duration) -> Result<(), String> {
        let target = {
            let guard = self.inner.lock().await;
            if guard.behavior != Behavior::Block {
                return Err("app is not in block behavior".to_string());
            }
            guard.block_generation
        };
        wait_for_watch_at_least(
            self.blocked_tx.subscribe(),
            target,
            timeout_duration,
            "app blocked",
        )
        .await
    }

    pub(crate) fn arm_blob_started(&self) {
        let target = (*self.blob_started_tx.borrow()).saturating_add(1);
        self.armed_blob_target.store(target, Ordering::SeqCst);
    }

    pub(crate) async fn wait_blob_started(&self, timeout_duration: Duration) -> Result<(), String> {
        let target = self.armed_blob_target.load(Ordering::SeqCst);
        if target == 0 {
            return Err("app blob-started wait is not armed".to_string());
        }
        wait_for_watch_at_least(
            self.blob_started_tx.subscribe(),
            target,
            timeout_duration,
            "app blob-started",
        )
        .await
    }

    async fn wait_if_blocked(&self) {
        let mut reported_generation = None;
        loop {
            let generation = {
                let guard = self.inner.lock().await;
                if guard.behavior != Behavior::Block {
                    None
                } else {
                    Some(guard.block_generation)
                }
            };
            let Some(generation) = generation else {
                break;
            };
            if reported_generation != Some(generation) {
                self.report_blocked_generation(generation);
                reported_generation = Some(generation);
            }
            self.notify.notified().await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn report_blocked(&self) -> Result<(), String> {
        let generation = {
            let guard = self.inner.lock().await;
            if guard.behavior != Behavior::Block {
                return Err("app is not in block behavior".to_string());
            }
            guard.block_generation
        };
        self.report_blocked_generation(generation);
        Ok(())
    }

    fn report_blocked_generation(&self, generation: u64) {
        self.blocked_tx.send_replace(generation);
    }

    pub(crate) fn report_blob_started(&self) {
        let current = (*self.blob_started_tx.borrow()).saturating_add(1);
        self.blob_started_tx.send_replace(current);
    }
}

pub(crate) struct ActixControlState {
    behavior: AtomicU8,
    block_generation: AtomicU64,
    blocked_tx: watch::Sender<u64>,
    unblock_version: AtomicU64,
    unblock_tx: watch::Sender<u64>,
    stopped_version: AtomicU64,
    stopped_tx: watch::Sender<u64>,
    actor: Mutex<Option<actix::Addr<ActixAppActor>>>,
}

impl ActixControlState {
    pub(crate) fn new() -> Self {
        let (blocked_tx, _) = watch::channel(0);
        let (unblock_tx, _) = watch::channel(0);
        let (stopped_tx, _) = watch::channel(0);
        Self {
            behavior: AtomicU8::new(encode_behavior(Behavior::Normal)),
            block_generation: AtomicU64::new(0),
            blocked_tx,
            unblock_version: AtomicU64::new(0),
            unblock_tx,
            stopped_version: AtomicU64::new(0),
            stopped_tx,
            actor: Mutex::new(None),
        }
    }

    pub(crate) async fn set(&self, behavior: Behavior) {
        self.behavior
            .store(encode_behavior(behavior), Ordering::SeqCst);
        if behavior == Behavior::Block {
            self.block_generation.fetch_add(1, Ordering::SeqCst);
        }
        if behavior != Behavior::Block {
            self.notify_unblocked();
        }
    }

    pub(crate) fn current(&self) -> Behavior {
        decode_behavior(self.behavior.load(Ordering::SeqCst))
    }

    pub(crate) async fn set_actor(&self, actor: actix::Addr<ActixAppActor>) {
        *self.actor.lock().await = Some(actor);
    }

    pub(crate) async fn stop_actor(&self) -> Result<(), String> {
        let before = self.stopped_version.load(Ordering::SeqCst);
        let mut stopped_rx = self.stopped_tx.subscribe();
        let actor = self.actor.lock().await.clone();
        let Some(actor) = actor else {
            return Err("actix actor missing".to_string());
        };
        actor.do_send(StopActixActor);
        wait_for_watch_advance(&mut stopped_rx, before, "actix stopped").await
    }

    pub(crate) async fn wait_blocked(&self, timeout_duration: Duration) -> Result<(), String> {
        if self.current() != Behavior::Block {
            return Err("actix is not in block behavior".to_string());
        }
        let target = self.block_generation.load(Ordering::SeqCst);
        wait_for_watch_at_least(
            self.blocked_tx.subscribe(),
            target,
            timeout_duration,
            "actix blocked",
        )
        .await
    }

    fn subscribe_unblock(&self) -> watch::Receiver<u64> {
        self.unblock_tx.subscribe()
    }

    fn notify_unblocked(&self) {
        let version = self.unblock_version.fetch_add(1, Ordering::SeqCst) + 1;
        self.unblock_tx.send_replace(version);
    }

    pub(crate) fn report_blocked(&self) {
        let generation = self.block_generation.load(Ordering::SeqCst);
        self.blocked_tx.send_replace(generation);
    }

    fn report_stopped(&self) {
        let version = self.stopped_version.fetch_add(1, Ordering::SeqCst) + 1;
        self.stopped_tx.send_replace(version);
    }
}

async fn wait_for_watch_at_least(
    mut rx: watch::Receiver<u64>,
    target: u64,
    timeout_duration: Duration,
    label: &str,
) -> Result<(), String> {
    tokio::time::timeout(timeout_duration, async {
        loop {
            if *rx.borrow() >= target {
                return Ok(());
            }
            rx.changed()
                .await
                .map_err(|_| format!("{label} channel closed"))?;
        }
    })
    .await
    .map_err(|_| format!("{label} timeout"))?
}

pub(crate) struct ControlState {
    pub(crate) app: Arc<BehaviorState>,
    pub(crate) actix: Arc<ActixControlState>,
}

impl ControlState {
    pub(crate) fn new() -> Self {
        Self {
            app: Arc::new(BehaviorState::new()),
            actix: Arc::new(ActixControlState::new()),
        }
    }
}

async fn wait_for_watch_advance(
    rx: &mut watch::Receiver<u64>,
    before: u64,
    label: &str,
) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if *rx.borrow() > before {
                return Ok(());
            }
            rx.changed()
                .await
                .map_err(|_| format!("{label} channel closed"))?;
        }
    })
    .await
    .map_err(|_| format!("{label} timeout"))?
}

struct DomusState {
    control: Arc<ControlState>,
    domus: Arc<RwLock<Arc<Domus<DomusResolver>>>>,
    driver_taberna: u64,
}

async fn build_domus(
    aurelia: &Aurelia,
    primary_addr: SocketAddr,
    config: DomusConfig,
    resolver: Arc<DomusResolver>,
    auth: Pkcs8AuthConfig,
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
    actix_bridge: Option<ActixTaberna>,
}

impl TabernaTasks {
    fn stop(&mut self) {
        let _ = self.shutdown.send(true);
        for handle in self.handles.drain(..) {
            handle.abort();
        }
        self.actix_bridge.take();
    }
}

async fn handle_app_request(mut request: TabernaRequest<WireCodec>, state: Arc<DomusState>) {
    state.control.app.wait_if_blocked().await;
    if request.message.msg_type != MSG_PING {
        request.reject();
        return;
    }
    match state.control.app.current().await {
        Behavior::Normal => {
            if let Some(id) = parse_id(&request.message.payload) {
                let domus = state.domus.read().await.clone();
                let driver_taberna = state.driver_taberna;
                if let Some(mut blob_receiver) = request.blob_receiver.take() {
                    state.control.app.report_blob_started();
                    request.accept();
                    tokio::spawn(async move {
                        let mut data = Vec::new();
                        let payload = match blob_receiver.read_to_end(&mut data).await {
                            Ok(_) => Bytes::from(format!("blob-pong:{id}:{}", data.len())),
                            Err(err) => Bytes::from(format!("blob-error:{id}:{err}")),
                        };
                        let response = WireMessage {
                            msg_type: MSG_PONG,
                            payload,
                        };
                        let _ = domus
                            .send(
                                &WireCodec,
                                driver_taberna,
                                &response,
                                SendOptions::MESSAGE_ONLY,
                            )
                            .await;
                    });
                    return;
                }
                let response = WireMessage {
                    msg_type: MSG_PONG,
                    payload: Bytes::from(format!("pong:{}", id)),
                };
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
            request.accept();
        }
        Behavior::Reject => {
            request.reject();
        }
        Behavior::Block => {
            request.accept();
        }
    }
}

struct ActixAppActor {
    state: Arc<DomusState>,
}

impl Actor for ActixAppActor {
    type Context = Context<Self>;

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        self.state.control.actix.report_stopped();
    }
}

struct StopActixActor;

impl actix::Message for StopActixActor {
    type Result = ();
}

impl Handler<StopActixActor> for ActixAppActor {
    type Result = ();

    fn handle(&mut self, _msg: StopActixActor, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}

impl Handler<ActixTabernaDelivery<WireMessage>> for ActixAppActor {
    type Result = ();

    fn handle(
        &mut self,
        delivery: ActixTabernaDelivery<WireMessage>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        if self.state.control.actix.current() == Behavior::Block {
            self.state.control.actix.report_blocked();
            let mut unblock_rx = self.state.control.actix.subscribe_unblock();
            async move {
                let _ = unblock_rx.changed().await;
                delivery
            }
            .into_actor(self)
            .map(|delivery, actor, ctx| actor.handle_delivery(delivery, ctx))
            .wait(ctx);
            return;
        }
        self.handle_delivery(delivery, ctx);
    }
}

impl ActixAppActor {
    fn handle_delivery(
        &mut self,
        mut delivery: ActixTabernaDelivery<WireMessage>,
        _ctx: &mut Context<Self>,
    ) {
        if delivery.message.msg_type != MSG_PING {
            return;
        }
        match self.state.control.actix.current() {
            Behavior::Normal => {
                if let Some(id) = parse_id(&delivery.message.payload) {
                    let state = Arc::clone(&self.state);
                    if let Some(mut blob_receiver) = delivery.blob_receiver.take() {
                        tokio::spawn(async move {
                            let mut data = Vec::new();
                            let payload = match blob_receiver.read_to_end(&mut data).await {
                                Ok(_) => Bytes::from(format!("blob-pong:{id}:{}", data.len())),
                                Err(err) => Bytes::from(format!("blob-error:{id}:{err}")),
                            };
                            send_driver_response(state, payload).await;
                        });
                    } else {
                        tokio::spawn(async move {
                            send_driver_response(state, Bytes::from(format!("pong:{id}"))).await;
                        });
                    }
                }
            }
            Behavior::Reject | Behavior::Block => {}
        }
    }
}

async fn send_driver_response(state: Arc<DomusState>, payload: Bytes) {
    let domus = state.domus.read().await.clone();
    let driver_taberna = state.driver_taberna;
    let response = WireMessage {
        msg_type: MSG_PONG,
        payload,
    };
    let _ = domus
        .send(
            &WireCodec,
            driver_taberna,
            &response,
            SendOptions::MESSAGE_ONLY,
        )
        .await;
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
    actix_taberna: u64,
) -> Result<TabernaTasks, Box<dyn std::error::Error>> {
    let app = domus.taberna(app_taberna, WireCodec).await?;
    let (shutdown, shutdown_rx) = watch::channel(false);
    let app_handle = tokio::spawn(run_app_loop(app, Arc::clone(&state), shutdown_rx));
    let actor = ActixAppActor::create(|ctx| {
        ctx.set_mailbox_capacity(1);
        ActixAppActor {
            state: Arc::clone(&state),
        }
    });
    state.control.actix.set_actor(actor.clone()).await;
    let actix_bridge = domus
        .actix_taberna(actix_taberna, WireCodec, actor.recipient())
        .await?;
    Ok(TabernaTasks {
        shutdown,
        handles: vec![app_handle],
        actix_bridge: Some(actix_bridge),
    })
}

#[actix::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let domus_index = env_u64("DOMUS_INDEX", 1);
    let app_taberna = env_u64("APP_TABERNA", APP_BASE + domus_index);
    let actix_taberna = env_u64("ACTIX_TABERNA", app_taberna + ACTIX_APP_OFFSET);
    let driver_taberna = env_u64("DRIVER_TABERNA", 9000);

    let primary_addr = env_socket("PRIMARY_ADDR");
    let driver_primary = env_socket("DRIVER_ADDR");
    let oob_port = env_u16("OOB_CONTROL_PORT", DEFAULT_OOB_CONTROL_PORT);
    let oob_addr = SocketAddr::new("0.0.0.0".parse().expect("ipv4 any"), oob_port);

    let ca_path = env::var("CA_CERT").expect("CA_CERT missing");
    let cert_path = env::var("DOMUS_CERT").expect("DOMUS_CERT missing");
    let key_path = env::var("DOMUS_KEY").expect("DOMUS_KEY missing");

    let mut config = DomusConfig::default();
    config.send_timeout = env_duration("SEND_TIMEOUT_MS", config.send_timeout);
    config.callis_connect_timeout =
        env_duration("CALLIS_CONNECT_TIMEOUT_MS", config.callis_connect_timeout);
    config.accept_timeout = env_duration("ACCEPT_TIMEOUT_MS", config.accept_timeout);
    config.listener_delay = env_duration("LISTENER_DELAY_MS", config.listener_delay);
    config.listener_reconnect_timeout = env_duration(
        "LISTENER_RECONNECT_TIMEOUT_MS",
        config.listener_reconnect_timeout,
    );
    config.keepalive_interval = env_duration("KEEPALIVE_MS", config.keepalive_interval);
    config.send_queue_size = env_usize("SEND_QUEUE_SIZE", config.send_queue_size);
    config.taberna_accept_queue_size = env_usize(
        "TABERNA_ACCEPT_QUEUE_SIZE",
        config.taberna_accept_queue_size,
    );
    if config.callis_connect_timeout > config.send_timeout {
        config.callis_connect_timeout = config.send_timeout;
    }

    let resolver = Arc::new(DomusResolver {
        driver_taberna,
        driver_primary,
    });
    let aurelia = Aurelia::new();
    let domus = build_domus(
        &aurelia,
        primary_addr,
        config.clone(),
        resolver.clone(),
        build_auth(&ca_path, &cert_path, &key_path),
    )
    .await?;
    let domus_handle: Arc<RwLock<Arc<Domus<DomusResolver>>>> = Arc::new(RwLock::new(domus));

    let controls = Arc::new(ControlState::new());
    let state = Arc::new(DomusState {
        control: Arc::clone(&controls),
        domus: Arc::clone(&domus_handle),
        driver_taberna,
    });

    let (runtime_tx, mut runtime_rx) = mpsc::channel(8);
    let mut taberna_tasks = spawn_taberna_tasks(
        domus_handle.read().await.clone(),
        Arc::clone(&state),
        app_taberna,
        actix_taberna,
    )
    .await?;

    // OOB control listener. Bind ordering is strict: the listener is spawned only after
    // build_domus has returned and the response/app tabernas have been registered. A
    // successful TCP connect on `oob_addr` is therefore a strict precondition that A1 is
    // fully initialised. The listener has its own shutdown channel scoped to the process
    // so it survives in-process restarts and only stops when the process is on its way out.
    let (oob_shutdown_tx, oob_shutdown_rx) = watch::channel(false);
    let oob_controls = Arc::clone(&controls);
    let oob_runtime_tx = runtime_tx.clone();
    tokio::spawn(async move {
        if let Err(err) =
            oob_control::serve(oob_addr, oob_controls, oob_runtime_tx, oob_shutdown_rx).await
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
                        // aurelia-test-allow-sleep: behavior-duration; shutdown downtime is the
                        // configured application behavior under test.
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
        "block" => Some(Behavior::Block),
        _ => None,
    }
}

fn parse_id(payload: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(payload);
    text.split(':').nth(1).map(|value| value.to_string())
}

fn build_auth(ca_path: &str, cert_path: &str, key_path: &str) -> Pkcs8AuthConfig {
    let ca_pem = std::fs::read(ca_path).expect("read ca");
    let cert_pem = std::fs::read(cert_path).expect("read cert");
    let pkcs8_key_pem = std::fs::read(key_path).expect("read key");

    Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
        ca_pem,
        cert_pem,
        pkcs8_key_pem: pkcs8_key_pem.into(),
    })
}
