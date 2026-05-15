// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{oneshot, Mutex, Notify};

use crate::codec::{EncodedMessage, MessageCodec};
use crate::config::{DomusConfig, DomusConfigAccess, DomusConfigBuilder};
use crate::message_id::PeerMessageIdAllocator;
use crate::peering::RouteLocalRemote;
use crate::taberna::{TabernaInbox, TabernaInboxHandle, TabernaRegistry, TabernaShutdownReport};
use crate::taberna::{TabernaRequest, TabernaRequestParts};
use crate::transport::{Transport, TransportBackend};
use crate::wire::PROTOCOL_VERSION;
use crate::wire::{ErrorPayload, WireHeader};
use crate::{a3_message_type, BlobReceiver, MessageType, SendOptions, SendOutcome, TabernaId};
use aurelia_data::DomusAddr;
use aurelia_data::RouteResolver;
use aurelia_ids::{AureliaError, ErrorId};
use caducus::MpscBuilder;
use std::time::Duration;

const PEERING_UNIT_TEST_TIMEOUT: Duration = Duration::from_secs(1);

mod blob_unit;
mod faults;
mod observability;
mod ring_buffer;
mod runtime;
mod session;
mod socket_transport;
mod transport;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct TestMessage {
    msg_type: MessageType,
    payload: Bytes,
}

pub(super) struct TestCodec;

impl MessageCodec for TestCodec {
    type AppMessage = TestMessage;

    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
        Ok(EncodedMessage::new(msg.msg_type, msg.payload.clone()))
    }

    fn decode_app(
        &self,
        msg_type: MessageType,
        payload: &[u8],
    ) -> Result<Self::AppMessage, AureliaError> {
        Ok(TestMessage {
            msg_type,
            payload: Bytes::copy_from_slice(payload),
        })
    }
}

struct EncodeFailCodec;

impl MessageCodec for EncodeFailCodec {
    type AppMessage = TestMessage;

    fn encode_app(&self, _msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
        Err(AureliaError::new(ErrorId::RemoteTabernaRejected))
    }

    fn decode_app(
        &self,
        msg_type: MessageType,
        payload: &[u8],
    ) -> Result<Self::AppMessage, AureliaError> {
        Ok(TestMessage {
            msg_type,
            payload: Bytes::copy_from_slice(payload),
        })
    }
}

struct DecodeFailCodec;

impl MessageCodec for DecodeFailCodec {
    type AppMessage = TestMessage;

    fn encode_app(&self, msg: &Self::AppMessage) -> Result<EncodedMessage, AureliaError> {
        Ok(EncodedMessage::new(msg.msg_type, msg.payload.clone()))
    }

    fn decode_app(
        &self,
        _msg_type: MessageType,
        _payload: &[u8],
    ) -> Result<Self::AppMessage, AureliaError> {
        Err(AureliaError::new(ErrorId::RemoteTabernaRejected))
    }
}

pub(super) fn test_message(msg_type: MessageType, payload: &'static [u8]) -> TestMessage {
    TestMessage {
        msg_type,
        payload: Bytes::from_static(payload),
    }
}

fn taberna_request(
    payload: &'static [u8],
) -> (
    TabernaRequest<TestCodec>,
    oneshot::Receiver<Result<(), AureliaError>>,
) {
    taberna_request_with_notify(payload, None)
}

fn taberna_request_with_notify(
    payload: &'static [u8],
    notify: Option<Arc<Notify>>,
) -> (
    TabernaRequest<TestCodec>,
    oneshot::Receiver<Result<(), AureliaError>>,
) {
    let (response, rx) = oneshot::channel();
    let request = TabernaRequest::new(
        test_message(a3_message_type(1), payload),
        None,
        response,
        notify,
    );
    (request, rx)
}

async fn receive_completion(
    rx: oneshot::Receiver<Result<(), AureliaError>>,
) -> Result<(), AureliaError> {
    tokio::time::timeout(Duration::from_millis(100), rx)
        .await
        .expect("completion wait")
        .expect("completion channel")
}

type AcceptedMessages = Arc<Mutex<Vec<(MessageType, Bytes, Option<BlobReceiver>)>>>;

struct MockSink {
    accepted: AcceptedMessages,
    mode: SinkMode,
    expected_msg_types: Vec<MessageType>,
}

#[derive(Clone, Copy, Debug)]
enum SinkMode {
    Ok,
    Err(AcceptErrorKind),
}

#[derive(Clone, Copy, Debug)]
enum AcceptErrorKind {
    RemoteTabernaRejected,
    TabernaBusy,
}

impl MockSink {
    fn new(expected_msg_types: Vec<MessageType>) -> Self {
        Self {
            accepted: Arc::new(Mutex::new(Vec::new())),
            mode: SinkMode::Ok,
            expected_msg_types,
        }
    }

    fn with_mode(expected_msg_types: Vec<MessageType>, mode: SinkMode) -> Self {
        Self {
            accepted: Arc::new(Mutex::new(Vec::new())),
            mode,
            expected_msg_types,
        }
    }
}

#[tokio::test]
async fn taberna_request_accept_completes_success() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let (request, rx) = taberna_request(b"accept");

        request.accept();

        receive_completion(rx).await.expect("accepted");
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn taberna_request_reject_completes_remote_rejected() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let (request, rx) = taberna_request(b"reject");

        request.reject();

        let err = receive_completion(rx)
            .await
            .expect_err("expected remote rejection");
        assert_eq!(err.kind, ErrorId::RemoteTabernaRejected);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn taberna_request_drop_without_decision_rejects() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let (request, rx) = taberna_request(b"drop");

        drop(request);

        let err = receive_completion(rx)
            .await
            .expect_err("expected drop rejection");
        assert_eq!(err.kind, ErrorId::RemoteTabernaRejected);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn taberna_request_into_parts_accept_completes_success() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let (request, rx) = taberna_request(b"parts-accept");

        let parts = request.into_parts();
        assert_eq!(parts.message.payload, Bytes::from_static(b"parts-accept"));
        parts.completion.accept();

        receive_completion(rx).await.expect("accepted");
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn taberna_request_accept_after_receiver_drop_notifies() {
    let notify = Arc::new(Notify::new());
    let (request, rx) =
        taberna_request_with_notify(b"accept-after-drop", Some(Arc::clone(&notify)));
    drop(rx);

    request.accept();
    tokio::time::timeout(Duration::from_millis(100), notify.notified())
        .await
        .expect("accept notification");
}

#[tokio::test]
async fn taberna_request_into_parts_busy_completes_taberna_busy() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let (request, rx) = taberna_request(b"parts-busy");

        let parts = request.into_parts();
        parts.completion.busy();

        let err = receive_completion(rx)
            .await
            .expect_err("expected taberna busy");
        assert_eq!(err.kind, ErrorId::TabernaBusy);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
#[cfg(feature = "actix")]
async fn taberna_request_into_parts_taberna_shutdown_completes_shutdown() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let (request, rx) = taberna_request(b"parts-shutdown");

        let parts = request.into_parts();
        parts.completion.taberna_shutdown();

        let err = receive_completion(rx)
            .await
            .expect_err("expected taberna shutdown");
        assert_eq!(err.kind, ErrorId::TabernaShutdown);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn taberna_request_into_parts_drop_message_then_accept_completes_success() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let (request, rx) = taberna_request(b"drop-message");

        let TabernaRequestParts {
            message,
            blob_receiver,
            completion,
        } = request.into_parts();
        drop(message);
        drop(blob_receiver);
        completion.accept();

        receive_completion(rx).await.expect("accepted");
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn taberna_request_into_parts_dropped_completion_rejects() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let (request, rx) = taberna_request(b"drop-completion");

        let parts = request.into_parts();
        drop(parts.completion);

        let err = receive_completion(rx)
            .await
            .expect_err("expected completion drop rejection");
        assert_eq!(err.kind, ErrorId::RemoteTabernaRejected);
    })
    .await
    .expect("async test timed out");
}

#[async_trait::async_trait]
impl TabernaInbox for MockSink {
    async fn enqueue(
        &self,
        msg_type: MessageType,
        payload: Bytes,
        blob_receiver: Option<BlobReceiver>,
        notify: Option<Arc<Notify>>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), AureliaError>>, AureliaError> {
        assert!(
            self.expected_msg_types.contains(&msg_type),
            "unexpected msg_type {msg_type}; expected one of {:?}",
            self.expected_msg_types
        );
        let (tx, rx) = tokio::sync::oneshot::channel();
        match self.mode {
            SinkMode::Ok => {
                self.accepted
                    .lock()
                    .await
                    .push((msg_type, payload, blob_receiver));
                let _ = tx.send(Ok(()));
                if let Some(notify) = notify.as_ref() {
                    notify.notify_one();
                }
            }
            SinkMode::Err(err) => {
                let err = match err {
                    AcceptErrorKind::RemoteTabernaRejected => {
                        AureliaError::new(ErrorId::RemoteTabernaRejected)
                    }
                    AcceptErrorKind::TabernaBusy => AureliaError::new(ErrorId::TabernaBusy),
                };
                let _ = tx.send(Err(err));
                if let Some(notify) = notify.as_ref() {
                    notify.notify_one();
                }
            }
        }
        Ok(rx)
    }
}

#[tokio::test]
async fn mock_sink_accepts_expected_input() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let msg_type = a3_message_type(10);
        let sink = MockSink::new(vec![msg_type]);

        let rx = sink
            .enqueue(msg_type, Bytes::from_static(b"ok"), None, None)
            .await
            .expect("expected input");

        receive_completion(rx).await.expect("accepted");
        let accepted = sink.accepted.lock().await;
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].0, msg_type);
        assert_eq!(accepted[0].1, Bytes::from_static(b"ok"));
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn mock_sink_rejects_unexpected_input_as_test_violation() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let expected = a3_message_type(10);
        let unexpected = a3_message_type(11);
        let sink = Arc::new(MockSink::new(vec![expected]));

        let task = {
            let sink = Arc::clone(&sink);
            tokio::spawn(async move {
                let _ = sink
                    .enqueue(unexpected, Bytes::from_static(b"bad"), None, None)
                    .await;
            })
        };

        let err = task
            .await
            .expect_err("unexpected input should fail the helper");
        assert!(err.is_panic());
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn mock_sink_explicit_rejection_is_application_rejection() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let msg_type = a3_message_type(12);
        let sink = MockSink::with_mode(
            vec![msg_type],
            SinkMode::Err(AcceptErrorKind::RemoteTabernaRejected),
        );

        let rx = sink
            .enqueue(msg_type, Bytes::from_static(b"reject"), None, None)
            .await
            .expect("expected input");

        let err = receive_completion(rx)
            .await
            .expect_err("explicit rejection");
        assert_eq!(err.kind, ErrorId::RemoteTabernaRejected);
    })
    .await
    .expect("async test timed out");
}

struct MockResolver {
    routes: Vec<(TabernaId, DomusAddr)>,
    mode: ResolverMode,
}

impl MockResolver {
    fn new(taberna_id: TabernaId, addr: DomusAddr) -> Self {
        Self {
            routes: vec![(taberna_id, addr)],
            mode: ResolverMode::Ok,
        }
    }

    fn empty(mode: ResolverMode) -> Self {
        Self {
            routes: Vec::new(),
            mode,
        }
    }

    fn delayed(taberna_id: TabernaId, addr: DomusAddr, duration: std::time::Duration) -> Self {
        Self {
            routes: vec![(taberna_id, addr)],
            mode: ResolverMode::Delay(duration),
        }
    }
}

#[tokio::test]
async fn mock_resolver_accepts_configured_id_and_rejects_neighbor() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let addr = DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5555));
        let resolver = MockResolver::new(7, addr.clone());

        let resolved = resolver.resolve(7).await.expect("configured route");
        assert_eq!(resolved, addr);

        let err = resolver
            .resolve(8)
            .await
            .expect_err("unexpected taberna id");
        assert_eq!(err.kind, ErrorId::UnknownTaberna);
    })
    .await
    .expect("async test timed out");
}

#[derive(Clone, Copy, Debug)]
enum ResolverMode {
    Ok,
    Unknown,
    Failed,
    Delay(std::time::Duration),
}

#[async_trait::async_trait]
impl RouteResolver for MockResolver {
    async fn resolve(&self, taberna_id: TabernaId) -> Result<DomusAddr, AureliaError> {
        match self.mode {
            ResolverMode::Ok => self
                .routes
                .iter()
                .find_map(|(id, addr)| (*id == taberna_id).then(|| addr.clone()))
                .ok_or_else(|| AureliaError::new(ErrorId::UnknownTaberna)),
            ResolverMode::Unknown => Err(AureliaError::new(ErrorId::UnknownTaberna)),
            ResolverMode::Failed => Err(AureliaError::with_message(
                ErrorId::PeerUnavailable,
                "resolver failed",
            )),
            ResolverMode::Delay(duration) => {
                tokio::time::sleep(duration).await;
                self.routes
                    .iter()
                    .find_map(|(id, addr)| (*id == taberna_id).then(|| addr.clone()))
                    .ok_or_else(|| AureliaError::new(ErrorId::UnknownTaberna))
            }
        }
    }
}

#[derive(Clone, Default)]
struct TestBackend;

#[async_trait::async_trait]
impl TransportBackend for TestBackend {
    type Addr = DomusAddr;
    type Listener = ();
    type Stream = tokio::io::DuplexStream;

    async fn bind(&self, _local: &Self::Addr) -> Result<Self::Listener, AureliaError> {
        Ok(())
    }

    async fn accept(
        &self,
        _listener: &mut Self::Listener,
    ) -> Result<
        crate::transport::backend::AuthenticatedStream<Self::Stream, Self::Addr>,
        AureliaError,
    > {
        Err(AureliaError::new(ErrorId::PeerUnavailable))
    }

    async fn dial(
        &self,
        _peer: &Self::Addr,
    ) -> Result<
        crate::transport::backend::AuthenticatedStream<Self::Stream, Self::Addr>,
        AureliaError,
    > {
        Err(AureliaError::new(ErrorId::PeerUnavailable))
    }
}

async fn test_transport(
    registry: Arc<TabernaRegistry>,
    config: DomusConfigAccess,
) -> Transport<TestBackend> {
    let backend = Arc::new(TestBackend);
    let transport = Transport::bind_with_backend(
        DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
        registry,
        config,
        crate::observability::new_observability(tokio::runtime::Handle::current()).1,
        tokio::runtime::Handle::current(),
        backend,
    )
    .await
    .expect("bind transport");
    transport
}

#[test]
fn wire_header_roundtrip() {
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags: 0x0102,
        msg_type: a3_message_type(42),
        peer_msg_id: 7,
        src_taberna: 11,
        dst_taberna: 22,
        payload_len: 256,
    };

    let encoded = header.encode();
    let decoded = WireHeader::decode(&encoded).expect("decode header");
    assert_eq!(header, decoded);
}

#[test]
fn wire_header_rejects_invalid_length() {
    let buf = [0u8; 4];
    let err = WireHeader::decode(&buf).expect_err("expected invalid length error");
    assert_eq!(err.kind, ErrorId::DecodeFailure);
}

#[test]
fn wire_header_rejects_unsupported_version() {
    let header = WireHeader {
        version: PROTOCOL_VERSION + 1,
        flags: 0,
        msg_type: a3_message_type(1),
        peer_msg_id: 1,
        src_taberna: 1,
        dst_taberna: 1,
        payload_len: 0,
    };

    let encoded = header.encode();
    let err = WireHeader::decode(&encoded).expect_err("expected unsupported version error");
    assert_eq!(err.kind, ErrorId::UnsupportedVersion);
}

#[test]
fn error_payload_rejects_short_buffer() {
    let buf = [0u8; 3];
    let err = ErrorPayload::from_bytes(&buf).expect_err("expected invalid length error");
    assert_eq!(err.kind, ErrorId::DecodeFailure);
}

#[test]
fn app_send_validator_rejects_wire_unrepresentable_length() {
    let err =
        crate::peering::validate_app_send(a3_message_type(1), u32::MAX as usize + 1, usize::MAX)
            .expect_err("expected wire length rejection");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}

#[test]
fn peer_message_id_rollover() {
    let allocator = PeerMessageIdAllocator::new(u32::MAX);
    assert_eq!(allocator.next(), u32::MAX);
    assert_eq!(allocator.next(), 0);
}

#[tokio::test]
async fn peering_rejects_local_non_a3_message_before_delivery() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let sink = Arc::new(MockSink::new(vec![a3_message_type(0)]));
        let taberna_id = 44;
        registry.register(taberna_id, sink.clone()).await.unwrap();

        let config = DomusConfigAccess::from_config(DomusConfig::default());
        let resolver = Arc::new(MockResolver::empty(ResolverMode::Ok));
        let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
        let peering = RouteLocalRemote::new(config, registry, resolver, transport);
        let codec = TestCodec;

        let err = peering
            .send(
                &codec,
                taberna_id,
                &TestMessage {
                    msg_type: 1,
                    payload: Bytes::from_static(b"not-a3"),
                },
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect_err("expected A3 range rejection");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
        assert!(sink.accepted.lock().await.is_empty());
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peering_rejects_local_oversized_payload_before_delivery() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let msg_type = a3_message_type(2);
        let sink = Arc::new(MockSink::new(vec![msg_type]));
        let taberna_id = 45;
        registry.register(taberna_id, sink.clone()).await.unwrap();

        let config = DomusConfigBuilder::new()
            .max_payload_len(3)
            .build()
            .expect("valid config");
        let config = DomusConfigAccess::from_config(config);
        let resolver = Arc::new(MockResolver::empty(ResolverMode::Ok));
        let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
        let peering = RouteLocalRemote::new(config, registry, resolver, transport);
        let codec = TestCodec;

        let err = peering
            .send(
                &codec,
                taberna_id,
                &TestMessage {
                    msg_type,
                    payload: Bytes::from_static(b"four"),
                },
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect_err("expected payload length rejection");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
        assert!(sink.accepted.lock().await.is_empty());
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peering_rejects_remote_non_a3_message_before_routing() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let config = DomusConfigAccess::from_config(DomusConfig::default());
        let resolver = Arc::new(MockResolver::empty(ResolverMode::Failed));
        let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
        let peering = RouteLocalRemote::new(config, registry, resolver, transport);
        let codec = TestCodec;

        let err = peering
            .send(
                &codec,
                999,
                &TestMessage {
                    msg_type: 1,
                    payload: Bytes::from_static(b"not-a3"),
                },
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect_err("expected A3 range rejection");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peering_rejects_remote_oversized_payload_before_routing() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let config = DomusConfigBuilder::new()
            .max_payload_len(3)
            .build()
            .expect("valid config");
        let config = DomusConfigAccess::from_config(config);
        let resolver = Arc::new(MockResolver::empty(ResolverMode::Failed));
        let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
        let peering = RouteLocalRemote::new(config, registry, resolver, transport);
        let codec = TestCodec;

        let err = peering
            .send(
                &codec,
                999,
                &TestMessage {
                    msg_type: a3_message_type(3),
                    payload: Bytes::from_static(b"four"),
                },
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect_err("expected payload length rejection");
        assert_eq!(err.kind, ErrorId::ProtocolViolation);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn taberna_registry_register_and_resolve() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = TabernaRegistry::new();
        let sink = Arc::new(MockSink::new(vec![a3_message_type(0)]));
        let taberna_id = 42;

        registry.register(taberna_id, sink.clone()).await.unwrap();
        let resolved = registry.resolve_local(taberna_id).await;
        assert!(resolved.is_some());

        registry.unregister(taberna_id).await;
        let resolved = registry.resolve_local(taberna_id).await;
        assert!(resolved.is_none());
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peering_dispatches_local_and_remote() {
    let registry = Arc::new(TabernaRegistry::new());
    let local_msg_type = a3_message_type(90);
    let remote_msg_type = a3_message_type(10);
    let sink = Arc::new(MockSink::new(vec![local_msg_type]));
    let taberna_id = 1;
    registry.register(taberna_id, sink.clone()).await.unwrap();

    let config = DomusConfigBuilder::new()
        .send_timeout(std::time::Duration::from_millis(2))
        .callis_connect_timeout(std::time::Duration::from_millis(2))
        .accept_timeout(std::time::Duration::from_millis(2))
        .build()
        .expect("valid domus config");
    let config = DomusConfigAccess::from_config(config);
    let resolver = Arc::new(MockResolver::new(
        999,
        DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5555)),
    ));
    let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);

    let peering = RouteLocalRemote::new(config, registry, resolver, transport);
    let codec = TestCodec;

    peering
        .send(
            &codec,
            taberna_id,
            &test_message(local_msg_type, b"local"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect("local send");

    {
        let accepted = sink.accepted.lock().await;
        assert_eq!(accepted.len(), 1);
    }

    let err = peering
        .send(
            &codec,
            999,
            &test_message(remote_msg_type, b"remote"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect_err("remote send timeout");
    assert_eq!(err.kind, ErrorId::SendTimeout);
}

#[tokio::test]
async fn peering_local_accept_rejected_maps_error() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let msg_type = a3_message_type(1);
        let sink = Arc::new(MockSink::with_mode(
            vec![msg_type],
            SinkMode::Err(AcceptErrorKind::RemoteTabernaRejected),
        ));
        let taberna_id = 5;
        registry.register(taberna_id, sink).await.unwrap();

        let config = DomusConfigAccess::from_config(DomusConfig::default());
        let resolver = Arc::new(MockResolver::empty(ResolverMode::Ok));
        let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
        let peering = RouteLocalRemote::new(config, registry, resolver, transport);
        let codec = TestCodec;

        let err = peering
            .send(
                &codec,
                taberna_id,
                &test_message(msg_type, b"reject"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect_err("expected rejection");
        assert_eq!(err.kind, ErrorId::RemoteTabernaRejected);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peering_local_accept_timeout_maps_error() {
    let registry = Arc::new(TabernaRegistry::new());
    let msg_type = a3_message_type(1);
    let taberna_id = 6;
    let domus_config = DomusConfigBuilder::new()
        .accept_timeout(std::time::Duration::from_millis(20))
        .taberna_accept_queue_size(1)
        .build()
        .expect("valid domus config");
    let config = DomusConfigAccess::from_config(domus_config.clone());
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
        config.clone(),
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    ));
    registry.register(taberna_id, inbox).await.unwrap();
    let _receiver_guard = receiver;

    let resolver = Arc::new(MockResolver::empty(ResolverMode::Ok));
    let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
    let peering = RouteLocalRemote::new(config, registry, resolver, transport);
    let codec = TestCodec;

    let err = peering
        .send(
            &codec,
            taberna_id,
            &test_message(msg_type, b"timeout"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect_err("expected taberna busy");
    assert_eq!(err.kind, ErrorId::TabernaBusy);
}

#[tokio::test]
async fn taberna_queue_expiry_emits_taberna_busy() {
    let domus_config = DomusConfigBuilder::new()
        .accept_timeout(Duration::from_millis(20))
        .taberna_accept_queue_size(1)
        .build()
        .expect("valid domus config");
    let config = DomusConfigAccess::from_config(domus_config.clone());
    let (sender, receiver) = MpscBuilder::new(
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    )
    .runtime(tokio::runtime::Handle::current())
    .build()
    .expect("caducus build");
    let inbox = TabernaInboxHandle::new(
        TestCodec,
        sender,
        config,
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    );
    let _receiver_guard = receiver;

    let accept_rx = inbox
        .enqueue(
            a3_message_type(1),
            Bytes::from_static(b"expire"),
            None,
            None,
        )
        .await
        .expect("enqueue");

    let err = receive_completion(accept_rx)
        .await
        .expect_err("expected taberna busy");
    assert_eq!(err.kind, ErrorId::TabernaBusy);
}

#[tokio::test]
async fn peering_encode_failure_is_normalized() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let msg_type = a3_message_type(1);
        let config = DomusConfigAccess::from_config(DomusConfig::default());
        let resolver = Arc::new(MockResolver::empty(ResolverMode::Ok));
        let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
        let peering = RouteLocalRemote::new(config, registry, resolver, transport);

        let err = peering
            .send(
                &EncodeFailCodec,
                5,
                &test_message(msg_type, b"encode"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect_err("expected encode failure");
        assert_eq!(err.kind, ErrorId::EncodeFailure);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peering_decode_failure_is_normalized_before_delivery() {
    let registry = Arc::new(TabernaRegistry::new());
    let msg_type = a3_message_type(1);
    let taberna_id = 16;
    let domus_config = DomusConfigBuilder::new()
        .accept_timeout(std::time::Duration::from_millis(20))
        .taberna_accept_queue_size(1)
        .build()
        .expect("valid domus config");
    let config = DomusConfigAccess::from_config(domus_config.clone());
    let (sender, _receiver) = MpscBuilder::new(
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    )
    .runtime(tokio::runtime::Handle::current())
    .build()
    .expect("caducus build");
    let inbox = Arc::new(TabernaInboxHandle::new(
        DecodeFailCodec,
        sender,
        config.clone(),
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    ));
    registry.register(taberna_id, inbox).await.unwrap();

    let resolver = Arc::new(MockResolver::empty(ResolverMode::Ok));
    let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
    let peering = RouteLocalRemote::new(config, registry, resolver, transport);

    let err = peering
        .send(
            &TestCodec,
            taberna_id,
            &test_message(msg_type, b"decode"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect_err("expected decode failure");
    assert_eq!(err.kind, ErrorId::DecodeFailure);
}

#[tokio::test]
async fn taberna_queue_shutdown_emits_domus_closed() {
    let registry = Arc::new(TabernaRegistry::new());
    let taberna_id = 8u64;
    let domus_config = DomusConfigBuilder::new()
        .accept_timeout(Duration::from_secs(1))
        .taberna_accept_queue_size(1)
        .build()
        .expect("valid domus config");
    let config = DomusConfigAccess::from_config(domus_config.clone());
    let (sender, receiver) = MpscBuilder::new(
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    )
    .shutdown_channel(Arc::new(TabernaShutdownReport::<TestCodec>::new()))
    .runtime(tokio::runtime::Handle::current())
    .build()
    .expect("caducus build");
    let inbox: Arc<dyn TabernaInbox> = Arc::new(TabernaInboxHandle::new(
        TestCodec,
        sender,
        config.clone(),
        domus_config.taberna_accept_queue_size,
        domus_config.accept_timeout,
    ));
    registry
        .register(taberna_id, Arc::clone(&inbox))
        .await
        .unwrap();
    let _receiver_guard = receiver;

    let accept_rx = inbox
        .enqueue(
            a3_message_type(1),
            Bytes::from_static(b"shutdown"),
            None,
            None,
        )
        .await
        .expect("enqueue");

    registry.shutdown().await;

    let result = tokio::time::timeout(Duration::from_millis(100), accept_rx)
        .await
        .expect("accept wait");
    let err = result.expect("accept recv").expect_err("domus closed");
    assert_eq!(err.kind, ErrorId::DomusClosed);
}

#[tokio::test]
async fn peering_local_accept_explicit_timeout_maps_error() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let msg_type = a3_message_type(1);
        let sink = Arc::new(MockSink::with_mode(
            vec![msg_type],
            SinkMode::Err(AcceptErrorKind::TabernaBusy),
        ));
        let taberna_id = 7;
        registry.register(taberna_id, sink).await.unwrap();

        let config = DomusConfigAccess::from_config(DomusConfig::default());
        let resolver = Arc::new(MockResolver::empty(ResolverMode::Ok));
        let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
        let peering = RouteLocalRemote::new(config, registry, resolver, transport);
        let codec = TestCodec;

        let err = peering
            .send(
                &codec,
                taberna_id,
                &test_message(msg_type, b"timeout"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect_err("expected taberna busy");
        assert_eq!(err.kind, ErrorId::TabernaBusy);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peering_route_resolution_errors_map() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let msg_type = a3_message_type(1);
        let config = DomusConfigAccess::from_config(DomusConfig::default());
        let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
        let taberna_id = 999;

        let resolver = Arc::new(MockResolver::empty(ResolverMode::Unknown));
        let peering = RouteLocalRemote::new(
            config.clone(),
            Arc::clone(&registry),
            resolver,
            Arc::clone(&transport),
        );
        let codec = TestCodec;
        let err = peering
            .send(
                &codec,
                taberna_id,
                &test_message(msg_type, b"x"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect_err("unknown taberna");
        assert_eq!(err.kind, ErrorId::UnknownTaberna);

        let resolver = Arc::new(MockResolver::empty(ResolverMode::Failed));
        let peering = RouteLocalRemote::new(config, registry, resolver, transport);
        let err = peering
            .send(
                &codec,
                taberna_id,
                &test_message(msg_type, b"x"),
                SendOptions::MESSAGE_ONLY,
            )
            .await
            .expect_err("resolver failed");
        assert_eq!(err.kind, ErrorId::PeerUnavailable);
        assert!(err
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("resolver failed"));
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peering_remote_dispatch_times_out_without_peer() {
    let registry = Arc::new(TabernaRegistry::new());
    let msg_type = a3_message_type(1);
    let config = DomusConfigBuilder::new()
        .send_timeout(std::time::Duration::from_millis(2))
        .callis_connect_timeout(std::time::Duration::from_millis(2))
        .accept_timeout(std::time::Duration::from_millis(2))
        .build()
        .expect("valid domus config");
    let config = DomusConfigAccess::from_config(config);
    let resolver = Arc::new(MockResolver::new(
        999,
        DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5555)),
    ));
    let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
    let peering = RouteLocalRemote::new(config, registry, resolver, transport);
    let codec = TestCodec;

    let err = peering
        .send(
            &codec,
            999,
            &test_message(msg_type, b"x"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect_err("expected send timeout");
    assert_eq!(err.kind, ErrorId::SendTimeout);
}

#[tokio::test]
async fn peering_route_resolution_timeout_maps_send_timeout() {
    let registry = Arc::new(TabernaRegistry::new());
    let msg_type = a3_message_type(1);
    let config = DomusConfigBuilder::new()
        .send_timeout(std::time::Duration::from_millis(1))
        .callis_connect_timeout(std::time::Duration::from_millis(1))
        .accept_timeout(std::time::Duration::from_millis(1))
        .build()
        .expect("valid domus config");
    let config = DomusConfigAccess::from_config(config);
    let resolver = Arc::new(MockResolver::delayed(
        500,
        DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5555)),
        std::time::Duration::from_millis(5),
    ));
    let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
    let peering = RouteLocalRemote::new(config, registry, resolver, transport);
    let codec = TestCodec;

    let err = peering
        .send(
            &codec,
            500,
            &test_message(msg_type, b"timeout"),
            SendOptions::MESSAGE_ONLY,
        )
        .await
        .expect_err("expected send timeout");
    assert_eq!(err.kind, ErrorId::SendTimeout);
}

#[tokio::test]
async fn peering_send_blob_local_streams_chunks() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let msg_type = a3_message_type(44);
        let sink = Arc::new(MockSink::new(vec![msg_type]));
        let taberna_id = 2;
        registry.register(taberna_id, sink.clone()).await.unwrap();

        let config = DomusConfigAccess::from_config(DomusConfig::default());
        let resolver = Arc::new(MockResolver::empty(ResolverMode::Unknown));
        let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
        let peering = RouteLocalRemote::new(config, registry, resolver, transport);
        let codec = TestCodec;

        let outcome = peering
            .send(
                &codec,
                taberna_id,
                &test_message(msg_type, b"blob"),
                SendOptions::BLOB,
            )
            .await
            .expect("send blob");
        let mut sender = match outcome {
            SendOutcome::Blob { sender } => sender,
            SendOutcome::MessageOnly => panic!("expected blob sender"),
        };
        use tokio::io::AsyncWriteExt;
        sender.write_all(b"first").await.expect("write first chunk");
        sender
            .write_all(b"second")
            .await
            .expect("write second chunk");
        sender.shutdown().await.expect("shutdown sender");

        let mut accepted = sink.accepted.lock().await;
        assert_eq!(accepted.len(), 1);
        let record = accepted.pop().expect("recorded blob");
        assert_eq!(record.0, msg_type);
        assert_eq!(record.1, Bytes::from_static(b"blob"));
        let mut receiver = record.2.expect("blob receiver");
        drop(accepted);

        use tokio::io::AsyncReadExt;
        let mut buffer = Vec::new();
        receiver.read_to_end(&mut buffer).await.expect("read blob");
        assert_eq!(buffer, b"firstsecond");
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peering_send_blob_local_respects_buffer_caps() {
    tokio::time::timeout(PEERING_UNIT_TEST_TIMEOUT, async {
        let registry = Arc::new(TabernaRegistry::new());
        let msg_type = a3_message_type(44);
        let sink = Arc::new(MockSink::new(vec![msg_type]));
        let taberna_id = 2;
        registry.register(taberna_id, sink.clone()).await.unwrap();

        let config = DomusConfigBuilder::new()
            .blob_window(4, 2)
            .blob_outbound_buffer_bytes(8)
            .blob_inbound_buffer_bytes(8)
            .build()
            .expect("valid domus config");
        let config = DomusConfigAccess::from_config(config);
        let resolver = Arc::new(MockResolver::empty(ResolverMode::Unknown));
        let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
        let peering = RouteLocalRemote::new(config, registry, resolver, transport);
        let codec = TestCodec;

        let outcome = peering
            .send(
                &codec,
                taberna_id,
                &test_message(msg_type, b"blob"),
                SendOptions::BLOB,
            )
            .await
            .expect("send blob");
        let sender = match outcome {
            SendOutcome::Blob { sender } => sender,
            SendOutcome::MessageOnly => panic!("expected blob sender"),
        };

        let mut accepted = sink.accepted.lock().await;
        assert_eq!(accepted.len(), 1);
        let record = accepted.pop().expect("recorded blob");
        let receiver = record.2.expect("blob receiver");
        drop(accepted);

        let err = peering
            .send(
                &codec,
                taberna_id,
                &test_message(msg_type, b"blob-two"),
                SendOptions::BLOB,
            )
            .await
            .expect_err("expected blob buffer full");
        assert_eq!(err.kind, ErrorId::BlobBufferFull);

        drop(sender);
        drop(receiver);

        let outcome = peering
            .send(
                &codec,
                taberna_id,
                &test_message(msg_type, b"blob-three"),
                SendOptions::BLOB,
            )
            .await
            .expect("send blob after release");
        let sender = match outcome {
            SendOutcome::Blob { sender } => sender,
            SendOutcome::MessageOnly => panic!("expected blob sender"),
        };
        drop(sender);

        let mut accepted = sink.accepted.lock().await;
        assert_eq!(accepted.len(), 1);
        let record = accepted.pop().expect("recorded blob");
        drop(record.2);
    })
    .await
    .expect("async test timed out");
}

#[tokio::test]
async fn peering_send_blob_remote_dispatch_times_out_without_peer() {
    let registry = Arc::new(TabernaRegistry::new());
    let msg_type = a3_message_type(33);
    let config = DomusConfigBuilder::new()
        .send_timeout(std::time::Duration::from_millis(2))
        .callis_connect_timeout(std::time::Duration::from_millis(2))
        .accept_timeout(std::time::Duration::from_millis(2))
        .build()
        .expect("valid domus config");
    let config = DomusConfigAccess::from_config(config);
    let resolver = Arc::new(MockResolver::new(
        901,
        DomusAddr::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5559)),
    ));
    let transport = Arc::new(test_transport(Arc::clone(&registry), config.clone()).await);
    let peering = RouteLocalRemote::new(config, registry, resolver, transport);
    let codec = TestCodec;

    let err = peering
        .send(
            &codec,
            901,
            &test_message(msg_type, b"remote"),
            SendOptions::BLOB,
        )
        .await
        .expect_err("expected send timeout");
    assert_eq!(err.kind, ErrorId::SendTimeout);
}
