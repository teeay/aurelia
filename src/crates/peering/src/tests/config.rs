// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

const CONFIG_ASYNC_TEST_TIMEOUT: Duration = Duration::from_millis(100);

fn gib(value: u64) -> u64 {
    value * 1024 * 1024 * 1024
}

#[test]
fn clamp_blob_buffers_to_half_memory_cap() {
    let config = DomusConfig {
        blob_outbound_buffer_bytes: gib(7),
        blob_inbound_buffer_bytes: gib(9),
        ..Default::default()
    };

    let (config, clamp) =
        normalize_domus_config(config, Some(gib(10))).expect("valid config with clamp");

    assert!(clamp.outbound_clamped);
    assert!(clamp.inbound_clamped);
    assert_eq!(clamp.cap_bytes, gib(5));
    assert_eq!(config.blob_outbound_buffer_bytes, gib(5));
    assert_eq!(config.blob_inbound_buffer_bytes, gib(5));
}

#[test]
fn reject_blob_buffers_below_reservation() {
    let config = DomusConfig {
        blob_window: BlobWindowConfig::new(MAX_BLOB_CHUNK_SIZE, MAX_BLOB_ACK_WINDOW),
        blob_outbound_buffer_bytes: gib(1),
        blob_inbound_buffer_bytes: gib(1),
        ..Default::default()
    };

    let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
    assert!(err.to_string().contains("blob_outbound_buffer_bytes"));
}

#[test]
fn reject_send_queue_size_over_max() {
    let config = DomusConfig {
        send_queue_size: MAX_SEND_QUEUE_SIZE + 1,
        ..Default::default()
    };
    let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
    assert!(err.to_string().contains("send_queue_size"));
}

#[test]
fn reject_zero_send_timeout() {
    let config = DomusConfig {
        send_timeout: Duration::from_secs(0),
        ..Default::default()
    };
    let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
    assert!(err.to_string().contains("send_timeout"));
}

#[test]
fn default_callis_and_tcp_timeout_values_match_contract() {
    let config = DomusConfig::default();
    assert_eq!(config.callis_connect_timeout, Duration::from_secs(15));
    assert_eq!(config.tcp_handshake_timeout, Duration::from_secs(10));
}

#[test]
fn reject_zero_callis_connect_timeout() {
    let config = DomusConfig {
        callis_connect_timeout: Duration::from_secs(0),
        ..Default::default()
    };
    let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
    assert!(err.to_string().contains("callis_connect_timeout"));
}

#[test]
fn reject_callis_connect_timeout_over_max() {
    let config = DomusConfig {
        callis_connect_timeout: MAX_CALLIS_CONNECT_TIMEOUT + Duration::from_secs(1),
        send_timeout: MAX_SEND_TIMEOUT,
        ..Default::default()
    };
    let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
    assert!(err.to_string().contains("callis_connect_timeout"));
}

#[test]
fn reject_callis_connect_timeout_over_send_timeout() {
    let config = DomusConfig {
        send_timeout: Duration::from_secs(10),
        callis_connect_timeout: Duration::from_secs(11),
        ..Default::default()
    };
    let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
    assert!(err.to_string().contains("callis_connect_timeout"));
}

#[test]
fn reject_zero_accept_timeout() {
    let config = DomusConfig {
        send_timeout: Duration::from_secs(1),
        callis_connect_timeout: Duration::from_secs(1),
        accept_timeout: Duration::from_secs(0),
        ..Default::default()
    };
    let err = normalize_domus_config(config, Some(gib(32))).expect_err("expected failure");
    assert!(err.to_string().contains("accept_timeout"));
}

#[tokio::test]
async fn config_store_updates_taberna_limits_cache() {
    tokio::time::timeout(CONFIG_ASYNC_TEST_TIMEOUT, async {
        let store = DomusConfigStore::new(DomusConfig::default());

        let next = DomusConfig {
            send_timeout: Duration::from_secs(10),
            callis_connect_timeout: Duration::from_secs(10),
            accept_timeout: Duration::from_secs(7),
            taberna_accept_queue_size: 9,
            ..Default::default()
        };
        store.update(next).await;

        assert_eq!(
            store.taberna_limits(),
            TabernaLimits {
                accept_queue_size: 9,
                accept_timeout: Duration::from_secs(7),
            }
        );
    })
    .await
    .expect("async test timed out");
}
