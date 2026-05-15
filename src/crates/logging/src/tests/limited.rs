// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn limited_logging_blocks_within_interval() {
    let (tx, rx) = watch::channel(120u64);
    let registry = LimitedLogRegistry::new(rx);
    let last = registry
        .last_log
        .get(&LogId::HandshakeTotalLimit)
        .expect("log id");
    last.store(now_secs().saturating_sub(240), Ordering::SeqCst);

    assert!(registry.should_log(LogId::HandshakeTotalLimit));
    assert!(!registry.should_log(LogId::HandshakeTotalLimit));

    let _ = tx.send_replace(0);
    assert!(registry.should_log(LogId::HandshakeTotalLimit));
}
