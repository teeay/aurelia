// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::runtime;
use super::Aurelia;

#[test]
fn runtime_initializes_and_spawns() {
    let _aurelia = Aurelia::init();
    let handle = runtime::handle();
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle.spawn(async move {
        let _ = tx.send(42u8);
    });
    let value = runtime::ensure().block_on(async { rx.await.expect("runtime task should send") });
    assert_eq!(value, 42u8);
}
