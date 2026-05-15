// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use tokio::sync::mpsc;

use super::*;

#[test]
fn producer_emission_drops_when_command_channel_is_full() {
    let (tx, mut rx) = mpsc::channel(1);
    let handle = ObservabilityHandle { tx };

    handle.dial_attempt();
    handle.dial_attempt();

    assert!(matches!(
        rx.try_recv().expect("first command"),
        ObservabilityCommand::DialAttempt
    ));
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}
