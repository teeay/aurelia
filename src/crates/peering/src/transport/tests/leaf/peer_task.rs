// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

struct DropSignal(Option<oneshot::Sender<()>>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

#[tokio::test]
async fn dropping_task_set_aborts_task_holding_spawner_clone() {
    let runtime_handle = tokio::runtime::Handle::current();
    let task_set = PeerTaskSet::new(&runtime_handle);
    let spawner = task_set.spawner();
    let (started_tx, started_rx) = oneshot::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let held_spawner = spawner.clone();

    spawner.spawn(async move {
        let _held_spawner = held_spawner;
        let _drop_signal = DropSignal(Some(dropped_tx));
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
    });

    timeout(Duration::from_millis(250), started_rx)
        .await
        .expect("task should start")
        .expect("start signal");
    drop(task_set);

    timeout(Duration::from_millis(250), dropped_rx)
        .await
        .expect("task should be aborted")
        .expect("drop signal");
}
