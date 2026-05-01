// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) async fn run_listener<B>(inner: Arc<TransportInner<B>>, mut listener: B::Listener)
where
    B: TransportBackend<Addr = DomusAddr> + 'static,
{
    let mut shutdown_rx = inner.shutdown_tx.subscribe();
    let mut listener_shutdown_rx = inner.listener_shutdown_tx.subscribe();
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = listener_shutdown_rx.changed() => {
                if *listener_shutdown_rx.borrow() {
                    break;
                }
            }
            accept = inner.backend.accept(&mut listener) => {
                let authenticated = match accept {
                    Ok(value) => value,
                    Err(err) => {
                        inner
                            .observability
                            .listener_failure(inner.local_addr.clone(), err.kind)
                            .await;
                        warn!(error = %err, "listener accept failed");
                        continue;
                    }
                };
                let peer_addr = authenticated.peer_addr.clone();
                let inner = Arc::clone(&inner);
                let runtime_handle = inner.runtime_handle.clone();
                runtime_handle.spawn(async move {
                    info!(peer = %peer_addr, "inbound callis accepted");
                    if let Ok(handle) = inner.peer_handle_inbound(peer_addr).await {
                        handle.inbound(authenticated).await;
                    }
                });
            }
        }
    }
}
