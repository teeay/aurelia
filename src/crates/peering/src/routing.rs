// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use crate::address::DomusAddr;
use aurelia_ids::AureliaError;
use aurelia_ids::TabernaId;

/// Application-supplied resolver that maps a target [`TabernaId`] to the
/// [`DomusAddr`] of the peer hosting it. Aurelia calls this on every send
/// so applications can implement service discovery as they see fit.
#[async_trait::async_trait]
pub trait RouteResolver: Send + Sync {
    /// Resolves `taberna_id` to the [`DomusAddr`] of the peer that hosts it.
    async fn resolve(&self, taberna_id: TabernaId) -> Result<DomusAddr, AureliaError>;
}
