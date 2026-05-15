// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::chunks::OutboundChunks;
use aurelia_ids::{AureliaError, ErrorId};

#[derive(Debug)]
pub(super) struct OutboundState {
    pub(super) lifecycle: OutboundLifecycle,
    pub(super) chunks: OutboundChunks,
}

impl OutboundState {
    pub(super) fn new(chunk_size: usize, window_size: usize) -> Self {
        Self {
            lifecycle: OutboundLifecycle::Open,
            chunks: OutboundChunks::new(chunk_size, window_size),
        }
    }
}

#[derive(Debug)]
pub(super) enum OutboundLifecycle {
    Open,
    Sealing,
    AwaitingComplete,
    Completed,
    Failed(AureliaError),
    Closed,
}

impl OutboundLifecycle {
    pub(super) fn failure(&self) -> Option<AureliaError> {
        match self {
            Self::Failed(err) => Some(err.clone()),
            _ => None,
        }
    }

    pub(super) fn sender_error(&self) -> Result<(), AureliaError> {
        match self {
            Self::Open => Ok(()),
            Self::Failed(err) => Err(err.clone()),
            Self::Sealing | Self::AwaitingComplete | Self::Completed | Self::Closed => {
                Err(AureliaError::new(ErrorId::PeerUnavailable))
            }
        }
    }

    pub(super) fn seal_error(&self) -> Result<(), AureliaError> {
        match self {
            Self::Open | Self::Sealing | Self::AwaitingComplete | Self::Completed => Ok(()),
            Self::Failed(err) => Err(err.clone()),
            Self::Closed => Err(AureliaError::new(ErrorId::PeerUnavailable)),
        }
    }

    pub(super) fn mark_sealed(&mut self) {
        if matches!(self, Self::Open) {
            *self = Self::Sealing;
        }
    }

    pub(super) fn mark_final_chunk_sent(&mut self) {
        if matches!(self, Self::Open | Self::Sealing) {
            *self = Self::AwaitingComplete;
        }
    }

    pub(super) fn mark_complete(&mut self) {
        if !matches!(self, Self::Failed(_)) {
            *self = Self::Completed;
        }
    }

    pub(super) fn mark_failed(&mut self, err: AureliaError) {
        if !matches!(self, Self::Failed(_)) {
            *self = Self::Failed(err);
        }
    }

    pub(super) fn replace_failure(&mut self, err: AureliaError) {
        *self = Self::Failed(err);
    }

    pub(super) fn mark_closed(&mut self) {
        if !matches!(self, Self::Failed(_)) {
            *self = Self::Closed;
        }
    }

    pub(super) fn can_drain_chunks(&self) -> bool {
        !matches!(self, Self::Failed(_) | Self::Closed)
    }

    pub(super) fn is_closed(&self) -> bool {
        matches!(self, Self::Closed)
    }

    pub(super) fn final_chunk_materialized(&self) -> bool {
        matches!(self, Self::AwaitingComplete | Self::Completed)
    }
}
