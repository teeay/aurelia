// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

pub mod limited;

pub use limited::{
    init_limited_logging, log_ids, LimitedLogContext, LimitedLogControl, LimitedLogRegistry, LogId,
};
