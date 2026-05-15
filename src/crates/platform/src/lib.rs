// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

//! Internal platform services shared by Aurelia crates.
//!
//! This crate is not part of the public `aurelia` API. It owns process-level
//! invariants that multiple internal crates need to share, starting with the
//! dedicated Aurelia runtime.

pub mod runtime;
