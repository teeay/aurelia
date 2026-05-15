// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CallisKind {
    Primary,
    Blob,
}

pub(crate) fn callis_kind_label(callis: CallisKind) -> &'static str {
    match callis {
        CallisKind::Primary => "primary",
        CallisKind::Blob => "blob",
    }
}
