// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Network address of a domus peer, abstracting over the supported transports.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DomusAddr {
    /// TCP socket address (used with mTLS).
    Tcp(SocketAddr),
    /// Unix domain socket path (used with peer-credential authentication).
    Socket(PathBuf),
}

/// Tag identifying which transport a [`DomusAddr`] uses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportKind {
    /// TCP transport with mTLS authentication.
    Tcp,
    /// Unix domain socket transport with peer-credential authentication.
    Socket,
}

impl DomusAddr {
    /// Returns the transport kind backing this address.
    pub fn kind(&self) -> TransportKind {
        match self {
            DomusAddr::Tcp(_) => TransportKind::Tcp,
            DomusAddr::Socket(_) => TransportKind::Socket,
        }
    }

    /// Returns the underlying [`SocketAddr`] if this is a TCP address.
    pub fn as_tcp(&self) -> Option<SocketAddr> {
        match self {
            DomusAddr::Tcp(addr) => Some(*addr),
            _ => None,
        }
    }
}

impl fmt::Display for DomusAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DomusAddr::Tcp(addr) => write!(f, "tcp://{addr}"),
            DomusAddr::Socket(path) => write!(f, "unix://{}", path.to_string_lossy()),
        }
    }
}

impl fmt::Display for TransportKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportKind::Tcp => write!(f, "tcp"),
            TransportKind::Socket => write!(f, "socket"),
        }
    }
}
