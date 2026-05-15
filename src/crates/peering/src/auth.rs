// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use zeroize::Zeroizing;

/// PKCS#8 private-key bytes that zeroize on drop after ownership is transferred.
pub struct Pkcs8PrivateKey {
    inner: Zeroizing<Vec<u8>>,
}

impl Pkcs8PrivateKey {
    /// Wraps private-key bytes so this value zeroizes them when dropped.
    ///
    /// Copies or buffers retained before constructing this value remain the caller's
    /// responsibility. Callers that already hold `Zeroizing<Vec<u8>>` can convert it directly.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            inner: Zeroizing::new(bytes),
        }
    }

    pub(crate) fn into_zeroizing(self) -> Zeroizing<Vec<u8>> {
        self.inner
    }
}

impl From<Vec<u8>> for Pkcs8PrivateKey {
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

impl From<Zeroizing<Vec<u8>>> for Pkcs8PrivateKey {
    fn from(value: Zeroizing<Vec<u8>>) -> Self {
        Self { inner: value }
    }
}

impl fmt::Debug for Pkcs8PrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Pkcs8PrivateKey(<redacted>)")
    }
}

/// PKCS#8 mTLS material in DER form.
pub struct Pkcs8DerConfig {
    /// DER-encoded trust-anchor certificate authority.
    pub ca_der: Vec<u8>,
    /// DER-encoded leaf certificate presented to peers.
    pub cert_der: Vec<u8>,
    /// DER-encoded PKCS#8 private key matching `cert_der`.
    pub pkcs8_key_der: Pkcs8PrivateKey,
}

impl fmt::Debug for Pkcs8DerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pkcs8DerConfig")
            .field("ca_der_len", &self.ca_der.len())
            .field("cert_der_len", &self.cert_der.len())
            .field("pkcs8_key_der", &self.pkcs8_key_der)
            .finish()
    }
}

/// PKCS#8 mTLS material in PEM form.
pub struct Pkcs8PemConfig {
    /// PEM-encoded trust-anchor certificate authority.
    pub ca_pem: Vec<u8>,
    /// PEM-encoded leaf certificate presented to peers.
    pub cert_pem: Vec<u8>,
    /// PEM-encoded PKCS#8 private key matching `cert_pem`.
    pub pkcs8_key_pem: Pkcs8PrivateKey,
}

impl fmt::Debug for Pkcs8PemConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pkcs8PemConfig")
            .field("ca_pem_len", &self.ca_pem.len())
            .field("cert_pem_len", &self.cert_pem.len())
            .field("pkcs8_key_pem", &self.pkcs8_key_pem)
            .finish()
    }
}

/// PKCS#8 mTLS material supplied when building a [`crate::Domus`].
pub enum Pkcs8AuthConfig {
    /// DER-encoded PKCS#8 material.
    Pkcs8Der(Pkcs8DerConfig),
    /// PEM-encoded PKCS#8 material.
    Pkcs8Pem(Pkcs8PemConfig),
}

impl fmt::Debug for Pkcs8AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pkcs8Der(config) => f.debug_tuple("Pkcs8Der").field(config).finish(),
            Self::Pkcs8Pem(config) => f.debug_tuple("Pkcs8Pem").field(config).finish(),
        }
    }
}
