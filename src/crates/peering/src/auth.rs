// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

/// PKCS#8 mTLS material in DER form.
#[derive(Clone, Debug)]
pub struct Pkcs8DerConfig {
    /// DER-encoded trust-anchor certificate authority.
    pub ca_der: Vec<u8>,
    /// DER-encoded leaf certificate presented to peers.
    pub cert_der: Vec<u8>,
    /// DER-encoded PKCS#8 private key matching `cert_der`.
    pub pkcs8_key_der: Vec<u8>,
}

/// PKCS#8 mTLS material in PEM form.
#[derive(Clone, Debug)]
pub struct Pkcs8PemConfig {
    /// PEM-encoded trust-anchor certificate authority.
    pub ca_pem: Vec<u8>,
    /// PEM-encoded leaf certificate presented to peers.
    pub cert_pem: Vec<u8>,
    /// PEM-encoded PKCS#8 private key matching `cert_pem`.
    pub pkcs8_key_pem: Vec<u8>,
}

/// PKCS#8 mTLS material accepted by the TCP transport.
#[derive(Clone, Debug)]
pub enum Pkcs8AuthConfig {
    /// DER-encoded PKCS#8 material.
    Pkcs8Der(Pkcs8DerConfig),
    /// PEM-encoded PKCS#8 material.
    Pkcs8Pem(Pkcs8PemConfig),
}

/// Authentication material supplied when building a [`crate::Domus`].
#[derive(Clone, Debug)]
pub enum DomusAuthConfig {
    /// PKCS#8 mTLS configuration for the TCP transport.
    Pkcs8(Pkcs8AuthConfig),
}
