// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use rustls_pki_types::{pem::PemObject, CertificateDer, PrivatePkcs8KeyDer};
use zeroize::Zeroizing;

use crate::auth::{Pkcs8AuthConfig, Pkcs8DerConfig, Pkcs8PemConfig};
use aurelia_ids::{AureliaError, ErrorId};

pub(crate) struct Pkcs8AuthMaterial {
    pub(crate) roots: Vec<CertificateDer<'static>>,
    pub(crate) certs: Vec<CertificateDer<'static>>,
    pub(crate) key_der: Zeroizing<Vec<u8>>,
}

pub(crate) fn parse_pkcs8_auth_material(
    auth: Pkcs8AuthConfig,
) -> Result<Pkcs8AuthMaterial, AureliaError> {
    match auth {
        Pkcs8AuthConfig::Pkcs8Der(Pkcs8DerConfig {
            ca_der,
            cert_der,
            pkcs8_key_der,
        }) => Ok(Pkcs8AuthMaterial {
            roots: vec![CertificateDer::from(ca_der)],
            certs: vec![CertificateDer::from(cert_der)],
            key_der: Zeroizing::new(pkcs8_key_der),
        }),
        Pkcs8AuthConfig::Pkcs8Pem(Pkcs8PemConfig {
            ca_pem,
            cert_pem,
            pkcs8_key_pem,
        }) => {
            let key_pem = Zeroizing::new(pkcs8_key_pem);
            let ca_certs = CertificateDer::pem_slice_iter(&ca_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
            let certs = CertificateDer::pem_slice_iter(&cert_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
            let mut keys = PrivatePkcs8KeyDer::pem_slice_iter(&key_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
            let key = keys
                .pop()
                .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
            if !keys.is_empty() || ca_certs.is_empty() || certs.is_empty() {
                return Err(AureliaError::new(ErrorId::ProtocolViolation));
            }
            Ok(Pkcs8AuthMaterial {
                roots: ca_certs,
                certs,
                key_der: Zeroizing::new(key.secret_pkcs8_der().to_vec()),
            })
        }
    }
}
