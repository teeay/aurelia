// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::net::SocketAddr;
use tokio::net::TcpStream;

pub(super) fn verify_peer_cert_uri_inbound(
    stream: &tokio_rustls::server::TlsStream<TcpStream>,
    peer_addr: SocketAddr,
) -> Result<SocketAddr, AureliaError> {
    let (_, session) = stream.get_ref();
    let Some(certs) = session.peer_certificates() else {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    };
    let cert_addr = extract_peer_uri_san_addr(certs)?;
    if cert_addr.ip() != peer_addr.ip() {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    Ok(cert_addr)
}

pub(super) fn verify_peer_cert_uri_outbound(
    stream: &tokio_rustls::client::TlsStream<TcpStream>,
    expected: SocketAddr,
) -> Result<(), AureliaError> {
    let (_, session) = stream.get_ref();
    let Some(certs) = session.peer_certificates() else {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    };
    let cert_addr = extract_peer_uri_san_addr(certs)?;
    if cert_addr != expected {
        return Err(AureliaError::new(ErrorId::ProtocolViolation));
    }
    Ok(())
}

pub(super) fn extract_peer_uri_san_addr(
    certs: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
) -> Result<SocketAddr, AureliaError> {
    let cert = certs
        .first()
        .ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
    let (_, parsed) = x509_parser::parse_x509_certificate(cert.as_ref())
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    let san = parsed
        .subject_alternative_name()
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    let san = san.ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))?;
    let mut found: Option<SocketAddr> = None;
    for entry in san.value.general_names.iter() {
        if let x509_parser::extensions::GeneralName::URI(uri) = entry {
            if let Some(addr) = parse_aurelia_tcp_uri(uri)? {
                if let Some(existing) = found {
                    if existing != addr {
                        return Err(AureliaError::new(ErrorId::ProtocolViolation));
                    }
                } else {
                    found = Some(addr);
                }
            }
        }
    }
    found.ok_or_else(|| AureliaError::new(ErrorId::ProtocolViolation))
}

pub(super) fn parse_aurelia_tcp_uri(uri: &str) -> Result<Option<SocketAddr>, AureliaError> {
    const PREFIX: &str = "aurelia+tcp://";
    let Some(rest) = uri.strip_prefix(PREFIX) else {
        return Ok(None);
    };
    let addr = rest
        .parse::<SocketAddr>()
        .map_err(|_| AureliaError::new(ErrorId::ProtocolViolation))?;
    Ok(Some(addr))
}
