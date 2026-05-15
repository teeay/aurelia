# Peering Threat Modeling

Status: Developed

## Objectives

- Document accepted security limitations in A1 peering.
- Record mitigation guidance for known out-of-scope controls.

## Technical Details

### Risk Register

| ID | Risk | Impact | Conditions | Status | Mitigation / Notes |
| --- | --- | --- | --- | --- | --- |
| TR-001 | Socket bind remove/bind window can fail if a path is recreated between steps. | Startup bind fails with `PeerUnavailable`; no privilege escalation. | Another process can write to the socket directory and creates a file between remove and bind. | Accepted | Process orchestration is out of scope for A1. Use filesystem permissions to restrict writers for the socket directory. |
| TR-002 | Certificate revocation (CRL/OCSP) is not checked for mTLS or socket auth. | Compromised cert remains valid until expiry. In-flight TLS sessions established with a since-rotated cert continue until the socket closes. | Any deployment relying on revocation for transport identity. | Accepted | Rotate certs at any time without disruption (smooth rotation); prefer short-lived certs; re-issue on suspected compromise. |
| TR-003 | A1 does not pin per-peer certificates. Any party that completes a valid mTLS / socket-auth handshake against the configured CA is accepted as the peer at the resolved address. | Across callis to the same peer address, distinct certificates are admitted. | Deployments that need narrower per-peer identity binding than CA scope. | Accepted | Tightly scope the CA used for peering; prefer short-lived certs and per-peer CA bundles when narrower binding is required. |

### Certificate Revocation (CRL/OCSP)

Peering does not integrate CRL or OCSP checks for TCP mTLS or socket authentication. Certificate
revocation status is not evaluated; a certificate remains valid until it expires. In-flight TLS
sessions established with a since-rotated certificate continue until the socket closes.

Risk acceptance:

- Revocation is an explicit non-objective for the current transport scope.
- This is a known limitation for deployments that rely on short-lived transport identities.

Mitigations:

- Prefer short-lived certificates and automated rotation; rotation via `Transport::reload_auth` is
  non-disruptive (smooth rotation), so it can be applied freely.
- Re-issue credentials when compromise is suspected.
- Ensure CA distribution and rotation processes can invalidate compromised identities promptly.

### No Per-Peer Certificate Pinning

A1 does not pin peer certificates across callis. Each callis is admitted on its own A0
authentication. Any party that completes a valid mTLS / socket-auth handshake against the
configured CA is accepted as the peer at the resolved address.

Risk acceptance:

- Smooth certificate rotation across callis is an explicit objective.
- Per-peer identity binding is delegated to CA scope.

Mitigations:

- Tightly scope the CA used for peering.
- Prefer per-peer CA bundles when narrower binding is required.

<!--
This file is part of the Aurelia workspace.
SPDX-FileCopyrightText: 2026 Zivatar Limited
SPDX-License-Identifier: Apache-2.0
-->
