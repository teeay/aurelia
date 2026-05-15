// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;

fn encode_blob_chunk_payload(payload: &BlobTransferChunkPayload) -> Vec<u8> {
    let mut out = Vec::with_capacity(BlobTransferChunkPayload::HEADER_LEN + payload.chunk.len());
    out.extend_from_slice(&payload.request_msg_id.to_be_bytes());
    out.extend_from_slice(&payload.chunk_id.to_be_bytes());
    out.extend_from_slice(&payload.flags.bits().to_be_bytes());
    out.extend_from_slice(&(payload.chunk.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload.chunk);
    out
}

use crate::wire::{BlobChunkFlags, BlobTransferChunkPayload, HelloPayload, WireFlags};
use aurelia_ids::ErrorId;

#[test]
fn wire_flags_values_are_stable() {
    assert_eq!(WireFlags::BLOB.bits(), 0x0001);
    assert_eq!(WireFlags::RECONNECT.bits(), 0x0002);
}

#[test]
fn hello_payload_roundtrip_primary() {
    let payload = HelloPayload::Primary;
    let bytes = payload.to_bytes();
    assert!(bytes.is_empty());
    let decoded = HelloPayload::from_bytes(&bytes).expect("decode primary");
    assert_eq!(decoded, payload);
}

#[test]
fn hello_payload_roundtrip_blob() {
    let payload = HelloPayload::Blob {
        chunk_size: 1200,
        ack_window_chunks: 64,
    };
    let bytes = payload.to_bytes();
    assert_eq!(bytes.len(), HelloPayload::BLOB_LEN);
    let decoded = HelloPayload::from_bytes(&bytes).expect("decode blob");
    assert_eq!(decoded, payload);
}

#[test]
fn hello_payload_rejects_invalid_length() {
    let bytes = vec![0u8; 3];
    assert!(HelloPayload::from_bytes(&bytes).is_err());
}

#[test]
fn blob_transfer_chunk_roundtrip() {
    let payload = BlobTransferChunkPayload {
        request_msg_id: 7,
        chunk_id: 2,
        flags: BlobChunkFlags::LAST_CHUNK,
        chunk: Bytes::from_static(b"abc"),
    };
    let bytes = encode_blob_chunk_payload(&payload);
    let decoded = BlobTransferChunkPayload::from_bytes(&bytes).expect("decode chunk");
    assert_eq!(decoded, payload);
}

#[test]
fn blob_transfer_chunk_rejects_invalid_length() {
    let payload = BlobTransferChunkPayload {
        request_msg_id: 1,
        chunk_id: 0,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"abc"),
    };
    let mut bytes = encode_blob_chunk_payload(&payload);
    bytes.pop();
    assert!(BlobTransferChunkPayload::from_bytes(&bytes).is_err());
}

#[test]
fn blob_transfer_chunk_rejects_unknown_flags() {
    let payload = BlobTransferChunkPayload {
        request_msg_id: 7,
        chunk_id: 2,
        flags: BlobChunkFlags::empty(),
        chunk: Bytes::from_static(b"abc"),
    };
    let mut bytes = encode_blob_chunk_payload(&payload);
    bytes[12] = 0x80;
    bytes[13] = 0x00;
    let err = BlobTransferChunkPayload::from_bytes(&bytes).expect_err("expected invalid flags");
    assert_eq!(err.kind, ErrorId::ProtocolViolation);
}
