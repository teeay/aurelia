// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use aurelia_ids::{
    AureliaError, ErrorId, MessageType, PeerMessageId, TabernaId, ERROR_MESSAGE_MAX_LEN,
};
use bytes::Buf;

pub(crate) const PROTOCOL_VERSION: u16 = 1;

fn decode_failure(message: impl Into<String>) -> AureliaError {
    AureliaError::with_message(ErrorId::DecodeFailure, message)
}

fn protocol_violation(message: impl Into<String>) -> AureliaError {
    AureliaError::with_message(ErrorId::ProtocolViolation, message)
}

fn unsupported_version() -> AureliaError {
    AureliaError::with_message(ErrorId::UnsupportedVersion, "unsupported protocol version")
}

fn read_u16(buf: &mut &[u8], message: &'static str) -> Result<u16, AureliaError> {
    if buf.remaining() < 2 {
        return Err(decode_failure(message));
    }
    Ok(buf.get_u16())
}

fn read_u32(buf: &mut &[u8], message: &'static str) -> Result<u32, AureliaError> {
    if buf.remaining() < 4 {
        return Err(decode_failure(message));
    }
    Ok(buf.get_u32())
}

fn read_u64(buf: &mut &[u8], message: &'static str) -> Result<u64, AureliaError> {
    if buf.remaining() < 8 {
        return Err(decode_failure(message));
    }
    Ok(buf.get_u64())
}

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) struct WireFlags: u16 {
        const BLOB = 0x0001;
        const RECONNECT = 0x0002;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HelloPayload {
    Primary,
    Blob {
        chunk_size: u32,
        ack_window_chunks: u32,
    },
}

impl HelloPayload {
    pub(crate) const PRIMARY_LEN: usize = 0;
    pub(crate) const BLOB_LEN: usize = 8;

    pub(crate) fn to_bytes(self) -> Vec<u8> {
        match self {
            Self::Blob {
                chunk_size,
                ack_window_chunks,
            } => {
                let mut out = Vec::with_capacity(Self::BLOB_LEN);
                out.extend_from_slice(&chunk_size.to_be_bytes());
                out.extend_from_slice(&ack_window_chunks.to_be_bytes());
                out
            }
            Self::Primary => Vec::new(),
        }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, AureliaError> {
        match bytes.len() {
            Self::PRIMARY_LEN => Ok(Self::Primary),
            Self::BLOB_LEN => {
                let mut buf = bytes;
                let chunk_size = read_u32(&mut buf, "invalid hello payload length")?;
                let ack_window_chunks = read_u32(&mut buf, "invalid hello payload length")?;
                Ok(Self::Blob {
                    chunk_size,
                    ack_window_chunks,
                })
            }
            _ => Err(decode_failure("invalid hello payload length")),
        }
    }
}

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) struct BlobChunkFlags: u16 {
        const LAST_CHUNK = 0x0001;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlobTransferChunkPayload {
    pub(crate) request_msg_id: PeerMessageId,
    pub(crate) chunk_id: u64,
    pub(crate) flags: BlobChunkFlags,
    pub(crate) chunk: bytes::Bytes,
}

impl BlobTransferChunkPayload {
    pub(crate) const HEADER_LEN: usize = 4 + 8 + 2 + 4;

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, AureliaError> {
        if bytes.len() < Self::HEADER_LEN {
            return Err(decode_failure("invalid blob chunk payload length"));
        }
        let mut buf = bytes;
        let request_msg_id = read_u32(&mut buf, "invalid blob chunk payload length")?;
        let chunk_id = read_u64(&mut buf, "invalid blob chunk payload length")?;
        let flags = read_u16(&mut buf, "invalid blob chunk payload length")?;
        let chunk_len = read_u32(&mut buf, "invalid blob chunk payload length")? as usize;
        if buf.remaining() != chunk_len {
            return Err(decode_failure("invalid blob chunk payload length"));
        }
        let flags = BlobChunkFlags::from_bits(flags).ok_or_else(|| {
            protocol_violation(format!("invalid blob chunk flags: 0x{:04x}", flags))
        })?;
        let chunk = bytes::Bytes::copy_from_slice(buf);
        Ok(Self {
            request_msg_id,
            chunk_id,
            flags,
            chunk,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlobTransferCompletePayload {
    pub(crate) request_msg_id: PeerMessageId,
}

impl BlobTransferCompletePayload {
    pub(crate) const LEN: usize = 4;

    pub(crate) fn to_bytes(self) -> [u8; Self::LEN] {
        self.request_msg_id.to_be_bytes()
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, AureliaError> {
        if bytes.len() != Self::LEN {
            return Err(decode_failure("invalid blob complete payload length"));
        }
        let mut buf = bytes;
        Ok(Self {
            request_msg_id: read_u32(&mut buf, "invalid blob complete payload length")?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ErrorPayload {
    pub(crate) error_id: u32,
    pub(crate) message: String,
}

impl ErrorPayload {
    pub(crate) fn new(error_id: u32, message: impl Into<String>) -> Self {
        let mut message = message.into();
        if message.len() > ERROR_MESSAGE_MAX_LEN {
            let mut idx = ERROR_MESSAGE_MAX_LEN;
            while idx > 0 && !message.is_char_boundary(idx) {
                idx -= 1;
            }
            message.truncate(idx);
        }
        Self { error_id, message }
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.message.len());
        out.extend_from_slice(&self.error_id.to_be_bytes());
        out.extend_from_slice(self.message.as_bytes());
        out
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, AureliaError> {
        if bytes.len() < 4 {
            return Err(decode_failure("invalid error payload length"));
        }
        let mut buf = bytes;
        let error_id = read_u32(&mut buf, "invalid error payload length")?;
        let message = std::str::from_utf8(buf)
            .map_err(|err| decode_failure(err.to_string()))?
            .to_string();
        Ok(Self { error_id, message })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WireHeader {
    pub(crate) version: u16,
    pub(crate) flags: u16,
    pub(crate) msg_type: MessageType,
    pub(crate) peer_msg_id: PeerMessageId,
    pub(crate) src_taberna: TabernaId,
    pub(crate) dst_taberna: TabernaId,
    pub(crate) payload_len: u32,
}

impl WireHeader {
    pub(crate) const LEN: usize = 32;

    pub(crate) fn encode(&self) -> [u8; Self::LEN] {
        // Keep encode as fixed-offset writes into a stack array. Decode benefits from
        // cursor-style reads; encode benefits from direct, allocation-free layout writes.
        let mut out = [0u8; Self::LEN];
        out[0..2].copy_from_slice(&self.version.to_be_bytes());
        out[2..4].copy_from_slice(&self.flags.to_be_bytes());
        out[4..8].copy_from_slice(&self.msg_type.to_be_bytes());
        out[8..12].copy_from_slice(&self.peer_msg_id.to_be_bytes());
        out[12..20].copy_from_slice(&self.src_taberna.to_be_bytes());
        out[20..28].copy_from_slice(&self.dst_taberna.to_be_bytes());
        out[28..32].copy_from_slice(&self.payload_len.to_be_bytes());
        out
    }

    pub(crate) fn decode(buf: &[u8]) -> Result<Self, AureliaError> {
        if buf.len() != Self::LEN {
            return Err(decode_failure("invalid wire header length"));
        }

        let mut buf = buf;
        let version = read_u16(&mut buf, "invalid wire header length")?;
        if version != PROTOCOL_VERSION {
            return Err(unsupported_version());
        }
        let flags = read_u16(&mut buf, "invalid wire header length")?;
        let msg_type = read_u32(&mut buf, "invalid wire header length")?;
        let peer_msg_id = read_u32(&mut buf, "invalid wire header length")?;
        let src_taberna = read_u64(&mut buf, "invalid wire header length")?;
        let dst_taberna = read_u64(&mut buf, "invalid wire header length")?;
        let payload_len = read_u32(&mut buf, "invalid wire header length")?;

        Ok(Self {
            version,
            flags,
            msg_type,
            peer_msg_id,
            src_taberna,
            dst_taberna,
            payload_len,
        })
    }
}
