// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use aurelia_ids::{
    AureliaError, ErrorId, MessageType, PeerMessageId, TabernaId, ERROR_MESSAGE_MAX_LEN,
};
use bytes::Buf;

pub const PROTOCOL_VERSION: u16 = 1;

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
    pub struct WireFlags: u16 {
        const BLOB = 0x0001;
        const RECONNECT = 0x0002;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HelloPayload {
    pub chunk_size: Option<u32>,
    pub ack_window_chunks: Option<u32>,
}

impl HelloPayload {
    pub const PRIMARY_LEN: usize = 0;
    pub const BLOB_LEN: usize = 8;

    pub fn to_bytes(self) -> Vec<u8> {
        match (self.chunk_size, self.ack_window_chunks) {
            (Some(chunk_size), Some(ack_window)) => {
                let mut out = Vec::with_capacity(Self::BLOB_LEN);
                out.extend_from_slice(&chunk_size.to_be_bytes());
                out.extend_from_slice(&ack_window.to_be_bytes());
                out
            }
            (None, None) => Vec::new(),
            _ => unreachable!("hello payload must be primary or blob"),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AureliaError> {
        match bytes.len() {
            Self::PRIMARY_LEN => Ok(Self {
                chunk_size: None,
                ack_window_chunks: None,
            }),
            Self::BLOB_LEN => {
                let mut buf = bytes;
                let chunk_size = read_u32(&mut buf, "invalid hello payload length")?;
                let ack_window = read_u32(&mut buf, "invalid hello payload length")?;
                Ok(Self {
                    chunk_size: Some(chunk_size),
                    ack_window_chunks: Some(ack_window),
                })
            }
            _ => Err(decode_failure("invalid hello payload length")),
        }
    }
}

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct BlobChunkFlags: u16 {
        const LAST_CHUNK = 0x0001;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobTransferStartPayload {
    pub request_msg_id: PeerMessageId,
}

impl BlobTransferStartPayload {
    pub const LEN: usize = 4;

    pub fn to_bytes(self) -> [u8; Self::LEN] {
        self.request_msg_id.to_be_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AureliaError> {
        if bytes.len() != Self::LEN {
            return Err(decode_failure("invalid blob start payload length"));
        }
        let mut buf = bytes;
        Ok(Self {
            request_msg_id: read_u32(&mut buf, "invalid blob start payload length")?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobTransferChunkPayload {
    pub request_msg_id: PeerMessageId,
    pub chunk_id: u64,
    pub flags: BlobChunkFlags,
    pub chunk: bytes::Bytes,
}

impl BlobTransferChunkPayload {
    pub const HEADER_LEN: usize = 4 + 8 + 2 + 4;

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::HEADER_LEN + self.chunk.len());
        out.extend_from_slice(&self.request_msg_id.to_be_bytes());
        out.extend_from_slice(&self.chunk_id.to_be_bytes());
        out.extend_from_slice(&self.flags.bits().to_be_bytes());
        out.extend_from_slice(&(self.chunk.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.chunk);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AureliaError> {
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
pub struct BlobTransferCompletePayload {
    pub request_msg_id: PeerMessageId,
}

impl BlobTransferCompletePayload {
    pub const LEN: usize = 4;

    pub fn to_bytes(self) -> [u8; Self::LEN] {
        self.request_msg_id.to_be_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AureliaError> {
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
pub struct ErrorPayload {
    pub error_id: u32,
    pub message: String,
}

impl ErrorPayload {
    pub fn new(error_id: u32, message: impl Into<String>) -> Self {
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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.message.len());
        out.extend_from_slice(&self.error_id.to_be_bytes());
        out.extend_from_slice(self.message.as_bytes());
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AureliaError> {
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
pub struct WireHeader {
    pub version: u16,
    pub flags: u16,
    pub msg_type: MessageType,
    pub peer_msg_id: PeerMessageId,
    pub src_taberna: TabernaId,
    pub dst_taberna: TabernaId,
    pub payload_len: u32,
}

impl WireHeader {
    pub const LEN: usize = 32;

    pub fn encode(&self) -> [u8; Self::LEN] {
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

    pub fn decode(buf: &[u8]) -> Result<Self, AureliaError> {
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
