// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) async fn send_control_frame(
    stream: &mut (impl AsyncWriteExt + Unpin),
    msg_type: MessageType,
    flags: u16,
    peer_msg_id: PeerMessageId,
    payload: &[u8],
) -> Result<(), AureliaError> {
    let header = control_header(msg_type, flags, peer_msg_id, payload.len())?;
    send_frame(stream, header, payload).await
}

pub(super) async fn send_outbound_frame(
    stream: &mut (impl AsyncWriteExt + Unpin),
    frame: OutboundFrame,
) -> Result<(), AureliaError> {
    match frame {
        OutboundFrame::Ack { peer_msg_id } => {
            let header = WireHeader {
                version: PROTOCOL_VERSION,
                flags: 0,
                msg_type: MSG_ACK,
                peer_msg_id,
                src_taberna: 0,
                dst_taberna: 0,
                payload_len: 0,
            };
            send_frame(stream, header, &[]).await
        }
        OutboundFrame::Message(message) => {
            let payload_len = wire_payload_len(message.payload.len())?;
            let header = WireHeader {
                version: PROTOCOL_VERSION,
                flags: message.flags,
                msg_type: message.msg_type,
                peer_msg_id: message.peer_msg_id,
                src_taberna: message.src_taberna,
                dst_taberna: message.dst_taberna,
                payload_len,
            };
            send_frame(stream, header, &message.payload).await
        }
        OutboundFrame::Control {
            msg_type,
            peer_msg_id,
            payload,
        } => {
            let header = control_header(msg_type, 0, peer_msg_id, payload.len())?;
            send_frame(stream, header, payload.as_ref()).await
        }
    }
}

pub(super) async fn send_blob_chunk_frame(
    stream: &mut (impl AsyncWriteExt + Unpin),
    peer_msg_id: PeerMessageId,
    request_msg_id: PeerMessageId,
    chunk_id: u64,
    flags: BlobChunkFlags,
    chunk: &Bytes,
) -> Result<(), AureliaError> {
    let connection_lost =
        |err: std::io::Error| AureliaError::with_message(ErrorId::ConnectionLost, err.to_string());
    let payload_len = BlobTransferChunkPayload::HEADER_LEN + chunk.len();
    let header = control_header(MSG_BLOB_TRANSFER_CHUNK, 0, peer_msg_id, payload_len)?;
    let chunk_len = wire_payload_len(chunk.len())?;
    let mut inner = [0u8; BlobTransferChunkPayload::HEADER_LEN];
    inner[0..4].copy_from_slice(&request_msg_id.to_be_bytes());
    inner[4..12].copy_from_slice(&chunk_id.to_be_bytes());
    inner[12..14].copy_from_slice(&flags.bits().to_be_bytes());
    inner[14..18].copy_from_slice(&chunk_len.to_be_bytes());

    stream
        .write_all(&header.encode())
        .await
        .map_err(connection_lost)?;
    stream.write_all(&inner).await.map_err(connection_lost)?;
    if !chunk.is_empty() {
        stream.write_all(chunk).await.map_err(connection_lost)?;
    }
    stream.flush().await.map_err(connection_lost)?;
    Ok(())
}

fn control_header(
    msg_type: MessageType,
    flags: u16,
    peer_msg_id: PeerMessageId,
    payload_len: usize,
) -> Result<WireHeader, AureliaError> {
    Ok(WireHeader {
        version: PROTOCOL_VERSION,
        flags,
        msg_type,
        peer_msg_id,
        src_taberna: 0,
        dst_taberna: 0,
        payload_len: wire_payload_len(payload_len)?,
    })
}

pub(crate) fn wire_payload_len(payload_len: usize) -> Result<u32, AureliaError> {
    payload_len.try_into().map_err(|_| {
        AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "payload length exceeds wire header capacity",
        )
    })
}

pub(super) async fn send_frame(
    stream: &mut (impl AsyncWriteExt + Unpin),
    header: WireHeader,
    payload: &[u8],
) -> Result<(), AureliaError> {
    let connection_lost =
        |err: std::io::Error| AureliaError::with_message(ErrorId::ConnectionLost, err.to_string());
    stream
        .write_all(&header.encode())
        .await
        .map_err(connection_lost)?;
    if !payload.is_empty() {
        stream.write_all(payload).await.map_err(connection_lost)?;
    }
    stream.flush().await.map_err(connection_lost)?;
    Ok(())
}

pub(super) async fn read_frame(
    stream: &mut (impl AsyncReadExt + Unpin),
    max_payload_len: usize,
) -> Result<Option<(WireHeader, Vec<u8>)>, AureliaError> {
    let mut header_buf = [0u8; WireHeader::LEN];
    match stream.read_exact(&mut header_buf).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => {
            return Err(AureliaError::with_message(
                ErrorId::ConnectionLost,
                err.to_string(),
            ));
        }
    }
    let header = WireHeader::decode(&header_buf)?;
    let payload_len = header.payload_len as usize;
    if payload_len > max_payload_len {
        return Err(AureliaError::with_message(
            ErrorId::ProtocolViolation,
            "payload length exceeds max",
        ));
    }
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|err| AureliaError::with_message(ErrorId::ConnectionLost, err.to_string()))?;
    }
    Ok(Some((header, payload)))
}

pub(super) struct FrameReadState {
    header_buf: [u8; WireHeader::LEN],
    header_len: usize,
    payload_header: Option<WireHeader>,
    payload_buf: Vec<u8>,
    payload_len: usize,
}

impl Default for FrameReadState {
    fn default() -> Self {
        Self {
            header_buf: [0; WireHeader::LEN],
            header_len: 0,
            payload_header: None,
            payload_buf: Vec::new(),
            payload_len: 0,
        }
    }
}

impl FrameReadState {
    pub(super) async fn read_next(
        &mut self,
        stream: &mut (impl AsyncReadExt + Unpin),
        max_payload_len: usize,
    ) -> Result<Option<(WireHeader, Vec<u8>)>, AureliaError> {
        if self.payload_header.is_none() {
            while self.header_len < WireHeader::LEN {
                let n = stream
                    .read(&mut self.header_buf[self.header_len..])
                    .await
                    .map_err(|err| {
                        AureliaError::with_message(ErrorId::ConnectionLost, err.to_string())
                    })?;
                if n == 0 {
                    if self.header_len == 0 {
                        return Ok(None);
                    }
                    return Err(AureliaError::with_message(
                        ErrorId::ConnectionLost,
                        "unexpected EOF while reading frame header",
                    ));
                }
                self.header_len += n;
            }

            let header = WireHeader::decode(&self.header_buf)?;
            let payload_len = header.payload_len as usize;
            if payload_len > max_payload_len {
                return Err(AureliaError::with_message(
                    ErrorId::ProtocolViolation,
                    "payload length exceeds max",
                ));
            }
            self.header_len = 0;
            if payload_len == 0 {
                return Ok(Some((header, Vec::new())));
            }
            self.payload_header = Some(header);
            self.payload_buf = vec![0; payload_len];
            self.payload_len = 0;
        }

        while self.payload_len < self.payload_buf.len() {
            let n = stream
                .read(&mut self.payload_buf[self.payload_len..])
                .await
                .map_err(|err| {
                    AureliaError::with_message(ErrorId::ConnectionLost, err.to_string())
                })?;
            if n == 0 {
                return Err(AureliaError::with_message(
                    ErrorId::ConnectionLost,
                    "unexpected EOF while reading frame payload",
                ));
            }
            self.payload_len += n;
        }

        let header = self
            .payload_header
            .take()
            .expect("payload header exists while reading payload");
        let payload = std::mem::take(&mut self.payload_buf);
        self.payload_len = 0;
        Ok(Some((header, payload)))
    }
}
