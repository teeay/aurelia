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
    let header = WireHeader {
        version: PROTOCOL_VERSION,
        flags,
        msg_type,
        peer_msg_id,
        src_taberna: 0,
        dst_taberna: 0,
        payload_len: payload.len() as u32,
    };
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
            let header = WireHeader {
                version: PROTOCOL_VERSION,
                flags: message.flags,
                msg_type: message.msg_type,
                peer_msg_id: message.peer_msg_id,
                src_taberna: message.src_taberna,
                dst_taberna: message.dst_taberna,
                payload_len: message.payload.len() as u32,
            };
            send_frame(stream, header, &message.payload).await
        }
        OutboundFrame::Control {
            msg_type,
            peer_msg_id,
            payload,
        } => {
            let header = WireHeader {
                version: PROTOCOL_VERSION,
                flags: 0,
                msg_type,
                peer_msg_id,
                src_taberna: 0,
                dst_taberna: 0,
                payload_len: payload.len() as u32,
            };
            send_frame(stream, header, payload.as_ref()).await
        }
        OutboundFrame::Close => Ok(()),
    }
}

pub(super) async fn send_frame(
    stream: &mut (impl AsyncWriteExt + Unpin),
    header: WireHeader,
    payload: &[u8],
) -> Result<(), AureliaError> {
    stream
        .write_all(&header.encode())
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::ConnectionLost, err.to_string()))?;
    if !payload.is_empty() {
        stream
            .write_all(payload)
            .await
            .map_err(|err| AureliaError::with_message(ErrorId::ConnectionLost, err.to_string()))?;
    }
    stream
        .flush()
        .await
        .map_err(|err| AureliaError::with_message(ErrorId::ConnectionLost, err.to_string()))?;
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
