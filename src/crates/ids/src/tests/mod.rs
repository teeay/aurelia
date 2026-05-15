// This file is part of the Aurelia workspace.
// SPDX-FileCopyrightText: 2026 Zivatar Limited
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn a3_message_type_uses_application_range() {
    assert_eq!(a3_message_type(0), 0x0100_0000);
    assert_eq!(a3_message_type(100), 0x0100_0064);
    assert_eq!(
        a3_message_type(A3_MESSAGE_TYPE_MAX_OFFSET),
        MessageType::MAX
    );
}

#[test]
fn try_a3_message_type_rejects_out_of_range_offsets() {
    assert_eq!(try_a3_message_type(A3_MESSAGE_TYPE_MAX_OFFSET + 1), None);
}

#[test]
fn classify_message_priority_uses_documented_ranges() {
    assert_eq!(
        classify_message_priority(0x0000_0000),
        MessagePriorityClass::A1
    );
    assert_eq!(
        classify_message_priority(A1_MESSAGE_TYPE_MAX),
        MessagePriorityClass::A1
    );
    assert_eq!(
        classify_message_priority(A2_MESSAGE_TYPE_BASE),
        MessagePriorityClass::A2
    );
    assert_eq!(
        classify_message_priority(A3_MESSAGE_TYPE_BASE - 1),
        MessagePriorityClass::A2
    );
    assert_eq!(
        classify_message_priority(A3_MESSAGE_TYPE_BASE),
        MessagePriorityClass::A3
    );
    assert_eq!(
        classify_message_priority(MessageType::MAX),
        MessagePriorityClass::A3
    );
}

#[test]
fn error_id_all_round_trips_numeric_discriminants() {
    for id in ErrorId::ALL {
        assert_eq!(ErrorId::try_from(id.as_u32()), Ok(*id));
    }
    assert!(ErrorId::try_from(0).is_err());
    assert!(ErrorId::try_from(u32::MAX).is_err());
}
