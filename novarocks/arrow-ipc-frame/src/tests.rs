// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use arrow_ipc::{
    Endianness, KeyValue, KeyValueArgs, Message, MessageArgs, MessageHeader, MetadataVersion,
    Schema, SchemaArgs,
};
use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer};

fn prefix(length: u32) -> [u8; 8] {
    let mut output = [0; 8];
    output[..4].copy_from_slice(&CONTINUATION_MARKER);
    output[4..].copy_from_slice(&length.to_le_bytes());
    output
}

#[test]
fn fixed_prefix_rejects_truncation_bad_marker_and_checked_offset_overflow() {
    let valid = prefix(17);
    for end in 0..8 {
        assert_eq!(
            continuation_prefix(&valid[..end], 0),
            Err(PrefixError::Range(RangeError::OutOfBounds))
        );
    }
    for byte in 0..4 {
        let mut wrong = valid;
        wrong[byte] = 0;
        assert_eq!(
            continuation_prefix(&wrong, 0),
            Err(PrefixError::NotContinuation)
        );
    }
    assert_eq!(
        continuation_prefix(&valid, usize::MAX),
        Err(PrefixError::Range(RangeError::Overflow))
    );
    assert_eq!(
        continuation_prefix(&valid, 1),
        Err(PrefixError::Range(RangeError::OutOfBounds))
    );
    // This primitive reports a u32 declaration without admitting its extent
    // or enforcing the caller's signed-length / resource profile.
    assert_eq!(
        continuation_prefix(&prefix(u32::MAX), 0),
        Ok(ContinuationPrefix::Metadata {
            start: 8,
            len: u32::MAX as usize
        })
    );
}

#[test]
fn nonzero_offset_eos_preserves_next_position_and_leaves_trailing_profile_to_caller() {
    let mut bytes = vec![11, 12, 13, 14, 15];
    bytes.extend_from_slice(&prefix(0));
    assert_eq!(
        continuation_prefix(&bytes, 5),
        Ok(ContinuationPrefix::End { next_offset: 13 })
    );
    bytes.extend_from_slice(&[90, 91]);
    let ContinuationPrefix::End { next_offset } = continuation_prefix(&bytes, 5).unwrap() else {
        panic!("the actual prefix is EOS");
    };
    assert_eq!(next_offset, 13);
    assert_eq!(&bytes[next_offset..], &[90, 91]);
    assert_ne!(
        next_offset,
        bytes.len(),
        "the stream owner must reject these trailing bytes"
    );

    let mut declared = vec![7; 3];
    declared.extend_from_slice(&prefix(19));
    assert_eq!(
        continuation_prefix(&declared, 3),
        Ok(ContinuationPrefix::Metadata { start: 11, len: 19 })
    );
    assert_eq!(
        metadata_slice(&declared, 11, 19),
        Err(RangeError::OutOfBounds)
    );
}

#[test]
fn borrowed_metadata_and_body_ranges_check_exact_end_and_overflow_without_copying() {
    let bytes = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19];
    let metadata = metadata_slice(&bytes, 2, 3).unwrap();
    assert_eq!(metadata, &[12, 13, 14]);
    assert_eq!(metadata.as_ptr(), bytes[2..].as_ptr());
    let body = checked_range(bytes.len(), 5, 5).unwrap();
    assert_eq!(&bytes[body], &[15, 16, 17, 18, 19]);
    assert_eq!(checked_range(10, 10, 0), Ok(10..10));
    assert!(metadata_slice(&bytes, 10, 0).unwrap().is_empty());
    assert_eq!(checked_range(10, 11, 0), Err(RangeError::OutOfBounds));
    assert_eq!(checked_range(10, 5, 6), Err(RangeError::OutOfBounds));
    assert_eq!(metadata_slice(&bytes, 2, 9), Err(RangeError::OutOfBounds));
    assert_eq!(
        checked_range(usize::MAX, usize::MAX, 0),
        Ok(usize::MAX..usize::MAX)
    );
    assert_eq!(
        checked_range(usize::MAX, usize::MAX, 1),
        Err(RangeError::Overflow)
    );
    assert_eq!(
        metadata_slice(&bytes, usize::MAX, 1),
        Err(RangeError::Overflow)
    );
}

#[test]
fn signed_lengths_reject_negatives_and_keep_exact_positive_boundary() {
    for negative in [i64::MIN, -42, -1] {
        assert_eq!(nonnegative_length(negative), Err(InvalidLength));
    }
    assert_eq!(nonnegative_length(0), Ok(0));
    assert_eq!(nonnegative_length(71), Ok(71));
    match usize::try_from(i64::MAX) {
        Ok(maximum) => assert_eq!(nonnegative_length(i64::MAX), Ok(maximum)),
        Err(_) => assert_eq!(nonnegative_length(i64::MAX), Err(InvalidLength)),
    }
}

#[test]
fn explicit_alignment_keeps_near_overflow_and_rejects_invalid_values() {
    for (alignment, below, exact, above) in [
        (8, 7, 8, 9),
        (16, 15, 16, 17),
        (32, 31, 32, 33),
        (64, 63, 64, 65),
    ] {
        assert_eq!(align_up(0, alignment), Ok(0));
        assert_eq!(align_up(below, alignment), Ok(exact));
        assert_eq!(align_up(exact, alignment), Ok(exact));
        assert_eq!(align_up(above, alignment), Ok(2 * exact));
        let last_aligned = usize::MAX - (alignment - 1);
        assert_eq!(align_up(last_aligned, alignment), Ok(last_aligned));
        assert_eq!(
            align_up(last_aligned + 1, alignment),
            Err(AlignmentError::Overflow)
        );
        assert_eq!(
            align_up(usize::MAX, alignment),
            Err(AlignmentError::Overflow)
        );
    }
    assert_eq!(align_up(usize::MAX, 1), Ok(usize::MAX));
    for invalid in [0, 3, 6, 12, usize::MAX] {
        assert_eq!(align_up(0, invalid), Err(AlignmentError::InvalidAlignment));
    }
}

fn schema_message() -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let key = builder.create_string("fixture-owner");
    let value = builder.create_string("borrowed-metadata");
    let metadata = KeyValue::create(
        &mut builder,
        &KeyValueArgs {
            key: Some(key),
            value: Some(value),
        },
    );
    let metadata = builder.create_vector(&[metadata]);
    let schema = Schema::create(
        &mut builder,
        &SchemaArgs {
            endianness: Endianness::Little,
            fields: None,
            custom_metadata: Some(metadata),
            features: None,
        },
    );
    let message = Message::create(
        &mut builder,
        &MessageArgs {
            version: MetadataVersion::V5,
            header_type: MessageHeader::Schema,
            header: Some(schema.as_union_value()),
            bodyLength: 0,
            custom_metadata: None,
        },
    );
    arrow_ipc::finish_message_buffer(&mut builder, message);
    builder.finished_data().to_vec()
}

fn verifier_options(depth: usize, tables: usize, apparent: usize) -> VerifierOptions {
    VerifierOptions {
        max_depth: depth,
        max_tables: tables,
        max_apparent_size: apparent,
        ignore_missing_null_terminator: false,
    }
}

#[test]
fn actual_arrow_schema_message_is_borrowed_and_uses_exact_depth_and_table_limits() {
    let metadata = schema_message();
    let mut frame = vec![77; 5];
    frame.extend_from_slice(&prefix(u32::try_from(metadata.len()).unwrap()));
    frame.extend_from_slice(&metadata);
    let ContinuationPrefix::Metadata { start, len } = continuation_prefix(&frame, 5).unwrap()
    else {
        panic!("the frame contains a real schema message");
    };
    let borrowed = metadata_slice(&frame, start, len).unwrap();
    assert_eq!(borrowed.as_ptr(), frame[start..].as_ptr());
    let options = verifier_options(3, 3, metadata.len() * 8);
    let message = verified_message(borrowed, &options).unwrap();
    assert_eq!(message.version(), MetadataVersion::V5);
    assert_eq!(message.header_type(), MessageHeader::Schema);
    assert_eq!(message.bodyLength(), 0);
    let schema = message.header_as_schema().unwrap();
    assert_eq!(schema.endianness(), Endianness::Little);
    let entry = schema.custom_metadata().unwrap().get(0);
    assert_eq!(entry.key(), Some("fixture-owner"));
    assert_eq!(entry.value(), Some("borrowed-metadata"));
    for text in [entry.key().unwrap(), entry.value().unwrap()] {
        let relative = borrowed
            .windows(text.len())
            .position(|candidate| candidate == text.as_bytes())
            .unwrap();
        assert_eq!(text.as_ptr(), borrowed[relative..].as_ptr());
    }
    // Actual tables are Message -> Schema -> KeyValue; no guessed default
    // verifier policy or decoded Arrow schema is introduced by this owner.
    assert!(matches!(
        verified_message(borrowed, &verifier_options(2, 3, metadata.len() * 8)),
        Err(InvalidFlatbuffer::DepthLimitReached)
    ));
    assert!(matches!(
        verified_message(borrowed, &verifier_options(3, 2, metadata.len() * 8)),
        Err(InvalidFlatbuffer::TooManyTables)
    ));
    assert!(verified_message(&borrowed[..4], &options).is_err());
}

#[test]
fn explicit_apparent_size_matches_official_verifier_at_the_actual_boundary() {
    let metadata = schema_message();
    // Discover this independently authored fixture's threshold with the
    // official Arrow verifier, not the wrapper or a duplicate verifier model.
    // Every trial has explicit finite limits; these are not production defaults.
    let mut low = 0;
    let mut high = metadata.len() * 8;
    assert!(arrow_ipc::root_as_message_with_opts(&verifier_options(3, 3, high), &metadata).is_ok());
    while low < high {
        let middle = low + (high - low) / 2;
        match arrow_ipc::root_as_message_with_opts(&verifier_options(3, 3, middle), &metadata) {
            Ok(_) => high = middle,
            Err(InvalidFlatbuffer::ApparentSizeTooLarge) => low = middle + 1,
            Err(error) => panic!("unexpected official verifier error: {error}"),
        }
    }
    assert!(low > 0);
    assert!(matches!(
        verified_message(&metadata, &verifier_options(3, 3, low - 1)),
        Err(InvalidFlatbuffer::ApparentSizeTooLarge)
    ));
    let message = verified_message(&metadata, &verifier_options(3, 3, low)).unwrap();
    assert_eq!(
        message
            .header_as_schema()
            .unwrap()
            .custom_metadata()
            .unwrap()
            .get(0)
            .value(),
        Some("borrowed-metadata")
    );
    assert!(verified_message(&metadata, &verifier_options(3, 3, low + 1)).is_ok());
}
