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

use std::sync::Arc;

use bytes::Bytes;
use novarocks::state_store::{
    ChangeCursor, ContinuationToken, Direction, Key, KeyRange, RangeRequest, ReadTransaction,
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreLimits, StoreRevision, Value,
    VersionToken, WriteTransaction,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

fn key(bytes: &'static [u8]) -> Key {
    Key::try_from(Bytes::from_static(bytes)).expect("valid key")
}

fn assert_object_safe(_: Arc<dyn StateStore>) {}
fn assert_read_object_safe(_: Box<dyn ReadTransaction>) {}
fn assert_write_object_safe(_: Box<dyn WriteTransaction>) {}

#[test]
fn contract_accepts_binary_payloads_and_rejects_invalid_ranges() {
    let binary = key(&[0, 255]);
    assert_eq!(binary.as_bytes(), &[0, 255]);
    assert_eq!(
        Value::try_from(Bytes::from_static(&[255, 0]))
            .expect("binary value")
            .as_bytes(),
        &[255, 0]
    );
    assert_eq!(
        VersionToken::try_from(Bytes::from_static(&[0, 255]))
            .expect("binary version")
            .as_bytes(),
        &[0, 255]
    );

    for (start, end) in [(key(&[1]), key(&[1])), (key(&[2]), key(&[1]))] {
        let error = KeyRange::new(start, end).expect_err("range must be increasing");
        assert_eq!(error.kind(), StateStoreErrorKind::InvalidRequest);
    }
}

#[test]
fn contract_enforces_common_binary_and_page_bounds() {
    let limits = StateStoreLimits::default();
    assert_eq!(
        Key::try_from(Bytes::from(vec![0; limits.max_key_bytes + 1]))
            .expect_err("oversized key")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    assert_eq!(
        Value::try_from(Bytes::from(vec![0; limits.max_value_bytes + 1]))
            .expect_err("oversized value")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    assert_eq!(
        StoreRevision::try_from(Bytes::new())
            .expect_err("empty revision")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    for page_size in [0, limits.max_page_size + 1] {
        let request = RangeRequest {
            range: KeyRange::new(key(&[]), key(&[255])).expect("range"),
            direction: Direction::Forward,
            page_size,
            continuation: None,
        };
        assert_eq!(
            request
                .validate(&limits)
                .expect_err("invalid page size")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
    }
}

#[test]
fn contract_prefix_range_requires_a_finite_successor() {
    let range = KeyRange::for_prefix(key(&[0, 255])).expect("finite prefix successor");
    assert_eq!(range.start.as_bytes(), &[0, 255]);
    assert_eq!(range.end.as_bytes(), &[1]);

    let error = KeyRange::for_prefix(key(&[255, 255])).expect_err("all-ff has no successor");
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidRequest);
}

#[test]
fn contract_continuation_binds_range_and_direction() {
    let forward = RangeRequest {
        range: KeyRange::new(key(&[0]), key(&[2])).expect("range"),
        direction: Direction::Forward,
        page_size: 10,
        continuation: None,
    };
    let reverse = RangeRequest {
        direction: Direction::Reverse,
        ..forward.clone()
    };
    let other_range = RangeRequest {
        range: KeyRange::new(key(&[0]), key(&[3])).expect("range"),
        ..forward.clone()
    };

    let token = forward.continuation_after(&key(&[1])).expect("token");
    assert_eq!(
        token.resume_after(&forward).expect("matching request"),
        key(&[1])
    );
    assert_eq!(
        token
            .resume_after(&reverse)
            .expect_err("direction mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert_eq!(
        token
            .resume_after(&other_range)
            .expect_err("range mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[test]
fn contract_codecs_reject_malformed_and_mismatched_tokens() {
    let request = RangeRequest {
        range: KeyRange::new(key(&[]), key(&[255])).expect("range"),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let token = request.continuation_after(&key(&[0, 255])).expect("token");
    let mut trailing = token.as_bytes().to_vec();
    trailing.push(0);
    assert_eq!(
        novarocks::state_store::ContinuationToken::try_from(Bytes::from(trailing))
            .expect("opaque token")
            .resume_after(&request)
            .expect_err("trailing bytes")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    let store_id = Uuid::now_v7();
    let revision = StoreRevision::try_from(Bytes::from_static(&[255, 255])).expect("revision");
    let cursor = ChangeCursor::new(store_id, revision.clone(), 42).expect("cursor");
    let (decoded_revision, sequence) = cursor.decode(store_id).expect("matching store");
    assert_eq!(decoded_revision, revision);
    assert_eq!(sequence, 42);
    assert_eq!(
        cursor
            .decode(Uuid::now_v7())
            .expect_err("store mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );

    let mut trailing = cursor.as_bytes().to_vec();
    trailing.push(0);
    assert_eq!(
        ChangeCursor::try_from(Bytes::from(trailing))
            .expect("opaque cursor")
            .decode(store_id)
            .expect_err("trailing bytes")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
}

#[test]
fn contract_continuation_codec_has_the_stable_v1_layout() {
    let request = RangeRequest {
        range: KeyRange::new(key(&[0, 255]), key(&[2])).expect("range"),
        direction: Direction::Reverse,
        page_size: 7,
        continuation: None,
    };
    let token = request.continuation_after(&key(&[1, 0])).expect("token");
    let encoded = token.as_bytes();

    let expected_fingerprint = Sha256::digest(
        [
            &[1, 1][..],
            &2_u32.to_be_bytes(),
            &[0, 255],
            &1_u32.to_be_bytes(),
            &[2],
        ]
        .concat(),
    );
    assert_eq!(&encoded[..2], &[1, 1]);
    assert_eq!(&encoded[2..34], expected_fingerprint.as_slice());
    assert_eq!(&encoded[34..38], &2_u32.to_be_bytes());
    assert_eq!(&encoded[38..], &[1, 0]);

    for malformed in [
        Bytes::new(),
        Bytes::from_static(&[2, 1]),
        Bytes::copy_from_slice(&encoded[..encoded.len() - 1]),
    ] {
        assert_eq!(
            ContinuationToken::try_from(malformed)
                .expect("opaque token")
                .resume_after(&request)
                .expect_err("malformed token")
                .kind(),
            StateStoreErrorKind::InvalidRequest
        );
    }
}

#[test]
fn contract_change_cursor_has_the_stable_v1_layout() {
    let store_id = Uuid::from_bytes([7; 16]);
    let revision = StoreRevision::try_from(Bytes::from_static(&[0, 255])).expect("revision");
    let cursor = ChangeCursor::new(store_id, revision, 0x01020304).expect("cursor");
    let encoded = cursor.as_bytes();

    assert_eq!(encoded[0], 1);
    assert_eq!(&encoded[1..17], store_id.as_bytes());
    assert_eq!(&encoded[17..21], &2_u32.to_be_bytes());
    assert_eq!(&encoded[21..23], &[0, 255]);
    assert_eq!(&encoded[23..27], &0x01020304_u32.to_be_bytes());

    for malformed in [
        Bytes::new(),
        Bytes::from_static(&[2]),
        Bytes::copy_from_slice(&encoded[..encoded.len() - 1]),
    ] {
        assert_eq!(
            ChangeCursor::try_from(malformed)
                .expect("opaque cursor")
                .decode(store_id)
                .expect_err("malformed cursor")
                .kind(),
            StateStoreErrorKind::InvalidRequest
        );
    }
}

#[test]
fn contract_error_surface_is_typed_and_provider_neutral() {
    let kinds = [
        StateStoreErrorKind::InvalidRequest,
        StateStoreErrorKind::InvalidConfiguration,
        StateStoreErrorKind::UnsupportedDeployment,
        StateStoreErrorKind::LimitExceeded,
        StateStoreErrorKind::DeadlineExceeded,
        StateStoreErrorKind::PreconditionFailed,
        StateStoreErrorKind::Conflict,
        StateStoreErrorKind::Transient,
        StateStoreErrorKind::Corruption,
        StateStoreErrorKind::ProviderUnavailable,
        StateStoreErrorKind::Cancelled,
        StateStoreErrorKind::Internal,
    ];
    for kind in kinds {
        let error = StateStoreError::new(kind, "state store operation failed");
        assert_eq!(error.kind(), kind);
        assert!(!error.to_string().contains("SELECT"));
        assert!(!error.to_string().contains("password"));
    }
}

#[test]
fn contract_traits_are_object_safe() {
    let _ = assert_object_safe as fn(Arc<dyn StateStore>);
    let _ = assert_read_object_safe as fn(Box<dyn ReadTransaction>);
    let _ = assert_write_object_safe as fn(Box<dyn WriteTransaction>);
}
