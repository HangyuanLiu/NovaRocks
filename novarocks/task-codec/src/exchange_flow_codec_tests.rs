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
use novarocks_execution_contract::TaskIdentity;
use novarocks_proto_codec::ProtocolErrorKind;
use novarocks_types::identity::{
    AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};

fn task(attempt: u64, ordinal: u32) -> TaskIdentity {
    TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(attempt).unwrap()).unwrap(),
        StageId::new(ordinal).unwrap(),
        TaskId::new(ordinal).unwrap(),
        BackendProcessId::new_v7(),
    )
}
fn request() -> ExchangePermitRequest {
    ExchangePermitRequest {
        stream: ExchangeStreamIdentity::try_new(
            ExchangeSource::new(task(1, 1), UniqueId::new(11, 12), 1),
            task(1, 2),
            UniqueId::new(21, 22),
            FragmentNodeId::new(7),
        )
        .unwrap(),
        sequence: ExchangeFrameSequence::FIRST,
        kind: ExchangeFrameKind::Data,
        geometry: ExchangeFrameGeometry::try_new(3, 9, 5).unwrap(),
    }
}
fn grant() -> ExchangePermitGrant {
    ExchangePermitGrant {
        request: request(),
        token: ExchangePermitToken::try_from_bytes([7; 16]).unwrap(),
    }
}
fn path() -> FieldPath {
    FieldPath::root("exchange")
}
fn frame(grant: ExchangePermitGrant) -> wire::ExchangeRequest {
    let stream = grant.request.stream;
    wire::ExchangeRequest {
        finst_id_hi: stream.destination_fragment_instance_id().high(),
        finst_id_lo: stream.destination_fragment_instance_id().low(),
        node_id: stream.destination_node().get(),
        source_finst_id_hi: stream.source().fragment_instance_id().high(),
        source_finst_id_lo: stream.source().fragment_instance_id().low(),
        sender_ordinal: stream.source().sender_ordinal(),
        sender_count: 2,
        sequence: grant.request.sequence.get(),
        eos: grant.request.kind == ExchangeFrameKind::End,
        payload: vec![1, 2, 3],
        permit_token: grant.token.to_bytes().to_vec(),
        stream: Some(encode_stream(stream)),
        frame_kind: encode_kind(grant.request.kind),
        ..Default::default()
    }
}

#[test]
fn permit_round_trip_preserves_full_identity_zero_sequence_and_geometry() {
    let grant = grant();
    let encoded = encode_permit_grant(grant);
    assert_eq!(
        decode_permit_grant(&encoded, grant.request, path()).unwrap(),
        grant
    );
    let mut zero = grant.request;
    zero.geometry = ExchangeFrameGeometry::try_new(0, 0, 0).unwrap();
    assert_eq!(
        decode_permit_request(&encode_permit_request(zero), path()).unwrap(),
        zero
    );
    for kind in [
        ExchangeFrameKind::LogicalSummary,
        ExchangeFrameKind::WriterResults,
    ] {
        let mut request = grant.request;
        request.kind = kind;
        assert_eq!(
            decode_permit_request(&encode_permit_request(request), path()).unwrap(),
            request
        );
    }
}

#[test]
fn missing_messages_negative_sequences_and_unknown_kinds_are_refused() {
    let valid = encode_permit_request(request());
    let mutations: [fn(&mut wire::AcquireExchangePermitRequest); 8] = [
        |r| r.stream = None,
        |r| r.stream.as_mut().unwrap().source_task = None,
        |r| r.stream.as_mut().unwrap().destination_task = None,
        |r| r.geometry = None,
        |r| r.sequence = -1,
        |r| r.frame_kind = 0,
        |r| r.frame_kind = 999,
        |r| r.stream.as_mut().unwrap().destination_node_id = -1,
    ];
    for mutate in mutations {
        let mut value = valid.clone();
        mutate(&mut value);
        assert!(decode_permit_request(&value, path()).is_err());
    }
    let mut overflow = valid;
    overflow.geometry.as_mut().unwrap().wire_bytes = u64::MAX;
    assert!(decode_permit_request(&overflow, path()).is_err());
}

#[test]
fn cross_attempt_and_invalid_task_process_are_refused_before_binding() {
    let mut wire = encode_permit_request(request());
    wire.stream.as_mut().unwrap().destination_task = Some(encode_task_identity(task(2, 2)));
    assert!(decode_permit_request(&wire, path()).is_err());
    wire.stream.as_mut().unwrap().destination_task = Some(encode_task_identity(task(1, 2)));
    wire.stream
        .as_mut()
        .unwrap()
        .source_task
        .as_mut()
        .unwrap()
        .backend_process_id
        .as_mut()
        .unwrap()
        .value = vec![0; 16];
    assert!(decode_permit_request(&wire, path()).is_err());
}

#[test]
fn grant_requires_exact_echo_and_nonempty_fixed_width_token() {
    let grant = grant();
    let valid = encode_permit_grant(grant);
    for token in [vec![], vec![0; 16], vec![7; 15], vec![7; 17]] {
        let mut value = valid.clone();
        value.token = token;
        assert!(decode_permit_grant(&value, grant.request, path()).is_err());
    }
    let mut missing = valid.clone();
    missing.request = None;
    assert!(decode_permit_grant(&missing, grant.request, path()).is_err());
    let mutations: [fn(&mut wire::AcquireExchangePermitRequest); 5] = [
        |r| r.sequence += 1,
        |r| r.frame_kind = wire::ExchangeFrameKind::WriterResults as i32,
        |r| r.geometry.as_mut().unwrap().decoded_bytes += 1,
        |r| r.stream.as_mut().unwrap().sender_ordinal = 0,
        |r| r.stream.as_mut().unwrap().destination_task = Some(encode_task_identity(task(1, 2))),
    ];
    for mutate in mutations {
        let mut value = valid.clone();
        mutate(value.request.as_mut().unwrap());
        assert_eq!(
            decode_permit_grant(&value, grant.request, path())
                .unwrap_err()
                .kind(),
            &ProtocolErrorKind::InconsistentFields
        );
    }
}

#[test]
fn data_header_cannot_move_a_permit_to_another_frame_or_duplicate_identity() {
    let grant = grant();
    let valid = frame(grant);
    validate_permitted_frame(&valid, grant, 2, path()).unwrap();
    let mutations: [fn(&mut wire::ExchangeRequest); 12] = [
        |r| r.stream = None,
        |r| r.node_id += 1,
        |r| r.source_finst_id_hi += 1,
        |r| r.finst_id_lo += 1,
        |r| r.sender_ordinal = 0,
        |r| r.sender_count = 3,
        |r| r.sequence += 1,
        |r| r.frame_kind = 999,
        |r| r.eos = true,
        |r| r.permit_token = vec![8; 16],
        |r| r.permit_token.clear(),
        |r| r.payload.push(4),
    ];
    for mutate in mutations {
        let mut value = valid.clone();
        mutate(&mut value);
        assert!(validate_permitted_frame(&value, grant, 2, path()).is_err());
    }
    assert!(validate_permitted_frame(&valid, grant, 0, path()).is_err());
    let mut changed = valid;
    changed.stream.as_mut().unwrap().source_task = Some(encode_task_identity(task(2, 1)));
    assert!(validate_permitted_frame(&changed, grant, 2, path()).is_err());
}

#[test]
fn end_requires_zero_geometry_and_an_empty_eos_payload() {
    let mut grant = grant();
    grant.request.kind = ExchangeFrameKind::End;
    assert!(decode_permit_request(&encode_permit_request(grant.request), path()).is_err());
    grant.request.geometry = ExchangeFrameGeometry::try_new(0, 0, 0).unwrap();
    let encoded = encode_permit_request(grant.request);
    assert_eq!(
        decode_permit_request(&encoded, path()).unwrap(),
        grant.request
    );
    let mut value = frame(grant);
    value.payload.clear();
    validate_permitted_frame(&value, grant, 2, path()).unwrap();
    value.payload.push(1);
    assert!(validate_permitted_frame(&value, grant, 2, path()).is_err());
}

#[test]
fn close_requires_an_explicit_disposition_and_exact_frozen_stream() {
    let stream = request().stream;
    for disposition in [
        ExchangeCloseDisposition::Consumed {
            final_sequence: ExchangeFrameSequence::FIRST,
        },
        ExchangeCloseDisposition::Withdrawn,
    ] {
        let close = ExchangeStreamClose {
            stream,
            disposition,
        };
        let valid = encode_normal_close(close, 2);
        assert_eq!(
            decode_normal_close(&valid, stream, 2, path()).unwrap(),
            close
        );
        let mutations: [fn(&mut wire::ExchangeNormalClosed); 8] = [
            |r| r.disposition = None,
            |r| r.destination_task = None,
            |r| r.destination_task = Some(encode_task_identity(task(2, 2))),
            |r| r.destination_node_id += 1,
            |r| r.source_finst_id_hi += 1,
            |r| r.destination_finst_id_lo += 1,
            |r| r.sender_count = 0,
            |r| r.sender_ordinal = 0,
        ];
        for mutate in mutations {
            let mut value = valid.clone();
            mutate(&mut value);
            assert!(decode_normal_close(&value, stream, 2, path()).is_err());
        }
    }
    let mut invalid = encode_normal_close(
        ExchangeStreamClose {
            stream,
            disposition: ExchangeCloseDisposition::Consumed {
                final_sequence: ExchangeFrameSequence::FIRST,
            },
        },
        2,
    );
    invalid.disposition = Some(wire::exchange_normal_closed::Disposition::Consumed(
        wire::ExchangeConsumed { final_sequence: -1 },
    ));
    assert!(decode_normal_close(&invalid, stream, 2, path()).is_err());
}

#[test]
fn closed_frame_envelope_still_requires_exact_frozen_fields_and_real_end_shape() {
    let grant = grant();
    let valid = frame(grant);
    let actual = validate_frame_envelope(&valid, grant.request.stream, 2, path()).unwrap();
    assert_eq!(actual.sequence, ExchangeFrameSequence::FIRST);
    assert_eq!(actual.kind, ExchangeFrameKind::Data);
    assert_eq!(actual.token.to_bytes(), [7; 16]);
    let mutations: [fn(&mut wire::ExchangeRequest); 13] = [
        |r| r.stream = None,
        |r| r.finst_id_hi += 1,
        |r| r.finst_id_lo += 1,
        |r| r.node_id += 1,
        |r| r.source_finst_id_hi += 1,
        |r| r.source_finst_id_lo += 1,
        |r| r.sender_ordinal = 0,
        |r| r.sender_count = 3,
        |r| r.sequence = -1,
        |r| r.frame_kind = 0,
        |r| r.frame_kind = 999,
        |r| r.eos = true,
        |r| r.permit_token.clear(),
    ];
    for mutate in mutations {
        let mut altered = valid.clone();
        mutate(&mut altered);
        assert!(validate_frame_envelope(&altered, grant.request.stream, 2, path()).is_err());
    }
    let mut end = valid;
    end.frame_kind = wire::ExchangeFrameKind::End as i32;
    end.eos = true;
    assert!(validate_frame_envelope(&end, grant.request.stream, 2, path()).is_err());
    end.payload.clear();
    assert_eq!(
        validate_frame_envelope(&end, grant.request.stream, 2, path())
            .unwrap()
            .kind,
        ExchangeFrameKind::End
    );
    // A syntactically valid token is still no permission: only the active
    // receiver's stored grant may authorize this alternate exact frame.
    end.permit_token = vec![8; 16];
    validate_frame_envelope(&end, grant.request.stream, 2, path()).unwrap();
    assert!(validate_permitted_frame(&end, grant, 2, path()).is_err());
}
