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

use std::collections::BTreeSet;

use super::super::catalog::schema_catalog;
use super::super::key::{
    MvKeyKind, decode_key, dependency_by_downstream_key, dependency_by_upstream_key,
    projection_by_id_key, sequence_key, target_lookup_key,
};
use super::{
    DecodedMvRecord, MvRecordKind, MvSequence, decode_projection, decode_record, encode_projection,
    encode_record,
};
use crate::dependency::{MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine};
use crate::persistence::projection::StoredMvProjection;
use crate::persistence::test_support::sample_projection;
use crate::product::MvTarget;
use crate::repository::MvTargetLookup;
use bytes::Bytes;
use novarocks_state_store_api::{Key, Value};
use uuid::Uuid;

fn projection() -> StoredMvProjection {
    StoredMvProjection {
        mv_id: 7,
        facts: sample_projection(
            MvTarget::from_parts(Some("ice"), "sales", "orders_mv"),
            Some(11),
        ),
    }
}

fn upstream() -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some("ICE".to_string()),
        database_or_namespace: "Sales".to_string(),
        name: "Orders".to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

#[test]
fn current_key_classifier_is_closed_and_rejects_legacy_runtime_families() {
    let projection = projection_by_id_key(9).expect("projection key");
    assert_eq!(
        std::str::from_utf8(projection.as_bytes()).unwrap(),
        "novarocks/frontend/mv/accelerator/v3/projection/by-id/0000000000000009"
    );
    assert_eq!(decode_key(&projection).unwrap().kind, MvKeyKind::Projection);
    assert_eq!(
        decode_key(&target_lookup_key("ICE", "Sales", "Orders").unwrap())
            .unwrap()
            .kind,
        MvKeyKind::TargetLookup
    );

    for raw in [
        "novarocks/frontend/mv/v1/definition/by-id/0000000000000001",
        "novarocks/frontend/mv/v1/refresh/by-id/0000000000000001",
        "novarocks/frontend/mv/v1/partition/by-mv/0000000000000001/61",
        "novarocks/frontend/mv/accelerator/v1/projection/by-id/0000000000000001",
        "novarocks/frontend/mv/accelerator/v2/projection/by-id/0000000000000001",
        "novarocks/frontend/mv/accelerator/v2/refresh/by-id/0000000000000001",
        "novarocks/frontend/mv/accelerator/v2/unknown/value",
    ] {
        let key = Key::try_from(Bytes::from(raw)).unwrap();
        assert!(decode_key(&key).is_err(), "{raw} must remain unreachable");
    }
}

#[test]
fn dependency_indexes_share_one_canonical_identity() {
    let downstream = dependency_by_downstream_key(7, &upstream(), 7).unwrap();
    let upstream = dependency_by_upstream_key(&upstream(), 7, 7).unwrap();
    assert_eq!(
        decode_key(&downstream).unwrap().kind,
        MvKeyKind::DependencyDownstream
    );
    assert_eq!(
        decode_key(&upstream).unwrap().kind,
        MvKeyKind::DependencyUpstream
    );
}

#[test]
fn projection_codec_round_trips_complete_source_revision_and_waterline() {
    let expected = projection();
    let key = projection_by_id_key(expected.mv_id).unwrap();
    let operation_id = Uuid::now_v7();
    let value = encode_projection(operation_id, &expected).unwrap();
    let decoded = decode_projection(&key, &value).unwrap();
    assert_eq!(decoded.operation_id, operation_id);
    assert_eq!(decoded.value, expected);
    let source = decoded.value.facts.source_revision();
    assert_eq!(source.target.instance_id.as_str(), "ice");
    assert_eq!(source.target.namespace.as_ref(), "sales");
    assert_eq!(source.target.table.as_ref(), "orders_mv");
    assert_eq!(source.deployment_owner.as_str(), "test-deployment");
    assert_eq!(source.process_incarnation.as_str(), "test-process");
    assert!(source.publication_revision.is_some());
    assert_eq!(
        source
            .publication_output_version
            .as_ref()
            .and_then(|version| version.snapshot_id()),
        Some(11)
    );
}

#[test]
fn envelope_rejects_wrong_kind_unknown_schema_and_corruption() {
    let operation_id = Uuid::now_v7();
    let value = encode_record(
        MvRecordKind::TargetLookup,
        operation_id,
        &MvTargetLookup { mv_id: 7 },
    )
    .unwrap();
    let wrong_key = projection_by_id_key(7).unwrap();
    assert!(decode_record::<MvTargetLookup>(&wrong_key, &value).is_err());

    let target = target_lookup_key("ice", "sales", "orders").unwrap();
    let mut v1_envelope = value.clone().into_bytes().to_vec();
    v1_envelope[4] = 1;
    let v1_envelope = Value::try_from(Bytes::from(v1_envelope)).unwrap();
    let error = decode_record::<MvTargetLookup>(&target, &v1_envelope)
        .expect_err("v1 envelope must not be readable");
    assert!(error.contains("unsupported MV Accelerator envelope version 1"));

    let mut unknown_schema = value.clone().into_bytes().to_vec();
    unknown_schema[6..10].copy_from_slice(&999_i32.to_be_bytes());
    let unknown_schema = Value::try_from(Bytes::from(unknown_schema)).unwrap();
    assert!(decode_record::<MvTargetLookup>(&target, &unknown_schema).is_err());

    let mut malformed = value.into_bytes().to_vec();
    malformed[0] = b'X';
    let malformed = Value::try_from(Bytes::from(malformed)).unwrap();
    assert!(decode_record::<MvTargetLookup>(&target, &malformed).is_err());
}

#[test]
fn sequence_contains_only_the_internal_mv_id_high_water_mark() {
    let key = sequence_key().unwrap();
    let sequence = MvSequence {
        last_allocated_id: 42,
    };
    let value = encode_record(MvRecordKind::Sequence, Uuid::now_v7(), &sequence).unwrap();
    let decoded: DecodedMvRecord<MvSequence> = decode_record(&key, &value).unwrap();
    assert_eq!(decoded.value, sequence);
}

#[test]
fn schema_catalog_contains_exactly_four_current_accelerator_subjects() {
    let catalog = schema_catalog().expect("MV Accelerator schema catalog");
    catalog.validate_unique_entries().unwrap();
    catalog.validate_full_transitive().unwrap();
    assert_eq!(
        catalog.subjects().collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "mv.accelerator_dependency",
            "mv.accelerator_projection",
            "mv.accelerator_sequence",
            "mv.accelerator_target_lookup",
        ])
    );
    for subject in catalog.subjects() {
        assert!(catalog.entry(subject, 1).is_err());
        assert!(catalog.entry(subject, 2).is_err());
        assert_eq!(catalog.latest(subject).unwrap().id(), 3);
    }
    assert!(catalog.entry("mv.definition", 4).is_err());
    assert!(catalog.entry("mv.refresh", 5).is_err());
    assert!(catalog.entry("mv.partition_state", 1).is_err());
}

#[test]
fn canonical_root_rejects_tampered_output_payload_and_missing_document() {
    let expected = projection();
    let key = projection_by_id_key(expected.mv_id).unwrap();
    let mut dto = super::StoredMvProjectionAvro::try_from(&expected).unwrap();
    dto.output_version.as_mut().unwrap().payload =
        serde_bytes::ByteBuf::from(b"different-output".to_vec());
    let value = encode_record(MvRecordKind::Projection, Uuid::now_v7(), &dto).unwrap();
    assert!(
        decode_projection(&key, &value)
            .unwrap_err()
            .contains("exact output")
    );
    let mut dto = super::StoredMvProjectionAvro::try_from(&expected).unwrap();
    dto.configuration.clear();
    let value = encode_record(MvRecordKind::Projection, Uuid::now_v7(), &dto).unwrap();
    assert!(decode_projection(&key, &value).is_err());
}
