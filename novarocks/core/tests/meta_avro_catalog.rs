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

use std::collections::BTreeMap;

use novarocks::meta::MetaPayload;
use novarocks::meta::avro::{
    decode_payload, encode_payload, encode_payload_with_schema, schema_catalog,
};
use novarocks::meta::repository::mv::{
    MvRefreshState, MvTargetLookup, RefreshCommitMarker, RefreshExternalOutcome, StoredMvRefresh,
};
use novarocks::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks::mv::persistence::dependency::StoredMvDependency;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TestEvolutionV1 {
    id: i64,
    name: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TestEvolutionV2 {
    id: i64,
    name: String,
    tags: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IcebergCatalogPropertiesAvro {
    properties: Vec<StringPair>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StringPair {
    key: String,
    value: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredMvDefinitionAvro {
    mv_id: i64,
    select_sql: String,
    base_table_refs: Vec<String>,
    primary_key_columns: Vec<String>,
    storage_engine: String,
    target_catalog: Option<String>,
    target_namespace: Option<String>,
    target_table: Option<String>,
    schema_contract: Option<String>,
    partition_spec: Option<String>,
    partition_state_complete: bool,
    last_refresh_ms: Option<i64>,
    last_refresh_rows: Option<i64>,
    last_refresh_snapshots: BTreeMap<String, i64>,
    last_refresh_table_uuids: BTreeMap<String, String>,
    last_refreshed_iceberg_snapshot_id: Option<i64>,
    refresh_in_progress: bool,
    active_refresh_id: Option<i64>,
    refresh_target_snapshots: BTreeMap<String, i64>,
    refresh_policy: StoredMvRefreshPolicyAvro,
    refresh_paused: bool,
    refresh_interval_ms: Option<i64>,
    max_staleness_ms: Option<i64>,
    last_scheduler_error: Option<String>,
    next_refresh_after_ms: Option<i64>,
    created_at_ms: i64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StoredMvRefreshPolicyAvro {
    Manual,
    AsyncOnChange,
    AsyncInterval,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredMvPartitionStateAvro {
    mv_id: i64,
    partition_key: String,
    status: MvPartitionRefreshStatusAvro,
    last_refresh_ms: Option<i64>,
    base_snapshots: BTreeMap<String, i64>,
    target_snapshot_id: Option<i64>,
    last_refresh_id: Option<i64>,
    failure_message: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum MvPartitionRefreshStatusAvro {
    Fresh,
    Refreshing,
    Failed,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct StoredMvRefreshAvroV1 {
    refresh_id: i64,
    mv_id: i64,
    state: MvRefreshState,
    target_catalog: Option<String>,
    target_namespace: Option<String>,
    target_table: Option<String>,
    staging_branch: Option<String>,
    expected_main_snapshot_id: Option<i64>,
    staging_snapshot_id: Option<i64>,
    published_snapshot_id: Option<i64>,
    target_snapshots: BTreeMap<String, i64>,
    base_table_uuids: BTreeMap<String, String>,
    rows: Option<i64>,
    marker: Option<RefreshCommitMarker>,
    external_outcome: Option<RefreshExternalOutcome>,
}

#[test]
fn avro_catalog_has_unique_subject_ids_and_fingerprints() -> TestResult {
    let catalog = schema_catalog()?;
    catalog.validate_unique_entries()?;
    let latest = catalog.latest("test.evolution")?;
    assert_eq!(latest.subject(), "test.evolution");
    assert_eq!(latest.id(), 2);
    assert_eq!(latest.fingerprint().len(), 16);
    assert_eq!(catalog.latest("iceberg.operation")?.id(), 2);
    assert_eq!(catalog.latest("mv.definition")?.id(), 2);
    assert_eq!(catalog.latest("mv.partition_state")?.id(), 1);
    Ok(())
}

#[test]
fn avro_catalog_enforces_full_transitive_compatibility() -> TestResult {
    schema_catalog()?.validate_full_transitive()?;
    Ok(())
}

#[test]
fn avro_codec_round_trips_latest_schema() -> TestResult {
    let payload = encode_payload(
        "test.evolution",
        &TestEvolutionV2 {
            id: 7,
            name: "mv".to_string(),
            tags: vec!["fast".to_string(), "safe".to_string()],
        },
    )?;
    let decoded: TestEvolutionV2 = decode_payload("test.evolution", &payload)?;
    assert_eq!(
        decoded,
        TestEvolutionV2 {
            id: 7,
            name: "mv".to_string(),
            tags: vec!["fast".to_string(), "safe".to_string()],
        }
    );
    Ok(())
}

#[test]
fn avro_codec_reads_older_writer_schema_with_latest_reader_defaults() -> TestResult {
    let catalog = schema_catalog()?;
    let writer = catalog.entry("test.evolution", 1)?;
    let payload = novarocks::meta::avro::encode_payload_with_schema(
        writer,
        &TestEvolutionV1 {
            id: 9,
            name: "old".to_string(),
        },
    )?;

    let decoded: TestEvolutionV2 = decode_payload("test.evolution", &payload)?;
    assert_eq!(
        decoded,
        TestEvolutionV2 {
            id: 9,
            name: "old".to_string(),
            tags: Vec::new(),
        }
    );
    Ok(())
}

#[test]
fn iceberg_catalog_properties_round_trip_as_string_pairs() -> TestResult {
    let expected = IcebergCatalogPropertiesAvro {
        properties: vec![
            StringPair {
                key: "type".to_string(),
                value: "rest".to_string(),
            },
            StringPair {
                key: "uri".to_string(),
                value: "http://localhost:8181".to_string(),
            },
            StringPair {
                key: "warehouse".to_string(),
                value: "s3://warehouse".to_string(),
            },
        ],
    };

    let payload = encode_payload("iceberg.catalog", &expected)?;
    assert_eq!(payload.schema_id, 1);
    let decoded: IcebergCatalogPropertiesAvro = decode_payload("iceberg.catalog", &payload)?;

    assert_eq!(decoded, expected);
    Ok(())
}

#[test]
fn mv_definition_round_trip_uses_json_string_contract_dto() -> TestResult {
    let expected = StoredMvDefinitionAvro {
        mv_id: 42,
        select_sql: "select id, sum(v) from db.orders group by id".to_string(),
        base_table_refs: vec!["iceberg.rest.db.orders".to_string()],
        primary_key_columns: vec!["id".to_string()],
        storage_engine: "iceberg".to_string(),
        target_catalog: Some("starrocks".to_string()),
        target_namespace: Some("mv".to_string()),
        target_table: Some("mv_orders".to_string()),
        schema_contract: Some(
            r#"{"columns":[{"name":"id","type":"BIGINT"},{"name":"total","type":"BIGINT"}]}"#
                .to_string(),
        ),
        partition_spec: Some(r#"{"fields":[{"source":"id","transform":"identity"}]}"#.to_string()),
        partition_state_complete: true,
        last_refresh_ms: Some(1_771_891_200_000),
        last_refresh_rows: Some(10),
        last_refresh_snapshots: BTreeMap::from([("iceberg.rest.db.orders".to_string(), 101)]),
        last_refresh_table_uuids: BTreeMap::from([(
            "iceberg.rest.db.orders".to_string(),
            "table-uuid-1".to_string(),
        )]),
        last_refreshed_iceberg_snapshot_id: Some(101),
        refresh_in_progress: true,
        active_refresh_id: Some(7),
        refresh_target_snapshots: BTreeMap::from([("iceberg.rest.db.orders".to_string(), 102)]),
        refresh_policy: StoredMvRefreshPolicyAvro::AsyncOnChange,
        refresh_paused: false,
        refresh_interval_ms: None,
        max_staleness_ms: Some(60_000),
        last_scheduler_error: None,
        next_refresh_after_ms: Some(1_771_891_260_000),
        created_at_ms: 1_771_891_100_000,
    };

    let payload = encode_payload("mv.definition", &expected)?;
    assert_eq!(payload.schema_id, 2);
    let decoded: StoredMvDefinitionAvro = decode_payload("mv.definition", &payload)?;

    assert_eq!(decoded, expected);
    Ok(())
}

#[test]
fn mv_partition_state_round_trip() -> TestResult {
    let expected = StoredMvPartitionStateAvro {
        mv_id: 42,
        partition_key: "spec=7;region=east".to_string(),
        status: MvPartitionRefreshStatusAvro::Fresh,
        last_refresh_ms: Some(1_700_000_000_000),
        base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 10)]),
        target_snapshot_id: Some(20),
        last_refresh_id: Some(30),
        failure_message: None,
    };

    let payload = encode_payload("mv.partition_state", &expected)?;
    assert_eq!(payload.schema_id, 1);
    let decoded: StoredMvPartitionStateAvro = decode_payload("mv.partition_state", &payload)?;
    assert_eq!(decoded, expected);
    Ok(())
}

#[test]
fn avro_codec_rejects_fingerprint_mismatch() -> TestResult {
    let mut payload = encode_payload(
        "test.evolution",
        &TestEvolutionV2 {
            id: 1,
            name: "bad".to_string(),
            tags: Vec::new(),
        },
    )?;
    payload.schema_fingerprint = "ffffffffffffffff".to_string();

    let err = decode_payload::<TestEvolutionV2>("test.evolution", &payload)
        .expect_err("fingerprint mismatch must fail");
    assert!(err.to_string().contains("fingerprint mismatch"), "{err}");
    Ok(())
}

#[test]
fn mv_refresh_v1_reads_with_latest_operation_id_default() -> TestResult {
    let writer = schema_catalog()?.entry("mv.refresh", 1)?;
    let payload = encode_payload_with_schema(
        writer,
        &StoredMvRefreshAvroV1 {
            refresh_id: 7,
            mv_id: 42,
            state: MvRefreshState::StagingCommitted,
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("analytics".to_string()),
            target_table: Some("orders_mv".to_string()),
            staging_branch: Some("nr_refresh_7".to_string()),
            expected_main_snapshot_id: Some(10),
            staging_snapshot_id: Some(11),
            published_snapshot_id: None,
            target_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 9)]),
            base_table_uuids: BTreeMap::from([(
                "ice.sales.orders".to_string(),
                "uuid-orders".to_string(),
            )]),
            rows: Some(3),
            marker: Some(RefreshCommitMarker {
                refresh_id: 7,
                mv_id: 42,
                token: "marker-7".to_string(),
            }),
            external_outcome: None,
        },
    )?;
    assert_eq!(payload.schema_id, 1);
    assert_eq!(payload.schema_fingerprint, writer.fingerprint());

    let decoded: StoredMvRefresh = decode_payload("mv.refresh", &payload)?;
    assert_eq!(decoded.refresh_id, 7);
    assert_eq!(decoded.mv_id, 42);
    assert_eq!(decoded.operation_id, None);
    assert_eq!(decoded.state, MvRefreshState::StagingCommitted);
    assert_eq!(decoded.staging_snapshot_id, Some(11));
    assert_eq!(decoded.marker.expect("marker").token, "marker-7");
    Ok(())
}

#[test]
fn mv_auxiliary_writer_schemas_round_trip_domain_dtos() -> TestResult {
    let lookup = MvTargetLookup { mv_id: 42 };
    let lookup_payload = encode_payload("mv.target_lookup", &lookup)?;
    assert_eq!(lookup_payload.schema_id, 1);
    assert_eq!(
        decode_payload::<MvTargetLookup>("mv.target_lookup", &lookup_payload)?,
        lookup
    );

    let dependency = StoredMvDependency {
        downstream_mv_id: 42,
        upstream: MvDependencyObjectRef {
            catalog: Some("ice".to_string()),
            database_or_namespace: "sales".to_string(),
            name: "orders".to_string(),
            object_type: MvDependencyObjectType::Table,
            storage_engine: MvDependencyStorageEngine::Iceberg,
        },
        created_at_ms: 1_700_000_000_000,
    };
    let dependency_payload = encode_payload("mv.dependency", &dependency)?;
    assert_eq!(dependency_payload.schema_id, 1);
    assert_eq!(
        decode_payload::<StoredMvDependency>("mv.dependency", &dependency_payload)?,
        dependency
    );

    let refresh = StoredMvRefresh {
        refresh_id: 8,
        mv_id: 42,
        operation_id: Some(88),
        state: MvRefreshState::PublishCommitted,
        target_catalog: Some("ice".to_string()),
        target_namespace: Some("analytics".to_string()),
        target_table: Some("orders_mv".to_string()),
        staging_branch: Some("nr_refresh_8".to_string()),
        expected_main_snapshot_id: Some(11),
        staging_snapshot_id: Some(12),
        published_snapshot_id: Some(13),
        target_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 10)]),
        base_table_uuids: BTreeMap::from([(
            "ice.sales.orders".to_string(),
            "uuid-orders".to_string(),
        )]),
        rows: Some(4),
        marker: Some(RefreshCommitMarker {
            refresh_id: 8,
            mv_id: 42,
            token: "marker-8".to_string(),
        }),
        external_outcome: Some(RefreshExternalOutcome {
            target_snapshot_id: Some(13),
            commit_id: "iceberg-snapshot-13".to_string(),
        }),
    };
    let refresh_payload = encode_payload("mv.refresh", &refresh)?;
    assert_eq!(refresh_payload.schema_id, 2);
    assert_eq!(
        decode_payload::<StoredMvRefresh>("mv.refresh", &refresh_payload)?,
        refresh
    );
    Ok(())
}

#[test]
fn mv_payload_decode_rejects_trailing_bytes() -> TestResult {
    let payload = encode_payload("mv.target_lookup", &MvTargetLookup { mv_id: 42 })?;
    let mut bytes = payload.bytes.to_vec();
    bytes.push(0);
    let corrupt = MetaPayload::avro(payload.schema_id, payload.schema_fingerprint, bytes.into());

    let err = decode_payload::<MvTargetLookup>("mv.target_lookup", &corrupt)
        .expect_err("trailing bytes must fail");
    assert!(err.to_string().contains("trailing bytes"), "{err}");
    Ok(())
}

#[test]
fn mv_writer_schema_bytes_are_frozen_for_frontend_repository_migration() -> TestResult {
    struct ExpectedSchema {
        subject: &'static str,
        schema_id: i32,
        bytes: &'static [u8],
        byte_count: usize,
        sha256: &'static str,
        fingerprint: &'static str,
    }

    let schemas = [
        ExpectedSchema {
            subject: "mv.definition",
            schema_id: 1,
            bytes: include_bytes!("../src/mv/persistence/schemas/mv.definition/0001.avsc"),
            byte_count: 2109,
            sha256: "dff507275143a8a48638f4add1a253c5879725826cbbbdc67948f42c7d63a090",
            fingerprint: "a98a8c81b6b02708",
        },
        ExpectedSchema {
            subject: "mv.definition",
            schema_id: 2,
            bytes: include_bytes!("../src/mv/persistence/schemas/mv.definition/0002.avsc"),
            byte_count: 2190,
            sha256: "217184b26af80ed241065adecbee461d0f104126b7eb8da554ae7a2fd4140436",
            fingerprint: "3651eb7f016bf9a8",
        },
        ExpectedSchema {
            subject: "mv.dependency",
            schema_id: 1,
            bytes: include_bytes!("../src/mv/persistence/schemas/mv.dependency/0001.avsc"),
            byte_count: 1039,
            sha256: "433bc7c8ac6f2b6f7ded80a030b1ed0622fa48b020f4aab1a8fd35eafa0fecce",
            fingerprint: "360c8bac443c702f",
        },
        ExpectedSchema {
            subject: "mv.target_lookup",
            schema_id: 1,
            bytes: include_bytes!("../src/meta/avro/schemas/mv.target_lookup/0001.avsc"),
            byte_count: 146,
            sha256: "d57fc9ff2211138c783d2a8dba0dcf955882cb4dbede16c053ed7d2a6ccd2db3",
            fingerprint: "56206b4e7ecb7427",
        },
        ExpectedSchema {
            subject: "mv.refresh",
            schema_id: 1,
            bytes: include_bytes!("../src/meta/avro/schemas/mv.refresh/0001.avsc"),
            byte_count: 2088,
            sha256: "e72cd4b66b6c4c4948aa65a22808022762d02d2aeba85b3d9d3942bfe93e1a27",
            fingerprint: "0fd27aeacc08224c",
        },
        ExpectedSchema {
            subject: "mv.refresh",
            schema_id: 2,
            bytes: include_bytes!("../src/meta/avro/schemas/mv.refresh/0002.avsc"),
            byte_count: 2163,
            sha256: "22fbe645bb5fc748a515e033d927eccedbed3b1d678e58abd6384e0bf43d7f69",
            fingerprint: "833b7eeb13e969bb",
        },
        ExpectedSchema {
            subject: "mv.partition_state",
            schema_id: 1,
            bytes: include_bytes!("../src/meta/avro/schemas/mv.partition_state/0001.avsc"),
            byte_count: 783,
            sha256: "3bfb0e9389dce01ea5a13e07331b5a2a1bbf15a7fb2816fa9070bbe1e53f4fdb",
            fingerprint: "523fcb534d7d43fe",
        },
    ];

    let catalog = schema_catalog()?;
    for expected in schemas {
        assert_eq!(
            expected.bytes.len(),
            expected.byte_count,
            "{}/{} byte count changed",
            expected.subject,
            expected.schema_id
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(expected.bytes)),
            expected.sha256,
            "{}/{} SHA-256 changed",
            expected.subject,
            expected.schema_id
        );

        let entry = catalog.entry(expected.subject, expected.schema_id)?;
        assert_eq!(entry.subject(), expected.subject);
        assert_eq!(entry.id(), expected.schema_id);
        assert_eq!(entry.raw_schema().as_bytes(), expected.bytes);
        assert_eq!(entry.fingerprint(), expected.fingerprint);
    }
    Ok(())
}
