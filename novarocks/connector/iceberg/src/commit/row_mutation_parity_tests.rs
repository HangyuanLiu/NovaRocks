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

//! Golden parity between this crate's row-mutation preparation and the legacy
//! Core implementation it replaces.
//!
//! This crate must not depend on Core, so the Core outcomes cannot be computed
//! side by side. They are frozen below instead, captured by running Core's
//! `prepare_iceberg_row_mutation` against the byte-identical fixture this file
//! rebuilds. The fixture pins the table UUID and `last-updated-ms`, which the
//! Iceberg metadata builder would otherwise randomize, because the signed
//! tokens hash the encoded table payload.
//!
//! Capture provenance: `novarocks::connector::iceberg::provider::
//! prepare_iceberg_row_mutation` at `main@05c65b6d4`, via a throwaway
//! Core-side test that was not committed.
//!
//! TEMPORARY: this module exists only while both implementations coexist. It
//! must be deleted in the same PR that removes the Core implementation
//! (SPI-5J); it is not a permanent conformance suite.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorExecutionBindingKey, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorRequestContext, ConnectorRowMutationIntent,
    ConnectorRowMutationPreparationOutcome, ConnectorRowMutationPreparationRequest,
    ConnectorTableHandle, ConnectorWriteOperationId, ConnectorWriteTargetRef,
};
use sha2::{Digest, Sha256};

use super::row_mutation_preparation::prepare_row_mutation;

/// Captured from Core. Any change here means the two implementations have
/// diverged, or the fixture stopped being deterministic.
const CORE_PAYLOAD_SHA256: &str =
    "2706abd5755e89eafa2271dd308e0cfa218327fa62e2b9e7642e09dfdc68c108";
const CORE_BASE_VERSION: &str =
    "iceberg/row-mutation-base/v1/11111111-2222-3333-4444-555555555555/main/none";
const CORE_PREPARATION_PAYLOAD: &str = "iceberg/row-mutation-preparation/v1/ice/11111111-2222-3333-4444-555555555555/main/none/PositionDelete";

/// `(role, index, token hex, ordinal, column name, nullable)` exactly as Core
/// signed them.
const CORE_SIGNED_LAYOUT: [(&str, usize, &str, u32, &str, bool); 7] = [
    (
        "identity",
        0,
        "6feeadba8b183b3212e06f5682aa40ba7f268d3110e938a9da7707d6484e0ecd",
        0,
        "_file",
        false,
    ),
    (
        "identity",
        1,
        "131b652bae7be5908569e3667b4fc9baf8f9bb6f58fd227ca2d32378ab831905",
        1,
        "_pos",
        false,
    ),
    (
        "before",
        0,
        "822abd3f1d450a5bcd40742c54e08708d5292b703d09f301968ce047ce823585",
        2,
        "id",
        false,
    ),
    (
        "before",
        1,
        "c60ce41ed31ef21850bef06fc0eccbeeaeda1caf03a87e936e716e887b0ac983",
        3,
        "name",
        true,
    ),
    (
        "after",
        0,
        "cea7cbf4340cea0cf6d1afd6ab0bc468c94648c38aa268f548856ec27c7260db",
        4,
        "id",
        false,
    ),
    (
        "after",
        1,
        "3f22fc56e398b9cbdec843e4432207ce5ac94f2a5358424ad49b6d2f1edad37b",
        5,
        "name",
        true,
    ),
    (
        "effect",
        0,
        "bb5b524dff2f8bbfa3a2102c67e5666de2c6ab262de8f2bdc8cc129f7443a7a6",
        6,
        "__row_mutation_effect",
        false,
    ),
];

/// The metadata Core hashed, with the UUID and timestamp already pinned.
const FIXTURE_METADATA_JSON: &str = r#"{"current-schema-id":0,"default-sort-order-id":0,"default-spec-id":0,"format-version":2,"last-column-id":2,"last-partition-id":999,"last-sequence-number":0,"last-updated-ms":1700000000000,"location":"file:///tmp/row-mutation","partition-specs":[{"fields":[],"spec-id":0}],"refs":{},"schemas":[{"fields":[{"id":1,"name":"id","required":true,"type":"long"},{"id":2,"name":"name","required":false,"type":"string"}],"schema-id":0,"type":"struct"}],"sort-orders":[{"fields":[],"order-id":0}],"table-uuid":"11111111-2222-3333-4444-555555555555"}"#;

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn instance_id() -> ConnectorInstanceId {
    ConnectorInstanceId::parse("ice").expect("instance id")
}

fn owner() -> ConnectorExecutionBindingKey {
    ConnectorExecutionBindingKey {
        instance_id: instance_id(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
    }
}

fn iceberg_schema() -> crate::iceberg::spec::Schema {
    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema")
}

/// Rebuild the exact bytes Core was handed.
fn fixture_payload_bytes() -> Vec<u8> {
    let payload = serde_json::json!({
        "namespace": "db",
        "table": "t",
        "table_info": {
            "catalog": "ice",
            "namespace": "db",
            "table": "t",
            "table_uuid": "11111111-2222-3333-4444-555555555555",
            "current_snapshot_id": null,
            "schema_id": 1,
            "location": "file:///tmp/row-mutation",
            "schema": serde_json::to_value(
                crate::schema_facts::iceberg_schema_def(&iceberg_schema())
            ).expect("schema def"),
            "serialized_metadata": FIXTURE_METADATA_JSON,
            "serialized_metadata_rows": null
        },
        "metadata_columns": ["_file", "_pos"],
        "metadata_table_type": null,
        "prepared_files": [],
        "explicit_files": null,
        "logical_type_columns": {},
        "hidden_columns": []
    });
    serde_json::to_vec(&payload).expect("payload bytes")
}

#[test]
fn the_fixture_still_hashes_to_the_bytes_core_was_given() {
    let mut hasher = Sha256::new();
    hasher.update(fixture_payload_bytes());
    assert_eq!(
        format!("{:x}", hasher.finalize()),
        CORE_PAYLOAD_SHA256,
        "the parity fixture drifted; every golden below was captured against the old bytes"
    );
}

#[test]
fn row_mutation_preparation_matches_the_core_goldens() {
    let handle = ConnectorTableHandle::try_new(instance_id(), Bytes::from(fixture_payload_bytes()))
        .expect("table handle");
    let request = ConnectorRowMutationPreparationRequest {
        operation_id: ConnectorWriteOperationId::from_bytes([9; 16]),
        table: handle,
        target_ref: ConnectorWriteTargetRef::parse("main".to_string()).expect("target ref"),
        intent: ConnectorRowMutationIntent::Delete,
        context: ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            16 * 1024,
            64 * 1024,
        )
        .expect("context"),
    };

    let prepared = match prepare_row_mutation(request, &owner()).expect("provider prepare") {
        ConnectorRowMutationPreparationOutcome::Prepared(prepared) => prepared,
        ConnectorRowMutationPreparationOutcome::Denied(error) => {
            panic!("provider denied a fixture Core prepared: {error}")
        }
    };

    assert_eq!(
        String::from_utf8_lossy(prepared.payload().as_ref()),
        CORE_PREPARATION_PAYLOAD
    );
    assert!(
        format!("{:?}", prepared.base_version()).contains(CORE_BASE_VERSION),
        "base version diverged from Core: {:?}",
        prepared.base_version()
    );

    let contract = prepared.match_contract();
    let mut observed: Vec<(&str, usize, String, u32, String, bool)> = Vec::new();
    for (index, field) in contract.identity_fields().iter().enumerate() {
        observed.push((
            "identity",
            index,
            hex_lower(field.token().to_bytes()),
            field.source_ordinal(),
            field.field().name().to_string(),
            field.field().is_nullable(),
        ));
    }
    for (index, field) in contract.before_fields().iter().enumerate() {
        observed.push((
            "before",
            index,
            hex_lower(field.token().to_bytes()),
            field.target_ordinal(),
            field.field().name().to_string(),
            field.field().is_nullable(),
        ));
    }
    for (index, field) in contract.after_fields().iter().enumerate() {
        observed.push((
            "after",
            index,
            hex_lower(field.token().to_bytes()),
            field.target_ordinal(),
            field.field().name().to_string(),
            field.field().is_nullable(),
        ));
    }
    let effect = contract.effect_field();
    observed.push((
        "effect",
        0,
        hex_lower(effect.token().to_bytes()),
        effect.target_ordinal(),
        effect.field().name().to_string(),
        effect.field().is_nullable(),
    ));

    assert_eq!(observed.len(), CORE_SIGNED_LAYOUT.len());
    for (got, want) in observed.iter().zip(CORE_SIGNED_LAYOUT.iter()) {
        assert_eq!(
            (got.0, got.1, got.2.as_str(), got.3, got.4.as_str(), got.5),
            (want.0, want.1, want.2, want.3, want.4, want.5),
            "signed layout diverged from Core"
        );
    }
}

fn hex_lower(bytes: [u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
