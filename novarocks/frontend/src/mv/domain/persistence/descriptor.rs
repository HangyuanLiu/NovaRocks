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

#[cfg(test)]
use novarocks_sql::planning::mv::ApplyKeySource;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::common::persisted_query_definition::PersistedQueryDefinition;
use crate::mv::domain::persistence::schema::MvSchemaContract;

pub const MV_DESCRIPTOR_VERSION: u16 = 2;
pub const MV_DESCRIPTOR_PACKAGE_ID_PROP: &str = "novarocks.mv.descriptor.package-id";
pub const MV_DESCRIPTOR_HASH_PROP: &str = "novarocks.mv.descriptor.hash";
pub const MV_DESCRIPTOR_INLINE_PROP: &str = "novarocks.mv.descriptor.inline";
// W2 adds `novarocks.mv.descriptor.location` for externalized descriptor payloads.
pub const MV_DESCRIPTOR_INLINE_MAX_BYTES: usize = 64 * 1024;
pub const MV_DESCRIPTOR_RAW_QUERY_SOURCE_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorDependency {
    pub catalog: String,
    pub namespace: String,
    pub name: String,
    pub object_type: String,
    pub storage_engine: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvDescriptorV2 {
    pub descriptor_version: u16,
    pub package_id: String,
    pub query_definition: PersistedQueryDefinition,
    pub visible_columns: Vec<String>,
    pub hidden_columns: Vec<String>,
    pub base_dependencies: Vec<DescriptorDependency>,
    #[serde(default)]
    pub schema_contract: Option<Value>,
    #[serde(default)]
    pub refresh_contract: Option<Value>,
    pub created_at_ms: i64,
}

impl MvDescriptorV2 {
    pub fn to_canonical_json(&self) -> Result<String, String> {
        let value = serde_json::to_value(self)
            .map_err(|err| format!("failed to serialize MV descriptor: {err}"))?;
        serde_json::to_string(&sort_json_value(value))
            .map_err(|err| format!("failed to render canonical MV descriptor JSON: {err}"))
    }

    pub fn content_hash(&self) -> Result<String, String> {
        let mut value = serde_json::to_value(self)
            .map_err(|err| format!("failed to serialize MV descriptor: {err}"))?;
        // Physical fields excluded from the identity hash so a descriptor rebuilt
        // from the same lake on a fresh cluster hashes identically (W0 acceptance:
        // logical content equality, physical products may differ).
        if let Some(obj) = value.as_object_mut() {
            obj.remove("created_at_ms");
        }
        let canonical = serde_json::to_string(&sort_json_value(value))
            .map_err(|err| format!("failed to render canonical MV descriptor hash JSON: {err}"))?;
        Ok(hex_encode(&Sha256::digest(canonical.as_bytes())))
    }

    pub fn from_json(s: &str) -> Result<Self, String> {
        // Check the version before deserializing the v2 shape. A v1 document
        // must never be treated as a partially-populated v2 descriptor.
        let value: Value = serde_json::from_str(s)
            .map_err(|err| format!("failed to parse MV descriptor JSON: {err}"))?;
        let version = value
            .get("descriptor_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| "MV descriptor is missing an integer descriptor_version".to_string())?;
        if version != u64::from(MV_DESCRIPTOR_VERSION) {
            return Err(format!(
                "unsupported MV descriptor version: expected {}, got {}",
                MV_DESCRIPTOR_VERSION, version
            ));
        }
        let descriptor: Self = serde_json::from_value(value)
            .map_err(|err| format!("failed to parse MV descriptor v2 JSON: {err}"))?;
        descriptor
            .query_definition
            .validate()
            .map_err(|error| format!("invalid MV descriptor query definition: {error}"))?;
        descriptor.validate_raw_query_source_size()?;
        Ok(descriptor)
    }

    pub fn to_storage_properties(&self) -> Result<Vec<(String, String)>, String> {
        self.query_definition
            .validate()
            .map_err(|error| format!("invalid MV descriptor query definition: {error}"))?;
        self.validate_raw_query_source_size()?;
        let inline = self.to_canonical_json()?;
        let inline_bytes = inline.len();
        if inline_bytes > MV_DESCRIPTOR_INLINE_MAX_BYTES {
            return Err(format!(
                "MV descriptor inline payload is {inline_bytes} bytes, exceeds 64KiB cap of {} bytes",
                MV_DESCRIPTOR_INLINE_MAX_BYTES
            ));
        }

        Ok(vec![
            (
                MV_DESCRIPTOR_PACKAGE_ID_PROP.to_string(),
                self.package_id.clone(),
            ),
            (MV_DESCRIPTOR_HASH_PROP.to_string(), self.content_hash()?),
            (MV_DESCRIPTOR_INLINE_PROP.to_string(), inline),
        ])
    }

    pub fn from_storage_properties(
        props: &std::collections::HashMap<String, String>,
    ) -> Result<Self, String> {
        let inline = props.get(MV_DESCRIPTOR_INLINE_PROP).ok_or_else(|| {
            format!(
                "MV table is missing required MV descriptor inline property `{MV_DESCRIPTOR_INLINE_PROP}`"
            )
        })?;
        let descriptor = Self::from_json(inline)?;

        if let Some(stored_hash) = props.get(MV_DESCRIPTOR_HASH_PROP) {
            let actual_hash = descriptor.content_hash()?;
            if stored_hash != &actual_hash {
                return Err(format!(
                    "MV descriptor hash mismatch: storage property has {stored_hash}, descriptor content hash is {actual_hash}"
                ));
            }
        }

        Ok(descriptor)
    }

    /// Store a typed MV schema contract into this descriptor's
    /// `schema_contract` field, serializing it to `serde_json::Value`.
    pub fn set_schema_contract(&mut self, contract: &MvSchemaContract) -> Result<(), String> {
        self.schema_contract = Some(
            serde_json::to_value(contract)
                .map_err(|e| format!("serialize MV schema contract into descriptor: {e}"))?,
        );
        Ok(())
    }

    /// Parse this descriptor's `schema_contract` field into a typed MV
    /// schema contract, if present.
    pub fn schema_contract_typed(&self) -> Result<Option<MvSchemaContract>, String> {
        match &self.schema_contract {
            None => Ok(None),
            Some(value) => serde_json::from_value::<MvSchemaContract>(value.clone())
                .map(Some)
                .map_err(|e| format!("parse MV schema contract from descriptor: {e}")),
        }
    }

    fn validate_raw_query_source_size(&self) -> Result<(), String> {
        let raw_bytes = self.query_definition.raw_query_source.len();
        if raw_bytes > MV_DESCRIPTOR_RAW_QUERY_SOURCE_MAX_BYTES {
            return Err(format!(
                "MV descriptor raw query source is {raw_bytes} bytes, exceeds 64KiB cap of {} bytes",
                MV_DESCRIPTOR_RAW_QUERY_SOURCE_MAX_BYTES
            ));
        }
        Ok(())
    }
}

fn sort_json_value(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json_value).collect()),
        Value::Object(object) => {
            let mut entries = object
                .into_iter()
                .map(|(key, value)| (key, sort_json_value(value)))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));

            let mut sorted = Map::new();
            for (key, value) in entries {
                sorted.insert(key, value);
            }
            Value::Object(sorted)
        }
        value => value,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> MvDescriptorV2 {
        MvDescriptorV2 {
            descriptor_version: MV_DESCRIPTOR_VERSION,
            package_id: "analytics.mv_orders".to_string(),
            query_definition: PersistedQueryDefinition::new(
                "SELECT id FROM ice.sales.orders",
                crate::common::persisted_query_definition::PersistedQueryDialect::StarRocks,
                "ice",
                "sales",
            )
            .expect("valid query definition"),
            visible_columns: vec!["id".to_string()],
            hidden_columns: vec!["__nova_base_row_id".to_string()],
            base_dependencies: vec![DescriptorDependency {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                name: "orders".to_string(),
                object_type: "table".to_string(),
                storage_engine: "iceberg".to_string(),
            }],
            schema_contract: Some(serde_json::json!({
                "z": 3,
                "a": {
                    "z": 2,
                    "a": 1
                }
            })),
            refresh_contract: None,
            created_at_ms: 123,
        }
    }

    #[test]
    fn descriptor_json_round_trips() {
        let descriptor = sample();
        let json = descriptor.to_canonical_json().unwrap();

        let parsed = MvDescriptorV2::from_json(&json).unwrap();

        assert_eq!(parsed, descriptor);
    }

    #[test]
    fn descriptor_json_rejects_unsupported_version() {
        let mut descriptor = sample();
        descriptor.descriptor_version = MV_DESCRIPTOR_VERSION + 1;
        let json = descriptor.to_canonical_json().unwrap();

        let err = MvDescriptorV2::from_json(&json).unwrap_err();

        assert!(err.contains("unsupported MV descriptor version"));
        assert!(err.contains(&(MV_DESCRIPTOR_VERSION + 1).to_string()));
    }

    #[test]
    fn descriptor_json_rejects_v1_before_attempting_v2_deserialization() {
        let old_shape =
            r#"{"descriptor_version":1,"logical_sql":"SELECT 1","dialect":"starrocks"}"#;

        let err = MvDescriptorV2::from_json(old_shape).unwrap_err();

        assert_eq!(err, "unsupported MV descriptor version: expected 2, got 1");
    }

    #[test]
    fn canonical_json_has_only_single_table_descriptor_fields() {
        let descriptor = sample();

        let canonical = descriptor.to_canonical_json().unwrap();
        let value: Value = serde_json::from_str(&canonical).unwrap();
        assert_eq!(
            value["descriptor_version"].as_u64(),
            Some(u64::from(MV_DESCRIPTOR_VERSION))
        );
        assert!(value.get("logical_sql").is_none());
        assert!(value.get("dialect").is_none());
        assert_eq!(
            value["query_definition"]["raw_query_source"],
            "SELECT id FROM ice.sales.orders"
        );
    }

    #[test]
    fn canonical_json_is_key_sorted_and_hash_stable() {
        let descriptor = sample();

        let canonical = descriptor.to_canonical_json().unwrap();

        assert_eq!(canonical, descriptor.to_canonical_json().unwrap());
        assert_eq!(
            descriptor.content_hash().unwrap(),
            descriptor.content_hash().unwrap()
        );
    }

    #[test]
    fn content_hash_excludes_created_at_ms() {
        let mut descriptor_a = sample();
        descriptor_a.created_at_ms = 123;
        let mut descriptor_b = sample();
        descriptor_b.created_at_ms = 999_999;

        assert_eq!(
            descriptor_a.content_hash().unwrap(),
            descriptor_b.content_hash().unwrap(),
            "descriptors differing only in created_at_ms must hash identically"
        );

        let mut descriptor_c = descriptor_b.clone();
        descriptor_c.query_definition.raw_query_source =
            "SELECT id FROM ice.sales.other".to_string();

        assert_ne!(
            descriptor_b.content_hash().unwrap(),
            descriptor_c.content_hash().unwrap(),
            "descriptors differing in a logical field must hash differently"
        );

        let mut descriptor_d = descriptor_b.clone();
        descriptor_d.query_definition.resolution.default_database = "other".to_string();
        assert_ne!(
            descriptor_b.content_hash().unwrap(),
            descriptor_d.content_hash().unwrap(),
            "descriptor hashes must include the frozen resolution context"
        );
    }

    #[test]
    fn descriptor_properties_carry_pkg_hash_inline() {
        let descriptor = sample();

        let props = descriptor.to_storage_properties().unwrap();
        let get = |key: &str| {
            props
                .iter()
                .find(|(prop_key, _)| prop_key == key)
                .map(|(_, value)| value.clone())
        };

        assert_eq!(
            get(MV_DESCRIPTOR_PACKAGE_ID_PROP).as_deref(),
            Some("analytics.mv_orders")
        );
        assert_eq!(
            get(MV_DESCRIPTOR_HASH_PROP),
            Some(descriptor.content_hash().unwrap())
        );
        assert_eq!(
            MvDescriptorV2::from_json(&get(MV_DESCRIPTOR_INLINE_PROP).unwrap()).unwrap(),
            descriptor
        );

        let props_map = props
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            MvDescriptorV2::from_storage_properties(&props_map).unwrap(),
            descriptor
        );
    }

    #[test]
    fn descriptor_properties_reject_hash_mismatch() {
        let descriptor = sample();
        let props = descriptor.to_storage_properties().unwrap();
        let mut props = props
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        props.insert(
            MV_DESCRIPTOR_HASH_PROP.to_string(),
            "not-the-hash".to_string(),
        );

        let err = MvDescriptorV2::from_storage_properties(&props).unwrap_err();

        assert!(err.contains("hash mismatch"), "got: {err}");
    }

    #[test]
    fn descriptor_properties_reject_raw_query_source_beyond_64kib() {
        let mut descriptor = sample();
        descriptor.query_definition.raw_query_source =
            "x".repeat(MV_DESCRIPTOR_RAW_QUERY_SOURCE_MAX_BYTES + 1);

        let err = descriptor.to_storage_properties().unwrap_err();

        assert!(err.contains("raw query source"), "got: {err}");
        assert!(err.contains("exceeds 64KiB cap"), "got: {err}");
        assert!(
            err.contains(&MV_DESCRIPTOR_RAW_QUERY_SOURCE_MAX_BYTES.to_string()),
            "got: {err}"
        );
    }

    #[test]
    fn descriptor_properties_reject_inline_payload_beyond_64kib() {
        let mut descriptor = sample();
        descriptor.query_definition.raw_query_source =
            "x".repeat(MV_DESCRIPTOR_RAW_QUERY_SOURCE_MAX_BYTES);

        let err = descriptor.to_storage_properties().unwrap_err();

        assert!(err.contains("inline payload"), "got: {err}");
        assert!(err.contains("64KiB cap"), "got: {err}");
    }

    #[test]
    fn schema_contract_typed_preserves_absence() {
        let mut descriptor = sample();
        descriptor.schema_contract = None;

        assert_eq!(descriptor.schema_contract_typed().unwrap(), None);

        let round_trip = MvDescriptorV2::from_json(&descriptor.to_canonical_json().unwrap())
            .expect("round-trip descriptor without schema contract");
        assert_eq!(round_trip.schema_contract_typed().unwrap(), None);
    }

    #[test]
    fn schema_contract_typed_round_trips() {
        use crate::mv::domain::persistence::schema::{
            BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
            HiddenApplyKeyContract, MvSchemaContract, OutputColumnLineage, OutputContract,
            TargetContract, TargetVisibleColumn,
        };
        use bytes::Bytes;
        use novarocks_spi::connector::ConnectorTableObjectId;
        let contract = MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.ns.orders".to_string(),
                table_object_id: ConnectorTableObjectId::try_new(Bytes::from_static(&[
                    0, 0xff, b'u',
                ]))
                .expect("valid opaque table object ID"),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    }],
                },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.ns.mv".to_string(),
                table_uuid: "t".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "long".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: None,
            },
        };
        let mut d = sample();
        d.set_schema_contract(&contract).unwrap();
        assert_eq!(d.schema_contract_typed().unwrap().as_ref(), Some(&contract));

        // A descriptor with no contract yields None. `sample()` itself sets
        // `schema_contract` to an arbitrary JSON blob (for canonical-JSON/hash
        // tests elsewhere), so build the `None` case explicitly here rather
        // than relying on `sample()`'s default.
        let mut empty = sample();
        empty.schema_contract = None;
        assert_eq!(empty.schema_contract_typed().unwrap(), None);
    }
}
