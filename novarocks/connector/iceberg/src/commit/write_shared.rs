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

//! Helpers shared by the write-control preparation paths.
//!
//! `prepare_write` and `prepare_row_mutation` resolve the same ref-scoped
//! facts before they diverge into their own field-signing rules. Keeping the
//! shared resolution here stops the two paths from drifting apart.

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorWriteFieldRequest};

use crate::iceberg::spec::{Schema, TableMetadata};

/// Resolve the snapshot a write against `target_ref` will be based on.
///
/// `main` resolves to the table's current snapshot; any other ref resolves to
/// that branch's head. `Ok(None)` means the ref exists but has no snapshot yet.
pub(crate) fn write_target_snapshot_id(
    metadata: &TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, ConnectorError> {
    if target_ref == "main" {
        return Ok(metadata.current_snapshot_id());
    }
    crate::ref_snapshot::resolve_branch_head_snapshot_id(metadata, target_ref)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))
}

/// Render a resolved base snapshot for the `base_version` / preparation
/// payload strings. Kept next to the resolver so both preparation paths spell
/// a missing snapshot the same way.
pub(crate) fn snapshot_token(target_snapshot_id: Option<i64>) -> String {
    target_snapshot_id.map_or_else(|| "none".to_string(), |id| id.to_string())
}

/// Resolve each requested write column against the frozen target schema and
/// restate it with the Arrow type the Iceberg writers actually consume.
///
/// The shared metadata schema owner authors Variant/Binary/Timestamptz
/// carriers and every declared logical domain. Both write paths consume those
/// complete fields instead of reconstructing an untagged carrier.
pub(crate) fn exact_requested_write_fields(
    metadata: &TableMetadata,
    requested: &[ConnectorWriteFieldRequest],
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    exact_requested_write_fields_at_schema(metadata, metadata.current_schema(), requested)
}

/// Resolve write fields against an already-frozen Iceberg schema.
pub(crate) fn exact_requested_write_fields_at_schema(
    metadata: &TableMetadata,
    iceberg_schema: &Schema,
    requested: &[ConnectorWriteFieldRequest],
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let arrow_schema = crate::scalar_integer_domain::metadata_sql_schema(metadata, iceberg_schema)?;
    requested
        .iter()
        .map(|request| {
            let requested_name = request.field().name();
            let (ordinal, iceberg_field) = iceberg_schema
                .as_struct()
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| field.name.eq_ignore_ascii_case(requested_name))
                .ok_or_else(|| {
                    invalid_write_activation(format!(
                        "Iceberg write input column `{requested_name}` is absent from the frozen target schema"
                    ))
                })?;
            let arrow_field = arrow_schema.field(ordinal);
            novarocks_type_contract::FunctionValueType::try_from_field(arrow_field).map_err(|error| {
                invalid_write_activation(format!("invalid frozen Iceberg write field `{}`: {error}", iceberg_field.name))
            })?;
            // Preserve the exact provider field, including nested domains,
            // dictionary identity and annotations; requested fields select
            // names and never author the target's logical type.
            Ok(ConnectorWriteFieldRequest::new(arrow_field.clone()))
        })
        .collect()
}

/// Resolve the exact schema owned by a previously resolved write base.
///
/// A missing snapshot denotes an empty table/ref and deliberately retains the
/// admitted metadata's current schema. A concrete snapshot always owns the
/// schema, including when that snapshot is an older branch head.
pub(crate) fn write_target_schema(
    metadata: &TableMetadata,
    target_snapshot_id: Option<i64>,
) -> Result<std::sync::Arc<Schema>, ConnectorError> {
    match target_snapshot_id {
        Some(snapshot_id) => metadata
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "resolved Iceberg write base snapshot is absent from admitted metadata",
                )
            })?
            .schema(metadata)
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!("resolve admitted Iceberg write base schema: {error}"),
                )
            }),
        None => Ok(metadata.current_schema().clone()),
    }
}

pub(crate) fn invalid_write_activation(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, SortOrder, TableMetadataBuilder,
        Type,
    };
    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

    fn metadata(properties: HashMap<String, String>) -> TableMetadata {
        let fields = [
            (1, "uuid", PrimitiveType::Uuid),
            (2, "variant", PrimitiveType::Variant),
            (3, "hll", PrimitiveType::Binary),
            (4, "bitmap", PrimitiveType::Binary),
            (5, "largeint", PrimitiveType::Fixed(16)),
            (6, "plain_fixed", PrimitiveType::Fixed(16)),
            (7, "plain_binary", PrimitiveType::Binary),
        ]
        .into_iter()
        .map(|(id, name, primitive)| {
            Arc::new(NestedField::optional(id, name, Type::Primitive(primitive)))
        })
        .collect::<Vec<_>>();
        TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(fields)
                .build()
                .expect("schema"),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/db/write_domain_test".to_string(),
            FormatVersion::V3,
            properties,
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn frozen_write_fields_keep_provider_uuid_variant_and_declared_opaque_domains() {
        let metadata = metadata(HashMap::from([
            ("novarocks.logical_type.hll".into(), "hll".into()),
            ("novarocks.logical_type.bitmap".into(), "bitmap".into()),
            ("novarocks.logical_type.largeint".into(), "largeint".into()),
        ]));
        let expected = [
            (
                "uuid",
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Uuid,
            ),
            ("variant", DataType::LargeBinary, ValueLogicalType::Variant),
            ("hll", DataType::Binary, ValueLogicalType::Hll),
            ("bitmap", DataType::Binary, ValueLogicalType::Bitmap),
            (
                "largeint",
                DataType::FixedSizeBinary(16),
                ValueLogicalType::LargeInt,
            ),
            (
                "plain_fixed",
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Physical,
            ),
            ("plain_binary", DataType::Binary, ValueLogicalType::Physical),
        ];
        // Names select the frozen provider fields. A caller's placeholder
        // carrier cannot author their target type or logical domain.
        let requests = expected
            .iter()
            .map(|(name, _, _)| {
                ConnectorWriteFieldRequest::new(Field::new(
                    name.to_uppercase(),
                    DataType::Null,
                    true,
                ))
            })
            .collect::<Vec<_>>();
        let fields =
            exact_requested_write_fields(&metadata, &requests).expect("exact write fields");
        let authored =
            crate::scalar_integer_domain::metadata_sql_schema(&metadata, metadata.current_schema())
                .expect("provider schema");
        for (ordinal, (request, (name, carrier, domain))) in fields.iter().zip(expected).enumerate()
        {
            assert_eq!(request.field(), authored.field(ordinal));
            assert_eq!(request.field().name(), name);
            assert_eq!(
                FunctionValueType::try_from_field(request.field()).expect("exact frozen type"),
                FunctionValueType::try_with_logical_type(carrier, true, domain)
                    .expect("expected provider type")
            );
        }
    }

    #[test]
    fn frozen_write_fields_reject_conflicting_provider_domain_declarations() {
        for (name, domain) in [
            ("plain_fixed", "hll"),
            ("plain_binary", "largeint"),
            ("uuid", "largeint"),
        ] {
            let metadata = metadata(HashMap::from([(
                format!("novarocks.logical_type.{name}"),
                domain.to_string(),
            )]));
            let request = ConnectorWriteFieldRequest::new(Field::new(name, DataType::Null, true));
            let error = exact_requested_write_fields(&metadata, &[request])
                .expect_err("a conflicting provider declaration cannot be silently erased");
            assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        }
    }
}
