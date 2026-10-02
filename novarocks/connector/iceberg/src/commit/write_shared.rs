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

use arrow::datatypes::Field;
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
/// The Variant/Binary/Timestamptz overrides exist because
/// `schema_to_arrow_schema` widens those Iceberg types beyond what the data
/// writers accept; keeping the override here stops each write path from
/// re-deciding it.
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
    let arrow_schema = crate::schema_mapping::sql_write_schema_from_iceberg(iceberg_schema)
        .map_err(|error| {
            invalid_write_activation(format!(
                "convert frozen Iceberg write schema to Arrow: {error}"
            ))
        })?;
    let arrow_schema = crate::scalar_integer_domain::apply_schema(
        arrow_schema,
        iceberg_schema,
        &crate::scalar_integer_domain::metadata_declarations(metadata)?,
    )?;
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
            let data_type = arrow_field.data_type().clone();
            Ok(ConnectorWriteFieldRequest::new(Field::new(
                &iceberg_field.name,
                data_type,
                !iceberg_field.required,
            )))
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
    use crate::iceberg::spec::{
        FormatVersion, MapType, NestedField, PartitionSpec, PrimitiveType, SortOrder, StructType,
        TableMetadataBuilder, Type,
    };
    use arrow::datatypes::DataType;
    use novarocks_types::logical_type::LogicalType;
    use std::sync::Arc;
    fn metadata(fields: Vec<Arc<NestedField>>) -> TableMetadata {
        let schema = Schema::builder().with_fields(fields).build().unwrap();
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///exact-write-schema".into(),
            FormatVersion::V3,
            Default::default(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }
    #[test]
    fn exact_write_projection_recursively_preserves_binary_variant_timezone_and_required_key() {
        use arrow::datatypes::TimeUnit;
        let table = metadata(vec![Arc::new(NestedField::optional(
            1,
            "payload",
            Type::Struct(StructType::new(vec![
                Arc::new(NestedField::required(
                    2,
                    "binary",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "variant",
                    Type::Primitive(PrimitiveType::Variant),
                )),
                Arc::new(NestedField::required(
                    4,
                    "timestamp",
                    Type::Primitive(PrimitiveType::TimestamptzNs),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "map",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::map_key_element(
                            6,
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::map_value_element(
                            7,
                            Type::Primitive(PrimitiveType::Binary),
                            false,
                        )),
                    )),
                )),
            ])),
        ))]);
        let read =
            crate::scalar_integer_domain::metadata_sql_schema(&table, table.current_schema())
                .unwrap();
        let requested = vec![novarocks_spi::connector::ConnectorWriteFieldRequest::new(
            read.field(0).clone(),
        )];
        let write =
            exact_requested_write_fields_at_schema(&table, table.current_schema(), &requested)
                .unwrap();
        let DataType::Struct(fields) = write[0].field().data_type() else {
            panic!("struct")
        };
        assert_eq!(fields[0].data_type(), &DataType::Binary);
        assert_eq!(fields[1].data_type(), &DataType::LargeBinary);
        assert_eq!(
            fields[2].data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        let DataType::Map(entries, _) = fields[3].data_type() else {
            panic!("map")
        };
        let DataType::Struct(entries) = entries.data_type() else {
            panic!("entries")
        };
        assert!(!entries[0].is_nullable());
        assert!(entries[1].is_nullable());
        assert_eq!(entries[1].data_type(), &DataType::Binary);
        let LogicalType::Struct(read_fields) =
            &crate::schema_mapping::exact_logical_fields(&table).unwrap()[0].data_type
        else {
            panic!("read")
        };
        let LogicalType::Map { key, .. } = &read_fields[3].data_type else {
            panic!("read map")
        };
        assert!(key.nullable);
    }
}
