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

use crate::mv::storage_observation::{
    MvObservedTargetField, MvSchemaValidationObservation, MvSchemaValidationPartitionContract,
    MvSchemaValidationPartitionField, MvSchemaValidationPartitionTransform,
};

const ICEBERG_ROW_LINEAGE_PROP: &str = "write.row-lineage";

pub(crate) fn current_iceberg_table_observation(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<MvSchemaValidationObservation, String> {
    current_iceberg_table_observation_with_schema(table, table.metadata().current_schema())
}

fn current_iceberg_table_observation_with_schema(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    schema: &novarocks_connector_iceberg::iceberg::spec::Schema,
) -> Result<MvSchemaValidationObservation, String> {
    let metadata = table.metadata();
    MvSchemaValidationObservation::try_new_with_maximum_payload(
        metadata.uuid().to_string(),
        schema.schema_id(),
        metadata.format_version() == novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3,
        row_lineage_enabled(metadata.properties()),
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| {
                MvObservedTargetField::new(
                    field.id,
                    field.name.clone(),
                    field.field_type.to_string(),
                    !field.required,
                )
            })
            .collect(),
        partition_contract(metadata.default_partition_spec(), schema)?,
    )
    .map_err(|error| error.to_string())
}

fn partition_contract(
    spec: &novarocks_connector_iceberg::iceberg::spec::PartitionSpec,
    schema: &novarocks_connector_iceberg::iceberg::spec::Schema,
) -> Result<MvSchemaValidationPartitionContract, String> {
    let fields = spec
        .fields()
        .iter()
        .map(|field| {
            let source = schema.field_by_id(field.source_id).ok_or_else(|| {
                format!(
                    "partition field {} references missing source field ID {}",
                    field.name, field.source_id
                )
            })?;
            let transform = match &field.transform {
                novarocks_connector_iceberg::iceberg::spec::Transform::Identity => {
                    MvSchemaValidationPartitionTransform::Identity
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Year => {
                    MvSchemaValidationPartitionTransform::Year
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Month => {
                    MvSchemaValidationPartitionTransform::Month
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Day => {
                    MvSchemaValidationPartitionTransform::Day
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Hour => {
                    MvSchemaValidationPartitionTransform::Hour
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Bucket(num_buckets) => {
                    MvSchemaValidationPartitionTransform::Bucket {
                        num_buckets: *num_buckets,
                    }
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Truncate(width) => {
                    MvSchemaValidationPartitionTransform::Truncate { width: *width }
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Void => {
                    MvSchemaValidationPartitionTransform::Void
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Unknown => {
                    MvSchemaValidationPartitionTransform::Unsupported(format!(
                        "{:?}",
                        field.transform
                    ))
                }
            };
            Ok(MvSchemaValidationPartitionField::new(
                field.field_id,
                field.name.clone(),
                field.source_id,
                source.name.clone(),
                transform,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(MvSchemaValidationPartitionContract::new(
        spec.spec_id(),
        fields,
    ))
}

fn row_lineage_enabled(props: &std::collections::HashMap<String, String>) -> bool {
    props
        .get(ICEBERG_ROW_LINEAGE_PROP)
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_lineage_enabled_recognizes_case_insensitive_true() {
        let mut properties = std::collections::HashMap::new();
        properties.insert(ICEBERG_ROW_LINEAGE_PROP.to_string(), "TRUE".to_string());
        assert!(row_lineage_enabled(&properties));
        properties.insert(ICEBERG_ROW_LINEAGE_PROP.to_string(), "false".to_string());
        assert!(!row_lineage_enabled(&properties));
        properties.clear();
        assert!(!row_lineage_enabled(&properties));
    }
}
