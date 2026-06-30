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

//! Lowering for `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE` (IVM-A1).
//!
//! The Thrift node carries identity, snapshot range, and a NovaRocks-private
//! typed plan produced at refresh/codegen time. Lowering converts that plan
//! into typed table descriptors, change files, object-store config, and
//! delete-side descriptors; it does not read connector catalog state or
//! reconstruct Iceberg table metadata.
//! Delete-side runtime state is captured into `IcebergRuntimeHandles` so
//! per-file operator code can borrow it instead of rebuilding it per file.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::node::iceberg_delta_scan::{
    ApplyKeySource, BaseTableIdent, DeletedFileVisibility, DeltaScanDeleteSide,
    DeltaScanDeleteSidePayload, DeltaSourceFile, DeltaSourceRole, EqualityDeleteTargetData,
    IcebergDeltaDataColumnPayload, IcebergDeltaScanNode, IcebergDeltaTablePayload,
    IcebergRuntimeHandles, PositionDeleteFileFormat, PositionDeleteSourceData,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::layout::{Layout, chunk_schema_for_layout};
use crate::lower::node::Lowered;
use crate::thrift::descriptors;
use crate::thrift::plan_nodes;

/// Lower an `ICEBERG_DELTA_SCAN_NODE` into an `ExecNode` of kind
/// `IcebergDeltaScan`. The node must carry a typed refresh/codegen-time
/// plan; this boundary does not read connector catalog state.
pub(crate) fn lower_iceberg_delta_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    out_layout: Layout,
) -> Result<Lowered, String> {
    let payload = node.iceberg_delta_scan_node.as_ref().ok_or_else(|| {
        format!(
            "ICEBERG_DELTA_SCAN_NODE node_id={} missing iceberg_delta_scan_node payload",
            node.node_id
        )
    })?;

    // Defense in depth: revalidate snapshot ids are non-negative even though
    // the standalone analyzer already rejects negative values. A Thrift node
    // from a non-analyzer producer (e.g. direct Thrift, future IVM planner
    // path) would bypass that guard and silently misinterpret the ids.
    let node_id = node.node_id;
    if payload.from_snapshot_id < 0 {
        return Err(format!(
            "ivm-a1 lower delta-scan (node_id={node_id}, {}.{}.{}): from_snapshot_id must be non-negative, got {}",
            payload.catalog, payload.iceberg_namespace, payload.table, payload.from_snapshot_id,
        ));
    }
    if payload.to_snapshot_id < 0 {
        return Err(format!(
            "ivm-a1 lower delta-scan (node_id={node_id}, {}.{}.{}): to_snapshot_id must be non-negative, got {}",
            payload.catalog, payload.iceberg_namespace, payload.table, payload.to_snapshot_id,
        ));
    }

    let plan = &payload.delta_plan;
    let table_payload = lower_table_payload(plan);
    let change_files = lower_delta_source_files(&plan.change_files)?;
    let delete_side_payload = lower_delete_side_payload(plan.delete_side.as_ref())?;
    let object_store_config = object_store_config_from_cloud_configuration(
        plan.cloud_configuration.as_ref(),
        &table_payload.table_location,
    )?;
    let object_store_factory = Arc::new(
        crate::connector::iceberg::changes::build_factory_for_table_location(
            &table_payload.table_location,
            object_store_config.as_ref(),
        )?,
    );
    let file_io = crate::connector::iceberg::changes::file_io_for_table_location(
        &table_payload.table_location,
        object_store_config.as_ref(),
    )?;
    let delete_side =
        build_delete_side_from_payload(delete_side_payload, object_store_config.as_ref())?;

    let output_chunk_schema: ChunkSchemaRef = if out_layout.order.is_empty() {
        Arc::new(crate::exec::chunk::ChunkSchema::empty())
    } else {
        let desc_tbl = desc_tbl.ok_or_else(|| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE node_id={} requires descriptor table to build chunk schema",
                node.node_id
            )
        })?;
        chunk_schema_for_layout(desc_tbl, &out_layout)?
    };

    let exec_node = IcebergDeltaScanNode {
        base_table_ident: BaseTableIdent {
            catalog: payload.catalog.clone(),
            namespace: payload.iceberg_namespace.clone(),
            table: payload.table.clone(),
        },
        table_location: table_payload.table_location.clone(),
        from_snapshot_id: payload.from_snapshot_id,
        to_snapshot_id: payload.to_snapshot_id,
        output_chunk_schema,
        apply_key_source: ApplyKeySource::BaseRowId,
        change_files,
        object_store_config,
        iceberg_runtime: Arc::new(IcebergRuntimeHandles {
            table: table_payload,
            object_store_factory,
            file_io,
            delete_side,
        }),
        node_id: node.node_id,
    };

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::IcebergDeltaScan(exec_node),
        },
        layout: out_layout,
    })
}

fn lower_table_payload(plan: &plan_nodes::TIcebergDeltaScanPlan) -> IcebergDeltaTablePayload {
    IcebergDeltaTablePayload {
        table_location: plan.table_location.clone(),
        data_columns: plan
            .data_columns
            .iter()
            .map(|column| IcebergDeltaDataColumnPayload {
                name: column.name.clone(),
                field_id: column.field_id,
            })
            .collect(),
    }
}

fn lower_delta_source_files(
    files: &[plan_nodes::TIcebergDeltaSourceFile],
) -> Result<Vec<DeltaSourceFile>, String> {
    files.iter().map(lower_delta_source_file).collect()
}

fn lower_delta_source_file(
    file: &plan_nodes::TIcebergDeltaSourceFile,
) -> Result<DeltaSourceFile, String> {
    let role = if file.role == plan_nodes::TIcebergDeltaSourceRole::DATA_FILE {
        reject_role_payload(
            file,
            "DATA_FILE",
            &[
                "position_deletes",
                "equality_field_ids",
                "equality_targets",
                "deleted_file_visibility",
            ],
        )?;
        DeltaSourceRole::DataFile
    } else if file.role == plan_nodes::TIcebergDeltaSourceRole::POSITION_DELETE {
        reject_role_payload(
            file,
            "POSITION_DELETE",
            &[
                "equality_field_ids",
                "equality_targets",
                "deleted_file_visibility",
            ],
        )?;
        let deletes = file.position_deletes.as_ref().ok_or_else(|| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE source file {} role POSITION_DELETE requires position_deletes",
                file.path
            )
        })?;
        DeltaSourceRole::PositionDelete {
            deletes: deletes
                .iter()
                .map(lower_position_delete_source)
                .collect::<Result<Vec<_>, _>>()?,
        }
    } else if file.role == plan_nodes::TIcebergDeltaSourceRole::EQUALITY_DELETE {
        reject_role_payload(
            file,
            "EQUALITY_DELETE",
            &["position_deletes", "deleted_file_visibility"],
        )?;
        let equality_field_ids = file.equality_field_ids.clone().ok_or_else(|| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE source file {} role EQUALITY_DELETE requires equality_field_ids",
                file.path
            )
        })?;
        let targets = file.equality_targets.as_ref().ok_or_else(|| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE source file {} role EQUALITY_DELETE requires equality_targets",
                file.path
            )
        })?;
        DeltaSourceRole::EqualityDelete {
            equality_field_ids,
            targets: targets.iter().map(lower_equality_delete_target).collect(),
        }
    } else if file.role == plan_nodes::TIcebergDeltaSourceRole::DELETED_DATA_FILE {
        reject_role_payload(
            file,
            "DELETED_DATA_FILE",
            &["position_deletes", "equality_field_ids", "equality_targets"],
        )?;
        DeltaSourceRole::DeletedDataFile {
            previous_data_file_visibility: file.deleted_file_visibility.as_ref().map(
                |visibility| DeletedFileVisibility {
                    already_deleted_positions: visibility.already_deleted_positions.clone(),
                },
            ),
        }
    } else {
        return Err(format!(
            "ICEBERG_DELTA_SCAN_NODE source file {} has unknown delta role {:?}",
            file.path, file.role
        ));
    };

    Ok(DeltaSourceFile {
        path: file.path.clone(),
        size: file.size,
        role,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        row_id_allow_list: file.row_id_allow_list.clone(),
    })
}

fn reject_role_payload(
    file: &plan_nodes::TIcebergDeltaSourceFile,
    role_name: &str,
    fields: &[&str],
) -> Result<(), String> {
    for field in fields {
        let present = match *field {
            "position_deletes" => file.position_deletes.is_some(),
            "equality_field_ids" => file.equality_field_ids.is_some(),
            "equality_targets" => file.equality_targets.is_some(),
            "deleted_file_visibility" => file.deleted_file_visibility.is_some(),
            _ => false,
        };
        if present {
            return Err(format!(
                "ICEBERG_DELTA_SCAN_NODE source file {} role {} must not carry {}",
                file.path, role_name, field
            ));
        }
    }
    Ok(())
}

fn lower_position_delete_source(
    delete: &plan_nodes::TIcebergDeltaPositionDeleteSource,
) -> Result<PositionDeleteSourceData, String> {
    Ok(PositionDeleteSourceData {
        delete_file_path: delete.delete_file_path.clone(),
        delete_file_size: delete.delete_file_size,
        referenced_data_file: delete.referenced_data_file.clone(),
        file_format: lower_position_delete_format(delete.file_format)?,
        content_offset: delete.content_offset,
        content_size_in_bytes: delete.content_size_in_bytes,
    })
}

fn lower_position_delete_format(
    format: plan_nodes::TIcebergDeltaPositionDeleteFileFormat,
) -> Result<PositionDeleteFileFormat, String> {
    match format {
        f if f == plan_nodes::TIcebergDeltaPositionDeleteFileFormat::PARQUET => {
            Ok(PositionDeleteFileFormat::Parquet)
        }
        f if f == plan_nodes::TIcebergDeltaPositionDeleteFileFormat::PUFFIN => {
            Ok(PositionDeleteFileFormat::Puffin)
        }
        other => Err(format!(
            "ICEBERG_DELTA_SCAN_NODE unsupported position-delete file format {:?}",
            other
        )),
    }
}

fn lower_equality_delete_target(
    target: &plan_nodes::TIcebergDeltaEqualityDeleteTarget,
) -> EqualityDeleteTargetData {
    EqualityDeleteTargetData {
        data_file_path: target.data_file_path.clone(),
        data_file_size: target.data_file_size,
        data_file_first_row_id: target.data_file_first_row_id,
        data_file_sequence_number: target.data_file_sequence_number,
    }
}

fn lower_delete_side_payload(
    payload: Option<&plan_nodes::TIcebergDeltaDeleteSidePlan>,
) -> Result<Option<DeltaScanDeleteSidePayload>, String> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    Ok(Some(DeltaScanDeleteSidePayload {
        base_data_file_lineage: lower_lineage_map(&payload.base_data_file_lineage),
        previous_data_file_lineage: lower_lineage_map(&payload.previous_data_file_lineage),
        previous_delete_visibility_data_files: payload
            .previous_delete_visibility_data_files
            .iter()
            .map(lower_delete_visibility_data_file)
            .collect::<Result<Vec<_>, _>>()?,
        previously_deleted_positions_per_file: payload
            .previously_deleted_positions_per_file
            .iter()
            .map(|(path, positions)| {
                let converted = positions
                    .iter()
                    .map(|position| {
                        u64::try_from(*position).map_err(|_| {
                            format!(
                                "ICEBERG_DELTA_SCAN_NODE previous deleted position is negative for {}: {}",
                                path, position
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((path.clone(), converted))
            })
            .collect::<Result<HashMap<_, _>, String>>()?,
        deleted_data_file_paths: payload.deleted_data_file_paths.iter().cloned().collect(),
    }))
}

fn lower_lineage_map(
    input: &BTreeMap<String, plan_nodes::TIcebergDeltaBaseDataFileLineage>,
) -> HashMap<String, crate::exec::node::iceberg_delta_scan::BaseDataFileLineage> {
    input
        .iter()
        .map(|(path, lineage)| {
            (
                path.clone(),
                crate::exec::node::iceberg_delta_scan::BaseDataFileLineage {
                    first_row_id: lineage.first_row_id,
                    data_sequence_number: lineage.data_sequence_number,
                },
            )
        })
        .collect()
}

fn lower_delete_visibility_data_file(
    file: &plan_nodes::TIcebergDeltaDeleteVisibilityDataFile,
) -> Result<crate::connector::iceberg::changes::DeleteVisibilityDataFileDescriptor, String> {
    Ok(
        crate::connector::iceberg::changes::DeleteVisibilityDataFileDescriptor {
            path: file.path.clone(),
            size: file.size,
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            delete_files: file
                .delete_files
                .iter()
                .map(lower_delete_visibility_delete_file)
                .collect::<Result<Vec<_>, _>>()?,
        },
    )
}

fn lower_delete_visibility_delete_file(
    file: &plan_nodes::TIcebergDeltaDeleteVisibilityDeleteFile,
) -> Result<crate::connector::iceberg::changes::DeleteVisibilityDeleteFileDescriptor, String> {
    Ok(
        crate::connector::iceberg::changes::DeleteVisibilityDeleteFileDescriptor {
            path: file.path.clone(),
            file_format: lower_delete_visibility_format(file.file_format)?,
            file_content: lower_delete_visibility_content(file.file_content)?,
            length: file.length,
            content_offset: file.content_offset,
            content_size_in_bytes: file.content_size_in_bytes,
        },
    )
}

fn lower_delete_visibility_format(
    format: plan_nodes::TIcebergDeltaDeleteFileFormat,
) -> Result<crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat, String> {
    match format {
        f if f == plan_nodes::TIcebergDeltaDeleteFileFormat::PARQUET => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Parquet)
        }
        f if f == plan_nodes::TIcebergDeltaDeleteFileFormat::PUFFIN => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Puffin)
        }
        other => Err(format!(
            "ICEBERG_DELTA_SCAN_NODE unsupported delete visibility file format {:?}",
            other
        )),
    }
}

fn lower_delete_visibility_content(
    content: plan_nodes::TIcebergDeltaDeleteFileContent,
) -> Result<crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent, String> {
    match content {
        c if c == plan_nodes::TIcebergDeltaDeleteFileContent::POSITION => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Position)
        }
        c if c == plan_nodes::TIcebergDeltaDeleteFileContent::EQUALITY => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Equality)
        }
        other => Err(format!(
            "ICEBERG_DELTA_SCAN_NODE unsupported delete visibility file content {:?}",
            other
        )),
    }
}

fn object_store_config_from_cloud_configuration(
    cloud: Option<&crate::thrift::cloud_configuration::TCloudConfiguration>,
    _table_location: &str,
) -> Result<Option<crate::fs::object_store::ObjectStoreConfig>, String> {
    let Some(cloud) = cloud else {
        return Ok(None);
    };
    let Some(props) = cloud.cloud_properties.as_ref() else {
        return Ok(None);
    };
    let credentials =
        crate::fs::object_store_credentials::ObjectStoreCredentials::optional_from_aws_s3_properties(
            crate::fs::object_store_credentials::ObjectStoreCredentialsSource::AwsS3Properties,
            props,
        )?;
    let Some(credentials) = credentials else {
        return Ok(None);
    };

    let mut config = credentials.to_object_store_config();
    crate::fs::object_store::apply_object_store_runtime_defaults(&mut config);
    Ok(Some(config))
}

fn build_delete_side_from_payload(
    payload: Option<DeltaScanDeleteSidePayload>,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Option<DeltaScanDeleteSide>, String> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    let mut previously_deleted_positions_per_file = std::collections::HashMap::new();
    for (path, positions) in payload.previously_deleted_positions_per_file {
        let mut bitmap = roaring::RoaringTreemap::new();
        for pos in positions {
            bitmap.insert(pos);
        }
        previously_deleted_positions_per_file.insert(path, bitmap);
    }
    let previous_delete_visibility =
        crate::engine::delete_flow::load_existing_delete_visibility_from_descriptors(
            &payload.previous_delete_visibility_data_files,
            object_store_config,
        )?;
    Ok(Some(DeltaScanDeleteSide {
        base_data_file_lineage: payload.base_data_file_lineage,
        previous_delete_visibility,
        previously_deleted_positions_per_file,
        previous_data_file_lineage: payload.previous_data_file_lineage,
        deleted_data_file_paths: payload.deleted_data_file_paths,
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;

    fn data_columns() -> Vec<plan_nodes::TIcebergDeltaDataColumn> {
        vec![plan_nodes::TIcebergDeltaDataColumn::new(
            "id".to_string(),
            1,
        )]
    }

    fn delta_plan(
        table_location: &str,
        cloud_configuration: Option<crate::thrift::cloud_configuration::TCloudConfiguration>,
        change_files: Vec<plan_nodes::TIcebergDeltaSourceFile>,
    ) -> plan_nodes::TIcebergDeltaScanPlan {
        plan_nodes::TIcebergDeltaScanPlan::new(
            table_location.to_string(),
            data_columns(),
            cloud_configuration,
            change_files,
            None::<plan_nodes::TIcebergDeltaDeleteSidePlan>,
        )
    }

    fn s3_cloud_configuration() -> crate::thrift::cloud_configuration::TCloudConfiguration {
        let cloud_properties = BTreeMap::from([
            (
                "aws.s3.endpoint".to_string(),
                "http://127.0.0.1:9000".to_string(),
            ),
            ("aws.s3.access_key".to_string(), "ak".to_string()),
            ("aws.s3.secret_key".to_string(), "sk".to_string()),
            (
                "aws.s3.enable_path_style_access".to_string(),
                "true".to_string(),
            ),
            ("aws.s3.region".to_string(), "us-east-1".to_string()),
        ]);
        crate::thrift::cloud_configuration::TCloudConfiguration::new(
            None::<crate::thrift::cloud_configuration::TCloudType>,
            None::<Vec<crate::thrift::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties),
            None::<bool>,
        )
    }

    #[test]
    fn delta_cloud_object_store_config_uses_shared_credentials_aliases() {
        let cloud_properties = BTreeMap::from([
            (
                "aws.s3.endpoint_url".to_string(),
                " http://localhost:9000 ".to_string(),
            ),
            ("aws.s3.accessKeyId".to_string(), " ak ".to_string()),
            ("aws.s3.accessKeySecret".to_string(), " sk ".to_string()),
            (
                "aws.s3.enable_path_style_access".to_string(),
                "yes".to_string(),
            ),
        ]);
        let cloud_configuration = crate::thrift::cloud_configuration::TCloudConfiguration::new(
            None::<crate::thrift::cloud_configuration::TCloudType>,
            None::<Vec<crate::thrift::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties),
            None::<bool>,
        );

        let cfg = object_store_config_from_cloud_configuration(
            Some(&cloud_configuration),
            "s3://bucket-a/table/root",
        )
        .expect("parse cloud config")
        .expect("credentials present");

        assert_eq!(cfg.endpoint, "http://localhost:9000");
        assert_eq!(cfg.access_key_id, "ak");
        assert_eq!(cfg.access_key_secret, "sk");
        assert_eq!(cfg.enable_path_style_access, Some(true));
    }

    #[test]
    fn delta_cloud_object_store_config_errors_on_invalid_bool() {
        let cloud_properties = BTreeMap::from([
            (
                "aws.s3.endpoint".to_string(),
                "http://localhost:9000".to_string(),
            ),
            ("aws.s3.access_key".to_string(), "ak".to_string()),
            ("aws.s3.secret_key".to_string(), "sk".to_string()),
            (
                "aws.s3.enable_path_style_access".to_string(),
                "maybe".to_string(),
            ),
        ]);
        let cloud_configuration = crate::thrift::cloud_configuration::TCloudConfiguration::new(
            None::<crate::thrift::cloud_configuration::TCloudType>,
            None::<Vec<crate::thrift::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties),
            None::<bool>,
        );

        let err = object_store_config_from_cloud_configuration(
            Some(&cloud_configuration),
            "s3://bucket-a/table/root",
        )
        .expect_err("invalid bool should fail");
        assert!(err.contains("invalid boolean value: maybe"), "{err}");
    }

    #[test]
    fn delta_scan_lowers_from_typed_thrift_payload_without_catalog_registry() {
        let mut node = crate::sql::codegen::nodes::default_plan_node();
        node.node_id = 42;
        node.node_type = plan_nodes::TPlanNodeType::ICEBERG_DELTA_SCAN_NODE;
        node.num_children = 0;
        node.row_tuples = vec![];
        node.iceberg_delta_scan_node = Some(plan_nodes::TIcebergDeltaScanNode {
            catalog: "ice".to_string(),
            iceberg_namespace: "db".to_string(),
            table: "orders".to_string(),
            from_snapshot_id: 10,
            to_snapshot_id: 11,
            delta_plan: delta_plan(
                "file:///tmp/iceberg_delta_payload_table",
                None,
                vec![plan_nodes::TIcebergDeltaSourceFile::new(
                    "file:///tmp/added.parquet".to_string(),
                    123,
                    plan_nodes::TIcebergDeltaSourceRole::DATA_FILE,
                    None::<i32>,
                    None::<String>,
                    Some(1000),
                    Some(7),
                    None::<std::collections::BTreeSet<i64>>,
                    None::<Vec<plan_nodes::TIcebergDeltaPositionDeleteSource>>,
                    None::<Vec<i32>>,
                    None::<Vec<plan_nodes::TIcebergDeltaEqualityDeleteTarget>>,
                    None::<plan_nodes::TIcebergDeltaDeletedFileVisibility>,
                )],
            ),
        });

        let lowered = lower_iceberg_delta_scan_node(
            &node,
            None,
            Layout {
                order: Vec::new(),
                index: HashMap::new(),
            },
        )
        .expect("lower from typed payload without registry");
        let ExecNodeKind::IcebergDeltaScan(scan) = lowered.node.kind else {
            panic!("expected iceberg delta scan");
        };
        assert_eq!(scan.change_files.len(), 1);
        assert_eq!(scan.change_files[0].path, "file:///tmp/added.parquet");
        assert_eq!(scan.base_table_ident.catalog, "ice");
    }

    #[test]
    fn delta_scan_s3_table_location_requires_object_store_config() {
        let mut node = crate::sql::codegen::nodes::default_plan_node();
        node.node_id = 43;
        node.node_type = plan_nodes::TPlanNodeType::ICEBERG_DELTA_SCAN_NODE;
        node.num_children = 0;
        node.row_tuples = vec![];
        node.iceberg_delta_scan_node = Some(plan_nodes::TIcebergDeltaScanNode {
            catalog: "ice".to_string(),
            iceberg_namespace: "db".to_string(),
            table: "orders".to_string(),
            from_snapshot_id: 10,
            to_snapshot_id: 11,
            delta_plan: delta_plan("s3://bucket/table", None, Vec::new()),
        });

        let err = lower_iceberg_delta_scan_node(
            &node,
            None,
            Layout {
                order: Vec::new(),
                index: HashMap::new(),
            },
        )
        .expect_err("s3 table location requires object-store config");
        assert!(
            err.contains("object store config") || err.contains("object_store_config"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn delta_scan_s3_table_location_uses_object_store_config() {
        let mut node = crate::sql::codegen::nodes::default_plan_node();
        node.node_id = 44;
        node.node_type = plan_nodes::TPlanNodeType::ICEBERG_DELTA_SCAN_NODE;
        node.num_children = 0;
        node.row_tuples = vec![];
        node.iceberg_delta_scan_node = Some(plan_nodes::TIcebergDeltaScanNode {
            catalog: "ice".to_string(),
            iceberg_namespace: "db".to_string(),
            table: "orders".to_string(),
            from_snapshot_id: 10,
            to_snapshot_id: 11,
            delta_plan: delta_plan(
                "s3://bucket/table",
                Some(s3_cloud_configuration()),
                Vec::new(),
            ),
        });

        let lowered = lower_iceberg_delta_scan_node(
            &node,
            None,
            Layout {
                order: Vec::new(),
                index: HashMap::new(),
            },
        )
        .expect("s3 table location lowers with object-store config");
        let ExecNodeKind::IcebergDeltaScan(scan) = lowered.node.kind else {
            panic!("expected iceberg delta scan");
        };
        assert_eq!(
            scan.object_store_config
                .as_ref()
                .map(|cfg| cfg.endpoint.as_str()),
            Some("http://127.0.0.1:9000")
        );
    }
}
