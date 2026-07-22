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

mod common;
mod delete_files;
mod file_range;
mod iceberg_data;
mod iceberg_delta;
mod iceberg_metadata;
#[cfg(feature = "compat")]
mod native_starrocks;
mod read_plan;
mod schema;
#[cfg(feature = "compat")]
mod starrocks;
mod variant_path;
mod virtual_columns;

use super::node::{DecodedNode, NativePlanDecodeContext};
use super::{NativeFragmentDecodeError, error::NativeFragmentLeafDecodeError};
use crate::exec::expr::ExprArena;
use crate::proto::plan;
use crate::protocol::common::error::{FieldPath, ProtocolErrorKind};

pub(crate) fn lower_scan_node(
    node: &plan::DistributedNode,
    _physical: &plan::PlanNode,
    scan: &plan::ScanNode,
    path: FieldPath,
    ctx: &NativePlanDecodeContext,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    if !scan.dict_columns.is_empty() {
        return Err(NativeFragmentDecodeError::unsupported(
            path.clone().field("dict_columns"),
            "ScanNode dict_columns are not supported by native lowering yet",
        ));
    }
    let table = scan.table.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(path.clone().field("table"), "ScanNode table missing")
    })?;
    let source = table.source.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("table").field("source"),
            "ScanNode table source missing",
        )
    })?;
    let source = source.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("table").field("source").field("kind"),
            "ScanNode table source kind missing",
        )
    })?;
    let source_path = path.clone().field("table").field("source");
    let output_columns = common::decode_scan_output_columns(scan, path.clone())?;
    match source {
        plan::scan_source::Kind::IcebergDataFiles(source) => {
            let table_path = source_path
                .clone()
                .field("iceberg_data_files")
                .field("table");
            let table = source.table.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    table_path.clone(),
                    "IcebergDataFiles table missing",
                )
            })?;
            let variant_path_plan = variant_path::parse_native_scan_variant_path_columns(
                scan,
                table,
                output_columns.columns(),
            )
            .map_err(|error| error.into_native(path.clone()))?;
            schema::validate_decoded_iceberg_output_schema(
                table,
                source_path.clone().field("iceberg_data_files"),
                &output_columns,
                &variant_path_plan,
            )?;
            let read_plan = read_plan::scan_read_plan(
                scan,
                table,
                &output_columns,
                path.clone(),
                source_path.clone().field("iceberg_data_files"),
            )?;
            iceberg_data::lower_iceberg_data_files_scan(node, scan, source, read_plan, ctx, arena)
                .map_err(|error| error.into_native(source_path.field("iceberg_data_files")))
        }
        plan::scan_source::Kind::IcebergMetadataTable(source) => {
            reject_variant_columns_for_source(scan, "IcebergMetadataTable")
                .map_err(|error| error.into_native(path.clone()))?;
            iceberg_metadata::lower_iceberg_metadata_scan(
                node,
                scan,
                source,
                &output_columns,
                ctx,
                arena,
            )
            .map_err(|error| error.into_native(source_path.field("iceberg_metadata_table")))
        }
        plan::scan_source::Kind::IcebergDeltaTable(source) => {
            reject_variant_columns_for_source(scan, "IcebergDeltaTable")
                .map_err(|error| error.into_native(path.clone()))?;
            iceberg_delta::lower_iceberg_delta_table_scan(
                node,
                scan,
                source,
                &output_columns,
                arena,
            )
            .map_err(|error| error.into_native(source_path.field("iceberg_delta_table")))
        }
        plan::scan_source::Kind::IcebergVersionTable(_) => {
            Err(NativeFragmentDecodeError::unsupported(
                source_path.field("iceberg_version_table"),
                "IcebergVersionTable native scan source is not implemented",
            ))
        }
        plan::scan_source::Kind::IcebergMvTargetState(_) => {
            Err(NativeFragmentDecodeError::unsupported(
                source_path.field("iceberg_mv_target_state"),
                "IcebergMvTargetState native scan source is not implemented",
            ))
        }
        plan::scan_source::Kind::IcebergMvTargetLocator(_) => {
            Err(NativeFragmentDecodeError::unsupported(
                source_path.field("iceberg_mv_target_locator"),
                "IcebergMvTargetLocator native scan source is not implemented",
            ))
        }
        plan::scan_source::Kind::StarrocksTable(source) => {
            reject_variant_columns_for_source(scan, "StarRocksTable")
                .map_err(|error| error.into_native(path.clone()))?;
            #[cfg(feature = "compat")]
            {
                starrocks::validate_starrocks_output_columns(&output_columns, source)?;
                starrocks::lower_starrocks_scan(node, scan, source, &output_columns, ctx, arena)
                    .map_err(|error| error.into_native(source_path.field("starrocks_table")))
            }
            #[cfg(not(feature = "compat"))]
            {
                let _ = (node, scan, source, ctx, arena);
                Err(NativeFragmentDecodeError::unsupported(
                    source_path.field("starrocks_table"),
                    "StarRocks native scan requires feature compat",
                ))
            }
        }
    }
}

fn reject_variant_columns_for_source(
    scan: &plan::ScanNode,
    source_name: &str,
) -> Result<(), NativeFragmentLeafDecodeError> {
    if scan.variant_columns.is_empty() {
        return Ok(());
    }
    Err(NativeFragmentLeafDecodeError::at_field(
        ProtocolErrorKind::Unsupported,
        "variant_columns",
        format!("{source_name} native scan does not support variant_columns"),
    ))
}

#[cfg(test)]
pub(crate) fn scan_read_binding_for_test(
    scan: &plan::ScanNode,
    table: &plan::IcebergTableInfo,
    _output_columns: &[crate::proto::common::OutputColumn],
) -> Result<(Vec<String>, Vec<(u32, u32)>), NativeFragmentDecodeError> {
    let scan_path = FieldPath::root("scan");
    let output_columns = common::decode_scan_output_columns(scan, scan_path.clone())?;
    let read_plan = read_plan::scan_read_plan(
        scan,
        table,
        &output_columns,
        scan_path.clone(),
        scan_path
            .field("table")
            .field("source")
            .field("iceberg_data_files"),
    )?;
    Ok((
        read_plan.read_columns,
        read_plan
            .variant_path_columns
            .iter()
            .map(|variant| {
                (
                    variant.source_slot_id.as_u32(),
                    variant.output_slot_id.as_u32(),
                )
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use arrow::datatypes::DataType;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::super::node::{
        NativePlanDecodeContext, decode_node, decode_node_with_runtime_filters,
    };
    use super::super::runtime_filter::NativeRuntimeFilterDecodeLedger;
    use super::schema::iceberg_arrow_schema_from_output_columns;
    use crate::common::ids::SlotId;
    #[cfg(feature = "compat")]
    use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
    use crate::connector::iceberg::delete_file::{IcebergFileContent, IcebergFileFormat};
    use crate::connector::{ConnectorRegistry, HdfsScanConfig, ScanConfig, ScanConnector};
    #[cfg(feature = "compat")]
    use crate::connector::{StarRocksScanConfig, StarRocksScanOp};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::iceberg_delta_scan::DeltaSourceRole;
    use crate::exec::node::scan::ScanMorsel;
    use crate::formats::FileFormatConfig;
    use crate::proto::{common, expr, novarocks, plan};
    use crate::protocol::common::error::ProtocolErrorKind;
    use crate::protocol::native::type_mapping::encode_type;
    use crate::runtime_filter::model::contract::NullSemantics;
    use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            is_internal: false,
        }
    }

    fn column_def(name: &str, data_type: DataType) -> plan::ColumnDef {
        plan::ColumnDef {
            name: name.to_string(),
            data_type: Some(type_desc(&data_type)),
            nullable: true,
            write_default_json: None,
            logical_type: None,
        }
    }

    fn schema_field(field_id: i32, name: &str) -> plan::IcebergSchemaFieldDef {
        plan::IcebergSchemaFieldDef {
            field_id,
            name: name.to_string(),
            initial_default_json: None,
            write_default_json: None,
            children: Vec::new(),
        }
    }

    fn table_info() -> plan::IcebergTableInfo {
        plan::IcebergTableInfo {
            catalog: "rest".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: Some(1),
            schema_id: 7,
            location: "s3://bucket/warehouse/db/t".to_string(),
            schema: Some(plan::IcebergSchemaDef {
                fields: vec![schema_field(10, "id"), schema_field(11, "flag")],
            }),
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn variant_table_info() -> plan::IcebergTableInfo {
        plan::IcebergTableInfo {
            schema: Some(plan::IcebergSchemaDef {
                fields: vec![schema_field(101, "v")],
            }),
            ..table_info()
        }
    }

    fn scan_node(source: plan::scan_source::Kind) -> plan::DistributedNode {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        scan_node_with(columns, Vec::new(), Vec::new(), source)
    }

    fn scan_node_with(
        columns: Vec<common::OutputColumn>,
        predicates: Vec<expr::Expr>,
        required_columns: Vec<String>,
        source: plan::scan_source::Kind,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: columns.clone(),
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: Some(plan::TableDef {
                        name: "t".to_string(),
                        columns: vec![
                            column_def("id", DataType::Int64),
                            column_def("flag", DataType::Boolean),
                        ],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource { kind: Some(source) }),
                    }),
                    alias: None,
                    columns,
                    predicates,
                    required_columns,
                    dict_columns: Vec::new(),
                    variant_columns: Vec::new(),
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    fn assert_scan_column_type_error(
        columns: Vec<common::OutputColumn>,
        required_columns: Vec<String>,
        expected_index: usize,
    ) {
        let node = scan_node_with(
            columns,
            Vec::new(),
            required_columns,
            iceberg_metadata_table_source(),
        );
        let error = decode_node(
            &node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("invalid scan column type must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            format!("plan_fragment.root.payload.physical.scan.columns[{expected_index}].type")
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
    }

    fn variant_scan_node() -> plan::DistributedNode {
        variant_scan_node_with_source_ids(1, 1)
    }

    fn variant_scan_node_with_source_ids(
        variant_source_column_id: u32,
        scan_source_column_id: u32,
    ) -> plan::DistributedNode {
        let output_columns = vec![output_column(2, "__nr_var_v_0", DataType::Int64)];
        let scan_columns = vec![
            output_column(scan_source_column_id, "v", DataType::LargeBinary),
            output_column(2, "__nr_var_v_0", DataType::Int64),
        ];
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: Some(plan::TableDef {
                        name: "t".to_string(),
                        columns: vec![
                            column_def("v", DataType::LargeBinary),
                            column_def("__nr_var_v_0", DataType::Int64),
                        ],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource {
                            kind: Some(plan::scan_source::Kind::IcebergDataFiles(
                                plan::IcebergDataFiles {
                                    table: Some(variant_table_info()),
                                    files: Vec::new(),
                                    cloud_properties: HashMap::new(),
                                    binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
                                },
                            )),
                        }),
                    }),
                    alias: None,
                    columns: scan_columns,
                    predicates: Vec::new(),
                    required_columns: vec!["__nr_var_v_0".to_string()],
                    dict_columns: Vec::new(),
                    variant_columns: vec![plan::ScanVariantColumn {
                        source_column_id: variant_source_column_id,
                        source_column: "v".to_string(),
                        synthetic_column_id: 2,
                        synthetic_column: "__nr_var_v_0".to_string(),
                        canonical_path: "$.a.b".to_string(),
                        requested_type: Some(type_desc(&DataType::Int64)),
                        strict: true,
                    }],
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    #[derive(Clone)]
    struct CapturingHdfsConnector {
        captured: Arc<Mutex<Option<HdfsScanConfig>>>,
    }

    impl ScanConnector for CapturingHdfsConnector {
        fn name(&self) -> &'static str {
            "hdfs"
        }

        fn create_scan_node(
            &self,
            cfg: ScanConfig,
        ) -> Result<crate::exec::node::scan::ScanNode, String> {
            let ScanConfig::Hdfs(cfg) = cfg else {
                return Err("capturing hdfs connector received non-HDFS config".to_string());
            };
            let cfg = *cfg;
            *self.captured.lock().expect("captured hdfs config lock") = Some(cfg.clone());
            Ok(crate::exec::node::scan::ScanNode::new(Arc::new(
                crate::connector::hdfs::HdfsScanOp::new(cfg),
            )))
        }
    }

    fn capturing_hdfs_registry() -> (Arc<ConnectorRegistry>, Arc<Mutex<Option<HdfsScanConfig>>>) {
        let captured = Arc::new(Mutex::new(None));
        let mut registry = ConnectorRegistry::default();
        registry.register_scan_connector(Arc::new(CapturingHdfsConnector {
            captured: Arc::clone(&captured),
        }));
        (Arc::new(registry), captured)
    }

    fn column_ref(column_id: u32, name: &str, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: Some(name.to_string()),
            })),
        }
    }

    fn membership_consumer_binding(binding_id: u32, node_id: i32) -> plan::RuntimeFilterBinding {
        let schema = ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches)
            .expect("membership schema");
        plan::RuntimeFilterBinding {
            binding_id,
            channel_id: 9,
            node_id,
            apply_point: i32::from(plan::RuntimeFilterApplyPoint::NodeInput),
            expression: Some(column_ref(1, "id", DataType::Int64)),
            contract: Some(plan::RuntimeFilterContract {
                kind: Some(plan::runtime_filter_contract::Kind::Membership(
                    plan::RuntimeFilterMembershipContract {
                        canonical_schema: schema.canonical_bytes().to_vec(),
                        schema_digest: schema.digest().bytes().to_vec(),
                    },
                )),
            }),
            reduction: Some(plan::RuntimeFilterReductionContract {
                kind: Some(plan::runtime_filter_reduction_contract::Kind::SetUnion(
                    true,
                )),
            }),
            role: Some(plan::runtime_filter_binding::Role::Consumer(
                plan::RuntimeFilterConsumerRole {
                    capabilities: vec![
                        i32::from(plan::RuntimeFilterArtifactCapability::Membership),
                        i32::from(plan::RuntimeFilterArtifactCapability::EmptyDomain),
                    ],
                    activation: Some(plan::RuntimeFilterConsumerActivation {
                        kind: Some(
                            plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true),
                        ),
                    }),
                    target: Some(plan::runtime_filter_consumer_role::Target::SourceBoundary(
                        true,
                    )),
                },
            )),
        }
    }

    fn lower_delta_scan_with_binding(
        node: &mut plan::DistributedNode,
    ) -> super::super::node::DecodedNode {
        node.runtime_filter_binding_ids = vec![1];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: node.fragment_id,
            bindings: vec![membership_consumer_binding(1, node.node_id)],
        };
        let mut ledger = NativeRuntimeFilterDecodeLedger::decode(node.fragment_id, Some(&table))
            .expect("decode delta-scan consumer table");
        let lowered = decode_node_with_runtime_filters(
            node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
            &mut ledger,
        )
        .expect("lower delta scan with dormant leaf-local consumer");
        ledger.finish().expect("delta-scan consumer consumed");
        lowered
    }

    fn file_range() -> novarocks::ScanRangeParams {
        novarocks::ScanRangeParams {
            range: Some(novarocks::ScanRange {
                kind: Some(novarocks::scan_range::Kind::File(
                    novarocks::FileScanRange {
                        file_format: "PARQUET".to_string(),
                        full_path: Some("s3://bucket/warehouse/db/t/data-1.parquet".to_string()),
                        relative_path: None,
                        table_id: None,
                        offset: 0,
                        length: 10,
                        file_length: 10,
                        delete_files: Vec::new(),
                        deletion_vector_descriptor: None,
                        first_row_id: None,
                        data_sequence_number: None,
                        modification_time: None,
                        datacache_options: None,
                        included_positions: Vec::new(),
                        serialized_split: None,
                        use_iceberg_jni_metadata_reader: false,
                        change_op: None,
                        file_pruning_min_max_values: HashMap::new(),
                    },
                )),
            }),
            volume_id: None,
            empty: None,
            has_more: None,
        }
    }

    fn starrocks_source() -> plan::scan_source::Kind {
        plan::scan_source::Kind::StarrocksTable(plan::StarRocksTableSource {
            catalog_name: "default_catalog".to_string(),
            db_id: 10,
            table_id: 20,
            schema_id: 30,
            storage_columns: vec![
                plan::StarRocksColumnStorageMeta {
                    name: "id".to_string(),
                    unique_id: 0,
                    default_value: None,
                },
                plan::StarRocksColumnStorageMeta {
                    name: "flag".to_string(),
                    unique_id: 12,
                    default_value: Some("false".to_string()),
                },
            ],
            current_schema: Some(plan::StarRocksTabletSchema {
                schema_id: 30,
                keys_type: plan::StarRocksKeysType::StarrocksKeysTypeDuplicate as i32,
                num_short_key_columns: Some(1),
                sort_key_idxes: vec![0],
                sort_key_unique_ids: vec![0],
                columns: vec![
                    plan::StarRocksColumnSchema {
                        unique_id: 0,
                        name: Some("id".to_string()),
                        physical_type: "BIGINT".to_string(),
                        is_key: Some(true),
                        aggregation: None,
                        nullable: Some(false),
                        default_value: None,
                        precision: None,
                        scale: None,
                        visible: Some(true),
                        children: vec![],
                    },
                    plan::StarRocksColumnSchema {
                        unique_id: 12,
                        name: Some("flag".to_string()),
                        physical_type: "BOOLEAN".to_string(),
                        is_key: Some(false),
                        aggregation: None,
                        nullable: Some(false),
                        default_value: Some("false".to_string()),
                        precision: None,
                        scale: None,
                        visible: Some(true),
                        children: vec![],
                    },
                ],
            }),
        })
    }

    fn starrocks_range(
        tablet_id: i64,
        partition_id: i64,
        version: i64,
    ) -> novarocks::ScanRangeParams {
        novarocks::ScanRangeParams {
            range: Some(novarocks::ScanRange {
                kind: Some(novarocks::scan_range::Kind::StarrocksTablet(
                    novarocks::StarRocksTabletScanRange {
                        tablet_id,
                        partition_id,
                        version,
                    },
                )),
            }),
            volume_id: None,
            empty: Some(false),
            has_more: Some(false),
        }
    }

    fn starrocks_empty_range(
        kind: Option<novarocks::scan_range::Kind>,
    ) -> novarocks::ScanRangeParams {
        novarocks::ScanRangeParams {
            range: kind.map(|kind| novarocks::ScanRange { kind: Some(kind) }),
            volume_id: None,
            empty: Some(true),
            has_more: Some(false),
        }
    }

    #[cfg(feature = "compat")]
    #[derive(Clone)]
    struct CapturingStarRocksConnector {
        captured: Arc<Mutex<Option<StarRocksScanConfig>>>,
    }

    #[cfg(feature = "compat")]
    impl ScanConnector for CapturingStarRocksConnector {
        fn name(&self) -> &'static str {
            "starrocks"
        }

        fn create_scan_node(
            &self,
            cfg: ScanConfig,
        ) -> Result<crate::exec::node::scan::ScanNode, String> {
            let ScanConfig::StarRocks(cfg) = cfg else {
                return Err("capturing StarRocks connector received non-StarRocks config".into());
            };
            let cfg = *cfg;
            *self
                .captured
                .lock()
                .expect("captured StarRocks config lock") = Some(cfg.clone());
            Ok(crate::exec::node::scan::ScanNode::new(Arc::new(
                StarRocksScanOp::new(cfg),
            )))
        }
    }

    #[cfg(feature = "compat")]
    fn capturing_starrocks_registry() -> (
        Arc<ConnectorRegistry>,
        Arc<Mutex<Option<StarRocksScanConfig>>>,
    ) {
        let captured = Arc::new(Mutex::new(None));
        let mut registry = ConnectorRegistry::new();
        registry.register_scan_connector(Arc::new(CapturingStarRocksConnector {
            captured: Arc::clone(&captured),
        }));
        (Arc::new(registry), captured)
    }

    #[cfg(not(feature = "compat"))]
    #[test]
    fn rejects_starrocks_native_scan_without_compat_feature() {
        let node = scan_node(starrocks_source());
        let ctx = NativePlanDecodeContext::default()
            .with_scan_ranges(10, vec![starrocks_range(300, 100, 7)]);
        let mut arena = ExprArena::default();

        let err = decode_node(&node, &mut arena, &ctx)
            .expect_err("StarRocks native scan requires compat connector support");
        assert_eq!(err, "StarRocks native scan requires feature compat");
    }

    #[cfg(feature = "compat")]
    #[test]
    fn native_starrocks_scan_decode_defers_tablet_resolution_without_registry_mutation() {
        let _guard = crate::connector::starrocks::lake::context::lock_runtime_test_state();
        let tablet_id = 8_700_000_000_000_001;
        let query_id = crate::runtime::query_context::QueryId {
            hi: 8_700_000_000_000_002,
            lo: 8_700_000_000_000_003,
        };
        let node = scan_node(starrocks_source());
        let (connectors, captured) = capturing_starrocks_registry();
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(connectors)
            .with_query_id(query_id)
            .with_scan_ranges(10, vec![starrocks_range(tablet_id, 100, 7)]);
        let mut arena = ExprArena::default();
        assert!(crate::runtime::starlet_shard_registry::select_paths(&[tablet_id]).is_empty());

        decode_node(&node, &mut arena, &ctx)
            .expect("valid StarRocks scan decode must defer tablet-path resolution");

        let cfg = captured
            .lock()
            .expect("captured StarRocks config lock")
            .clone()
            .expect("StarRocks connector config");
        assert!(!cfg.properties.contains_key("partition_storage_paths"));
        assert!(cfg.deferred_lake_resolution.is_some());
        assert!(crate::runtime::starlet_shard_registry::select_paths(&[tablet_id]).is_empty());
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn greater_than(left: expr::Expr, right: expr::Expr) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: true,
            kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Gt as i32,
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
            }))),
        }
    }

    fn equals(left: expr::Expr, right: expr::Expr) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: true,
            kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Eq as i32,
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
            }))),
        }
    }

    fn not_equals(left: expr::Expr, right: expr::Expr) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: true,
            kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Ne as i32,
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
            }))),
        }
    }

    fn iceberg_delta_table_source() -> plan::scan_source::Kind {
        plan::scan_source::Kind::IcebergDeltaTable(plan::IcebergDeltaTable {
            table: Some(table_info()),
            from_snapshot_id: 1,
            to_snapshot_id: 2,
            delta_plan: Some(plan::IcebergDeltaScanPlan {
                table_location: "file:///tmp/novarocks-delta-table".to_string(),
                data_columns: vec![plan::IcebergDeltaDataColumn {
                    name: "id".to_string(),
                    field_id: 10,
                }],
                cloud_properties: HashMap::new(),
                change_files: vec![plan::IcebergDeltaSourceFile {
                    path: "file:///tmp/novarocks-delta-table/data-1.parquet".to_string(),
                    size: 10,
                    role: plan::IcebergDeltaSourceRole::DataFile as i32,
                    partition_spec_id: Some(0),
                    partition_key: None,
                    first_row_id: Some(100),
                    data_sequence_number: Some(7),
                    row_id_allow_list: Vec::new(),
                    position_deletes: Vec::new(),
                    equality_field_ids: Vec::new(),
                    equality_targets: Vec::new(),
                    deleted_file_visibility: None,
                }],
                delete_side: None,
            }),
        })
    }

    fn iceberg_metadata_table_source() -> plan::scan_source::Kind {
        plan::scan_source::Kind::IcebergMetadataTable(plan::IcebergMetadataTable {
            table: Some(table_info()),
            metadata_table_type: plan::IcebergMetadataTableType::Snapshots as i32,
            serialized_table: "{}".to_string(),
            cloud_properties: HashMap::new(),
            metadata_payload: None,
        })
    }

    fn file_range_with_deletion_vector() -> novarocks::ScanRangeParams {
        let mut range = file_range();
        let Some(novarocks::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        file.deletion_vector_descriptor = Some(novarocks::DeletionVectorDescriptor {
            storage_type: Some("PUFFIN".to_string()),
            path_or_inline_dv: Some("s3://bucket/warehouse/db/t/delete-1.puffin".to_string()),
            offset: Some(12),
            size_in_bytes: Some(34),
            cardinality: Some(2),
        });
        range
    }

    fn file_range_with_change_op_and_pruning() -> novarocks::ScanRangeParams {
        let mut range = file_range();
        let Some(novarocks::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        file.change_op = Some(crate::exec::change_op::CHANGE_OP_DELETE.into());
        file.file_pruning_min_max_values = HashMap::from([(
            0,
            novarocks::FilePruningMinMaxValue {
                value_kind: 2,
                has_null: true,
                all_null: false,
                min_int_value: Some(10),
                max_int_value: Some(20),
                min_float_value: None,
                max_float_value: None,
            },
        )]);
        range
    }

    #[test]
    fn lowers_iceberg_data_file_scan_to_scan_node() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let lowered = decode_node(&node, &mut arena, &ctx).expect("lower native scan");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        assert_eq!(scan.node_id(), Some(10));
        assert_eq!(scan.output_chunk_schema().slot_ids(), &[SlotId::new(1)]);
    }

    #[test]
    fn iceberg_metadata_invalid_column_type_uses_scan_columns_wire_path() {
        let mut invalid = output_column(1, "id", DataType::Int64);
        invalid.r#type = Some(common::TypeDesc::default());
        assert_scan_column_type_error(vec![invalid], Vec::new(), 0);
    }

    #[test]
    fn required_column_filter_preserves_original_scan_column_index() {
        let preceding = output_column(1, "id", DataType::Int64);
        let mut invalid = output_column(2, "flag", DataType::Boolean);
        invalid.r#type = Some(common::TypeDesc::default());
        assert_scan_column_type_error(vec![preceding, invalid], vec!["flag".to_string()], 1);
    }

    fn iceberg_data_files_source() -> plan::scan_source::Kind {
        plan::scan_source::Kind::IcebergDataFiles(plan::IcebergDataFiles {
            table: Some(table_info()),
            files: Vec::new(),
            cloud_properties: HashMap::new(),
            binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
        })
    }

    fn append_hidden_table_column(
        node: &mut plan::DistributedNode,
        name: &str,
        data_type: Option<common::TypeDesc>,
        logical_type: Option<common::TypeDesc>,
    ) {
        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut()
        else {
            panic!("expected physical node");
        };
        let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_mut() else {
            panic!("expected scan node");
        };
        let table = scan.table.as_mut().expect("scan table");
        table.columns.push(plan::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            write_default_json: None,
            logical_type,
        });
        let Some(plan::scan_source::Kind::IcebergDataFiles(source)) = table
            .source
            .as_mut()
            .and_then(|source| source.kind.as_mut())
        else {
            panic!("expected Iceberg data source");
        };
        source
            .table
            .as_mut()
            .expect("Iceberg table")
            .schema
            .as_mut()
            .expect("Iceberg schema")
            .fields
            .push(schema_field(12, name));
    }

    fn assert_hidden_table_column_type_error(node: &plan::DistributedNode, expected_field: &str) {
        let error = decode_node(
            node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("invalid hidden table column type must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            format!("plan_fragment.root.payload.physical.scan.table.columns[2].{expected_field}")
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
    }

    #[test]
    fn required_hidden_table_column_type_uses_table_wire_path() {
        let mut node = scan_node_with(
            vec![output_column(1, "id", DataType::Int64)],
            Vec::new(),
            vec!["id".to_string(), "hidden_required".to_string()],
            iceberg_data_files_source(),
        );
        append_hidden_table_column(
            &mut node,
            "hidden_required",
            Some(type_desc(&DataType::Int64)),
            Some(common::TypeDesc::default()),
        );
        assert_hidden_table_column_type_error(&node, "logical_type");
    }

    #[test]
    fn predicate_hidden_table_column_type_uses_table_wire_path() {
        let mut node = scan_node_with(
            vec![output_column(1, "id", DataType::Int64)],
            vec![greater_than(
                column_ref(3, "hidden_predicate", DataType::Int64),
                int_literal(0),
            )],
            Vec::new(),
            iceberg_data_files_source(),
        );
        append_hidden_table_column(
            &mut node,
            "hidden_predicate",
            Some(common::TypeDesc::default()),
            None,
        );
        assert_hidden_table_column_type_error(&node, "data_type");
    }

    #[test]
    fn iceberg_projection_schema_error_uses_scan_column_wire_path() {
        let node = scan_node_with(
            vec![output_column(1, "missing", DataType::Int64)],
            Vec::new(),
            Vec::new(),
            plan::scan_source::Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            }),
        );
        let error = decode_node(
            &node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("missing Iceberg projection field must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.scan.columns[0].name"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
    }

    #[cfg(feature = "compat")]
    #[test]
    fn starrocks_storage_schema_error_uses_scan_column_wire_path() {
        let node = scan_node_with(
            vec![output_column(1, "missing", DataType::Int64)],
            Vec::new(),
            Vec::new(),
            starrocks_source(),
        );
        let error = decode_node(
            &node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("missing StarRocks storage column must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.scan.columns[0].name"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InconsistentFields);
    }

    #[test]
    fn file_range_offset_error_uses_exact_indexed_path() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let mut range = file_range();
        let Some(novarocks::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        file.offset = -1;
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![range]);

        let error = decode_node(&node, &mut ExprArena::default(), &ctx)
            .expect_err("negative file offset must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.scan.table.source.iceberg_data_files.ranges[0].offset"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
    }

    #[test]
    fn delete_file_error_uses_exact_indexed_descriptor_path() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let mut range = file_range();
        let Some(novarocks::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        file.delete_files.push(novarocks::IcebergDeleteFile {
            full_path: None,
            file_format: "PARQUET".to_string(),
            file_content: "POSITION_DELETES".to_string(),
            length: Some(1),
        });
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![range]);

        let error = decode_node(&node, &mut ExprArena::default(), &ctx)
            .expect_err("missing delete file path must fail");
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.scan.table.source.iceberg_data_files.ranges[0].delete_files[0].full_path"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn lowers_iceberg_data_file_scan_deletion_vector_to_puffin_delete_file() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range_with_deletion_vector()]);
        let mut arena = ExprArena::default();
        let lowered = decode_node(&node, &mut arena, &ctx).expect("lower native scan");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        let morsels = scan.build_morsels().expect("build morsels");
        let [ScanMorsel::FileRange { delete_files, .. }] = morsels.morsels.as_slice() else {
            panic!("expected one file morsel, got {:?}", morsels.morsels);
        };
        assert_eq!(delete_files.len(), 1);
        let dv = &delete_files[0];
        assert_eq!(dv.file_format, IcebergFileFormat::Puffin);
        assert_eq!(dv.file_content, IcebergFileContent::PositionDeletes);
        assert_eq!(
            dv.path,
            "s3://bucket/warehouse/db/t/delete-1.puffin".to_string()
        );
        assert_eq!(dv.content_offset, Some(12));
        assert_eq!(dv.content_size_in_bytes, Some(34));
    }

    #[test]
    fn lowers_iceberg_data_file_scan_change_op_and_pruning_metadata() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range_with_change_op_and_pruning()]);
        let mut arena = ExprArena::default();
        let lowered = decode_node(&node, &mut arena, &ctx).expect("lower native scan");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        let morsels = scan.build_morsels().expect("build morsels");
        let [
            ScanMorsel::FileRange {
                ivm_change_op,
                iceberg_file_pruning,
                ..
            },
        ] = morsels.morsels.as_slice()
        else {
            panic!("expected one file morsel, got {:?}", morsels.morsels);
        };
        assert_eq!(
            *ivm_change_op,
            Some(crate::exec::change_op::CHANGE_OP_DELETE)
        );
        let pruning = iceberg_file_pruning
            .as_ref()
            .expect("file pruning metadata");
        let stats = pruning.columns.get("id").expect("id stats");
        assert_eq!(stats.null_count, Some(1));
        assert_eq!(stats.lower_bound, Some(10_i64.to_le_bytes().to_vec()));
        assert_eq!(stats.upper_bound, Some(20_i64.to_le_bytes().to_vec()));
    }

    #[test]
    fn rejects_native_file_pruning_ordinal_outside_iceberg_schema() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let mut range = file_range_with_change_op_and_pruning();
        let Some(novarocks::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        let value = file
            .file_pruning_min_max_values
            .remove(&0)
            .expect("test pruning value");
        file.file_pruning_min_max_values.insert(2, value);

        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![range]);
        let mut arena = ExprArena::default();
        let err = decode_node(&node, &mut arena, &ctx)
            .expect_err("reject out-of-range file pruning ordinal");
        assert!(
            err.contains("file pruning ordinal 2 exceeds Iceberg schema field count 2"),
            "{err}"
        );
    }

    #[test]
    fn rejects_native_file_pruning_unspecified_value_kind() {
        let mut range = file_range_with_change_op_and_pruning();
        let Some(novarocks::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        file.file_pruning_min_max_values
            .get_mut(&0)
            .expect("test pruning value")
            .value_kind = 0;

        let err = super::super::decode_scan_range_params(&range)
            .expect_err("reject unspecified file pruning value kind");
        assert!(
            err.to_string()
                .contains("unsupported file pruning value_kind 0"),
            "{err}"
        );
    }

    #[test]
    fn lowers_native_iceberg_scan_variant_path_columns() {
        let node = variant_scan_node();
        let (registry, captured_hdfs) = capturing_hdfs_registry();
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(registry)
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();

        let lowered = decode_node(&node, &mut arena, &ctx)
            .expect("lower native scan with variant path column");

        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(2)]);
        let scan = match lowered.node.kind {
            ExecNodeKind::Scan(scan) => scan,
            ExecNodeKind::Project(project) => {
                assert!(project.is_subordinate);
                assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(2)]);
                let ExecNodeKind::Scan(scan) = project.input.kind else {
                    panic!("expected project input scan");
                };
                scan
            }
            other => panic!("expected Scan or Project over Scan, got {other:?}"),
        };
        assert_eq!(scan.output_chunk_schema().slot_ids(), &[SlotId::new(2)]);

        let hdfs_cfg = captured_hdfs
            .lock()
            .expect("captured hdfs config lock")
            .clone()
            .expect("captured hdfs config");
        let Some(FileFormatConfig::Parquet(parquet_cfg)) = hdfs_cfg.format else {
            panic!("expected parquet scan config");
        };
        assert_eq!(parquet_cfg.columns, vec!["v".to_string()]);
        assert_eq!(parquet_cfg.chunk_schema.slot_ids(), &[SlotId::new(3)]);
        assert_eq!(parquet_cfg.variant_path_columns.len(), 1);
        let spec = &parquet_cfg.variant_path_columns[0];
        assert_eq!(spec.source_slot_id, SlotId::new(1));
        assert_eq!(spec.source_read_slot_id, SlotId::new(3));
        assert_eq!(spec.output_slot_id, SlotId::new(2));
        assert_eq!(spec.source_name, "v");
        assert_eq!(spec.output_name, "__nr_var_v_0");
        assert_eq!(spec.canonical_path, "$.a.b");
        assert_eq!(spec.requested_type, DataType::Int64);
        assert!(spec.strict);
        assert_eq!(spec.source_field_id, Some(101));
    }

    #[test]
    fn native_variant_source_hidden_slot_reserves_source_slot_id() {
        let node = variant_scan_node_with_source_ids(3, 3);
        let (registry, captured_hdfs) = capturing_hdfs_registry();
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(registry)
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();

        let lowered = decode_node(&node, &mut arena, &ctx)
            .expect("lower native scan with colliding source slot");

        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(2)]);
        let hdfs_cfg = captured_hdfs
            .lock()
            .expect("captured hdfs config lock")
            .clone()
            .expect("captured hdfs config");
        let Some(FileFormatConfig::Parquet(parquet_cfg)) = hdfs_cfg.format else {
            panic!("expected parquet scan config");
        };
        assert_eq!(parquet_cfg.chunk_schema.slot_ids(), &[SlotId::new(4)]);
        let spec = &parquet_cfg.variant_path_columns[0];
        assert_eq!(spec.source_slot_id, SlotId::new(3));
        assert_eq!(spec.source_read_slot_id, SlotId::new(4));
        assert_ne!(spec.source_read_slot_id, spec.source_slot_id);
    }

    #[test]
    fn rejects_native_variant_source_id_name_mismatch() {
        let node = variant_scan_node_with_source_ids(4, 3);
        let (registry, _) = capturing_hdfs_registry();
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(registry)
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();

        let err = decode_node(&node, &mut arena, &ctx).expect_err("reject source id/name drift");

        assert!(
            err.contains("source_column_id=4 is not a scan column"),
            "{err}"
        );
    }

    #[test]
    fn lowers_iceberg_delta_table_scan_from_native_payload() {
        let node = scan_node(iceberg_delta_table_source());
        let ctx = NativePlanDecodeContext::default();
        let mut arena = ExprArena::default();
        let lowered = decode_node(&node, &mut arena, &ctx).expect("lower native delta scan");
        let ExecNodeKind::IcebergDeltaScan(scan) = lowered.node.kind else {
            panic!("expected IcebergDeltaScan");
        };
        assert_eq!(scan.node_id, 10);
        assert_eq!(scan.base_table_ident.catalog, "rest");
        assert_eq!(scan.from_snapshot_id, 1);
        assert_eq!(scan.to_snapshot_id, 2);
        assert_eq!(scan.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(scan.change_files.len(), 1);
        assert_eq!(
            scan.change_files[0].path,
            "file:///tmp/novarocks-delta-table/data-1.parquet"
        );
        assert!(matches!(
            scan.change_files[0].role,
            DeltaSourceRole::DataFile
        ));
    }

    #[test]
    fn lowering_iceberg_delta_table_scan_does_not_open_delete_files() {
        let mut source = iceberg_delta_table_source();
        let plan::scan_source::Kind::IcebergDeltaTable(delta_source) = &mut source else {
            unreachable!();
        };
        delta_source.delta_plan.as_mut().unwrap().delete_side =
            Some(plan::IcebergDeltaDeleteSidePlan {
                base_data_file_lineage: HashMap::new(),
                previous_data_file_lineage: HashMap::new(),
                previous_delete_visibility_data_files: vec![
                    plan::IcebergDeltaDeleteVisibilityDataFile {
                        path: "file:///definitely-missing/data.parquet".to_string(),
                        size: 1,
                        first_row_id: Some(0),
                        data_sequence_number: Some(1),
                        delete_files: vec![plan::IcebergDeltaDeleteVisibilityDeleteFile {
                            path: "file:///definitely-missing/delete.parquet".to_string(),
                            file_format: plan::IcebergDeltaDeleteFileFormat::Parquet as i32,
                            file_content: plan::IcebergDeltaDeleteFileContent::Position as i32,
                            length: Some(1),
                            content_offset: None,
                            content_size_in_bytes: None,
                        }],
                    },
                ],
                previously_deleted_positions_per_file: HashMap::new(),
                deleted_data_file_paths: Vec::new(),
            });
        let node = scan_node(source);
        let ctx = NativePlanDecodeContext::default();
        let mut arena = ExprArena::default();

        let lowered = decode_node(&node, &mut arena, &ctx)
            .expect("native fragment decoding must not touch Iceberg storage");
        let ExecNodeKind::IcebergDeltaScan(_scan) = lowered.node.kind else {
            panic!("expected IcebergDeltaScan");
        };
    }

    #[test]
    fn lowers_iceberg_delta_table_scan_predicates_to_filter() {
        let node = scan_node_with(
            vec![output_column(1, "id", DataType::Int64)],
            vec![greater_than(
                column_ref(1, "id", DataType::Int64),
                int_literal(10),
            )],
            Vec::new(),
            iceberg_delta_table_source(),
        );
        let ctx = NativePlanDecodeContext::default();
        let mut arena = ExprArena::default();
        let lowered =
            decode_node(&node, &mut arena, &ctx).expect("lower native delta scan with predicate");
        let ExecNodeKind::Filter(filter) = lowered.node.kind else {
            panic!("expected Filter wrapper");
        };
        assert_eq!(filter.node_id, 10);
        assert!(matches!(
            arena.node(filter.predicate),
            Some(ExprNode::Gt(_, _))
        ));
        let ExecNodeKind::IcebergDeltaScan(scan) = filter.input.kind else {
            panic!("expected Filter input IcebergDeltaScan");
        };
        assert_eq!(scan.node_id, 10);
        assert_eq!(scan.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_iceberg_delta_table_scan_with_leaf_local_dormant_consumer() {
        let mut node = scan_node(iceberg_delta_table_source());
        let lowered = lower_delta_scan_with_binding(&mut node);
        let ExecNodeKind::IcebergDeltaScan(scan) = lowered.node.kind else {
            panic!("expected IcebergDeltaScan");
        };
        assert_eq!(scan.native_runtime_filter_specs().len(), 1);
    }

    #[test]
    fn lowers_filtered_iceberg_delta_table_scan_with_leaf_local_dormant_consumer() {
        let mut node = scan_node_with(
            vec![output_column(1, "id", DataType::Int64)],
            vec![greater_than(
                column_ref(1, "id", DataType::Int64),
                int_literal(10),
            )],
            Vec::new(),
            iceberg_delta_table_source(),
        );
        let lowered = lower_delta_scan_with_binding(&mut node);
        let ExecNodeKind::Filter(filter) = lowered.node.kind else {
            panic!("expected Filter wrapper");
        };
        let ExecNodeKind::IcebergDeltaScan(scan) = filter.input.kind else {
            panic!("expected Filter input IcebergDeltaScan");
        };
        assert_eq!(scan.native_runtime_filter_specs().len(), 1);
    }

    #[test]
    fn lowers_iceberg_metadata_scan_predicate_to_scan_conjunct() {
        let node = scan_node_with(
            vec![output_column(1, "snapshot_id", DataType::Int64)],
            vec![greater_than(
                column_ref(1, "snapshot_id", DataType::Int64),
                int_literal(0),
            )],
            Vec::new(),
            iceberg_metadata_table_source(),
        );
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();

        let lowered =
            decode_node(&node, &mut arena, &ctx).expect("lower metadata scan with predicate");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        assert!(scan.conjunct_predicate().is_some());
    }

    #[test]
    fn iceberg_data_file_scan_output_schema_carries_field_ids() {
        let schema = iceberg_arrow_schema_from_output_columns(
            &table_info(),
            &[output_column(1, "id", DataType::Int64)],
        )
        .expect("iceberg schema");
        assert_eq!(
            schema.field(0).metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"10".to_string())
        );
    }

    #[test]
    fn iceberg_data_file_scan_accepts_file_and_pos_virtual_columns() {
        let columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "_file", DataType::Utf8),
            output_column(3, "_pos", DataType::Int64),
            output_column(4, "_row_id", DataType::Int64),
            output_column(5, "_last_updated_sequence_number", DataType::Int64),
        ];
        let schema = iceberg_arrow_schema_from_output_columns(&table_info(), &columns)
            .expect("iceberg output schema");
        assert_eq!(schema.field(1).name(), "_file");
        assert_eq!(schema.field(2).name(), "_pos");
        assert_eq!(schema.field(3).name(), "_row_id");
        assert_eq!(schema.field(4).name(), "_last_updated_sequence_number");
        assert!(
            !schema
                .field(1)
                .metadata()
                .contains_key(PARQUET_FIELD_ID_META_KEY)
        );
        assert!(
            !schema
                .field(2)
                .metadata()
                .contains_key(PARQUET_FIELD_ID_META_KEY)
        );
        assert_eq!(
            schema.field(3).metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string())
        );

        let node = scan_node_with(
            columns,
            Vec::new(),
            Vec::new(),
            plan::scan_source::Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            }),
        );
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let lowered = decode_node(&node, &mut arena, &ctx).expect("lower native scan");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        assert_eq!(
            scan.output_chunk_schema().slot_ids(),
            &[
                SlotId::new(1),
                SlotId::new(2),
                SlotId::new(3),
                SlotId::new(4),
                SlotId::new(5)
            ]
        );
        let virtual_spec = scan.iceberg_virtual().expect("iceberg virtual spec");
        assert_eq!(virtual_spec.file_path_slot, Some(SlotId::new(2)));
        assert_eq!(virtual_spec.row_pos_slot, Some(SlotId::new(3)));
        assert_eq!(virtual_spec.row_id_slot, Some(SlotId::new(4)));
        assert_eq!(virtual_spec.last_updated_seq_slot, Some(SlotId::new(5)));
    }

    #[test]
    fn duplicate_iceberg_virtual_column_uses_conflicting_name_wire_path() {
        let node = scan_node_with(
            vec![
                output_column(1, "id", DataType::Int64),
                output_column(2, "_file", DataType::Utf8),
                output_column(3, "_file", DataType::Utf8),
            ],
            Vec::new(),
            Vec::new(),
            iceberg_data_files_source(),
        );
        let error = decode_node(
            &node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("duplicate virtual column must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.scan.columns[2].name"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InconsistentFields);
    }

    #[test]
    fn virtual_carrier_id_overflow_uses_max_column_id_wire_path() {
        let node = scan_node_with(
            vec![output_column(u32::MAX, "_row_id", DataType::Int64)],
            Vec::new(),
            Vec::new(),
            iceberg_data_files_source(),
        );
        let error = decode_node(
            &node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("virtual carrier column id overflow must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.scan.columns[0].column_id"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
    }

    #[test]
    fn iceberg_virtual_only_scan_reads_count_carrier_and_projects_outputs() {
        let node = scan_node_with(
            vec![output_column(4, "_row_id", DataType::Int64)],
            Vec::new(),
            Vec::new(),
            plan::scan_source::Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            }),
        );
        let (registry, captured) = capturing_hdfs_registry();
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(registry)
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let lowered = decode_node(&node, &mut arena, &ctx).expect("lower virtual-only native scan");

        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(4)]);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected scan wrapper project");
        };
        assert!(project.is_subordinate);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(4)]);
        let ExecNodeKind::Scan(scan) = project.input.kind else {
            panic!("expected project input scan");
        };
        assert_eq!(
            scan.output_chunk_schema().slot_ids(),
            &[SlotId::new(4), SlotId::new(5)]
        );
        let virtual_spec = scan.iceberg_virtual().expect("iceberg virtual spec");
        assert_eq!(virtual_spec.row_id_slot, Some(SlotId::new(4)));

        let cfg = captured
            .lock()
            .expect("captured hdfs config lock")
            .clone()
            .expect("captured hdfs config");
        let Some(FileFormatConfig::Parquet(parquet_cfg)) = cfg.format else {
            panic!("expected parquet scan config");
        };
        assert_eq!(parquet_cfg.columns, ["___count___".to_string()]);
        assert_eq!(parquet_cfg.chunk_schema.slot_ids(), &[SlotId::new(5)]);
    }

    #[test]
    fn rejects_missing_scan_ranges() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()));
        let mut arena = ExprArena::default();
        let err = decode_node(&node, &mut arena, &ctx).unwrap_err();
        assert!(err.contains("missing scan ranges"), "err={err}");
    }

    #[test]
    fn predicate_only_required_column_uses_read_layout_and_projects_outputs() {
        let node = scan_node_with(
            vec![output_column(1, "id", DataType::Int64)],
            vec![column_ref(2, "flag", DataType::Boolean)],
            vec!["id".to_string(), "flag".to_string()],
            plan::scan_source::Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            }),
        );
        let ctx = NativePlanDecodeContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let lowered = decode_node(&node, &mut arena, &ctx).expect("lower native scan");
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(1)]);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected scan wrapper project");
        };
        assert!(project.is_subordinate);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        let ExecNodeKind::Scan(scan) = project.input.kind else {
            panic!("expected project input scan");
        };
        assert_eq!(
            scan.output_chunk_schema().slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
    }
}
