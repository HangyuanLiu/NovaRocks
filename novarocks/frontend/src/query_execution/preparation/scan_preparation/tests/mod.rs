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

mod dispatch;
mod iceberg;
mod projection;

use std::num::NonZeroU64;
use std::sync::Arc;

use crate::connector::FixtureConnectorRegistry;
use crate::connector::scan_model::{FixtureDeleteFile, FixtureScanFile};
use crate::query_execution::preparation::scan::{
    ResolvedReadReason, ResolvedScanExecution, ScanBindingResolver,
};
use novarocks_sql::plan_read::{
    DistributedNode, DistributedNodeKind, DistributedPlan, PlanScanNode,
};
use novarocks_sql::planning::catalog::{ConnectorReadTableFacts, materialize_connector_read_table};
use novarocks_sql::planning::query_execution::{
    SqlScanPreparationCategory, SqlScanPreparationFacts, scan_preparation_facts,
};
use novarocks_sql::test_support::{NativeScanFixture, native_scan_plan};

fn prepare_scan_bindings(
    plan: &DistributedPlan,
    connectors: &FixtureConnectorRegistry,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    prepare_scan_bindings_with_controls(plan, &controls, resolver)
}

/// Prepare a plan whose scans must offer the connector these runtime filters.
fn prepare_scan_bindings_with_runtime_filters(
    plan: &DistributedPlan,
    connectors: &FixtureConnectorRegistry,
    runtime_filter_scans: &[novarocks_sql::planning::query_execution::SqlRuntimeFilterSourceScanRequest],
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    let query_bindings = fixture_query_table_bindings(plan, &controls);
    let typed = fixture_typed_control_registry(plan, &controls);
    super::prepare_scan_bindings(
        plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&query_bindings),
        None,
        &fixture_scan_preparation_options(typed),
        runtime_filter_scans,
    )
}

/// Prepare a plan whose change-window lane resolves through the production
/// query-local resolver, exactly as a refresh does.
fn prepare_scan_bindings_with_delta_resolver(
    plan: &DistributedPlan,
    connectors: &FixtureConnectorRegistry,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    let query_bindings = fixture_query_table_bindings(plan, &controls);
    let typed = fixture_typed_control_registry(plan, &controls);
    let resolver = crate::query_execution::planning::delta_scan::QueryTableBindingScanResolver::new(
        &query_bindings,
    );
    super::prepare_scan_bindings(
        plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&query_bindings),
        Some(&resolver),
        &fixture_scan_preparation_options(typed),
        &[],
    )
}

/// Prepare a tokenized SQL scan against a caller-owned control resolver, for
/// tests that must hold the same resolver across admission and preparation.
fn prepare_scan_bindings_with_controls(
    plan: &DistributedPlan,
    controls: &crate::connector::FixtureControlResolver,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let query_bindings = fixture_query_table_bindings(plan, controls);
    let typed = fixture_typed_control_registry(plan, controls);
    super::prepare_scan_bindings(
        plan,
        controls,
        &crate::connector::test_request_context(),
        Some(&query_bindings),
        resolver,
        &fixture_scan_preparation_options(typed),
        &[],
    )
}

/// Fixture options carrying a typed control registry, mirroring how the
/// composition root hands one to production preparation.
fn fixture_scan_preparation_options(
    typed: Arc<crate::connector::typed_control_registry::TypedConnectorControlRegistry>,
) -> super::ScanPreparationOptions {
    super::ScanPreparationOptions::single_backend_fixture().with_typed_connector_control(
        typed,
        novarocks_spi::connector::read_stack::ConnectorSession::try_new(
            "fixture-query",
            "fixture-user",
            "UTC",
            "en_US",
            std::time::SystemTime::UNIX_EPOCH,
        )
        .expect("fixture connector session"),
    )
}

/// Install one typed control per catalog instance the sealed plan scans.
///
/// The key is the exact generation the fixture lease already froze, so the
/// registry answers precisely what production would answer.
fn fixture_typed_control_registry(
    plan: &DistributedPlan,
    controls: &crate::connector::FixtureControlResolver,
) -> Arc<crate::connector::typed_control_registry::TypedConnectorControlRegistry> {
    use novarocks_spi::connector::{ConnectorControlResolver, ConnectorInstanceId};

    let registry =
        Arc::new(crate::connector::typed_control_registry::TypedConnectorControlRegistry::new());
    let mut facts = Vec::new();
    for fragment in plan.fragments() {
        fn collect(node: &DistributedNode, facts: &mut Vec<SqlScanPreparationFacts>) {
            if let DistributedNodeKind::Scan(scan) = &node.payload {
                facts.push(scan_preparation_facts(scan));
            }
            for child in &node.children {
                collect(child, facts);
            }
        }
        collect(&fragment.root, &mut facts);
    }
    let mut installed = std::collections::BTreeSet::new();
    for facts in facts {
        let catalog = facts.identity().catalog().to_string();
        if !installed.insert(catalog.clone()) {
            continue;
        }
        let Ok(instance_id) = ConnectorInstanceId::parse(&catalog) else {
            continue;
        };
        let Ok(lease) = controls.acquire_current(&instance_id) else {
            continue;
        };
        let key = novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: lease.binding().descriptor().instance_id.clone(),
            incarnation: lease.binding().incarnation(),
        };
        let control = Arc::new(FixtureTypedControl {
            catalog: catalog.clone(),
            incarnation: key.incarnation.to_bytes(),
        });
        registry
            .install(
                key,
                crate::connector::typed_control_registry::TypedConnectorControl::new(
                    control.clone(),
                    control,
                ),
            )
            .expect("fixture typed control install");
    }
    registry
}

/// A typed control that freezes any named relation as an Iceberg DATA table
/// and binds every fixture column by name. It declines all pushdown and
/// enumerates no split, so a test observes exactly what preparation itself
/// decided.
struct FixtureTypedControl {
    catalog: String,
    incarnation: [u8; 16],
}

impl FixtureTypedControl {
    /// Every column the fixture tables expose, in a stable field-id order.
    /// Preparation looks these up by output column name, so the set only has
    /// to cover the names a fixture scan can project.
    ///
    /// `__change_op` is the change-window relation's own signed-operation
    /// column: a delta scan outputs it, so a connector that exposes change
    /// windows must bind it like any other column.
    const COLUMN_NAMES: [&'static str; 14] = [
        "id",
        "category",
        "v",
        "agg",
        "extra",
        "k",
        "__branch_id__",
        "__nova_join_row_key",
        "__nova_base_row_id",
        "_file",
        "_pos",
        "_row_id",
        "_last_updated_sequence_number",
        "__change_op",
    ];

    fn bindings() -> Vec<novarocks_proto::connector_read::TypedColumnBinding> {
        use super::super::typed_predicate::test_support::column_handle;

        Self::COLUMN_NAMES
            .iter()
            .enumerate()
            .map(|(ordinal, name)| {
                novarocks_proto::connector_read::TypedColumnBinding::new(
                    *name,
                    column_handle(ordinal as i32 + 1, name),
                    false,
                )
            })
            .collect()
    }

    /// The same columns in the wire shape a change-window handle carries.
    fn iceberg_columns() -> Vec<novarocks_proto_models::connector_read::IcebergColumnHandle> {
        use novarocks_proto_models::connector_read as dto;

        Self::COLUMN_NAMES
            .iter()
            .enumerate()
            .map(|(ordinal, name)| dto::IcebergColumnHandle {
                base_column_identity: Some(dto::ColumnIdentity {
                    field_id: ordinal as i32 + 1,
                    name: (*name).to_owned(),
                    category: dto::ColumnIdentityCategory::Primitive as i32,
                    children: Vec::new(),
                }),
                base_type_json: "\"long\"".to_owned(),
                field_id_path: Vec::new(),
                type_json: "\"long\"".to_owned(),
                nullable: true,
                comment: None,
            })
            .collect()
    }
}

impl novarocks_proto::connector_read::TypedConnectorMetadata for FixtureTypedControl {
    fn get_table_handle(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        name: &novarocks_spi::connector::read_stack::SchemaTableName,
        version: novarocks_proto::connector_read::TypedRelationVersion,
        _reference: Option<&str>,
    ) -> Result<
        Option<novarocks_proto::connector_read::CatalogTableHandle>,
        novarocks_spi::connector::ConnectorError,
    > {
        use novarocks_proto_models::connector_read as dto;

        let snapshot_id = match version {
            novarocks_proto::connector_read::TypedRelationVersion::Current => 1,
            novarocks_proto::connector_read::TypedRelationVersion::SnapshotId(snapshot_id) => {
                snapshot_id
            }
            novarocks_proto::connector_read::TypedRelationVersion::Reference => 1,
        };
        let raw = dto::CatalogTableHandle {
            catalog_name: self.catalog.clone(),
            instance_incarnation: self.incarnation.to_vec(),
            transaction: Some(dto::ConnectorTransactionHandle {
                handle: Some(dto::connector_transaction_handle::Handle::Iceberg(
                    dto::HiveTransactionHandle {
                        auto_commit: true,
                        uuid: vec![2_u8; 16],
                    },
                )),
            }),
            relation: Some(dto::catalog_table_handle::Relation::Table(
                dto::ConnectorTableHandle {
                    handle: Some(dto::connector_table_handle::Handle::Iceberg(
                        dto::IcebergTableHandle {
                            schema_table_name: Some(dto::SchemaTableName {
                                schema_name: name.schema_name().to_owned(),
                                table_name: name.table_name().to_owned(),
                            }),
                            snapshot_id: Some(snapshot_id),
                            table_schema_json: "{}".to_owned(),
                            spec_id: None,
                            partition_spec_jsons: std::collections::BTreeMap::new(),
                            format_version: 2,
                            unenforced_predicate: Some(dto::TupleDomain {
                                none: false,
                                column_domains: Vec::new(),
                            }),
                            enforced_predicate: Some(dto::TupleDomain {
                                none: false,
                                column_domains: Vec::new(),
                            }),
                            limit: None,
                            projected_columns: Vec::new(),
                            name_mapping_json: None,
                            table_location: "s3://bucket/table".to_owned(),
                            storage_properties: std::collections::BTreeMap::new(),
                        },
                    )),
                },
            )),
        };
        Ok(Some(
            novarocks_proto::connector_read::CatalogTableHandle::parse(
                raw,
                novarocks_proto::FieldPath::root("catalog_table_handle"),
            )
            .expect("fixture catalog table handle"),
        ))
    }

    fn get_column_bindings(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _table: &novarocks_proto::connector_read::CatalogTableHandle,
    ) -> Result<
        Vec<novarocks_proto::connector_read::TypedColumnBinding>,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(Self::bindings())
    }

    fn apply_filter(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _table: &novarocks_proto::connector_read::CatalogTableHandle,
        _constraint: &novarocks_proto::connector_read::WireConstraint,
    ) -> Result<
        Option<novarocks_proto::connector_read::TypedFilterApplication>,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(None)
    }

    fn apply_projection(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _table: &novarocks_proto::connector_read::CatalogTableHandle,
        _assignments: &[novarocks_proto::connector_read::ScanAssignment],
    ) -> Result<
        Option<novarocks_proto::connector_read::CatalogTableHandle>,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(None)
    }

    fn apply_limit(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _table: &novarocks_proto::connector_read::CatalogTableHandle,
        _limit: u64,
    ) -> Result<
        Option<novarocks_proto::connector_read::TypedLimitApplication>,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(None)
    }

    fn get_system_table_plan(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _name: &novarocks_spi::connector::read_stack::SchemaTableName,
    ) -> Result<
        Option<novarocks_proto::connector_read::TypedSystemTablePlan>,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(None)
    }

    /// Freeze one change window pinned to exactly the endpoints preparation
    /// asked for, so a test can read them back off the carrier.
    fn get_change_window_plan(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        name: &novarocks_spi::connector::read_stack::SchemaTableName,
        window: novarocks_proto::connector_read::TypedChangeWindow,
    ) -> Result<
        Option<novarocks_proto::connector_read::CatalogTableHandle>,
        novarocks_spi::connector::ConnectorError,
    > {
        use novarocks_proto_models::connector_read as dto;

        let raw = dto::CatalogTableHandle {
            catalog_name: self.catalog.clone(),
            instance_incarnation: self.incarnation.to_vec(),
            transaction: Some(dto::ConnectorTransactionHandle {
                handle: Some(dto::connector_transaction_handle::Handle::Iceberg(
                    dto::HiveTransactionHandle {
                        auto_commit: true,
                        uuid: vec![2_u8; 16],
                    },
                )),
            }),
            relation: Some(dto::catalog_table_handle::Relation::ChangeWindow(
                dto::ConnectorChangeWindowHandle {
                    handle: Some(dto::connector_change_window_handle::Handle::Iceberg(
                        dto::IcebergChangeWindowHandle {
                            schema_table_name: Some(dto::SchemaTableName {
                                schema_name: name.schema_name().to_owned(),
                                table_name: name.table_name().to_owned(),
                            }),
                            table_schema_json: "{}".to_owned(),
                            columns: Self::iceberg_columns(),
                            name_mapping_json: None,
                            from_snapshot_id_exclusive: window.from_snapshot_id(),
                            to_snapshot_id_inclusive: window.to_snapshot_id(),
                            partition_spec_jsons: std::collections::BTreeMap::new(),
                        },
                    )),
                },
            )),
        };
        Ok(Some(
            novarocks_proto::connector_read::CatalogTableHandle::parse(
                raw,
                novarocks_proto::FieldPath::root("catalog_table_handle"),
            )
            .expect("fixture catalog change window handle"),
        ))
    }
}

impl novarocks_proto::connector_read::TypedConnectorSplitManager for FixtureTypedControl {
    fn get_splits(
        &self,
        _session: &novarocks_spi::connector::read_stack::ConnectorSession,
        _table: &novarocks_proto::connector_read::CatalogTableHandle,
        _columns: &[novarocks_proto::connector_read::ScanAssignment],
        _dynamic_filter_columns: &std::collections::BTreeSet<
            novarocks_proto::connector_read::ValidatedColumnHandle,
        >,
        _constraint: &novarocks_proto::connector_read::WireConstraint,
    ) -> Result<
        Box<dyn novarocks_proto::connector_read::TypedConnectorSplitSource>,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "the fixture typed control enumerates no split",
        ))
    }
}

/// The shared fixture allocates the same token that the sealed SQL scan embeds.
/// Concrete SQL source construction remains in SQL test support; Core sees
/// only copied scan facts and supplies the provider admission beside that token.
fn fixture_query_table_bindings(
    plan: &DistributedPlan,
    controls: &crate::connector::FixtureControlResolver,
) -> crate::catalog_application::query_bindings::QueryTableBindingStore {
    use crate::catalog_application::query_bindings::{
        QueryScanMaterialization, QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore,
    };
    use novarocks_spi::connector::{
        ConnectorControlResolver, ConnectorInstanceId, ConnectorReadSelector,
        ConnectorTableIdentity, ConnectorTablePlanningFacts, ConnectorTableRequest,
        ConnectorTableResolution,
    };

    fn collect(node: &DistributedNode, facts: &mut Vec<SqlScanPreparationFacts>) {
        if let DistributedNodeKind::Scan(scan) = &node.payload {
            facts.push(scan_preparation_facts(scan));
        }
        for child in &node.children {
            collect(child, facts);
        }
    }

    let mut fixture_facts = Vec::new();
    for fragment in plan.fragments() {
        collect(&fragment.root, &mut fixture_facts);
    }
    // One physical table binding can occur both as a current/locator read and
    // as one or more frozen reads. Preserve every frozen selector before the
    // per-binding fixture admission below coalesces repeated scan facts.
    let frozen_snapshot_ids = fixture_facts
        .iter()
        .filter_map(|facts| {
            facts
                .frozen_snapshot_id()
                .map(|snapshot_id| (facts.binding(), snapshot_id))
        })
        .collect::<Vec<_>>();
    fixture_facts.sort_by_key(|facts| facts.binding().ordinal().get());
    fixture_facts.dedup_by_key(|facts| facts.binding());
    let store = QueryTableBindingStore::try_new_with_scope_for_test(
        NonZeroU64::new(1).expect("fixture scope"),
    );
    for facts in fixture_facts {
        let binding_frozen_snapshot_ids = frozen_snapshot_ids
            .iter()
            .filter_map(|(binding, snapshot_id)| {
                (*binding == facts.binding()).then_some(*snapshot_id)
            })
            .collect::<Vec<_>>();
        if facts.category() == SqlScanPreparationCategory::ConnectorRead {
            // This source kind is supplied by its dedicated resolver tests;
            // no catalog admission is expected before resolver dispatch.
            continue;
        }
        let planning_lease = controls
            .acquire_current(
                &ConnectorInstanceId::parse(facts.identity().catalog())
                    .expect("fixture catalog must be a valid connector instance"),
            )
            .ok();
        if planning_lease.is_none() && facts.category() == SqlScanPreparationCategory::Delta {
            // Resolver-only negative tests deliberately omit connector admission so
            // they can assert the resolver error before generic read planning.
            continue;
        }
        store
        .resolve_or_insert_with_id(
            QueryTableBindingKey::strict_base(
                facts.identity().catalog(),
                facts.identity().namespace(),
                facts.identity().table(),
            ),
            |binding| {
                if binding != facts.binding() {
                    return Err("sealed scan fixture binding token must match Core fixture store".to_string());
                }
                let lease = planning_lease.clone().ok_or_else(|| {
                    "scan fixture must acquire an exact connector lease".to_string()
                })?;
                let metadata = lease
                    .binding()
                    .metadata()
                    .load_table(ConnectorTableRequest {
                        table: ConnectorTableIdentity {
                            instance_id: ConnectorInstanceId::parse(facts.identity().catalog())
                                .expect("fixture catalog must be valid"),
                            namespace: Arc::from(facts.identity().namespace()),
                            table: Arc::from(facts.identity().table()),
                        },
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        context: crate::connector::test_request_context(),
                    })
                    .map_err(|error| error.to_string())?;
                let scan_materialization = QueryScanMaterialization {
                    table: metadata.table,
                    schema: metadata.schema,
                    selector: ConnectorReadSelector::Current,
                    statistics_pin: None,
                    planning_lease: lease.clone(),
                };
                let frozen_snapshot_materializations = binding_frozen_snapshot_ids
                    .into_iter()
                    .map(|snapshot_id| {
                        let lease = planning_lease.clone().ok_or_else(|| {
                            "frozen scan fixture must acquire an exact connector lease".to_string()
                        })?;
                        let metadata = lease
                            .binding()
                            .metadata()
                            .load_table(ConnectorTableRequest {
                                table: ConnectorTableIdentity {
                                    instance_id: ConnectorInstanceId::parse(facts.identity().catalog())
                                        .expect("fixture catalog must be valid"),
                                    namespace: Arc::from(facts.identity().namespace()),
                                    table: Arc::from(facts.identity().table()),
                                },
                                resolution: ConnectorTableResolution::StrictBaseTable,
                                context: crate::connector::test_request_context(),
                            })
                            .map_err(|error| error.to_string())?;
                        Ok((
                            snapshot_id,
                            QueryScanMaterialization {
                                table: metadata.table,
                                schema: metadata.schema,
                                selector: ConnectorReadSelector::SnapshotId(snapshot_id),
                                statistics_pin: None,
                                planning_lease: lease,
                            },
                        ))
                    })
                    .collect::<Result<std::collections::BTreeMap<_, _>, String>>()?;
                Ok(QueryTableBinding {
                    resolved: materialize_connector_read_table(ConnectorReadTableFacts {
                        catalog: facts.identity().catalog().to_string(),
                        namespace: facts.identity().namespace().to_string(),
                        table: facts.identity().table().to_string(),
                        columns: scan_materialization
                            .schema
                            .fields()
                            .iter()
                            .map(|field| novarocks_types::schema::ColumnDef {
                                name: field.name().to_string(),
                                data_type: field.data_type().clone(),
                                nullable: field.is_nullable(),
                                write_default: None,
                                logical_type: None,
                            })
                            .collect(),
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        schema: scan_materialization.schema.clone(),
                        binding,
                        selector: ConnectorReadSelector::Current,
                        planning_facts: ConnectorTablePlanningFacts::empty(),
                    })
                    .map_err(|error| format!("fixture SQL materialization: {error}"))?
                    .into_resolved_table(),
                    statistics_pin: None,
                    admission: planning_lease
                        .clone()
                        .map(crate::catalog_application::query_bindings::QueryTableBindingAdmission::Exact)
                        .unwrap_or(crate::catalog_application::query_bindings::QueryTableBindingAdmission::Local),
                    scan_materialization: Some(scan_materialization.clone()),
                    mv_target_read: match facts.mv_target() {
                        Some(target)
                            if matches!(
                                facts.category(),
                                SqlScanPreparationCategory::MvTargetState
                                    | SqlScanPreparationCategory::MvTargetLocator
                            ) => Some(
                            crate::catalog_application::query_bindings::MvTargetReadAdmission {
                                full: scan_materialization.clone(),
                                affected_partitions: scan_materialization.clone(),
                                target_table_uuid: target.target_table_uuid().to_string(),
                                frozen_snapshot_id: target.target_snapshot_id(),
                            },
                        ),
                        _ => None,
                    },
                    write_target_admission: None,
                    frozen_snapshot_materializations,
                    admitted_change_scans: std::collections::BTreeMap::new(),
                })
            },
        )
        .expect("fixture query binding");
    }
    store
}

struct StaticResolver {
    execution: ResolvedScanExecution,
}

impl ScanBindingResolver for StaticResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        Ok(Some(self.execution.clone()))
    }
}

fn data_file(path: &str) -> FixtureScanFile {
    let mut file = FixtureScanFile::new(path);
    file.partition_spec_id = Some(0);
    file.sequence_number = Some(1);
    file
}

fn equality_delete_file(
    equality_column_names: Vec<&str>,
    equality_field_ids: Vec<i32>,
) -> FixtureDeleteFile {
    FixtureDeleteFile::equality(
        "s3://bucket/eq-delete.parquet",
        &equality_column_names,
        &equality_field_ids,
    )
}

fn registry(files: Vec<FixtureScanFile>) -> FixtureConnectorRegistry {
    let registry = FixtureConnectorRegistry::new();
    crate::connector::scan_model::register_planned_files_fixture(
        &registry,
        "test_catalog",
        files,
        None,
    );
    registry
}
