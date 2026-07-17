use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use super::{
    RustScopedAliases, RustScopedUsePath, cfg_attr_generated_path_values,
    cfg_attribute_requires_test, decode_rust_string_literal, manifest_dir, path_attribute_value,
    production_rs_files_from_entries, rel, rs_files, rust_all_source_canonical_paths,
    rust_canonical_path_segments_in_scope, rust_lexically_sanitized, rust_module_items,
    rust_production_canonical_paths, rust_production_scoped_aliases,
    rust_raw_production_use_statements, rust_raw_use_statements, rust_resolve_scoped_paths,
    rust_sanitized_production_text, rust_scoped_aliases, rust_source_module_segments,
    rust_source_tokens, src_dir,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EngineFileOwner {
    path: &'static str,
    target_owner: &'static str,
    migration_task: &'static str,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct EngineBoundarySnapshot {
    engine_files: BTreeSet<String>,
    engine_module_declarations: BTreeSet<String>,
    external_engine_dependencies: BTreeMap<String, BTreeSet<String>>,
    standalone_state_dependencies: BTreeMap<String, BTreeSet<String>>,
    forwarding_reexports: BTreeSet<String>,
    lower_layer_frontend_dependencies: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct EngineBoundaryBaseline {
    file_owners: &'static [EngineFileOwner],
    engine_module_declarations: &'static [&'static str],
    external_engine_dependencies: &'static [(&'static str, &'static [&'static str])],
    standalone_state_dependencies: &'static [(&'static str, &'static [&'static str])],
    forwarding_reexports: &'static [&'static str],
}

const EMPTY_BASELINE: EngineBoundaryBaseline = EngineBoundaryBaseline {
    file_owners: &[],
    engine_module_declarations: &[],
    external_engine_dependencies: &[],
    standalone_state_dependencies: &[],
    forwarding_reexports: &[],
};

const ENGINE_FILE_OWNERS: &[EngineFileOwner] = &[
    EngineFileOwner {
        path: "src/engine/aggregate.rs",
        target_owner: "dml",
        migration_task: "EBD-10A",
    },
    EngineFileOwner {
        path: "src/engine/backend_ops.rs",
        target_owner: "coordinator",
        migration_task: "EBD-5B",
    },
    EngineFileOwner {
        path: "src/engine/backend_resolver.rs",
        target_owner: "coordinator",
        migration_task: "EBD-5B",
    },
    EngineFileOwner {
        path: "src/engine/delete_flow.rs",
        target_owner: "dml",
        migration_task: "EBD-11",
    },
    EngineFileOwner {
        path: "src/engine/delete_predicate_translate.rs",
        target_owner: "connector",
        migration_task: "EBD-11",
    },
    EngineFileOwner {
        path: "src/engine/dml_change_stream.rs",
        target_owner: "dml",
        migration_task: "EBD-12B",
    },
    EngineFileOwner {
        path: "src/engine/equality_delete_flow.rs",
        target_owner: "dml",
        migration_task: "EBD-11",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_change_stream_write.rs",
        target_owner: "split:connector,dml",
        migration_task: "EBD-9/EBD-12B",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_ctas.rs",
        target_owner: "dml",
        migration_task: "EBD-10B",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_expire_snapshots.rs",
        target_owner: "table_maintenance",
        migration_task: "EBD-13",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_maintenance.rs",
        target_owner: "split:connector,table_maintenance",
        migration_task: "EBD-13",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_ref_flow.rs",
        target_owner: "split:catalog,connector",
        migration_task: "EBD-5A",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_remove_orphan_files.rs",
        target_owner: "split:connector,table_maintenance",
        migration_task: "EBD-13",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_rewrite_manifests.rs",
        target_owner: "split:connector,table_maintenance",
        migration_task: "EBD-13",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_truncate.rs",
        target_owner: "dml",
        migration_task: "EBD-10B",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_view.rs",
        target_owner: "catalog",
        migration_task: "EBD-6B",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_view_rewrite.rs",
        target_owner: "split:catalog,sql",
        migration_task: "EBD-6B",
    },
    EngineFileOwner {
        path: "src/engine/iceberg_writer.rs",
        target_owner: "split:connector,dml",
        migration_task: "EBD-9/EBD-10A",
    },
    EngineFileOwner {
        path: "src/engine/information_schema.rs",
        target_owner: "catalog",
        migration_task: "EBD-6A",
    },
    EngineFileOwner {
        path: "src/engine/insert.rs",
        target_owner: "dml",
        migration_task: "EBD-10A",
    },
    EngineFileOwner {
        path: "src/engine/insert_flow.rs",
        target_owner: "dml",
        migration_task: "EBD-10A",
    },
    EngineFileOwner {
        path: "src/engine/mod.rs",
        target_owner: "split:frontend,runtime",
        migration_task: "EBD-21/EBD-22",
    },
    EngineFileOwner {
        path: "src/engine/mutation_flow.rs",
        target_owner: "dml",
        migration_task: "EBD-12A",
    },
    EngineFileOwner {
        path: "src/engine/mv/agg_state/aggregate_sql_calls.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv/agg_state/mod.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv/agg_state/mv_agg_state.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv/agg_state/mv_shape.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv/agg_state/physical_column.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv/agg_state/sql_type.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv/agg_state/state_codec.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv/analysis.rs",
        target_owner: "mv",
        migration_task: "EBD-15",
    },
    EngineFileOwner {
        path: "src/engine/mv/apply_key.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv/dependency.rs",
        target_owner: "mv",
        migration_task: "EBD-15",
    },
    EngineFileOwner {
        path: "src/engine/mv/iceberg_aggregate_state.rs",
        target_owner: "mv",
        migration_task: "EBD-17",
    },
    EngineFileOwner {
        path: "src/engine/mv/iceberg_backend.rs",
        target_owner: "mv",
        migration_task: "EBD-16/EBD-17/EBD-18",
    },
    EngineFileOwner {
        path: "src/engine/mv/iceberg_discovery.rs",
        target_owner: "mv",
        migration_task: "EBD-16",
    },
    EngineFileOwner {
        path: "src/engine/mv/iceberg_guard.rs",
        target_owner: "mv",
        migration_task: "EBD-16",
    },
    EngineFileOwner {
        path: "src/engine/mv/iceberg_join_branch.rs",
        target_owner: "mv",
        migration_task: "EBD-17",
    },
    EngineFileOwner {
        path: "src/engine/mv/iceberg_join_coalesce.rs",
        target_owner: "mv",
        migration_task: "EBD-17",
    },
    EngineFileOwner {
        path: "src/engine/mv/iceberg_refresh.rs",
        target_owner: "mv",
        migration_task: "EBD-16/EBD-17/EBD-18/EBD-19",
    },
    EngineFileOwner {
        path: "src/engine/mv/iceberg_target_apply.rs",
        target_owner: "mv",
        migration_task: "EBD-17",
    },
    EngineFileOwner {
        path: "src/engine/mv/lake_rebuild.rs",
        target_owner: "mv",
        migration_task: "EBD-19",
    },
    EngineFileOwner {
        path: "src/engine/mv/lifecycle.rs",
        target_owner: "mv",
        migration_task: "EBD-14/EBD-18",
    },
    EngineFileOwner {
        path: "src/engine/mv/mod.rs",
        target_owner: "mv",
        migration_task: "EBD-19",
    },
    EngineFileOwner {
        path: "src/engine/mv/partition/derivation.rs",
        target_owner: "mv",
        migration_task: "EBD-15",
    },
    EngineFileOwner {
        path: "src/engine/mv/partition/key.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv/partition/mapping.rs",
        target_owner: "mv",
        migration_task: "EBD-15",
    },
    EngineFileOwner {
        path: "src/engine/mv/partition/mod.rs",
        target_owner: "mv",
        migration_task: "EBD-15",
    },
    EngineFileOwner {
        path: "src/engine/mv/partition/planner.rs",
        target_owner: "mv",
        migration_task: "EBD-16",
    },
    EngineFileOwner {
        path: "src/engine/mv/rebind.rs",
        target_owner: "mv",
        migration_task: "EBD-15",
    },
    EngineFileOwner {
        path: "src/engine/mv/recovery.rs",
        target_owner: "mv",
        migration_task: "EBD-19",
    },
    EngineFileOwner {
        path: "src/engine/mv/refresh_context.rs",
        target_owner: "mv",
        migration_task: "EBD-14/EBD-15/EBD-16",
    },
    EngineFileOwner {
        path: "src/engine/mv/refresh_contract.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv/refresh_driver.rs",
        target_owner: "mv",
        migration_task: "EBD-17",
    },
    EngineFileOwner {
        path: "src/engine/mv/refresh_io.rs",
        target_owner: "mv",
        migration_task: "EBD-17",
    },
    EngineFileOwner {
        path: "src/engine/mv/refresh_pin.rs",
        target_owner: "mv",
        migration_task: "EBD-18",
    },
    EngineFileOwner {
        path: "src/engine/mv/refresh_property.rs",
        target_owner: "mv",
        migration_task: "EBD-14/EBD-16",
    },
    EngineFileOwner {
        path: "src/engine/mv/scan_binding.rs",
        target_owner: "mv",
        migration_task: "EBD-15",
    },
    EngineFileOwner {
        path: "src/engine/mv/schema_contract.rs",
        target_owner: "mv",
        migration_task: "EBD-14/EBD-16",
    },
    EngineFileOwner {
        path: "src/engine/mv/stateless_rebuild.rs",
        target_owner: "mv",
        migration_task: "EBD-17",
    },
    EngineFileOwner {
        path: "src/engine/mv/table_ref.rs",
        target_owner: "mv",
        migration_task: "EBD-14",
    },
    EngineFileOwner {
        path: "src/engine/mv_flow.rs",
        target_owner: "mv",
        migration_task: "EBD-16/EBD-17/EBD-18",
    },
    EngineFileOwner {
        path: "src/engine/mv_maintenance/integration_tests.rs",
        target_owner: "mv",
        migration_task: "EBD-19",
    },
    EngineFileOwner {
        path: "src/engine/mv_maintenance/mod.rs",
        target_owner: "mv",
        migration_task: "EBD-19",
    },
    EngineFileOwner {
        path: "src/engine/mv_maintenance/policy.rs",
        target_owner: "mv",
        migration_task: "EBD-19",
    },
    EngineFileOwner {
        path: "src/engine/mv_maintenance/stats.rs",
        target_owner: "mv",
        migration_task: "EBD-19",
    },
    EngineFileOwner {
        path: "src/engine/mv_rewrite_prep.rs",
        target_owner: "mv",
        migration_task: "EBD-15",
    },
    EngineFileOwner {
        path: "src/engine/mv_scheduler.rs",
        target_owner: "mv",
        migration_task: "EBD-19",
    },
    EngineFileOwner {
        path: "src/engine/query_prep.rs",
        target_owner: "split:catalog,connector,frontend,mv,sql",
        migration_task: "EBD-5A/EBD-15/EBD-20",
    },
    EngineFileOwner {
        path: "src/engine/query_stats.rs",
        target_owner: "statistics",
        migration_task: "EBD-7",
    },
    EngineFileOwner {
        path: "src/engine/statement.rs",
        target_owner: "split:catalog,dml,frontend,mv,sql,table_maintenance",
        migration_task: "EBD-5A/EBD-6A/EBD-6B/EBD-10A/EBD-10B/EBD-11/EBD-12A/EBD-13/EBD-16/EBD-21",
    },
    EngineFileOwner {
        path: "src/engine/statistics.rs",
        target_owner: "statistics",
        migration_task: "EBD-7",
    },
    EngineFileOwner {
        path: "src/engine/view_rewrite.rs",
        target_owner: "split:catalog,sql",
        migration_task: "EBD-6B",
    },
    EngineFileOwner {
        path: "src/engine/virtual_table.rs",
        target_owner: "catalog",
        migration_task: "EBD-6A",
    },
    EngineFileOwner {
        path: "src/engine/write_operation_lifecycle.rs",
        target_owner: "dml",
        migration_task: "EBD-9",
    },
    EngineFileOwner {
        path: "src/engine/write_transaction.rs",
        target_owner: "dml",
        migration_task: "EBD-9",
    },
];

const ENGINE_MODULE_DECLARATIONS: &[&str] = &[
    "src/engine/mod.rs||external|path=default|aggregate",
    "src/engine/mod.rs||external|path=default|backend_ops",
    "src/engine/mod.rs||external|path=default|backend_resolver",
    "src/engine/mod.rs||external|path=default|delete_flow",
    "src/engine/mod.rs||external|path=default|delete_predicate_translate",
    "src/engine/mod.rs||external|path=default|dml_change_stream",
    "src/engine/mod.rs||external|path=default|equality_delete_flow",
    "src/engine/mod.rs||external|path=default|iceberg_change_stream_write",
    "src/engine/mod.rs||external|path=default|iceberg_ctas",
    "src/engine/mod.rs||external|path=default|iceberg_expire_snapshots",
    "src/engine/mod.rs||external|path=default|iceberg_maintenance",
    "src/engine/mod.rs||external|path=default|iceberg_ref_flow",
    "src/engine/mod.rs||external|path=default|iceberg_remove_orphan_files",
    "src/engine/mod.rs||external|path=default|iceberg_rewrite_manifests",
    "src/engine/mod.rs||external|path=default|iceberg_truncate",
    "src/engine/mod.rs||external|path=default|iceberg_view",
    "src/engine/mod.rs||external|path=default|iceberg_view_rewrite",
    "src/engine/mod.rs||external|path=default|iceberg_writer",
    "src/engine/mod.rs||external|path=default|information_schema",
    "src/engine/mod.rs||external|path=default|insert",
    "src/engine/mod.rs||external|path=default|insert_flow",
    "src/engine/mod.rs||external|path=default|mutation_flow",
    "src/engine/mod.rs||external|path=default|mv",
    "src/engine/mod.rs||external|path=default|mv_flow",
    "src/engine/mod.rs||external|path=default|mv_maintenance",
    "src/engine/mod.rs||external|path=default|mv_rewrite_prep",
    "src/engine/mod.rs||external|path=default|mv_scheduler",
    "src/engine/mod.rs||external|path=default|query_prep",
    "src/engine/mod.rs||external|path=default|query_stats",
    "src/engine/mod.rs||external|path=default|statement",
    "src/engine/mod.rs||external|path=default|statistics",
    "src/engine/mod.rs||external|path=default|view_rewrite",
    "src/engine/mod.rs||external|path=default|virtual_table",
    "src/engine/mod.rs||external|path=default|write_operation_lifecycle",
    "src/engine/mod.rs||external|path=default|write_transaction",
    "src/engine/mv/agg_state/mod.rs||external|path=default|aggregate_sql_calls",
    "src/engine/mv/agg_state/mod.rs||external|path=default|mv_agg_state",
    "src/engine/mv/agg_state/mod.rs||external|path=default|mv_shape",
    "src/engine/mv/agg_state/mod.rs||external|path=default|physical_column",
    "src/engine/mv/agg_state/mod.rs||external|path=default|sql_type",
    "src/engine/mv/agg_state/mod.rs||external|path=default|state_codec",
    "src/engine/mv/mod.rs||external|path=default|agg_state",
    "src/engine/mv/mod.rs||external|path=default|analysis",
    "src/engine/mv/mod.rs||external|path=default|apply_key",
    "src/engine/mv/mod.rs||external|path=default|dependency",
    "src/engine/mv/mod.rs||external|path=default|iceberg_aggregate_state",
    "src/engine/mv/mod.rs||external|path=default|iceberg_backend",
    "src/engine/mv/mod.rs||external|path=default|iceberg_discovery",
    "src/engine/mv/mod.rs||external|path=default|iceberg_guard",
    "src/engine/mv/mod.rs||external|path=default|iceberg_join_branch",
    "src/engine/mv/mod.rs||external|path=default|iceberg_join_coalesce",
    "src/engine/mv/mod.rs||external|path=default|iceberg_refresh",
    "src/engine/mv/mod.rs||external|path=default|iceberg_target_apply",
    "src/engine/mv/mod.rs||external|path=default|lake_rebuild",
    "src/engine/mv/mod.rs||external|path=default|lifecycle",
    "src/engine/mv/mod.rs||external|path=default|partition",
    "src/engine/mv/mod.rs||external|path=default|rebind",
    "src/engine/mv/mod.rs||external|path=default|recovery",
    "src/engine/mv/mod.rs||external|path=default|refresh_context",
    "src/engine/mv/mod.rs||external|path=default|refresh_contract",
    "src/engine/mv/mod.rs||external|path=default|refresh_driver",
    "src/engine/mv/mod.rs||external|path=default|refresh_io",
    "src/engine/mv/mod.rs||external|path=default|refresh_pin",
    "src/engine/mv/mod.rs||external|path=default|refresh_property",
    "src/engine/mv/mod.rs||external|path=default|scan_binding",
    "src/engine/mv/mod.rs||external|path=default|schema_contract",
    "src/engine/mv/mod.rs||external|path=default|stateless_rebuild",
    "src/engine/mv/mod.rs||external|path=default|table_ref",
    "src/engine/mv/partition/mod.rs||external|path=default|derivation",
    "src/engine/mv/partition/mod.rs||external|path=default|key",
    "src/engine/mv/partition/mod.rs||external|path=default|mapping",
    "src/engine/mv/partition/mod.rs||external|path=default|planner",
    "src/engine/mv_maintenance/mod.rs||external|path=default|policy",
    "src/engine/mv_maintenance/mod.rs||external|path=default|stats",
];

const EXTERNAL_ENGINE_DEPENDENCIES: &[(&str, &[&str])] = &[
    (
        "src/connector/backend.rs",
        &[
            "crate::engine::mv::lifecycle::CreateMvRequest",
            "crate::engine::mv::lifecycle::DropMvRequest",
            "crate::engine::mv::lifecycle::ListMvsRequest",
            "crate::engine::mv::lifecycle::MvListRow",
            "crate::engine::mv::lifecycle::RefreshCtx",
            "crate::engine::mv::lifecycle::RefreshError",
            "crate::engine::mv::lifecycle::RefreshOutcome",
            "crate::engine::mv::lifecycle::RefreshPlan",
            "crate::engine::mv::lifecycle::RefreshRequest",
        ],
    ),
    (
        "src/connector/iceberg/analyze.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::backend_resolver::resolve_table_target",
            "crate::engine::execute_query_with_catalog_service",
            "crate::engine::iceberg_writer::invalidate_iceberg_caches",
        ],
    ),
    (
        "src/connector/iceberg/catalog/schema_update.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::backend_resolver::TargetBackend",
            "crate::engine::backend_resolver::resolve_existing_table_target",
            "crate::engine::iceberg_writer::invalidate_iceberg_caches",
            "crate::engine::statement::AddPosition",
            "crate::engine::statement::AlterIcebergPropertiesStmt",
            "crate::engine::statement::AlterIcebergSchemaStmt",
            "crate::engine::statement::ColumnPath",
            "crate::engine::statement::IcebergSchemaChange",
            "crate::engine::statement::PropertiesOp",
        ],
    ),
    (
        "src/connector/iceberg/changes.rs",
        &[
            "crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile",
            "crate::engine::delete_flow::data_file_row_is_visible",
        ],
    ),
    (
        "src/connector/iceberg/compact.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::backend_resolver::TargetBackend",
            "crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry",
            "crate::engine::iceberg_writer::data_file_to_written_file",
            "crate::engine::iceberg_writer::invalidate_iceberg_caches",
            "crate::engine::iceberg_writer::run_select_to_chunks",
            "crate::engine::mv::iceberg_refresh::write_chunks_as_iceberg_data_files",
        ],
    ),
    (
        "src/connector/mod.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::mv::iceberg_backend::IcebergMvBackend::new",
        ],
    ),
    (
        "src/connector/starrocks/fe_v2_meta.rs",
        &["crate::engine::recover_starrocks_tablet_paths_from_current_engine"],
    ),
    (
        "src/connector/starrocks/lake/delete_predicate_proto.rs",
        &[
            "crate::engine::delete_predicate_translate::BinaryTerm",
            "crate::engine::delete_predicate_translate::CmpOp",
            "crate::engine::delete_predicate_translate::DeletePredicateTerms",
            "crate::engine::delete_predicate_translate::InTerm",
            "crate::engine::delete_predicate_translate::IsNullTerm",
        ],
    ),
    (
        "src/connector/starrocks/table/backend.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::mv::lifecycle::BackendRefreshOutcome",
            "crate::engine::mv::lifecycle::BackendRefreshPlan",
            "crate::engine::mv::lifecycle::CreateMvRequest",
            "crate::engine::mv::lifecycle::DropMvRequest",
            "crate::engine::mv::lifecycle::ListMvsRequest",
            "crate::engine::mv::lifecycle::MvBaseRef",
            "crate::engine::mv::lifecycle::MvListRow",
            "crate::engine::mv::lifecycle::MvStorageEngine",
            "crate::engine::mv::lifecycle::RefreshCtx",
            "crate::engine::mv::lifecycle::RefreshError",
            "crate::engine::mv::lifecycle::RefreshMode",
            "crate::engine::mv::lifecycle::RefreshOutcome",
            "crate::engine::mv::lifecycle::RefreshPlan",
            "crate::engine::mv::lifecycle::RefreshRequest",
            "crate::engine::mv::lifecycle::StarRocksTableRefreshOutcome",
            "crate::engine::mv::lifecycle::StarRocksTableRefreshPlan",
            "crate::engine::mv::partition::AffectedTargetPartitions::not_derived",
        ],
    ),
    (
        "src/connector/starrocks/table/ddl.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::StatementResult",
            "crate::engine::mv::agg_state::physical_column::StarRocksPhysicalColumn",
        ],
    ),
    (
        "src/connector/starrocks/table/erase.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/ivm_change_stream.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::mv::table_ref::IcebergTableRef",
        ],
    ),
    (
        "src/connector/starrocks/table/ivm_delta_aggregate.rs",
        &[
            "crate::engine::mv::agg_state::aggregate_sql_calls::AggregateSqlCalls",
            "crate::engine::mv::agg_state::mv_agg_state::AGG_RETRACTION_COUNT_STATE_COLUMN",
            "crate::engine::mv::agg_state::mv_agg_state::aggregate_shape_needs_retraction_count_state",
            "crate::engine::mv::agg_state::mv_agg_state::sanitize_state_column_name",
            "crate::engine::mv::agg_state::mv_shape::AggregateCallShape",
            "crate::engine::mv::agg_state::mv_shape::AggregateFunctionKind",
            "crate::engine::mv::agg_state::mv_shape::AggregateInput",
            "crate::engine::mv::agg_state::mv_shape::VisibleAggregateOutput",
        ],
    ),
    (
        "src/connector/starrocks/table/ivm_delta_source.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::execute_query",
            "crate::engine::mv::agg_state::mv_shape::query_has_aggregate_surface",
            "crate::engine::mv::table_ref::IcebergTableRef",
            "crate::engine::mv_flow::validate_incremental_mv_base_ref",
            "crate::engine::mv_flow::write_mv_delete_temp_parquet",
            "crate::engine::query_prep::IcebergFileForQuery",
            "crate::engine::query_prep::build_iceberg_delta_table_def_with_files",
            "crate::engine::query_prep::delete_temp_iceberg_file_for_query",
        ],
    ),
    (
        "src/connector/starrocks/table/mv_apply_policy.rs",
        &["crate::engine::mv::agg_state::mv_shape::IncrementalMvShape"],
    ),
    (
        "src/connector/starrocks/table/mv_ddl.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::StatementResult",
            "crate::engine::mv::agg_state::aggregate_sql_calls::AggregateSqlCalls::from",
            "crate::engine::mv::agg_state::mv_agg_state::ROW_ID_COLUMN",
            "crate::engine::mv::agg_state::mv_agg_state::aggregate_input_types_from_resolved_query",
            "crate::engine::mv::agg_state::mv_agg_state::build_aggregate_mv_layout_with_input_types",
            "crate::engine::mv::agg_state::mv_shape::AggregateFunctionKind",
            "crate::engine::mv::agg_state::mv_shape::AggregateMvShape",
            "crate::engine::mv::agg_state::mv_shape::IncrementalMvShape",
            "crate::engine::mv::agg_state::mv_shape::VisibleAggregateOutput",
            "crate::engine::mv::agg_state::mv_shape::classify_incremental_mv_query",
            "crate::engine::mv::agg_state::physical_column::StarRocksPhysicalColumn",
            "crate::engine::mv::agg_state::physical_column::starrocks_physical_column",
            "crate::engine::mv::agg_state::sql_type::arrow_data_type_to_sql_type",
            "crate::engine::mv::analysis::ResolvedTableRef",
            "crate::engine::mv::dependency::ensure_no_downstream_dependencies",
            "crate::engine::mv::dependency::resolve_create_mv_dependencies",
            "crate::engine::mv::dependency::starrocks_mv_dependency_ref",
            "crate::engine::mv::dependency::validate_no_create_cycle",
            "crate::engine::mv::lifecycle::MvListRow",
            "crate::engine::mv::lifecycle::MvStorageEngine",
            "crate::engine::mv::table_ref::IcebergTableRef",
            "crate::engine::mv_flow::refresh_metadata_request_for_create",
            "crate::engine::query_prep::drop_local_table_registration_if_exists",
        ],
    ),
    (
        "src/connector/starrocks/table/mv_refresh.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::StatementResult",
            "crate::engine::mv::agg_state::aggregate_sql_calls::AggregateSqlCalls::from",
            "crate::engine::mv::agg_state::mv_agg_state::aggregate_input_types_from_resolved_query",
            "crate::engine::mv::agg_state::mv_agg_state::build_aggregate_mv_layout_with_input_types",
            "crate::engine::mv::agg_state::mv_agg_state::materialize_aggregate_result_chunks",
            "crate::engine::mv::agg_state::mv_shape::AggregateMvShape",
            "crate::engine::mv::agg_state::mv_shape::IncrementalMvShape",
            "crate::engine::mv::agg_state::mv_shape::classify_incremental_mv_query",
            "crate::engine::mv::agg_state::mv_shape::rewrite_select_sql_for_state",
            "crate::engine::mv::refresh_io::acquire_mv_refresh_lock",
            "crate::engine::mv::table_ref::IcebergTableRef",
            "crate::engine::mv_flow::analyze_visible_query",
            "crate::engine::mv_flow::execute_query_for_mv_refresh",
            "crate::engine::mv_flow::execute_query_for_mv_refresh_with_catalog",
        ],
    ),
    (
        "src/connector/starrocks/table/refresh_pin.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::mv::table_ref::IcebergTableRef",
        ],
    ),
    (
        "src/connector/starrocks/table/scan_planner.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/txn.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::StatementResult",
            "crate::engine::build_local_insert_batch",
            "crate::engine::execute_query",
            "crate::engine::mv::agg_state::mv_agg_state",
            "crate::engine::reorder_insert_rows",
        ],
    ),
    (
        "src/exec/expr/agg/functions/state_combinators/bool_or_and.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::decode_bool_state",
            "crate::engine::mv::agg_state::state_codec::encode_bool_state",
        ],
    ),
    (
        "src/exec/expr/agg/functions/state_combinators/count.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::decode_count_state",
            "crate::engine::mv::agg_state::state_codec::encode_count_state",
        ],
    ),
    (
        "src/exec/expr/agg/functions/state_combinators/min_max.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::MultisetEntry",
            "crate::engine::mv::agg_state::state_codec::decode_multiset_with_key_type",
            "crate::engine::mv::agg_state::state_codec::encode_multiset",
            "crate::engine::mv::agg_state::state_codec::write_key_at",
        ],
    ),
    (
        "src/exec/expr/agg/functions/state_combinators/sum.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::decode_sum_decimal128",
            "crate::engine::mv::agg_state::state_codec::decode_sum_int64",
            "crate::engine::mv::agg_state::state_codec::encode_sum_decimal128",
            "crate::engine::mv::agg_state::state_codec::encode_sum_int64",
        ],
    ),
    (
        "src/exec/expr/function/mv_state/approx_count_distinct.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::KeyValue",
            "crate::engine::mv::agg_state::state_codec::decode_multiset_self_describing",
            "crate::engine::mv::agg_state::state_codec::read_key",
        ],
    ),
    (
        "src/exec/expr/function/mv_state/avg.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::decode_avg_decimal128",
            "crate::engine::mv::agg_state::state_codec::decode_avg_int64",
        ],
    ),
    (
        "src/exec/expr/function/mv_state/bool_or_and.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::decode_bool_state",
            "crate::engine::mv::agg_state::state_codec::encode_bool_state",
        ],
    ),
    (
        "src/exec/expr/function/mv_state/count.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::decode_count_state",
            "crate::engine::mv::agg_state::state_codec::encode_count_state",
        ],
    ),
    (
        "src/exec/expr/function/mv_state/count_distinct.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::MultisetEntry",
            "crate::engine::mv::agg_state::state_codec::decode_multiset_self_describing",
        ],
    ),
    (
        "src/exec/expr/function/mv_state/dispatch.rs",
        &["crate::engine::mv::agg_state::mv_agg_state::aggregate_group_row_id_array"],
    ),
    (
        "src/exec/expr/function/mv_state/min_max.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::KeyValue",
            "crate::engine::mv::agg_state::state_codec::MultisetEntry",
            "crate::engine::mv::agg_state::state_codec::decode_multiset_self_describing",
            "crate::engine::mv::agg_state::state_codec::decode_multiset_with_key_type",
            "crate::engine::mv::agg_state::state_codec::encode_multiset",
            "crate::engine::mv::agg_state::state_codec::key_type_tag_for_data_type",
            "crate::engine::mv::agg_state::state_codec::read_key",
            "crate::engine::mv::agg_state::state_codec::union_multisets",
        ],
    ),
    (
        "src/exec/expr/function/mv_state/sum.rs",
        &[
            "crate::engine::mv::agg_state::state_codec::decode_sum_decimal128",
            "crate::engine::mv::agg_state::state_codec::decode_sum_int64",
            "crate::engine::mv::agg_state::state_codec::encode_sum_decimal128",
            "crate::engine::mv::agg_state::state_codec::encode_sum_int64",
        ],
    ),
    (
        "src/exec/expr/function/string/join_row_key.rs",
        &["crate::engine::mv::iceberg_join_coalesce::stable_join_row_key"],
    ),
    (
        "src/exec/node/iceberg_delta_scan.rs",
        &["crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile"],
    ),
    (
        "src/lower/compat/node/iceberg_delta_scan.rs",
        &["crate::engine::delete_flow::load_existing_delete_visibility_from_descriptors"],
    ),
    (
        "src/lower/novarocks/scan/delete_files.rs",
        &["crate::engine::delete_flow::load_existing_delete_visibility_from_descriptors"],
    ),
    (
        "src/server/mod.rs",
        &[
            "crate::engine::StandaloneNovaRocks",
            "crate::engine::StandaloneOptions",
            "crate::engine::StatementResult",
            "crate::engine::mv_maintenance::MaintenanceCoordinatorConfig",
            "crate::engine::mv_maintenance::start_maintenance_coordinator_for_server",
            "crate::engine::mv_scheduler::RefreshCoordinatorConfig",
            "crate::engine::mv_scheduler::start_refresh_coordinator_for_server",
            "crate::engine::statement::looks_like_show_alter_table_optimize",
            "crate::engine::statement::looks_like_show_create_table",
            "crate::engine::statement::looks_like_show_create_view",
            "crate::engine::statement::looks_like_show_views",
        ],
    ),
    (
        "src/sql/optimizer/rewrite/required_columns.rs",
        &["crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN"],
    ),
    (
        "src/sql/planner/imv_rewrite/action_column.rs",
        &[
            "crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN",
            "crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN",
        ],
    ),
    (
        "src/sql/planner/imv_rewrite/aggregate_rewrite.rs",
        &[
            "crate::engine::mv::agg_state::aggregate_sql_calls::AggregateSqlCalls",
            "crate::engine::mv::agg_state::mv_agg_state::AggregateMvLayout",
            "crate::engine::mv::agg_state::mv_agg_state::AggregateStateColumn",
            "crate::engine::mv::agg_state::mv_agg_state::AggregateStateRole",
            "crate::engine::mv::agg_state::mv_shape::AggregateFunctionKind",
            "crate::engine::mv::agg_state::mv_shape::VisibleAggregateOutput",
            "crate::engine::mv::iceberg_refresh::IcebergMvTarget",
        ],
    ),
    (
        "src/sql/planner/imv_rewrite/annotation.rs",
        &[
            "crate::engine::mv::partition::PartitionDerivationSpec",
            "crate::engine::mv::refresh_context::IcebergMvRewriteContext",
        ],
    ),
    (
        "src/sql/planner/imv_rewrite/apply_key.rs",
        &[
            "crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN",
            "crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN",
        ],
    ),
    (
        "src/sql/planner/imv_rewrite/branch_union.rs",
        &[
            "crate::engine::mv::agg_state::mv_agg_state::AggregateStateRole::RetractionCount",
            "crate::engine::mv::agg_state::mv_agg_state::AggregateStateRole::Single",
            "crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN",
        ],
    ),
    (
        "src/sql/planner/imv_rewrite/entrypoint.rs",
        &["crate::engine::mv::refresh_context::IcebergMvRewriteContext"],
    ),
    (
        "src/sql/planner/imv_rewrite/join_delta.rs",
        &["crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN"],
    ),
    (
        "src/sql/planner/imv_rewrite/join_refresh_builder.rs",
        &[
            "crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN",
            "crate::engine::mv::refresh_context::IcebergMvRewriteContext",
        ],
    ),
    (
        "src/sql/planner/imv_rewrite/join_refresh_descriptor.rs",
        &["crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN"],
    ),
    (
        "src/sql/planner/imv_rewrite/partition_derivation.rs",
        &["crate::engine::mv::partition::resolve_partition_derivation_spec"],
    ),
    (
        "src/sql/planner/imv_rewrite/scan_binding.rs",
        &[
            "crate::engine::mv::refresh_context::IcebergMvRewriteContext",
            "crate::engine::mv::table_ref::IcebergTableRef",
        ],
    ),
    (
        "src/sql/planner/imv_rewrite/target_locator.rs",
        &[
            "crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN",
            "crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN",
            "crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN",
        ],
    ),
    (
        "src/sql/planner/imv_rewrite/union_delta.rs",
        &["crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN"],
    ),
];

const STANDALONE_STATE_DEPENDENCIES: &[(&str, &[&str])] = &[
    (
        "src/connector/iceberg/analyze.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/iceberg/catalog/schema_update.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/iceberg/compact.rs",
        &["crate::engine::StandaloneState"],
    ),
    ("src/connector/mod.rs", &["crate::engine::StandaloneState"]),
    (
        "src/connector/starrocks/table/backend.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/ddl.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/erase.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/ivm_change_stream.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/ivm_delta_source.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/mv_ddl.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/mv_refresh.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/refresh_pin.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/scan_planner.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/connector/starrocks/table/txn.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/backend_ops.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/backend_resolver.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/delete_flow.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/dml_change_stream.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/equality_delete_flow.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_change_stream_write.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_ctas.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_expire_snapshots.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_maintenance.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_ref_flow.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_remove_orphan_files.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_rewrite_manifests.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_truncate.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_view.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_view_rewrite.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/iceberg_writer.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/information_schema.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/insert_flow.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mutation_flow.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv/analysis.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv/dependency.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv/iceberg_backend.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv/iceberg_discovery.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv/iceberg_guard.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv/iceberg_refresh.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv/lake_rebuild.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv/refresh_io.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv/refresh_pin.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv/stateless_rebuild.rs",
        &["crate::engine::StandaloneState"],
    ),
    ("src/engine/mv_flow.rs", &["crate::engine::StandaloneState"]),
    (
        "src/engine/mv_maintenance/mod.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv_maintenance/stats.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv_rewrite_prep.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/mv_scheduler.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/query_prep.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/query_stats.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/statement.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/statistics.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/virtual_table.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/write_operation_lifecycle.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/write_transaction.rs",
        &["crate::engine::StandaloneState"],
    ),
];

const FORWARDING_REEXPORTS: &[&str] = &[];

const CURRENT_ENGINE_BOUNDARY_BASELINE: EngineBoundaryBaseline = EngineBoundaryBaseline {
    file_owners: ENGINE_FILE_OWNERS,
    engine_module_declarations: ENGINE_MODULE_DECLARATIONS,
    external_engine_dependencies: EXTERNAL_ENGINE_DEPENDENCIES,
    standalone_state_dependencies: STANDALONE_STATE_DEPENDENCIES,
    forwarding_reexports: FORWARDING_REEXPORTS,
};

#[derive(Clone, Debug)]
struct GuardSource {
    path: String,
    text: String,
}

impl GuardSource {
    fn new(path: &str, text: &str) -> Self {
        Self {
            path: path.to_string(),
            text: text.to_string(),
        }
    }
}

fn is_engine_path(path: &[String]) -> bool {
    path.len() >= 2 && path[0] == "crate" && path[1] == "engine"
}

fn is_legacy_ebd_2_owner_path(path: &[String]) -> bool {
    path.len() >= 3
        && path[0] == "crate"
        && path[1] == "engine"
        && matches!(path[2].as_str(), "sql_expr" | "procedure")
}

fn is_legacy_ebd_3a_owner_path(path: &[String]) -> bool {
    path.len() >= 3
        && path[0] == "crate"
        && path[1] == "engine"
        && matches!(path[2].as_str(), "parquet" | "stream_load")
}

fn is_legacy_ebd_3b_owner_path(path: &[String]) -> bool {
    path.len() >= 3 && path[0] == "crate" && path[1] == "engine" && path[2] == "query_options"
}

fn rust_all_source_text(text: &str) -> String {
    let tokens = rust_source_tokens(text);
    let mut all_source = rust_lexically_sanitized(text).into_bytes();
    let mut cursor = 0usize;
    while cursor + 1 < tokens.len() {
        if tokens[cursor].text != "#" || tokens[cursor + 1].text != "[" {
            cursor += 1;
            continue;
        }

        let mut depth = 0usize;
        let mut close = cursor + 1;
        while close < tokens.len() {
            match tokens[close].text.as_str() {
                "[" => depth += 1,
                "]" => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            close += 1;
        }
        if close == tokens.len() {
            break;
        }

        let attribute = &text[tokens[cursor].start..tokens[close].end];
        if cfg_attribute_requires_test(attribute) {
            for byte in &mut all_source[tokens[cursor].start..tokens[close].end] {
                if !matches!(*byte, b'\n' | b'\r') {
                    *byte = b' ';
                }
            }
        }
        cursor = close + 1;
    }

    String::from_utf8(all_source).expect("all-source sanitizer must preserve UTF-8")
}

fn legacy_ebd_3b_references(text: &str, source_rel: &str) -> BTreeSet<String> {
    let mut references = rust_all_source_canonical_paths(text, source_rel)
        .into_iter()
        .filter(|path| is_legacy_ebd_3b_owner_path(path))
        .map(|path| format!("path:{}", path.join("::")))
        .collect::<BTreeSet<_>>();
    references.extend(
        rust_source_tokens(text)
            .into_iter()
            .filter(|token| token.text == "StandaloneQueryOptions")
            .map(|_| "symbol:StandaloneQueryOptions".to_string()),
    );
    references
}

fn engine_root_query_options_export_surfaces(text: &str) -> BTreeSet<String> {
    rust_raw_production_use_statements(&rust_all_source_text(text))
        .into_iter()
        .filter(|import| import.inline_modules.is_empty() && import.visibility != "private")
        .filter_map(|import| {
            let export_name = match import.path.alias.as_deref() {
                Some("_") => None,
                Some(alias) => Some(alias),
                None => import.path.segments.last().map(String::as_str),
            }?;
            (export_name == "query_options").then(|| {
                format!(
                    "{}|{}|query_options",
                    import.visibility,
                    import.path.segments.join("::")
                )
            })
        })
        .collect()
}

fn has_top_level_production_struct(text: &str, name: &str) -> bool {
    let production = rust_sanitized_production_text(text);
    let tokens = rust_source_tokens(&production);
    let mut brace_depth = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        match token.text.as_str() {
            "{" => brace_depth += 1,
            "}" => brace_depth = brace_depth.saturating_sub(1),
            "struct"
                if brace_depth == 0
                    && tokens.get(index + 1).is_some_and(|next| next.text == name) =>
            {
                return true;
            }
            _ => {}
        }
    }
    false
}

fn additional_query_options_owner_paths<'a>(
    sources: impl IntoIterator<Item = &'a GuardSource>,
    canonical_owner: &str,
) -> BTreeSet<String> {
    sources
        .into_iter()
        .filter(|source| source.path.starts_with("src/") && source.path != canonical_owner)
        .filter(|source| has_top_level_production_struct(&source.text, "QueryOptions"))
        .map(|source| source.path.clone())
        .collect()
}

const EBD_3C_RESULT_SURFACES: &[&str] = &[
    "QueryResult",
    "QueryResultColumn",
    "build_string_query_result",
    "record_batch_to_chunk",
];

fn is_legacy_ebd_3c_result_path(path: &[String]) -> bool {
    path.len() >= 3
        && path[0] == "crate"
        && path[1] == "engine"
        && EBD_3C_RESULT_SURFACES.contains(&path[2].as_str())
}

fn is_runtime_ebd_3c_result_path(path: &[String]) -> bool {
    path.len() == 4
        && path[0] == "crate"
        && path[1] == "runtime"
        && path[2] == "query_result"
        && EBD_3C_RESULT_SURFACES.contains(&path[3].as_str())
}

fn ebd_3c_forwarded_runtime_paths(path: &[String]) -> Vec<Vec<String>> {
    if is_runtime_ebd_3c_result_path(path) {
        return vec![path.to_vec()];
    }
    let forwards_module = path
        == [
            "crate".to_string(),
            "runtime".to_string(),
            "query_result".to_string(),
        ];
    let forwards_glob = path
        == [
            "crate".to_string(),
            "runtime".to_string(),
            "query_result".to_string(),
            "*".to_string(),
        ];
    if !forwards_module && !forwards_glob {
        return Vec::new();
    }
    EBD_3C_RESULT_SURFACES
        .iter()
        .map(|surface| {
            ["crate", "runtime", "query_result", surface]
                .into_iter()
                .map(str::to_string)
                .collect()
        })
        .collect()
}

#[derive(Clone, Debug)]
struct Ebd3cDeclaration {
    visibility: String,
    name: String,
    inline_modules: Vec<String>,
    alias_targets: Option<Vec<RustScopedUsePath>>,
    audit_targets: Vec<RustScopedUsePath>,
}

fn ebd_3c_visibility(visibility: &syn::Visibility) -> String {
    match visibility {
        syn::Visibility::Inherited => "private".to_string(),
        syn::Visibility::Public(_) => "pub".to_string(),
        syn::Visibility::Restricted(restricted) if restricted.path.is_ident("self") => {
            "private".to_string()
        }
        syn::Visibility::Restricted(restricted) => format!(
            "pub({})",
            restricted
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::")
        ),
    }
}

fn ebd_3c_path_segments(path: &syn::Path) -> Vec<String> {
    path.segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect()
}

struct Ebd3cTypePathCollector<'a> {
    shadows: &'a BTreeSet<String>,
    paths: BTreeSet<Vec<String>>,
}

impl<'ast> syn::visit::Visit<'ast> for Ebd3cTypePathCollector<'_> {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        let segments = ebd_3c_path_segments(path);
        if !segments.is_empty() && !self.shadows.contains(&segments[0]) {
            self.paths.insert(segments);
        }
        syn::visit::visit_path(self, path);
    }
}

#[derive(Default)]
struct Ebd3cExprPathCollector {
    paths: BTreeSet<Vec<String>>,
}

impl<'ast> syn::visit::Visit<'ast> for Ebd3cExprPathCollector {
    fn visit_expr_path(&mut self, path: &'ast syn::ExprPath) {
        let segments = ebd_3c_path_segments(&path.path);
        if !segments.is_empty() {
            self.paths.insert(segments);
        }
        syn::visit::visit_expr_path(self, path);
    }
}

fn ebd_3c_scoped_targets(
    paths: BTreeSet<Vec<String>>,
    inline_modules: &[String],
) -> Vec<RustScopedUsePath> {
    paths
        .into_iter()
        .map(|segments| RustScopedUsePath {
            segments,
            inline_modules: inline_modules.to_vec(),
        })
        .collect()
}

fn ebd_3c_push_value_declaration(
    visibility: String,
    name: String,
    expr: &syn::Expr,
    inline_modules: &[String],
    declarations: &mut Vec<Ebd3cDeclaration>,
) {
    use syn::visit::Visit;

    let mut collector = Ebd3cExprPathCollector::default();
    collector.visit_expr(expr);
    declarations.push(Ebd3cDeclaration {
        visibility,
        name,
        inline_modules: inline_modules.to_vec(),
        alias_targets: None,
        audit_targets: ebd_3c_scoped_targets(collector.paths, inline_modules),
    });
}

fn ebd_3c_collect_declarations(
    items: &[syn::Item],
    inline_modules: &mut Vec<String>,
    declarations: &mut Vec<Ebd3cDeclaration>,
) {
    use syn::visit::Visit;

    for item in items {
        match item {
            syn::Item::Type(item) => {
                let shadows = item
                    .generics
                    .params
                    .iter()
                    .filter_map(|parameter| match parameter {
                        syn::GenericParam::Type(parameter) => Some(parameter.ident.to_string()),
                        syn::GenericParam::Const(parameter) => Some(parameter.ident.to_string()),
                        syn::GenericParam::Lifetime(_) => None,
                    })
                    .collect::<BTreeSet<_>>();
                let mut rhs = Ebd3cTypePathCollector {
                    shadows: &shadows,
                    paths: BTreeSet::new(),
                };
                rhs.visit_type(&item.ty);
                let alias_targets = ebd_3c_scoped_targets(rhs.paths.clone(), inline_modules);

                let mut audit_paths = rhs.paths;
                for parameter in &item.generics.params {
                    match parameter {
                        syn::GenericParam::Type(parameter) => {
                            if let Some(default) = &parameter.default {
                                let mut collector = Ebd3cTypePathCollector {
                                    shadows: &shadows,
                                    paths: BTreeSet::new(),
                                };
                                collector.visit_type(default);
                                audit_paths.extend(collector.paths);
                            }
                        }
                        syn::GenericParam::Const(parameter) => {
                            if let Some(default) = &parameter.default {
                                let mut collector = Ebd3cExprPathCollector::default();
                                collector.visit_expr(default);
                                audit_paths.extend(collector.paths);
                            }
                        }
                        syn::GenericParam::Lifetime(_) => {}
                    }
                }
                declarations.push(Ebd3cDeclaration {
                    visibility: ebd_3c_visibility(&item.vis),
                    name: item.ident.to_string(),
                    inline_modules: inline_modules.clone(),
                    alias_targets: Some(alias_targets),
                    audit_targets: ebd_3c_scoped_targets(audit_paths, inline_modules),
                });
            }
            syn::Item::Const(item) => {
                ebd_3c_push_value_declaration(
                    ebd_3c_visibility(&item.vis),
                    item.ident.to_string(),
                    &item.expr,
                    inline_modules,
                    declarations,
                );
            }
            syn::Item::Static(item) => {
                ebd_3c_push_value_declaration(
                    ebd_3c_visibility(&item.vis),
                    item.ident.to_string(),
                    &item.expr,
                    inline_modules,
                    declarations,
                );
            }
            syn::Item::Impl(item) => {
                for impl_item in &item.items {
                    let syn::ImplItem::Const(item) = impl_item else {
                        continue;
                    };
                    ebd_3c_push_value_declaration(
                        ebd_3c_visibility(&item.vis),
                        item.ident.to_string(),
                        &item.expr,
                        inline_modules,
                        declarations,
                    );
                }
            }
            syn::Item::Trait(item) => {
                let visibility = ebd_3c_visibility(&item.vis);
                for trait_item in &item.items {
                    let syn::TraitItem::Const(item) = trait_item else {
                        continue;
                    };
                    let Some((_, default)) = &item.default else {
                        continue;
                    };
                    ebd_3c_push_value_declaration(
                        visibility.clone(),
                        item.ident.to_string(),
                        default,
                        inline_modules,
                        declarations,
                    );
                }
            }
            syn::Item::Mod(item) => {
                let Some((_, items)) = &item.content else {
                    continue;
                };
                inline_modules.push(item.ident.to_string());
                ebd_3c_collect_declarations(items, inline_modules, declarations);
                inline_modules.pop();
            }
            _ => {}
        }
    }
}

fn ebd_3c_declarations(source: &str) -> Result<Vec<Ebd3cDeclaration>, syn::Error> {
    let file = syn::parse_file(source)?;
    let mut declarations = Vec::new();
    ebd_3c_collect_declarations(&file.items, &mut Vec::new(), &mut declarations);
    Ok(declarations)
}

fn source_defines_named_item(source: &str, kind: &str, name: &str) -> bool {
    let sanitized = rust_lexically_sanitized(source);
    rust_source_tokens(&sanitized)
        .windows(2)
        .any(|tokens| tokens[0].text == kind && tokens[1].text == name)
}

fn ebd_3c_legacy_paths_in_sources(sources: &[GuardSource]) -> BTreeSet<String> {
    sources
        .iter()
        .flat_map(|source| {
            rust_all_source_canonical_paths(&source.text, &source.path)
                .into_iter()
                .filter(|path| is_legacy_ebd_3c_result_path(path))
                .map(|path| format!("{}|{}", source.path, path.join("::")))
        })
        .collect()
}

fn ebd_3c_all_source_forwarding_surfaces(source: &GuardSource) -> BTreeSet<String> {
    let sanitized = rust_lexically_sanitized(&source.text);
    let aliases = rust_scoped_aliases(&sanitized);
    let mut surfaces = BTreeSet::new();
    for import in rust_raw_use_statements(&sanitized)
        .into_iter()
        .filter(|import| import.visibility != "private")
    {
        let Some(export_scope) = forwarding_export_scope(&source.path, &import.inline_modules)
        else {
            continue;
        };
        let Some(export_name) =
            forwarding_export_name(&import.path.segments, import.path.alias.as_deref())
        else {
            continue;
        };
        let Some(targets) = resolve_forwarding_paths(
            &import.path.segments,
            &source.path,
            &import.inline_modules,
            &aliases,
            &mut BTreeSet::new(),
            0,
        ) else {
            continue;
        };
        for target in targets {
            let Some(canonical) = rust_canonical_path_segments_in_scope(
                &target.segments,
                &source.path,
                &target.inline_modules,
            ) else {
                continue;
            };
            if source.path == "src/runtime/query_result.rs" {
                continue;
            }
            for forwarded in ebd_3c_forwarded_runtime_paths(&canonical) {
                surfaces.insert(format!(
                    "{}|{}|{}|{}|{}",
                    source.path,
                    export_scope,
                    import.visibility,
                    export_name,
                    forwarded.join("::")
                ));
            }
        }
    }
    surfaces
}

fn ebd_3c_all_source_declaration_surfaces(source: &GuardSource) -> BTreeSet<String> {
    if source.path == "src/runtime/query_result.rs" {
        return BTreeSet::new();
    }
    let sanitized = rust_lexically_sanitized(&source.text);
    let declarations = ebd_3c_declarations(&source.text)
        .unwrap_or_else(|error| panic!("failed to parse {} for EBD-3C: {error}", source.path));
    let mut aliases = rust_scoped_aliases(&sanitized);

    for import in rust_raw_use_statements(&sanitized)
        .into_iter()
        .filter(|import| import.path.segments.last().is_some_and(|leaf| leaf == "*"))
    {
        let Some(targets) = resolve_forwarding_paths(
            &import.path.segments,
            &source.path,
            &import.inline_modules,
            &aliases,
            &mut BTreeSet::new(),
            0,
        ) else {
            continue;
        };
        for target in targets {
            let Some(canonical) = rust_canonical_path_segments_in_scope(
                &target.segments,
                &source.path,
                &target.inline_modules,
            ) else {
                continue;
            };
            if canonical
                != [
                    "crate".to_string(),
                    "runtime".to_string(),
                    "query_result".to_string(),
                    "*".to_string(),
                ]
            {
                continue;
            }
            for surface in EBD_3C_RESULT_SURFACES {
                aliases
                    .entry((import.inline_modules.clone(), surface.to_string()))
                    .or_default()
                    .push(RustScopedUsePath {
                        segments: ["crate", "runtime", "query_result", surface]
                            .into_iter()
                            .map(str::to_string)
                            .collect(),
                        inline_modules: Vec::new(),
                    });
            }
        }
    }
    for declaration in &declarations {
        if let Some(alias_targets) = &declaration.alias_targets {
            aliases.insert(
                (declaration.inline_modules.clone(), declaration.name.clone()),
                alias_targets.clone(),
            );
        }
    }

    let mut surfaces = BTreeSet::new();
    for declaration in declarations
        .iter()
        .filter(|declaration| declaration.visibility != "private")
    {
        let Some(export_scope) = forwarding_export_scope(&source.path, &declaration.inline_modules)
        else {
            continue;
        };
        for target in &declaration.audit_targets {
            let Some(targets) = resolve_forwarding_paths(
                &target.segments,
                &source.path,
                &target.inline_modules,
                &aliases,
                &mut BTreeSet::new(),
                0,
            ) else {
                continue;
            };
            for target in targets {
                let Some(canonical) = rust_canonical_path_segments_in_scope(
                    &target.segments,
                    &source.path,
                    &target.inline_modules,
                ) else {
                    continue;
                };
                if is_runtime_ebd_3c_result_path(&canonical) {
                    surfaces.insert(format!(
                        "{}|{}|{}|{}|{}",
                        source.path,
                        export_scope,
                        declaration.visibility,
                        declaration.name,
                        canonical.join("::")
                    ));
                }
            }
        }
    }
    surfaces
}

fn ebd_3c_all_source_alias_surfaces(source: &GuardSource) -> BTreeSet<String> {
    ebd_3c_all_source_forwarding_surfaces(source)
        .into_iter()
        .chain(ebd_3c_all_source_declaration_surfaces(source))
        .collect()
}

fn is_legacy_ebd_4a_owner_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments.starts_with(&["crate", "engine", "catalog", "normalize_identifier"])
        || segments.starts_with(&["crate", "engine", "ResolvedLocalTableName"])
        || segments.starts_with(&["crate", "engine", "name_resolve"])
}

fn is_catalog_identifier_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments.starts_with(&["crate", "catalog", "identifier"])
}

fn is_standalone_state_path(path: &[String]) -> bool {
    is_engine_path(path) && path.get(2).is_some_and(|item| item == "StandaloneState")
}

fn is_top_level_frontend_path(path: &[String]) -> bool {
    path.len() >= 2 && path[0] == "crate" && path[1] == "frontend"
}

fn source_is_lower_layer(path: &str) -> bool {
    ["src/sql/", "src/exec/", "src/connector/", "src/meta/"]
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

fn source_is_engine(path: &str) -> bool {
    path == "src/engine.rs" || path.starts_with("src/engine/")
}

fn insert_dependency(
    dependencies: &mut BTreeMap<String, BTreeSet<String>>,
    source: &str,
    dependency: String,
) {
    dependencies
        .entry(source.to_string())
        .or_default()
        .insert(dependency);
}

fn remove_redundant_descendant_paths(paths: BTreeSet<Vec<String>>) -> BTreeSet<Vec<String>> {
    paths
        .iter()
        .filter(|path| {
            !paths.iter().any(|candidate| {
                candidate.len() < path.len() && path.starts_with(candidate.as_slice())
            })
        })
        .cloned()
        .collect()
}

fn forwarding_export_scope(source_path: &str, inline_modules: &[String]) -> Option<String> {
    let mut scope = rust_source_module_segments(source_path)?;
    scope.extend(inline_modules.iter().cloned());
    Some(scope.join("::"))
}

fn forwarding_export_name(path: &[String], alias: Option<&str>) -> Option<String> {
    match alias {
        Some("_") => None,
        Some(alias) => Some(alias.to_string()),
        None => path.last().cloned(),
    }
}

fn forwarding_qualified_alias_candidates(
    path: &[String],
    source_path: &str,
    inline_modules: &[String],
    aliases: &RustScopedAliases,
) -> Vec<((Vec<String>, String), Vec<String>)> {
    let Some(canonical) = rust_canonical_path_segments_in_scope(path, source_path, inline_modules)
    else {
        return Vec::new();
    };
    let Some(source_scope) = rust_source_module_segments(source_path) else {
        return Vec::new();
    };
    let Some(relative) = canonical.strip_prefix(source_scope.as_slice()) else {
        return Vec::new();
    };

    (0..relative.len())
        .rev()
        .filter_map(|index| {
            let alias_key = (relative[..index].to_vec(), relative[index].clone());
            aliases.contains_key(&alias_key).then(|| {
                let suffix = relative[index + 1..].to_vec();
                (alias_key, suffix)
            })
        })
        .collect()
}

fn resolve_forwarding_paths(
    path: &[String],
    source_path: &str,
    inline_modules: &[String],
    aliases: &RustScopedAliases,
    resolving: &mut BTreeSet<(Vec<String>, String)>,
    depth: usize,
) -> Option<Vec<RustScopedUsePath>> {
    if depth > aliases.len() {
        return None;
    }

    let resolved = rust_resolve_scoped_paths(path, inline_modules, aliases, resolving, depth)?;
    let mut targets = BTreeSet::new();
    for resolved_path in resolved {
        let alias_candidates = forwarding_qualified_alias_candidates(
            &resolved_path.segments,
            source_path,
            &resolved_path.inline_modules,
            aliases,
        );
        if alias_candidates.is_empty() {
            targets.insert(resolved_path);
            continue;
        }

        for (alias_key, suffix) in alias_candidates {
            if !resolving.insert(alias_key.clone()) {
                continue;
            }
            let mut candidate_targets = BTreeSet::new();
            for alias_target in &aliases[&alias_key] {
                let mut target_path = alias_target.segments.clone();
                target_path.extend(suffix.iter().cloned());
                if let Some(nested_targets) = resolve_forwarding_paths(
                    &target_path,
                    source_path,
                    &alias_target.inline_modules,
                    aliases,
                    resolving,
                    depth + 1,
                ) {
                    candidate_targets.extend(nested_targets);
                }
            }
            resolving.remove(&alias_key);
            if !candidate_targets.is_empty() {
                targets.extend(candidate_targets);
                break;
            }
        }
    }
    (!targets.is_empty()).then(|| targets.into_iter().collect())
}

fn collect_source_dependencies(snapshot: &mut EngineBoundarySnapshot, source: &GuardSource) {
    let production = rust_sanitized_production_text(&source.text);
    let canonical_paths = rust_production_canonical_paths(&production, &source.path)
        .into_iter()
        .collect::<BTreeSet<_>>();

    if !source_is_engine(&source.path) {
        for path in remove_redundant_descendant_paths(
            canonical_paths
                .iter()
                .filter(|path| is_engine_path(path))
                .cloned()
                .collect(),
        ) {
            insert_dependency(
                &mut snapshot.external_engine_dependencies,
                &source.path,
                path.join("::"),
            );
        }
    }

    for path in canonical_paths
        .iter()
        .filter(|path| is_standalone_state_path(path))
    {
        insert_dependency(
            &mut snapshot.standalone_state_dependencies,
            &source.path,
            path.join("::"),
        );
    }

    if source_is_lower_layer(&source.path) {
        snapshot.lower_layer_frontend_dependencies.extend(
            canonical_paths
                .iter()
                .filter(|path| is_top_level_frontend_path(path))
                .map(|path| format!("{}|{}", source.path, path.join("::"))),
        );
    }

    let scoped_aliases = rust_production_scoped_aliases(&production);
    for import in rust_raw_production_use_statements(&production)
        .into_iter()
        .filter(|import| import.visibility != "private")
    {
        let Some(export_scope) = forwarding_export_scope(&source.path, &import.inline_modules)
        else {
            continue;
        };
        let Some(export_name) =
            forwarding_export_name(&import.path.segments, import.path.alias.as_deref())
        else {
            continue;
        };
        let Some(resolved_targets) = resolve_forwarding_paths(
            &import.path.segments,
            &source.path,
            &import.inline_modules,
            &scoped_aliases,
            &mut BTreeSet::new(),
            0,
        ) else {
            continue;
        };

        for resolved_target in resolved_targets {
            let Some(target) = rust_canonical_path_segments_in_scope(
                &resolved_target.segments,
                &source.path,
                &resolved_target.inline_modules,
            ) else {
                continue;
            };
            let source_engine = source_is_engine(&source.path);
            let target_engine = is_engine_path(&target);
            if source_engine != target_engine {
                snapshot.forwarding_reexports.insert(format!(
                    "{}|{}|{}|{}|{}",
                    source.path,
                    export_scope,
                    import.visibility,
                    export_name,
                    target.join("::")
                ));
            }
        }
    }
}

fn normalized_module_target(source_path: &str, target: &str) -> String {
    let joined = Path::new(source_path)
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join(target);
    let mut normalized = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir if normalized.pop() => {}
            Component::ParentDir => normalized.push(".."),
            Component::Normal(part) => normalized.push(part),
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
        }
    }
    normalized.display().to_string()
}

fn normalized_attribute_token_identity(attribute: &str) -> String {
    let tokens = rust_source_tokens(attribute);
    let mut identity = String::new();
    let mut cursor = 0usize;
    for token in tokens {
        if let Some(value) = decode_rust_string_literal(attribute[cursor..token.start].trim()) {
            identity.push_str(&format!("{value:?}"));
        }
        identity.push_str(&token.text);
        cursor = token.end;
    }
    if let Some(value) = decode_rust_string_literal(attribute[cursor..].trim()) {
        identity.push_str(&format!("{value:?}"));
    }
    identity
}

fn module_path_metadata(source_path: &str, attributes: &[String]) -> String {
    let direct = attributes
        .iter()
        .filter_map(|attribute| path_attribute_value(attribute))
        .map(|target| normalized_module_target(source_path, &target))
        .collect::<BTreeSet<_>>();
    let conditional = attributes
        .iter()
        .flat_map(|attribute| {
            let identity = normalized_attribute_token_identity(attribute);
            cfg_attr_generated_path_values(attribute)
                .into_iter()
                .map(move |target| {
                    format!(
                        "{identity}=>{}",
                        normalized_module_target(source_path, &target)
                    )
                })
        })
        .collect::<BTreeSet<_>>();

    let mut metadata = if direct.is_empty() {
        "default".to_string()
    } else {
        format!(
            "direct:{}",
            direct.into_iter().collect::<Vec<_>>().join(",")
        )
    };
    if !conditional.is_empty() {
        metadata.push_str(";cfg:");
        metadata.push_str(&conditional.into_iter().collect::<Vec<_>>().join(","));
    }
    metadata
}

fn collect_engine_module_declarations(
    snapshot: &mut EngineBoundarySnapshot,
    source_path: &str,
    text: &str,
) {
    for item in rust_module_items(text).into_iter().filter(|item| {
        !item
            .attributes
            .iter()
            .any(|attribute| cfg_attribute_requires_test(attribute))
    }) {
        let inline_scope = item
            .inline_modules
            .iter()
            .map(|module| module.name.as_str())
            .collect::<Vec<_>>()
            .join("::");
        let kind = if item.is_external {
            "external"
        } else {
            "inline"
        };
        snapshot.engine_module_declarations.insert(format!(
            "{source_path}|{inline_scope}|{kind}|path={}|{}",
            module_path_metadata(source_path, &item.attributes),
            item.name
        ));
    }
}

fn collect_engine_boundary_snapshot(
    repo: &Path,
    sources: &[GuardSource],
) -> EngineBoundarySnapshot {
    let mut snapshot = EngineBoundarySnapshot::default();
    for source in sources {
        collect_source_dependencies(&mut snapshot, source);
    }

    let engine_dir = repo.join("src/engine");
    if engine_dir.is_dir() {
        for path in rs_files(&engine_dir) {
            let source_path = rel(&path);
            snapshot.engine_files.insert(source_path.clone());
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {source_path}: {error}"));
            collect_engine_module_declarations(&mut snapshot, &source_path, &text);
        }
    } else {
        for source in sources
            .iter()
            .filter(|source| source_is_engine(&source.path))
        {
            snapshot.engine_files.insert(source.path.clone());
            collect_engine_module_declarations(&mut snapshot, &source.path, &source.text);
        }
    }
    snapshot
}

fn baseline_map(
    entries: &'static [(&'static str, &'static [&'static str])],
) -> BTreeMap<String, BTreeSet<String>> {
    entries
        .iter()
        .map(|(source, dependencies)| {
            (
                (*source).to_string(),
                dependencies
                    .iter()
                    .map(|dependency| (*dependency).to_string())
                    .collect(),
            )
        })
        .collect()
}

fn append_set_diff(
    violations: &mut Vec<String>,
    actual: &BTreeSet<String>,
    expected: &BTreeSet<String>,
    missing_prefix: &str,
    unexpected_prefix: &str,
) {
    violations.extend(
        expected
            .difference(actual)
            .map(|item| format!("{missing_prefix} {item}")),
    );
    violations.extend(
        actual
            .difference(expected)
            .map(|item| format!("{unexpected_prefix} {item}")),
    );
}

fn append_map_diff(
    violations: &mut Vec<String>,
    actual: &BTreeMap<String, BTreeSet<String>>,
    expected: &BTreeMap<String, BTreeSet<String>>,
    missing_prefix: &str,
    unexpected_prefix: &str,
) {
    let actual = actual
        .iter()
        .flat_map(|(source, dependencies)| {
            dependencies
                .iter()
                .map(move |dependency| format!("{source}|{dependency}"))
        })
        .collect::<BTreeSet<_>>();
    let expected = expected
        .iter()
        .flat_map(|(source, dependencies)| {
            dependencies
                .iter()
                .map(move |dependency| format!("{source}|{dependency}"))
        })
        .collect::<BTreeSet<_>>();
    append_set_diff(
        violations,
        &actual,
        &expected,
        missing_prefix,
        unexpected_prefix,
    );
}

fn engine_boundary_violations(
    actual: &EngineBoundarySnapshot,
    baseline: &EngineBoundaryBaseline,
) -> Vec<String> {
    let expected_files = baseline
        .file_owners
        .iter()
        .map(|owner| owner.path.to_string())
        .collect::<BTreeSet<_>>();
    let missing_files = expected_files
        .difference(&actual.engine_files)
        .collect::<BTreeSet<_>>();
    let mut violations = baseline
        .file_owners
        .iter()
        .filter(|owner| missing_files.contains(&owner.path.to_string()))
        .map(|owner| {
            format!(
                "engine-file-missing: {}|target-owner={}|migration-task={}",
                owner.path, owner.target_owner, owner.migration_task
            )
        })
        .collect::<Vec<_>>();
    violations.extend(
        actual
            .engine_files
            .difference(&expected_files)
            .map(|path| format!("engine-file-unexpected: {path}")),
    );

    let expected_modules = baseline
        .engine_module_declarations
        .iter()
        .map(|item| (*item).to_string())
        .collect::<BTreeSet<_>>();
    append_set_diff(
        &mut violations,
        &actual.engine_module_declarations,
        &expected_modules,
        "engine-module-missing:",
        "engine-module-unexpected:",
    );
    append_map_diff(
        &mut violations,
        &actual.external_engine_dependencies,
        &baseline_map(baseline.external_engine_dependencies),
        "engine-dependency-missing:",
        "engine-dependency-unexpected:",
    );
    append_map_diff(
        &mut violations,
        &actual.standalone_state_dependencies,
        &baseline_map(baseline.standalone_state_dependencies),
        "standalone-state-missing:",
        "standalone-state-unexpected:",
    );
    let expected_reexports = baseline
        .forwarding_reexports
        .iter()
        .map(|item| (*item).to_string())
        .collect::<BTreeSet<_>>();
    append_set_diff(
        &mut violations,
        &actual.forwarding_reexports,
        &expected_reexports,
        "forwarding-reexport-missing:",
        "forwarding-reexport-unexpected:",
    );
    violations.extend(
        actual
            .lower_layer_frontend_dependencies
            .iter()
            .map(|item| format!("frontend-reverse-dependency: {item}")),
    );
    violations
}

fn current_source_tree_snapshot() -> EngineBoundarySnapshot {
    let repo = Path::new(manifest_dir());
    let src = src_dir();
    let production_files =
        production_rs_files_from_entries(&src, &[src.join("lib.rs"), src.join("main.rs")]);
    let sources = production_files
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", rel(&path)));
            GuardSource::new(&rel(&path), &text)
        })
        .collect::<Vec<_>>();
    collect_engine_boundary_snapshot(repo, &sources)
}

fn is_strictly_sorted(items: &[&str]) -> bool {
    items.windows(2).all(|pair| pair[0] < pair[1])
}

fn target_owner_is_allowed(target_owner: &str) -> bool {
    const ALLOWED: &[&str] = &[
        "catalog",
        "connector",
        "coordinator",
        "dml",
        "formats",
        "frontend",
        "mv",
        "runtime",
        "sql",
        "statistics",
        "table_maintenance",
    ];
    let owners = target_owner.strip_prefix("split:").unwrap_or(target_owner);
    let owners = owners.split(',').collect::<Vec<_>>();
    !owners.is_empty()
        && owners.windows(2).all(|pair| pair[0] < pair[1])
        && owners.iter().all(|owner| ALLOWED.contains(owner))
}

fn baseline_arrays_are_canonical() -> bool {
    ENGINE_FILE_OWNERS
        .windows(2)
        .all(|pair| pair[0].path < pair[1].path)
        && is_strictly_sorted(ENGINE_MODULE_DECLARATIONS)
        && EXTERNAL_ENGINE_DEPENDENCIES
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0)
        && EXTERNAL_ENGINE_DEPENDENCIES
            .iter()
            .all(|(_, dependencies)| is_strictly_sorted(dependencies))
        && STANDALONE_STATE_DEPENDENCIES
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0)
        && STANDALONE_STATE_DEPENDENCIES
            .iter()
            .all(|(_, dependencies)| is_strictly_sorted(dependencies))
        && is_strictly_sorted(FORWARDING_REEXPORTS)
}

#[test]
fn ebd_1_engine_migration_firewall_matches_source_tree() {
    let actual = current_source_tree_snapshot();
    assert!(
        actual.engine_files.len() >= 76,
        "EBD-1 must scan the full engine tree, found only {} files",
        actual.engine_files.len()
    );
    assert!(
        !actual.external_engine_dependencies.is_empty(),
        "EBD-1 engine dependency scan must be non-vacuous"
    );
    assert!(
        actual.lower_layer_frontend_dependencies.is_empty(),
        "EBD-1 lower layers must not depend on frontend: {:?}",
        actual.lower_layer_frontend_dependencies
    );
    assert!(
        baseline_arrays_are_canonical(),
        "EBD-1 baseline arrays must be exact sorted unique canonical sets"
    );
    let owner_paths = CURRENT_ENGINE_BOUNDARY_BASELINE
        .file_owners
        .iter()
        .map(|entry| entry.path.to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        owner_paths, actual.engine_files,
        "engine owner manifest must be exact"
    );
    assert!(
        CURRENT_ENGINE_BOUNDARY_BASELINE
            .file_owners
            .iter()
            .all(|entry| {
                target_owner_is_allowed(entry.target_owner)
                    && !entry.migration_task.is_empty()
                    && entry
                        .migration_task
                        .split('/')
                        .all(|task| task.starts_with("EBD-"))
            }),
        "every engine file must have an allowed target owner and EBD migration task"
    );
    let violations = engine_boundary_violations(&actual, &CURRENT_ENGINE_BOUNDARY_BASELINE);
    assert!(
        violations.is_empty(),
        "EBD-1 engine migration firewall failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn ebd_2_sql_literal_tools_have_canonical_owners() {
    const NEW_OWNERS: &[&str] = &["src/sql/literal.rs", "src/sql/parser/procedure.rs"];
    const OLD_OWNERS: &[&str] = &["src/engine/sql_expr.rs", "src/engine/procedure.rs"];
    const FORBIDDEN_IDENTIFIERS: &[&str] = &[
        "Chunk",
        "QueryResult",
        "QueryResultColumn",
        "StandaloneSession",
        "StandaloneState",
        "record_batch_to_chunk",
    ];

    let repo = Path::new(manifest_dir());
    let src = src_dir();
    let mut violations = BTreeSet::new();

    for owner in NEW_OWNERS {
        let path = repo.join(owner);
        if !path.is_file() {
            violations.insert(format!("new-owner-missing: {owner}"));
            continue;
        }

        let text = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {owner}: {error}"));
        let all_source = rust_lexically_sanitized(&text);
        for dependency in rust_all_source_canonical_paths(&text, owner)
            .into_iter()
            .filter(|dependency| is_engine_path(dependency))
        {
            violations.insert(format!(
                "new-owner-engine-dependency: {owner}|{}",
                dependency.join("::")
            ));
        }
        for identifier in rust_source_tokens(&all_source)
            .into_iter()
            .map(|token| token.text)
            .filter(|identifier| FORBIDDEN_IDENTIFIERS.contains(&identifier.as_str()))
        {
            violations.insert(format!(
                "new-owner-forbidden-identifier: {owner}|{identifier}"
            ));
        }
    }

    for owner in OLD_OWNERS {
        if repo.join(owner).exists() {
            violations.insert(format!("old-owner-still-present: {owner}"));
        }
    }

    for path in production_rs_files_from_entries(&src, &[src.join("lib.rs"), src.join("main.rs")]) {
        let source = rel(&path);
        if OLD_OWNERS.contains(&source.as_str()) {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {source}: {error}"));
        let production = rust_sanitized_production_text(&text);
        for dependency in rust_production_canonical_paths(&production, &source)
            .into_iter()
            .filter(|dependency| is_legacy_ebd_2_owner_path(dependency))
        {
            violations.insert(format!(
                "old-canonical-path: {source}|{}",
                dependency.join("::")
            ));
        }
    }

    for path in rs_files(&src) {
        let source = rel(&path);
        if OLD_OWNERS.contains(&source.as_str()) {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {source}: {error}"));
        for dependency in rust_all_source_canonical_paths(&text, &source)
            .into_iter()
            .filter(|dependency| is_legacy_ebd_2_owner_path(dependency))
        {
            violations.insert(format!(
                "old-canonical-path: {source}|{}",
                dependency.join("::")
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "EBD-2 SQL/literal owner boundary failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn ebd_2_all_source_detector_covers_test_aliases_and_ignores_noise() {
    let source = r#"
// use crate::engine::sql_expr as legacy_literal;
const DOCUMENTATION: &str = "crate::engine::sql_expr::literal_from_batch";

#[cfg(test)]
mod tests {
    use crate::engine::procedure as legacy;

    fn parser_model() {
        let _ = legacy::CallProcedureStmt::default;
    }
}
"#;

    let production = rust_sanitized_production_text(source);
    assert!(
        rust_production_canonical_paths(&production, "src/sql/literal.rs")
            .into_iter()
            .all(|path| !is_legacy_ebd_2_owner_path(&path)),
        "production scan must continue to ignore cfg(test) source"
    );
    assert_eq!(
        rust_all_source_canonical_paths(source, "src/sql/literal.rs")
            .into_iter()
            .filter(|path| is_legacy_ebd_2_owner_path(path))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            vec!["crate", "engine", "procedure"]
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>(),
            vec![
                "crate",
                "engine",
                "procedure",
                "CallProcedureStmt",
                "default"
            ]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>(),
        ])
    );
}

#[test]
fn ebd_3a_format_io_has_canonical_owners() {
    const NEW_FORMAT_OWNER: &str = "src/formats/parquet/local_io.rs";
    const SQL_LITERAL_OWNER: &str = "src/sql/literal.rs";
    const OLD_OWNERS: &[&str] = &["src/engine/parquet.rs", "src/engine/stream_load.rs"];
    const FORBIDDEN_FORMAT_IDENTIFIERS: &[&str] = &[
        "Chunk",
        "ColumnDef",
        "Literal",
        "QueryResult",
        "StandaloneSession",
        "StandaloneState",
        "TableDef",
    ];
    const TEMPORAL_PARSER_SYMBOLS: &[&str] = &[
        "parse_date_string_to_days",
        "parse_datetime_string_to_micros",
        "parse_datetime_string_to_nanos",
    ];

    let repo = Path::new(manifest_dir());
    let src = src_dir();
    let mut violations = BTreeSet::new();

    let format_owner_path = repo.join(NEW_FORMAT_OWNER);
    if !format_owner_path.is_file() {
        violations.insert(format!("new-owner-missing: {NEW_FORMAT_OWNER}"));
    } else {
        let text = fs::read_to_string(&format_owner_path)
            .unwrap_or_else(|error| panic!("failed to read {NEW_FORMAT_OWNER}: {error}"));
        let all_source = rust_lexically_sanitized(&text);
        for dependency in rust_all_source_canonical_paths(&text, NEW_FORMAT_OWNER)
            .into_iter()
            .filter(|dependency| {
                is_engine_path(dependency)
                    || dependency.len() >= 2 && dependency[0] == "crate" && dependency[1] == "sql"
            })
        {
            violations.insert(format!(
                "new-format-owner-forbidden-dependency: {NEW_FORMAT_OWNER}|{}",
                dependency.join("::")
            ));
        }
        for identifier in rust_source_tokens(&all_source)
            .into_iter()
            .map(|token| token.text)
            .filter(|identifier| FORBIDDEN_FORMAT_IDENTIFIERS.contains(&identifier.as_str()))
        {
            violations.insert(format!(
                "new-format-owner-forbidden-identifier: {NEW_FORMAT_OWNER}|{identifier}"
            ));
        }
    }

    let sql_literal_path = repo.join(SQL_LITERAL_OWNER);
    if !sql_literal_path.is_file() {
        violations.insert(format!("new-owner-missing: {SQL_LITERAL_OWNER}"));
    } else {
        let text = fs::read_to_string(&sql_literal_path)
            .unwrap_or_else(|error| panic!("failed to read {SQL_LITERAL_OWNER}: {error}"));
        let production = rust_sanitized_production_text(&text);
        for dependency in rust_all_source_canonical_paths(&text, SQL_LITERAL_OWNER)
            .into_iter()
            .filter(|dependency| is_engine_path(dependency))
        {
            violations.insert(format!(
                "sql-literal-owner-engine-dependency: {SQL_LITERAL_OWNER}|{}",
                dependency.join("::")
            ));
        }
        let tokens = rust_source_tokens(&production)
            .into_iter()
            .map(|token| token.text)
            .collect::<Vec<_>>();
        for symbol in TEMPORAL_PARSER_SYMBOLS {
            if !tokens
                .windows(2)
                .any(|tokens| tokens[0] == "fn" && tokens[1] == *symbol)
            {
                violations.insert(format!(
                    "temporal-parser-symbol-missing: {SQL_LITERAL_OWNER}|{symbol}"
                ));
            }
        }
    }

    for owner in OLD_OWNERS {
        if repo.join(owner).exists() {
            violations.insert(format!("old-owner-still-present: {owner}"));
        }
    }

    for path in production_rs_files_from_entries(&src, &[src.join("lib.rs"), src.join("main.rs")]) {
        let source = rel(&path);
        if OLD_OWNERS.contains(&source.as_str()) {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {source}: {error}"));
        let production = rust_sanitized_production_text(&text);
        for dependency in rust_production_canonical_paths(&production, &source)
            .into_iter()
            .filter(|dependency| is_legacy_ebd_3a_owner_path(dependency))
        {
            violations.insert(format!(
                "old-canonical-path: {source}|{}",
                dependency.join("::")
            ));
        }
    }

    for root in [&src, &repo.join("tests")] {
        for path in rs_files(root) {
            let source = rel(&path);
            if OLD_OWNERS.contains(&source.as_str()) {
                continue;
            }
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {source}: {error}"));
            for dependency in rust_all_source_canonical_paths(&text, &source)
                .into_iter()
                .filter(|dependency| is_legacy_ebd_3a_owner_path(dependency))
            {
                violations.insert(format!(
                    "old-canonical-path: {source}|{}",
                    dependency.join("::")
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "EBD-3A format I/O owner boundary failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn ebd_3a_all_source_detector_covers_test_aliases_and_ignores_noise() {
    let source = r#"
// use crate::engine::parquet as legacy_parquet;
const DOCUMENTATION: &str = "crate::engine::stream_load::parse_stream_load_payload";

#[cfg(test)]
mod tests {
    use crate::engine::parquet as legacy_parquet;
    use crate::engine::stream_load as legacy_stream_load;

    fn legacy_paths() {
        let _ = legacy_parquet::cast_batch_to_schema;
        let _ = legacy_stream_load::parse_stream_load_payload;
    }
}
"#;

    let production = rust_sanitized_production_text(source);
    assert!(
        rust_production_canonical_paths(&production, "src/formats/parquet/local_io.rs")
            .into_iter()
            .all(|path| !is_legacy_ebd_3a_owner_path(&path)),
        "production scan must continue to ignore cfg(test) source"
    );

    let legacy_paths = rust_all_source_canonical_paths(source, "src/formats/parquet/local_io.rs")
        .into_iter()
        .filter(|path| is_legacy_ebd_3a_owner_path(path))
        .collect::<BTreeSet<_>>();
    assert!(
        legacy_paths.contains(
            &vec!["crate", "engine", "parquet", "cast_batch_to_schema"]
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>()
        ),
        "all-source scan must resolve the cfg(test) parquet alias"
    );
    assert!(
        legacy_paths.contains(
            &vec![
                "crate",
                "engine",
                "stream_load",
                "parse_stream_load_payload"
            ]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
        ),
        "all-source scan must resolve the cfg(test) stream-load alias"
    );
}

#[test]
fn ebd_3b_query_options_have_one_runtime_owner() {
    const RUNTIME_OWNER: &str = "src/runtime/query_options.rs";
    const OLD_OWNER: &str = "src/engine/query_options.rs";
    const FORBIDDEN_RUNTIME_IDENTIFIERS: &[&str] = &[
        "NovaRocksMysqlShim",
        "SessionDatabaseContext",
        "StandaloneSession",
        "StandaloneState",
    ];

    let repo = Path::new(manifest_dir());
    let src = src_dir();
    let mut violations = BTreeSet::new();

    let runtime_owner_path = repo.join(RUNTIME_OWNER);
    if !runtime_owner_path.is_file() {
        violations.insert(format!("runtime-owner-missing: {RUNTIME_OWNER}"));
    } else {
        let text = fs::read_to_string(&runtime_owner_path)
            .unwrap_or_else(|error| panic!("failed to read {RUNTIME_OWNER}: {error}"));
        let tokens = rust_source_tokens(&text)
            .into_iter()
            .map(|token| token.text)
            .collect::<Vec<_>>();
        if !has_top_level_production_struct(&text, "QueryOptions") {
            violations.insert(format!(
                "runtime-contract-missing: {RUNTIME_OWNER}|QueryOptions"
            ));
        }
        for dependency in rust_all_source_canonical_paths(&text, RUNTIME_OWNER)
            .into_iter()
            .filter(|dependency| {
                is_engine_path(dependency)
                    || dependency.len() >= 2
                        && dependency[0] == "crate"
                        && matches!(dependency[1].as_str(), "frontend" | "server")
            })
        {
            violations.insert(format!(
                "runtime-owner-forbidden-dependency: {RUNTIME_OWNER}|{}",
                dependency.join("::")
            ));
        }
        for identifier in tokens
            .iter()
            .filter(|identifier| FORBIDDEN_RUNTIME_IDENTIFIERS.contains(&identifier.as_str()))
        {
            violations.insert(format!(
                "runtime-owner-forbidden-identifier: {RUNTIME_OWNER}|{identifier}"
            ));
        }
    }

    if repo.join(OLD_OWNER).exists() {
        violations.insert(format!("old-owner-still-present: {OLD_OWNER}"));
    }

    let engine_root = repo.join("src/engine/mod.rs");
    let engine_root_text = fs::read_to_string(&engine_root)
        .unwrap_or_else(|error| panic!("failed to read src/engine/mod.rs: {error}"));
    if rust_module_items(&engine_root_text)
        .into_iter()
        .any(|item| item.name == "query_options")
    {
        violations.insert("old-module-still-declared: src/engine/mod.rs|query_options".to_string());
    }
    for export in engine_root_query_options_export_surfaces(&engine_root_text) {
        violations.insert(format!("engine-query-options-surface-reexport: {export}"));
    }

    let src_sources = rs_files(&src)
        .into_iter()
        .map(|path| {
            let source = rel(&path);
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {source}: {error}"));
            GuardSource::new(&source, &text)
        })
        .collect::<Vec<_>>();
    for duplicate_owner in additional_query_options_owner_paths(&src_sources, RUNTIME_OWNER) {
        violations.insert(format!("duplicate-query-options-owner: {duplicate_owner}"));
    }

    for root in [&src, &repo.join("tests")] {
        for path in rs_files(root) {
            let source = rel(&path);
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {source}: {error}"));
            for reference in legacy_ebd_3b_references(&text, &source) {
                violations.insert(format!(
                    "legacy-query-options-reference: {source}|{reference}"
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "EBD-3B query options owner boundary failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn ebd_3b_detector_covers_test_aliases_reexports_and_ignores_noise() {
    let source = r###"
// use crate::engine::query_options::StandaloneQueryOptions;
const DOCUMENTATION: &str = "crate::engine::query_options::StandaloneQueryOptions";
const RAW_DOCUMENTATION: &str = r#"StandaloneQueryOptions via crate::engine::query_options"#;
use crate::runtime::query_options::QueryOptions;

#[cfg(test)]
mod tests {
    use crate::engine::query_options as legacy;
    pub(crate) use legacy::StandaloneQueryOptions as LegacyOptions;

    fn legacy_options(_: LegacyOptions) {}
}
"###;

    assert!(
        legacy_ebd_3b_references(
            &rust_sanitized_production_text(source),
            "src/runtime/query_options.rs"
        )
        .is_empty(),
        "production scan must ignore cfg(test), comments, strings, raw strings, and QueryOptions"
    );

    let references = legacy_ebd_3b_references(source, "src/runtime/query_options.rs");
    assert!(
        references.contains("path:crate::engine::query_options"),
        "all-source scan must resolve the cfg(test) namespace alias: {references:?}"
    );
    assert!(
        references.contains("path:crate::engine::query_options::StandaloneQueryOptions"),
        "all-source scan must resolve the cfg(test) forwarding re-export: {references:?}"
    );
    assert_eq!(
        references
            .iter()
            .filter(|reference| reference.starts_with("symbol:"))
            .collect::<Vec<_>>(),
        vec![&"symbol:StandaloneQueryOptions".to_string()],
        "the legacy symbol detector must be exact and deduplicated"
    );
}

#[test]
fn ebd_3b_detector_rejects_direct_and_grouped_legacy_targets_and_reverse_surfaces() {
    let direct_legacy = r#"
pub(crate) use crate::engine::query_options::StandaloneQueryOptions as LegacyOptions;
"#;
    let grouped_legacy = r#"
pub(crate) use crate::engine::{query_options::StandaloneQueryOptions as LegacyOptions};
"#;
    let test_direct_legacy = r#"
#[cfg(test)]
pub(crate) use crate::engine::query_options::StandaloneQueryOptions as LegacyOptions;
"#;
    let test_grouped_legacy = r#"
#[cfg(test)]
pub(crate) use crate::engine::{query_options::StandaloneQueryOptions as LegacyOptions};
"#;
    for source in [
        direct_legacy,
        grouped_legacy,
        test_direct_legacy,
        test_grouped_legacy,
    ] {
        let references = legacy_ebd_3b_references(source, "src/runtime/query_options.rs");
        assert!(
            references.contains("path:crate::engine::query_options::StandaloneQueryOptions"),
            "legacy target direction must be exact for direct and grouped imports: {references:?}"
        );
    }

    let direct_reverse = r#"
pub(crate) use crate::runtime::query_options as query_options;
"#;
    let grouped_reverse = r#"
pub(crate) use crate::runtime::{query_options};
"#;
    let test_only_reverse = r#"
#[cfg(test)]
pub(crate) use crate::runtime::{query_options};
"#;
    let test_only_direct_reverse = r#"
#[cfg(test)]
pub(crate) use crate::runtime::query_options as query_options;
"#;
    for source in [
        direct_reverse,
        grouped_reverse,
        test_only_direct_reverse,
        test_only_reverse,
    ] {
        let surfaces = engine_root_query_options_export_surfaces(source);
        assert_eq!(
            surfaces.len(),
            1,
            "engine root reverse surface must be caught for direct, grouped, and test source: {surfaces:?}"
        );
    }

    let unrelated = r#"
pub(crate) use crate::runtime::query_options::QueryOptions;
mod nested {
    pub(crate) use crate::runtime::query_options as query_options;
}
"#;
    assert!(
        engine_root_query_options_export_surfaces(unrelated).is_empty(),
        "a type export or nested namespace must not reconstruct crate::engine::query_options"
    );

    let transitive_reverse = r#"
mod bridge {
    pub(crate) use crate::runtime::query_options as query_options;
}
pub(crate) use bridge::query_options;
"#;
    assert_eq!(
        engine_root_query_options_export_surfaces(transitive_reverse),
        BTreeSet::from(["pub(crate)|bridge::query_options|query_options".to_string()]),
        "the root local export name must reject a transitive query_options surface"
    );
}

#[test]
fn ebd_3b_runtime_owner_requires_top_level_production_struct() {
    assert!(has_top_level_production_struct(
        "pub(crate) struct QueryOptions {}",
        "QueryOptions"
    ));

    for fake_owner in [
        r#"mod nested { pub(crate) struct QueryOptions {} }"#,
        r#"#[cfg(test)] struct QueryOptions;"#,
        r#"#[cfg(test)] mod tests { struct QueryOptions; }"#,
        r#"const DOC: &str = "struct QueryOptions"; // struct QueryOptions"#,
    ] {
        assert!(
            !has_top_level_production_struct(fake_owner, "QueryOptions"),
            "nested, cfg(test), comment, and string definitions are not runtime owners: {fake_owner}"
        );
    }

    assert!(
        legacy_ebd_3b_references(
            "#[cfg(test)] struct QueryOptions;",
            "tests/query_options_fixture.rs"
        )
        .is_empty(),
        "unrelated same-name test structs are not legacy EBD-3B references"
    );

    let sources = [
        GuardSource::new(
            "src/runtime/query_options.rs",
            "pub(crate) struct QueryOptions {}",
        ),
        GuardSource::new("src/other.rs", "pub(crate) struct QueryOptions {}"),
        GuardSource::new(
            "src/nested.rs",
            "mod nested { pub(crate) struct QueryOptions {} }",
        ),
        GuardSource::new(
            "src/test_only.rs",
            "#[cfg(test)] pub(crate) struct QueryOptions;",
        ),
        GuardSource::new("tests/fixture.rs", "pub(crate) struct QueryOptions {}"),
    ];
    assert_eq!(
        additional_query_options_owner_paths(&sources, "src/runtime/query_options.rs"),
        BTreeSet::from(["src/other.rs".to_string()]),
        "only a second top-level production owner under src must be rejected"
    );
}

#[test]
fn ebd_3c_query_result_boundary_has_one_runtime_owner() {
    let repo = Path::new(manifest_dir());
    let owner_rel = "src/runtime/query_result.rs";
    let owner_text = fs::read_to_string(repo.join(owner_rel)).unwrap();
    let owner_production = rust_sanitized_production_text(&owner_text);
    let mut violations = BTreeSet::new();

    for (kind, name) in [
        ("struct", "QueryResult"),
        ("struct", "QueryResultColumn"),
        ("fn", "build_string_query_result"),
        ("fn", "record_batch_to_chunk"),
    ] {
        if !source_defines_named_item(&owner_production, kind, name) {
            violations.insert(format!("runtime-owner-missing: {kind} {name}"));
        }
    }

    for path in rust_production_canonical_paths(&owner_production, owner_rel) {
        let canonical = path.join("::");
        if ["crate::engine", "crate::frontend", "crate::server"]
            .iter()
            .any(|prefix| canonical == *prefix || canonical.starts_with(&format!("{prefix}::")))
        {
            violations.insert(format!("runtime-reverse-dependency: {canonical}"));
        }
        if canonical == "crate::sql" || canonical.starts_with("crate::sql::") {
            violations.insert(format!("runtime-sql-dependency-growth: {canonical}"));
        }
    }
    if rust_source_tokens(&owner_production)
        .iter()
        .any(|token| token.text == "StandaloneState")
    {
        violations.insert("runtime-owner-mentions-StandaloneState".to_string());
    }

    let expected_runtime_sql_edges = BTreeSet::new();
    let actual_runtime_sql_edges = rs_files(&repo.join("src/runtime"))
        .into_iter()
        .flat_map(|path| {
            let source = rel(&path);
            let text = fs::read_to_string(path).unwrap();
            let production = rust_sanitized_production_text(&text);
            rust_production_canonical_paths(&production, &source)
                .into_iter()
                .filter(|path| path.len() >= 2 && path[0] == "crate" && path[1] == "sql")
                .map(move |path| format!("{source}|{}", path.join("::")))
        })
        .collect::<BTreeSet<_>>();
    for missing in expected_runtime_sql_edges.difference(&actual_runtime_sql_edges) {
        violations.insert(format!("runtime-sql-edge-missing: {missing}"));
    }
    for unexpected in actual_runtime_sql_edges.difference(&expected_runtime_sql_edges) {
        violations.insert(format!("runtime-sql-edge-unexpected: {unexpected}"));
    }

    let guard_rel = "tests/architecture_guard/ebd_1_engine_boundary.rs";
    let sources = rs_files(&repo.join("src"))
        .into_iter()
        .chain(rs_files(&repo.join("tests")))
        .filter(|path| rel(path) != guard_rel)
        .map(|path| {
            let source = rel(&path);
            let text = fs::read_to_string(path).unwrap();
            GuardSource::new(&source, &text)
        })
        .collect::<Vec<_>>();
    violations.extend(ebd_3c_legacy_paths_in_sources(&sources));

    for path in rs_files(&repo.join("src")) {
        let source = rel(&path);
        let text = fs::read_to_string(path).unwrap();
        for (kind, name) in [
            ("struct", "QueryResult"),
            ("struct", "QueryResultColumn"),
            ("enum", "QueryResult"),
            ("enum", "QueryResultColumn"),
            ("trait", "QueryResult"),
            ("trait", "QueryResultColumn"),
            ("type", "QueryResult"),
            ("type", "QueryResultColumn"),
            ("fn", "build_string_query_result"),
            ("fn", "record_batch_to_chunk"),
        ] {
            if source_defines_named_item(&text, kind, name) && source != owner_rel {
                violations.insert(format!("duplicate-result-owner: {source}|{kind} {name}"));
            }
        }
    }
    violations.extend(sources.iter().flat_map(ebd_3c_all_source_alias_surfaces));

    assert!(
        violations.is_empty(),
        "EBD-3C query-result boundary failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn ebd_3c_all_source_declaration_detector_covers_alias_test_reexport_and_ignores_noise() {
    let malicious = GuardSource::new(
        "src/engine/mod.rs",
        r#"
use crate::engine::QueryResult;
use crate::engine::{QueryResultColumn as LegacyColumn, record_batch_to_chunk};
#[cfg(test)]
mod tests {
    use crate::engine::build_string_query_result as text_result;
}
use crate::runtime::query_result as canonical_result;
#[cfg(test)]
pub use canonical_result::QueryResult as EngineResult;
pub type EngineResultAlias = crate::runtime::query_result::QueryResult;
type PrivateResultAlias = crate::runtime::query_result::QueryResult;
pub type TransitiveResultAlias = PrivateResultAlias;
#[cfg(test)]
pub type EngineColumnAlias = canonical_result::QueryResultColumn;
#[cfg(test)]
pub use crate::runtime::query_result as TestResultModule;
pub use canonical_result::*;
pub use canonical_result::build_string_query_result as TextResultBuilder;
pub use canonical_result::record_batch_to_chunk as ChunkBuilder;
"#,
    );
    let legacy = ebd_3c_legacy_paths_in_sources(std::slice::from_ref(&malicious));
    for expected in [
        "crate::engine::QueryResult",
        "crate::engine::QueryResultColumn",
        "crate::engine::record_batch_to_chunk",
        "crate::engine::build_string_query_result",
    ] {
        assert!(
            legacy.iter().any(|hit| hit.ends_with(expected)),
            "missing {expected}: {legacy:?}"
        );
    }
    let forwarding = ebd_3c_all_source_alias_surfaces(&malicious);
    assert!(
        forwarding.iter().any(|hit| {
            hit.contains("|EngineResult|")
                && hit.ends_with("crate::runtime::query_result::QueryResult")
        }),
        "test-only transitive forwarding must fail: {forwarding:?}"
    );
    for (export_name, surface) in [
        ("EngineResultAlias", "QueryResult"),
        ("TransitiveResultAlias", "QueryResult"),
        ("EngineColumnAlias", "QueryResultColumn"),
        ("TextResultBuilder", "build_string_query_result"),
        ("ChunkBuilder", "record_batch_to_chunk"),
    ] {
        assert!(
            forwarding.iter().any(|hit| {
                hit.contains(&format!("|{export_name}|"))
                    && hit.ends_with(&format!("crate::runtime::query_result::{surface}"))
            }),
            "renamed alias must fail for {surface}: {forwarding:?}"
        );
    }
    for export_name in ["TestResultModule", "*"] {
        for surface in EBD_3C_RESULT_SURFACES {
            assert!(
                forwarding.iter().any(|hit| {
                    hit.contains(&format!("|{export_name}|"))
                        && hit.ends_with(&format!("crate::runtime::query_result::{surface}"))
                }),
                "module/glob forwarding must expose {surface}: {forwarding:?}"
            );
        }
    }

    let clean = GuardSource::new(
        "src/consumer.rs",
        r##"
// use crate::engine::QueryResult;
const DOC: &str = r#"crate::engine::{QueryResult, record_batch_to_chunk}"#;
fn query_result_local_name() {}
use crate::runtime::query_result::QueryResult;
type LocalResult = crate::runtime::query_result::QueryResult;
"##,
    );
    assert!(ebd_3c_legacy_paths_in_sources(std::slice::from_ref(&clean)).is_empty());
    assert!(ebd_3c_all_source_alias_surfaces(&clean).is_empty());

    assert_query_result_declaration_detector_rejects_generic_defaults_and_callable_aliases();
    assert_query_result_declaration_detector_respects_generic_shadowing_and_private_consumers();
}

fn assert_query_result_declaration_detector_rejects_generic_defaults_and_callable_aliases() {
    let malicious = GuardSource::new(
        "src/engine/mod.rs",
        r#"
pub type DefaultedResult<T = crate::runtime::query_result::QueryResult> = T;
pub(crate) const STRING_RESULT_FACTORY: fn(&str, Vec<String>)
    -> Result<crate::runtime::query_result::QueryResult, String> =
    crate::runtime::query_result::build_string_query_result;
#[cfg(test)]
pub static CHUNK_FACTORY: fn(arrow::record_batch::RecordBatch)
    -> Result<crate::exec::chunk::Chunk, String> =
    crate::runtime::query_result::record_batch_to_chunk;
pub const CAST_CHUNK_FACTORY: fn(arrow::record_batch::RecordBatch)
    -> Result<crate::exec::chunk::Chunk, String> =
    crate::runtime::query_result::record_batch_to_chunk
        as fn(arrow::record_batch::RecordBatch)
            -> Result<crate::exec::chunk::Chunk, String>;
pub const NESTED_STRING_FACTORY: fn(&str, Vec<String>)
    -> Result<crate::runtime::query_result::QueryResult, String> = {
    let factory = crate::runtime::query_result::build_string_query_result;
    factory
};

pub struct ResultFactories;
impl ResultFactories {
    pub const IMPL_TEXT: fn(&str, Vec<String>)
        -> Result<crate::runtime::query_result::QueryResult, String> =
        crate::runtime::query_result::build_string_query_result;
    pub const IMPL_CHUNK: fn(arrow::record_batch::RecordBatch)
        -> Result<crate::exec::chunk::Chunk, String> = {
        crate::runtime::query_result::record_batch_to_chunk
            as fn(arrow::record_batch::RecordBatch)
                -> Result<crate::exec::chunk::Chunk, String>
    };
    const PRIVATE_TEXT: fn(&str, Vec<String>)
        -> Result<crate::runtime::query_result::QueryResult, String> =
        crate::runtime::query_result::build_string_query_result;
}

pub trait ResultFactoryDefaults {
    const TRAIT_TEXT: fn(&str, Vec<String>)
        -> Result<crate::runtime::query_result::QueryResult, String> =
        crate::runtime::query_result::build_string_query_result;
}
"#,
    );

    let declarations = ebd_3c_all_source_alias_surfaces(&malicious);
    for (export_name, surface) in [
        ("DefaultedResult", "QueryResult"),
        ("STRING_RESULT_FACTORY", "build_string_query_result"),
        ("CHUNK_FACTORY", "record_batch_to_chunk"),
        ("CAST_CHUNK_FACTORY", "record_batch_to_chunk"),
        ("NESTED_STRING_FACTORY", "build_string_query_result"),
        ("IMPL_TEXT", "build_string_query_result"),
        ("IMPL_CHUNK", "record_batch_to_chunk"),
        ("TRAIT_TEXT", "build_string_query_result"),
    ] {
        assert!(
            declarations.iter().any(|hit| {
                hit.contains(&format!("|{export_name}|"))
                    && hit.ends_with(&format!("crate::runtime::query_result::{surface}"))
            }),
            "visible declaration must fail for {surface}: {declarations:?}"
        );
    }
}

fn assert_query_result_declaration_detector_respects_generic_shadowing_and_private_consumers() {
    let clean = GuardSource::new(
        "src/consumer.rs",
        r#"
type T = crate::runtime::query_result::QueryResult;
pub type Identity<T> = T;
const LOCAL_STRING_FACTORY: fn(&str, Vec<String>)
    -> Result<crate::runtime::query_result::QueryResult, String> =
    crate::runtime::query_result::build_string_query_result;
static LOCAL_CHUNK_FACTORY: fn(arrow::record_batch::RecordBatch)
    -> Result<crate::exec::chunk::Chunk, String> =
    crate::runtime::query_result::record_batch_to_chunk;

pub struct CleanFactories;
impl CleanFactories {
    const PRIVATE_CHUNK: fn(arrow::record_batch::RecordBatch)
        -> Result<crate::exec::chunk::Chunk, String> =
        crate::runtime::query_result::record_batch_to_chunk;
}

trait PrivateFactoryDefaults {
    const PRIVATE_TEXT: fn(&str, Vec<String>)
        -> Result<crate::runtime::query_result::QueryResult, String> =
        crate::runtime::query_result::build_string_query_result;
}
"#,
    );

    assert!(
        ebd_3c_all_source_alias_surfaces(&clean).is_empty(),
        "generic parameters shadow module aliases and private consumers stay allowed"
    );
}

const EBD_4A_OWNER: &str = "src/catalog/identifier.rs";
const EBD_4A_CANONICAL_STRUCTS: &[&str] = &[
    "CatalogNamespaceIdentity",
    "LocalTableIdentity",
    "TableIdentity",
];
const EBD_4A_CANONICAL_FUNCTIONS: &[&str] = &[
    "normalize_identifier",
    "normalize_optional_identifier",
    "resolve_catalog_namespace_name",
    "resolve_catalog_table_name",
    "resolve_local_table_name",
];

fn ebd_4a_dependency_crate_roots_from_manifest(manifest: &toml::Value) -> BTreeSet<String> {
    let mut roots = BTreeSet::new();
    let mut collect = |table: &toml::map::Map<String, toml::Value>| {
        for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
            let Some(dependencies) = table.get(section).and_then(toml::Value::as_table) else {
                continue;
            };
            roots.extend(dependencies.keys().map(|name| name.replace('-', "_")));
        }
    };
    if let Some(root) = manifest.as_table() {
        collect(root);
        if let Some(targets) = root.get("target").and_then(toml::Value::as_table) {
            for target in targets.values().filter_map(toml::Value::as_table) {
                collect(target);
            }
        }
    }
    roots
}

fn ebd_4a_dependency_crate_roots() -> BTreeSet<String> {
    let manifest = fs::read_to_string(Path::new(manifest_dir()).join("Cargo.toml"))
        .expect("read Cargo.toml for EBD-4A dependency audit");
    let manifest = manifest
        .parse::<toml::Value>()
        .expect("parse Cargo.toml for EBD-4A dependency audit");
    ebd_4a_dependency_crate_roots_from_manifest(&manifest)
}

fn ebd_4a_allowed_catalog_path(path: &[String]) -> bool {
    path == ["crate"] || (path.len() >= 2 && path[0] == "crate" && path[1] == "catalog")
}

fn ebd_4a_audit_catalog_dependencies(source: &GuardSource) -> BTreeSet<String> {
    struct ExternalPathAudit<'a> {
        dependency_roots: &'a BTreeSet<String>,
        source: &'a str,
        violations: BTreeSet<String>,
    }

    impl<'ast> syn::visit::Visit<'ast> for ExternalPathAudit<'_> {
        fn visit_item_extern_crate(&mut self, item: &'ast syn::ItemExternCrate) {
            let root = item.ident.to_string();
            if !matches!(root.as_str(), "std" | "core" | "alloc") {
                self.violations.insert(format!(
                    "catalog-forbidden-external-crate: {}|{root}",
                    self.source
                ));
            }
            syn::visit::visit_item_extern_crate(self, item);
        }

        fn visit_path(&mut self, path: &'ast syn::Path) {
            let segments = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            if let Some(root) = segments.first()
                && self.dependency_roots.contains(root)
                && !matches!(root.as_str(), "std" | "core" | "alloc")
            {
                self.violations.insert(format!(
                    "catalog-forbidden-external-path: {}|{}",
                    self.source,
                    segments.join("::")
                ));
            }
            if segments.first().is_some_and(|segment| segment == "crate")
                && !ebd_4a_allowed_catalog_path(&segments)
            {
                self.violations.insert(format!(
                    "catalog-forbidden-crate-path: {}|{}",
                    self.source,
                    segments.join("::")
                ));
            }
            syn::visit::visit_path(self, path);
        }
    }

    let sanitized = rust_lexically_sanitized(&source.text);
    let dependency_roots = ebd_4a_dependency_crate_roots();
    let aliases = super::rust_scoped_aliases(&sanitized);
    let mut violations = BTreeSet::new();

    if rust_source_tokens(&sanitized)
        .iter()
        .any(|token| token.text == "StandaloneState")
    {
        violations.insert(format!(
            "catalog-forbidden-StandaloneState-token: {}",
            source.path
        ));
    }

    for import in super::rust_raw_use_statements(&sanitized) {
        let resolved = rust_resolve_scoped_paths(
            &import.path.segments,
            &import.inline_modules,
            &aliases,
            &mut BTreeSet::new(),
            0,
        )
        .unwrap_or_else(|| {
            vec![RustScopedUsePath {
                segments: import.path.segments,
                inline_modules: import.inline_modules,
            }]
        });
        for path in resolved {
            let root = path.segments.first().map(String::as_str);
            if matches!(root, Some("std" | "core" | "alloc")) {
                continue;
            }
            if matches!(root, Some("crate" | "self" | "super")) {
                let canonical = rust_canonical_path_segments_in_scope(
                    &path.segments,
                    &source.path,
                    &path.inline_modules,
                );
                if canonical
                    .as_deref()
                    .is_none_or(|canonical| !ebd_4a_allowed_catalog_path(canonical))
                {
                    violations.insert(format!(
                        "catalog-forbidden-use: {}|{}",
                        source.path,
                        path.segments.join("::")
                    ));
                }
                continue;
            }
            violations.insert(format!(
                "catalog-forbidden-use: {}|{}",
                source.path,
                path.segments.join("::")
            ));
        }
    }

    for path in rust_all_source_canonical_paths(&source.text, &source.path)
        .into_iter()
        .filter(|path| path.first().is_some_and(|segment| segment == "crate"))
        .filter(|path| !ebd_4a_allowed_catalog_path(path))
    {
        violations.insert(format!(
            "catalog-forbidden-crate-path: {}|{}",
            source.path,
            path.join("::")
        ));
    }

    if let Ok(file) = syn::parse_file(&source.text) {
        let mut audit = ExternalPathAudit {
            dependency_roots: &dependency_roots,
            source: &source.path,
            violations: BTreeSet::new(),
        };
        syn::visit::Visit::visit_file(&mut audit, &file);
        violations.extend(audit.violations);
    } else {
        violations.insert(format!("catalog-source-parse-failed: {}", source.path));
    }
    violations
}

fn ebd_4a_audit_owner_items(source: &GuardSource) -> BTreeSet<String> {
    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!("catalog-owner-parse-failed: {}", source.path)]);
    };
    let structs = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Struct(item) => Some(item.ident.to_string()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let functions = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Fn(item) => Some(item.sig.ident.to_string()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let mut violations = BTreeSet::new();
    for item in EBD_4A_CANONICAL_STRUCTS {
        if !structs.contains(*item) {
            violations.insert(format!("catalog-owner-struct-missing: {item}"));
        }
    }
    for item in EBD_4A_CANONICAL_FUNCTIONS {
        if !functions.contains(*item) {
            violations.insert(format!("catalog-owner-function-missing: {item}"));
        }
    }
    if functions.contains("resolve_iceberg_table_name_explicit") {
        violations.insert(
            "catalog-owner-zero-caller-helper-present: resolve_iceberg_table_name_explicit"
                .to_string(),
        );
    }
    violations
}

fn ebd_4a_audit_exact_legacy_owner_definitions(sources: &[GuardSource]) -> BTreeSet<String> {
    struct LegacyDefinitionAudit<'a> {
        source_path: &'a str,
        item_name: &'a str,
        allowed_kinds: &'a [&'a str],
        violations: BTreeSet<String>,
    }

    impl LegacyDefinitionAudit<'_> {
        fn reject(&mut self, kind: &str, name: &syn::Ident) {
            if name == self.item_name && self.allowed_kinds.contains(&kind) {
                self.violations.insert(format!(
                    "catalog-legacy-owner-definition: {}|{kind} {}",
                    self.source_path, self.item_name
                ));
            }
        }
    }

    impl<'ast> syn::visit::Visit<'ast> for LegacyDefinitionAudit<'_> {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            self.reject("function", &item.sig.ident);
            syn::visit::visit_item_fn(self, item);
        }

        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            self.reject("struct", &item.ident);
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            self.reject("type", &item.ident);
            syn::visit::visit_item_type(self, item);
        }
    }

    let mut violations = BTreeSet::new();
    for (source_path, item_name, allowed_kinds) in [
        (
            "src/engine/catalog.rs",
            "normalize_identifier",
            &["function", "type"] as &[&str],
        ),
        (
            "src/engine/catalog_mgr/metadata.rs",
            "TableIdentity",
            &["struct", "type"] as &[&str],
        ),
        (
            "src/engine/mod.rs",
            "ResolvedLocalTableName",
            &["struct", "type"] as &[&str],
        ),
    ] {
        let Some(source) = sources.iter().find(|source| source.path == source_path) else {
            continue;
        };
        let Ok(file) = syn::parse_file(&source.text) else {
            violations.insert(format!("catalog-legacy-owner-parse-failed: {source_path}"));
            continue;
        };
        let mut audit = LegacyDefinitionAudit {
            source_path,
            item_name,
            allowed_kinds,
            violations: BTreeSet::new(),
        };
        syn::visit::Visit::visit_file(&mut audit, &file);
        violations.extend(audit.violations);
    }
    violations
}

fn ebd_4a_audit_legacy_paths_and_forwarding(sources: &[GuardSource]) -> BTreeSet<String> {
    let mut violations = BTreeSet::new();
    for source in sources {
        for path in rust_all_source_canonical_paths(&source.text, &source.path)
            .into_iter()
            .filter(|path| is_legacy_ebd_4a_owner_path(path))
        {
            violations.insert(format!(
                "catalog-legacy-path: {}|{}",
                source.path,
                path.join("::")
            ));
        }

        if source.path == EBD_4A_OWNER {
            continue;
        }
        let sanitized = rust_lexically_sanitized(&source.text);
        let aliases = super::rust_scoped_aliases(&sanitized);
        for import in super::rust_raw_use_statements(&sanitized)
            .into_iter()
            .filter(|import| import.visibility != "private")
        {
            let Some(resolved) = resolve_forwarding_paths(
                &import.path.segments,
                &source.path,
                &import.inline_modules,
                &aliases,
                &mut BTreeSet::new(),
                0,
            ) else {
                continue;
            };
            for target in resolved {
                let Some(canonical) = rust_canonical_path_segments_in_scope(
                    &target.segments,
                    &source.path,
                    &target.inline_modules,
                ) else {
                    continue;
                };
                if is_catalog_identifier_path(&canonical) {
                    violations.insert(format!(
                        "catalog-identifier-forwarding-reexport: {}|{}|{}",
                        source.path,
                        import.visibility,
                        canonical.join("::")
                    ));
                }
            }
        }
    }
    violations
}

const EBD_4B1_OWNER: &str = "src/catalog/schema.rs";
const EBD_4B1_EXPECTED_VARIANTS: &[&str] = &[
    "TinyInt",
    "SmallInt",
    "Int",
    "BigInt",
    "LargeInt",
    "Float",
    "Double",
    "Decimal{precision:u8,scale:i8}",
    "String",
    "Json",
    "Binary",
    "Bitmap",
    "Hll",
    "Boolean",
    "Date",
    "DateTime",
    "DateTimeNs",
    "Time",
    "Array(Box<SqlType>)",
    "Map(Box<SqlType>,Box<SqlType>)",
    "Struct(Vec<(String,SqlType)>)",
    "Variant",
];

fn ebd_4b1_type_shape(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Path(path) if path.qself.is_none() => {
            let mut rendered = Vec::new();
            for segment in &path.path.segments {
                let arguments = match &segment.arguments {
                    syn::PathArguments::None => String::new(),
                    syn::PathArguments::AngleBracketed(arguments) => {
                        let arguments = arguments
                            .args
                            .iter()
                            .map(|argument| match argument {
                                syn::GenericArgument::Type(ty) => ebd_4b1_type_shape(ty),
                                _ => None,
                            })
                            .collect::<Option<Vec<_>>>()?;
                        format!("<{}>", arguments.join(","))
                    }
                    syn::PathArguments::Parenthesized(_) => return None,
                };
                rendered.push(format!("{}{arguments}", segment.ident));
            }
            Some(rendered.join("::"))
        }
        syn::Type::Tuple(tuple) => Some(format!(
            "({})",
            tuple
                .elems
                .iter()
                .map(ebd_4b1_type_shape)
                .collect::<Option<Vec<_>>>()?
                .join(",")
        )),
        syn::Type::Array(array) => {
            let syn::Expr::Lit(length) = &array.len else {
                return None;
            };
            let syn::Lit::Int(length) = &length.lit else {
                return None;
            };
            Some(format!(
                "[{};{}]",
                ebd_4b1_type_shape(&array.elem)?,
                length.base10_digits()
            ))
        }
        _ => None,
    }
}

fn ebd_4b1_variant_shape(variant: &syn::Variant) -> Option<String> {
    if variant.discriminant.is_some() {
        return None;
    }
    let name = variant.ident.to_string();
    match &variant.fields {
        syn::Fields::Unit => Some(name),
        syn::Fields::Unnamed(fields) => Some(format!(
            "{name}({})",
            fields
                .unnamed
                .iter()
                .map(|field| ebd_4b1_type_shape(&field.ty))
                .collect::<Option<Vec<_>>>()?
                .join(",")
        )),
        syn::Fields::Named(fields) => Some(format!(
            "{name}{{{}}}",
            fields
                .named
                .iter()
                .map(|field| {
                    Some(format!(
                        "{}:{}",
                        field.ident.as_ref()?,
                        ebd_4b1_type_shape(&field.ty)?
                    ))
                })
                .collect::<Option<Vec<_>>>()?
                .join(",")
        )),
    }
}

fn ebd_4b1_derive_set(item: &syn::ItemEnum) -> Option<BTreeSet<String>> {
    let mut derives = BTreeSet::new();
    for attribute in item
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("derive"))
    {
        let paths = attribute
            .parse_args_with(
                syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated,
            )
            .ok()?;
        derives.extend(paths.into_iter().map(|path| {
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::")
        }));
    }
    Some(derives)
}

fn ebd_4b1_attribute_is(attribute: &syn::Attribute, name: &str) -> bool {
    attribute.path().is_ident(name)
}

fn ebd_4b1_is_pub(visibility: &syn::Visibility) -> bool {
    matches!(visibility, syn::Visibility::Public(_))
}

fn ebd_4b1_is_pub_crate(visibility: &syn::Visibility) -> bool {
    matches!(
        visibility,
        syn::Visibility::Restricted(restricted)
            if restricted.in_token.is_none() && restricted.path.is_ident("crate")
    )
}

fn ebd_4b1_audit_schema_dependencies(source: &GuardSource) -> BTreeSet<String> {
    struct CratePathAudit<'a> {
        source: &'a str,
        violations: BTreeSet<String>,
    }

    impl<'ast> syn::visit::Visit<'ast> for CratePathAudit<'_> {
        fn visit_path(&mut self, path: &'ast syn::Path) {
            let segments = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            if segments.len() > 1
                && segments
                    .first()
                    .is_some_and(|root| matches!(root.as_str(), "crate" | "self" | "super"))
            {
                self.violations.insert(format!(
                    "catalog-schema-forbidden-local-path: {}|{}",
                    self.source,
                    segments.join("::")
                ));
            }
            syn::visit::visit_path(self, path);
        }
    }

    let mut violations = ebd_4a_audit_catalog_dependencies(source);
    if let Ok(file) = syn::parse_file(&source.text) {
        let has_column_data_type_exception = file.items.iter().any(|item| {
            let syn::Item::Struct(item) = item else {
                return false;
            };
            item.ident == "ColumnDef"
                && item.fields.iter().any(|field| {
                    field
                        .ident
                        .as_ref()
                        .is_some_and(|ident| ident == "data_type")
                        && ebd_4b2b_path_segments(&field.ty)
                            .is_some_and(|(path, _)| path == ["arrow", "datatypes", "DataType"])
                })
        });
        if has_column_data_type_exception {
            violations.remove(&format!(
                "catalog-forbidden-external-path: {}|arrow::datatypes::DataType",
                source.path
            ));
        }
        let mut audit = CratePathAudit {
            source: &source.path,
            violations: BTreeSet::new(),
        };
        syn::visit::Visit::visit_file(&mut audit, &file);
        violations.extend(audit.violations);
    }
    violations.extend(ebd_4b2b_audit_schema_dependencies(source));
    violations
}

fn ebd_4b1_audit_schema_owner(source: &GuardSource) -> BTreeSet<String> {
    struct SqlTypeEnumCounter {
        count: usize,
    }

    impl<'ast> syn::visit::Visit<'ast> for SqlTypeEnumCounter {
        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            if item.ident == "SqlType" {
                self.count += 1;
            }
            syn::visit::visit_item_enum(self, item);
        }
    }

    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "catalog-schema-owner-parse-failed: {}",
            source.path
        )]);
    };
    let mut violations = ebd_4b1_audit_schema_dependencies(source);
    violations.remove(&format!(
        "catalog-schema-forbidden-local-path: {}|crate",
        source.path
    ));
    let mut counter = SqlTypeEnumCounter { count: 0 };
    syn::visit::Visit::visit_file(&mut counter, &file);
    if counter.count != 1 {
        violations.insert(format!(
            "catalog-schema-owner-enum-count: expected=1 actual={}",
            counter.count
        ));
    }

    let canonical = file.items.iter().find_map(|item| match item {
        syn::Item::Enum(item) if item.ident == "SqlType" => Some(item),
        _ => None,
    });
    let Some(canonical) = canonical else {
        violations.insert("catalog-schema-owner-enum-missing: SqlType".to_string());
        return violations;
    };
    if !ebd_4b1_is_pub(&canonical.vis) {
        violations.insert("catalog-schema-owner-visibility: SqlType must be pub".to_string());
    }
    if !canonical.generics.params.is_empty() || canonical.generics.where_clause.is_some() {
        violations.insert("catalog-schema-owner-generics-forbidden: SqlType".to_string());
    }
    for attribute in &canonical.attrs {
        if !ebd_4b1_attribute_is(attribute, "derive") && !ebd_4b1_attribute_is(attribute, "doc") {
            violations.insert(format!(
                "catalog-schema-owner-semantic-attribute-forbidden: SqlType|{}",
                attribute
                    .path()
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect::<Vec<_>>()
                    .join("::")
            ));
        }
    }
    for variant in &canonical.variants {
        for attribute in &variant.attrs {
            if !ebd_4b1_attribute_is(attribute, "doc") {
                violations.insert(format!(
                    "catalog-schema-variant-semantic-attribute-forbidden: {}|{}",
                    variant.ident,
                    attribute
                        .path()
                        .segments
                        .iter()
                        .map(|segment| segment.ident.to_string())
                        .collect::<Vec<_>>()
                        .join("::")
                ));
            }
        }
        for field in &variant.fields {
            for attribute in &field.attrs {
                if !ebd_4b1_attribute_is(attribute, "doc") {
                    violations.insert(format!(
                        "catalog-schema-field-semantic-attribute-forbidden: {}|{}",
                        variant.ident,
                        attribute
                            .path()
                            .segments
                            .iter()
                            .map(|segment| segment.ident.to_string())
                            .collect::<Vec<_>>()
                            .join("::")
                    ));
                }
            }
        }
    }

    let expected_derives = BTreeSet::from([
        "Clone".to_string(),
        "Debug".to_string(),
        "Eq".to_string(),
        "PartialEq".to_string(),
    ]);
    match ebd_4b1_derive_set(canonical) {
        Some(actual) if actual == expected_derives => {}
        Some(actual) => {
            violations.insert(format!(
                "catalog-schema-owner-derives: expected={expected_derives:?} actual={actual:?}"
            ));
        }
        None => {
            violations.insert("catalog-schema-owner-derives-parse-failed: SqlType".to_string());
        }
    }

    let actual_variants = canonical
        .variants
        .iter()
        .map(ebd_4b1_variant_shape)
        .collect::<Option<Vec<_>>>();
    let expected_variants = EBD_4B1_EXPECTED_VARIANTS
        .iter()
        .map(|variant| (*variant).to_string())
        .collect::<Vec<_>>();
    match actual_variants {
        Some(actual) if actual == expected_variants => {}
        Some(actual) => {
            violations.insert(format!(
                "catalog-schema-owner-variants: expected={expected_variants:?} actual={actual:?}"
            ));
        }
        None => {
            violations
                .insert("catalog-schema-owner-variant-shape-unsupported: SqlType".to_string());
        }
    }
    violations
}

fn ebd_4b1_audit_schema_module(source: &GuardSource) -> BTreeSet<String> {
    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "catalog-schema-module-parse-failed: {}",
            source.path
        )]);
    };
    let modules = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Mod(item) if item.ident == "schema" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut violations = BTreeSet::new();
    if modules.len() != 1 {
        violations.insert(format!(
            "catalog-schema-module-count: expected=1 actual={}",
            modules.len()
        ));
        return violations;
    }
    if modules[0].content.is_some() {
        violations.insert("catalog-schema-module-must-be-file-backed".to_string());
    }
    if !ebd_4b1_is_pub_crate(&modules[0].vis) {
        violations.insert("catalog-schema-module-visibility: expected pub(crate)".to_string());
    }
    if !modules[0].attrs.is_empty() {
        violations.insert("catalog-schema-module-attributes-forbidden".to_string());
    }
    violations
}

fn is_legacy_ebd_4b1_sql_type_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments.starts_with(&["crate", "sql", "SqlType"])
        || segments.starts_with(&["crate", "sql", "parser", "ast", "SqlType"])
}

fn is_catalog_schema_sql_type_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments.starts_with(&["crate", "catalog", "schema", "SqlType"])
}

fn is_exact_catalog_schema_sql_type_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments == ["crate", "catalog", "schema", "SqlType"]
}

fn is_catalog_schema_module_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments == ["crate", "catalog", "schema"]
}

fn is_catalog_schema_glob_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments == ["crate", "catalog", "schema", "*"]
}

fn is_catalog_schema_sql_type_forward_target(path: &[String]) -> bool {
    is_catalog_schema_module_path(path)
        || is_catalog_schema_glob_path(path)
        || is_catalog_schema_sql_type_path(path)
}

#[derive(Clone, Debug)]
struct Ebd4b1ModuleUseStatement {
    visibility: String,
    segments: Vec<String>,
    alias: Option<String>,
    inline_modules: Vec<String>,
}

fn ebd_4b1_visibility(visibility: &syn::Visibility) -> String {
    match visibility {
        syn::Visibility::Inherited => "private".to_string(),
        syn::Visibility::Public(_) => "pub".to_string(),
        syn::Visibility::Restricted(restricted) => {
            let path = restricted
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::");
            if path == "self" {
                "private".to_string()
            } else if restricted.in_token.is_some() {
                format!("pub(in {path})")
            } else {
                format!("pub({path})")
            }
        }
    }
}

fn ebd_4b1_flatten_use_tree(
    tree: &syn::UseTree,
    prefix: &[String],
    output: &mut Vec<(Vec<String>, Option<String>)>,
) {
    match tree {
        syn::UseTree::Path(path) => {
            let mut prefix = prefix.to_vec();
            prefix.push(path.ident.to_string());
            ebd_4b1_flatten_use_tree(&path.tree, &prefix, output);
        }
        syn::UseTree::Name(name) => {
            let mut segments = prefix.to_vec();
            if name.ident != "self" {
                segments.push(name.ident.to_string());
            }
            output.push((segments, None));
        }
        syn::UseTree::Rename(rename) => {
            let mut segments = prefix.to_vec();
            if rename.ident != "self" {
                segments.push(rename.ident.to_string());
            }
            output.push((segments, Some(rename.rename.to_string())));
        }
        syn::UseTree::Glob(_) => {
            let mut segments = prefix.to_vec();
            segments.push("*".to_string());
            output.push((segments, None));
        }
        syn::UseTree::Group(group) => {
            for item in &group.items {
                ebd_4b1_flatten_use_tree(item, prefix, output);
            }
        }
    }
}

fn ebd_4b1_collect_module_scope_inputs(
    items: &[syn::Item],
    inline_modules: &mut Vec<String>,
    imports: &mut Vec<Ebd4b1ModuleUseStatement>,
) {
    for item in items {
        match item {
            syn::Item::Use(item) => {
                let mut flattened = Vec::new();
                ebd_4b1_flatten_use_tree(&item.tree, &[], &mut flattened);
                imports.extend(flattened.into_iter().map(|(segments, alias)| {
                    Ebd4b1ModuleUseStatement {
                        visibility: ebd_4b1_visibility(&item.vis),
                        segments,
                        alias,
                        inline_modules: inline_modules.clone(),
                    }
                }));
            }
            syn::Item::Mod(item) => {
                let Some((_, nested)) = &item.content else {
                    continue;
                };
                inline_modules.push(item.ident.to_string());
                ebd_4b1_collect_module_scope_inputs(nested, inline_modules, imports);
                inline_modules.pop();
            }
            _ => {}
        }
    }
}

fn ebd_4b1_module_scope_inputs(
    file: &syn::File,
) -> (Vec<Ebd4b1ModuleUseStatement>, RustScopedAliases) {
    let mut imports = Vec::new();
    ebd_4b1_collect_module_scope_inputs(&file.items, &mut Vec::new(), &mut imports);

    let mut aliases = RustScopedAliases::new();
    for import in &imports {
        let local_name = match import.alias.as_deref() {
            Some("_") => None,
            Some(alias) => Some(alias.to_string()),
            None => import
                .segments
                .last()
                .filter(|leaf| !matches!(leaf.as_str(), "*" | "crate" | "self" | "super"))
                .cloned(),
        };
        let Some(local_name) = local_name else {
            continue;
        };
        let target = RustScopedUsePath {
            segments: import.segments.clone(),
            inline_modules: import.inline_modules.clone(),
        };
        let targets = aliases
            .entry((import.inline_modules.clone(), local_name))
            .or_default();
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    (imports, aliases)
}

fn ebd_4b1_audit_extern_self_aliases(source: &GuardSource, file: &syn::File) -> BTreeSet<String> {
    struct ExternSelfAliasAudit<'a> {
        source_path: &'a str,
        violations: BTreeSet<String>,
    }

    impl<'ast> syn::visit::Visit<'ast> for ExternSelfAliasAudit<'_> {
        fn visit_item_extern_crate(&mut self, item: &'ast syn::ItemExternCrate) {
            if item.ident == "self"
                && let Some((_, rename)) = &item.rename
            {
                self.violations.insert(format!(
                    "catalog-schema-extern-self-alias-forbidden: {}|{}",
                    self.source_path, rename
                ));
            }
        }
    }

    let mut audit = ExternSelfAliasAudit {
        source_path: &source.path,
        violations: BTreeSet::new(),
    };
    syn::visit::Visit::visit_file(&mut audit, file);
    audit.violations
}

fn ebd_4b1_canonical_schema_glob_scopes(
    source: &GuardSource,
    imports: &[Ebd4b1ModuleUseStatement],
    aliases: &RustScopedAliases,
) -> BTreeSet<Vec<String>> {
    let mut scopes = BTreeSet::new();
    for import in imports {
        let Some(resolved) = resolve_forwarding_paths(
            &import.segments,
            &source.path,
            &import.inline_modules,
            aliases,
            &mut BTreeSet::new(),
            0,
        ) else {
            continue;
        };
        if resolved.into_iter().any(|target| {
            rust_canonical_path_segments_in_scope(
                &target.segments,
                &source.path,
                &target.inline_modules,
            )
            .is_some_and(|canonical| is_catalog_schema_glob_path(&canonical))
        }) {
            scopes.insert(import.inline_modules.clone());
        }
    }
    scopes
}

fn ebd_4b1_direct_alias_rhs_path(ty: &syn::Type) -> Option<Vec<String>> {
    match ty {
        syn::Type::Paren(paren) => ebd_4b1_direct_alias_rhs_path(&paren.elem),
        syn::Type::Group(group) => ebd_4b1_direct_alias_rhs_path(&group.elem),
        syn::Type::Path(path)
            if path.qself.is_none()
                && path
                    .path
                    .segments
                    .iter()
                    .all(|segment| matches!(segment.arguments, syn::PathArguments::None)) =>
        {
            Some(
                path.path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect(),
            )
        }
        _ => None,
    }
}

fn ebd_4b1_macro_tokens(item: &syn::ItemMacro) -> Vec<String> {
    rust_source_tokens(&item.mac.tokens.to_string())
        .into_iter()
        .map(|token| token.text)
        .collect()
}

fn ebd_4b1_matching_macro_group(tokens: &[String], open: usize) -> Option<usize> {
    fn close_for(token: &str) -> Option<&'static str> {
        match token {
            "(" => Some(")"),
            "[" => Some("]"),
            "{" => Some("}"),
            _ => None,
        }
    }

    let mut expected = vec![close_for(tokens.get(open)?)?];
    for (index, token) in tokens.iter().enumerate().skip(open + 1) {
        if let Some(close) = close_for(token) {
            expected.push(close);
        } else if expected.last().is_some_and(|close| token == close) {
            expected.pop();
            if expected.is_empty() {
                return Some(index);
            }
        } else if matches!(token.as_str(), ")" | "]" | "}") {
            return None;
        }
    }
    None
}

fn ebd_4b1_macro_rule_transcribers(item: &syn::ItemMacro) -> Vec<Vec<String>> {
    if !item.mac.path.is_ident("macro_rules") || item.ident.is_none() {
        return Vec::new();
    }
    let tokens = ebd_4b1_macro_tokens(item);
    let mut transcribers = Vec::new();
    let mut index = 0usize;
    while index < tokens.len() {
        if matches!(tokens[index].as_str(), "(" | "[" | "{") {
            let Some(close) = ebd_4b1_matching_macro_group(&tokens, index) else {
                break;
            };
            index = close + 1;
            continue;
        }
        if tokens.get(index).is_some_and(|token| token == "=")
            && tokens.get(index + 1).is_some_and(|token| token == ">")
            && tokens
                .get(index + 2)
                .is_some_and(|token| matches!(token.as_str(), "(" | "[" | "{"))
        {
            let open = index + 2;
            let Some(close) = ebd_4b1_matching_macro_group(&tokens, open) else {
                break;
            };
            transcribers.push(tokens[open + 1..close].to_vec());
            index = close + 1;
            continue;
        }
        index += 1;
    }
    transcribers
}

fn ebd_4b1_macro_generated_definition(tokens: &[String]) -> Option<&str> {
    const TYPE_NAMESPACE_ITEMS: &[&str] = &["enum", "struct", "union", "trait", "type", "mod"];
    tokens.windows(2).find_map(|pair| {
        (TYPE_NAMESPACE_ITEMS.contains(&pair[0].as_str()) && pair[1] == "SqlType")
            .then_some(pair[0].as_str())
    })
}

fn ebd_4b1_macro_generated_direct_aliases(tokens: &[String]) -> Vec<(String, Vec<String>)> {
    let mut aliases = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if token != "type" {
            continue;
        }
        let end = tokens[index + 1..]
            .iter()
            .position(|token| token == ";")
            .map_or(tokens.len(), |offset| index + 1 + offset);
        let alias = &tokens[index + 1..end];
        let Some(equals) = alias.iter().position(|token| token == "=") else {
            continue;
        };
        let Some(name) = alias.first() else {
            continue;
        };
        let mut rhs_tokens = Vec::new();
        let mut rhs_index = equals + 1;
        while rhs_index < alias.len() {
            if alias.get(rhs_index).is_some_and(|token| token == "$")
                && alias
                    .get(rhs_index + 1)
                    .is_some_and(|token| token == "crate")
            {
                rhs_tokens.push("crate".to_string());
                rhs_index += 2;
            } else {
                rhs_tokens.push(alias[rhs_index].clone());
                rhs_index += 1;
            }
        }
        let rhs = rhs_tokens.join(" ");
        let Ok(rhs) = syn::parse_str::<syn::Type>(&rhs) else {
            continue;
        };
        if let Some(path) = ebd_4b1_direct_alias_rhs_path(&rhs) {
            aliases.push((name.clone(), path));
        }
    }
    aliases
}

fn ebd_4b1_audit_definitions(sources: &[GuardSource]) -> BTreeSet<String> {
    struct SqlTypeDefinitionAudit<'a> {
        source_path: &'a str,
        aliases: &'a RustScopedAliases,
        canonical_glob_scopes: &'a BTreeSet<Vec<String>>,
        inline_modules: Vec<String>,
        allow_canonical_enum: bool,
        canonical_enum_count: usize,
        violations: BTreeSet<String>,
    }

    impl SqlTypeDefinitionAudit<'_> {
        fn audit_direct_alias_target(&mut self, name: &str, path: &[String], kind: &str) {
            let resolved = rust_resolve_scoped_paths(
                path,
                &self.inline_modules,
                self.aliases,
                &mut BTreeSet::new(),
                0,
            )
            .unwrap_or_else(|| {
                vec![RustScopedUsePath {
                    segments: path.to_vec(),
                    inline_modules: self.inline_modules.clone(),
                }]
            });
            for target in resolved {
                let Some(canonical) = rust_canonical_path_segments_in_scope(
                    &target.segments,
                    self.source_path,
                    &target.inline_modules,
                ) else {
                    continue;
                };
                if is_catalog_schema_sql_type_path(&canonical)
                    || is_legacy_ebd_4b1_sql_type_path(&canonical)
                {
                    self.violations.insert(format!(
                        "{kind}: {}|{}|{}",
                        self.source_path,
                        name,
                        canonical.join("::")
                    ));
                }
            }
            if path == ["SqlType"] && self.canonical_glob_scopes.contains(&self.inline_modules) {
                self.violations.insert(format!(
                    "{kind}: {}|{}|crate::catalog::schema::SqlType",
                    self.source_path, name
                ));
            }
        }
    }

    impl<'ast> syn::visit::Visit<'ast> for SqlTypeDefinitionAudit<'_> {
        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            if item.ident == "SqlType" {
                if self.allow_canonical_enum {
                    self.canonical_enum_count += 1;
                } else {
                    self.violations.insert(format!(
                        "catalog-schema-secondary-enum: {}|SqlType",
                        self.source_path
                    ));
                }
            }
        }

        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            if item.ident == "SqlType" {
                self.violations.insert(format!(
                    "catalog-schema-secondary-struct: {}|SqlType",
                    self.source_path
                ));
            }
        }

        fn visit_item_union(&mut self, item: &'ast syn::ItemUnion) {
            if item.ident == "SqlType" {
                self.violations.insert(format!(
                    "catalog-schema-secondary-union: {}|SqlType",
                    self.source_path
                ));
            }
        }

        fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
            if item.ident == "SqlType" {
                self.violations.insert(format!(
                    "catalog-schema-secondary-trait: {}|SqlType",
                    self.source_path
                ));
            }
        }

        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            if item.ident == "SqlType" {
                self.violations.insert(format!(
                    "catalog-schema-type-alias: {}|SqlType",
                    self.source_path
                ));
            }

            if let Some(path) = ebd_4b1_direct_alias_rhs_path(&item.ty) {
                self.audit_direct_alias_target(
                    &item.ident.to_string(),
                    &path,
                    "catalog-schema-type-alias-target",
                );
            }
        }

        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            for transcriber in ebd_4b1_macro_rule_transcribers(item) {
                if let Some(kind) = ebd_4b1_macro_generated_definition(&transcriber) {
                    self.violations.insert(format!(
                        "catalog-schema-macro-generated-definition: {}|{kind}|SqlType",
                        self.source_path
                    ));
                }
                for (name, path) in ebd_4b1_macro_generated_direct_aliases(&transcriber) {
                    self.audit_direct_alias_target(
                        &name,
                        &path,
                        "catalog-schema-macro-generated-alias",
                    );
                }
            }
        }

        fn visit_item_fn(&mut self, _item: &'ast syn::ItemFn) {}

        fn visit_item_impl(&mut self, _item: &'ast syn::ItemImpl) {}

        fn visit_item_const(&mut self, _item: &'ast syn::ItemConst) {}

        fn visit_item_static(&mut self, _item: &'ast syn::ItemStatic) {}

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if item.ident == "SqlType" {
                self.violations.insert(format!(
                    "catalog-schema-secondary-module: {}|SqlType",
                    self.source_path
                ));
            }
            let Some((_, items)) = &item.content else {
                return;
            };
            self.inline_modules.push(item.ident.to_string());
            for item in items {
                syn::visit::Visit::visit_item(self, item);
            }
            self.inline_modules.pop();
        }
    }

    let mut violations = BTreeSet::new();
    for source in sources {
        let Ok(file) = syn::parse_file(&source.text) else {
            violations.insert(format!(
                "catalog-schema-definition-parse-failed: {}",
                source.path
            ));
            continue;
        };
        let (imports, aliases) = ebd_4b1_module_scope_inputs(&file);
        let canonical_glob_scopes =
            ebd_4b1_canonical_schema_glob_scopes(source, &imports, &aliases);
        let mut audit = SqlTypeDefinitionAudit {
            source_path: &source.path,
            aliases: &aliases,
            canonical_glob_scopes: &canonical_glob_scopes,
            inline_modules: Vec::new(),
            allow_canonical_enum: source.path == EBD_4B1_OWNER,
            canonical_enum_count: 0,
            violations: ebd_4b1_audit_extern_self_aliases(source, &file),
        };
        syn::visit::Visit::visit_file(&mut audit, &file);
        if audit.allow_canonical_enum && audit.canonical_enum_count != 1 {
            audit.violations.insert(format!(
                "catalog-schema-canonical-enum-count: {}|expected=1 actual={}",
                source.path, audit.canonical_enum_count
            ));
        }
        violations.extend(audit.violations);
    }
    violations
}

fn ebd_4b1_audit_legacy_paths_and_forwarding(sources: &[GuardSource]) -> BTreeSet<String> {
    let mut violations = BTreeSet::new();
    for source in sources {
        let legacy_paths = rust_all_source_canonical_paths(&source.text, &source.path)
            .into_iter()
            .filter(|path| is_legacy_ebd_4b1_sql_type_path(path))
            .collect::<BTreeSet<_>>();
        for path in remove_redundant_descendant_paths(legacy_paths) {
            violations.insert(format!(
                "catalog-schema-legacy-path: {}|{}",
                source.path,
                path.join("::")
            ));
        }

        if source.path == EBD_4B1_OWNER {
            continue;
        }
        let Ok(file) = syn::parse_file(&source.text) else {
            violations.insert(format!(
                "catalog-schema-forwarding-parse-failed: {}",
                source.path
            ));
            continue;
        };
        let (imports, aliases) = ebd_4b1_module_scope_inputs(&file);
        for import in imports {
            let Some(resolved) = resolve_forwarding_paths(
                &import.segments,
                &source.path,
                &import.inline_modules,
                &aliases,
                &mut BTreeSet::new(),
                0,
            ) else {
                continue;
            };
            for target in resolved {
                let Some(canonical) = rust_canonical_path_segments_in_scope(
                    &target.segments,
                    &source.path,
                    &target.inline_modules,
                ) else {
                    continue;
                };
                if import.visibility == "private" && is_catalog_schema_glob_path(&canonical) {
                    violations.insert(format!(
                        "catalog-schema-private-glob-import: {}|{}",
                        source.path,
                        canonical.join("::")
                    ));
                } else if import.visibility != "private"
                    && is_catalog_schema_sql_type_forward_target(&canonical)
                {
                    violations.insert(format!(
                        "catalog-schema-forwarding-reexport: {}|{}|{}",
                        source.path,
                        import.visibility,
                        canonical.join("::")
                    ));
                }
            }
        }
    }
    violations
}

fn ebd_4b1_option_inner_path(ty: &syn::Type) -> Option<(Vec<String>, bool)> {
    let syn::Type::Path(option) = ty else {
        return None;
    };
    if option.qself.is_some() {
        return None;
    }
    let outer_segments = option
        .path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    let plain_prelude = option.path.leading_colon.is_none() && outer_segments == ["Option"];
    if !plain_prelude
        && outer_segments != ["std", "option", "Option"]
        && outer_segments != ["core", "option", "Option"]
    {
        return None;
    }
    if option
        .path
        .segments
        .iter()
        .take(option.path.segments.len().saturating_sub(1))
        .any(|segment| !matches!(segment.arguments, syn::PathArguments::None))
    {
        return None;
    }
    let option_segment = option.path.segments.last()?;
    let syn::PathArguments::AngleBracketed(arguments) = &option_segment.arguments else {
        return None;
    };
    if arguments.args.len() != 1 {
        return None;
    }
    let syn::GenericArgument::Type(syn::Type::Path(inner)) = arguments.args.first()? else {
        return None;
    };
    if inner.qself.is_some()
        || inner
            .path
            .segments
            .iter()
            .any(|segment| !matches!(segment.arguments, syn::PathArguments::None))
    {
        return None;
    }
    Some((
        inner
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect(),
        plain_prelude,
    ))
}

fn ebd_4b1_file_defines_root_type_name(file: &syn::File, name: &str) -> bool {
    file.items.iter().any(|item| match item {
        syn::Item::Enum(item) => item.ident == name,
        syn::Item::Mod(item) => item.ident == name,
        syn::Item::Struct(item) => item.ident == name,
        syn::Item::Trait(item) => item.ident == name,
        syn::Item::Type(item) => item.ident == name,
        syn::Item::Union(item) => item.ident == name,
        _ => false,
    })
}

fn ebd_4b1_audit_column_def(source: &GuardSource) -> BTreeSet<String> {
    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "catalog-schema-column-def-parse-failed: {}",
            source.path
        )]);
    };
    let Some(column_def) = file.items.iter().find_map(|item| match item {
        syn::Item::Struct(item) if item.ident == "ColumnDef" => Some(item),
        _ => None,
    }) else {
        return BTreeSet::from(["catalog-schema-column-def-missing".to_string()]);
    };
    let Some(field) = column_def.fields.iter().find(|field| {
        field
            .ident
            .as_ref()
            .is_some_and(|ident| ident == "logical_type")
    }) else {
        return BTreeSet::from(["catalog-schema-column-def-logical-type-missing".to_string()]);
    };
    let Some((inner_path, plain_prelude)) = ebd_4b1_option_inner_path(&field.ty) else {
        return BTreeSet::from([
            "catalog-schema-column-def-logical-type-must-be-Option-SqlType".to_string(),
        ]);
    };

    let (_, aliases) = ebd_4b1_module_scope_inputs(&file);
    if plain_prelude
        && (aliases.contains_key(&(Vec::new(), "Option".to_string()))
            || ebd_4b1_file_defines_root_type_name(&file, "Option"))
    {
        return BTreeSet::from([
            "catalog-schema-column-def-logical-type-must-use-prelude-Option".to_string(),
        ]);
    }
    let Some(resolved) =
        rust_resolve_scoped_paths(&inner_path, &[], &aliases, &mut BTreeSet::new(), 0)
    else {
        return BTreeSet::from(
            ["catalog-schema-column-def-logical-type-not-canonical".to_string()],
        );
    };
    let canonical = resolved
        .into_iter()
        .map(|path| {
            rust_canonical_path_segments_in_scope(
                &path.segments,
                &source.path,
                &path.inline_modules,
            )
        })
        .collect::<Option<Vec<_>>>();
    if canonical.is_some_and(|paths| {
        !paths.is_empty()
            && paths
                .iter()
                .all(|path| is_exact_catalog_schema_sql_type_path(path))
    }) {
        BTreeSet::new()
    } else {
        BTreeSet::from(["catalog-schema-column-def-logical-type-not-canonical".to_string()])
    }
}

fn ebd_4b1_collect_repo_sources() -> Vec<GuardSource> {
    let repo = Path::new(manifest_dir());
    let src = src_dir();
    let mut sources = Vec::new();
    for root in [&src, &repo.join("tests")] {
        for path in rs_files(root) {
            let source = rel(&path);
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {source}: {error}"));
            sources.push(GuardSource::new(&source, &text));
        }
    }
    sources
}

#[test]
fn ebd_4b1_catalog_logical_type_boundary_is_complete() {
    let sources = ebd_4b1_collect_repo_sources();
    let mut violations = BTreeSet::new();

    if let Some(owner) = sources.iter().find(|source| source.path == EBD_4B1_OWNER) {
        violations.extend(ebd_4b1_audit_schema_owner(owner));
    } else {
        violations.insert(format!("catalog-schema-owner-missing: {EBD_4B1_OWNER}"));
    }
    if let Some(catalog_mod) = sources
        .iter()
        .find(|source| source.path == "src/catalog/mod.rs")
    {
        violations.extend(ebd_4b1_audit_schema_module(catalog_mod));
    } else {
        violations.insert("catalog-schema-module-owner-missing: src/catalog/mod.rs".to_string());
    }
    if let Some(identifier) = sources.iter().find(|source| source.path == EBD_4A_OWNER) {
        violations.extend(ebd_4a_audit_catalog_dependencies(identifier));
    } else {
        violations.insert(format!("catalog-identifier-owner-missing: {EBD_4A_OWNER}"));
    }
    if let Some(owner) = sources.iter().find(|source| source.path == EBD_4B1_OWNER) {
        violations.extend(ebd_4b1_audit_column_def(owner));
    } else {
        violations.insert(format!(
            "catalog-schema-column-def-owner-missing: {EBD_4B1_OWNER}"
        ));
    }
    violations.extend(ebd_4b1_audit_definitions(&sources));
    violations.extend(ebd_4b1_audit_legacy_paths_and_forwarding(&sources));

    assert!(
        violations.is_empty(),
        "EBD-4B1 catalog logical type boundary failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn ebd_4b1_detector_requires_exact_sql_type_shape_visibility_and_derives() {
    let valid_text = r#"
#[doc = "catalog type vocabulary"]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlType {
    #[doc = "tiny integer"]
    TinyInt, SmallInt, Int, BigInt, LargeInt, Float, Double,
    Decimal { #[doc = "precision"] precision: u8, scale: i8 },
    String, Json, Binary, Bitmap, Hll, Boolean, Date, DateTime, DateTimeNs, Time,
    Array(Box<SqlType>),
    Map(Box<SqlType>, Box<SqlType>),
    Struct(Vec<(String, SqlType)>),
    Variant,
}
"#;
    let valid = GuardSource::new(EBD_4B1_OWNER, valid_text);
    assert!(
        ebd_4b1_audit_schema_owner(&valid).is_empty(),
        "exact enum fixture must pass"
    );

    let invalid = [
        ("DateTimeNs,", ""),
        ("scale: i8", "scale: i16"),
        ("Map(Box<SqlType>, Box<SqlType>)", "Map(Box<SqlType>)"),
        ("Struct(Vec<(String, SqlType)>)", "Struct(Vec<SqlType>)"),
        ("Variant,", "Variant, Unknown,"),
        ("Variant,", "#[cfg(any())] Variant,"),
        ("precision: u8", "#[cfg(any())] precision: u8"),
        (
            "#[derive(Clone, Debug, PartialEq, Eq)]",
            "#[cfg(any())]\n#[derive(Clone, Debug, PartialEq, Eq)]",
        ),
        (
            "#[derive(Clone, Debug, PartialEq, Eq)]",
            "#[cfg_attr(test, repr(u8))]\n#[derive(Clone, Debug, PartialEq, Eq)]",
        ),
        ("TinyInt,", "TinyInt = 1,"),
        ("pub enum SqlType", "pub enum SqlType<T>"),
        ("pub enum SqlType", "pub(crate) enum SqlType"),
        (
            "#[derive(Clone, Debug, PartialEq, Eq)]",
            "#[derive(Clone, Debug, PartialEq)]",
        ),
    ];
    for (from, to) in invalid {
        let source = GuardSource::new(EBD_4B1_OWNER, &valid_text.replace(from, to));
        assert!(
            !ebd_4b1_audit_schema_owner(&source).is_empty(),
            "invalid exact enum fixture passed: {from:?} -> {to:?}"
        );
    }
}

#[test]
fn ebd_4b1_detector_seals_schema_dependencies_without_weakening_identifier() {
    let valid = GuardSource::new(
        EBD_4B1_OWNER,
        r###"
use std::fmt;
use core::cmp;
use alloc::string::String;
// use crate::engine::StandaloneState;
const TEXT: &str = "crate::sql::SqlType iceberg::spec::Literal";
const RAW: &str = r#"arrow::datatypes::DataType"#;
fn local(_: String) { let _ = (fmt::Error, cmp::Ordering::Equal); }
"###,
    );
    let valid_violations = ebd_4b1_audit_schema_dependencies(&valid);
    assert!(
        valid_violations.is_empty(),
        "valid std-only schema fixture was rejected: {valid_violations:?}"
    );

    for text in [
        "use crate::sql::SqlType;",
        "use crate::engine::StandaloneState;",
        "use iceberg::spec::Literal;",
        "fn bad(_: arrow::datatypes::DataType) {}",
    ] {
        let invalid = GuardSource::new(EBD_4B1_OWNER, text);
        assert!(
            !ebd_4b1_audit_schema_dependencies(&invalid).is_empty(),
            "forbidden schema dependency passed: {text}"
        );
    }

    let identifier_valid = GuardSource::new(
        EBD_4A_OWNER,
        "use crate::catalog::identifier::TableIdentity; fn local(_: TableIdentity) {}",
    );
    assert!(ebd_4a_audit_catalog_dependencies(&identifier_valid).is_empty());
    let identifier_invalid = GuardSource::new(
        EBD_4A_OWNER,
        "use crate::engine::StandaloneState; fn bad(_: StandaloneState) {}",
    );
    assert!(!ebd_4a_audit_catalog_dependencies(&identifier_invalid).is_empty());
}

#[test]
fn ebd_4b1_detector_rejects_legacy_definitions_paths_and_forwarding() {
    let allowed = [
        GuardSource::new(
            "src/runtime/query_result.rs",
            r###"
use crate::catalog::schema::SqlType;
// use crate::sql::SqlType;
const TEXT: &str = "crate::sql::parser::ast::SqlType";
const RAW: &str = r#"pub use crate::catalog::schema::SqlType"#;
struct SqlTypeInfo;
type SqlTypeFactory = fn() -> SqlType;
type Validator = fn(&SqlType) -> bool;
type SqlTypeCollection = Vec<SqlType>;
"###,
        ),
        GuardSource::new(
            "src/catalog/function_scope_noise.rs",
            r#"
use crate::catalog::schema::SqlType;
fn local_aliases() {
    use arrow::datatypes::DataType as SqlType;
    pub use crate::catalog::schema::SqlType as LocalForward;
    let _ = std::mem::size_of::<SqlType>();
}
fn local_option_alias() { use std::result::Result as Option; }
"#,
        ),
        GuardSource::new(
            "src/catalog/private_visibility.rs",
            r#"
pub(self) use crate::catalog::schema::SqlType as SelfType;
pub(in self) use crate::catalog::schema::SqlType as InSelfType;
"#,
        ),
        GuardSource::new(
            "src/catalog/macro_consumer.rs",
            r#"
use crate::catalog::schema::SqlType;
fn consume(value: SqlType) { let _ = matches!(value, SqlType::Int); }
macro_rules! consume_type { ($ty:ty) => { const _: usize = std::mem::size_of::<$ty>(); } }
consume_type!(Vec<SqlType>);
macro_rules! assert_impl { ($ty:ty) => {} }
assert_impl!(SqlType);
macro_rules! composite_alias { () => { type Collection = Vec<crate::catalog::schema::SqlType>; } }
composite_alias!();
macro_rules! matcher_only { (type CatalogType = crate::catalog::schema::SqlType;) => {} }
"#,
        ),
        GuardSource::new(
            EBD_4B1_OWNER,
            "#[derive(Clone, Debug, PartialEq, Eq)] pub enum SqlType { TinyInt }",
        ),
    ];
    assert!(ebd_4b1_audit_definitions(&allowed).is_empty());
    assert!(ebd_4b1_audit_legacy_paths_and_forwarding(&allowed).is_empty());

    let invalid = [
        GuardSource::new("src/sql/parser/ast/mod.rs", "pub enum SqlType { Int }"),
        GuardSource::new(
            "src/server/direct.rs",
            "use crate::sql::SqlType; fn direct(_: SqlType) {}",
        ),
        GuardSource::new(
            "src/server/grouped.rs",
            "use crate::sql::{SqlType, catalog::ColumnDef}; fn grouped(_: SqlType) {}",
        ),
        GuardSource::new(
            "src/server/alias.rs",
            "use crate::sql::parser::ast::SqlType as Legacy; fn alias(_: Legacy) {}",
        ),
        GuardSource::new(
            "src/server/nested.rs",
            "mod nested { use crate::sql::SqlType; type Local = SqlType; }",
        ),
        GuardSource::new(
            "src/server/test_only.rs",
            "#[cfg(test)] mod tests { use crate::sql::SqlType as Legacy; fn test(_: Legacy) {} }",
        ),
        GuardSource::new(
            "src/catalog/alias.rs",
            "pub(crate) type SqlType = crate::catalog::schema::SqlType;",
        ),
        GuardSource::new(
            "src/catalog/canonical_named_alias.rs",
            "pub(crate) type CatalogType = crate::catalog::schema::SqlType;",
        ),
        GuardSource::new(
            "src/catalog/canonical_short_alias.rs",
            "use crate::catalog::schema::SqlType; pub(crate) type CatalogType = SqlType;",
        ),
        GuardSource::new(
            "src/catalog/canonical_parenthesized_alias.rs",
            "use crate::catalog::schema::SqlType; pub(crate) type CatalogType = (SqlType);",
        ),
        GuardSource::new(
            "src/catalog/legacy_named_alias.rs",
            "use crate::sql::SqlType as OldType; type LegacyCatalogType = OldType;",
        ),
        GuardSource::new(
            "src/catalog/test_named_alias.rs",
            "#[cfg(test)] mod tests { use crate::catalog::schema::SqlType as Canonical; type TestCatalogType = Canonical; }",
        ),
        GuardSource::new(
            "src/catalog/glob_named_alias.rs",
            "use crate::catalog::schema::*; pub(crate) type CatalogType = SqlType;",
        ),
        GuardSource::new("src/catalog/struct_owner.rs", "struct SqlType;"),
        GuardSource::new("src/catalog/union_owner.rs", "union SqlType { value: u64 }"),
        GuardSource::new("src/catalog/trait_owner.rs", "trait SqlType {}"),
        GuardSource::new("src/catalog/module_owner.rs", "mod SqlType {}"),
        GuardSource::new(
            "src/catalog/forward.rs",
            r#"
pub use crate::catalog::schema::SqlType;
pub(crate) use crate::catalog::schema::{SqlType as CatalogType};
pub(in crate::catalog) use crate::catalog::schema::SqlType as InCatalogType;
mod nested { pub(super) use crate::catalog::schema::SqlType as NestedType; }
#[cfg(test)] mod tests { pub(crate) use crate::catalog::schema::SqlType as TestType; }
"#,
        ),
        GuardSource::new(
            "src/catalog/glob_forward.rs",
            r#"
pub(crate) use crate::catalog::schema::*;
mod nested { pub(super) use crate::catalog::schema::*; }
#[cfg(test)] mod tests { pub use crate::catalog::schema::*; }
"#,
        ),
        GuardSource::new(
            "src/catalog/module_forward.rs",
            r#"
pub(crate) use crate::catalog::schema;
pub use crate::catalog::schema as catalog_schema;
pub(super) use crate::catalog::{schema};
mod nested { pub(crate) use crate::catalog::schema; }
#[cfg(test)] mod tests { pub use crate::catalog::schema as test_schema; }
"#,
        ),
        GuardSource::new(
            "src/catalog/extern_self_private.rs",
            "extern crate self as nr;",
        ),
        GuardSource::new(
            "src/catalog/extern_self_pub_self.rs",
            "pub(self) extern crate self as nr;",
        ),
        GuardSource::new(
            "src/catalog/extern_self_pub_in_self.rs",
            "pub(in self) extern crate self as nr;",
        ),
        GuardSource::new(
            "src/catalog/extern_self_pub_crate.rs",
            "pub(crate) extern crate self as nr;",
        ),
        GuardSource::new(
            "src/catalog/extern_self_public.rs",
            "pub extern crate self as nr;",
        ),
        GuardSource::new(
            "src/catalog/extern_self_nested.rs",
            "mod nested { extern crate self as nr; pub(crate) use nr::catalog::schema::SqlType as NestedType; }",
        ),
        GuardSource::new(
            "src/catalog/extern_self_cfg.rs",
            "#[cfg(test)] mod tests { extern crate self as nr; }",
        ),
        GuardSource::new(
            "src/catalog/extern_self_function.rs",
            "fn local() { extern crate self as nr; let _: nr::sql::SqlType; }",
        ),
        GuardSource::new(
            "src/catalog/extern_self_block.rs",
            "fn local() { { extern crate self as nr; let _: nr::sql::SqlType; } }",
        ),
        GuardSource::new(
            "src/catalog/extern_self_impl.rs",
            "struct Holder; impl Holder { fn method() { extern crate self as nr; let _: nr::sql::SqlType; } }",
        ),
        GuardSource::new(
            "src/catalog/extern_self_const.rs",
            "const LOCAL: () = { extern crate self as nr; let _: Option<nr::sql::SqlType> = None; };",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_struct.rs",
            "macro_rules! shadow { () => { struct SqlType; } } shadow!();",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_enum.rs",
            "macro_rules! shadow { () => { enum SqlType { Int } } } shadow!();",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_union.rs",
            "macro_rules! shadow { () => { union SqlType { value: u64 } } } shadow!();",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_trait.rs",
            "macro_rules! shadow { () => { trait SqlType {} } } shadow!();",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_type.rs",
            "macro_rules! shadow { () => { type SqlType = u64; } } shadow!();",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_mod.rs",
            "macro_rules! shadow { () => { mod SqlType {} } } shadow!();",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_alias.rs",
            "macro_rules! aliases { () => { type CatalogType = crate::catalog::schema::SqlType; } } aliases!();",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_short_alias.rs",
            "use crate::catalog::schema::SqlType; macro_rules! aliases { () => { type CatalogType = SqlType; } } aliases!();",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_glob_alias.rs",
            "use crate::catalog::schema::*; macro_rules! aliases { () => { type CatalogType = SqlType; } } aliases!();",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_legacy_alias.rs",
            "use crate::sql::SqlType as Legacy; macro_rules! aliases { () => { type CatalogType = Legacy; } } aliases!();",
        ),
        GuardSource::new(
            "src/catalog/macro_generated_dollar_crate_alias.rs",
            r#"
macro_rules! aliases {
    (first) => { type CatalogType = $crate::catalog::schema::SqlType; };
    (second) => {{ type NestedCatalogType = ($crate::catalog::schema::SqlType); }};
}
"#,
        ),
    ];
    let definitions = ebd_4b1_audit_definitions(&invalid);
    assert!(definitions.iter().any(|item| {
        item == "catalog-schema-secondary-enum: src/sql/parser/ast/mod.rs|SqlType"
    }));
    assert!(
        definitions
            .iter()
            .any(|item| item == "catalog-schema-type-alias: src/catalog/alias.rs|SqlType")
    );
    assert!(definitions.iter().any(|item| {
        item.contains("src/catalog/canonical_named_alias.rs|CatalogType")
            && item.contains("crate::catalog::schema::SqlType")
    }));
    assert!(definitions.iter().any(|item| {
        item.contains("src/catalog/canonical_short_alias.rs|CatalogType")
            && item.contains("crate::catalog::schema::SqlType")
    }));
    assert!(definitions.iter().any(|item| {
        item.contains("src/catalog/canonical_parenthesized_alias.rs|CatalogType")
            && item.contains("crate::catalog::schema::SqlType")
    }));
    assert!(definitions.iter().any(|item| {
        item.contains("src/catalog/legacy_named_alias.rs|LegacyCatalogType")
            && item.contains("crate::sql::SqlType")
    }));
    assert!(definitions.iter().any(|item| {
        item.contains("src/catalog/test_named_alias.rs|TestCatalogType")
            && item.contains("crate::catalog::schema::SqlType")
    }));
    assert!(definitions.iter().any(|item| {
        item.contains("src/catalog/glob_named_alias.rs|CatalogType")
            && item.contains("crate::catalog::schema::SqlType")
    }));
    for (path, kind) in [
        ("struct_owner.rs", "struct"),
        ("union_owner.rs", "union"),
        ("trait_owner.rs", "trait"),
        ("module_owner.rs", "module"),
    ] {
        assert!(
            definitions.iter().any(|item| {
                item.contains(path) && item.contains(kind) && item.contains("SqlType")
            }),
            "secondary {kind} owner fixture was missed: {definitions:?}"
        );
    }

    let paths = ebd_4b1_audit_legacy_paths_and_forwarding(&invalid);
    for path in [
        "direct.rs",
        "grouped.rs",
        "alias.rs",
        "nested.rs",
        "test_only.rs",
    ] {
        assert!(
            paths
                .iter()
                .any(|item| item.contains(path) && item.starts_with("catalog-schema-legacy-path:")),
            "legacy path fixture was missed: {path}; got {paths:?}"
        );
    }
    for visibility in [
        "|pub|",
        "|pub(crate)|",
        "|pub(in crate::catalog)|",
        "|pub(super)|",
    ] {
        assert!(
            paths
                .iter()
                .any(|item| item.contains("forward") && item.contains(visibility)),
            "forwarding visibility fixture was missed: {visibility}; got {paths:?}"
        );
    }
    assert!(paths.iter().any(|item| {
        item.contains("src/catalog/forward.rs") && item.contains("catalog::schema::SqlType")
    }));
    for visibility in ["|pub|", "|pub(crate)|", "|pub(super)|"] {
        assert!(
            paths.iter().any(|item| {
                item.contains("src/catalog/glob_forward.rs")
                    && item.contains(visibility)
                    && item.contains("catalog::schema::*")
            }),
            "canonical schema glob fixture was missed: {visibility}; got {paths:?}"
        );
    }
    for visibility in ["|pub|", "|pub(crate)|", "|pub(super)|"] {
        assert!(
            paths.iter().any(|item| {
                item.contains("src/catalog/module_forward.rs")
                    && item.contains(visibility)
                    && item.ends_with("crate::catalog::schema")
            }),
            "canonical schema module forwarding fixture was missed: {visibility}; got {paths:?}"
        );
    }
    assert!(paths.iter().any(|item| {
        item.starts_with("catalog-schema-private-glob-import:")
            && item.contains("src/catalog/glob_named_alias.rs")
    }));
    for path in [
        "extern_self_private.rs",
        "extern_self_pub_self.rs",
        "extern_self_pub_in_self.rs",
        "extern_self_pub_crate.rs",
        "extern_self_public.rs",
        "extern_self_nested.rs",
        "extern_self_cfg.rs",
        "extern_self_function.rs",
        "extern_self_block.rs",
        "extern_self_impl.rs",
        "extern_self_const.rs",
    ] {
        assert!(
            definitions.iter().any(|item| item
                .starts_with("catalog-schema-extern-self-alias-forbidden:")
                && item.contains(path)),
            "extern-self declaration fixture was missed: {path}; got {definitions:?}"
        );
    }
    for kind in ["struct", "enum", "union", "trait", "type", "mod"] {
        assert!(definitions.iter().any(|item| {
            item.starts_with("catalog-schema-macro-generated-definition:")
                && item.contains(&format!("macro_generated_{kind}.rs"))
                && item.contains(&format!("|{kind}|SqlType"))
        }));
    }
    for path in [
        "macro_generated_alias.rs",
        "macro_generated_short_alias.rs",
        "macro_generated_glob_alias.rs",
        "macro_generated_legacy_alias.rs",
        "macro_generated_dollar_crate_alias.rs",
    ] {
        assert!(definitions.iter().any(|item| {
            item.starts_with("catalog-schema-macro-generated-alias:") && item.contains(path)
        }));
    }
    for name in ["CatalogType", "NestedCatalogType"] {
        assert!(definitions.iter().any(|item| {
            item.starts_with("catalog-schema-macro-generated-alias:")
                && item.contains("macro_generated_dollar_crate_alias.rs")
                && item.contains(name)
        }));
    }
}

#[test]
fn ebd_4b1_detector_requires_file_backed_module_and_canonical_column_field() {
    let module = GuardSource::new("src/catalog/mod.rs", "pub(crate) mod schema;");
    assert!(ebd_4b1_audit_schema_module(&module).is_empty());
    for invalid in [
        "mod schema;",
        "pub mod schema;",
        "pub(crate) mod schema {}",
        "#[path = \"other.rs\"] pub(crate) mod schema;",
        "#[cfg(test)] pub(crate) mod schema;",
        "#[cfg_attr(test, path = \"test.rs\")] pub(crate) mod schema;",
    ] {
        assert!(
            !ebd_4b1_audit_schema_module(&GuardSource::new("src/catalog/mod.rs", invalid,))
                .is_empty()
        );
    }

    let canonical = GuardSource::new(
        "src/sql/catalog.rs",
        "use crate::catalog::schema::SqlType; pub struct ColumnDef { pub logical_type: Option<SqlType> }",
    );
    assert!(ebd_4b1_audit_column_def(&canonical).is_empty());
    let canonical_with_function_scope_noise = GuardSource::new(
        "src/sql/catalog.rs",
        r#"
use crate::catalog::schema::SqlType;
pub struct ColumnDef { pub logical_type: Option<SqlType> }
fn local_aliases() {
    use arrow::datatypes::DataType as SqlType;
    use std::result::Result as Option;
}
"#,
    );
    assert!(
        ebd_4b1_audit_column_def(&canonical_with_function_scope_noise).is_empty(),
        "function-local aliases must not pollute root ColumnDef resolution"
    );
    let canonical_qualified = GuardSource::new(
        "src/sql/catalog.rs",
        "pub struct ColumnDef { pub logical_type: Option<crate::catalog::schema::SqlType> }",
    );
    assert!(ebd_4b1_audit_column_def(&canonical_qualified).is_empty());
    let canonical_std_option = GuardSource::new(
        "src/sql/catalog.rs",
        "pub struct ColumnDef { pub logical_type: std::option::Option<crate::catalog::schema::SqlType> }",
    );
    assert!(ebd_4b1_audit_column_def(&canonical_std_option).is_empty());
    let canonical_core_option = GuardSource::new(
        "src/sql/catalog.rs",
        "pub struct ColumnDef { pub logical_type: core::option::Option<crate::catalog::schema::SqlType> }",
    );
    assert!(ebd_4b1_audit_column_def(&canonical_core_option).is_empty());
    let legacy = GuardSource::new(
        "src/sql/catalog.rs",
        "pub struct ColumnDef { pub logical_type: Option<crate::sql::SqlType> }",
    );
    assert_eq!(
        ebd_4b1_audit_column_def(&legacy),
        BTreeSet::from(["catalog-schema-column-def-logical-type-not-canonical".to_string(),])
    );
    let custom_option = GuardSource::new(
        "src/sql/catalog.rs",
        "pub struct ColumnDef { pub logical_type: other::Option<crate::catalog::schema::SqlType> }",
    );
    assert!(
        ebd_4b1_audit_column_def(&custom_option)
            .iter()
            .any(|item| item.contains("Option"))
    );
    let descendant = GuardSource::new(
        "src/sql/catalog.rs",
        "pub struct ColumnDef { pub logical_type: Option<crate::catalog::schema::SqlType::Variant> }",
    );
    assert!(
        ebd_4b1_audit_column_def(&descendant)
            .contains("catalog-schema-column-def-logical-type-not-canonical")
    );
    let ambiguous = GuardSource::new(
        "src/sql/catalog.rs",
        r#"
use crate::catalog::schema::SqlType;
use crate::sql::SqlType;
pub struct ColumnDef { pub logical_type: Option<SqlType> }
"#,
    );
    assert!(
        ebd_4b1_audit_column_def(&ambiguous)
            .contains("catalog-schema-column-def-logical-type-not-canonical")
    );
    let imported_custom_option = GuardSource::new(
        "src/sql/catalog.rs",
        r#"
use other::Option;
use crate::catalog::schema::SqlType;
pub struct ColumnDef { pub logical_type: Option<SqlType> }
"#,
    );
    assert!(
        ebd_4b1_audit_column_def(&imported_custom_option)
            .iter()
            .any(|item| item.contains("Option"))
    );
    let local_custom_option = GuardSource::new(
        "src/sql/catalog.rs",
        r#"
struct Option<T>(T);
use crate::catalog::schema::SqlType;
pub struct ColumnDef { pub logical_type: Option<SqlType> }
"#,
    );
    assert!(
        ebd_4b1_audit_column_def(&local_custom_option)
            .iter()
            .any(|item| item.contains("Option"))
    );
    let qself_inner = GuardSource::new(
        "src/sql/catalog.rs",
        "pub struct ColumnDef { pub logical_type: Option<<Wrapper as Trait>::SqlType> }",
    );
    assert!(
        ebd_4b1_audit_column_def(&qself_inner)
            .iter()
            .any(|item| item.contains("Option"))
    );
}

const EBD_4B2A_OWNER: &str = "src/catalog/schema.rs";
const EBD_4B2A_EXPECTED_VARIANTS: &[&str] = &[
    "Null",
    "Boolean(bool)",
    "Int32(i32)",
    "Int64(i64)",
    "Float32{bits:u32}",
    "Float64{bits:u64}",
    "Decimal{unscaled:i128,precision:u8,scale:i8}",
    "String(String)",
    "Binary(Vec<u8>)",
    "Date{days_since_epoch:i32}",
    "TimeMicros{micros_since_midnight:i64}",
    "TimestampMicros{micros_since_epoch:i64}",
    "TimestamptzMicros{micros_since_epoch:i64}",
    "TimestampNanos{nanos_since_epoch:i64}",
    "TimestamptzNanos{nanos_since_epoch:i64}",
    "Uuid([u8;16])",
    "Fixed{size:u64,bytes:Vec<u8>}",
    "Struct(Vec<(String,ColumnDefault)>)",
    "Array(Vec<ColumnDefault>)",
    "Map(Vec<(ColumnDefault,ColumnDefault)>)",
];

fn is_exact_catalog_schema_column_default_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments == ["crate", "catalog", "schema", "ColumnDefault"]
}

fn is_catalog_schema_column_default_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments.starts_with(&["crate", "catalog", "schema", "ColumnDefault"])
}

fn is_iceberg_literal_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments.starts_with(&["iceberg", "spec", "Literal"])
}

fn is_connector_iceberg_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments.starts_with(&["crate", "connector", "iceberg"])
}

fn ebd_4b2a_resolve_type_path(
    path: &[String],
    source: &GuardSource,
    aliases: &RustScopedAliases,
    inline_modules: &[String],
) -> Vec<Vec<String>> {
    rust_resolve_scoped_paths(path, inline_modules, aliases, &mut BTreeSet::new(), 0)
        .unwrap_or_else(|| {
            vec![RustScopedUsePath {
                segments: path.to_vec(),
                inline_modules: inline_modules.to_vec(),
            }]
        })
        .into_iter()
        .filter_map(|resolved| {
            if is_iceberg_literal_path(&resolved.segments) {
                return Some(resolved.segments);
            }
            rust_canonical_path_segments_in_scope(
                &resolved.segments,
                &source.path,
                &resolved.inline_modules,
            )
        })
        .collect()
}

fn ebd_4b2a_audit_schema_owner(source: &GuardSource) -> BTreeSet<String> {
    struct ColumnDefaultCounter {
        enums: usize,
        validators: usize,
    }

    impl<'ast> syn::visit::Visit<'ast> for ColumnDefaultCounter {
        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            if item.ident == "ColumnDefault" {
                self.enums += 1;
            }
            syn::visit::visit_item_enum(self, item);
        }

        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if item.sig.ident == "validate_column_default" {
                self.validators += 1;
            }
            syn::visit::visit_item_fn(self, item);
        }
    }

    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "catalog-default-owner-parse-failed: {}",
            source.path
        )]);
    };
    let mut violations = ebd_4b1_audit_schema_dependencies(source);
    violations.remove(&format!(
        "catalog-schema-forbidden-local-path: {}|crate",
        source.path
    ));
    let mut counter = ColumnDefaultCounter {
        enums: 0,
        validators: 0,
    };
    syn::visit::Visit::visit_file(&mut counter, &file);
    if counter.enums != 1 {
        violations.insert(format!(
            "catalog-default-owner-enum-count: expected=1 actual={}",
            counter.enums
        ));
    }
    if counter.validators != 1 {
        violations.insert(format!(
            "catalog-default-validator-count: expected=1 actual={}",
            counter.validators
        ));
    }

    let canonical = file.items.iter().find_map(|item| match item {
        syn::Item::Enum(item) if item.ident == "ColumnDefault" => Some(item),
        _ => None,
    });
    let Some(canonical) = canonical else {
        violations.insert("catalog-default-owner-enum-missing: ColumnDefault".to_string());
        return violations;
    };
    if !ebd_4b1_is_pub(&canonical.vis) {
        violations
            .insert("catalog-default-owner-visibility: ColumnDefault must be pub".to_string());
    }
    if !canonical.generics.params.is_empty() || canonical.generics.where_clause.is_some() {
        violations.insert("catalog-default-owner-generics-forbidden: ColumnDefault".to_string());
    }
    for attribute in &canonical.attrs {
        if !ebd_4b1_attribute_is(attribute, "derive") && !ebd_4b1_attribute_is(attribute, "doc") {
            violations.insert(format!(
                "catalog-default-owner-semantic-attribute-forbidden: {}",
                attribute
                    .path()
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect::<Vec<_>>()
                    .join("::")
            ));
        }
    }
    for variant in &canonical.variants {
        for attribute in &variant.attrs {
            if !ebd_4b1_attribute_is(attribute, "doc") {
                violations.insert(format!(
                    "catalog-default-variant-semantic-attribute-forbidden: {}|{}",
                    variant.ident,
                    attribute
                        .path()
                        .segments
                        .iter()
                        .map(|segment| segment.ident.to_string())
                        .collect::<Vec<_>>()
                        .join("::")
                ));
            }
        }
        for field in &variant.fields {
            for attribute in &field.attrs {
                if !ebd_4b1_attribute_is(attribute, "doc") {
                    violations.insert(format!(
                        "catalog-default-field-semantic-attribute-forbidden: {}|{}",
                        variant.ident,
                        attribute
                            .path()
                            .segments
                            .iter()
                            .map(|segment| segment.ident.to_string())
                            .collect::<Vec<_>>()
                            .join("::")
                    ));
                }
            }
        }
    }

    let expected_derives = BTreeSet::from([
        "Clone".to_string(),
        "Debug".to_string(),
        "Eq".to_string(),
        "PartialEq".to_string(),
    ]);
    match ebd_4b1_derive_set(canonical) {
        Some(actual) if actual == expected_derives => {}
        Some(actual) => {
            violations.insert(format!(
                "catalog-default-owner-derives: expected={expected_derives:?} actual={actual:?}"
            ));
        }
        None => {
            violations
                .insert("catalog-default-owner-derives-parse-failed: ColumnDefault".to_string());
        }
    }

    let actual_variants = canonical
        .variants
        .iter()
        .map(ebd_4b1_variant_shape)
        .collect::<Option<Vec<_>>>();
    let expected_variants = EBD_4B2A_EXPECTED_VARIANTS
        .iter()
        .map(|variant| (*variant).to_string())
        .collect::<Vec<_>>();
    match actual_variants {
        Some(actual) if actual == expected_variants => {}
        Some(actual) => {
            violations.insert(format!(
                "catalog-default-owner-variants: expected={expected_variants:?} actual={actual:?}"
            ));
        }
        None => {
            violations.insert(
                "catalog-default-owner-variant-shape-unsupported: ColumnDefault".to_string(),
            );
        }
    }

    let validator = file.items.iter().find_map(|item| match item {
        syn::Item::Fn(item) if item.sig.ident == "validate_column_default" => Some(item),
        _ => None,
    });
    let validator_is_exact = validator.is_some_and(|item| {
        ebd_4b1_is_pub_crate(&item.vis)
            && item.sig.constness.is_none()
            && item.sig.asyncness.is_none()
            && item.sig.unsafety.is_none()
            && item.sig.abi.is_none()
            && item.sig.generics.params.is_empty()
            && item.sig.generics.where_clause.is_none()
            && item.sig.inputs.len() == 1
            && item.sig.inputs.first().is_some_and(|argument| match argument {
                syn::FnArg::Typed(argument) => {
                    matches!(argument.pat.as_ref(), syn::Pat::Ident(ident) if ident.ident == "value")
                        && matches!(argument.ty.as_ref(), syn::Type::Reference(reference)
                            if reference.mutability.is_none()
                                && matches!(reference.elem.as_ref(), syn::Type::Path(path)
                                    if path.qself.is_none() && path.path.is_ident("ColumnDefault")))
                }
                syn::FnArg::Receiver(_) => false,
            })
            && matches!(&item.sig.output,
                syn::ReturnType::Type(_, ty)
                    if ebd_4b1_type_shape(ty).as_deref() == Some("Result<(),String>"))
    });
    if !validator_is_exact {
        violations.insert(
            "catalog-default-validator-signature: expected pub(crate) fn validate_column_default(&ColumnDefault) -> Result<(), String>"
                .to_string(),
        );
    }
    violations
}

fn ebd_4b2a_audit_column_def(source: &GuardSource) -> BTreeSet<String> {
    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "catalog-default-column-def-parse-failed: {}",
            source.path
        )]);
    };
    let Some(column_def) = file.items.iter().find_map(|item| match item {
        syn::Item::Struct(item) if item.ident == "ColumnDef" => Some(item),
        _ => None,
    }) else {
        return BTreeSet::from(["catalog-default-column-def-missing".to_string()]);
    };
    let actual_fields = column_def
        .fields
        .iter()
        .filter_map(|field| field.ident.as_ref().map(ToString::to_string))
        .collect::<Vec<_>>();
    let expected_fields = [
        "name",
        "data_type",
        "nullable",
        "write_default",
        "logical_type",
    ];
    let mut violations = BTreeSet::new();
    if actual_fields != expected_fields {
        violations.insert(format!(
            "catalog-default-column-def-fields: expected={expected_fields:?} actual={actual_fields:?}"
        ));
    }
    let Some(field) = column_def.fields.iter().find(|field| {
        field
            .ident
            .as_ref()
            .is_some_and(|ident| ident == "write_default")
    }) else {
        violations.insert("catalog-default-column-def-write-default-missing".to_string());
        return violations;
    };
    let Some((inner_path, plain_prelude)) = ebd_4b1_option_inner_path(&field.ty) else {
        violations.insert(
            "catalog-default-column-def-write-default-must-be-Option-ColumnDefault".to_string(),
        );
        return violations;
    };
    let (imports, aliases) = ebd_4b1_module_scope_inputs(&file);
    if plain_prelude
        && (aliases.contains_key(&(Vec::new(), "Option".to_string()))
            || ebd_4b1_file_defines_root_type_name(&file, "Option"))
    {
        violations
            .insert("catalog-default-column-def-write-default-must-use-prelude-Option".to_string());
        return violations;
    }
    let canonical = ebd_4b2a_resolve_type_path(&inner_path, source, &aliases, &[]);
    let canonical_schema_glob =
        ebd_4b1_canonical_schema_glob_scopes(source, &imports, &aliases).contains(&Vec::new());
    let exact = !canonical.is_empty()
        && canonical
            .iter()
            .all(|path| is_exact_catalog_schema_column_default_path(path));
    let exact_through_glob = inner_path == ["ColumnDefault"] && canonical_schema_glob;
    if !exact && !exact_through_glob {
        violations.insert(format!(
            "catalog-default-column-def-write-default-not-canonical: {}",
            canonical
                .iter()
                .map(|path| path.join("::"))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if canonical.iter().any(|path| is_iceberg_literal_path(path)) {
        violations.insert("catalog-default-column-def-vendor-literal-forbidden".to_string());
    }
    violations
}

fn ebd_4b2a_iceberg_literal_glob_scopes(
    source: &GuardSource,
    imports: &[Ebd4b1ModuleUseStatement],
    aliases: &RustScopedAliases,
) -> BTreeSet<Vec<String>> {
    let mut scopes = BTreeSet::new();
    for import in imports {
        let direct_iceberg_glob = import
            .segments
            .iter()
            .map(String::as_str)
            .eq(["iceberg", "spec", "*"]);
        let resolved = resolve_forwarding_paths(
            &import.segments,
            &source.path,
            &import.inline_modules,
            aliases,
            &mut BTreeSet::new(),
            0,
        )
        .unwrap_or_else(|| {
            vec![RustScopedUsePath {
                segments: import.segments.clone(),
                inline_modules: import.inline_modules.clone(),
            }]
        });
        if direct_iceberg_glob
            || resolved.into_iter().any(|path| {
                path.segments
                    .iter()
                    .map(String::as_str)
                    .eq(["iceberg", "spec", "*"])
            })
        {
            scopes.insert(import.inline_modules.clone());
        }
    }
    scopes
}

fn ebd_4b2a_type_contains_iceberg_literal(
    ty: &syn::Type,
    source: &GuardSource,
    aliases: &RustScopedAliases,
    iceberg_glob_scopes: &BTreeSet<Vec<String>>,
    inline_modules: &[String],
) -> bool {
    struct IcebergLiteralTypeAudit<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        iceberg_glob_scopes: &'a BTreeSet<Vec<String>>,
        inline_modules: &'a [String],
        found: bool,
    }

    impl<'ast> syn::visit::Visit<'ast> for IcebergLiteralTypeAudit<'_> {
        fn visit_type_path(&mut self, path: &'ast syn::TypePath) {
            if path.qself.is_none() {
                let segments = path
                    .path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect::<Vec<_>>();
                let resolved = ebd_4b2a_resolve_type_path(
                    &segments,
                    self.source,
                    self.aliases,
                    self.inline_modules,
                );
                if resolved.iter().any(|path| is_iceberg_literal_path(path))
                    || segments == ["Literal"]
                        && self.iceberg_glob_scopes.contains(self.inline_modules)
                {
                    self.found = true;
                }
            }
            syn::visit::visit_type_path(self, path);
        }
    }

    let mut audit = IcebergLiteralTypeAudit {
        source,
        aliases,
        iceberg_glob_scopes,
        inline_modules,
        found: false,
    };
    syn::visit::Visit::visit_type(&mut audit, ty);
    audit.found
}

fn ebd_4b2a_is_exact_iceberg_literal_option(
    ty: &syn::Type,
    source: &GuardSource,
    aliases: &RustScopedAliases,
    iceberg_glob_scopes: &BTreeSet<Vec<String>>,
    inline_modules: &[String],
) -> bool {
    let Some((inner, _)) = ebd_4b1_option_inner_path(ty) else {
        return false;
    };
    let resolved = ebd_4b2a_resolve_type_path(&inner, source, aliases, inline_modules);
    (!resolved.is_empty() && resolved.iter().all(|path| is_iceberg_literal_path(path)))
        || inner == ["Literal"] && iceberg_glob_scopes.contains(inline_modules)
}

fn ebd_4b2a_audit_catalog_raw_iceberg_literals(source: &GuardSource) -> BTreeSet<String> {
    struct CatalogRawLiteralAudit<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        iceberg_glob_scopes: &'a BTreeSet<Vec<String>>,
        inline_modules: Vec<String>,
        violations: BTreeSet<String>,
    }

    impl CatalogRawLiteralAudit<'_> {
        fn type_contains_literal(&self, ty: &syn::Type) -> bool {
            ebd_4b2a_type_contains_iceberg_literal(
                ty,
                self.source,
                self.aliases,
                self.iceberg_glob_scopes,
                &self.inline_modules,
            )
        }
    }

    impl<'ast> syn::visit::Visit<'ast> for CatalogRawLiteralAudit<'_> {
        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            for field in &item.fields {
                if !self.type_contains_literal(&field.ty) {
                    continue;
                }
                let field_name = field
                    .ident
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<unnamed>".to_string());
                let owned_by_column_def_guard =
                    item.ident == "ColumnDef" && field_name == "write_default";
                let allowed_iceberg_schema_field = item.ident == "IcebergSchemaFieldDef"
                    && self.inline_modules.is_empty()
                    && matches!(field_name.as_str(), "initial_default" | "write_default")
                    && ebd_4b2a_is_exact_iceberg_literal_option(
                        &field.ty,
                        self.source,
                        self.aliases,
                        self.iceberg_glob_scopes,
                        &self.inline_modules,
                    );
                if !owned_by_column_def_guard && !allowed_iceberg_schema_field {
                    self.violations.insert(format!(
                        "catalog-default-raw-iceberg-literal-forbidden: {}|struct {}|{}",
                        self.source.path, item.ident, field_name
                    ));
                }
            }
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            for variant in &item.variants {
                for field in &variant.fields {
                    if self.type_contains_literal(&field.ty) {
                        self.violations.insert(format!(
                            "catalog-default-raw-iceberg-literal-forbidden: {}|enum {}|{}",
                            self.source.path, item.ident, variant.ident
                        ));
                    }
                }
            }
            syn::visit::visit_item_enum(self, item);
        }

        fn visit_item_union(&mut self, item: &'ast syn::ItemUnion) {
            for field in &item.fields.named {
                if self.type_contains_literal(&field.ty) {
                    self.violations.insert(format!(
                        "catalog-default-raw-iceberg-literal-forbidden: {}|union {}",
                        self.source.path, item.ident
                    ));
                }
            }
            syn::visit::visit_item_union(self, item);
        }

        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            if self.type_contains_literal(&item.ty) {
                self.violations.insert(format!(
                    "catalog-default-raw-iceberg-literal-forbidden: {}|type {}",
                    self.source.path, item.ident
                ));
            }
            syn::visit::visit_item_type(self, item);
        }

        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            for transcriber in ebd_4b1_macro_rule_transcribers(item) {
                let Some(file) = ebd_4b2a_macro_transcriber_file(&transcriber) else {
                    continue;
                };
                for generated in &file.items {
                    match generated {
                        syn::Item::Struct(generated) => {
                            for field in &generated.fields {
                                if self.type_contains_literal(&field.ty) {
                                    self.violations.insert(format!(
                                        "catalog-default-macro-raw-iceberg-literal-forbidden: {}|struct {}",
                                        self.source.path, generated.ident
                                    ));
                                }
                            }
                        }
                        syn::Item::Enum(generated) => {
                            for variant in &generated.variants {
                                for field in &variant.fields {
                                    if self.type_contains_literal(&field.ty) {
                                        self.violations.insert(format!(
                                            "catalog-default-macro-raw-iceberg-literal-forbidden: {}|enum {}|{}",
                                            self.source.path, generated.ident, variant.ident
                                        ));
                                    }
                                }
                            }
                        }
                        syn::Item::Union(generated) => {
                            for field in &generated.fields.named {
                                if self.type_contains_literal(&field.ty) {
                                    self.violations.insert(format!(
                                        "catalog-default-macro-raw-iceberg-literal-forbidden: {}|union {}",
                                        self.source.path, generated.ident
                                    ));
                                }
                            }
                        }
                        syn::Item::Type(generated) => {
                            if self.type_contains_literal(&generated.ty) {
                                self.violations.insert(format!(
                                    "catalog-default-macro-raw-iceberg-literal-forbidden: {}|type {}",
                                    self.source.path, generated.ident
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }
            syn::visit::visit_item_macro(self, item);
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            let Some((_, items)) = &item.content else {
                return;
            };
            self.inline_modules.push(item.ident.to_string());
            for item in items {
                syn::visit::Visit::visit_item(self, item);
            }
            self.inline_modules.pop();
        }
    }

    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "catalog-default-raw-iceberg-literal-parse-failed: {}",
            source.path
        )]);
    };
    let (imports, aliases) = ebd_4b1_module_scope_inputs(&file);
    let iceberg_glob_scopes = ebd_4b2a_iceberg_literal_glob_scopes(source, &imports, &aliases);
    let mut audit = CatalogRawLiteralAudit {
        source,
        aliases: &aliases,
        iceberg_glob_scopes: &iceberg_glob_scopes,
        inline_modules: Vec::new(),
        violations: BTreeSet::new(),
    };
    syn::visit::Visit::visit_file(&mut audit, &file);
    audit.violations
}

fn ebd_4b2a_audit_parser_connector_dependency(source: &GuardSource) -> BTreeSet<String> {
    remove_redundant_descendant_paths(
        rust_all_source_canonical_paths(&source.text, &source.path)
            .into_iter()
            .filter(|path| is_connector_iceberg_path(path))
            .collect(),
    )
    .into_iter()
    .map(|path| {
        format!(
            "catalog-default-parser-connector-dependency: {}|{}",
            source.path,
            path.join("::")
        )
    })
    .collect()
}

fn ebd_4b2a_macro_generated_definition(tokens: &[String]) -> Option<&str> {
    const TYPE_NAMESPACE_ITEMS: &[&str] = &["enum", "struct", "union", "trait", "type", "mod"];
    tokens.windows(2).find_map(|pair| {
        (TYPE_NAMESPACE_ITEMS.contains(&pair[0].as_str()) && pair[1] == "ColumnDefault")
            .then_some(pair[0].as_str())
    })
}

fn ebd_4b2a_macro_transcriber_file(tokens: &[String]) -> Option<syn::File> {
    let mut normalized = Vec::new();
    let mut index = 0usize;
    while index < tokens.len() {
        if tokens.get(index).is_some_and(|token| token == "$")
            && tokens.get(index + 1).is_some_and(|token| token == "crate")
        {
            normalized.push("crate".to_string());
            index += 2;
        } else {
            normalized.push(tokens[index].clone());
            index += 1;
        }
    }
    syn::parse_file(&normalized.join(" ")).ok()
}

fn ebd_4b2a_type_contains_column_default(
    ty: &syn::Type,
    source: &GuardSource,
    aliases: &RustScopedAliases,
    canonical_glob_scopes: &BTreeSet<Vec<String>>,
    inline_modules: &[String],
) -> bool {
    struct ColumnDefaultTypeAudit<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        canonical_glob_scopes: &'a BTreeSet<Vec<String>>,
        inline_modules: &'a [String],
        found: bool,
    }

    impl<'ast> syn::visit::Visit<'ast> for ColumnDefaultTypeAudit<'_> {
        fn visit_type_path(&mut self, path: &'ast syn::TypePath) {
            if path.qself.is_none() {
                let segments = path
                    .path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect::<Vec<_>>();
                let resolved = ebd_4b2a_resolve_type_path(
                    &segments,
                    self.source,
                    self.aliases,
                    self.inline_modules,
                );
                if resolved
                    .iter()
                    .any(|path| is_catalog_schema_column_default_path(path))
                    || segments == ["ColumnDefault"]
                        && (self.source.path == EBD_4B2A_OWNER
                            || self.canonical_glob_scopes.contains(self.inline_modules))
                {
                    self.found = true;
                }
            }
            syn::visit::visit_type_path(self, path);
        }
    }

    let mut audit = ColumnDefaultTypeAudit {
        source,
        aliases,
        canonical_glob_scopes,
        inline_modules,
        found: false,
    };
    syn::visit::Visit::visit_type(&mut audit, ty);
    audit.found
}

fn ebd_4b2a_audit_definitions_and_forwarding(sources: &[GuardSource]) -> BTreeSet<String> {
    struct ColumnDefaultDefinitionAudit<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        canonical_glob_scopes: &'a BTreeSet<Vec<String>>,
        inline_modules: Vec<String>,
        violations: BTreeSet<String>,
    }

    impl ColumnDefaultDefinitionAudit<'_> {
        fn audit_alias_target(&mut self, name: &str, path: &[String], kind: &str) {
            let canonical =
                ebd_4b2a_resolve_type_path(path, self.source, self.aliases, &self.inline_modules);
            if canonical
                .iter()
                .any(|path| is_catalog_schema_column_default_path(path))
                || path == ["ColumnDefault"]
                    && self.canonical_glob_scopes.contains(&self.inline_modules)
            {
                self.violations.insert(format!(
                    "{kind}: {}|{}|crate::catalog::schema::ColumnDefault",
                    self.source.path, name
                ));
            }
        }

        fn audit_wrapper<'a>(
            &mut self,
            name: &str,
            kind: &str,
            fields: impl IntoIterator<Item = &'a syn::Field>,
        ) {
            let fields = fields.into_iter().collect::<Vec<_>>();
            let contains_default = fields.iter().any(|field| {
                ebd_4b2a_type_contains_column_default(
                    &field.ty,
                    self.source,
                    self.aliases,
                    self.canonical_glob_scopes,
                    &self.inline_modules,
                )
            });
            let looks_like_default_vocabulary =
                name.to_ascii_lowercase().contains("default") || fields.len() == 1;
            if contains_default && looks_like_default_vocabulary {
                self.violations.insert(format!(
                    "catalog-default-wrapper-forbidden: {}|{kind}|{name}",
                    self.source.path
                ));
            }
        }
    }

    impl<'ast> syn::visit::Visit<'ast> for ColumnDefaultDefinitionAudit<'_> {
        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            let is_canonical = item.ident == "ColumnDefault"
                && self.source.path == EBD_4B2A_OWNER
                && self.inline_modules.is_empty();
            if item.ident == "ColumnDefault" && !is_canonical {
                self.violations.insert(format!(
                    "catalog-default-secondary-enum: {}|ColumnDefault",
                    self.source.path
                ));
            }
            if !is_canonical {
                self.audit_wrapper(
                    &item.ident.to_string(),
                    "enum",
                    item.variants
                        .iter()
                        .flat_map(|variant| variant.fields.iter()),
                );
            }
            syn::visit::visit_item_enum(self, item);
        }

        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            if item.ident == "ColumnDefault" {
                self.violations.insert(format!(
                    "catalog-default-secondary-struct: {}|ColumnDefault",
                    self.source.path
                ));
            }
            self.audit_wrapper(&item.ident.to_string(), "struct", item.fields.iter());
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_item_union(&mut self, item: &'ast syn::ItemUnion) {
            if item.ident == "ColumnDefault" {
                self.violations.insert(format!(
                    "catalog-default-secondary-union: {}|ColumnDefault",
                    self.source.path
                ));
            }
            self.audit_wrapper(&item.ident.to_string(), "union", item.fields.named.iter());
            syn::visit::visit_item_union(self, item);
        }

        fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
            if item.ident == "ColumnDefault" {
                self.violations.insert(format!(
                    "catalog-default-secondary-trait: {}|ColumnDefault",
                    self.source.path
                ));
            }
            syn::visit::visit_item_trait(self, item);
        }

        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            if item.ident == "ColumnDefault" {
                self.violations.insert(format!(
                    "catalog-default-type-alias: {}|ColumnDefault",
                    self.source.path
                ));
            }
            if let Some(path) = ebd_4b1_direct_alias_rhs_path(&item.ty) {
                self.audit_alias_target(
                    &item.ident.to_string(),
                    &path,
                    "catalog-default-type-alias-target",
                );
            }
            syn::visit::visit_item_type(self, item);
        }

        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            for transcriber in ebd_4b1_macro_rule_transcribers(item) {
                if let Some(kind) = ebd_4b2a_macro_generated_definition(&transcriber) {
                    self.violations.insert(format!(
                        "catalog-default-macro-generated-definition: {}|{kind}|ColumnDefault",
                        self.source.path
                    ));
                }
                for (name, path) in ebd_4b1_macro_generated_direct_aliases(&transcriber) {
                    self.audit_alias_target(&name, &path, "catalog-default-macro-generated-alias");
                }
                if let Some(file) = ebd_4b2a_macro_transcriber_file(&transcriber) {
                    for generated in &file.items {
                        match generated {
                            syn::Item::Struct(generated) => self.audit_wrapper(
                                &generated.ident.to_string(),
                                "macro-struct",
                                generated.fields.iter(),
                            ),
                            syn::Item::Enum(generated) => self.audit_wrapper(
                                &generated.ident.to_string(),
                                "macro-enum",
                                generated
                                    .variants
                                    .iter()
                                    .flat_map(|variant| variant.fields.iter()),
                            ),
                            syn::Item::Union(generated) => self.audit_wrapper(
                                &generated.ident.to_string(),
                                "macro-union",
                                generated.fields.named.iter(),
                            ),
                            _ => {}
                        }
                    }
                }
            }
            syn::visit::visit_item_macro(self, item);
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if item.ident == "ColumnDefault" {
                self.violations.insert(format!(
                    "catalog-default-secondary-module: {}|ColumnDefault",
                    self.source.path
                ));
            }
            let Some((_, items)) = &item.content else {
                return;
            };
            self.inline_modules.push(item.ident.to_string());
            for item in items {
                syn::visit::Visit::visit_item(self, item);
            }
            self.inline_modules.pop();
        }
    }

    let mut violations = BTreeSet::new();
    for source in sources {
        let Ok(file) = syn::parse_file(&source.text) else {
            violations.insert(format!(
                "catalog-default-definition-parse-failed: {}",
                source.path
            ));
            continue;
        };
        let (imports, aliases) = ebd_4b1_module_scope_inputs(&file);
        let canonical_glob_scopes =
            ebd_4b1_canonical_schema_glob_scopes(source, &imports, &aliases);
        let mut audit = ColumnDefaultDefinitionAudit {
            source,
            aliases: &aliases,
            canonical_glob_scopes: &canonical_glob_scopes,
            inline_modules: Vec::new(),
            violations: BTreeSet::new(),
        };
        syn::visit::Visit::visit_file(&mut audit, &file);
        violations.extend(audit.violations);

        if source.path == EBD_4B2A_OWNER {
            continue;
        }
        for import in imports {
            if import.visibility == "private" {
                continue;
            }
            let Some(resolved) = resolve_forwarding_paths(
                &import.segments,
                &source.path,
                &import.inline_modules,
                &aliases,
                &mut BTreeSet::new(),
                0,
            ) else {
                continue;
            };
            for target in resolved {
                let Some(canonical) = rust_canonical_path_segments_in_scope(
                    &target.segments,
                    &source.path,
                    &target.inline_modules,
                ) else {
                    continue;
                };
                if is_catalog_schema_column_default_path(&canonical)
                    || is_catalog_schema_module_path(&canonical)
                    || is_catalog_schema_glob_path(&canonical)
                {
                    violations.insert(format!(
                        "catalog-default-forwarding-reexport: {}|{}|{}",
                        source.path,
                        import.visibility,
                        canonical.join("::")
                    ));
                }
            }
        }
    }
    violations
}

#[test]
fn ebd_4b2a_detector_requires_exact_column_default_owner() {
    let valid = GuardSource::new(
        EBD_4B2A_OWNER,
        r#"
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnDefault {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32 { bits: u32 },
    Float64 { bits: u64 },
    Decimal { unscaled: i128, precision: u8, scale: i8 },
    String(String),
    Binary(Vec<u8>),
    Date { days_since_epoch: i32 },
    TimeMicros { micros_since_midnight: i64 },
    TimestampMicros { micros_since_epoch: i64 },
    TimestamptzMicros { micros_since_epoch: i64 },
    TimestampNanos { nanos_since_epoch: i64 },
    TimestamptzNanos { nanos_since_epoch: i64 },
    Uuid([u8; 16]),
    Fixed { size: u64, bytes: Vec<u8> },
    Struct(Vec<(String, ColumnDefault)>),
    Array(Vec<ColumnDefault>),
    Map(Vec<(ColumnDefault, ColumnDefault)>),
}
pub(crate) fn validate_column_default(value: &ColumnDefault) -> Result<(), String> {
    let _ = value;
    Ok(())
}
"#,
    );
    let valid_violations = ebd_4b2a_audit_schema_owner(&valid);
    assert!(
        valid_violations.is_empty(),
        "valid owner fixture was rejected: {valid_violations:?}"
    );

    for (from, to) in [
        ("pub enum ColumnDefault", "pub(crate) enum ColumnDefault"),
        ("PartialEq, Eq", "PartialEq"),
        ("Float32 { bits: u32 }", "Float32(f32)"),
        (
            "Decimal { unscaled: i128, precision: u8, scale: i8 }",
            "Decimal { unscaled: i128, scale: i8, precision: u8 }",
        ),
        (
            "pub(crate) fn validate_column_default",
            "pub fn validate_column_default",
        ),
    ] {
        let invalid = GuardSource::new(EBD_4B2A_OWNER, &valid.text.replacen(from, to, 1));
        assert!(
            !ebd_4b2a_audit_schema_owner(&invalid).is_empty(),
            "owner mutation was missed: {from} -> {to}"
        );
    }
}

#[test]
fn ebd_4b2a_detector_rejects_old_column_field_but_allows_iceberg_schema_field() {
    let canonical = GuardSource::new(
        "src/sql/catalog.rs",
        r#"
use crate::catalog::schema::{ColumnDefault, SqlType};
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub write_default: Option<ColumnDefault>,
    pub logical_type: Option<SqlType>,
}
pub struct IcebergSchemaFieldDef {
    pub initial_default: Option<iceberg::spec::Literal>,
    pub write_default: Option<iceberg::spec::Literal>,
}
"#,
    );
    assert!(ebd_4b2a_audit_column_def(&canonical).is_empty());
    assert!(
        ebd_4b2a_audit_catalog_raw_iceberg_literals(&canonical).is_empty(),
        "the exact IcebergSchemaFieldDef exception must remain allowed"
    );
    let canonical_glob = GuardSource::new(
        "src/sql/catalog.rs",
        r#"
use iceberg::spec::*;
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub write_default: Option<crate::catalog::schema::ColumnDefault>,
    pub logical_type: Option<crate::catalog::schema::SqlType>,
}
pub struct IcebergSchemaFieldDef {
    pub initial_default: Option<Literal>,
    pub write_default: Option<Literal>,
}
"#,
    );
    assert!(
        ebd_4b2a_audit_catalog_raw_iceberg_literals(&canonical_glob).is_empty(),
        "semantic root IcebergSchemaFieldDef fields must allow an Iceberg spec glob import"
    );

    for old_field in [
        "pub write_default: Option<iceberg::spec::Literal>",
        "pub write_default: Option<::iceberg::spec::Literal>",
    ] {
        let old = GuardSource::new(
            "src/sql/catalog.rs",
            &canonical
                .text
                .replace("pub write_default: Option<ColumnDefault>", old_field),
        );
        let violations = ebd_4b2a_audit_column_def(&old);
        assert!(
            violations
                .iter()
                .any(|item| item.contains("write-default-not-canonical")),
            "old field fixture was missed: {violations:?}"
        );
    }

    for import in [
        "use iceberg::spec::Literal as VendorDefault;",
        "use iceberg::spec::{Literal as VendorDefault, Type};",
        "#[cfg(test)] use iceberg::spec::Literal as VendorDefault;",
    ] {
        let aliased = GuardSource::new(
            "src/sql/catalog.rs",
            &format!(
                "{import}\n{}",
                canonical
                    .text
                    .replace("Option<ColumnDefault>", "Option<VendorDefault>")
            ),
        );
        assert!(
            !ebd_4b2a_audit_column_def(&aliased).is_empty(),
            "vendor alias fixture was missed: {import}"
        );
    }

    for forbidden in [
        r#"
pub struct ColumnDef { pub name: String, pub data_type: DataType, pub nullable: bool, pub write_default: Option<crate::catalog::schema::ColumnDefault>, pub logical_type: Option<crate::catalog::schema::SqlType> }
pub struct IcebergSchemaFieldDef { pub initial_default: Option<iceberg::spec::Literal>, pub write_default: Option<iceberg::spec::Literal> }
pub struct OtherSchemaField { pub write_default: Option<iceberg::spec::Literal> }
"#,
        r#"
pub struct ColumnDef { pub name: String, pub data_type: DataType, pub nullable: bool, pub write_default: Option<crate::catalog::schema::ColumnDefault>, pub logical_type: Option<crate::catalog::schema::SqlType> }
pub struct IcebergSchemaFieldDef { pub initial_default: Option<iceberg::spec::Literal>, pub write_default: Option<iceberg::spec::Literal> }
type VendorDefault = iceberg::spec::Literal;
"#,
        r#"
use iceberg::spec::Literal as VendorDefault;
pub struct ColumnDef { pub name: String, pub data_type: DataType, pub nullable: bool, pub write_default: Option<crate::catalog::schema::ColumnDefault>, pub logical_type: Option<crate::catalog::schema::SqlType> }
pub struct IcebergSchemaFieldDef { pub initial_default: Option<iceberg::spec::Literal>, pub write_default: Option<iceberg::spec::Literal> }
enum OtherDefault { Value(VendorDefault) }
"#,
        r#"
pub struct ColumnDef { pub name: String, pub data_type: DataType, pub nullable: bool, pub write_default: Option<crate::catalog::schema::ColumnDefault>, pub logical_type: Option<crate::catalog::schema::SqlType> }
pub struct IcebergSchemaFieldDef { pub initial_default: Option<iceberg::spec::Literal>, pub write_default: Option<iceberg::spec::Literal>, pub fallback_default: Option<iceberg::spec::Literal> }
"#,
        r#"
pub struct ColumnDef { pub name: String, pub data_type: DataType, pub nullable: bool, pub write_default: Option<crate::catalog::schema::ColumnDefault>, pub logical_type: Option<crate::catalog::schema::SqlType> }
pub struct IcebergSchemaFieldDef { pub initial_default: Option<iceberg::spec::Literal>, pub write_default: Option<iceberg::spec::Literal> }
mod nested { pub struct IcebergSchemaFieldDef { pub initial_default: Option<iceberg::spec::Literal>, pub write_default: Option<iceberg::spec::Literal> } }
"#,
        r#"
pub struct ColumnDef { pub name: String, pub data_type: DataType, pub nullable: bool, pub write_default: Option<crate::catalog::schema::ColumnDefault>, pub logical_type: Option<crate::catalog::schema::SqlType> }
pub struct IcebergSchemaFieldDef { pub initial_default: Option<iceberg::spec::Literal>, pub write_default: Option<iceberg::spec::Literal> }
macro_rules! raw_holder { () => { struct DefaultValue(iceberg::spec::Literal); } }
raw_holder!();
"#,
    ] {
        let source = GuardSource::new("src/sql/catalog.rs", forbidden);
        assert!(
            !ebd_4b2a_audit_catalog_raw_iceberg_literals(&source).is_empty(),
            "raw Iceberg literal escape was missed: {forbidden}"
        );
    }
}

#[test]
fn ebd_4b2a_detector_rejects_secondary_alias_forward_and_macro_owners() {
    let allowed = [
        GuardSource::new(
            "src/sql/literal.rs",
            r###"
use crate::catalog::schema::ColumnDefault;
fn consume(value: ColumnDefault) { let _ = value; }
const TEXT: &str = "pub use crate::catalog::schema::ColumnDefault";
const RAW: &str = r#"type ColumnDefault = iceberg::spec::Literal"#;
// struct ColumnDefault(iceberg::spec::Literal);
"###,
        ),
        GuardSource::new(
            "src/sql/catalog_consumer.rs",
            r#"
use crate::catalog::schema::ColumnDefault;
struct ColumnDef {
    name: String,
    data_type: DataType,
    nullable: bool,
    write_default: Option<ColumnDefault>,
    logical_type: Option<SqlType>,
}
"#,
        ),
        GuardSource::new(
            EBD_4B2A_OWNER,
            "#[derive(Clone, Debug, PartialEq, Eq)] pub enum ColumnDefault { Null }",
        ),
    ];
    assert!(
        ebd_4b2a_audit_definitions_and_forwarding(&allowed).is_empty(),
        "legitimate use or lexical noise must be allowed"
    );

    let invalid = [
        GuardSource::new("src/sql/secondary.rs", "pub enum ColumnDefault { Null }"),
        GuardSource::new(
            "src/sql/alias.rs",
            "pub type DefaultValue = crate::catalog::schema::ColumnDefault;",
        ),
        GuardSource::new(
            "src/sql/forward.rs",
            "pub use crate::catalog::schema::ColumnDefault as DefaultValue;",
        ),
        GuardSource::new(
            "src/sql/grouped_forward.rs",
            "pub(crate) use crate::catalog::schema::{ColumnDefault as DefaultValue, SqlType};",
        ),
        GuardSource::new(
            "src/sql/glob_forward.rs",
            "pub use crate::catalog::schema::*;",
        ),
        GuardSource::new(
            "src/sql/macro_owner.rs",
            "macro_rules! owner { () => { struct ColumnDefault; } } owner!();",
        ),
        GuardSource::new(
            "src/sql/macro_alias.rs",
            "macro_rules! owner { () => { type DefaultValue = $crate::catalog::schema::ColumnDefault; } } owner!();",
        ),
        GuardSource::new(
            "src/sql/test_owner.rs",
            "#[cfg(test)] mod tests { pub struct ColumnDefault; }",
        ),
        GuardSource::new(
            "src/sql/direct_wrapper.rs",
            "use crate::catalog::schema::ColumnDefault; struct DefaultValue(ColumnDefault);",
        ),
        GuardSource::new(
            "src/sql/named_wrapper.rs",
            "use crate::catalog::schema::ColumnDefault; struct DefaultEnvelope { value: ColumnDefault }",
        ),
        GuardSource::new(
            "src/sql/alias_wrapper.rs",
            "use crate::catalog::schema::ColumnDefault as Neutral; struct DefaultAlias { value: Neutral }",
        ),
        GuardSource::new(
            "src/sql/glob_wrapper.rs",
            "use crate::catalog::schema::*; enum DefaultChoice { Wrapped(ColumnDefault), Missing }",
        ),
        GuardSource::new(
            "src/sql/relative_wrapper.rs",
            "struct Wrapper(crate::catalog::schema::ColumnDefault);",
        ),
        GuardSource::new(
            "src/sql/cfg_wrapper.rs",
            "#[cfg(test)] mod tests { use crate::catalog::schema::ColumnDefault; struct DefaultValue { value: ColumnDefault } }",
        ),
        GuardSource::new(
            "src/sql/macro_tuple_wrapper.rs",
            "macro_rules! wrapper { () => { struct DefaultValue($crate::catalog::schema::ColumnDefault); } } wrapper!();",
        ),
        GuardSource::new(
            "src/sql/macro_named_wrapper.rs",
            "macro_rules! wrapper { () => { struct DefaultEnvelope { value: $crate::catalog::schema::ColumnDefault } } } wrapper!();",
        ),
        GuardSource::new(
            "src/sql/macro_enum_wrapper.rs",
            "macro_rules! wrapper { () => { enum DefaultChoice { Value($crate::catalog::schema::ColumnDefault), Missing } } } wrapper!();",
        ),
        GuardSource::new(
            "src/sql/macro_union_wrapper.rs",
            "macro_rules! wrapper { () => { union DefaultUnion { value: std::mem::ManuallyDrop<$crate::catalog::schema::ColumnDefault> } } } wrapper!();",
        ),
    ];
    let violations = ebd_4b2a_audit_definitions_and_forwarding(&invalid);
    for path in [
        "secondary.rs",
        "alias.rs",
        "forward.rs",
        "grouped_forward.rs",
        "glob_forward.rs",
        "macro_owner.rs",
        "macro_alias.rs",
        "test_owner.rs",
        "direct_wrapper.rs",
        "named_wrapper.rs",
        "alias_wrapper.rs",
        "glob_wrapper.rs",
        "relative_wrapper.rs",
        "cfg_wrapper.rs",
        "macro_tuple_wrapper.rs",
        "macro_named_wrapper.rs",
        "macro_enum_wrapper.rs",
        "macro_union_wrapper.rs",
    ] {
        assert!(
            violations.iter().any(|item| item.contains(path)),
            "definition fixture was missed: {path}; got {violations:?}"
        );
    }
}

#[test]
fn ebd_4b2a_detector_rejects_parser_connector_paths_and_ignores_noise() {
    let allowed = GuardSource::new(
        "src/sql/parser/dialect/create_table.rs",
        r###"
use crate::sql::literal::default_literal_to_column_default;
// crate::connector::iceberg::default_value::default_literal_to_iceberg();
const TEXT: &str = "crate::connector::iceberg";
const RAW: &str = r#"use crate::connector::iceberg::*;"#;
fn validate() { let _ = default_literal_to_column_default; }
"###,
    );
    assert!(ebd_4b2a_audit_parser_connector_dependency(&allowed).is_empty());

    for dependency in [
        "use crate::connector::iceberg::default_value; fn validate() { let _ = default_value::default_literal_to_iceberg; }",
        "use crate::connector::{iceberg::default_value}; fn validate() { let _ = default_value::default_literal_to_iceberg; }",
        "use crate::connector::iceberg as vendor; fn validate() { let _ = vendor::default_value::default_literal_to_iceberg; }",
        "use crate::connector::iceberg::*; fn validate() { let _ = default_value::default_literal_to_iceberg; }",
        "use super::super::super::super::connector::iceberg::default_value; fn validate() { let _ = default_value::default_literal_to_iceberg; }",
        "#[cfg(test)] mod tests { use crate::connector::iceberg::default_value; fn validate() { let _ = default_value::default_literal_to_iceberg; } }",
    ] {
        let invalid = GuardSource::new("src/sql/parser/dialect/create_table.rs", dependency);
        assert!(
            !ebd_4b2a_audit_parser_connector_dependency(&invalid).is_empty(),
            "parser dependency fixture was missed: {dependency}"
        );
    }
}

#[test]
fn ebd_4b2a_catalog_write_default_boundary_is_complete() {
    let sources = ebd_4b1_collect_repo_sources();
    let mut violations = BTreeSet::new();

    if let Some(owner) = sources.iter().find(|source| source.path == EBD_4B2A_OWNER) {
        violations.extend(ebd_4b2a_audit_schema_owner(owner));
    } else {
        violations.insert(format!("catalog-default-owner-missing: {EBD_4B2A_OWNER}"));
    }
    if let Some(owner) = sources.iter().find(|source| source.path == EBD_4B2A_OWNER) {
        violations.extend(ebd_4b2a_audit_column_def(owner));
    } else {
        violations.insert(format!(
            "catalog-default-column-def-owner-missing: {EBD_4B2A_OWNER}"
        ));
    }
    if let Some(catalog) = sources
        .iter()
        .find(|source| source.path == "src/sql/catalog.rs")
    {
        violations.extend(ebd_4b2a_audit_catalog_raw_iceberg_literals(catalog));
    } else {
        violations.insert("catalog-default-model-owner-missing: src/sql/catalog.rs".to_string());
    }
    if let Some(parser) = sources
        .iter()
        .find(|source| source.path == "src/sql/parser/dialect/create_table.rs")
    {
        violations.extend(ebd_4b2a_audit_parser_connector_dependency(parser));
    } else {
        violations.insert(
            "catalog-default-parser-owner-missing: src/sql/parser/dialect/create_table.rs"
                .to_string(),
        );
    }
    violations.extend(ebd_4b2a_audit_definitions_and_forwarding(&sources));

    let actual = current_source_tree_snapshot();
    let actual_counts = [
        actual.engine_files.len(),
        actual.engine_module_declarations.len(),
        actual
            .external_engine_dependencies
            .values()
            .map(BTreeSet::len)
            .sum(),
        actual
            .standalone_state_dependencies
            .values()
            .map(BTreeSet::len)
            .sum(),
        actual.forwarding_reexports.len(),
    ];
    let expected_counts = [76, 74, 210, 55, 0];
    if actual_counts != expected_counts {
        violations.insert(format!(
            "catalog-default-ebd-1-baseline-drift: expected={expected_counts:?} actual={actual_counts:?}"
        ));
    }

    assert!(
        violations.is_empty(),
        "EBD-4B2A catalog write-default boundary failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

const EBD_4B2B_OWNER: &str = "src/catalog/schema.rs";

fn ebd_4b2b_path_segments(ty: &syn::Type) -> Option<(Vec<String>, bool)> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    if path.qself.is_some()
        || path
            .path
            .segments
            .iter()
            .any(|segment| !matches!(segment.arguments, syn::PathArguments::None))
    {
        return None;
    }
    Some((
        path.path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect(),
        path.path.leading_colon.is_some(),
    ))
}

fn ebd_4b2b_plain_name_is_shadowed(
    file: &syn::File,
    aliases: &RustScopedAliases,
    name: &str,
) -> bool {
    aliases.contains_key(&(Vec::new(), name.to_string()))
        || ebd_4b1_file_defines_root_type_name(file, name)
}

fn ebd_4b2b_root_is_shadowed(file: &syn::File, aliases: &RustScopedAliases, root: &str) -> bool {
    ebd_4b2b_plain_name_is_shadowed(file, aliases, root)
        || file.items.iter().any(|item| {
            let syn::Item::ExternCrate(item) = item else {
                return false;
            };
            item.rename
                .as_ref()
                .map_or_else(|| item.ident == root, |(_, rename)| rename == root)
        })
}

fn ebd_4b2b_option_inner_name(
    file: &syn::File,
    aliases: &RustScopedAliases,
    ty: &syn::Type,
) -> Option<String> {
    let syn::Type::Path(option) = ty else {
        return None;
    };
    let root = option.path.segments.first()?.ident.to_string();
    if matches!(root.as_str(), "std" | "core" | "alloc")
        && ebd_4b2b_root_is_shadowed(file, aliases, &root)
    {
        return None;
    }
    let Some((inner, plain_prelude)) = ebd_4b1_option_inner_path(ty) else {
        return None;
    };
    if plain_prelude && ebd_4b2b_plain_name_is_shadowed(file, aliases, "Option") {
        return None;
    }
    (inner.len() == 1).then(|| inner[0].clone())
}

fn ebd_4b2b_struct_derive_set(item: &syn::ItemStruct) -> Option<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for attribute in item
        .attrs
        .iter()
        .filter(|attribute| ebd_4b1_attribute_is(attribute, "derive"))
    {
        let paths = attribute
            .parse_args_with(
                syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated,
            )
            .ok()?;
        names.extend(paths.into_iter().map(|path| {
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::")
        }));
    }
    Some(names)
}

fn ebd_4b2b_audit_column_def_owner(source: &GuardSource) -> BTreeSet<String> {
    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "catalog-column-owner-missing: {}|parse-failed",
            source.path
        )]);
    };
    let root_columns = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Struct(item) if item.ident == "ColumnDef" => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();
    if root_columns.len() != 1 {
        return BTreeSet::from([format!(
            "catalog-column-owner-missing: {}|expected=1 actual={}",
            source.path,
            root_columns.len()
        )]);
    }
    let column = root_columns[0];
    let mut violations = BTreeSet::new();
    if !ebd_4b1_is_pub(&column.vis)
        || !column.generics.params.is_empty()
        || column.generics.where_clause.is_some()
    {
        violations.insert(format!(
            "catalog-column-owner-fields: {}|visibility-or-generics",
            source.path
        ));
    }
    if column.attrs.iter().any(|attribute| {
        !ebd_4b1_attribute_is(attribute, "derive") && !ebd_4b1_attribute_is(attribute, "doc")
    }) {
        violations.insert(format!(
            "catalog-column-owner-fields: {}|semantic-attribute",
            source.path
        ));
    }
    let expected_derives = BTreeSet::from([
        "Clone".to_string(),
        "Debug".to_string(),
        "PartialEq".to_string(),
    ]);
    if ebd_4b2b_struct_derive_set(column).as_ref() != Some(&expected_derives) {
        violations.insert(format!(
            "catalog-column-owner-fields: {}|derives",
            source.path
        ));
    }

    let syn::Fields::Named(fields) = &column.fields else {
        violations.insert(format!(
            "catalog-column-owner-fields: {}|named-fields-required",
            source.path
        ));
        return violations;
    };
    let actual_names = fields
        .named
        .iter()
        .filter_map(|field| field.ident.as_ref().map(ToString::to_string))
        .collect::<Vec<_>>();
    let expected_names = [
        "name",
        "data_type",
        "nullable",
        "write_default",
        "logical_type",
    ];
    if actual_names != expected_names {
        violations.insert(format!(
            "catalog-column-owner-fields: {}|expected={expected_names:?} actual={actual_names:?}",
            source.path
        ));
        return violations;
    }
    if fields.named.iter().any(|field| {
        !ebd_4b1_is_pub(&field.vis)
            || field
                .attrs
                .iter()
                .any(|attribute| !ebd_4b1_attribute_is(attribute, "doc"))
    }) {
        violations.insert(format!(
            "catalog-column-owner-fields: {}|field-visibility-or-attribute",
            source.path
        ));
    }

    let (_, aliases) = ebd_4b1_module_scope_inputs(&file);
    let name_ok = ebd_4b2b_path_segments(&fields.named[0].ty).is_some_and(|(path, _)| {
        (path == ["std", "string", "String"] && !ebd_4b2b_root_is_shadowed(&file, &aliases, "std"))
            || (path == ["alloc", "string", "String"]
                && !ebd_4b2b_root_is_shadowed(&file, &aliases, "alloc"))
            || (path == ["String"] && !ebd_4b2b_plain_name_is_shadowed(&file, &aliases, "String"))
    });
    let data_type_ok = ebd_4b2b_path_segments(&fields.named[1].ty)
        .is_some_and(|(path, _)| path == ["arrow", "datatypes", "DataType"])
        && !ebd_4b2b_root_is_shadowed(&file, &aliases, "arrow");
    let nullable_ok =
        ebd_4b2b_path_segments(&fields.named[2].ty).is_some_and(|(path, _)| path == ["bool"]);
    let write_default_ok = ebd_4b2b_option_inner_name(&file, &aliases, &fields.named[3].ty)
        .as_deref()
        == Some("ColumnDefault");
    let logical_type_ok = ebd_4b2b_option_inner_name(&file, &aliases, &fields.named[4].ty)
        .as_deref()
        == Some("SqlType");
    for (name, valid) in [
        ("name", name_ok),
        ("data_type", data_type_ok),
        ("nullable", nullable_ok),
        ("write_default", write_default_ok),
        ("logical_type", logical_type_ok),
    ] {
        if !valid {
            violations.insert(format!(
                "catalog-column-owner-field-type: {}|{name}",
                source.path
            ));
        }
    }
    violations
}

fn ebd_4b2b_audit_schema_dependencies(source: &GuardSource) -> BTreeSet<String> {
    struct SchemaDependencyAudit<'a> {
        source: &'a str,
        dependency_roots: &'a BTreeSet<String>,
        in_column_data_type: bool,
        violations: BTreeSet<String>,
    }

    impl<'ast> syn::visit::Visit<'ast> for SchemaDependencyAudit<'_> {
        fn visit_item_extern_crate(&mut self, item: &'ast syn::ItemExternCrate) {
            if !matches!(item.ident.to_string().as_str(), "std" | "core" | "alloc") {
                self.violations.insert(format!(
                    "catalog-column-arrow-dependency-forbidden: {}|extern-crate:{}",
                    self.source, item.ident
                ));
            }
            syn::visit::visit_item_extern_crate(self, item);
        }

        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            if item.ident == "ColumnDef" {
                for field in &item.fields {
                    self.in_column_data_type = field
                        .ident
                        .as_ref()
                        .is_some_and(|ident| ident == "data_type");
                    syn::visit::Visit::visit_field(self, field);
                    self.in_column_data_type = false;
                }
                return;
            }
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_path(&mut self, path: &'ast syn::Path) {
            let segments = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            let permitted_arrow =
                self.in_column_data_type && segments == ["arrow", "datatypes", "DataType"];
            let forbidden_external = segments.first().is_some_and(|root| {
                self.dependency_roots.contains(root)
                    && !matches!(root.as_str(), "std" | "core" | "alloc")
                    && !permitted_arrow
            });
            let forbidden_local = segments.len() > 1
                && segments
                    .first()
                    .is_some_and(|root| matches!(root.as_str(), "crate" | "self" | "super"));
            if forbidden_external || forbidden_local {
                self.violations.insert(format!(
                    "catalog-column-arrow-dependency-forbidden: {}|{}",
                    self.source,
                    segments.join("::")
                ));
            }
            syn::visit::visit_path(self, path);
        }
    }

    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "catalog-column-arrow-dependency-forbidden: {}|parse-failed",
            source.path
        )]);
    };
    let dependency_roots = ebd_4a_dependency_crate_roots();
    let mut audit = SchemaDependencyAudit {
        source: &source.path,
        dependency_roots: &dependency_roots,
        in_column_data_type: false,
        violations: BTreeSet::new(),
    };
    syn::visit::Visit::visit_file(&mut audit, &file);
    audit.violations
}

fn ebd_4b2b_is_legacy_column_def_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments.starts_with(&["crate", "sql", "catalog", "ColumnDef"])
        || segments.starts_with(&["crate", "engine", "catalog", "ColumnDef"])
        || segments.starts_with(&["crate", "engine", "ColumnDef"])
}

fn ebd_4b2b_is_canonical_column_def_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments.starts_with(&["crate", "catalog", "schema", "ColumnDef"])
}

fn ebd_4b2b_is_canonical_schema_forward_target(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments == ["crate", "catalog", "schema"]
        || segments == ["crate", "catalog", "schema", "*"]
        || ebd_4b2b_is_canonical_column_def_path(path)
}

fn ebd_4b2b_is_legacy_column_def_glob(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    segments == ["crate", "sql", "catalog", "*"]
        || segments == ["crate", "engine", "catalog", "*"]
        || segments == ["crate", "engine", "*"]
}

fn ebd_4b2b_macro_defines_column_def(item: &syn::ItemMacro) -> bool {
    ebd_4b1_macro_rule_transcribers(item).iter().any(|tokens| {
        tokens.windows(2).any(|pair| {
            matches!(
                pair[0].as_str(),
                "enum" | "struct" | "union" | "trait" | "type" | "mod"
            ) && pair[1] == "ColumnDef"
        })
    })
}

fn ebd_4b2b_macro_invokes_column_def(item: &syn::ItemMacro) -> bool {
    item.ident.is_none()
        && rust_source_tokens(&item.mac.tokens.to_string())
            .iter()
            .any(|token| token.text == "ColumnDef")
}

fn ebd_4b2b_type_contains_canonical_column_def(
    ty: &syn::Type,
    source: &GuardSource,
    aliases: &RustScopedAliases,
    inline_modules: &[String],
) -> bool {
    struct ColumnPathAudit<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        inline_modules: &'a [String],
        found: bool,
    }
    impl<'ast> syn::visit::Visit<'ast> for ColumnPathAudit<'_> {
        fn visit_path(&mut self, path: &'ast syn::Path) {
            let segments = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            if let Some(resolved) = rust_resolve_scoped_paths(
                &segments,
                self.inline_modules,
                self.aliases,
                &mut BTreeSet::new(),
                0,
            ) {
                self.found |= resolved.into_iter().any(|resolved| {
                    rust_canonical_path_segments_in_scope(
                        &resolved.segments,
                        &self.source.path,
                        &resolved.inline_modules,
                    )
                    .is_some_and(|canonical| ebd_4b2b_is_canonical_column_def_path(&canonical))
                });
            }
            syn::visit::visit_path(self, path);
        }
    }
    let mut audit = ColumnPathAudit {
        source,
        aliases,
        inline_modules,
        found: false,
    };
    syn::visit::Visit::visit_type(&mut audit, ty);
    audit.found
}

fn ebd_4b2b_audit_legacy_paths_and_forwarding(sources: &[GuardSource]) -> BTreeSet<String> {
    struct ColumnDefinitionAudit<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        inline_modules: Vec<String>,
        violations: BTreeSet<String>,
    }
    impl<'ast> syn::visit::Visit<'ast> for ColumnDefinitionAudit<'_> {
        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            let canonical = self.source.path == EBD_4B2B_OWNER
                && self.inline_modules.is_empty()
                && item.ident == "ColumnDef";
            if item.ident == "ColumnDef" && !canonical {
                self.violations.insert(format!(
                    "catalog-column-secondary-definition: {}|struct|ColumnDef",
                    self.source.path
                ));
            }
            if !canonical
                && item.fields.len() == 1
                && item.fields.iter().any(|field| {
                    ebd_4b2b_type_contains_canonical_column_def(
                        &field.ty,
                        self.source,
                        self.aliases,
                        &self.inline_modules,
                    )
                })
            {
                self.violations.insert(format!(
                    "catalog-column-forwarding-reexport: {}|wrapper|{}",
                    self.source.path, item.ident
                ));
            }
            syn::visit::visit_item_struct(self, item);
        }
        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            if item.ident == "ColumnDef" {
                self.violations.insert(format!(
                    "catalog-column-secondary-definition: {}|enum|ColumnDef",
                    self.source.path
                ));
            }
            syn::visit::visit_item_enum(self, item);
        }
        fn visit_item_union(&mut self, item: &'ast syn::ItemUnion) {
            if item.ident == "ColumnDef" {
                self.violations.insert(format!(
                    "catalog-column-secondary-definition: {}|union|ColumnDef",
                    self.source.path
                ));
            }
            syn::visit::visit_item_union(self, item);
        }
        fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
            if item.ident == "ColumnDef" {
                self.violations.insert(format!(
                    "catalog-column-secondary-definition: {}|trait|ColumnDef",
                    self.source.path
                ));
            }
            syn::visit::visit_item_trait(self, item);
        }
        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            if item.ident == "ColumnDef"
                || ebd_4b2b_type_contains_canonical_column_def(
                    &item.ty,
                    self.source,
                    self.aliases,
                    &self.inline_modules,
                )
            {
                self.violations.insert(format!(
                    "catalog-column-forwarding-reexport: {}|type-alias|{}",
                    self.source.path, item.ident
                ));
            }
            syn::visit::visit_item_type(self, item);
        }
        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            if ebd_4b2b_macro_defines_column_def(item) || ebd_4b2b_macro_invokes_column_def(item) {
                self.violations.insert(format!(
                    "catalog-column-secondary-definition: {}|macro|ColumnDef",
                    self.source.path
                ));
            }
        }
        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if item.ident == "ColumnDef" {
                self.violations.insert(format!(
                    "catalog-column-secondary-definition: {}|module|ColumnDef",
                    self.source.path
                ));
            }
            let Some((_, items)) = &item.content else {
                return;
            };
            self.inline_modules.push(item.ident.to_string());
            for item in items {
                syn::visit::Visit::visit_item(self, item);
            }
            self.inline_modules.pop();
        }
    }

    let mut violations = BTreeSet::new();
    for source in sources {
        for path in remove_redundant_descendant_paths(
            rust_all_source_canonical_paths(&source.text, &source.path)
                .into_iter()
                .filter(|path| ebd_4b2b_is_legacy_column_def_path(path))
                .collect(),
        ) {
            violations.insert(format!(
                "catalog-column-legacy-path: {}|{}",
                source.path,
                path.join("::")
            ));
        }
        let Ok(file) = syn::parse_file(&source.text) else {
            violations.insert(format!(
                "catalog-column-secondary-definition: {}|parse-failed",
                source.path
            ));
            continue;
        };
        let (imports, aliases) = ebd_4b1_module_scope_inputs(&file);
        let mut audit = ColumnDefinitionAudit {
            source,
            aliases: &aliases,
            inline_modules: Vec::new(),
            violations: BTreeSet::new(),
        };
        syn::visit::Visit::visit_file(&mut audit, &file);
        violations.extend(audit.violations);

        if source.path == EBD_4B2B_OWNER {
            continue;
        }
        for import in imports {
            let Some(resolved) = resolve_forwarding_paths(
                &import.segments,
                &source.path,
                &import.inline_modules,
                &aliases,
                &mut BTreeSet::new(),
                0,
            ) else {
                continue;
            };
            for target in resolved {
                let Some(canonical) = rust_canonical_path_segments_in_scope(
                    &target.segments,
                    &source.path,
                    &target.inline_modules,
                ) else {
                    continue;
                };
                if import.visibility != "private"
                    && (ebd_4b2b_is_canonical_schema_forward_target(&canonical)
                        || ebd_4b2b_is_legacy_column_def_path(&canonical))
                {
                    violations.insert(format!(
                        "catalog-column-forwarding-reexport: {}|{}|{}",
                        source.path,
                        import.visibility,
                        canonical.join("::")
                    ));
                } else if import
                    .segments
                    .first()
                    .is_some_and(|segment| segment == "crate")
                    && ebd_4b2b_is_legacy_column_def_glob(&canonical)
                {
                    violations.insert(format!(
                        "catalog-column-legacy-path: {}|{}",
                        source.path,
                        canonical.join("::")
                    ));
                }
            }
        }
    }
    violations
}

#[test]
fn ebd_4b2b_detector_requires_exact_column_def_owner() {
    let valid = GuardSource::new(
        EBD_4B2B_OWNER,
        r#"
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
    pub write_default: Option<ColumnDefault>,
    pub logical_type: Option<SqlType>,
}
"#,
    );
    assert!(
        ebd_4b2b_audit_column_def_owner(&valid).is_empty(),
        "exact ColumnDef owner fixture must pass"
    );

    for (from, to) in [
        ("pub struct ColumnDef", "pub(crate) struct ColumnDef"),
        (
            "#[derive(Clone, Debug, PartialEq)]",
            "#[derive(Clone, Debug, PartialEq, Eq)]",
        ),
        ("pub struct ColumnDef", "#[repr(C)]\npub struct ColumnDef"),
        ("pub struct ColumnDef", "#[cfg(test)]\npub struct ColumnDef"),
        ("pub struct ColumnDef", "pub struct ColumnDef<T>"),
        ("pub name: String", "name: String"),
        (
            "pub name: String,\n    pub data_type: arrow::datatypes::DataType,",
            "pub data_type: arrow::datatypes::DataType,\n    pub name: String,",
        ),
    ] {
        let invalid = GuardSource::new(EBD_4B2B_OWNER, &valid.text.replacen(from, to, 1));
        assert!(
            !ebd_4b2b_audit_column_def_owner(&invalid).is_empty(),
            "owner mutation was missed: {from:?} -> {to:?}"
        );
    }
}

#[test]
fn ebd_4b2b_detector_rejects_arrow_shadow_and_noncanonical_fields() {
    let valid = GuardSource::new(
        EBD_4B2B_OWNER,
        r#"
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: std::string::String,
    pub data_type: ::arrow::datatypes::DataType,
    pub nullable: bool,
    pub write_default: core::option::Option<ColumnDefault>,
    pub logical_type: std::option::Option<SqlType>,
}
"#,
    );
    assert!(ebd_4b2b_audit_column_def_owner(&valid).is_empty());
    assert!(ebd_4b2b_audit_schema_dependencies(&valid).is_empty());

    for text in [
        r#"
type String = Vec<u8>;
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
struct Option<T>(T);
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
mod std { pub mod string { pub struct String; } }
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: std::string::String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
mod alloc { pub mod string { pub struct String; } }
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: alloc::string::String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
mod core { pub mod option { pub struct Option<T>(T); } }
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: core::option::Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
mod arrow { pub mod datatypes { pub struct DataType; } }
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
type arrow = std::marker::PhantomData<()>;
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
extern crate arrow;
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
extern crate self as arrow;
pub mod datatypes { pub struct DataType; }
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
use arrow::datatypes::DataType as ArrowType;
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: ArrowType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
use arrow::datatypes::*;
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
use arrow::{datatypes::DataType, array::ArrayRef};
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<crate::catalog::schema::ColumnDefault>, pub logical_type: Option<SqlType> }
"#,
        r#"
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }
fn forbidden(_: arrow::array::ArrayRef) {}
"#,
    ] {
        let invalid = GuardSource::new(EBD_4B2B_OWNER, text);
        let mut violations = ebd_4b2b_audit_column_def_owner(&invalid);
        violations.extend(ebd_4b2b_audit_schema_dependencies(&invalid));
        assert!(
            !violations.is_empty(),
            "noncanonical field or Arrow escape was missed: {text}"
        );
    }
}

#[test]
fn ebd_4b2b_detector_rejects_legacy_paths_forwarding_and_ignores_proto_noise() {
    let allowed = [
        GuardSource::new(
            "src/sql/consumer.rs",
            r###"
use crate::catalog::schema::ColumnDef;
fn consume(value: ColumnDef) { let _ = value; }
const TEXT: &str = "crate::sql::catalog::ColumnDef";
const RAW: &str = r#"pub use crate::engine::ColumnDef"#;
// struct ColumnDef;
"###,
        ),
        GuardSource::new(
            "src/lower/novarocks/scan/common.rs",
            "fn encode(value: plan::ColumnDef) { let _ = value; }",
        ),
        GuardSource::new(
            "src/sql/parser/ast/ddl.rs",
            "pub struct TableColumnDef { pub name: String }",
        ),
        GuardSource::new(
            "src/sql/catalog.rs",
            "pub struct IcebergSchemaFieldDef { pub children: Vec<IcebergSchemaFieldDef> }",
        ),
        GuardSource::new(
            EBD_4B2B_OWNER,
            "#[derive(Clone, Debug, PartialEq)] pub struct ColumnDef { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool, pub write_default: Option<ColumnDefault>, pub logical_type: Option<SqlType> }",
        ),
    ];
    assert!(
        ebd_4b2b_audit_legacy_paths_and_forwarding(&allowed).is_empty(),
        "canonical consumers and same-name protocol/parser noise must pass"
    );

    let invalid = [
        GuardSource::new(
            "src/sql/direct.rs",
            "fn consume(_: crate::sql::catalog::ColumnDef) {}",
        ),
        GuardSource::new(
            "src/sql/alias.rs",
            "use crate::engine::ColumnDef as OldColumn; fn consume(_: OldColumn) {}",
        ),
        GuardSource::new(
            "src/sql/module_alias.rs",
            "use crate::sql::catalog; fn consume(_: catalog::ColumnDef) {}",
        ),
        GuardSource::new(
            "src/sql/glob.rs",
            "use crate::engine::catalog::*; fn consume(_: ColumnDef) {}",
        ),
        GuardSource::new(
            "src/sql/relative.rs",
            "fn consume(_: super::catalog::ColumnDef) {}",
        ),
        GuardSource::new(
            "src/sql/forward.rs",
            "pub use crate::catalog::schema::ColumnDef;",
        ),
        GuardSource::new(
            "src/sql/forward_glob.rs",
            "pub use crate::catalog::schema::*;",
        ),
        GuardSource::new(
            "src/sql/forward_module.rs",
            "pub use crate::catalog::schema as model;",
        ),
        GuardSource::new(
            "src/sql/alias_owner.rs",
            "type OldColumn = crate::catalog::schema::ColumnDef;",
        ),
        GuardSource::new(
            "src/sql/wrapper.rs",
            "struct ColumnEnvelope(crate::catalog::schema::ColumnDef);",
        ),
        GuardSource::new(
            "src/sql/named_wrapper.rs",
            "struct ColumnEnvelope { value: crate::catalog::schema::ColumnDef }",
        ),
        GuardSource::new(
            "src/sql/secondary.rs",
            "#[cfg(test)] mod tests { pub struct ColumnDef; }",
        ),
        GuardSource::new(
            "src/sql/macro_owner.rs",
            "macro_rules! owner { () => { struct ColumnDef; } } owner!();",
        ),
        GuardSource::new(
            "src/sql/macro_parameter_owner.rs",
            "macro_rules! owner { ($name:ident) => { struct $name; } } owner!(ColumnDef);",
        ),
    ];
    let violations = ebd_4b2b_audit_legacy_paths_and_forwarding(&invalid);
    for path in [
        "direct.rs",
        "alias.rs",
        "module_alias.rs",
        "glob.rs",
        "relative.rs",
        "forward.rs",
        "forward_glob.rs",
        "forward_module.rs",
        "alias_owner.rs",
        "wrapper.rs",
        "named_wrapper.rs",
        "secondary.rs",
        "macro_owner.rs",
        "macro_parameter_owner.rs",
    ] {
        assert!(
            violations.iter().any(|violation| violation.contains(path)),
            "legacy or secondary fixture was missed: {path}; got {violations:?}"
        );
    }
}

#[test]
fn ebd_4b2b_catalog_column_def_owner_is_complete() {
    let sources = ebd_4b1_collect_repo_sources();
    let mut violations = BTreeSet::new();
    if let Some(owner) = sources.iter().find(|source| source.path == EBD_4B2B_OWNER) {
        violations.extend(ebd_4b2b_audit_column_def_owner(owner));
        violations.extend(ebd_4b2b_audit_schema_dependencies(owner));
    } else {
        violations.insert(format!("catalog-column-owner-missing: {EBD_4B2B_OWNER}"));
    }
    violations.extend(ebd_4b2b_audit_legacy_paths_and_forwarding(&sources));

    for (source, dependency) in [
        (
            "src/connector/iceberg/catalog/registry.rs",
            "crate::engine::catalog::ColumnDef",
        ),
        (
            "src/connector/starrocks/table/catalog.rs",
            "crate::engine::catalog::ColumnDef",
        ),
        (
            "src/connector/starrocks/table/txn.rs",
            "crate::engine::catalog::ColumnDef",
        ),
    ] {
        if EXTERNAL_ENGINE_DEPENDENCIES
            .iter()
            .any(|(path, dependencies)| *path == source && dependencies.contains(&dependency))
        {
            violations.insert(format!(
                "catalog-column-external-engine-dependency: {source}|{dependency}"
            ));
        }
    }

    let actual = current_source_tree_snapshot();
    let actual_counts = [
        actual.engine_files.len(),
        actual.engine_module_declarations.len(),
        actual
            .external_engine_dependencies
            .values()
            .map(BTreeSet::len)
            .sum(),
        actual
            .standalone_state_dependencies
            .values()
            .map(BTreeSet::len)
            .sum(),
        actual.forwarding_reexports.len(),
    ];
    let expected_counts = [76, 74, 210, 55, 0];
    if actual_counts != expected_counts {
        violations.insert(format!(
            "catalog-column-ebd-1-baseline-drift: expected={expected_counts:?} actual={actual_counts:?}"
        ));
    }

    assert!(
        violations.is_empty(),
        "EBD-4B2B catalog ColumnDef owner failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

const EBD_4B3A_CONNECTOR_CATALOG: &str = "src/connector/starrocks/table/catalog.rs";
const EBD_4B3A_SCAN_PLANNER: &str = "src/connector/starrocks/table/scan_planner.rs";
const EBD_4B3A_GLOBAL_LEGACY_IDENTIFIERS: &[&str] = &[
    "PhysicalTableLayout",
    "StarRocksTabletRef",
    "starrocks_table_physical_layout",
];
const EBD_4B3A_CATALOG_SHADOW_IDENTIFIERS: &[&str] = &[
    "get_physical_layout",
    "physical_layouts",
    "register_starrocks_table",
];

fn ebd_4b3a_identifiers(source: &GuardSource) -> BTreeSet<String> {
    rust_source_tokens(&source.text)
        .into_iter()
        .filter(|token| {
            token
                .text
                .as_bytes()
                .first()
                .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
        })
        .map(|token| token.text)
        .collect()
}

fn ebd_4b3a_is_pub_super(visibility: &syn::Visibility) -> bool {
    matches!(
        visibility,
        syn::Visibility::Restricted(restricted)
            if restricted.path.is_ident("super")
    )
}

fn ebd_4b3a_is_i64(ty: &syn::Type) -> bool {
    matches!(
        ty,
        syn::Type::Path(path)
            if path.qself.is_none()
                && path.path.segments.len() == 1
                && path.path.is_ident("i64")
    )
}

fn ebd_4b3a_is_runtime_reference(ty: &syn::Type) -> bool {
    let syn::Type::Reference(reference) = ty else {
        return false;
    };
    let syn::Type::Path(path) = reference.elem.as_ref() else {
        return false;
    };
    reference.mutability.is_none()
        && path.qself.is_none()
        && path.path.segments.len() == 1
        && path.path.is_ident("StarRocksTableRuntime")
}

fn ebd_4b3a_is_scan_tablet_vec(ty: &syn::Type) -> bool {
    let syn::Type::Path(path) = ty else {
        return false;
    };
    if path.qself.is_some()
        || !(path.path.segments.len() == 1
            || ebd_4b3a_path_is_exact(&path.path, &["std", "vec", "Vec"]))
    {
        return false;
    }
    let Some(segment) = path.path.segments.last() else {
        return false;
    };
    if segment.ident != "Vec" {
        return false;
    }
    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return false;
    };
    let mut args = arguments.args.iter();
    let Some(syn::GenericArgument::Type(syn::Type::Path(inner))) = args.next() else {
        return false;
    };
    args.next().is_none()
        && inner.qself.is_none()
        && inner.path.segments.len() == 1
        && inner.path.is_ident("StarRocksScanTablet")
}

fn ebd_4b3a_defines_scan_tablet_selector(source: &GuardSource) -> bool {
    let Ok(file) = syn::parse_file(&source.text) else {
        return false;
    };
    struct DefinitionVisitor {
        found: bool,
    }
    impl<'ast> syn::visit::Visit<'ast> for DefinitionVisitor {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            self.found |= item.sig.ident == "starrocks_scan_tablets";
            syn::visit::visit_item_fn(self, item);
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            self.found |= item.sig.ident == "starrocks_scan_tablets";
            syn::visit::visit_impl_item_fn(self, item);
        }

        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            self.found |= item
                .mac
                .tokens
                .to_string()
                .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
                .any(|token| token == "starrocks_scan_tablets");
            syn::visit::visit_item_macro(self, item);
        }
    }
    let mut visitor = DefinitionVisitor { found: false };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    visitor.found
}

fn ebd_4b3a_audit_legacy_catalog_layout(sources: &[GuardSource]) -> BTreeSet<String> {
    let mut violations = BTreeSet::new();
    for source in sources
        .iter()
        .filter(|source| source.path.starts_with("src/") || source.path.starts_with("tests/"))
    {
        let identifiers = ebd_4b3a_identifiers(source);
        for legacy in EBD_4B3A_GLOBAL_LEGACY_IDENTIFIERS {
            if identifiers.contains(*legacy) {
                violations.insert(format!(
                    "catalog-physical-layout-legacy-surface: {}|{}",
                    source.path, legacy
                ));
            }
        }
        if source.path == "src/sql/catalog.rs" || source.path.starts_with("src/engine/") {
            for legacy in EBD_4B3A_CATALOG_SHADOW_IDENTIFIERS {
                if identifiers.contains(*legacy) {
                    violations.insert(format!(
                        "catalog-physical-layout-shadow-surface: {}|{}",
                        source.path, legacy
                    ));
                }
            }
        }
        if source.path != EBD_4B3A_CONNECTOR_CATALOG && identifiers.contains("StarRocksScanTablet")
        {
            violations.insert(format!(
                "catalog-physical-layout-scan-tablet-owner-escape: {}",
                source.path
            ));
        }
        if !source.path.starts_with("src/connector/starrocks/table/")
            && identifiers.contains("starrocks_scan_tablets")
        {
            violations.insert(format!(
                "catalog-physical-layout-selector-owner-escape: {}",
                source.path
            ));
        }
        if source.path != EBD_4B3A_CONNECTOR_CATALOG
            && ebd_4b3a_defines_scan_tablet_selector(source)
        {
            violations.insert(format!(
                "catalog-physical-layout-selector-definition-escape: {}",
                source.path
            ));
        }
    }
    violations
}

fn ebd_4b3a_audit_connector_owner(source: &GuardSource) -> BTreeSet<String> {
    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "catalog-physical-layout-connector-owner: {}|parse-failed",
            source.path
        )]);
    };

    #[derive(Default)]
    struct OwnerVisitor<'ast> {
        structs: Vec<&'ast syn::ItemStruct>,
        functions: Vec<&'ast syn::ItemFn>,
        impl_function_count: usize,
        macro_owner_mention: bool,
    }

    impl<'ast> syn::visit::Visit<'ast> for OwnerVisitor<'ast> {
        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            if item.ident == "StarRocksScanTablet" {
                self.structs.push(item);
            }
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if item.sig.ident == "starrocks_scan_tablets" {
                self.functions.push(item);
            }
            syn::visit::visit_item_fn(self, item);
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if item.sig.ident == "starrocks_scan_tablets" {
                self.impl_function_count += 1;
            }
            syn::visit::visit_impl_item_fn(self, item);
        }

        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            self.macro_owner_mention |= item
                .mac
                .tokens
                .to_string()
                .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
                .any(|token| matches!(token, "StarRocksScanTablet" | "starrocks_scan_tablets"));
            syn::visit::visit_item_macro(self, item);
        }
    }

    let mut owner = OwnerVisitor::default();
    syn::visit::Visit::visit_file(&mut owner, &file);
    let mut violations = BTreeSet::new();
    if owner.macro_owner_mention {
        violations.insert(format!(
            "catalog-physical-layout-connector-owner: {}|macro-owner-mention",
            source.path
        ));
    }
    if owner.structs.len() != 1 {
        violations.insert(format!(
            "catalog-physical-layout-connector-owner: {}|StarRocksScanTablet-count={}",
            source.path,
            owner.structs.len()
        ));
    } else {
        let item = owner.structs[0];
        let expected_derives = BTreeSet::from([
            "Clone".to_string(),
            "Debug".to_string(),
            "Eq".to_string(),
            "PartialEq".to_string(),
        ]);
        let syn::Fields::Named(fields) = &item.fields else {
            violations.insert(format!(
                "catalog-physical-layout-connector-owner: {}|named-fields-required",
                source.path
            ));
            return violations;
        };
        let expected_fields = BTreeSet::from([
            "tablet_id".to_string(),
            "partition_id".to_string(),
            "version".to_string(),
        ]);
        let actual_names = fields
            .named
            .iter()
            .filter_map(|field| field.ident.as_ref().map(ToString::to_string))
            .collect::<BTreeSet<_>>();
        if !ebd_4b3a_is_pub_super(&item.vis)
            || !ebd_4b2b_struct_derive_set(item)
                .is_some_and(|derives| derives.is_superset(&expected_derives))
            || actual_names != expected_fields
            || fields.named.len() != expected_fields.len()
            || fields
                .named
                .iter()
                .any(|field| !ebd_4b3a_is_pub_super(&field.vis) || !ebd_4b3a_is_i64(&field.ty))
        {
            violations.insert(format!(
                "catalog-physical-layout-connector-owner: {}|StarRocksScanTablet-shape",
                source.path
            ));
        }
    }

    let function_count = owner.functions.len() + owner.impl_function_count;
    if function_count != 1 {
        violations.insert(format!(
            "catalog-physical-layout-connector-owner: {}|starrocks_scan_tablets-count={}",
            source.path, function_count
        ));
    } else {
        let Some(function) = owner.functions.first().copied() else {
            violations.insert(format!(
                "catalog-physical-layout-connector-owner: {}|starrocks_scan_tablets-free-function-required",
                source.path
            ));
            return violations;
        };
        let valid_input = function.sig.inputs.len() == 1
            && matches!(
                function.sig.inputs.first(),
                Some(syn::FnArg::Typed(argument)) if ebd_4b3a_is_runtime_reference(&argument.ty)
            );
        let valid_output = matches!(
            &function.sig.output,
            syn::ReturnType::Type(_, ty) if ebd_4b3a_is_scan_tablet_vec(ty)
        );
        if !ebd_4b3a_is_pub_super(&function.vis)
            || !function.sig.generics.params.is_empty()
            || function.sig.generics.where_clause.is_some()
            || !valid_input
            || !valid_output
        {
            violations.insert(format!(
                "catalog-physical-layout-connector-owner: {}|starrocks_scan_tablets-signature",
                source.path
            ));
        }
    }
    violations
}

fn ebd_4b3a_path_ends_with(path: &syn::Path, suffix: &[&str]) -> bool {
    path.segments.len() >= suffix.len()
        && path
            .segments
            .iter()
            .rev()
            .zip(suffix.iter().rev())
            .all(|(segment, expected)| segment.ident == *expected)
}

fn ebd_4b3a_path_is_exact(path: &syn::Path, segments: &[&str]) -> bool {
    path.segments.len() == segments.len()
        && path
            .segments
            .iter()
            .zip(segments)
            .all(|(segment, expected)| segment.ident == *expected)
}

fn ebd_4b3a_is_runtime_selector_call(expr: &syn::Expr) -> bool {
    let syn::Expr::Call(call) = expr else {
        return false;
    };
    let syn::Expr::Path(function) = call.func.as_ref() else {
        return false;
    };
    let Some(syn::Expr::Path(argument)) = call.args.first() else {
        return false;
    };
    call.args.len() == 1
        && ebd_4b3a_path_is_exact(
            &function.path,
            &["super", "catalog", "starrocks_scan_tablets"],
        )
        && argument.path.is_ident("runtime")
}

fn ebd_4b3a_closure_maps_all_split_fields(closure: &syn::ExprClosure) -> bool {
    let Some(syn::Pat::Ident(parameter)) = closure.inputs.first() else {
        return false;
    };
    if closure.inputs.len() != 1 {
        return false;
    }
    let returned = match closure.body.as_ref() {
        syn::Expr::Block(block) => match block.block.stmts.last() {
            Some(syn::Stmt::Expr(expr, None)) => expr,
            _ => return false,
        },
        expr => expr,
    };
    let syn::Expr::Call(call) = returned else {
        return false;
    };
    let syn::Expr::Path(function) = call.func.as_ref() else {
        return false;
    };
    let Some(syn::Expr::Path(connector_id)) = call.args.first() else {
        return false;
    };
    let Some(syn::Expr::Struct(split)) = call.args.iter().nth(1) else {
        return false;
    };
    let expected = ["tablet_id", "partition_id", "version"];
    call.args.len() == 2
        && ebd_4b3a_path_is_exact(&function.path, &["Split", "new"])
        && connector_id.path.is_ident("CONNECTOR_ID")
        && ebd_4b3a_path_is_exact(&split.path, &["StarRocksSplit"])
        && split.rest.is_none()
        && split.fields.len() == expected.len()
        && expected.iter().all(|expected_field| {
            split.fields.iter().any(|field| {
                let syn::Member::Named(member) = &field.member else {
                    return false;
                };
                let syn::Expr::Field(value) = &field.expr else {
                    return false;
                };
                let syn::Expr::Path(base) = value.base.as_ref() else {
                    return false;
                };
                let syn::Member::Named(value_member) = &value.member else {
                    return false;
                };
                member == expected_field
                    && value_member == expected_field
                    && base.path.is_ident(&parameter.ident)
            })
        })
}

fn ebd_4b3a_plan_splits_uses_live_selector(block: &syn::Block) -> bool {
    #[derive(Default)]
    struct BindingAudit {
        pattern_counts: BTreeMap<String, usize>,
        assigned: BTreeSet<String>,
    }

    impl<'ast> syn::visit::Visit<'ast> for BindingAudit {
        fn visit_pat_ident(&mut self, item: &'ast syn::PatIdent) {
            *self
                .pattern_counts
                .entry(item.ident.to_string())
                .or_default() += 1;
            syn::visit::visit_pat_ident(self, item);
        }

        fn visit_expr_assign(&mut self, item: &'ast syn::ExprAssign) {
            if let syn::Expr::Path(left) = item.left.as_ref()
                && let Some(ident) = left.path.get_ident()
            {
                self.assigned.insert(ident.to_string());
            }
            syn::visit::visit_expr_assign(self, item);
        }

        fn visit_expr_binary(&mut self, item: &'ast syn::ExprBinary) {
            if matches!(
                item.op,
                syn::BinOp::AddAssign(_)
                    | syn::BinOp::SubAssign(_)
                    | syn::BinOp::MulAssign(_)
                    | syn::BinOp::DivAssign(_)
                    | syn::BinOp::RemAssign(_)
                    | syn::BinOp::BitXorAssign(_)
                    | syn::BinOp::BitAndAssign(_)
                    | syn::BinOp::BitOrAssign(_)
                    | syn::BinOp::ShlAssign(_)
                    | syn::BinOp::ShrAssign(_)
            ) && let syn::Expr::Path(left) = item.left.as_ref()
                && let Some(ident) = left.path.get_ident()
            {
                self.assigned.insert(ident.to_string());
            }
            syn::visit::visit_expr_binary(self, item);
        }
    }

    let mut binding_audit = BindingAudit::default();
    syn::visit::Visit::visit_block(&mut binding_audit, block);
    let mut binding_counts = BTreeMap::<String, usize>::new();
    let mut selector_bindings = BTreeSet::new();
    for statement in &block.stmts {
        let syn::Stmt::Local(local) = statement else {
            continue;
        };
        let syn::Pat::Ident(binding) = &local.pat else {
            continue;
        };
        if binding.mutability.is_some() || binding.by_ref.is_some() || binding.subpat.is_some() {
            continue;
        }
        let name = binding.ident.to_string();
        *binding_counts.entry(name.clone()).or_default() += 1;
        if local
            .init
            .as_ref()
            .is_some_and(|init| ebd_4b3a_is_runtime_selector_call(init.expr.as_ref()))
        {
            selector_bindings.insert(name);
        }
    }
    selector_bindings.retain(|name| {
        binding_counts.get(name) == Some(&1)
            && binding_audit.pattern_counts.get(name) == Some(&1)
            && !binding_audit.assigned.contains(name)
    });

    struct ReturnAudit {
        count: usize,
    }
    impl<'ast> syn::visit::Visit<'ast> for ReturnAudit {
        fn visit_expr_return(&mut self, item: &'ast syn::ExprReturn) {
            self.count += 1;
            syn::visit::visit_expr_return(self, item);
        }

        fn visit_expr_closure(&mut self, _item: &'ast syn::ExprClosure) {}

        fn visit_item_fn(&mut self, _item: &'ast syn::ItemFn) {}
    }
    let tail_is_explicit_return = matches!(
        block.stmts.last(),
        Some(syn::Stmt::Expr(syn::Expr::Return(_), None))
    );
    let mut return_audit = ReturnAudit { count: 0 };
    syn::visit::Visit::visit_block(&mut return_audit, block);
    if return_audit.count != usize::from(tail_is_explicit_return) {
        return false;
    }

    let returned = match block.stmts.last() {
        Some(syn::Stmt::Expr(syn::Expr::Return(returned), None)) => {
            let Some(expr) = returned.expr.as_deref() else {
                return false;
            };
            expr
        }
        Some(syn::Stmt::Expr(expr, None)) => expr,
        _ => return false,
    };
    let syn::Expr::Call(ok) = returned else {
        return false;
    };
    let syn::Expr::Path(ok_path) = ok.func.as_ref() else {
        return false;
    };
    let Some(syn::Expr::MethodCall(collect)) = ok.args.first() else {
        return false;
    };
    let syn::Expr::MethodCall(map) = collect.receiver.as_ref() else {
        return false;
    };
    let syn::Expr::MethodCall(into_iter) = map.receiver.as_ref() else {
        return false;
    };
    let selector_value = ebd_4b3a_is_runtime_selector_call(into_iter.receiver.as_ref())
        || matches!(
            into_iter.receiver.as_ref(),
            syn::Expr::Path(binding)
                if binding.path.get_ident().is_some_and(|ident| {
                    selector_bindings.contains(&ident.to_string())
                })
        );
    ok.args.len() == 1
        && ebd_4b3a_path_is_exact(&ok_path.path, &["Ok"])
        && collect.method == "collect"
        && collect.args.is_empty()
        && map.method == "map"
        && map.args.len() == 1
        && into_iter.method == "into_iter"
        && into_iter.args.is_empty()
        && selector_value
        && matches!(
            map.args.first(),
            Some(syn::Expr::Closure(closure))
                if ebd_4b3a_closure_maps_all_split_fields(closure)
        )
}

fn ebd_4b3a_audit_scan_planner(source: &GuardSource) -> BTreeSet<String> {
    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "catalog-physical-layout-scan-planner-binding: {}|parse-failed",
            source.path
        )]);
    };

    struct PlannerVisitor {
        plan_splits_count: usize,
        valid_plan_splits_count: usize,
    }

    impl<'ast> syn::visit::Visit<'ast> for PlannerVisitor {
        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            let connector_impl = item.trait_.as_ref().is_some_and(|(_, path, _)| {
                ebd_4b3a_path_ends_with(path, &["ConnectorScanPlanner"])
            }) && matches!(
                item.self_ty.as_ref(),
                syn::Type::Path(path)
                    if ebd_4b3a_path_ends_with(&path.path, &["StarRocksTableScanPlanner"])
            );
            if connector_impl {
                for impl_item in &item.items {
                    let syn::ImplItem::Fn(function) = impl_item else {
                        continue;
                    };
                    if function.sig.ident == "plan_splits" {
                        self.plan_splits_count += 1;
                        if ebd_4b3a_plan_splits_uses_live_selector(&function.block) {
                            self.valid_plan_splits_count += 1;
                        }
                    }
                }
            }
            syn::visit::visit_item_impl(self, item);
        }
    }

    let mut visitor = PlannerVisitor {
        plan_splits_count: 0,
        valid_plan_splits_count: 0,
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    if visitor.plan_splits_count == 1 && visitor.valid_plan_splits_count == 1 {
        BTreeSet::new()
    } else {
        BTreeSet::from([format!(
            "catalog-physical-layout-scan-planner-binding: {}|plan_splits={} valid={}",
            source.path, visitor.plan_splits_count, visitor.valid_plan_splits_count
        )])
    }
}

#[test]
fn ebd_4b3a_detector_rejects_catalog_physical_layout_state() {
    let allowed = [
        GuardSource::new(
            "src/sql/catalog.rs",
            r###"
// PhysicalTableLayout get_physical_layout physical_layouts
const TEXT: &str = "StarRocksTabletRef register_starrocks_table";
pub enum ScanSource { StarRocks { db_id: i64, table_id: i64 } }
"###,
        ),
        GuardSource::new(
            EBD_4B3A_CONNECTOR_CATALOG,
            "pub(super) struct StarRocksScanTablet { pub(super) tablet_id: i64, pub(super) partition_id: i64, pub(super) version: i64 }",
        ),
        GuardSource::new(
            "src/coordinator/layout.rs",
            "struct LayoutCache { physical_layouts: usize } fn get_physical_layout() {} fn register_starrocks_table() {}",
        ),
    ];
    assert!(ebd_4b3a_audit_legacy_catalog_layout(&allowed).is_empty());

    for source in [
        GuardSource::new(
            "src/sql/catalog.rs",
            "#[cfg(test)] mod tests { struct PhysicalTableLayout; }",
        ),
        GuardSource::new(
            "src/engine/catalog.rs",
            "type Layout = StarRocksTabletRef; struct Db { physical_layouts: usize }",
        ),
        GuardSource::new(
            "src/engine/catalog.rs",
            "macro_rules! legacy { () => { fn get_physical_layout() {} } } legacy!();",
        ),
        GuardSource::new(
            "src/sql/forward.rs",
            "pub use crate::connector::starrocks::table::catalog::StarRocksScanTablet;",
        ),
        GuardSource::new("src/engine/selector.rs", "fn starrocks_scan_tablets() {}"),
        GuardSource::new(
            EBD_4B3A_SCAN_PLANNER,
            "fn starrocks_scan_tablets(runtime: &Runtime) { let _ = runtime; }",
        ),
        GuardSource::new(
            EBD_4B3A_SCAN_PLANNER,
            "macro_rules! duplicate { () => { fn starrocks_scan_tablets() {} } }",
        ),
    ] {
        assert!(
            !ebd_4b3a_audit_legacy_catalog_layout(&[source.clone()]).is_empty(),
            "legacy fixture was missed: {}",
            source.path
        );
    }
}

#[test]
fn ebd_4b3a_detector_requires_connector_owned_scan_tablet_selection() {
    let owner = GuardSource::new(
        EBD_4B3A_CONNECTOR_CATALOG,
        r#"
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StarRocksScanTablet {
    pub(super) tablet_id: i64,
    pub(super) partition_id: i64,
    pub(super) version: i64,
}
pub(super) fn starrocks_scan_tablets(
    runtime: &StarRocksTableRuntime,
) -> Vec<StarRocksScanTablet> { let _ = runtime; Vec::new() }
"#,
    );
    assert!(ebd_4b3a_audit_connector_owner(&owner).is_empty());
    let equivalent_owner = GuardSource::new(
        EBD_4B3A_CONNECTOR_CATALOG,
        r#"
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(in super) struct StarRocksScanTablet {
    pub(in super) version: i64,
    pub(in super) tablet_id: i64,
    pub(in super) partition_id: i64,
}
pub(in super) fn starrocks_scan_tablets(
    runtime: &StarRocksTableRuntime,
) -> std::vec::Vec<StarRocksScanTablet> { let _ = runtime; std::vec::Vec::new() }
"#,
    );
    assert!(
        ebd_4b3a_audit_connector_owner(&equivalent_owner).is_empty(),
        "semantically equivalent private owner spelling must remain allowed"
    );
    let planner = GuardSource::new(
        EBD_4B3A_SCAN_PLANNER,
        r#"
impl ConnectorScanPlanner for StarRocksTableScanPlanner {
    fn plan_splits(&self) {
        let runtime = runtime();
        Ok(super::catalog::starrocks_scan_tablets(runtime)
            .into_iter()
            .map(|tablet| {
                Split::new(
                    CONNECTOR_ID,
                    StarRocksSplit {
                        tablet_id: tablet.tablet_id,
                        partition_id: tablet.partition_id,
                        version: tablet.version,
                    },
                )
            })
            .collect::<Vec<_>>())
    }
}
"#,
    );
    assert!(ebd_4b3a_audit_scan_planner(&planner).is_empty());
    let extracted_planner = GuardSource::new(
        EBD_4B3A_SCAN_PLANNER,
        r#"
impl ConnectorScanPlanner for StarRocksTableScanPlanner {
    fn plan_splits(&self) {
        let runtime = runtime();
        let tablets = super::catalog::starrocks_scan_tablets(runtime);
        Ok(tablets
            .into_iter()
            .map(|tablet| Split::new(CONNECTOR_ID, StarRocksSplit {
                tablet_id: tablet.tablet_id,
                partition_id: tablet.partition_id,
                version: tablet.version,
            }))
            .collect::<Vec<_>>())
    }
}
"#,
    );
    assert!(
        ebd_4b3a_audit_scan_planner(&extracted_planner).is_empty(),
        "a single immutable selector binding must remain allowed"
    );

    for invalid in [
        owner
            .text
            .replacen("pub(super) struct", "pub(crate) struct", 1),
        owner
            .text
            .replacen("pub(super) tablet_id", "pub(crate) tablet_id", 1),
        owner.text.replacen("version: i64", "version: u64", 1),
        owner.text.replacen("pub(super) fn", "pub(crate) fn", 1),
        owner.text.replacen(
            "Vec<StarRocksScanTablet>",
            "Result<Vec<StarRocksScanTablet>, String>",
            1,
        ),
        format!("{0}\nmod duplicate {{ {0} }}", owner.text),
        format!(
            "{}\nmacro_rules! duplicate {{ () => {{ struct StarRocksScanTablet; fn starrocks_scan_tablets() {{}} }} }}",
            owner.text
        ),
        equivalent_owner.text.replace(
            "std::vec::Vec<StarRocksScanTablet>",
            "custom::Vec<StarRocksScanTablet>",
        ),
    ] {
        assert!(
            !ebd_4b3a_audit_connector_owner(&GuardSource::new(
                EBD_4B3A_CONNECTOR_CATALOG,
                &invalid,
            ))
            .is_empty(),
            "connector owner mutation was missed: {invalid}"
        );
    }
    assert!(
        !ebd_4b3a_audit_scan_planner(&GuardSource::new(
            EBD_4B3A_SCAN_PLANNER,
            r#"
#[cfg(test)]
fn decoy(runtime: &Runtime) {
    let _ = starrocks_scan_tablets(runtime);
    let _ = StarRocksSplit { tablet_id: 1, partition_id: 2, version: 3 };
}
impl ConnectorScanPlanner for StarRocksTableScanPlanner {
    fn plan_splits(&self) { let _ = runtime.tablets.iter(); }
}
"#,
        ))
        .is_empty()
    );
    assert!(
        !ebd_4b3a_audit_scan_planner(&GuardSource::new(
            EBD_4B3A_SCAN_PLANNER,
            &planner
                .text
                .replace("version: tablet.version", "version: tablet.tablet_id"),
        ))
        .is_empty(),
        "split field rewiring must fail closed"
    );
    assert!(
        !ebd_4b3a_audit_scan_planner(&GuardSource::new(
            EBD_4B3A_SCAN_PLANNER,
            &planner.text.replace(
                "super::catalog::starrocks_scan_tablets",
                "starrocks_scan_tablets"
            ),
        ))
        .is_empty(),
        "a local same-name selector must not satisfy the canonical owner call"
    );
    assert!(
        !ebd_4b3a_audit_scan_planner(&GuardSource::new(
            EBD_4B3A_SCAN_PLANNER,
            r#"
impl ConnectorScanPlanner for StarRocksTableScanPlanner {
    fn plan_splits(&self) {
        let runtime = runtime();
        let _unused = super::catalog::starrocks_scan_tablets(runtime)
            .into_iter()
            .map(|tablet| Split::new(CONNECTOR_ID, StarRocksSplit {
                tablet_id: tablet.tablet_id,
                partition_id: tablet.partition_id,
                version: tablet.version,
            }))
            .collect::<Vec<_>>();
        Ok(fake_tablets().into_iter().map(fake_split).collect::<Vec<_>>())
    }
}
"#,
        ))
        .is_empty(),
        "an unused in-method selector decoy must not satisfy the returned pipeline"
    );
    assert!(
        !ebd_4b3a_audit_scan_planner(&GuardSource::new(
            EBD_4B3A_SCAN_PLANNER,
            &extracted_planner.text.replace(
                "let tablets = super::catalog::starrocks_scan_tablets(runtime);",
                "let mut tablets = super::catalog::starrocks_scan_tablets(runtime); tablets = fake_tablets();",
            ),
        ))
        .is_empty(),
        "a mutable or reassigned selector binding must fail closed"
    );
    assert!(
        !ebd_4b3a_audit_scan_planner(&GuardSource::new(
            EBD_4B3A_SCAN_PLANNER,
            &planner.text.replace(
                "let runtime = runtime();",
                "let runtime = runtime(); if use_fake() { return Ok(fake_splits()); }",
            ),
        ))
        .is_empty(),
        "an early returned split path must fail closed"
    );
}

#[test]
fn ebd_4b3a_catalog_physical_layout_retirement_is_complete() {
    let sources = ebd_4b1_collect_repo_sources();
    let mut violations = ebd_4b3a_audit_legacy_catalog_layout(&sources);
    if let Some(owner) = sources
        .iter()
        .find(|source| source.path == EBD_4B3A_CONNECTOR_CATALOG)
    {
        violations.extend(ebd_4b3a_audit_connector_owner(owner));
    } else {
        violations.insert(format!(
            "catalog-physical-layout-connector-owner-missing: {EBD_4B3A_CONNECTOR_CATALOG}"
        ));
    }
    if let Some(planner) = sources
        .iter()
        .find(|source| source.path == EBD_4B3A_SCAN_PLANNER)
    {
        violations.extend(ebd_4b3a_audit_scan_planner(planner));
    } else {
        violations.insert(format!(
            "catalog-physical-layout-scan-planner-missing: {EBD_4B3A_SCAN_PLANNER}"
        ));
    }

    let actual = current_source_tree_snapshot();
    let actual_counts = [
        actual.engine_files.len(),
        actual.engine_module_declarations.len(),
        actual
            .external_engine_dependencies
            .values()
            .map(BTreeSet::len)
            .sum(),
        actual
            .standalone_state_dependencies
            .values()
            .map(BTreeSet::len)
            .sum(),
        actual.forwarding_reexports.len(),
    ];
    let expected_counts = [76, 74, 210, 55, 0];
    if actual_counts != expected_counts {
        violations.insert(format!(
            "catalog-physical-layout-ebd-1-baseline-drift: expected={expected_counts:?} actual={actual_counts:?}"
        ));
    }

    assert!(
        violations.is_empty(),
        "EBD-4B3A catalog physical layout retirement failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

const EBD_4B3B_PROVIDER: &str = "src/sql/catalog/provider.rs";
const EBD_4B3B_ENGINE: &str = "src/engine/mod.rs";
const EBD_4B3B_SQL_CATALOG: &str = "src/sql/catalog.rs";

fn ebd_4b3b_type_is_table_lookup_mode(ty: &syn::Type) -> bool {
    matches!(
        ty,
        syn::Type::Path(path)
            if path.qself.is_none()
                && path.path.segments.last().is_some_and(|segment| segment.ident == "TableLookupMode")
    )
}

fn ebd_4b3b_path_ends_with(path: &syn::Path, ident: &str) -> bool {
    path.segments
        .last()
        .is_some_and(|segment| segment.ident == ident)
}

fn ebd_4b3b_pattern_mentions_explain(pattern: &syn::Pat) -> bool {
    match pattern {
        syn::Pat::Ident(pattern) => pattern.ident == "Explain",
        syn::Pat::Or(pattern) => pattern.cases.iter().any(ebd_4b3b_pattern_mentions_explain),
        syn::Pat::Path(pattern) => ebd_4b3b_path_ends_with(&pattern.path, "Explain"),
        syn::Pat::Reference(pattern) => ebd_4b3b_pattern_mentions_explain(&pattern.pat),
        syn::Pat::Struct(pattern) => ebd_4b3b_path_ends_with(&pattern.path, "Explain"),
        syn::Pat::Tuple(pattern) => pattern.elems.iter().any(ebd_4b3b_pattern_mentions_explain),
        syn::Pat::TupleStruct(pattern) => {
            ebd_4b3b_path_ends_with(&pattern.path, "Explain")
                || pattern.elems.iter().any(ebd_4b3b_pattern_mentions_explain)
        }
        syn::Pat::Type(pattern) => ebd_4b3b_pattern_mentions_explain(&pattern.pat),
        _ => false,
    }
}

fn ebd_4b3b_explain_arm_reads_full_table_payload(expr: &syn::Expr) -> bool {
    #[derive(Default)]
    struct ExplainPayloadVisitor {
        full_builder_calls: usize,
        files_field_reads: usize,
        macro_mentions: usize,
    }

    impl<'ast> syn::visit::Visit<'ast> for ExplainPayloadVisitor {
        fn visit_expr_call(&mut self, item: &'ast syn::ExprCall) {
            if let syn::Expr::Path(function) = item.func.as_ref()
                && ebd_4b3b_path_ends_with(&function.path, "build_table_def")
            {
                self.full_builder_calls += 1;
            }
            syn::visit::visit_expr_call(self, item);
        }

        fn visit_expr_method_call(&mut self, item: &'ast syn::ExprMethodCall) {
            if item.method == "build_table_def" {
                self.full_builder_calls += 1;
            }
            syn::visit::visit_expr_method_call(self, item);
        }

        fn visit_expr_field(&mut self, item: &'ast syn::ExprField) {
            if matches!(&item.member, syn::Member::Named(member) if member == "files") {
                self.files_field_reads += 1;
            }
            syn::visit::visit_expr_field(self, item);
        }

        fn visit_macro(&mut self, item: &'ast syn::Macro) {
            let tokens = item.tokens.to_string();
            if tokens
                .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
                .any(|token| matches!(token, "build_table_def" | "files"))
            {
                self.macro_mentions += 1;
            }
            syn::visit::visit_macro(self, item);
        }
    }

    let mut visitor = ExplainPayloadVisitor::default();
    syn::visit::Visit::visit_expr(&mut visitor, expr);
    visitor.full_builder_calls > 0 || visitor.files_field_reads > 0 || visitor.macro_mentions > 0
}

fn ebd_4b3b_ordinary_lookup_is_schema_only(block: &syn::Block) -> bool {
    #[derive(Default)]
    struct OrdinaryLookupVisitor {
        schema_only_dispatches: usize,
        neutral_resolution_dispatches: usize,
        other_dispatches: usize,
        full_load_calls: usize,
    }

    impl<'ast> syn::visit::Visit<'ast> for OrdinaryLookupVisitor {
        fn visit_expr_call(&mut self, item: &'ast syn::ExprCall) {
            if let syn::Expr::Path(function) = item.func.as_ref()
                && function.path.segments.last().is_some_and(|segment| {
                    matches!(
                        segment.ident.to_string().as_str(),
                        "build_table_def" | "load_table_for_read"
                    )
                })
            {
                self.full_load_calls += 1;
            }
            syn::visit::visit_expr_call(self, item);
        }

        fn visit_expr_method_call(&mut self, item: &'ast syn::ExprMethodCall) {
            match item.method.to_string().as_str() {
                "build_table_def" | "load_table_for_read" => self.full_load_calls += 1,
                "get_table_with_mode" => {
                    let schema_only = item.args.last().is_some_and(|argument| {
                        matches!(
                            argument,
                            syn::Expr::Path(path)
                                if ebd_4b3b_path_ends_with(&path.path, "SchemaOnly")
                        )
                    });
                    if schema_only {
                        self.schema_only_dispatches += 1;
                    } else {
                        self.other_dispatches += 1;
                    }
                }
                "resolve_table_for_analysis_once" => {
                    self.neutral_resolution_dispatches += 1;
                }
                _ => {}
            }
            syn::visit::visit_expr_method_call(self, item);
        }
    }

    let mut visitor = OrdinaryLookupVisitor::default();
    syn::visit::Visit::visit_block(&mut visitor, block);
    (visitor.schema_only_dispatches == 1 && visitor.neutral_resolution_dispatches == 0
        || visitor.schema_only_dispatches == 0 && visitor.neutral_resolution_dispatches == 1)
        && visitor.other_dispatches == 0
        && visitor.full_load_calls == 0
}

fn ebd_4b3b_resolution_helper_is_metadata_only(block: &syn::Block) -> bool {
    fn unwrap_expr(expr: &syn::Expr) -> &syn::Expr {
        match expr {
            syn::Expr::Group(group) => unwrap_expr(&group.expr),
            syn::Expr::Paren(paren) => unwrap_expr(&paren.expr),
            syn::Expr::Try(try_expr) => unwrap_expr(&try_expr.expr),
            _ => expr,
        }
    }

    fn binding_name(local: &syn::Local) -> Option<String> {
        let syn::Pat::Ident(pattern) = &local.pat else {
            return None;
        };
        Some(pattern.ident.to_string())
    }

    fn method_call<'a>(expr: &'a syn::Expr, method: &str) -> Option<&'a syn::ExprMethodCall> {
        let syn::Expr::MethodCall(call) = unwrap_expr(expr) else {
            return None;
        };
        (call.method == method).then_some(call)
    }

    fn path_ident(expr: &syn::Expr) -> Option<String> {
        let syn::Expr::Path(path) = unwrap_expr(expr) else {
            return None;
        };
        (path.qself.is_none() && path.path.segments.len() == 1)
            .then(|| path.path.segments[0].ident.to_string())
    }

    fn method_count(expr: &syn::Expr, method: &str) -> usize {
        struct MethodVisitor<'a> {
            method: &'a str,
            count: usize,
        }

        impl<'ast> syn::visit::Visit<'ast> for MethodVisitor<'_> {
            fn visit_expr_method_call(&mut self, item: &'ast syn::ExprMethodCall) {
                if item.method == self.method {
                    self.count += 1;
                }
                syn::visit::visit_expr_method_call(self, item);
            }
        }

        let mut visitor = MethodVisitor { method, count: 0 };
        syn::visit::Visit::visit_expr(&mut visitor, expr);
        visitor.count
    }

    fn field_is_from_binding(expr: &syn::Expr, binding: &str, field: &str) -> bool {
        let syn::Expr::Field(field_expr) = unwrap_expr(expr) else {
            return false;
        };
        matches!(&field_expr.member, syn::Member::Named(member) if member == field)
            && path_ident(&field_expr.base).as_deref() == Some(binding)
    }

    fn result_uses_resolved_metadata(
        expr: &syn::Expr,
        metadata_binding: &str,
        planner_binding: &str,
    ) -> bool {
        let syn::Expr::Call(ok_call) = unwrap_expr(expr) else {
            return false;
        };
        if !matches!(
            ok_call.func.as_ref(),
            syn::Expr::Path(path)
                if path.qself.is_none()
                    && ebd_4b3b_path_ends_with(&path.path, "Ok")
                    && ok_call.args.len() == 1
        ) {
            return false;
        }
        let Some(argument) = ok_call.args.first() else {
            return false;
        };
        let syn::Expr::Struct(result) = unwrap_expr(argument) else {
            return false;
        };
        if !ebd_4b3b_path_ends_with(&result.path, "ResolvedAnalyzerTable")
            || result.rest.is_some()
            || result.fields.len() != 2
        {
            return false;
        }

        let mut catalog_from_metadata = false;
        let mut planner_from_metadata = false;
        for field in &result.fields {
            match &field.member {
                syn::Member::Named(member) if member == "catalog" => {
                    catalog_from_metadata =
                        field_is_from_binding(&field.expr, metadata_binding, "table");
                }
                syn::Member::Named(member) if member == "planner" => {
                    planner_from_metadata =
                        path_ident(&field.expr).as_deref() == Some(planner_binding);
                }
                _ => return false,
            }
        }
        catalog_from_metadata && planner_from_metadata
    }

    fn resolution_branch_is_exact(block: &syn::Block, external_catalog: &str) -> bool {
        let [
            syn::Stmt::Local(metadata_local),
            syn::Stmt::Local(planner_local),
            syn::Stmt::Expr(result, None),
        ] = block.stmts.as_slice()
        else {
            return false;
        };
        let Some(metadata_binding) = binding_name(metadata_local) else {
            return false;
        };
        let Some(metadata_init) = &metadata_local.init else {
            return false;
        };
        let Some(resolve) = method_call(&metadata_init.expr, "resolve") else {
            return false;
        };
        if method_count(&resolve.receiver, "registry") != 1
            || resolve.args.len() != 3
            || resolve.args.first().and_then(path_ident).as_deref() != Some(external_catalog)
            || resolve.args.iter().nth(1).and_then(path_ident).as_deref() != Some("database")
            || resolve.args.iter().nth(2).and_then(path_ident).as_deref() != Some("table")
        {
            return false;
        }

        let Some(planner_binding) = binding_name(planner_local) else {
            return false;
        };
        let Some(planner_init) = &planner_local.init else {
            return false;
        };
        let Some(to_table_def) = method_call(&planner_init.expr, "to_table_def") else {
            return false;
        };
        if path_ident(&to_table_def.receiver).as_deref() != Some(&metadata_binding) {
            return false;
        }

        result_uses_resolved_metadata(result, &metadata_binding, &planner_binding)
    }

    fn external_catalog_binding(pattern: &syn::Pat) -> Option<String> {
        let syn::Pat::TupleStruct(pattern) = pattern else {
            return None;
        };
        if !ebd_4b3b_path_ends_with(&pattern.path, "Some") || pattern.elems.len() != 1 {
            return None;
        }
        let Some(syn::Pat::Ident(binding)) = pattern.elems.first() else {
            return None;
        };
        if binding.by_ref.is_some()
            || binding.mutability.is_some()
            || binding.subpat.is_some()
            || matches!(
                binding.ident.to_string().as_str(),
                "self" | "database" | "table"
            )
        {
            return None;
        }
        Some(binding.ident.to_string())
    }

    fn is_default_catalog_some_pattern(pattern: &syn::Pat) -> bool {
        let syn::Pat::TupleStruct(pattern) = pattern else {
            return false;
        };
        ebd_4b3b_path_ends_with(&pattern.path, "Some")
            && pattern.elems.len() == 1
            && matches!(
                pattern.elems.first(),
                Some(syn::Pat::Lit(literal))
                    if matches!(
                        &literal.lit,
                        syn::Lit::Str(value) if value.value() == "default_catalog"
                    )
            )
    }

    fn is_none_pattern(pattern: &syn::Pat) -> bool {
        matches!(
            pattern,
            syn::Pat::Ident(none)
                if none.ident == "None"
                    && none.by_ref.is_none()
                    && none.mutability.is_none()
                    && none.subpat.is_none()
        ) || matches!(
            pattern,
            syn::Pat::Path(path) if ebd_4b3b_path_ends_with(&path.path, "None")
        )
    }

    fn is_default_catalog_pattern(pattern: &syn::Pat) -> bool {
        let syn::Pat::Or(pattern) = pattern else {
            return false;
        };
        if pattern.cases.len() != 2 {
            return false;
        }
        let mut default_some = 0;
        let mut none = 0;
        for case in &pattern.cases {
            if is_default_catalog_some_pattern(case) {
                default_some += 1;
            } else if is_none_pattern(case) {
                none += 1;
            } else {
                return false;
            }
        }
        default_some == 1 && none == 1
    }

    fn external_resolution_tail_is_exact(block: &syn::Block) -> bool {
        let [syn::Stmt::Expr(tail, None)] = block.stmts.as_slice() else {
            return false;
        };
        let syn::Expr::Match(match_expr) = unwrap_expr(tail) else {
            return false;
        };
        let Some(effective_catalog) = method_call(&match_expr.expr, "effective_catalog") else {
            return false;
        };
        if path_ident(&effective_catalog.receiver).as_deref() != Some("self")
            || effective_catalog.args.len() != 1
            || effective_catalog
                .args
                .first()
                .and_then(path_ident)
                .as_deref()
                != Some("catalog")
        {
            return false;
        }

        let [default_arm, external_arm] = match_expr.arms.as_slice() else {
            return false;
        };
        if default_arm.guard.is_some()
            || external_arm.guard.is_some()
            || !is_default_catalog_pattern(&default_arm.pat)
        {
            return false;
        }
        let Some(external_catalog) = external_catalog_binding(&external_arm.pat) else {
            return false;
        };
        let syn::Expr::Block(external_body) = unwrap_expr(&external_arm.body) else {
            return false;
        };
        resolution_branch_is_exact(&external_body.block, &external_catalog)
    }

    #[derive(Default)]
    struct ResolutionVisitor {
        resolve_calls: usize,
        catalog_table_calls: usize,
        planner_table_calls: usize,
        forbidden_calls: usize,
    }

    impl<'ast> syn::visit::Visit<'ast> for ResolutionVisitor {
        fn visit_expr_method_call(&mut self, item: &'ast syn::ExprMethodCall) {
            match item.method.to_string().as_str() {
                "resolve" => self.resolve_calls += 1,
                "to_catalog_table" => self.catalog_table_calls += 1,
                "to_table_def" => self.planner_table_calls += 1,
                "load_table_for_read"
                | "build_table_def"
                | "build_schema_table_def"
                | "build_metadata_rows_table_def"
                | "catalog_backend"
                | "table_source" => self.forbidden_calls += 1,
                _ => {}
            }
            syn::visit::visit_expr_method_call(self, item);
        }
    }

    let mut visitor = ResolutionVisitor::default();
    syn::visit::Visit::visit_block(&mut visitor, block);
    visitor.resolve_calls == 1
        && visitor.planner_table_calls == 1
        && visitor.catalog_table_calls == 0
        && visitor.forbidden_calls == 0
        && external_resolution_tail_is_exact(block)
}

fn ebd_4b3b_schema_only_helper_is_metadata_only(block: &syn::Block) -> bool {
    #[derive(Default)]
    struct SchemaOnlyArmVisitor {
        schema_only_arms: usize,
        valid_schema_only_arms: usize,
    }

    #[derive(Default)]
    struct SchemaOnlyBodyVisitor {
        resolve_calls: usize,
        to_table_def_calls: usize,
        forbidden_calls: usize,
    }

    impl<'ast> syn::visit::Visit<'ast> for SchemaOnlyBodyVisitor {
        fn visit_expr_call(&mut self, item: &'ast syn::ExprCall) {
            if let syn::Expr::Path(function) = item.func.as_ref()
                && let Some(segment) = function.path.segments.last()
            {
                match segment.ident.to_string().as_str() {
                    "resolve" => self.resolve_calls += 1,
                    "to_table_def" => self.to_table_def_calls += 1,
                    "load_table_for_read"
                    | "build_table_def"
                    | "build_schema_table_def"
                    | "build_metadata_rows_table_def"
                    | "catalog_backend"
                    | "table_source" => self.forbidden_calls += 1,
                    _ => {}
                }
            }
            syn::visit::visit_expr_call(self, item);
        }

        fn visit_expr_method_call(&mut self, item: &'ast syn::ExprMethodCall) {
            match item.method.to_string().as_str() {
                "resolve" => self.resolve_calls += 1,
                "to_table_def" => self.to_table_def_calls += 1,
                "load_table_for_read"
                | "build_table_def"
                | "build_schema_table_def"
                | "build_metadata_rows_table_def"
                | "catalog_backend"
                | "table_source" => self.forbidden_calls += 1,
                _ => {}
            }
            syn::visit::visit_expr_method_call(self, item);
        }
    }

    impl<'ast> syn::visit::Visit<'ast> for SchemaOnlyArmVisitor {
        fn visit_expr_match(&mut self, item: &'ast syn::ExprMatch) {
            for arm in &item.arms {
                let schema_only = matches!(
                    &arm.pat,
                    syn::Pat::Path(pattern)
                        if ebd_4b3b_path_ends_with(&pattern.path, "SchemaOnly")
                );
                if schema_only {
                    self.schema_only_arms += 1;
                    let mut body = SchemaOnlyBodyVisitor::default();
                    syn::visit::Visit::visit_expr(&mut body, &arm.body);
                    if body.resolve_calls == 1
                        && body.to_table_def_calls == 1
                        && body.forbidden_calls == 0
                    {
                        self.valid_schema_only_arms += 1;
                    }
                }
            }
            syn::visit::visit_expr_match(self, item);
        }
    }

    let mut visitor = SchemaOnlyArmVisitor::default();
    syn::visit::Visit::visit_block(&mut visitor, block);
    visitor.schema_only_arms == 1 && visitor.valid_schema_only_arms == 1
}

fn ebd_4b3b_audit_statistics_lookup_decoupling(sources: &[GuardSource]) -> BTreeSet<String> {
    let mut violations = BTreeSet::new();
    for source in sources.iter().filter(|source| {
        matches!(
            source.path.as_str(),
            EBD_4B3B_PROVIDER | EBD_4B3B_ENGINE | EBD_4B3B_SQL_CATALOG
        )
    }) {
        let identifiers = ebd_4b3a_identifiers(source);
        if identifiers.contains("ExplainStats") {
            violations.insert(format!(
                "catalog-statistics-lookup-mode: {}|ExplainStats",
                source.path
            ));
        }

        let Ok(file) = syn::parse_file(&source.text) else {
            violations.insert(format!(
                "catalog-statistics-lookup-parse-failed: {}",
                source.path
            ));
            continue;
        };

        struct BoundaryVisitor<'a> {
            path: &'a str,
            in_planner_provider_impl: bool,
            violations: &'a mut BTreeSet<String>,
        }

        impl<'ast> syn::visit::Visit<'ast> for BoundaryVisitor<'_> {
            fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
                if self.path == EBD_4B3B_PROVIDER
                    && item.ident == "CatalogServiceProvider"
                    && let syn::Fields::Named(fields) = &item.fields
                {
                    for field in &fields.named {
                        if field
                            .ident
                            .as_ref()
                            .is_some_and(|ident| ident == "default_mode")
                            || ebd_4b3b_type_is_table_lookup_mode(&field.ty)
                        {
                            self.violations.insert(format!(
                                "catalog-statistics-provider-mode-field: {}",
                                self.path
                            ));
                        }
                    }
                }
                syn::visit::visit_item_struct(self, item);
            }

            fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
                let was_in_planner_provider_impl = self.in_planner_provider_impl;
                self.in_planner_provider_impl = self.path == EBD_4B3B_PROVIDER
                    && matches!(
                        item.self_ty.as_ref(),
                        syn::Type::Path(path)
                            if path.path.segments.last().is_some_and(|segment| {
                                segment.ident == "CatalogServiceProvider"
                            })
                    )
                    && item
                        .trait_
                        .as_ref()
                        .and_then(|(_, path, _)| path.segments.last())
                        .is_some_and(|segment| segment.ident == "PlannerTableProvider");
                syn::visit::visit_item_impl(self, item);
                self.in_planner_provider_impl = was_in_planner_provider_impl;
            }

            fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
                if self.path == EBD_4B3B_PROVIDER
                    && item.sig.ident == "new"
                    && item.sig.inputs.iter().any(|input| {
                        matches!(
                            input,
                            syn::FnArg::Typed(argument)
                                if ebd_4b3b_type_is_table_lookup_mode(&argument.ty)
                        )
                    })
                {
                    self.violations.insert(format!(
                        "catalog-statistics-provider-constructor-mode: {}",
                        self.path
                    ));
                }
                if self.in_planner_provider_impl
                    && matches!(
                        item.sig.ident.to_string().as_str(),
                        "get_table" | "get_table_in_catalog"
                    )
                    && !ebd_4b3b_ordinary_lookup_is_schema_only(&item.block)
                {
                    self.violations.insert(format!(
                        "catalog-statistics-ordinary-lookup-shape: {}|{}",
                        self.path, item.sig.ident
                    ));
                }
                if self.path == EBD_4B3B_PROVIDER
                    && item.sig.ident == "iceberg_table_def"
                    && !ebd_4b3b_schema_only_helper_is_metadata_only(&item.block)
                {
                    self.violations.insert(format!(
                        "catalog-statistics-schema-only-helper-shape: {}|{}",
                        self.path, item.sig.ident
                    ));
                }
                if self.path == EBD_4B3B_PROVIDER
                    && item.sig.ident == "resolve_table_for_analysis_once"
                    && !ebd_4b3b_resolution_helper_is_metadata_only(&item.block)
                {
                    self.violations.insert(format!(
                        "catalog-statistics-neutral-resolution-helper-shape: {}|{}",
                        self.path, item.sig.ident
                    ));
                }
                syn::visit::visit_impl_item_fn(self, item);
            }

            fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
                if self.path == EBD_4B3B_ENGINE
                    && item.sig.ident == "build_analyzer_provider"
                    && item.sig.inputs.iter().any(|input| {
                        matches!(
                            input,
                            syn::FnArg::Typed(argument)
                                if ebd_4b3b_type_is_table_lookup_mode(&argument.ty)
                        )
                    })
                {
                    self.violations.insert(
                        "catalog-statistics-analyzer-provider-mode: src/engine/mod.rs".to_string(),
                    );
                }
                syn::visit::visit_item_fn(self, item);
            }

            fn visit_expr_call(&mut self, item: &'ast syn::ExprCall) {
                if self.path == EBD_4B3B_ENGINE
                    && let syn::Expr::Path(function) = item.func.as_ref()
                    && ebd_4b3b_path_ends_with(&function.path, "build_analyzer_provider")
                    && item.args.len() != 4
                {
                    self.violations.insert(format!(
                        "catalog-statistics-analyzer-provider-call-shape: {}|args={}",
                        self.path,
                        item.args.len()
                    ));
                }
                syn::visit::visit_expr_call(self, item);
            }

            fn visit_expr_match(&mut self, item: &'ast syn::ExprMatch) {
                if self.path == EBD_4B3B_ENGINE {
                    for arm in &item.arms {
                        if ebd_4b3b_pattern_mentions_explain(&arm.pat)
                            && ebd_4b3b_explain_arm_reads_full_table_payload(&arm.body)
                        {
                            self.violations.insert(
                                "catalog-statistics-explain-full-payload-read: src/engine/mod.rs"
                                    .to_string(),
                            );
                        }
                    }
                }
                syn::visit::visit_expr_match(self, item);
            }
        }

        let mut visitor = BoundaryVisitor {
            path: &source.path,
            in_planner_provider_impl: false,
            violations: &mut violations,
        };
        syn::visit::Visit::visit_file(&mut visitor, &file);
    }
    violations
}

#[test]
fn ebd_4b3b_detector_rejects_statistics_lookup_rewiring() {
    let sources = vec![
        GuardSource::new(
            EBD_4B3B_SQL_CATALOG,
            "enum TableLookupMode { SchemaOnly, ExplainStats }",
        ),
        GuardSource::new(
            EBD_4B3B_PROVIDER,
            r#"
struct CatalogServiceProvider { default_mode: TableLookupMode }
impl PlannerTableProvider for CatalogServiceProvider {
    fn new(default_mode: TableLookupMode) -> Self { todo!() }
    fn iceberg_table_def(&self, mode: &TableLookupMode) {
        match mode {
            TableLookupMode::SchemaOnly => {
                let resolved = backend.load_table_for_read("ice", "db", "t");
                source.build_table_def(&resolved);
            }
            _ => {}
        }
    }
    fn get_table(&self, database: &str, table: &str) {
        source.build_table_def(&resolved);
    }
    fn get_table_in_catalog(&self, catalog: Option<&str>, database: &str, table: &str) {
        self.get_table_with_mode(catalog, database, table, TableLookupMode::ExplainStats);
    }
}
"#,
        ),
        GuardSource::new(
            EBD_4B3B_ENGINE,
            r#"
fn build_analyzer_provider(mode: crate::sql::catalog::TableLookupMode) {}
fn execute() {
    build_analyzer_provider(None, &catalog, &mgr, &connectors, mode);
    match statement {
        Statement::Explain { .. } => {
            source.build_table_def(&resolved);
            let _ = table.files;
        }
        _ => {}
    }
}
"#,
        ),
    ];
    let violations = ebd_4b3b_audit_statistics_lookup_decoupling(&sources);
    assert!(violations.iter().any(|item| item.contains("ExplainStats")));
    assert!(
        violations
            .iter()
            .any(|item| item.contains("provider-mode-field"))
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("provider-constructor-mode"))
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("ordinary-lookup-shape"))
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("schema-only-helper-shape"))
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("analyzer-provider-mode"))
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("analyzer-provider-call-shape"))
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("explain-full-payload-read"))
    );
}

#[test]
fn ebd_4b3b_detector_rejects_catalog_table_not_derived_from_resolved_metadata() {
    let sources = vec![GuardSource::new(
        EBD_4B3B_PROVIDER,
        r#"
struct CatalogServiceProvider;
impl CatalogServiceProvider {
    fn resolve_table_for_analysis_once(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        let metadata = self
            .service
            .registry()
            .read()
            .expect("catalog service registry read lock")
            .resolve(catalog.unwrap(), database, table)?;
        let planner = metadata.to_table_def();
        Ok(ResolvedAnalyzerTable {
            catalog: CatalogTable::default(),
            planner,
        })
    }
}
"#,
    )];

    let violations = ebd_4b3b_audit_statistics_lookup_decoupling(&sources);
    assert!(
        violations
            .iter()
            .any(|item| item.contains("neutral-resolution-helper-shape")),
        "a fake CatalogTable must not satisfy one-resolution metadata provenance: {violations:?}"
    );
}

#[test]
fn ebd_4b3b_detector_rejects_unreachable_metadata_provenance_decoy() {
    let sources = vec![GuardSource::new(
        EBD_4B3B_PROVIDER,
        r#"
struct CatalogServiceProvider;
impl CatalogServiceProvider {
    fn resolve_table_for_analysis_once(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        if false {
            let _ = {
                let metadata = self
                    .service
                    .registry()
                    .read()
                    .expect("catalog service registry read lock")
                    .resolve(catalog.unwrap(), database, table)?;
                let planner = metadata.to_table_def();
                Ok(ResolvedAnalyzerTable {
                    catalog: metadata.table,
                    planner,
                })
            };
        }
        Ok(ResolvedAnalyzerTable {
            catalog: CatalogTable::default(),
            planner: TableDef::default(),
        })
    }
}
"#,
    )];

    let violations = ebd_4b3b_audit_statistics_lookup_decoupling(&sources);
    assert!(
        violations
            .iter()
            .any(|item| item.contains("neutral-resolution-helper-shape")),
        "unreachable metadata provenance must not validate a fake tail result: {violations:?}"
    );
}

#[test]
fn ebd_4b3b_detector_rejects_shadowed_or_misbound_external_catalog_arms() {
    for (case, match_arms) in [
        (
            "preceding Some wildcard",
            r#"
            Some(_) => Ok(ResolvedAnalyzerTable {
                catalog: CatalogTable::default(),
                planner: TableDef::default(),
            }),
            Some(catalog) => {
                let metadata = self
                    .service
                    .registry()
                    .read()
                    .expect("catalog service registry read lock")
                    .resolve(catalog, database, table)?;
                let planner = metadata.to_table_def();
                Ok(ResolvedAnalyzerTable {
                    catalog: metadata.table,
                    planner,
                })
            }
            Some("default_catalog") | None => Ok(ResolvedAnalyzerTable {
                catalog: CatalogTable::default(),
                planner: TableDef::default(),
            }),
"#,
        ),
        (
            "preceding catch all",
            r#"
            Some("default_catalog") | None => Ok(ResolvedAnalyzerTable {
                catalog: CatalogTable::default(),
                planner: TableDef::default(),
            }),
            _ => Ok(ResolvedAnalyzerTable {
                catalog: CatalogTable::default(),
                planner: TableDef::default(),
            }),
            Some(catalog) => {
                let metadata = self
                    .service
                    .registry()
                    .read()
                    .expect("catalog service registry read lock")
                    .resolve(catalog, database, table)?;
                let planner = metadata.to_table_def();
                Ok(ResolvedAnalyzerTable {
                    catalog: metadata.table,
                    planner,
                })
            }
"#,
        ),
        (
            "misbound resolve catalog",
            r#"
            Some("default_catalog") | None => Ok(ResolvedAnalyzerTable {
                catalog: CatalogTable::default(),
                planner: TableDef::default(),
            }),
            Some(catalog) => {
                let metadata = self
                    .service
                    .registry()
                    .read()
                    .expect("catalog service registry read lock")
                    .resolve(other_catalog, database, table)?;
                let planner = metadata.to_table_def();
                Ok(ResolvedAnalyzerTable {
                    catalog: metadata.table,
                    planner,
                })
            }
"#,
        ),
    ] {
        let source = format!(
            r#"
struct CatalogServiceProvider;
impl CatalogServiceProvider {{
    fn resolve_table_for_analysis_once(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {{
        match self.effective_catalog(catalog) {{
{match_arms}
        }}
    }}
}}
"#
        );
        let violations = ebd_4b3b_audit_statistics_lookup_decoupling(&[GuardSource::new(
            EBD_4B3B_PROVIDER,
            &source,
        )]);
        assert!(
            violations
                .iter()
                .any(|item| item.contains("neutral-resolution-helper-shape")),
            "{case} must not satisfy the external catalog resolution partition: {violations:?}"
        );
    }
}

#[test]
fn ebd_4b3b_detector_rejects_external_catalog_arm_before_default_arm() {
    let sources = vec![GuardSource::new(
        EBD_4B3B_PROVIDER,
        r#"
struct CatalogServiceProvider;
impl CatalogServiceProvider {
    fn resolve_table_for_analysis_once(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        match self.effective_catalog(catalog) {
            Some(catalog) => {
                let metadata = self
                    .service
                    .registry()
                    .read()
                    .expect("catalog service registry read lock")
                    .resolve(catalog, database, table)?;
                let planner = metadata.to_table_def();
                Ok(ResolvedAnalyzerTable {
                    catalog: metadata.table,
                    planner,
                })
            }
            Some("default_catalog") | None => Ok(ResolvedAnalyzerTable {
                catalog: CatalogTable::default(),
                planner: TableDef::default(),
            }),
        }
    }
}
"#,
    )];

    let violations = ebd_4b3b_audit_statistics_lookup_decoupling(&sources);
    assert!(
        violations
            .iter()
            .any(|item| item.contains("neutral-resolution-helper-shape")),
        "external catalog arm before default arm must not satisfy the resolution partition: {violations:?}"
    );
}

#[test]
fn ebd_4b3b_statistics_lookup_decoupling_is_complete() {
    let repo = Path::new(manifest_dir());
    let sources = [EBD_4B3B_PROVIDER, EBD_4B3B_ENGINE, EBD_4B3B_SQL_CATALOG]
        .into_iter()
        .map(|relative| {
            let path = repo.join(relative);
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"));
            GuardSource::new(relative, &text)
        })
        .collect::<Vec<_>>();
    let violations = ebd_4b3b_audit_statistics_lookup_decoupling(&sources);
    assert!(
        violations.is_empty(),
        "EBD-4B3B statistics lookup decoupling failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

const EBD_4B3C_PLANNER_OWNER: &str = "src/sql/planner/table.rs";
const EBD_4B3E_CONNECTOR_OWNER: &str = "src/connector/iceberg/scan_model.rs";
const EBD_4B3C_CODEGEN_HELPER: &str = "src/connector/scan_planning/starrocks.rs";
const EBD_4B3C_COORDINATOR_ENTRY: &str = "src/coordinator/prepare/scan_preparation.rs";
const EBD_4B3E_CONNECTOR_MODEL_SYMBOLS: &[&str] = &[
    "IcebergColumnStats",
    "IcebergPartitionValue",
    "IcebergPartitionFieldValue",
    "IcebergDeleteFileFormat",
    "IcebergDeleteFileContent",
    "IcebergDeleteFileInfo",
    "IcebergSchemaFieldDef",
    "IcebergSchemaDef",
    "IcebergTableInfo",
    "IcebergDataFileInfo",
    "IcebergDataFileBinding",
];
const EBD_4B3C_PLANNER_MODEL_SYMBOLS: &[&str] = &[
    "IcebergMvTargetStateScan",
    "IcebergMvTargetLocatorScan",
    "BranchScope",
    "IcebergMvTargetStateRowFilter",
    "IcebergMvTargetStatePartitionConstraint",
    "ScanSource",
    "TableDef",
];
const EBD_4B3C_MODEL_SYMBOLS: &[&str] = &[
    "IcebergColumnStats",
    "IcebergPartitionValue",
    "IcebergPartitionFieldValue",
    "IcebergDeleteFileFormat",
    "IcebergDeleteFileContent",
    "IcebergDeleteFileInfo",
    "IcebergSchemaFieldDef",
    "IcebergSchemaDef",
    "IcebergTableInfo",
    "IcebergDataFileInfo",
    "IcebergDataFileBinding",
    "IcebergMvTargetStateScan",
    "IcebergMvTargetLocatorScan",
    "BranchScope",
    "IcebergMvTargetStateRowFilter",
    "IcebergMvTargetStatePartitionConstraint",
    "ScanSource",
    "TableDef",
];

#[derive(Clone, Copy)]
struct Ebd4b3cOwnerSpec {
    name: &'static str,
    kind: &'static str,
    visibility: &'static str,
    derives: &'static [&'static str],
    shape_hash: u64,
}

const EBD_4B3C_OWNER_SPECS: &[Ebd4b3cOwnerSpec] = &[
    Ebd4b3cOwnerSpec {
        name: "IcebergColumnStats",
        kind: "struct",
        visibility: "pub",
        derives: &["Clone", "Debug"],
        shape_hash: 2056551809977803008,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergPartitionValue",
        kind: "enum",
        visibility: "pub",
        derives: &["Clone", "Debug", "PartialEq"],
        shape_hash: 17655657493373355219,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergPartitionFieldValue",
        kind: "struct",
        visibility: "pub",
        derives: &["Clone", "Debug", "PartialEq"],
        shape_hash: 5730817505100012193,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergDeleteFileFormat",
        kind: "enum",
        visibility: "pub",
        derives: &["Clone", "Debug", "Eq", "PartialEq"],
        shape_hash: 17320865237198704987,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergDeleteFileContent",
        kind: "enum",
        visibility: "pub",
        derives: &["Clone", "Debug", "Eq", "PartialEq"],
        shape_hash: 4721273103440761884,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergDeleteFileInfo",
        kind: "struct",
        visibility: "pub",
        derives: &["Clone", "Debug", "Eq", "PartialEq"],
        shape_hash: 10349318917319020024,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergSchemaFieldDef",
        kind: "struct",
        visibility: "pub",
        derives: &["Clone", "Debug", "PartialEq"],
        shape_hash: 8686511160588708780,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergSchemaDef",
        kind: "struct",
        visibility: "pub",
        derives: &["Clone", "Debug", "PartialEq"],
        shape_hash: 16944456593516362218,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergTableInfo",
        kind: "struct",
        visibility: "pub",
        derives: &["Clone", "Debug", "PartialEq"],
        shape_hash: 9007559196177372905,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergMvTargetStateScan",
        kind: "struct",
        visibility: "pub(crate)",
        derives: &["Clone", "Debug", "PartialEq"],
        shape_hash: 13033557410242340071,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergMvTargetLocatorScan",
        kind: "struct",
        visibility: "pub",
        derives: &["Clone", "Debug", "Eq", "PartialEq"],
        shape_hash: 13561036483016194962,
    },
    Ebd4b3cOwnerSpec {
        name: "BranchScope",
        kind: "struct",
        visibility: "pub(crate)",
        derives: &["Clone", "Debug", "Eq", "PartialEq"],
        shape_hash: 158761904751257978,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergMvTargetStateRowFilter",
        kind: "enum",
        visibility: "pub(crate)",
        derives: &["Clone", "Debug", "Eq", "PartialEq"],
        shape_hash: 336745192447927450,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergMvTargetStatePartitionConstraint",
        kind: "enum",
        visibility: "pub(crate)",
        derives: &["Clone", "Debug", "Eq", "PartialEq"],
        shape_hash: 16390858955688211517,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergDataFileInfo",
        kind: "struct",
        visibility: "pub",
        derives: &["Clone", "Debug"],
        shape_hash: 9697454222230598461,
    },
    Ebd4b3cOwnerSpec {
        name: "ScanSource",
        kind: "enum",
        visibility: "pub",
        derives: &["Clone", "Debug"],
        shape_hash: 9708048268714969001,
    },
    Ebd4b3cOwnerSpec {
        name: "IcebergDataFileBinding",
        kind: "enum",
        visibility: "pub",
        derives: &["Clone", "Copy", "Debug", "Eq", "PartialEq"],
        shape_hash: 13307274270418829994,
    },
    Ebd4b3cOwnerSpec {
        name: "TableDef",
        kind: "struct",
        visibility: "pub",
        derives: &["Clone", "Debug"],
        shape_hash: 13039605040232147,
    },
];

fn ebd_4b3c_is_model_symbol(name: &str) -> bool {
    EBD_4B3C_MODEL_SYMBOLS.contains(&name)
}

fn ebd_4b3e_is_connector_model_symbol(name: &str) -> bool {
    EBD_4B3E_CONNECTOR_MODEL_SYMBOLS.contains(&name)
}

fn ebd_4b3c_expected_owner(name: &str) -> Option<&'static str> {
    if ebd_4b3e_is_connector_model_symbol(name) {
        Some(EBD_4B3E_CONNECTOR_OWNER)
    } else if EBD_4B3C_PLANNER_MODEL_SYMBOLS.contains(&name) {
        Some(EBD_4B3C_PLANNER_OWNER)
    } else {
        None
    }
}

fn ebd_4b3c_visibility(visibility: &syn::Visibility) -> String {
    match visibility {
        syn::Visibility::Inherited => "private".to_string(),
        syn::Visibility::Public(_) => "pub".to_string(),
        syn::Visibility::Restricted(restricted)
            if restricted.in_token.is_none() && restricted.path.is_ident("crate") =>
        {
            "pub(crate)".to_string()
        }
        syn::Visibility::Restricted(restricted) => format!(
            "pub({})",
            restricted
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::")
        ),
    }
}

fn ebd_4b3c_derive_set(attributes: &[syn::Attribute]) -> Option<BTreeSet<String>> {
    let mut derives = BTreeSet::new();
    for attribute in attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("derive"))
    {
        let paths = attribute
            .parse_args_with(
                syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated,
            )
            .ok()?;
        derives.extend(paths.into_iter().map(|path| {
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::")
        }));
    }
    Some(derives)
}

fn ebd_4b3c_fnv1a(text: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn ebd_4b3c_root_declaration_shapes(source: &str) -> BTreeMap<String, (String, u64)> {
    let sanitized = rust_sanitized_production_text(source);
    let tokens = rust_source_tokens(&sanitized);
    let mut shapes = BTreeMap::new();
    let mut brace_depth = 0usize;
    let mut index = 0usize;
    while index < tokens.len() {
        let token = &tokens[index];
        if brace_depth == 0
            && matches!(token.text.as_str(), "struct" | "enum")
            && tokens
                .get(index + 1)
                .is_some_and(|next| ebd_4b3c_is_model_symbol(&next.text))
        {
            let name = tokens[index + 1].text.clone();
            let kind = token.text.clone();
            let mut end = index + 2;
            let mut declaration_depth = 0usize;
            while end < tokens.len() {
                match tokens[end].text.as_str() {
                    "{" => declaration_depth += 1,
                    "}" => {
                        declaration_depth = declaration_depth.saturating_sub(1);
                        if declaration_depth == 0 {
                            end += 1;
                            break;
                        }
                    }
                    ";" if declaration_depth == 0 => {
                        end += 1;
                        break;
                    }
                    _ => {}
                }
                end += 1;
            }
            let shape = tokens[index..end]
                .iter()
                .map(|token| token.text.as_str())
                .collect::<String>();
            shapes.insert(name, (kind, ebd_4b3c_fnv1a(&shape)));
            index = end;
            continue;
        }
        match token.text.as_str() {
            "{" => brace_depth += 1,
            "}" => brace_depth = brace_depth.saturating_sub(1),
            _ => {}
        }
        index += 1;
    }
    shapes
}

fn ebd_4b3c_audit_owner(source: &GuardSource) -> BTreeSet<String> {
    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "planner-scan-model-owner-parse-failed: {}",
            source.path
        )]);
    };
    let shapes = ebd_4b3c_root_declaration_shapes(&source.text);
    let mut violations = BTreeSet::new();
    for spec in EBD_4B3C_OWNER_SPECS
        .iter()
        .filter(|spec| ebd_4b3c_expected_owner(spec.name) == Some(source.path.as_str()))
    {
        let matching = file
            .items
            .iter()
            .filter_map(|item| match item {
                syn::Item::Struct(item) if item.ident == spec.name => {
                    Some(("struct", &item.vis, &item.attrs))
                }
                syn::Item::Enum(item) if item.ident == spec.name => {
                    Some(("enum", &item.vis, &item.attrs))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            violations.insert(format!(
                "planner-scan-model-owner-count: {}|{}|expected=1 actual={}",
                source.path,
                spec.name,
                matching.len()
            ));
            continue;
        }
        let (kind, visibility, attributes) = matching[0];
        let actual_derives = ebd_4b3c_derive_set(attributes);
        let expected_derives = spec
            .derives
            .iter()
            .map(|derive| (*derive).to_string())
            .collect::<BTreeSet<_>>();
        let shape = shapes.get(spec.name);
        if kind != spec.kind
            || ebd_4b3c_visibility(visibility) != spec.visibility
            || actual_derives.as_ref() != Some(&expected_derives)
            || shape.is_none_or(|(shape_kind, hash)| {
                shape_kind != spec.kind || *hash != spec.shape_hash
            })
        {
            violations.insert(format!(
                "planner-scan-model-owner-shape: {}|{}|kind={kind}|visibility={}|derives={actual_derives:?}|shape={shape:?}",
                source.path,
                spec.name,
                ebd_4b3c_visibility(visibility),
            ));
        }
    }
    violations
}

fn ebd_4b3c_is_legacy_model_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    let roots = [
        &["crate", "sql", "catalog"][..],
        &["crate", "engine", "catalog"][..],
        &["crate", "engine"][..],
    ];
    roots.iter().any(|root| {
        segments.starts_with(root)
            && segments
                .get(root.len())
                .is_some_and(|leaf| *leaf == "*" || ebd_4b3c_is_model_symbol(leaf))
    })
}

fn ebd_4b3c_is_canonical_model_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    for (root, owner) in [
        (
            &["crate", "sql", "planner", "table"][..],
            EBD_4B3C_PLANNER_OWNER,
        ),
        (
            &["crate", "connector", "iceberg", "scan_model"][..],
            EBD_4B3E_CONNECTOR_OWNER,
        ),
    ] {
        if segments.starts_with(root)
            && segments.get(root.len()).is_some_and(|leaf| {
                *leaf == "*"
                    || ebd_4b3c_expected_owner(leaf).is_some_and(|expected| expected == owner)
            })
        {
            return true;
        }
    }
    false
}

fn ebd_4b3e_is_retired_planner_raw_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    let root = ["crate", "sql", "planner", "table"];
    segments.starts_with(&root)
        && segments
            .get(root.len())
            .is_some_and(|leaf| *leaf == "*" || ebd_4b3e_is_connector_model_symbol(leaf))
}

fn ebd_4b3c_type_contains_model_path(
    ty: &syn::Type,
    source: &GuardSource,
    aliases: &RustScopedAliases,
    inline_modules: &[String],
) -> bool {
    struct ModelPathAudit<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        inline_modules: &'a [String],
        found: bool,
    }
    impl<'ast> syn::visit::Visit<'ast> for ModelPathAudit<'_> {
        fn visit_path(&mut self, path: &'ast syn::Path) {
            let segments = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            if let Some(resolved) = rust_resolve_scoped_paths(
                &segments,
                self.inline_modules,
                self.aliases,
                &mut BTreeSet::new(),
                0,
            ) {
                self.found |= resolved.into_iter().any(|resolved| {
                    rust_canonical_path_segments_in_scope(
                        &resolved.segments,
                        &self.source.path,
                        &resolved.inline_modules,
                    )
                    .is_some_and(|canonical| {
                        ebd_4b3c_is_legacy_model_path(&canonical)
                            || ebd_4b3c_is_canonical_model_path(&canonical)
                    })
                });
            }
            syn::visit::visit_path(self, path);
        }
    }
    let mut audit = ModelPathAudit {
        source,
        aliases,
        inline_modules,
        found: false,
    };
    syn::visit::Visit::visit_type(&mut audit, ty);
    audit.found
}

fn ebd_4b3c_macro_mentions_model(item: &syn::ItemMacro) -> bool {
    rust_source_tokens(&item.mac.tokens.to_string())
        .iter()
        .any(|token| ebd_4b3c_is_model_symbol(&token.text))
}

fn ebd_4b3c_audit_paths_definitions_and_forwarding(sources: &[GuardSource]) -> BTreeSet<String> {
    struct DefinitionAudit<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        inline_modules: Vec<String>,
        violations: BTreeSet<String>,
    }
    impl DefinitionAudit<'_> {
        fn canonical_root(&self, name: &str) -> bool {
            ebd_4b3c_expected_owner(name) == Some(self.source.path.as_str())
                && self.inline_modules.is_empty()
                && ebd_4b3c_is_model_symbol(name)
        }

        fn record_definition(&mut self, kind: &str, name: &str) {
            let independent_homonym = self.source.path == "src/connector/iceberg/report.rs"
                && kind == "struct"
                && name == "IcebergColumnStats";
            if ebd_4b3c_is_model_symbol(name) && !self.canonical_root(name) && !independent_homonym
            {
                self.violations.insert(format!(
                    "planner-scan-model-secondary-owner: {}|{kind}|{name}",
                    self.source.path
                ));
            }
        }

        fn record_wrapper<'a>(&mut self, name: &str, fields: impl Iterator<Item = &'a syn::Field>) {
            let fields = fields.collect::<Vec<_>>();
            // These are connector-owned runtime envelopes with their own behavior,
            // not alternate planner-model owners or transparent forwarding aliases.
            let semantic_connector_wrapper = matches!(
                (self.source.path.as_str(), name),
                (
                    "src/connector/iceberg/file_pruning.rs",
                    "IcebergFilePruningMetadata"
                ) | ("src/connector/iceberg/scan_planner.rs", "IcebergSplit")
                    | ("src/engine/catalog.rs", "DatabaseDef")
                    | ("src/protocol/native/encode/build.rs", "PlannedIcebergFiles")
            );
            let tuple_wrapper = fields.iter().any(|field| field.ident.is_none());
            let named_wrapper = fields.iter().any(|field| {
                field.ident.as_ref().is_some_and(|ident| {
                    matches!(
                        ident.to_string().as_str(),
                        "inner" | "value" | "wrapped" | "delegate"
                    )
                })
            });
            if !semantic_connector_wrapper
                && (tuple_wrapper || fields.len() == 1 || named_wrapper)
                && fields.iter().any(|field| {
                    ebd_4b3c_type_contains_model_path(
                        &field.ty,
                        self.source,
                        self.aliases,
                        &self.inline_modules,
                    )
                })
            {
                self.violations.insert(format!(
                    "planner-scan-model-forwarding-wrapper: {}|{}",
                    self.source.path, name
                ));
            }
        }
    }
    impl<'ast> syn::visit::Visit<'ast> for DefinitionAudit<'_> {
        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            self.record_definition("struct", &item.ident.to_string());
            if !self.canonical_root(&item.ident.to_string()) {
                self.record_wrapper(&item.ident.to_string(), item.fields.iter());
            }
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            self.record_definition("enum", &item.ident.to_string());
            if !self.canonical_root(&item.ident.to_string())
                && item.variants.len() == 1
                && item.variants[0].fields.len() == 1
            {
                self.record_wrapper(&item.ident.to_string(), item.variants[0].fields.iter());
            }
            syn::visit::visit_item_enum(self, item);
        }

        fn visit_item_union(&mut self, item: &'ast syn::ItemUnion) {
            self.record_definition("union", &item.ident.to_string());
            syn::visit::visit_item_union(self, item);
        }

        fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
            self.record_definition("trait", &item.ident.to_string());
            syn::visit::visit_item_trait(self, item);
        }

        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            self.record_definition("type", &item.ident.to_string());
            if !ebd_5a1_is_allowed_sql_specialization_alias(
                &self.source.path,
                &self.inline_modules,
                self.aliases,
                item,
            ) && ebd_4b3c_type_contains_model_path(
                &item.ty,
                self.source,
                self.aliases,
                &self.inline_modules,
            ) {
                self.violations.insert(format!(
                    "planner-scan-model-forwarding-alias: {}|{}",
                    self.source.path, item.ident
                ));
            }
            syn::visit::visit_item_type(self, item);
        }

        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            if ebd_4b3c_macro_mentions_model(item) {
                self.violations.insert(format!(
                    "planner-scan-model-macro-surface: {}",
                    self.source.path
                ));
            }
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            self.record_definition("module", &item.ident.to_string());
            let Some((_, items)) = &item.content else {
                return;
            };
            self.inline_modules.push(item.ident.to_string());
            for item in items {
                syn::visit::Visit::visit_item(self, item);
            }
            self.inline_modules.pop();
        }
    }

    let mut violations = BTreeSet::new();
    for source in sources {
        for path in remove_redundant_descendant_paths(
            rust_production_canonical_paths(&source.text, &source.path)
                .into_iter()
                .filter(|path| {
                    ebd_4b3c_is_legacy_model_path(path)
                        || ebd_4b3e_is_retired_planner_raw_path(path)
                })
                .collect(),
        ) {
            violations.insert(format!(
                "planner-scan-model-retired-path: {}|{}",
                source.path,
                path.join("::")
            ));
        }

        let Ok(file) = syn::parse_file(&source.text) else {
            violations.insert(format!(
                "planner-scan-model-source-parse-failed: {}",
                source.path
            ));
            continue;
        };
        let (imports, aliases) = ebd_4b1_module_scope_inputs(&file);
        let mut definitions = DefinitionAudit {
            source,
            aliases: &aliases,
            inline_modules: Vec::new(),
            violations: BTreeSet::new(),
        };
        syn::visit::Visit::visit_file(&mut definitions, &file);
        violations.extend(definitions.violations);

        for import in imports {
            if import.inline_modules.iter().any(|module| module == "tests") {
                continue;
            }
            let Some(resolved) = resolve_forwarding_paths(
                &import.segments,
                &source.path,
                &import.inline_modules,
                &aliases,
                &mut BTreeSet::new(),
                0,
            ) else {
                continue;
            };
            for target in resolved {
                let Some(canonical) = rust_canonical_path_segments_in_scope(
                    &target.segments,
                    &source.path,
                    &target.inline_modules,
                ) else {
                    continue;
                };
                if ebd_4b3c_is_legacy_model_path(&canonical)
                    || (ebd_4b3e_is_retired_planner_raw_path(&canonical)
                        && canonical.last().is_some_and(|leaf| leaf != "*"))
                {
                    violations.insert(format!(
                        "planner-scan-model-retired-import: {}|{}|{}",
                        source.path,
                        import.visibility,
                        canonical.join("::")
                    ));
                } else if import.visibility != "private"
                    && !canonical.last().is_some_and(|name| {
                        ebd_4b3c_expected_owner(name) == Some(source.path.as_str())
                    })
                    && ebd_4b3c_is_canonical_model_path(&canonical)
                {
                    violations.insert(format!(
                        "planner-scan-model-forwarding-reexport: {}|{}|{}",
                        source.path,
                        import.visibility,
                        canonical.join("::")
                    ));
                }
            }
        }
    }
    violations
}

fn ebd_4b3e_audit_planner_literal_carrier(source: &GuardSource) -> BTreeSet<String> {
    struct LiteralAudit<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        inline_modules: Vec<String>,
        violations: BTreeSet<String>,
    }
    impl<'ast> syn::visit::Visit<'ast> for LiteralAudit<'_> {
        fn visit_path(&mut self, path: &'ast syn::Path) {
            let segments = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            if let Some(resolved) = rust_resolve_scoped_paths(
                &segments,
                &self.inline_modules,
                self.aliases,
                &mut BTreeSet::new(),
                0,
            ) {
                for path in resolved {
                    if path.segments == ["iceberg", "spec", "Literal"] {
                        self.violations.insert(format!(
                            "planner-scan-model-iceberg-literal-carrier: {}|{}",
                            self.source.path,
                            path.segments.join("::")
                        ));
                    }
                }
            }
            syn::visit::visit_path(self, path);
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            let Some((_, items)) = &item.content else {
                return;
            };
            self.inline_modules.push(item.ident.to_string());
            for item in items {
                syn::visit::Visit::visit_item(self, item);
            }
            self.inline_modules.pop();
        }

        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            let tokens = rust_source_tokens(&item.mac.tokens.to_string());
            if tokens.windows(5).any(|window| {
                window[0].text == "iceberg"
                    && window[1].text == "::"
                    && window[2].text == "spec"
                    && window[3].text == "::"
                    && window[4].text == "Literal"
            }) {
                self.violations.insert(format!(
                    "planner-scan-model-iceberg-literal-macro-carrier: {}",
                    self.source.path
                ));
            }
            syn::visit::visit_item_macro(self, item);
        }
    }

    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "planner-scan-model-literal-audit-parse-failed: {}",
            source.path
        )]);
    };
    let (imports, aliases) = ebd_4b1_module_scope_inputs(&file);
    let mut violations = imports
        .into_iter()
        .filter(|import| {
            import.segments == ["iceberg", "spec", "Literal"]
                || import.segments == ["iceberg", "spec", "*"]
        })
        .map(|import| {
            format!(
                "planner-scan-model-iceberg-literal-import: {}|{}",
                source.path,
                import.segments.join("::")
            )
        })
        .collect::<BTreeSet<_>>();
    let mut audit = LiteralAudit {
        source,
        aliases: &aliases,
        inline_modules: Vec::new(),
        violations: BTreeSet::new(),
    };
    syn::visit::Visit::visit_file(&mut audit, &file);
    violations.extend(audit.violations);
    violations
}

#[derive(Default)]
struct Ebd4b3cDynamicSnapshot {
    violations: BTreeSet<String>,
    helper_definitions: usize,
    helper_begin_scan_calls: usize,
    helper_plan_splits_calls: usize,
    coordinator_helper_calls: usize,
}

fn ebd_4b3c_audit_dynamic_seam(sources: &[GuardSource]) -> Ebd4b3cDynamicSnapshot {
    fn attrs_are_test_only(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attribute| {
            if attribute.path().is_ident("test") {
                return true;
            }
            let syn::Meta::List(list) = &attribute.meta else {
                return false;
            };
            if !list.path.is_ident("cfg") {
                return false;
            }
            cfg_attribute_requires_test(&format!("#[cfg({})]", list.tokens))
        })
    }

    struct DynamicVisitor<'a> {
        source: &'a str,
        functions: Vec<String>,
        snapshot: &'a mut Ebd4b3cDynamicSnapshot,
    }
    impl DynamicVisitor<'_> {
        fn current_function(&self) -> &str {
            self.functions
                .last()
                .map(String::as_str)
                .unwrap_or("<module>")
        }

        fn enter_function(&mut self, name: String, block: &syn::Block) {
            if self.source == EBD_4B3C_CODEGEN_HELPER && name == "plan_native_starrocks_scan" {
                self.snapshot.helper_definitions += 1;
            }
            self.functions.push(name);
            syn::visit::Visit::visit_block(self, block);
            self.functions.pop();
        }

        fn record_dynamic_call(&mut self, method: &str) {
            let in_helper = self.source == EBD_4B3C_CODEGEN_HELPER
                && self.current_function() == "plan_native_starrocks_scan";
            let in_coordinator = self.source == EBD_4B3C_COORDINATOR_ENTRY;
            let in_connector = self.source.starts_with("src/connector/");
            if in_helper {
                if method == "begin_scan" {
                    self.snapshot.helper_begin_scan_calls += 1;
                } else {
                    self.snapshot.helper_plan_splits_calls += 1;
                }
            } else if !in_coordinator && !in_connector {
                self.snapshot.violations.insert(format!(
                    "planner-scan-dynamic-orchestration: {}|{}|{}",
                    self.source,
                    self.current_function(),
                    method
                ));
            }
        }

        fn record_encoder_lookup(&mut self, method: &str) {
            if self.source.starts_with("src/sql/codegen/proto_encode/")
                && matches!(
                    method,
                    "get_table"
                        | "get_table_in_catalog"
                        | "get_table_with_mode"
                        | "catalog_backend"
                        | "table_source"
                        | "scan_planner"
                        | "load_table"
                        | "load_table_for_read"
                )
            {
                self.snapshot.violations.insert(format!(
                    "planner-scan-encoder-requery: {}|{}|{}",
                    self.source,
                    self.current_function(),
                    method
                ));
            }
        }
    }
    impl<'ast> syn::visit::Visit<'ast> for DynamicVisitor<'_> {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            self.enter_function(item.sig.ident.to_string(), &item.block);
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            self.enter_function(item.sig.ident.to_string(), &item.block);
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            syn::visit::visit_item_mod(self, item);
        }

        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            syn::visit::visit_item_impl(self, item);
        }

        fn visit_expr_method_call(&mut self, item: &'ast syn::ExprMethodCall) {
            let method = item.method.to_string();
            if matches!(method.as_str(), "begin_scan" | "plan_splits") {
                self.record_dynamic_call(&method);
            }
            self.record_encoder_lookup(&method);
            syn::visit::visit_expr_method_call(self, item);
        }

        fn visit_expr_call(&mut self, item: &'ast syn::ExprCall) {
            if let syn::Expr::Path(function) = item.func.as_ref()
                && let Some(method) = function.path.segments.last()
            {
                let method = method.ident.to_string();
                if matches!(method.as_str(), "begin_scan" | "plan_splits") {
                    self.record_dynamic_call(&method);
                }
                self.record_encoder_lookup(&method);
                if method == "plan_native_starrocks_scan" {
                    if self.source == EBD_4B3C_COORDINATOR_ENTRY
                        && self.current_function() == "prepare_scan_node"
                    {
                        self.snapshot.coordinator_helper_calls += 1;
                    } else {
                        self.snapshot.violations.insert(format!(
                            "planner-scan-helper-call-escape: {}|{}",
                            self.source,
                            self.current_function()
                        ));
                    }
                }
            }
            syn::visit::visit_expr_call(self, item);
        }
    }

    let mut snapshot = Ebd4b3cDynamicSnapshot::default();
    for source in sources
        .iter()
        .filter(|source| source.path.starts_with("src/"))
    {
        let Ok(file) = syn::parse_file(&source.text) else {
            snapshot.violations.insert(format!(
                "planner-scan-dynamic-parse-failed: {}",
                source.path
            ));
            continue;
        };
        let mut visitor = DynamicVisitor {
            source: &source.path,
            functions: Vec::new(),
            snapshot: &mut snapshot,
        };
        syn::visit::Visit::visit_file(&mut visitor, &file);

        if source.path.starts_with("src/sql/analyzer/")
            || source.path.starts_with("src/sql/optimizer/")
        {
            for path in rust_production_canonical_paths(&source.text, &source.path) {
                let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
                if segments.starts_with(&["crate", "connector", "scan_planning"])
                    && segments.get(3).is_some_and(|name| {
                        matches!(
                            *name,
                            "ConnectorScanPlanner" | "ScanHandle" | "Split" | "TableHandle"
                        )
                    })
                {
                    snapshot.violations.insert(format!(
                        "planner-scan-static-layer-dynamic-type: {}|{}",
                        source.path,
                        path.join("::")
                    ));
                }
            }
        }

        if source.path.starts_with("src/sql/codegen/proto_encode/") {
            for path in rust_production_canonical_paths(&source.text, &source.path) {
                let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
                if segments.starts_with(&["crate", "sql", "catalog"])
                    && segments
                        .get(3)
                        .is_some_and(|name| *name == "CatalogProvider")
                    || segments.starts_with(&["crate", "connector"])
                        && segments.iter().any(|name| {
                            matches!(
                                *name,
                                "ConnectorRegistry"
                                    | "CatalogBackend"
                                    | "TableSource"
                                    | "ConnectorScanPlanner"
                            )
                        })
                {
                    snapshot.violations.insert(format!(
                        "planner-scan-encoder-requery-dependency: {}|{}",
                        source.path,
                        path.join("::")
                    ));
                }
            }
        }

        if source.path.starts_with("src/connector/")
            && rust_source_tokens(&rust_sanitized_production_text(&source.text))
                .iter()
                .any(|token| token.text == "ConnectorScanPlanner")
        {
            for path in rust_production_canonical_paths(&source.text, &source.path) {
                if path
                    == ["crate", "sql", "catalog", "CatalogProvider"]
                        .into_iter()
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                {
                    snapshot.violations.insert(format!(
                        "planner-scan-connector-catalog-provider: {}",
                        source.path
                    ));
                }
            }
        }
    }
    snapshot
}

fn ebd_4b3c_completion_violations(sources: &[GuardSource]) -> BTreeSet<String> {
    let mut violations = BTreeSet::new();
    for expected_owner in [EBD_4B3C_PLANNER_OWNER, EBD_4B3E_CONNECTOR_OWNER] {
        if let Some(owner) = sources.iter().find(|source| source.path == expected_owner) {
            violations.extend(ebd_4b3c_audit_owner(owner));
        } else {
            violations.insert(format!(
                "planner-scan-model-owner-missing: {expected_owner}"
            ));
        }
    }
    let audited_sources = sources
        .iter()
        .filter(|source| source.path != "tests/architecture_guard/ebd_1_engine_boundary.rs")
        .cloned()
        .collect::<Vec<_>>();
    for planner_source in audited_sources
        .iter()
        .filter(|source| source.path.starts_with("src/sql/planner/"))
    {
        violations.extend(ebd_4b3e_audit_planner_literal_carrier(planner_source));
    }
    violations.extend(ebd_4b3c_audit_paths_definitions_and_forwarding(
        &audited_sources,
    ));
    let dynamic = ebd_4b3c_audit_dynamic_seam(&audited_sources);
    violations.extend(dynamic.violations);
    for (surface, actual, expected) in [
        ("helper-definition", dynamic.helper_definitions, 1),
        ("helper-begin-scan", dynamic.helper_begin_scan_calls, 0),
        ("helper-plan-splits", dynamic.helper_plan_splits_calls, 0),
        (
            "coordinator-helper-call",
            dynamic.coordinator_helper_calls,
            1,
        ),
    ] {
        if actual != expected {
            violations.insert(format!(
                "planner-scan-dynamic-seam-count: {surface}|expected={expected} actual={actual}"
            ));
        }
    }
    violations
}

#[test]
fn ebd_4b3c_detector_covers_owner_paths_forwarding_and_noise() {
    let allowed = [
        GuardSource::new(
            "src/sql/analyzer/allowed.rs",
            r###"
	use crate::sql::planner::table::{ScanSource, TableDef};
	use crate::connector::iceberg::scan_model::IcebergTableInfo;
	use crate::sql::catalog::TableLookupMode;
fn lookup(mode: TableLookupMode) {
    let _ = TableLookupMode::IcebergMetadata { metadata_table_type: kind() };
    let _: crate::proto::plan::TableDef = protobuf();
    let _: plan::ScanSource = protobuf();
}
"###,
        ),
        GuardSource::new(
            "tests/fixtures/ebd_4b3c_noise.rs",
            r###"
// crate::sql::catalog::TableDef
const TEXT: &str = "crate::engine::ScanSource";
const RAW: &str = r#"use crate::sql::catalog::TableDef;"#;
"###,
        ),
        GuardSource::new(
            EBD_4B3C_COORDINATOR_ENTRY,
            "fn prepare_scan_node() { let _ = plan_native_starrocks_scan(); planner.begin_scan(); planner.plan_splits(); }",
        ),
        GuardSource::new(
            EBD_4B3C_CODEGEN_HELPER,
            "fn plan_native_starrocks_scan() {}",
        ),
    ];
    assert!(
        ebd_4b3c_audit_paths_definitions_and_forwarding(&allowed).is_empty(),
        "legal canonical imports, lookup mode, protobuf paths, and lexical noise must remain allowed"
    );
    let dynamic = ebd_4b3c_audit_dynamic_seam(&allowed);
    assert!(
        dynamic.violations.is_empty()
            && dynamic.helper_definitions == 1
            && dynamic.helper_begin_scan_calls == 0
            && dynamic.helper_plan_splits_calls == 0
            && dynamic.coordinator_helper_calls == 1,
        "the coordinator entry and its one allowlisted helper must remain legal: {:?}",
        dynamic.violations
    );

    let invalid = [
        GuardSource::new("src/sql/direct.rs", "use crate::sql::catalog::TableDef;"),
        GuardSource::new(
            "src/sql/grouped.rs",
            "use crate::sql::catalog::{ScanSource, TableDef as Legacy};",
        ),
        GuardSource::new(
            "src/sql/module_alias.rs",
            "use crate::sql::catalog as legacy; type Local = legacy::TableDef;",
        ),
        GuardSource::new("src/sql/glob.rs", "use crate::sql::catalog::*;"),
        GuardSource::new(
            "src/sql/catalog/nested/relative.rs",
            "use super::super::TableDef;",
        ),
        GuardSource::new(
            "src/engine/forward.rs",
            "pub use crate::sql::planner::table::ScanSource;",
        ),
        GuardSource::new(
            "src/sql/alias.rs",
            "pub type Legacy = crate::sql::planner::table::TableDef;",
        ),
        GuardSource::new(
            "src/sql/secondary.rs",
            "pub struct TableDef { pub name: String }",
        ),
        GuardSource::new(
            "src/sql/test_owner.rs",
            "#[cfg(test)] mod tests { pub enum ScanSource { Fake } }",
        ),
        GuardSource::new(
            "src/sql/macro_owner.rs",
            "macro_rules! owner { () => { struct TableDef; } } owner!();",
        ),
        GuardSource::new(
            "src/sql/wrapper.rs",
            "pub struct LegacyTable(pub crate::sql::planner::table::TableDef);",
        ),
        GuardSource::new(
            "src/sql/named_wrapper.rs",
            "pub struct LegacyTable { pub inner: crate::sql::planner::table::TableDef }",
        ),
        GuardSource::new(
            "src/sql/retired_direct.rs",
            "use crate::sql::planner::table::IcebergTableInfo;",
        ),
        GuardSource::new(
            "src/sql/retired_alias.rs",
            "use crate::sql::planner::table as old; type Legacy = old::IcebergDataFileInfo;",
        ),
        GuardSource::new(
            "src/sql/retired_glob.rs",
            "use crate::sql::planner::table::*;",
        ),
        GuardSource::new(
            "src/sql/connector_forward.rs",
            "pub use crate::connector::iceberg::scan_model::IcebergTableInfo;",
        ),
        GuardSource::new(
            "src/sql/connector_wrapper.rs",
            "pub struct LegacyIceberg(pub crate::connector::iceberg::scan_model::IcebergTableInfo);",
        ),
        GuardSource::new(
            "src/sql/private_connector_wrapper.rs",
            "struct LegacyIceberg(crate::connector::iceberg::scan_model::IcebergTableInfo);",
        ),
        GuardSource::new(
            "src/sql/multifield_connector_wrapper.rs",
            "pub struct LegacyIceberg { inner: crate::connector::iceberg::scan_model::IcebergTableInfo, marker: u8 }",
        ),
        GuardSource::new(
            "src/sql/payload_connector_wrapper.rs",
            "pub struct LegacyIceberg { payload: crate::connector::iceberg::scan_model::IcebergTableInfo }",
        ),
    ];
    let violations = ebd_4b3c_audit_paths_definitions_and_forwarding(&invalid);
    for fixture in [
        "direct.rs",
        "grouped.rs",
        "module_alias.rs",
        "glob.rs",
        "relative.rs",
        "forward.rs",
        "alias.rs",
        "secondary.rs",
        "test_owner.rs",
        "macro_owner.rs",
        "wrapper.rs",
        "named_wrapper.rs",
        "retired_direct.rs",
        "retired_alias.rs",
        "retired_glob.rs",
        "connector_forward.rs",
        "connector_wrapper.rs",
        "private_connector_wrapper.rs",
        "multifield_connector_wrapper.rs",
        "payload_connector_wrapper.rs",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(fixture)),
            "planner model detector missed {fixture}: {violations:?}"
        );
    }

    let planner_literal = GuardSource::new(
        EBD_4B3C_PLANNER_OWNER,
        "use iceberg::spec::Literal; pub struct PlannerCarrier { value: Literal }",
    );
    assert!(
        !ebd_4b3e_audit_planner_literal_carrier(&planner_literal).is_empty(),
        "planner Iceberg literal carrier must fail closed"
    );

    let dynamic_invalid = [
        GuardSource::new(
            "src/sql/analyzer/bad.rs",
            "fn analyze() { planner.begin_scan(); }",
        ),
        GuardSource::new(
            "src/sql/optimizer/bad.rs",
            "fn optimize() { planner.plan_splits(); }",
        ),
        GuardSource::new(
            "src/sql/codegen/bad.rs",
            "fn encode() { ConnectorScanPlanner::begin_scan(planner); plan_native_starrocks_scan(); }",
        ),
        GuardSource::new(
            "src/sql/codegen/proto_encode/requery.rs",
            "use crate::connector::ConnectorRegistry; use crate::sql::catalog::CatalogProvider; fn encode() { registry.scan_planner(); catalog.get_table(\"db\", \"t\"); }",
        ),
        GuardSource::new(
            "src/connector/bad.rs",
            "use crate::sql::catalog::CatalogProvider; impl ConnectorScanPlanner for Bad {}",
        ),
    ];
    let dynamic = ebd_4b3c_audit_dynamic_seam(&dynamic_invalid);
    for fixture in [
        "analyzer/bad.rs",
        "optimizer/bad.rs",
        "codegen/bad.rs",
        "proto_encode/requery.rs",
        "connector/bad.rs",
    ] {
        assert!(
            dynamic
                .violations
                .iter()
                .any(|violation| violation.contains(fixture)),
            "dynamic seam detector missed {fixture}: {:?}",
            dynamic.violations
        );
    }
}

#[test]
fn ebd_4b3e_detector_rejects_literal_carriers_across_the_planner() {
    let mut sources = ebd_4b1_collect_repo_sources();
    sources.push(GuardSource::new(
        "src/sql/planner/optimizer_bridge/literal_carrier.rs",
        "struct PlannerCarrier { value: iceberg::spec::Literal }",
    ));
    sources.push(GuardSource::new(
        "src/sql/planner/optimizer_bridge/literal_macro_carrier.rs",
        "macro_rules! carrier { () => { struct PlannerCarrier { value: iceberg::spec::Literal } }; }",
    ));
    let violations = ebd_4b3c_completion_violations(&sources);
    for fixture in ["literal_carrier.rs", "literal_macro_carrier.rs"] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(fixture)),
            "planner-wide Iceberg literal carrier escaped completion guard for {fixture}: {violations:?}"
        );
    }
}

#[test]
fn ebd_4b3c_planner_scan_model_owner_is_complete() {
    let sources = ebd_4b1_collect_repo_sources();
    let violations = ebd_4b3c_completion_violations(&sources);
    assert!(
        violations.is_empty(),
        "EBD-4B3C planner scan model owner cutover failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

const EBD_4B3G_OWNER: &str = "src/connector/iceberg/scan_range.rs";
const EBD_4B3G_COORDINATOR: &str = "src/coordinator/prepare/scan_preparation.rs";
const EBD_4B3G_APIS: &[&str] = &[
    "equality_delete_required_columns",
    "plan_iceberg_scan_ranges",
];
const EBD_4B3G_CONCRETE_ADAPTER_INPUTS: &[&str] = &[
    "ScanHandle",
    "Split",
    "IcebergScanHandle",
    "IcebergSplit",
    "IcebergDataFileInfo",
    "IcebergDeleteFileInfo",
];
const EBD_4B3G_OPERATION_SYMBOLS: &[&str] = &[
    "ConnectorScanContext",
    "NativeFileScanPlan",
    "PruningColumn",
    "to_native_file_scan",
    "iceberg_to_native_file_scan",
    "build_iceberg_native_scan_ranges",
    "pruning_columns_for_scan",
    "build_native_file_scan_range_params_for_file",
    "native_file_pruning_min_max_values",
    "find_column_stats",
    "native_min_max_value_from_stats",
    "decode_bool_bound",
    "decode_int_bound_for_type",
    "decode_float_bound_for_type",
    "plan_hdfs_file_splits",
    "validate_iceberg_delete_apply_cost",
    "build_native_file_scan_range_params",
    "equality_delete_required_columns",
    "IcebergScanRangeContext",
    "PlannedIcebergScanRanges",
    "plan_iceberg_scan_ranges",
];
const EBD_4B3G_REQUIRED_OWNER_SYMBOLS: &[&str] = &[
    "IcebergScanRangeContext",
    "PlannedIcebergScanRanges",
    "equality_delete_required_columns",
    "plan_iceberg_scan_ranges",
];
const EBD_4B3G_EXCLUSIVE_OWNER_SYMBOLS: &[&str] = &[
    "ConnectorScanContext",
    "NativeFileScanPlan",
    "PruningColumn",
    "to_native_file_scan",
    "iceberg_to_native_file_scan",
    "build_iceberg_native_scan_ranges",
    "pruning_columns_for_scan",
    "build_native_file_scan_range_params_for_file",
    "native_file_pruning_min_max_values",
    "plan_hdfs_file_splits",
    "validate_iceberg_delete_apply_cost",
    "build_native_file_scan_range_params",
    "equality_delete_required_columns",
    "IcebergScanRangeContext",
    "PlannedIcebergScanRanges",
    "plan_iceberg_scan_ranges",
];

fn ebd_4b3g_connector_scan_range_path(path: &[String]) -> bool {
    path.iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .starts_with(&["crate", "connector", "iceberg", "scan_range"])
}

fn ebd_4b3g_api_for_path(path: &[String]) -> Option<&'static str> {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    EBD_4B3G_APIS.iter().copied().find(|api| {
        segments == ["crate", "connector", "iceberg", "scan_range", *api]
            || (segments.len() == 1 && segments[0] == *api)
    })
}

fn ebd_4b3g_concrete_adapter_path(path: &[String]) -> bool {
    let segments = path.iter().map(String::as_str).collect::<Vec<_>>();
    (segments.starts_with(&["crate", "connector", "scan_planning"])
        && segments
            .get(3)
            .is_some_and(|name| EBD_4B3G_CONCRETE_ADAPTER_INPUTS.contains(name)))
        || (segments.starts_with(&["crate", "connector", "iceberg", "scan_planner"])
            && segments
                .get(4)
                .is_some_and(|name| EBD_4B3G_CONCRETE_ADAPTER_INPUTS.contains(name)))
        || (segments.starts_with(&["crate", "connector", "iceberg", "scan_model"])
            && segments
                .get(4)
                .is_some_and(|name| EBD_4B3G_CONCRETE_ADAPTER_INPUTS.contains(name)))
        || segments.starts_with(&["crate", "connector", "iceberg", "file_pruning"])
}

type Ebd4b3gForwardingMap = BTreeMap<Vec<String>, Vec<Vec<String>>>;

fn ebd_4b3g_forwarding_map(sources: &[GuardSource]) -> Ebd4b3gForwardingMap {
    let mut forwarding = Ebd4b3gForwardingMap::new();
    for source in sources {
        let aliases = rust_production_scoped_aliases(&source.text);
        let Some(source_module) = rust_source_module_segments(&source.path) else {
            continue;
        };
        for import in rust_raw_production_use_statements(&source.text)
            .into_iter()
            .filter(|import| import.visibility != "private")
        {
            let Some(export_name) =
                forwarding_export_name(&import.path.segments, import.path.alias.as_deref())
            else {
                continue;
            };
            let Some(targets) = resolve_forwarding_paths(
                &import.path.segments,
                &source.path,
                &import.inline_modules,
                &aliases,
                &mut BTreeSet::new(),
                0,
            ) else {
                continue;
            };
            let mut export = source_module.clone();
            export.extend(import.inline_modules.iter().cloned());
            export.push(export_name);
            for target in targets {
                let Some(canonical) = rust_canonical_path_segments_in_scope(
                    &target.segments,
                    &source.path,
                    &target.inline_modules,
                ) else {
                    continue;
                };
                let targets = forwarding.entry(export.clone()).or_default();
                if !targets.contains(&canonical) {
                    targets.push(canonical);
                }
            }
        }
    }
    forwarding
}

fn ebd_4b3g_expand_forwarding_path(
    path: &[String],
    forwarding: &Ebd4b3gForwardingMap,
    resolving: &mut BTreeSet<Vec<String>>,
    depth: usize,
) -> Vec<Vec<String>> {
    if depth > forwarding.len() || !resolving.insert(path.to_vec()) {
        return Vec::new();
    }
    let prefix = (1..=path.len())
        .rev()
        .find(|length| forwarding.contains_key(&path[..*length]));
    let mut resolved = BTreeSet::new();
    if let Some(prefix) = prefix {
        for target in &forwarding[&path[..prefix]] {
            let mut candidate = target.clone();
            candidate.extend_from_slice(&path[prefix..]);
            resolved.extend(ebd_4b3g_expand_forwarding_path(
                &candidate,
                forwarding,
                resolving,
                depth + 1,
            ));
        }
    } else {
        resolved.insert(path.to_vec());
    }
    resolving.remove(path);
    resolved.into_iter().collect()
}

fn ebd_4b3g_resolve_source_path(
    path: &[String],
    source: &GuardSource,
    inline_modules: &[String],
    aliases: &RustScopedAliases,
    local_aliases: &[BTreeMap<String, Vec<Vec<String>>>],
    forwarding: &Ebd4b3gForwardingMap,
) -> Vec<Vec<String>> {
    if path.len() == 1 && EBD_4B3G_APIS.contains(&path[0].as_str()) {
        return vec![path.to_vec()];
    }
    if let Some(targets) = local_aliases
        .iter()
        .rev()
        .find_map(|scope| scope.get(&path[0]))
    {
        return targets
            .iter()
            .flat_map(|target| {
                let mut target = target.clone();
                target.extend_from_slice(&path[1..]);
                ebd_4b3g_expand_forwarding_path(&target, forwarding, &mut BTreeSet::new(), 0)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
    }
    let resolved = resolve_forwarding_paths(
        path,
        &source.path,
        inline_modules,
        aliases,
        &mut BTreeSet::new(),
        0,
    )
    .unwrap_or_else(|| {
        vec![RustScopedUsePath {
            segments: path.to_vec(),
            inline_modules: inline_modules.to_vec(),
        }]
    });
    resolved
        .into_iter()
        .filter_map(|path| {
            rust_canonical_path_segments_in_scope(
                &path.segments,
                &source.path,
                &path.inline_modules,
            )
        })
        .flat_map(|path| {
            ebd_4b3g_expand_forwarding_path(&path, forwarding, &mut BTreeSet::new(), 0)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn ebd_4b3g_production_symbol_counts(source: &GuardSource) -> BTreeMap<String, usize> {
    rust_source_tokens(&rust_sanitized_production_text(&source.text))
        .into_iter()
        .filter(|token| EBD_4B3G_OPERATION_SYMBOLS.contains(&token.text.as_str()))
        .fold(BTreeMap::new(), |mut counts, token| {
            *counts.entry(token.text).or_default() += 1;
            counts
        })
}

fn ebd_4b3g_owned_definitions(source: &GuardSource) -> BTreeMap<String, usize> {
    fn attrs_are_test_only(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attribute| {
            if attribute.path().is_ident("test") {
                return true;
            }
            let syn::Meta::List(list) = &attribute.meta else {
                return false;
            };
            list.path.is_ident("cfg")
                && cfg_attribute_requires_test(&format!("#[cfg({})]", list.tokens))
        })
    }

    struct DefinitionVisitor {
        counts: BTreeMap<String, usize>,
    }
    impl DefinitionVisitor {
        fn record(&mut self, name: String) {
            if EBD_4B3G_OPERATION_SYMBOLS.contains(&name.as_str()) {
                *self.counts.entry(name).or_default() += 1;
            }
        }
    }
    impl<'ast> syn::visit::Visit<'ast> for DefinitionVisitor {
        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            if !attrs_are_test_only(&item.attrs) {
                self.record(item.ident.to_string());
                syn::visit::visit_item_struct(self, item);
            }
        }

        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            if !attrs_are_test_only(&item.attrs) {
                self.record(item.ident.to_string());
                syn::visit::visit_item_enum(self, item);
            }
        }

        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if !attrs_are_test_only(&item.attrs) {
                self.record(item.sig.ident.to_string());
                syn::visit::visit_item_fn(self, item);
            }
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if !attrs_are_test_only(&item.attrs) {
                self.record(item.sig.ident.to_string());
                syn::visit::visit_impl_item_fn(self, item);
            }
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !attrs_are_test_only(&item.attrs) {
                syn::visit::visit_item_mod(self, item);
            }
        }
    }

    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeMap::new();
    };
    let mut visitor = DefinitionVisitor {
        counts: BTreeMap::new(),
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);

    let tokens = rust_source_tokens(&rust_sanitized_production_text(&source.text));
    for window in tokens.windows(2) {
        if matches!(window[0].text.as_str(), "fn" | "struct" | "enum")
            && EBD_4B3G_OPERATION_SYMBOLS.contains(&window[1].text.as_str())
            && !visitor.counts.contains_key(&window[1].text)
        {
            *visitor.counts.entry(window[1].text.clone()).or_default() += 1;
        }
    }
    visitor.counts
}

fn ebd_4b3g_owner_public_surface_violations(source: &GuardSource) -> BTreeSet<String> {
    const CALLABLE_APIS: &[&str] = &[
        "equality_delete_required_columns",
        "plan_iceberg_scan_ranges",
    ];
    const TYPE_APIS: &[&str] = &["IcebergScanRangeContext", "PlannedIcebergScanRanges"];

    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::from([format!(
            "iceberg-scan-range-owner-public-surface: {}|parse-failed",
            source.path
        )]);
    };
    struct SurfaceVisitor {
        modules: Vec<String>,
        impls: Vec<String>,
        callable: BTreeMap<String, String>,
        types: BTreeMap<String, String>,
    }
    impl SurfaceVisitor {
        fn qualified(&self, name: &str) -> String {
            self.modules
                .iter()
                .chain(self.impls.iter())
                .map(String::as_str)
                .chain(std::iter::once(name))
                .collect::<Vec<_>>()
                .join("::")
        }
    }
    impl<'ast> syn::visit::Visit<'ast> for SurfaceVisitor {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            let name = item.sig.ident.to_string();
            let visibility = ebd_4b3c_visibility(&item.vis);
            if visibility != "private" {
                self.callable.insert(self.qualified(&name), visibility);
            }
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            let visibility = ebd_4b3c_visibility(&item.vis);
            if visibility != "private" {
                self.callable
                    .insert(self.qualified(&item.sig.ident.to_string()), visibility);
            }
        }

        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            let name = item.ident.to_string();
            if TYPE_APIS.contains(&name.as_str()) {
                self.types
                    .insert(self.qualified(&name), ebd_4b3c_visibility(&item.vis));
            }
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            let Some((_, items)) = &item.content else {
                return;
            };
            self.modules.push(item.ident.to_string());
            for item in items {
                syn::visit::Visit::visit_item(self, item);
            }
            self.modules.pop();
        }

        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            let name = match item.self_ty.as_ref() {
                syn::Type::Path(path) => path
                    .path
                    .segments
                    .last()
                    .map(|segment| segment.ident.to_string())
                    .unwrap_or_else(|| "<impl>".to_string()),
                _ => "<impl>".to_string(),
            };
            self.impls.push(name);
            for item in &item.items {
                syn::visit::Visit::visit_impl_item(self, item);
            }
            self.impls.pop();
        }

        fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
            let visibility = ebd_4b3c_visibility(&item.vis);
            if visibility == "private" {
                return;
            }
            self.impls.push(item.ident.to_string());
            for trait_item in &item.items {
                if let syn::TraitItem::Fn(method) = trait_item {
                    self.callable.insert(
                        self.qualified(&method.sig.ident.to_string()),
                        visibility.clone(),
                    );
                }
            }
            self.impls.pop();
        }
    }

    let mut surface = SurfaceVisitor {
        modules: Vec::new(),
        impls: Vec::new(),
        callable: BTreeMap::new(),
        types: BTreeMap::new(),
    };
    syn::visit::Visit::visit_file(&mut surface, &file);
    for import in rust_raw_production_use_statements(&source.text)
        .into_iter()
        .filter(|import| import.visibility != "private")
    {
        let Some(export_name) =
            forwarding_export_name(&import.path.segments, import.path.alias.as_deref())
        else {
            continue;
        };
        let qualified = import
            .inline_modules
            .iter()
            .map(String::as_str)
            .chain(std::iter::once(export_name.as_str()))
            .collect::<Vec<_>>()
            .join("::");
        surface.callable.insert(qualified, import.visibility);
    }

    let expected_callable = CALLABLE_APIS
        .iter()
        .map(|name| ((*name).to_string(), "pub(crate)".to_string()))
        .collect::<BTreeMap<_, _>>();
    let expected_types = TYPE_APIS
        .iter()
        .map(|name| ((*name).to_string(), "pub(crate)".to_string()))
        .collect::<BTreeMap<_, _>>();
    let mut violations = BTreeSet::new();
    if surface.callable != expected_callable {
        violations.insert(format!(
            "iceberg-scan-range-owner-public-callables: expected={expected_callable:?} actual={:?}",
            surface.callable
        ));
    }
    if surface.types != expected_types {
        violations.insert(format!(
            "iceberg-scan-range-owner-public-types: expected={expected_types:?} actual={:?}",
            surface.types
        ));
    }
    violations
}

#[derive(Default)]
struct Ebd4b3gReferenceSnapshot {
    violations: BTreeSet<String>,
    allowed_direct_calls: BTreeMap<String, usize>,
}

fn ebd_4b3g_reference_audit(
    source: &GuardSource,
    forwarding: &Ebd4b3gForwardingMap,
) -> Ebd4b3gReferenceSnapshot {
    fn attrs_are_test_only(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attribute| {
            if attribute.path().is_ident("test") {
                return true;
            }
            let syn::Meta::List(list) = &attribute.meta else {
                return false;
            };
            list.path.is_ident("cfg")
                && cfg_attribute_requires_test(&format!("#[cfg({})]", list.tokens))
        })
    }

    fn canonical_api_path(path: &[String]) -> Option<&'static str> {
        ebd_4b3g_api_for_path(path)
    }

    fn syntactic_canonical_api(path: &syn::Path) -> Option<&'static str> {
        let segments = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        let strings = segments.iter().map(String::as_str).collect::<Vec<_>>();
        EBD_4B3G_APIS
            .iter()
            .copied()
            .find(|api| strings == ["crate", "connector", "iceberg", "scan_range", *api])
    }

    struct ReferenceVisitor<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        forwarding: &'a Ebd4b3gForwardingMap,
        inline_modules: Vec<String>,
        functions: Vec<String>,
        snapshot: Ebd4b3gReferenceSnapshot,
    }
    impl ReferenceVisitor<'_> {
        fn current_function(&self) -> String {
            let function = self
                .functions
                .last()
                .cloned()
                .unwrap_or_else(|| "<module>".to_string());
            if self.inline_modules.is_empty() {
                function
            } else {
                format!("{}::{function}", self.inline_modules.join("::"))
            }
        }

        fn resolved_apis(&self, path: &syn::Path) -> BTreeSet<&'static str> {
            let segments = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            ebd_4b3g_resolve_source_path(
                &segments,
                self.source,
                &self.inline_modules,
                self.aliases,
                &[],
                self.forwarding,
            )
            .into_iter()
            .filter_map(|path| canonical_api_path(&path))
            .collect()
        }

        fn record_reference(&mut self, api: &str, kind: &str) {
            self.snapshot.violations.insert(format!(
                "iceberg-scan-range-api-reference: {}|{}|{}|{}",
                self.source.path,
                self.current_function(),
                kind,
                api
            ));
        }

        fn record_macro_tokens(&mut self, mac: &syn::Macro) {
            let tokens = rust_source_tokens(&mac.tokens.to_string())
                .into_iter()
                .map(|token| token.text)
                .collect::<Vec<_>>();
            for api in EBD_4B3G_APIS {
                let canonical = [
                    "crate",
                    "::",
                    "connector",
                    "::",
                    "iceberg",
                    "::",
                    "scan_range",
                    "::",
                    api,
                ];
                if tokens
                    .windows(canonical.len())
                    .any(|window| window.iter().map(String::as_str).eq(canonical))
                    || tokens.iter().any(|token| token == api)
                {
                    self.record_reference(api, "macro");
                }
            }
        }
    }
    impl<'ast> syn::visit::Visit<'ast> for ReferenceVisitor<'_> {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            self.functions.push(item.sig.ident.to_string());
            syn::visit::Visit::visit_block(self, &item.block);
            self.functions.pop();
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            self.functions.push(item.sig.ident.to_string());
            syn::visit::Visit::visit_block(self, &item.block);
            self.functions.pop();
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            let Some((_, items)) = &item.content else {
                return;
            };
            self.inline_modules.push(item.ident.to_string());
            for item in items {
                syn::visit::Visit::visit_item(self, item);
            }
            self.inline_modules.pop();
        }

        fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
            if let syn::Expr::Path(function) = call.func.as_ref() {
                let mut apis = self.resolved_apis(&function.path);
                if function.qself.is_some()
                    && let Some(api) = function
                        .path
                        .segments
                        .last()
                        .map(|segment| segment.ident.to_string())
                        .and_then(|name| EBD_4B3G_APIS.iter().copied().find(|api| name == *api))
                {
                    apis.insert(api);
                }
                if !apis.is_empty() {
                    for api in apis {
                        let allowed = self.source.path == EBD_4B3G_COORDINATOR
                            && self.current_function() == "plan_iceberg_file_ranges"
                            && function.qself.is_none()
                            && syntactic_canonical_api(&function.path) == Some(api);
                        if allowed {
                            *self
                                .snapshot
                                .allowed_direct_calls
                                .entry(api.to_string())
                                .or_default() += 1;
                        } else {
                            self.record_reference(api, "call");
                        }
                    }
                    for argument in &call.args {
                        syn::visit::Visit::visit_expr(self, argument);
                    }
                    return;
                }
            }
            syn::visit::visit_expr_call(self, call);
        }

        fn visit_expr_path(&mut self, path: &'ast syn::ExprPath) {
            let mut apis = self.resolved_apis(&path.path);
            if path.qself.is_some()
                && let Some(api) = path
                    .path
                    .segments
                    .last()
                    .map(|segment| segment.ident.to_string())
                    .and_then(|name| EBD_4B3G_APIS.iter().copied().find(|api| name == *api))
            {
                apis.insert(api);
            }
            for api in apis {
                self.record_reference(api, "value");
            }
        }

        fn visit_expr_macro(&mut self, item: &'ast syn::ExprMacro) {
            self.record_macro_tokens(&item.mac);
        }

        fn visit_stmt_macro(&mut self, item: &'ast syn::StmtMacro) {
            self.record_macro_tokens(&item.mac);
        }

        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            if !attrs_are_test_only(&item.attrs) {
                self.record_macro_tokens(&item.mac);
            }
        }
    }

    let Ok(file) = syn::parse_file(&source.text) else {
        return Ebd4b3gReferenceSnapshot {
            violations: BTreeSet::from([format!(
                "iceberg-scan-range-reference-parse-failed: {}",
                source.path
            )]),
            allowed_direct_calls: BTreeMap::new(),
        };
    };
    let (_, aliases) = ebd_4b1_module_scope_inputs(&file);
    let mut visitor = ReferenceVisitor {
        source,
        aliases: &aliases,
        forwarding,
        inline_modules: Vec::new(),
        functions: Vec::new(),
        snapshot: Ebd4b3gReferenceSnapshot::default(),
    };

    for import in rust_raw_production_use_statements(&source.text) {
        let resolved = resolve_forwarding_paths(
            &import.path.segments,
            &source.path,
            &import.inline_modules,
            &aliases,
            &mut BTreeSet::new(),
            0,
        )
        .unwrap_or_else(|| {
            vec![RustScopedUsePath {
                segments: import.path.segments,
                inline_modules: import.inline_modules,
            }]
        });
        for path in resolved {
            let Some(canonical) = rust_canonical_path_segments_in_scope(
                &path.segments,
                &source.path,
                &path.inline_modules,
            ) else {
                continue;
            };
            for target in
                ebd_4b3g_expand_forwarding_path(&canonical, forwarding, &mut BTreeSet::new(), 0)
            {
                if let Some(api) = canonical_api_path(&target) {
                    visitor.snapshot.violations.insert(format!(
                        "iceberg-scan-range-api-reference: {}|<module>|import|{}",
                        source.path, api
                    ));
                }
            }
        }
    }
    syn::visit::Visit::visit_file(&mut visitor, &file);
    visitor.snapshot
}

fn ebd_4b3g_production_paths(
    source: &GuardSource,
    forwarding: &Ebd4b3gForwardingMap,
) -> BTreeSet<Vec<String>> {
    rust_production_canonical_paths(&source.text, &source.path)
        .into_iter()
        .flat_map(|path| {
            ebd_4b3g_expand_forwarding_path(&path, forwarding, &mut BTreeSet::new(), 0)
        })
        .collect()
}

fn ebd_4b3g_operational_surface_violations(
    source: &GuardSource,
    forwarding: &Ebd4b3gForwardingMap,
) -> BTreeSet<String> {
    let paths = ebd_4b3g_production_paths(source, forwarding);
    let adapter_dependencies = paths
        .iter()
        .filter(|path| ebd_4b3g_connector_scan_range_path(path))
        .map(|path| path.join("::"))
        .collect::<BTreeSet<_>>();
    let concrete_dependencies = paths
        .iter()
        .filter(|path| ebd_4b3g_concrete_adapter_path(path))
        .map(|path| path.join("::"))
        .collect::<BTreeSet<_>>();
    let strong_codegen_input = paths.iter().any(|path| {
        ["ScanHandle", "Split", "IcebergScanHandle", "IcebergSplit"]
            .iter()
            .any(|name| path.last().is_some_and(|leaf| leaf == name))
            && ebd_4b3g_concrete_adapter_path(path)
    });
    let pruning_dependency = paths.iter().any(|path| {
        path.iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .starts_with(&["crate", "connector", "iceberg", "file_pruning"])
    });

    let mut violations = BTreeSet::new();
    if source.path.starts_with("src/sql/codegen/")
        && (!adapter_dependencies.is_empty() || strong_codegen_input || pruning_dependency)
    {
        violations.insert(format!(
            "iceberg-scan-range-codegen-concrete-adapter: {}|adapter={adapter_dependencies:?}|concrete={concrete_dependencies:?}",
            source.path
        ));
    }
    if source.path != EBD_4B3G_OWNER
        && source.path != EBD_4B3G_COORDINATOR
        && !adapter_dependencies.is_empty()
    {
        violations.insert(format!(
            "iceberg-scan-range-forwarding-surface: {}|{adapter_dependencies:?}",
            source.path
        ));
    }
    violations.extend(ebd_4b3g_semantic_owner_violations(source));
    violations
}

#[derive(Clone)]
struct Ebd4b3gMacroRule {
    matcher: Vec<String>,
    transcriber: Vec<String>,
}

fn ebd_4b3g_macro_rules(item: &syn::ItemMacro) -> Vec<Ebd4b3gMacroRule> {
    if !item.mac.path.is_ident("macro_rules") || item.ident.is_none() {
        return Vec::new();
    }
    let tokens = ebd_4b1_macro_tokens(item);
    let mut rules = Vec::new();
    let mut index = 0usize;
    while index < tokens.len() {
        if !matches!(tokens.get(index).map(String::as_str), Some("(" | "[" | "{")) {
            index += 1;
            continue;
        }
        let matcher_open = index;
        let Some(matcher_close) = ebd_4b1_matching_macro_group(&tokens, matcher_open) else {
            break;
        };
        if tokens
            .get(matcher_close + 1)
            .is_none_or(|token| token != "=")
            || tokens
                .get(matcher_close + 2)
                .is_none_or(|token| token != ">")
            || !matches!(
                tokens.get(matcher_close + 3).map(String::as_str),
                Some("(" | "[" | "{")
            )
        {
            index = matcher_close + 1;
            continue;
        }
        let transcriber_open = matcher_close + 3;
        let Some(transcriber_close) = ebd_4b1_matching_macro_group(&tokens, transcriber_open)
        else {
            break;
        };
        rules.push(Ebd4b3gMacroRule {
            matcher: tokens[matcher_open + 1..matcher_close].to_vec(),
            transcriber: tokens[transcriber_open + 1..transcriber_close].to_vec(),
        });
        index = transcriber_close + 1;
    }
    rules
}

fn ebd_4b3g_macro_arguments(mac: &syn::Macro) -> Vec<String> {
    rust_source_tokens(&mac.tokens.to_string())
        .into_iter()
        .map(|token| token.text)
        .collect()
}

fn ebd_4b3g_expand_macro_rule(
    rule: &Ebd4b3gMacroRule,
    arguments: &[String],
) -> Option<Vec<String>> {
    fn unsupported_repetition(matcher: &[String]) -> bool {
        matcher
            .windows(2)
            .any(|window| window[0] == "$" && matches!(window[1].as_str(), "(" | "[" | "{"))
    }

    fn conservative_operational_expansion(
        rule: &Ebd4b3gMacroRule,
        arguments: &[String],
    ) -> Option<Vec<String>> {
        let transcriber_tokens = rule
            .transcriber
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let uses_variables = rule.transcriber.windows(2).any(|window| {
            window[0] == "$"
                && window[1] != "crate"
                && !matches!(window[1].as_str(), "(" | "[" | "{")
        });
        let has_operational_shape = [
            "ScanRangeParams",
            "FileScanRange",
            "IcebergDeleteFile",
            "DeletionVectorDescriptor",
            "FilePruningMinMaxValue",
            "file_pruning",
        ]
        .iter()
        .any(|token| transcriber_tokens.contains(token));
        if !uses_variables || !has_operational_shape || arguments.is_empty() {
            return None;
        }
        let mut expanded = rule.transcriber.clone();
        expanded.extend(arguments.iter().cloned());
        Some(expanded)
    }

    fn balanced(tokens: &[String]) -> bool {
        let mut groups = Vec::new();
        for token in tokens {
            match token.as_str() {
                "(" | "[" | "{" => groups.push(token.as_str()),
                ")" => {
                    if groups.pop() != Some("(") {
                        return false;
                    }
                }
                "]" => {
                    if groups.pop() != Some("[") {
                        return false;
                    }
                }
                "}" => {
                    if groups.pop() != Some("{") {
                        return false;
                    }
                }
                _ => {}
            }
        }
        groups.is_empty()
    }

    fn fragment_matches(fragment: &str, tokens: &[String]) -> bool {
        if tokens.is_empty() || !balanced(tokens) {
            return false;
        }
        match fragment {
            "ident" | "lifetime" => tokens.len() == 1,
            "path" => syn::parse_str::<syn::Path>(&tokens.join(" ")).is_ok(),
            _ => true,
        }
    }

    fn bind_matcher(
        matcher: &[String],
        arguments: &[String],
        matcher_index: usize,
        argument_index: usize,
        bindings: &BTreeMap<String, Vec<String>>,
    ) -> Option<BTreeMap<String, Vec<String>>> {
        if matcher_index == matcher.len() {
            return (argument_index == arguments.len()).then(|| bindings.clone());
        }
        if matcher.get(matcher_index).is_some_and(|token| token == "$")
            && matcher
                .get(matcher_index + 2)
                .is_some_and(|token| token == ":")
        {
            let name = matcher.get(matcher_index + 1)?;
            let fragment = matcher.get(matcher_index + 3)?;
            for end in (argument_index + 1..=arguments.len()).rev() {
                let candidate = &arguments[argument_index..end];
                if !fragment_matches(fragment, candidate) {
                    continue;
                }
                let mut next_bindings = bindings.clone();
                if let Some(existing) = next_bindings.get(name)
                    && existing != candidate
                {
                    continue;
                }
                next_bindings.insert(name.clone(), candidate.to_vec());
                if let Some(bound) =
                    bind_matcher(matcher, arguments, matcher_index + 4, end, &next_bindings)
                {
                    return Some(bound);
                }
            }
            return None;
        }
        if matcher.get(matcher_index) != arguments.get(argument_index) {
            return None;
        }
        bind_matcher(
            matcher,
            arguments,
            matcher_index + 1,
            argument_index + 1,
            bindings,
        )
    }

    let bindings = match bind_matcher(&rule.matcher, arguments, 0, 0, &BTreeMap::new()) {
        Some(bindings) => bindings,
        None if unsupported_repetition(&rule.matcher) => {
            return conservative_operational_expansion(rule, arguments);
        }
        None => return None,
    };
    let mut expanded = Vec::new();
    let mut index = 0usize;
    while index < rule.transcriber.len() {
        if rule.transcriber[index] == "$"
            && let Some(param) = rule.transcriber.get(index + 1)
            && let Some(argument) = bindings.get(param)
        {
            expanded.extend(argument.iter().cloned());
            index += 2;
        } else {
            expanded.push(rule.transcriber[index].clone());
            index += 1;
        }
    }
    Some(expanded)
}

fn ebd_4b3g_expanded_macro_invocations(file: &syn::File) -> Vec<(String, Vec<String>)> {
    struct MacroCollector {
        definitions: BTreeMap<String, Vec<Ebd4b3gMacroRule>>,
        invocations: Vec<(String, Vec<String>)>,
    }
    impl<'ast> syn::visit::Visit<'ast> for MacroCollector {
        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            if item.mac.path.is_ident("macro_rules") {
                if let Some(name) = &item.ident {
                    self.definitions
                        .insert(name.to_string(), ebd_4b3g_macro_rules(item));
                }
            } else if let Some(name) = item.mac.path.segments.last() {
                self.invocations
                    .push((name.ident.to_string(), ebd_4b3g_macro_arguments(&item.mac)));
            }
            syn::visit::visit_item_macro(self, item);
        }

        fn visit_expr_macro(&mut self, item: &'ast syn::ExprMacro) {
            if let Some(name) = item.mac.path.segments.last() {
                self.invocations
                    .push((name.ident.to_string(), ebd_4b3g_macro_arguments(&item.mac)));
            }
        }

        fn visit_stmt_macro(&mut self, item: &'ast syn::StmtMacro) {
            if let Some(name) = item.mac.path.segments.last() {
                self.invocations
                    .push((name.ident.to_string(), ebd_4b3g_macro_arguments(&item.mac)));
            }
        }
    }

    let mut collector = MacroCollector {
        definitions: BTreeMap::new(),
        invocations: Vec::new(),
    };
    syn::visit::Visit::visit_file(&mut collector, file);
    collector
        .invocations
        .into_iter()
        .flat_map(|(name, arguments)| {
            collector
                .definitions
                .get(&name)
                .into_iter()
                .flatten()
                .filter_map(move |rule| {
                    ebd_4b3g_expand_macro_rule(rule, &arguments)
                        .map(|expanded| (name.clone(), expanded))
                })
        })
        .collect()
}

fn ebd_4b3g_semantic_owner_violations(source: &GuardSource) -> BTreeSet<String> {
    fn attrs_are_test_only(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attribute| {
            if attribute.path().is_ident("test") {
                return true;
            }
            let syn::Meta::List(list) = &attribute.meta else {
                return false;
            };
            list.path.is_ident("cfg")
                && cfg_attribute_requires_test(&format!("#[cfg({})]", list.tokens))
        })
    }

    fn operational_owner_tokens(tokens: &BTreeSet<String>) -> bool {
        let strong_input = ["ScanHandle", "Split", "IcebergScanHandle", "IcebergSplit"]
            .iter()
            .any(|name| tokens.contains(*name));
        let data_file_input = ["IcebergDataFileInfo", "IcebergDeleteFileInfo"]
            .iter()
            .any(|name| tokens.contains(*name));
        let range_operation = [
            "ScanRangeParams",
            "FileScanRange",
            "IcebergDeleteFile",
            "DeletionVectorDescriptor",
            "FilePruningMinMaxValue",
        ]
        .iter()
        .any(|name| tokens.contains(*name));
        let pruning_operation = tokens.contains("file_pruning");
        (strong_input && (range_operation || pruning_operation))
            || (data_file_input && range_operation)
    }

    fn collect_type_aliases(
        items: &[syn::Item],
        inline_modules: &mut Vec<String>,
        aliases: &mut RustScopedAliases,
    ) {
        for item in items {
            match item {
                syn::Item::Type(item) => {
                    if let Some(path) = ebd_4b1_direct_alias_rhs_path(&item.ty) {
                        aliases.insert(
                            (inline_modules.clone(), item.ident.to_string()),
                            vec![RustScopedUsePath {
                                segments: path,
                                inline_modules: inline_modules.clone(),
                            }],
                        );
                    }
                }
                syn::Item::Mod(item) => {
                    let Some((_, items)) = &item.content else {
                        continue;
                    };
                    inline_modules.push(item.ident.to_string());
                    collect_type_aliases(items, inline_modules, aliases);
                    inline_modules.pop();
                }
                _ => {}
            }
        }
    }

    struct PathTokens<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        inline_modules: &'a [String],
        tokens: BTreeSet<String>,
    }
    impl<'ast> syn::visit::Visit<'ast> for PathTokens<'_> {
        fn visit_path(&mut self, path: &'ast syn::Path) {
            let segments = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            let resolved = resolve_forwarding_paths(
                &segments,
                &self.source.path,
                self.inline_modules,
                self.aliases,
                &mut BTreeSet::new(),
                0,
            )
            .unwrap_or_else(|| {
                vec![RustScopedUsePath {
                    segments,
                    inline_modules: self.inline_modules.to_vec(),
                }]
            });
            for path in resolved {
                if let Some(canonical) = rust_canonical_path_segments_in_scope(
                    &path.segments,
                    &self.source.path,
                    &path.inline_modules,
                ) {
                    self.tokens.extend(canonical);
                }
            }
            syn::visit::visit_path(self, path);
        }

        fn visit_item_fn(&mut self, _item: &'ast syn::ItemFn) {}

        fn visit_impl_item_fn(&mut self, _item: &'ast syn::ImplItemFn) {}
    }

    struct SemanticVisitor<'a> {
        source: &'a GuardSource,
        aliases: &'a RustScopedAliases,
        inline_modules: Vec<String>,
        violations: BTreeSet<String>,
    }
    impl SemanticVisitor<'_> {
        fn function_tokens(
            &self,
            signature: &syn::Signature,
            block: &syn::Block,
        ) -> BTreeSet<String> {
            let mut paths = PathTokens {
                source: self.source,
                aliases: self.aliases,
                inline_modules: &self.inline_modules,
                tokens: BTreeSet::new(),
            };
            syn::visit::Visit::visit_signature(&mut paths, signature);
            syn::visit::Visit::visit_block(&mut paths, block);
            paths.tokens
        }

        fn record_function(&mut self, name: &str, signature: &syn::Signature, block: &syn::Block) {
            if self.source.path == EBD_4B3G_OWNER
                || (self.source.path == EBD_4B3G_COORDINATOR
                    && self.inline_modules.is_empty()
                    && name == "plan_iceberg_file_ranges")
            {
                return;
            }
            if operational_owner_tokens(&self.function_tokens(signature, block)) {
                let qualified = if self.inline_modules.is_empty() {
                    name.to_string()
                } else {
                    format!("{}::{name}", self.inline_modules.join("::"))
                };
                self.violations.insert(format!(
                    "iceberg-scan-range-secondary-operational-owner: {}|{}",
                    self.source.path, qualified
                ));
            }
        }
    }
    impl<'ast> syn::visit::Visit<'ast> for SemanticVisitor<'_> {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            self.record_function(&item.sig.ident.to_string(), &item.sig, &item.block);
            syn::visit::visit_item_fn(self, item);
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            self.record_function(&item.sig.ident.to_string(), &item.sig, &item.block);
            syn::visit::visit_impl_item_fn(self, item);
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            let Some((_, items)) = &item.content else {
                return;
            };
            self.inline_modules.push(item.ident.to_string());
            for item in items {
                syn::visit::Visit::visit_item(self, item);
            }
            self.inline_modules.pop();
        }

        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            if attrs_are_test_only(&item.attrs) || !item.mac.path.is_ident("macro_rules") {
                return;
            }
            for transcriber in ebd_4b1_macro_rule_transcribers(item) {
                let tokens = transcriber.into_iter().collect::<BTreeSet<_>>();
                if operational_owner_tokens(&tokens) {
                    self.violations.insert(format!(
                        "iceberg-scan-range-secondary-operational-owner: {}|macro|{}",
                        self.source.path,
                        item.ident
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "<anonymous>".to_string())
                    ));
                }
            }
        }
    }

    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::new();
    };
    let (_, mut aliases) = ebd_4b1_module_scope_inputs(&file);
    collect_type_aliases(&file.items, &mut Vec::new(), &mut aliases);
    let mut visitor = SemanticVisitor {
        source,
        aliases: &aliases,
        inline_modules: Vec::new(),
        violations: BTreeSet::new(),
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    if source.path != EBD_4B3G_OWNER {
        for (macro_name, expanded) in ebd_4b3g_expanded_macro_invocations(&file) {
            let mut tokens = expanded.iter().cloned().collect::<BTreeSet<_>>();
            for token in &expanded {
                let path = vec![token.clone()];
                let Some(resolved) =
                    rust_resolve_scoped_paths(&path, &[], &aliases, &mut BTreeSet::new(), 0)
                else {
                    continue;
                };
                for resolved in resolved {
                    if let Some(canonical) = rust_canonical_path_segments_in_scope(
                        &resolved.segments,
                        &source.path,
                        &resolved.inline_modules,
                    ) {
                        tokens.extend(canonical);
                    }
                }
            }
            if operational_owner_tokens(&tokens) {
                visitor.violations.insert(format!(
                    "iceberg-scan-range-secondary-operational-owner: {}|macro|{}",
                    source.path, macro_name
                ));
            }
        }
    }
    visitor.violations
}

fn ebd_4b3g_audit(sources: &[GuardSource], require_completion: bool) -> BTreeSet<String> {
    let mut violations = BTreeSet::new();
    let audited_sources = sources
        .iter()
        .filter(|source| source.path != "tests/architecture_guard/ebd_1_engine_boundary.rs")
        .collect::<Vec<_>>();
    let forwarding =
        ebd_4b3g_forwarding_map(&audited_sources.iter().cloned().cloned().collect::<Vec<_>>());
    let mut allowed_direct_calls = BTreeMap::<String, usize>::new();

    for source in &audited_sources {
        violations.extend(ebd_4b3g_operational_surface_violations(source, &forwarding));
        let references = ebd_4b3g_reference_audit(source, &forwarding);
        violations.extend(references.violations);
        for (api, count) in references.allowed_direct_calls {
            *allowed_direct_calls.entry(api).or_default() += count;
        }
        let symbol_counts = ebd_4b3g_production_symbol_counts(source);
        let adapter_path = rust_production_canonical_paths(&source.text, &source.path)
            .iter()
            .any(|path| ebd_4b3g_connector_scan_range_path(path));
        if source.path.starts_with("src/sql/codegen/")
            && (!symbol_counts.is_empty() || adapter_path)
        {
            violations.insert(format!(
                "iceberg-scan-range-codegen-owner: {}|symbols={:?}|adapter_path={adapter_path}",
                source.path, symbol_counts
            ));
        }
        if source.path != EBD_4B3G_OWNER {
            for (symbol, count) in ebd_4b3g_owned_definitions(source) {
                if EBD_4B3G_EXCLUSIVE_OWNER_SYMBOLS.contains(&symbol.as_str()) {
                    violations.insert(format!(
                        "iceberg-scan-range-secondary-owner: {}|{symbol}|count={count}",
                        source.path
                    ));
                }
            }
        }
    }

    if !require_completion {
        return violations;
    }

    let Some(owner) = audited_sources
        .iter()
        .find(|source| source.path == EBD_4B3G_OWNER)
    else {
        violations.insert(format!(
            "iceberg-scan-range-owner-missing: {EBD_4B3G_OWNER}"
        ));
        return violations;
    };
    let owner_definitions = ebd_4b3g_owned_definitions(owner);
    violations.extend(ebd_4b3g_owner_public_surface_violations(owner));
    for symbol in EBD_4B3G_REQUIRED_OWNER_SYMBOLS {
        let actual = owner_definitions.get(*symbol).copied().unwrap_or_default();
        if actual != 1 {
            violations.insert(format!(
                "iceberg-scan-range-owner-definition: {symbol}|expected=1 actual={actual}"
            ));
        }
    }

    for symbol in EBD_4B3G_APIS {
        let actual = allowed_direct_calls
            .get(*symbol)
            .copied()
            .unwrap_or_default();
        if actual != 1 {
            violations.insert(format!(
                "iceberg-scan-range-resolved-call-count: {symbol}|expected=1 actual={actual}"
            ));
        }
    }

    let dynamic =
        ebd_4b3c_audit_dynamic_seam(&audited_sources.into_iter().cloned().collect::<Vec<_>>());
    violations.extend(
        dynamic
            .violations
            .into_iter()
            .filter(|violation| violation.contains("encoder-requery")),
    );
    violations
}

#[test]
fn ebd_4b3g_detector_covers_direct_alias_ufcs_forwarding_macro_and_noise() {
    let invalid = [
        GuardSource::new(
            "src/sql/codegen/direct.rs",
            "fn emit() { crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges(); }",
        ),
        GuardSource::new(
            "src/sql/codegen/alias.rs",
            "use crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges as emit; fn encode() { emit(); }",
        ),
        GuardSource::new(
            "src/sql/codegen/module_alias.rs",
            "use crate::connector::iceberg::scan_range as adapter; fn encode() { adapter::plan_iceberg_scan_ranges(); }",
        ),
        GuardSource::new(
            "src/sql/codegen/ufcs.rs",
            "fn encode() { <IcebergScanRangeContext as Adapter>::plan_iceberg_scan_ranges(); }",
        ),
        GuardSource::new(
            "src/sql/codegen/helper_forward.rs",
            "fn forward() { plan_iceberg_scan_ranges(); } fn encode() { forward(); }",
        ),
        GuardSource::new(
            "src/sql/codegen/macro.rs",
            "macro_rules! emit { () => { plan_iceberg_scan_ranges(); } } emit!();",
        ),
        GuardSource::new(
            "src/adapter_facade.rs",
            "pub use crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges as emit;",
        ),
        GuardSource::new(
            "src/sql/codegen/facade_codegen.rs",
            "use crate::adapter_facade::emit; fn encode() { emit(); }",
        ),
        GuardSource::new(
            "src/sql/codegen/renamed_rewrite.rs",
            r#"
use crate::connector::iceberg::scan_model::{IcebergDataFileInfo, IcebergDeleteFileInfo};
use crate::connector::iceberg::scan_planner::{IcebergScanHandle, IcebergSplit};
use crate::connector::scan_planning::{ScanHandle, Split};
fn emit_ranges(
    scan: &ScanHandle,
    native: &IcebergScanHandle,
    splits: &[Split],
    iceberg_splits: &[IcebergSplit],
    files: &[IcebergDataFileInfo],
    deletes: &[IcebergDeleteFileInfo],
) {
    crate::connector::iceberg::file_pruning::file_may_satisfy_scan_predicates();
}
"#,
        ),
        GuardSource::new(
            "src/coordinator/macro_secondary_owner.rs",
            r#"
macro_rules! adapter {
    ($name:ident) => {
        fn $name(
            scan: &crate::connector::scan_planning::ScanHandle,
            splits: &[crate::connector::scan_planning::Split],
        ) -> Vec<crate::runtime::scan_range::ScanRangeParams> {
            crate::connector::iceberg::file_pruning::file_may_satisfy_scan_predicates();
            Vec::new()
        }
    };
}
adapter!(emit_ranges);
"#,
        ),
        GuardSource::new(
            "src/coordinator/nested_secondary_owner.rs",
            r#"
mod nested {
    fn emit_ranges(
        scan: &crate::connector::scan_planning::ScanHandle,
        splits: &[crate::connector::scan_planning::Split],
    ) -> Vec<crate::runtime::scan_range::ScanRangeParams> {
        crate::connector::iceberg::file_pruning::file_may_satisfy_scan_predicates();
        Vec::new()
    }
}
"#,
        ),
        GuardSource::new(
            "src/coordinator/cfg_production_secondary_owner.rs",
            r#"
#[cfg(feature = "iceberg")]
fn emit_ranges(
    scan: &crate::connector::scan_planning::ScanHandle,
    splits: &[crate::connector::scan_planning::Split],
) -> Vec<crate::runtime::scan_range::ScanRangeParams> {
    crate::connector::iceberg::file_pruning::file_may_satisfy_scan_predicates();
    Vec::new()
}
"#,
        ),
    ];
    let violations = ebd_4b3g_audit(&invalid, false);
    for fixture in [
        "direct.rs",
        "alias.rs",
        "module_alias.rs",
        "ufcs.rs",
        "helper_forward.rs",
        "macro.rs",
        "facade_codegen.rs",
        "renamed_rewrite.rs",
        "macro_secondary_owner.rs",
        "nested_secondary_owner.rs",
        "cfg_production_secondary_owner.rs",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(fixture)),
            "Iceberg scan-range detector missed {fixture}: {violations:?}"
        );
    }

    let noise = [GuardSource::new(
        "src/sql/codegen/noise.rs",
        r###"
// plan_iceberg_scan_ranges();
const TEXT: &str = "crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges";
const RAW: &str = r#"IcebergScanRangeContext"#;
#[cfg(test)]
fn test_only() { plan_iceberg_scan_ranges(); }
#[cfg(test)]
fn test_only_rewrite(_: ScanHandle, _: Split, _: IcebergDataFileInfo) {
    file_pruning::file_may_satisfy_scan_predicates();
}
fn encode_prepared_ranges() {}
"###,
    )];
    assert!(
        ebd_4b3g_audit(&noise, false).is_empty(),
        "comments, strings, test-only code, and prepared-range mapping must remain legal"
    );
}

#[test]
fn ebd_4b3g_owner_public_surface_is_exact() {
    let valid = GuardSource::new(
        EBD_4B3G_OWNER,
        r#"
pub(crate) struct IcebergScanRangeContext;
pub(crate) struct PlannedIcebergScanRanges;
pub(crate) fn equality_delete_required_columns() {}
pub(crate) fn plan_iceberg_scan_ranges() {}
fn private_helper() {}
"#,
    );
    assert!(
        ebd_4b3g_owner_public_surface_violations(&valid).is_empty(),
        "the exact typed owner surface must remain legal"
    );

    let invalid = GuardSource::new(
        EBD_4B3G_OWNER,
        r#"
pub(crate) struct IcebergScanRangeContext;
pub(crate) struct PlannedIcebergScanRanges;
pub(crate) fn equality_delete_required_columns() {}
pub(crate) fn plan_iceberg_scan_ranges() {}
pub(crate) fn renamed_adapter_helper() {}
"#,
    );
    let violations = ebd_4b3g_owner_public_surface_violations(&invalid);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("renamed_adapter_helper")),
        "a third callable API escaped the exact public-surface guard: {violations:?}"
    );

    for (fixture, source) in [
        (
            "impl_method",
            r#"
pub(crate) struct IcebergScanRangeContext;
pub(crate) struct PlannedIcebergScanRanges;
pub(crate) fn equality_delete_required_columns() {}
pub(crate) fn plan_iceberg_scan_ranges() {}
struct Adapter;
impl Adapter { pub(crate) fn third_api() {} }
"#,
        ),
        (
            "nested_module",
            r#"
pub(crate) struct IcebergScanRangeContext;
pub(crate) struct PlannedIcebergScanRanges;
pub(crate) fn equality_delete_required_columns() {}
pub(crate) fn plan_iceberg_scan_ranges() {}
mod nested { pub(crate) fn third_api() {} }
"#,
        ),
        (
            "reexport",
            r#"
pub(crate) struct IcebergScanRangeContext;
pub(crate) struct PlannedIcebergScanRanges;
pub(crate) fn equality_delete_required_columns() {}
pub(crate) fn plan_iceberg_scan_ranges() {}
fn private_helper() {}
pub(crate) use private_helper as third_api;
"#,
        ),
        (
            "external_reexport",
            r#"
pub(crate) struct IcebergScanRangeContext;
pub(crate) struct PlannedIcebergScanRanges;
pub(crate) fn equality_delete_required_columns() {}
pub(crate) fn plan_iceberg_scan_ranges() {}
pub(crate) use crate::other::third_api;
"#,
        ),
        (
            "nested_trait_method",
            r#"
pub(crate) struct IcebergScanRangeContext;
pub(crate) struct PlannedIcebergScanRanges;
pub(crate) fn equality_delete_required_columns() {}
pub(crate) fn plan_iceberg_scan_ranges() {}
mod nested {
    pub(crate) trait Extra {
        fn third_api();
    }
}
"#,
        ),
    ] {
        let source = GuardSource::new(EBD_4B3G_OWNER, source);
        let violations = ebd_4b3g_owner_public_surface_violations(&source);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("third_api")),
            "{fixture} callable escaped recursive public-surface audit: {violations:?}"
        );
    }
}

#[test]
fn ebd_4b3g_semantic_owner_rejects_coordinator_alias_and_generic_macro_rewrites() {
    let invalid = [
        GuardSource::new(
            EBD_4B3G_COORDINATOR,
            r#"
use crate::connector::iceberg::scan_model::IcebergDataFileInfo as Data;
use crate::runtime::scan_range::ScanRangeParams as Range;
fn renamed_coordinator_rewrite(file: &Data) -> Range {
    Range::file(crate::runtime::scan_range::FileScanRange::default())
}
"#,
        ),
        GuardSource::new(
            "src/coordinator/type_alias_rewrite.rs",
            r#"
type H = crate::connector::iceberg::scan_model::IcebergDataFileInfo;
type R = crate::runtime::scan_range::ScanRangeParams;
fn emit(file: &H) -> R {
    R::file(crate::runtime::scan_range::FileScanRange::default())
}
"#,
        ),
        GuardSource::new(
            "src/coordinator/generic_macro_rewrite.rs",
            r#"
type H = crate::connector::iceberg::scan_model::IcebergDataFileInfo;
type R = crate::runtime::scan_range::ScanRangeParams;
macro_rules! make_adapter {
    ($input:path, $range:path) => {
        fn emit(file: &$input) -> $range {
            $range::file(crate::runtime::scan_range::FileScanRange::default())
        }
    };
}
make_adapter!(H, R);
"#,
        ),
        GuardSource::new(
            "src/coordinator/generic_macro_separator_rewrite.rs",
            r#"
type H = crate::connector::iceberg::scan_model::IcebergDataFileInfo;
type R = crate::runtime::scan_range::ScanRangeParams;
macro_rules! make_adapter {
    ($input:path => $range:path) => {
        fn emit(file: &$input) -> $range {
            $range::file(crate::runtime::scan_range::FileScanRange::default())
        }
    };
}
make_adapter!(H => R);
"#,
        ),
        GuardSource::new(
            "src/coordinator/generic_macro_repetition_rewrite.rs",
            r#"
type H = crate::connector::iceberg::scan_model::IcebergDataFileInfo;
type R = crate::runtime::scan_range::ScanRangeParams;
macro_rules! make_adapters {
    ($( $input:path => $range:path );+) => {
        $(
            fn emit(file: &$input) -> $range {
                $range::file(crate::runtime::scan_range::FileScanRange::default())
            }
        )+
    };
}
make_adapters!(H => R);
"#,
        ),
    ];
    let violations = ebd_4b3g_audit(&invalid, false);
    for expected in [
        "scan_preparation.rs|renamed_coordinator_rewrite",
        "type_alias_rewrite.rs|emit",
        "generic_macro_rewrite.rs|macro",
        "generic_macro_separator_rewrite.rs|macro",
        "generic_macro_repetition_rewrite.rs|macro",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "semantic owner audit missed {expected}: {violations:?}"
        );
    }

    let non_operational_noise = GuardSource::new(
        "src/coordinator/generic_macro_noise.rs",
        r#"
type H = crate::connector::iceberg::scan_model::IcebergDataFileInfo;
type R = crate::runtime::scan_range::ScanRangeParams;
macro_rules! describe_types {
    ($( $input:path => $range:path );+) => {
        $(const _: &str = stringify!($input, $range);)+
    };
}
describe_types!(H => R);
"#,
    );
    assert!(
        ebd_4b3g_semantic_owner_violations(&non_operational_noise).is_empty(),
        "non-operational repetition macro noise must remain legal"
    );
}

#[test]
fn ebd_4b3g_resolved_call_sites_cover_aliases_ufcs_and_generic_macros() {
    let sources = [
        GuardSource::new(
            "src/facade_a.rs",
            "pub use crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges as emit;",
        ),
        GuardSource::new(
            "src/facade_b.rs",
            "pub use crate::facade_a::emit as forward;",
        ),
        GuardSource::new(
            "src/calls/direct.rs",
            "fn direct() { crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges(); }",
        ),
        GuardSource::new(
            "src/calls/grouped.rs",
            "use crate::connector::iceberg::scan_range::{equality_delete_required_columns as discover}; fn grouped() { discover(); }",
        ),
        GuardSource::new(
            "src/calls/module_alias.rs",
            "use crate::connector::iceberg::scan_range as adapter; fn module_alias() { adapter::plan_iceberg_scan_ranges(); }",
        ),
        GuardSource::new(
            "src/calls/local_alias.rs",
            "fn local_alias() { use crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges as emit; emit(); }",
        ),
        GuardSource::new(
            "src/calls/transitive.rs",
            "use crate::facade_b::forward; fn transitive() { forward(); }",
        ),
        GuardSource::new(
            "src/calls/function_value.rs",
            "use crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges as emit; fn function_value() { let first = emit; let second = first; second(); }",
        ),
        GuardSource::new(
            "src/calls/ufcs.rs",
            "fn ufcs() { <Adapter as Trait>::plan_iceberg_scan_ranges(); }",
        ),
        GuardSource::new(
            "src/calls/stmt_macro.rs",
            "macro_rules! invoke { ($f:path) => { $f(); } } fn stmt_macro() { invoke!(crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges); }",
        ),
        GuardSource::new(
            "src/calls/expr_macro.rs",
            "macro_rules! invoke { ($f:path) => { $f() } } fn expr_macro() { let _ = invoke!(crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges); }",
        ),
        GuardSource::new(
            "src/calls/item_macro.rs",
            "macro_rules! invoke { ($f:path) => { const _: () = (); } } invoke!(crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges);",
        ),
    ];
    let forwarding = ebd_4b3g_forwarding_map(&sources);
    let violations = sources
        .iter()
        .flat_map(|source| ebd_4b3g_reference_audit(source, &forwarding).violations)
        .collect::<BTreeSet<_>>();
    for fixture in [
        "direct.rs",
        "grouped.rs",
        "module_alias.rs",
        "local_alias.rs",
        "transitive.rs",
        "function_value.rs",
        "ufcs.rs",
        "stmt_macro.rs",
        "expr_macro.rs",
        "item_macro.rs",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(fixture)),
            "reference-site audit missed {fixture}: {violations:?}"
        );
    }

    let noise = GuardSource::new(
        "src/calls/noise.rs",
        r###"
// plan_iceberg_scan_ranges();
const TEXT: &str = "crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges";
macro_rules! unused { ($f:path) => { $f(); } }
#[cfg(test)]
fn test_only() {
    crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges();
}
"###,
    );
    assert!(
        ebd_4b3g_reference_audit(&noise, &Ebd4b3gForwardingMap::new())
            .violations
            .is_empty(),
        "noise, unused macro definitions, and test-only calls must remain legal"
    );
}

#[test]
fn ebd_4b3g_resolved_call_sites_track_scoped_value_flow_and_macro_semantics() {
    let source = GuardSource::new(
        "src/calls/value_flow.rs",
        r#"
use crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges as api;
struct Holder<F> { call: F }
fn value_flow(other: fn()) {
    let casted = api as fn();
    (casted)();
    let tuple = (api, other);
    tuple.0();
    let holder = Holder { call: api };
    (holder.call)();
    let mut assigned = other;
    assigned = api;
    assigned();
    let shadowed = api;
    {
        let shadowed = other;
        shadowed();
    }
    shadowed();
}
"#,
    );
    let violations = ebd_4b3g_reference_audit(&source, &Ebd4b3gForwardingMap::new()).violations;
    assert!(
        !violations.is_empty(),
        "cast/tuple/field/assignment/HOF references must be rejected without dataflow: {violations:?}"
    );

    let macro_source = GuardSource::new(
        "src/calls/macro_semantics.rs",
        r#"
macro_rules! invoke { ($f:path) => { $f(); } }
macro_rules! text { ($f:path) => { stringify!($f) } }
macro_rules! unused { ($f:path) => { 7 } }
fn macro_semantics() {
    invoke!(crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges);
    let _ = text!(crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges);
    let _ = unused!(crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges);
}
"#,
    );
    let violations =
        ebd_4b3g_reference_audit(&macro_source, &Ebd4b3gForwardingMap::new()).violations;
    assert!(
        !violations.is_empty(),
        "production macro references, including stringify/unused parameters, must be rejected: {violations:?}"
    );
}

#[test]
fn ebd_4b3g_call_site_guard_rejects_dead_expected_call_and_live_helper_alias() {
    let sources = [
        GuardSource::new(
            EBD_4B3G_OWNER,
            r#"
pub(crate) struct IcebergScanRangeContext;
pub(crate) struct PlannedIcebergScanRanges;
pub(crate) fn equality_delete_required_columns() {}
pub(crate) fn plan_iceberg_scan_ranges() {}
"#,
        ),
        GuardSource::new(
            EBD_4B3G_COORDINATOR,
            r#"
use crate::connector::iceberg::scan_range::{
    equality_delete_required_columns,
    plan_iceberg_scan_ranges,
};
fn plan_iceberg_file_ranges() {
    equality_delete_required_columns();
    if false { plan_iceberg_scan_ranges(); }
}
fn other_helper() {
    let live = plan_iceberg_scan_ranges as fn();
    live();
}
"#,
        ),
    ];
    let violations = ebd_4b3g_audit(&sources, true);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("scan_preparation.rs|other_helper")),
        "live cast alias in another helper escaped call-site ownership: {violations:?}"
    );
}

#[test]
fn ebd_4b3g_resolved_call_site_detector_rejects_extra_and_dead_helper_calls() {
    let sources = [
        GuardSource::new(
            EBD_4B3G_OWNER,
            r#"
pub(crate) struct IcebergScanRangeContext;
pub(crate) struct PlannedIcebergScanRanges;
pub(crate) fn equality_delete_required_columns() {}
pub(crate) fn plan_iceberg_scan_ranges() {}
"#,
        ),
        GuardSource::new(
            EBD_4B3G_COORDINATOR,
            r#"
use crate::connector::iceberg::scan_range::{
    equality_delete_required_columns as discover,
    plan_iceberg_scan_ranges as plan,
};
fn plan_iceberg_file_ranges() {
    let discover_fn = discover;
    discover_fn();
    let plan_fn = plan;
    plan_fn();
}
fn dead_helper() {
    crate::connector::iceberg::scan_range::plan_iceberg_scan_ranges();
}
"#,
        ),
        GuardSource::new(
            "src/coordinator/extra.rs",
            r#"
use crate::connector::iceberg::scan_range as adapter;
fn extra() {
    let call = adapter::equality_delete_required_columns;
    call();
}
"#,
        ),
    ];
    let violations = ebd_4b3g_audit(&sources, true);
    for expected in ["scan_preparation.rs|dead_helper", "extra.rs|extra"] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "resolved call-site detector missed {expected}: {violations:?}"
        );
    }
}

#[test]
fn ebd_4b3g_iceberg_scan_range_adapter_owner_is_complete() {
    let sources = ebd_4b1_collect_repo_sources();
    let violations = ebd_4b3g_audit(&sources, true);
    assert!(
        violations.is_empty(),
        "EBD-4B3G Iceberg scan-range adapter owner cutover failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

const EBD_4B3F_MODEL_OWNER: &str = "src/connector/scan_model/starrocks.rs";
const EBD_4B3F_FACADE_OWNER: &str = "src/connector/scan_planning/starrocks.rs";
const EBD_4B3F_COMPAT_OWNER: &str = "src/connector/starrocks/table/scan_adapter.rs";
const EBD_4B3F_LEGACY_OWNER: &str = "src/sql/codegen/scan/connector.rs";
const EBD_4B3F_COORDINATOR: &str = "src/coordinator/prepare/scan_preparation.rs";
const EBD_4B3F_DESCRIPTOR_SYMBOLS: &[&str] = &[
    "StarRocksStorageColumnDescriptor",
    "StarRocksKeysTypeDescriptor",
    "StarRocksColumnSchemaDescriptor",
    "StarRocksTabletSchemaDescriptor",
    "StarRocksScanSourceDescriptor",
    "PlannedNativeStarRocksScan",
];

fn ebd_4b3f_named_declarations(source: &str) -> BTreeMap<String, usize> {
    struct DeclarationVisitor {
        declarations: BTreeMap<String, usize>,
    }

    impl DeclarationVisitor {
        fn record(&mut self, name: &syn::Ident) {
            let name = name.to_string();
            if EBD_4B3F_DESCRIPTOR_SYMBOLS.contains(&name.as_str()) {
                *self.declarations.entry(name).or_default() += 1;
            }
        }
    }

    impl<'ast> syn::visit::Visit<'ast> for DeclarationVisitor {
        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            self.record(&item.ident);
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            self.record(&item.ident);
            syn::visit::visit_item_enum(self, item);
        }

        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            self.record(&item.ident);
            syn::visit::visit_item_type(self, item);
        }
    }

    let Ok(file) = syn::parse_file(source) else {
        return BTreeMap::new();
    };
    let mut visitor = DeclarationVisitor {
        declarations: BTreeMap::new(),
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    visitor.declarations
}

fn ebd_4b3f_symbol_call_count(source: &str, symbol: &str) -> usize {
    let tokens = rust_source_tokens(&rust_sanitized_production_text(source));
    tokens
        .windows(2)
        .enumerate()
        .filter(|(index, window)| {
            window[0].text == symbol
                && window[1].text == "("
                && index
                    .checked_sub(1)
                    .and_then(|previous| tokens.get(previous))
                    .is_none_or(|token| token.text != "fn")
        })
        .count()
}

fn ebd_4b3f_completion_violations(sources: &[GuardSource]) -> BTreeSet<String> {
    let by_path = sources
        .iter()
        .map(|source| (source.path.as_str(), source))
        .collect::<BTreeMap<_, _>>();
    let mut violations = BTreeSet::new();

    let Some(model) = by_path.get(EBD_4B3F_MODEL_OWNER) else {
        violations.insert(format!(
            "starrocks-scan-model-owner-missing: {EBD_4B3F_MODEL_OWNER}"
        ));
        return violations;
    };
    let model_declarations = ebd_4b3f_named_declarations(&model.text);
    for symbol in EBD_4B3F_DESCRIPTOR_SYMBOLS {
        let owner_count = model_declarations.get(*symbol).copied().unwrap_or_default();
        if owner_count != 1 {
            violations.insert(format!(
                "starrocks-scan-model-owner-count: {EBD_4B3F_MODEL_OWNER}|{symbol}|expected=1 actual={owner_count}"
            ));
        }
        for source in sources
            .iter()
            .filter(|source| source.path != EBD_4B3F_MODEL_OWNER)
        {
            let count = ebd_4b3f_named_declarations(&source.text)
                .get(*symbol)
                .copied()
                .unwrap_or_default();
            if count != 0 {
                violations.insert(format!(
                    "starrocks-scan-model-secondary-owner: {}|{symbol}|count={count}",
                    source.path
                ));
            }
        }
    }
    if !rust_sanitized_production_text(&model.text).contains("validate_starrocks_source_descriptor")
    {
        violations.insert(format!(
            "starrocks-scan-model-validator-missing: {EBD_4B3F_MODEL_OWNER}"
        ));
    }

    let Some(facade) = by_path.get(EBD_4B3F_FACADE_OWNER) else {
        violations.insert(format!(
            "starrocks-scan-facade-owner-missing: {EBD_4B3F_FACADE_OWNER}"
        ));
        return violations;
    };
    let facade_production = rust_sanitized_production_text(&facade.text);
    for required in [
        "plan_native_starrocks_scan",
        "plan_native_starrocks_scan_with_compat",
    ] {
        if !facade_production.contains(required) {
            violations.insert(format!(
                "starrocks-scan-facade-surface-missing: {EBD_4B3F_FACADE_OWNER}|{required}"
            ));
        }
    }
    for required in [
        "feature = \"compat\"",
        "not(feature = \"compat\")",
        "StarRocks native scan planning requires feature compat",
    ] {
        if !facade.text.contains(required) {
            violations.insert(format!(
                "starrocks-scan-facade-surface-missing: {EBD_4B3F_FACADE_OWNER}|{required}"
            ));
        }
    }
    for forbidden in ["begin_scan", "plan_splits", "starrocks_tablet"] {
        if facade_production.contains(forbidden) {
            violations.insert(format!(
                "starrocks-scan-facade-live-planning: {EBD_4B3F_FACADE_OWNER}|{forbidden}"
            ));
        }
    }

    let Some(compat) = by_path.get(EBD_4B3F_COMPAT_OWNER) else {
        violations.insert(format!(
            "starrocks-scan-compat-owner-missing: {EBD_4B3F_COMPAT_OWNER}"
        ));
        return violations;
    };
    let compat_production = rust_sanitized_production_text(&compat.text);
    for required in [
        "plan_native_starrocks_scan_with_compat",
        "begin_scan",
        "plan_splits",
        "starrocks_tablet",
        "validate_starrocks_source_descriptor",
    ] {
        if !compat_production.contains(required) {
            violations.insert(format!(
                "starrocks-scan-compat-operation-missing: {EBD_4B3F_COMPAT_OWNER}|{required}"
            ));
        }
    }

    if by_path.contains_key(EBD_4B3F_LEGACY_OWNER) {
        violations.insert(format!(
            "starrocks-scan-legacy-owner-still-present: {EBD_4B3F_LEGACY_OWNER}"
        ));
    }
    let coordinator_calls = by_path
        .get(EBD_4B3F_COORDINATOR)
        .map(|source| ebd_4b3f_symbol_call_count(&source.text, "plan_native_starrocks_scan"))
        .unwrap_or_default();
    if coordinator_calls != 1 {
        violations.insert(format!(
            "starrocks-scan-coordinator-call-count: {EBD_4B3F_COORDINATOR}|expected=1 actual={coordinator_calls}"
        ));
    }
    for source in sources
        .iter()
        .filter(|source| source.path.starts_with("src/sql/codegen/"))
    {
        let production = rust_sanitized_production_text(&source.text);
        if production.contains("plan_native_starrocks_scan") {
            violations.insert(format!(
                "starrocks-scan-codegen-reference: {}|plan_native_starrocks_scan",
                source.path
            ));
        }
    }

    violations
}

#[test]
fn ebd_4b3f_detector_rejects_secondary_owner_and_extra_coordinator_call() {
    let mut sources = ebd_4b1_collect_repo_sources();
    sources.push(GuardSource::new(
        "src/sql/codegen/secondary_owner.rs",
        "struct PlannedNativeStarRocksScan; fn encode() { plan_native_starrocks_scan(); }",
    ));
    let coordinator = sources
        .iter_mut()
        .find(|source| source.path == EBD_4B3F_COORDINATOR)
        .expect("coordinator fixture");
    coordinator
        .text
        .push_str("\nfn duplicate_orchestration() { plan_native_starrocks_scan(); }\n");
    let violations = ebd_4b3f_completion_violations(&sources);
    for expected in [
        "starrocks-scan-model-secondary-owner",
        "starrocks-scan-codegen-reference",
        "starrocks-scan-coordinator-call-count",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "EBD-4B3F detector missed {expected}: {violations:?}"
        );
    }
}

#[test]
fn ebd_4b3f_starrocks_scan_adapter_owner_is_complete() {
    let sources = ebd_4b1_collect_repo_sources();
    let violations = ebd_4b3f_completion_violations(&sources);
    assert!(
        violations.is_empty(),
        "EBD-4B3F StarRocks scan adapter owner cutover failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

const EBD_4B3D_TABLE_OWNER: &str = "src/catalog/table.rs";
const EBD_4B3D_PROVIDER_OWNER: &str = "src/catalog/provider.rs";
const EBD_4B3D_PARTITION_OWNER: &str = "src/catalog/partition.rs";
const EBD_4B3D_PLANNER_EXTENSION_OWNER: &str = "src/sql/catalog.rs";
const EBD_4B3D_ANALYZER_ENTRY: &str = "src/sql/analyzer/resolve_from.rs";
const EBD_4B3D_PLANNER_PROVIDER_OWNER: &str = "src/sql/catalog/provider.rs";

fn ebd_4b3d_named_item_count(source: &str, kind: &str, name: &str) -> usize {
    let Ok(file) = syn::parse_file(source) else {
        return 0;
    };

    struct Visitor<'a> {
        kind: &'a str,
        name: &'a str,
        count: usize,
    }

    impl syn::visit::Visit<'_> for Visitor<'_> {
        fn visit_item_struct(&mut self, item: &syn::ItemStruct) {
            if self.kind == "struct" && item.ident == self.name {
                self.count += 1;
            }
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_item_trait(&mut self, item: &syn::ItemTrait) {
            if self.kind == "trait" && item.ident == self.name {
                self.count += 1;
            }
            syn::visit::visit_item_trait(self, item);
        }

        fn visit_item_type(&mut self, item: &syn::ItemType) {
            if self.kind == "type" && item.ident == self.name {
                self.count += 1;
            }
            syn::visit::visit_item_type(self, item);
        }
    }

    let mut visitor = Visitor {
        kind,
        name,
        count: 0,
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    visitor.count
}

fn ebd_4b3d_neutral_provider_connector_calls(source: &str) -> BTreeSet<String> {
    fn attrs_are_test_only(attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attribute| {
            if attribute.path().is_ident("test") {
                return true;
            }
            let syn::Meta::List(list) = &attribute.meta else {
                return false;
            };
            list.path.is_ident("cfg")
                && cfg_attribute_requires_test(&format!("#[cfg({})]", list.tokens))
        })
    }

    #[derive(Default)]
    struct FunctionCalls {
        local: BTreeSet<String>,
        forbidden: BTreeSet<String>,
    }

    struct CallVisitor {
        calls: FunctionCalls,
    }

    impl<'ast> syn::visit::Visit<'ast> for CallVisitor {
        fn visit_expr_call(&mut self, item: &'ast syn::ExprCall) {
            if let syn::Expr::Path(path) = item.func.as_ref()
                && let Some(segment) = path.path.segments.last()
            {
                self.calls.local.insert(segment.ident.to_string());
            }
            syn::visit::visit_expr_call(self, item);
        }

        fn visit_expr_method_call(&mut self, item: &'ast syn::ExprMethodCall) {
            let method = item.method.to_string();
            if matches!(
                method.as_str(),
                "catalog_backend"
                    | "table_source"
                    | "load_table"
                    | "load_table_for_read"
                    | "build_table_def"
                    | "build_schema_table_def"
                    | "build_metadata_rows_table_def"
            ) {
                self.calls.forbidden.insert(method.clone());
            }
            if matches!(item.receiver.as_ref(), syn::Expr::Path(path) if path.path.is_ident("self"))
            {
                self.calls.local.insert(method);
            }
            syn::visit::visit_expr_method_call(self, item);
        }
    }

    let Ok(file) = syn::parse_file(source) else {
        return BTreeSet::from(["<parse-failed>".to_string()]);
    };

    struct FunctionCollector {
        functions: BTreeMap<String, Vec<FunctionCalls>>,
        roots: BTreeSet<String>,
    }

    impl<'ast> syn::visit::Visit<'ast> for FunctionCollector {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            let mut visitor = CallVisitor {
                calls: FunctionCalls::default(),
            };
            syn::visit::Visit::visit_block(&mut visitor, &item.block);
            self.functions
                .entry(item.sig.ident.to_string())
                .or_default()
                .push(visitor.calls);
            syn::visit::visit_item_fn(self, item);
        }

        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            if attrs_are_test_only(&item.attrs) {
                return;
            }
            let catalog_provider_impl = item
                .trait_
                .as_ref()
                .and_then(|(_, path, _)| path.segments.last())
                .is_some_and(|segment| segment.ident == "CatalogProvider");
            for impl_item in &item.items {
                let syn::ImplItem::Fn(function) = impl_item else {
                    continue;
                };
                if attrs_are_test_only(&function.attrs) {
                    continue;
                }
                let name = function.sig.ident.to_string();
                let mut visitor = CallVisitor {
                    calls: FunctionCalls::default(),
                };
                syn::visit::Visit::visit_block(&mut visitor, &function.block);
                self.functions
                    .entry(name.clone())
                    .or_default()
                    .push(visitor.calls);
                if catalog_provider_impl {
                    self.roots.insert(name);
                }
            }
            syn::visit::visit_item_impl(self, item);
        }
    }

    let mut collector = FunctionCollector {
        functions: BTreeMap::new(),
        roots: BTreeSet::new(),
    };
    syn::visit::Visit::visit_file(&mut collector, &file);

    let mut reachable = collector.roots.clone();
    let mut pending = collector.roots.into_iter().collect::<Vec<_>>();
    while let Some(function) = pending.pop() {
        let Some(nodes) = collector.functions.get(&function) else {
            continue;
        };
        for calls in nodes {
            for callee in &calls.local {
                if collector.functions.contains_key(callee) && reachable.insert(callee.clone()) {
                    pending.push(callee.clone());
                }
            }
        }
    }

    reachable
        .into_iter()
        .filter_map(|function| collector.functions.get(&function))
        .flatten()
        .flat_map(|calls| calls.forbidden.iter().cloned())
        .collect()
}

fn ebd_4b3d_public_reexports(source: &str) -> BTreeSet<String> {
    fn visit_tree(tree: &syn::UseTree, prefix: &mut Vec<String>, reexports: &mut BTreeSet<String>) {
        match tree {
            syn::UseTree::Path(path) => {
                prefix.push(path.ident.to_string());
                visit_tree(&path.tree, prefix, reexports);
                prefix.pop();
            }
            syn::UseTree::Name(name) => {
                let symbol = name.ident.to_string();
                if matches!(
                    symbol.as_str(),
                    "CatalogTable" | "CatalogProvider" | "LegacyRangePartition"
                ) {
                    reexports.insert(symbol);
                }
            }
            syn::UseTree::Rename(rename) => {
                for symbol in [rename.ident.to_string(), rename.rename.to_string()] {
                    if matches!(
                        symbol.as_str(),
                        "CatalogTable" | "CatalogProvider" | "LegacyRangePartition"
                    ) {
                        reexports.insert(symbol);
                    }
                }
            }
            syn::UseTree::Group(group) => {
                for item in &group.items {
                    visit_tree(item, prefix, reexports);
                }
            }
            syn::UseTree::Glob(_) => {
                let path = prefix.join("::");
                if path.ends_with("catalog::table") {
                    reexports.insert("CatalogTable".to_string());
                } else if path.ends_with("catalog::provider") {
                    reexports.insert("CatalogProvider".to_string());
                } else if path.ends_with("catalog::partition") {
                    reexports.insert("LegacyRangePartition".to_string());
                }
            }
        }
    }

    struct Visitor {
        reexports: BTreeSet<String>,
    }

    impl syn::visit::Visit<'_> for Visitor {
        fn visit_item_use(&mut self, item: &syn::ItemUse) {
            if !matches!(item.vis, syn::Visibility::Inherited) {
                visit_tree(&item.tree, &mut Vec::new(), &mut self.reexports);
            }
            syn::visit::visit_item_use(self, item);
        }
    }

    let Ok(file) = syn::parse_file(source) else {
        return BTreeSet::from(["<parse-failed>".to_string()]);
    };
    let mut visitor = Visitor {
        reexports: BTreeSet::new(),
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    visitor.reexports
}

fn ebd_4b3d_macro_secondary_owners(source: &str) -> BTreeSet<String> {
    struct Visitor {
        owners: BTreeSet<String>,
    }

    impl syn::visit::Visit<'_> for Visitor {
        fn visit_item_macro(&mut self, item: &syn::ItemMacro) {
            let tokens = item.mac.tokens.to_string();
            for (kind, symbol) in [
                ("struct", "CatalogTable"),
                ("type", "CatalogTable"),
                ("trait", "CatalogProvider"),
                ("type", "CatalogProvider"),
                ("struct", "LegacyRangePartition"),
                ("type", "LegacyRangePartition"),
            ] {
                if tokens.contains(&format!("{kind} {symbol}")) {
                    self.owners.insert(symbol.to_string());
                }
            }
            syn::visit::visit_item_macro(self, item);
        }
    }

    let Ok(file) = syn::parse_file(source) else {
        return BTreeSet::from(["<parse-failed>".to_string()]);
    };
    let mut visitor = Visitor {
        owners: BTreeSet::new(),
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    visitor.owners
}

fn ebd_4b3d_completion_violations(sources: &[GuardSource]) -> BTreeSet<String> {
    let sources = sources
        .iter()
        .filter(|source| source.path != "tests/architecture_guard/ebd_1_engine_boundary.rs")
        .map(|source| (source.path.as_str(), source))
        .collect::<BTreeMap<_, _>>();
    let mut violations = BTreeSet::new();

    for (path, kind, symbol) in [
        (EBD_4B3D_TABLE_OWNER, "struct", "CatalogTable"),
        (EBD_4B3D_PROVIDER_OWNER, "trait", "CatalogProvider"),
        (EBD_4B3D_PARTITION_OWNER, "struct", "LegacyRangePartition"),
        (
            EBD_4B3D_PLANNER_EXTENSION_OWNER,
            "struct",
            "ResolvedAnalyzerTable",
        ),
        (
            EBD_4B3D_PLANNER_EXTENSION_OWNER,
            "trait",
            "PlannerTableProvider",
        ),
        (
            EBD_4B3D_PLANNER_EXTENSION_OWNER,
            "trait",
            "IcebergMetadataTableProvider",
        ),
    ] {
        let count = sources
            .get(path)
            .map(|source| ebd_4b3d_named_item_count(&source.text, kind, symbol))
            .unwrap_or_default();
        if count != 1 {
            violations.insert(format!(
                "neutral-table-owner-count: {path}|{kind}|{symbol}|expected=1 actual={count}"
            ));
        }
    }

    for path in [EBD_4B3D_TABLE_OWNER, EBD_4B3D_PROVIDER_OWNER] {
        let Some(source) = sources.get(path) else {
            continue;
        };
        let production = rust_sanitized_production_text(&source.text);
        for forbidden in ["TableDef", "ScanSource", "Iceberg"] {
            if production.contains(forbidden) {
                violations.insert(format!(
                    "neutral-table-owner-dependency: {path}|{forbidden}"
                ));
            }
        }
        for dependency in rust_production_canonical_paths(&source.text, path) {
            if dependency.starts_with(&["crate".to_string(), "sql".to_string()])
                || dependency.starts_with(&["crate".to_string(), "engine".to_string()])
                || dependency.starts_with(&["crate".to_string(), "connector".to_string()])
            {
                violations.insert(format!(
                    "neutral-table-owner-dependency: {path}|{}",
                    dependency.join("::")
                ));
            }
        }
    }

    for source in sources.values() {
        let production = rust_sanitized_production_text(&source.text);
        for retired in ["TableLookupMode", "get_table_with_mode"] {
            if production.contains(retired) {
                violations.insert(format!(
                    "neutral-table-retired-surface: {}|{retired}",
                    source.path
                ));
            }
        }
        let legacy_count =
            ebd_4b3d_named_item_count(&source.text, "struct", "LegacyRangePartition");
        let legacy_alias_count =
            ebd_4b3d_named_item_count(&source.text, "type", "LegacyRangePartition");
        if source.path != EBD_4B3D_PARTITION_OWNER && (legacy_count != 0 || legacy_alias_count != 0)
        {
            violations.insert(format!(
                "neutral-table-partition-secondary-owner: {}|count={}",
                source.path,
                legacy_count + legacy_alias_count
            ));
        }
        let catalog_provider_count =
            ebd_4b3d_named_item_count(&source.text, "trait", "CatalogProvider");
        let catalog_provider_alias_count =
            ebd_4b3d_named_item_count(&source.text, "type", "CatalogProvider");
        if source.path != EBD_4B3D_PROVIDER_OWNER
            && (catalog_provider_count != 0 || catalog_provider_alias_count != 0)
        {
            violations.insert(format!(
                "neutral-table-provider-secondary-owner: {}|count={}",
                source.path,
                catalog_provider_count + catalog_provider_alias_count
            ));
        }
        let catalog_table_count = ebd_4b3d_named_item_count(&source.text, "struct", "CatalogTable");
        let catalog_table_alias_count =
            ebd_4b3d_named_item_count(&source.text, "type", "CatalogTable");
        if source.path != EBD_4B3D_TABLE_OWNER
            && (catalog_table_count != 0 || catalog_table_alias_count != 0)
        {
            violations.insert(format!(
                "neutral-table-model-secondary-owner: {}|count={}",
                source.path,
                catalog_table_count + catalog_table_alias_count
            ));
        }
        for symbol in ebd_4b3d_macro_secondary_owners(&source.text) {
            let canonical_owner = match symbol.as_str() {
                "CatalogTable" => EBD_4B3D_TABLE_OWNER,
                "CatalogProvider" => EBD_4B3D_PROVIDER_OWNER,
                "LegacyRangePartition" => EBD_4B3D_PARTITION_OWNER,
                _ => continue,
            };
            if source.path != canonical_owner {
                violations.insert(format!(
                    "neutral-table-macro-secondary-owner: {}|{symbol}",
                    source.path
                ));
            }
        }
        for symbol in ebd_4b3d_public_reexports(&source.text) {
            violations.insert(format!(
                "neutral-table-public-reexport: {}|{symbol}",
                source.path
            ));
        }
    }

    let planner_extension = sources
        .get(EBD_4B3D_PLANNER_EXTENSION_OWNER)
        .map(|source| rust_sanitized_production_text(&source.text))
        .unwrap_or_default();
    for required in [
        "CatalogTable",
        "resolve_table_for_analysis",
        "get_iceberg_metadata_table",
    ] {
        if !planner_extension.contains(required) {
            violations.insert(format!(
                "neutral-table-planner-extension-missing: {EBD_4B3D_PLANNER_EXTENSION_OWNER}|{required}"
            ));
        }
    }
    if let Some(source) = sources.get(EBD_4B3D_PLANNER_EXTENSION_OWNER) {
        let mut planner_methods = BTreeSet::new();
        if let Ok(file) = syn::parse_file(&source.text) {
            struct Visitor<'a> {
                methods: &'a mut BTreeSet<String>,
            }
            impl syn::visit::Visit<'_> for Visitor<'_> {
                fn visit_item_trait(&mut self, item: &syn::ItemTrait) {
                    if item.ident == "PlannerTableProvider" {
                        self.methods.extend(item.items.iter().filter_map(|item| {
                            let syn::TraitItem::Fn(function) = item else {
                                return None;
                            };
                            Some(function.sig.ident.to_string())
                        }));
                    }
                    syn::visit::visit_item_trait(self, item);
                }
            }
            syn::visit::Visit::visit_file(
                &mut Visitor {
                    methods: &mut planner_methods,
                },
                &file,
            );
        }
        let allowed = BTreeSet::from([
            "resolve_table_for_analysis".to_string(),
            "iceberg_metadata_provider".to_string(),
        ]);
        for method in planner_methods.difference(&allowed) {
            violations.insert(format!(
                "neutral-table-planner-extra-entry: {EBD_4B3D_PLANNER_EXTENSION_OWNER}|{method}"
            ));
        }
    }

    let analyzer = sources
        .get(EBD_4B3D_ANALYZER_ENTRY)
        .map(|source| rust_sanitized_production_text(&source.text))
        .unwrap_or_default();
    for required in ["resolve_table_for_analysis", "get_iceberg_metadata_table"] {
        if !analyzer.contains(required) {
            violations.insert(format!(
                "neutral-table-analyzer-seam-missing: {EBD_4B3D_ANALYZER_ENTRY}|{required}"
            ));
        }
    }

    let engine_provider = sources
        .get(EBD_4B3D_PLANNER_PROVIDER_OWNER)
        .map(|source| rust_sanitized_production_text(&source.text))
        .unwrap_or_default();
    for required in [
        "crate::catalog::provider::CatalogProvider",
        "PlannerTableProvider",
        "IcebergMetadataTableProvider",
    ] {
        if !engine_provider.contains(required) {
            violations.insert(format!(
                "neutral-table-planner-provider-port-missing: {EBD_4B3D_PLANNER_PROVIDER_OWNER}|{required}"
            ));
        }
    }
    for call in sources
        .get(EBD_4B3D_PLANNER_PROVIDER_OWNER)
        .map(|source| ebd_4b3d_neutral_provider_connector_calls(&source.text))
        .unwrap_or_default()
    {
        violations.insert(format!(
            "neutral-table-ordinary-provider-connector-call: {EBD_4B3D_PLANNER_PROVIDER_OWNER}|{call}"
        ));
    }

    for source in sources.values() {
        if source.path.starts_with("src/engine/") {
            for symbol in ebd_4b3d_public_reexports(&source.text) {
                violations.insert(format!(
                    "neutral-table-engine-forwarding-provider: {}|{symbol}",
                    source.path
                ));
            }
        }
    }

    violations
}

#[test]
fn ebd_4b3d_neutral_table_provider_split_is_complete() {
    let sources = ebd_4b1_collect_repo_sources();
    let violations = ebd_4b3d_completion_violations(&sources);
    assert!(
        violations.is_empty(),
        "EBD-4B3D neutral table/provider split failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn ebd_4b3d_detector_covers_dependencies_retired_surfaces_and_provider_leaks() {
    let valid = vec![
        GuardSource::new(
            EBD_4B3D_TABLE_OWNER,
            r#"
pub(crate) struct CatalogTable;
const COMMENT_NOISE: &str = "crate::sql::planner::table::TableDef";
"#,
        ),
        GuardSource::new(
            EBD_4B3D_PROVIDER_OWNER,
            "pub(crate) trait CatalogProvider {}",
        ),
        GuardSource::new(
            EBD_4B3D_PARTITION_OWNER,
            "pub(crate) struct LegacyRangePartition;",
        ),
        GuardSource::new(
            EBD_4B3D_PLANNER_EXTENSION_OWNER,
            r#"
use crate::catalog::table::CatalogTable;
struct ResolvedAnalyzerTable { catalog: CatalogTable }
trait PlannerTableProvider {
    fn resolve_table_for_analysis(&self) -> ResolvedAnalyzerTable;
}
trait IcebergMetadataTableProvider {
    fn get_iceberg_metadata_table(&self);
}
"#,
        ),
        GuardSource::new(
            EBD_4B3D_ANALYZER_ENTRY,
            "fn resolve() { provider.resolve_table_for_analysis(); metadata.get_iceberg_metadata_table(); }",
        ),
        GuardSource::new(
            EBD_4B3D_PLANNER_PROVIDER_OWNER,
            r#"
use crate::catalog::provider::CatalogProvider;
use crate::sql::catalog::{IcebergMetadataTableProvider, PlannerTableProvider};
struct CatalogServiceProvider;
impl CatalogProvider for CatalogServiceProvider {
    fn get_table(&self) { self.resolve_table_for_analysis_once(); }
}
"#,
        ),
        GuardSource::new("src/engine/catalog.rs", "struct InMemoryCatalog;"),
        GuardSource::new("src/engine/mod.rs", "mod catalog;"),
    ];
    let valid_violations = ebd_4b3d_completion_violations(&valid);
    assert!(
        valid_violations.is_empty(),
        "legal neutral/planner split fixture must remain accepted: {valid_violations:?}"
    );

    let mut invalid = valid;
    invalid[3] = GuardSource::new(
        EBD_4B3D_PLANNER_EXTENSION_OWNER,
        r#"
use crate::catalog::table::CatalogTable;
struct ResolvedAnalyzerTable { catalog: CatalogTable }
trait PlannerTableProvider {
    fn resolve_table_for_analysis(&self) -> ResolvedAnalyzerTable;
    fn get_table(&self);
}
trait IcebergMetadataTableProvider {
    fn get_iceberg_metadata_table(&self);
}
"#,
    );
    invalid[0] = GuardSource::new(
        EBD_4B3D_TABLE_OWNER,
        r#"
use super::super::sql::{catalog::PlannerTableProvider as Provider, planner::*};
pub(crate) struct CatalogTable;
"#,
    );
    invalid.push(GuardSource::new(
        "src/sql/legacy_provider.rs",
        r#"
trait CatalogProvider {}
enum TableLookupMode { SchemaOnly }
fn get_table_with_mode() {}
"#,
    ));
    invalid[5] = GuardSource::new(
        EBD_4B3D_PLANNER_PROVIDER_OWNER,
        r#"
use crate::catalog::provider::CatalogProvider;
use crate::sql::catalog::{IcebergMetadataTableProvider, PlannerTableProvider};
struct CatalogServiceProvider;
impl CatalogProvider for CatalogServiceProvider {
    fn get_table(&self) {
        self.lookup_neutral();
        nested::nested_lookup(self);
    }
}
impl CatalogServiceProvider {
    fn lookup_neutral(&self) {
        self.catalog_backend();
        self.load_table_for_read();
        self.build_table_def();
    }
}
struct DuplicateMethodOwner;
impl DuplicateMethodOwner {
    fn lookup_neutral(&self) {}
}
mod nested {
    fn nested_lookup(provider: &super::CatalogServiceProvider) {
        provider.table_source();
        provider.build_schema_table_def();
    }
}
"#,
    );
    invalid[6] = GuardSource::new(
        "src/engine/catalog.rs",
        "pub use crate::catalog::provider::CatalogProvider as EngineCatalogProvider;",
    );
    invalid.push(GuardSource::new(
        "src/sql/nested_provider.rs",
        r#"
mod nested {
    trait CatalogProvider {}
    type LegacyRangePartition = ();
}
"#,
    ));
    invalid.push(GuardSource::new(
        "src/sql/provider_reexport.rs",
        "pub use crate::catalog::provider::*;",
    ));
    invalid.push(GuardSource::new(
        "src/sql/provider_macro.rs",
        "macro_rules! define_provider { () => { trait CatalogProvider {} } }",
    ));

    let violations = ebd_4b3d_completion_violations(&invalid);
    for expected in [
        "neutral-table-owner-dependency: src/catalog/table.rs|crate::sql",
        "neutral-table-planner-extra-entry: src/sql/catalog.rs|get_table",
        "neutral-table-provider-secondary-owner: src/sql/legacy_provider.rs",
        "neutral-table-provider-secondary-owner: src/sql/nested_provider.rs",
        "neutral-table-partition-secondary-owner: src/sql/nested_provider.rs",
        "neutral-table-macro-secondary-owner: src/sql/provider_macro.rs|CatalogProvider",
        "neutral-table-public-reexport: src/sql/provider_reexport.rs|CatalogProvider",
        "neutral-table-retired-surface: src/sql/legacy_provider.rs|TableLookupMode",
        "neutral-table-retired-surface: src/sql/legacy_provider.rs|get_table_with_mode",
        "neutral-table-ordinary-provider-connector-call: src/sql/catalog/provider.rs|catalog_backend",
        "neutral-table-ordinary-provider-connector-call: src/sql/catalog/provider.rs|load_table_for_read",
        "neutral-table-ordinary-provider-connector-call: src/sql/catalog/provider.rs|build_table_def",
        "neutral-table-ordinary-provider-connector-call: src/sql/catalog/provider.rs|table_source",
        "neutral-table-ordinary-provider-connector-call: src/sql/catalog/provider.rs|build_schema_table_def",
        "neutral-table-engine-forwarding-provider: src/engine/catalog.rs",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "EBD-4B3D detector missed {expected}: {violations:?}"
        );
    }
}

const EBD_5A1_MEMORY_OWNER: &str = "src/catalog/memory.rs";
const EBD_5A1_REGISTRY_OWNER: &str = "src/catalog/registry.rs";
const EBD_5A1_CACHE_OWNER: &str = "src/catalog/schema_cache.rs";
const EBD_5A1_SERVICE_OWNER: &str = "src/catalog/service.rs";
const EBD_5A1_SQL_PROVIDER_OWNER: &str = "src/sql/catalog/provider.rs";

const EBD_5A1_REQUIRED_OWNERS: &[(&str, &str)] = &[
    (EBD_5A1_MEMORY_OWNER, "MemoryCatalog"),
    (EBD_5A1_REGISTRY_OWNER, "CatalogRegistry"),
    (EBD_5A1_CACHE_OWNER, "SchemaCache"),
    (EBD_5A1_SERVICE_OWNER, "CatalogService"),
    (EBD_5A1_SQL_PROVIDER_OWNER, "CatalogServiceProvider"),
];

const EBD_5A1_RETIRED_OWNER_SYMBOLS: &[&str] =
    &["InMemoryCatalog", "CatalogMgr", "CatalogMgrProvider"];
const EBD_5A1_RETIRED_OWNER_PATHS: &[&str] = &[
    "src/engine/catalog.rs",
    "src/engine/catalog_mgr/catalog.rs",
    "src/engine/catalog_mgr/iceberg.rs",
    "src/engine/catalog_mgr/internal.rs",
    "src/engine/catalog_mgr/metadata.rs",
    "src/engine/catalog_mgr/mod.rs",
    "src/engine/catalog_mgr/provider.rs",
    "src/engine/catalog_mgr/schema_cache.rs",
];

fn ebd_5a1_attrs_are_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attribute| {
        if attribute.path().is_ident("test") {
            return true;
        }
        let syn::Meta::List(list) = &attribute.meta else {
            return false;
        };
        list.path.is_ident("cfg")
            && cfg_attribute_requires_test(&format!("#[cfg({})]", list.tokens))
    })
}

#[derive(Default)]
struct Ebd5a1Definitions {
    items: BTreeMap<(String, String), usize>,
    macros: BTreeSet<String>,
}

fn ebd_5a1_definitions(source: &str) -> Ebd5a1Definitions {
    struct Visitor {
        definitions: Ebd5a1Definitions,
    }

    impl Visitor {
        fn record(&mut self, kind: &str, name: &syn::Ident) {
            *self
                .definitions
                .items
                .entry((kind.to_string(), name.to_string()))
                .or_default() += 1;
        }

        fn record_macro(&mut self, item: &syn::ItemMacro) {
            let tokens = rust_source_tokens(&item.mac.tokens.to_string());
            for window in tokens.windows(2) {
                if matches!(
                    window[0].text.as_str(),
                    "struct" | "enum" | "trait" | "type"
                ) && (EBD_5A1_REQUIRED_OWNERS
                    .iter()
                    .any(|(_, symbol)| window[1].text == *symbol)
                    || EBD_5A1_RETIRED_OWNER_SYMBOLS.contains(&window[1].text.as_str()))
                {
                    self.definitions.macros.insert(window[1].text.clone());
                }
            }
        }
    }

    impl<'ast> syn::visit::Visit<'ast> for Visitor {
        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.record("struct", &item.ident);
                syn::visit::visit_item_struct(self, item);
            }
        }

        fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.record("enum", &item.ident);
                syn::visit::visit_item_enum(self, item);
            }
        }

        fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.record("trait", &item.ident);
                syn::visit::visit_item_trait(self, item);
            }
        }

        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.record("type", &item.ident);
                syn::visit::visit_item_type(self, item);
            }
        }

        fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.record_macro(item);
                syn::visit::visit_item_macro(self, item);
            }
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                syn::visit::visit_item_mod(self, item);
            }
        }
    }

    let Ok(file) = syn::parse_file(source) else {
        return Ebd5a1Definitions::default();
    };
    let mut visitor = Visitor {
        definitions: Ebd5a1Definitions::default(),
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    visitor.definitions
}

fn ebd_5a1_public_forwarding_reexports(source: &GuardSource) -> BTreeSet<String> {
    let forwarding_symbols = EBD_5A1_REQUIRED_OWNERS
        .iter()
        .map(|(_, symbol)| *symbol)
        .chain(EBD_5A1_RETIRED_OWNER_SYMBOLS.iter().copied())
        .collect::<BTreeSet<_>>();

    rust_raw_production_use_statements(&source.text)
        .into_iter()
        .filter(|import| import.visibility != "private")
        .filter_map(|import| {
            let path = rust_canonical_path_segments_in_scope(
                &import.path.segments,
                &source.path,
                &import.inline_modules,
            )
            .unwrap_or(import.path.segments);
            let aliases_symbol = import
                .path
                .alias
                .as_deref()
                .is_some_and(|alias| forwarding_symbols.contains(alias));
            let forwards_symbol = path
                .iter()
                .any(|segment| forwarding_symbols.contains(segment.as_str()));
            let forwards_old_engine_module = path.starts_with(&[
                "crate".to_string(),
                "engine".to_string(),
                "catalog".to_string(),
            ]) || path.starts_with(&[
                "crate".to_string(),
                "engine".to_string(),
                "catalog_mgr".to_string(),
            ]);
            let forwards_canonical_module = [
                ["crate", "catalog", "memory"].as_slice(),
                ["crate", "catalog", "registry"].as_slice(),
                ["crate", "catalog", "schema_cache"].as_slice(),
                ["crate", "catalog", "service"].as_slice(),
                ["crate", "sql", "catalog", "provider"].as_slice(),
            ]
            .iter()
            .any(|module| {
                path.iter()
                    .map(String::as_str)
                    .take(module.len())
                    .eq(module.iter().copied())
            });
            (aliases_symbol
                || forwards_symbol
                || forwards_old_engine_module
                || forwards_canonical_module)
                .then(|| path.join("::"))
        })
        .collect()
}

fn ebd_5a1_runtime_symbol(path: &[String]) -> Option<&'static str> {
    const RUNTIME_PATHS: &[(&[&str], &str)] = &[
        (
            &["crate", "catalog", "memory", "MemoryCatalog"],
            "MemoryCatalog",
        ),
        (
            &["crate", "catalog", "registry", "CatalogRegistry"],
            "CatalogRegistry",
        ),
        (
            &["crate", "catalog", "schema_cache", "SchemaCache"],
            "SchemaCache",
        ),
        (
            &["crate", "catalog", "service", "CatalogService"],
            "CatalogService",
        ),
        (
            &[
                "crate",
                "sql",
                "catalog",
                "provider",
                "CatalogServiceProvider",
            ],
            "CatalogServiceProvider",
        ),
    ];
    RUNTIME_PATHS
        .iter()
        .find(|(candidate, _)| {
            path.iter()
                .map(String::as_str)
                .eq(candidate.iter().copied())
        })
        .map(|(_, symbol)| *symbol)
}

fn ebd_5a1_runtime_types_in_type(
    ty: &syn::Type,
    source_path: &str,
    inline_modules: &[String],
    aliases: &RustScopedAliases,
) -> BTreeSet<String> {
    struct Visitor<'a> {
        source_path: &'a str,
        inline_modules: &'a [String],
        aliases: &'a RustScopedAliases,
        found: BTreeSet<String>,
    }

    impl<'ast> syn::visit::Visit<'ast> for Visitor<'_> {
        fn visit_type_path(&mut self, path: &'ast syn::TypePath) {
            let segments = path
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            let resolved = rust_resolve_scoped_paths(
                &segments,
                self.inline_modules,
                self.aliases,
                &mut BTreeSet::new(),
                0,
            )
            .unwrap_or_else(|| {
                vec![RustScopedUsePath {
                    segments: segments.clone(),
                    inline_modules: self.inline_modules.to_vec(),
                }]
            });
            let alias_resolved = resolved.iter().any(|target| {
                target.segments != segments || target.inline_modules != self.inline_modules
            });
            for resolved in resolved {
                let canonical = rust_canonical_path_segments_in_scope(
                    &resolved.segments,
                    self.source_path,
                    &resolved.inline_modules,
                );
                let direct_symbol = (!alias_resolved).then(|| {
                    segments.last().and_then(|symbol| {
                        EBD_5A1_REQUIRED_OWNERS
                            .iter()
                            .any(|(_, required)| symbol == required)
                            .then_some(symbol.as_str())
                    })
                });
                if let Some(symbol) = canonical
                    .as_deref()
                    .and_then(ebd_5a1_runtime_symbol)
                    .or(direct_symbol.flatten())
                {
                    self.found.insert(symbol.to_string());
                }
            }
            syn::visit::visit_type_path(self, path);
        }
    }

    let mut visitor = Visitor {
        source_path,
        inline_modules,
        aliases,
        found: BTreeSet::new(),
    };
    syn::visit::Visit::visit_type(&mut visitor, ty);
    visitor.found
}

fn ebd_5a1_canonical_type_path(
    ty: &syn::Type,
    source_path: &str,
    inline_modules: &[String],
    aliases: &RustScopedAliases,
) -> Option<(Vec<String>, Vec<Vec<String>>)> {
    let syn::Type::Path(type_path) = ty else {
        return None;
    };
    if type_path.qself.is_some() {
        return None;
    }
    let segments = type_path
        .path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    let resolved =
        rust_resolve_scoped_paths(&segments, inline_modules, aliases, &mut BTreeSet::new(), 0)?;
    if resolved.len() != 1 {
        return None;
    }
    let target = resolved.into_iter().next().expect("single resolved path");
    let canonical = rust_canonical_path_segments_in_scope(
        &target.segments,
        source_path,
        &target.inline_modules,
    )?;

    let last = type_path.path.segments.last()?;
    let syn::PathArguments::AngleBracketed(arguments) = &last.arguments else {
        return Some((canonical, Vec::new()));
    };
    let mut generic_types = Vec::new();
    for argument in &arguments.args {
        let syn::GenericArgument::Type(argument) = argument else {
            return None;
        };
        let (canonical, nested) =
            ebd_5a1_canonical_type_path(argument, source_path, inline_modules, aliases)?;
        if !nested.is_empty() {
            return None;
        }
        generic_types.push(canonical);
    }
    Some((canonical, generic_types))
}

fn ebd_5a1_is_allowed_sql_specialization_alias(
    source_path: &str,
    inline_modules: &[String],
    aliases: &RustScopedAliases,
    item: &syn::ItemType,
) -> bool {
    if !inline_modules.is_empty()
        || !ebd_4b1_is_pub_crate(&item.vis)
        || !item.generics.params.is_empty()
        || item.generics.where_clause.is_some()
    {
        return false;
    }
    let Some((target, arguments)) =
        ebd_5a1_canonical_type_path(&item.ty, source_path, inline_modules, aliases)
    else {
        return false;
    };
    match (source_path, item.ident.to_string().as_str()) {
        ("src/sql/catalog/local.rs", "PlannerMemoryCatalog") => {
            target == ["crate", "catalog", "memory", "MemoryCatalog"]
                && arguments == [["crate", "sql", "planner", "table", "TableDef"]]
        }
        ("src/sql/catalog.rs", "StandaloneCatalogService") => {
            target == ["crate", "catalog", "service", "CatalogService"]
                && arguments
                    == [
                        ["crate", "sql", "planner", "table", "TableDef"],
                        [
                            "crate",
                            "sql",
                            "catalog",
                            "metadata",
                            "CatalogRuntimeMetadata",
                        ],
                    ]
        }
        _ => false,
    }
}

fn ebd_5a1_runtime_alias_and_wrapper_violations(source: &GuardSource) -> BTreeSet<String> {
    struct Visitor<'a> {
        path: &'a str,
        aliases: &'a RustScopedAliases,
        inline_modules: Vec<String>,
        violations: BTreeSet<String>,
    }

    impl<'ast> syn::visit::Visit<'ast> for Visitor<'_> {
        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            if ebd_5a1_attrs_are_test_only(&item.attrs) {
                return;
            }
            if ebd_5a1_is_allowed_sql_specialization_alias(
                self.path,
                &self.inline_modules,
                self.aliases,
                item,
            ) {
                return;
            }
            for symbol in ebd_5a1_runtime_types_in_type(
                &item.ty,
                self.path,
                &self.inline_modules,
                self.aliases,
            ) {
                self.violations.insert(format!(
                    "catalog-runtime-type-alias: {}|{}|{symbol}",
                    self.path, item.ident
                ));
            }
            syn::visit::visit_item_type(self, item);
        }

        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            if ebd_5a1_attrs_are_test_only(&item.attrs) {
                return;
            }
            let canonical_owner = EBD_5A1_REQUIRED_OWNERS
                .iter()
                .any(|(path, symbol)| self.path == *path && item.ident == *symbol);
            if !canonical_owner && item.fields.len() == 1 {
                let field = item.fields.iter().next().expect("single struct field");
                for symbol in ebd_5a1_runtime_types_in_type(
                    &field.ty,
                    self.path,
                    &self.inline_modules,
                    self.aliases,
                ) {
                    self.violations.insert(format!(
                        "catalog-runtime-wrapper: {}|{}|{symbol}",
                        self.path, item.ident
                    ));
                }
            }
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if ebd_5a1_attrs_are_test_only(&item.attrs) {
                return;
            }
            if let Some((_, items)) = &item.content {
                self.inline_modules.push(item.ident.to_string());
                for nested in items {
                    syn::visit::Visit::visit_item(self, nested);
                }
                self.inline_modules.pop();
            }
        }
    }

    let Ok(file) = syn::parse_file(&source.text) else {
        return BTreeSet::new();
    };
    let aliases = rust_production_scoped_aliases(&source.text);
    let mut visitor = Visitor {
        path: &source.path,
        aliases: &aliases,
        inline_modules: Vec::new(),
        violations: BTreeSet::new(),
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    visitor.violations
}

fn ebd_5a1_production_function_names(source: &str) -> BTreeSet<String> {
    struct Visitor {
        names: BTreeSet<String>,
    }

    impl<'ast> syn::visit::Visit<'ast> for Visitor {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.names.insert(item.sig.ident.to_string());
                syn::visit::visit_item_fn(self, item);
            }
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.names.insert(item.sig.ident.to_string());
                syn::visit::visit_impl_item_fn(self, item);
            }
        }

        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                syn::visit::visit_item_impl(self, item);
            }
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                syn::visit::visit_item_mod(self, item);
            }
        }
    }

    let Ok(file) = syn::parse_file(source) else {
        return BTreeSet::new();
    };
    let mut visitor = Visitor {
        names: BTreeSet::new(),
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    visitor.names
}

fn ebd_5a1_connector_state_registration_violations(
    sources: &BTreeMap<&str, &GuardSource>,
) -> BTreeSet<String> {
    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct CallTarget {
        path: String,
        conservative_leaf: bool,
    }

    #[derive(Default)]
    struct FunctionNode {
        qualified_path: String,
        source_path: String,
        name: String,
        accepts_state: bool,
        touches_catalog_runtime: bool,
        direct_registration: bool,
        calls: BTreeSet<CallTarget>,
    }

    #[derive(Default)]
    struct SignatureTypes {
        state: bool,
        catalog_params: BTreeSet<String>,
    }

    impl SignatureTypes {
        fn inspect(
            signature: &syn::Signature,
            source_path: &str,
            inline_modules: &[String],
            aliases: &RustScopedAliases,
        ) -> Self {
            fn type_contains_leaf(ty: &syn::Type, names: &[&str]) -> bool {
                struct Visitor<'a> {
                    names: &'a [&'a str],
                    found: bool,
                }

                impl<'ast> syn::visit::Visit<'ast> for Visitor<'_> {
                    fn visit_path(&mut self, path: &'ast syn::Path) {
                        self.found |= path.segments.iter().any(|segment| {
                            self.names.contains(&segment.ident.to_string().as_str())
                        });
                        if !self.found {
                            syn::visit::visit_path(self, path);
                        }
                    }
                }

                let mut visitor = Visitor {
                    names,
                    found: false,
                };
                syn::visit::Visit::visit_type(&mut visitor, ty);
                visitor.found
            }

            let mut result = Self::default();
            for input in &signature.inputs {
                let syn::FnArg::Typed(input) = input else {
                    continue;
                };
                let syn::Pat::Ident(binding) = input.pat.as_ref() else {
                    continue;
                };
                if type_contains_leaf(&input.ty, &["StandaloneState"]) {
                    result.state = true;
                }
                let runtime_type =
                    !ebd_5a1_runtime_types_in_type(&input.ty, source_path, inline_modules, aliases)
                        .is_empty()
                        || type_contains_leaf(
                            &input.ty,
                            &["CatalogMgr", "StandaloneCatalogService"],
                        );
                if runtime_type {
                    result.catalog_params.insert(binding.ident.to_string());
                }
            }
            result
        }
    }

    struct FunctionBody {
        legacy_registry: bool,
        service: bool,
        service_registration: bool,
        calls: BTreeSet<(Vec<String>, bool)>,
        tainted: BTreeSet<String>,
    }

    impl FunctionBody {
        fn new(tainted: BTreeSet<String>) -> Self {
            Self {
                legacy_registry: false,
                service: false,
                service_registration: false,
                calls: BTreeSet::new(),
                tainted,
            }
        }

        fn expr_is_tainted(&self, expr: &syn::Expr) -> bool {
            match expr {
                syn::Expr::Path(path) => path
                    .path
                    .segments
                    .last()
                    .is_some_and(|segment| self.tainted.contains(&segment.ident.to_string())),
                syn::Expr::Field(field) => {
                    matches!(
                        &field.member,
                        syn::Member::Named(name)
                            if matches!(name.to_string().as_str(), "catalog_mgr" | "catalog_service")
                    ) || self.expr_is_tainted(&field.base)
                }
                syn::Expr::Reference(reference) => self.expr_is_tainted(&reference.expr),
                syn::Expr::Paren(paren) => self.expr_is_tainted(&paren.expr),
                syn::Expr::Group(group) => self.expr_is_tainted(&group.expr),
                syn::Expr::Cast(cast) => self.expr_is_tainted(&cast.expr),
                syn::Expr::Await(await_expr) => self.expr_is_tainted(&await_expr.base),
                syn::Expr::Try(try_expr) => self.expr_is_tainted(&try_expr.expr),
                syn::Expr::Unary(unary) => self.expr_is_tainted(&unary.expr),
                _ => false,
            }
        }
    }

    impl<'ast> syn::visit::Visit<'ast> for FunctionBody {
        fn visit_local(&mut self, local: &'ast syn::Local) {
            let tainted = local
                .init
                .as_ref()
                .is_some_and(|init| self.expr_is_tainted(&init.expr));
            syn::visit::visit_local(self, local);
            if tainted && let syn::Pat::Ident(binding) = &local.pat {
                self.tainted.insert(binding.ident.to_string());
            }
        }

        fn visit_expr_field(&mut self, field: &'ast syn::ExprField) {
            if let syn::Member::Named(name) = &field.member {
                match name.to_string().as_str() {
                    "catalog_mgr" => self.legacy_registry = true,
                    "catalog_service" => self.service = true,
                    _ => {}
                }
            }
            syn::visit::visit_expr_field(self, field);
        }

        fn visit_expr_path(&mut self, path: &'ast syn::ExprPath) {
            if let Some(name) = path
                .path
                .segments
                .last()
                .map(|segment| segment.ident.to_string())
            {
                if matches!(
                    name.as_str(),
                    "register_default_catalog_mgr_entries" | "register_iceberg_catalog_mgr_entry"
                ) {
                    self.legacy_registry = true;
                }
            }
            syn::visit::visit_expr_path(self, path);
        }

        fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
            let tainted = call.args.iter().any(|arg| self.expr_is_tainted(arg));
            if tainted && let syn::Expr::Path(path) = call.func.as_ref() {
                self.calls.insert((
                    path.path
                        .segments
                        .iter()
                        .map(|segment| segment.ident.to_string())
                        .collect(),
                    false,
                ));
            }
            syn::visit::visit_expr_call(self, call);
        }

        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            let method = call.method.to_string();
            let tainted = self.expr_is_tainted(&call.receiver)
                || call.args.iter().any(|arg| self.expr_is_tainted(arg));
            if tainted {
                self.calls.insert((vec![method.clone()], true));
            }
            if matches!(method.as_str(), "register_catalog" | "unregister_catalog") {
                self.service_registration |= self.expr_is_tainted(&call.receiver);
            }
            syn::visit::visit_expr_method_call(self, call);
        }
    }

    struct Visitor<'a> {
        source_path: &'a str,
        module: Vec<String>,
        inline_modules: Vec<String>,
        aliases: &'a RustScopedAliases,
        impl_self_path: Option<Vec<String>>,
        functions: Vec<FunctionNode>,
    }

    impl Visitor<'_> {
        fn resolved_paths(&self, path: Vec<String>) -> BTreeSet<Vec<String>> {
            if path.first().is_some_and(|segment| segment == "Self")
                && let Some(self_path) = &self.impl_self_path
            {
                let mut target = self_path.clone();
                target.extend_from_slice(&path[1..]);
                return BTreeSet::from([target]);
            }

            rust_resolve_scoped_paths(
                &path,
                &self.inline_modules,
                self.aliases,
                &mut BTreeSet::new(),
                0,
            )
            .unwrap_or_else(|| {
                vec![RustScopedUsePath {
                    segments: path,
                    inline_modules: self.inline_modules.clone(),
                }]
            })
            .into_iter()
            .filter_map(|target| {
                rust_canonical_path_segments_in_scope(
                    &target.segments,
                    self.source_path,
                    &target.inline_modules,
                )
            })
            .collect()
        }

        fn nominal_self_path(&self, ty: &syn::Type) -> Option<Vec<String>> {
            let syn::Type::Path(path) = ty else {
                return None;
            };
            if path.qself.is_some() {
                return None;
            }
            let segments = path
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect();
            let paths = self.resolved_paths(segments);
            (paths.len() == 1)
                .then(|| paths.into_iter().next())
                .flatten()
        }

        fn call_targets(&self, calls: BTreeSet<(Vec<String>, bool)>) -> BTreeSet<CallTarget> {
            calls
                .into_iter()
                .flat_map(|(call, conservative_leaf)| {
                    self.resolved_paths(call)
                        .into_iter()
                        .map(move |path| CallTarget {
                            path: path.join("::"),
                            conservative_leaf,
                        })
                })
                .collect()
        }

        fn inspect(&mut self, signature: &syn::Signature, block: &syn::Block) {
            let signature_types = SignatureTypes::inspect(
                signature,
                self.source_path,
                &self.inline_modules,
                self.aliases,
            );
            let mut body = FunctionBody::new(signature_types.catalog_params.clone());
            syn::visit::Visit::visit_block(&mut body, block);
            let name = signature.ident.to_string();
            let mut function_path = self.impl_self_path.clone().unwrap_or_else(|| {
                let mut path = self.module.clone();
                path.extend(self.inline_modules.iter().cloned());
                path
            });
            function_path.push(name.clone());
            self.functions.push(FunctionNode {
                qualified_path: function_path.join("::"),
                source_path: self.source_path.to_string(),
                name,
                accepts_state: signature_types.state,
                touches_catalog_runtime: body.legacy_registry
                    || body.service
                    || !signature_types.catalog_params.is_empty(),
                direct_registration: body.legacy_registry || body.service_registration,
                calls: self.call_targets(body.calls),
            });
        }
    }

    impl<'ast> syn::visit::Visit<'ast> for Visitor<'_> {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.inspect(&item.sig, &item.block);
                syn::visit::visit_item_fn(self, item);
            }
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.inspect(&item.sig, &item.block);
                syn::visit::visit_impl_item_fn(self, item);
            }
        }

        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            if ebd_5a1_attrs_are_test_only(&item.attrs) {
                return;
            }
            let previous = self.impl_self_path.take();
            self.impl_self_path = self.nominal_self_path(&item.self_ty);
            for impl_item in &item.items {
                syn::visit::Visit::visit_impl_item(self, impl_item);
            }
            self.impl_self_path = previous;
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if ebd_5a1_attrs_are_test_only(&item.attrs) {
                return;
            }
            if let Some((_, items)) = &item.content {
                self.inline_modules.push(item.ident.to_string());
                for nested in items {
                    syn::visit::Visit::visit_item(self, nested);
                }
                self.inline_modules.pop();
            }
        }
    }

    let mut functions = Vec::new();
    for source in sources
        .values()
        .filter(|source| source.path.starts_with("src/connector/"))
    {
        let Ok(file) = syn::parse_file(&source.text) else {
            continue;
        };
        let Some(module) = rust_source_module_segments(&source.path) else {
            continue;
        };
        let aliases = rust_production_scoped_aliases(&source.text);
        let mut visitor = Visitor {
            source_path: &source.path,
            module,
            inline_modules: Vec::new(),
            aliases: &aliases,
            impl_self_path: None,
            functions: Vec::new(),
        };
        syn::visit::Visit::visit_file(&mut visitor, &file);
        functions.extend(visitor.functions);
    }

    let mut exact_candidates = BTreeMap::<String, Vec<usize>>::new();
    let mut leaf_candidates = BTreeMap::<String, Vec<usize>>::new();
    for (index, node) in functions.iter().enumerate() {
        exact_candidates
            .entry(node.qualified_path.clone())
            .or_default()
            .push(index);
        leaf_candidates
            .entry(node.name.clone())
            .or_default()
            .push(index);
    }

    let mut reaches_registration = functions
        .iter()
        .enumerate()
        .filter(|(_, node)| node.direct_registration)
        .map(|(index, _)| index)
        .collect::<BTreeSet<_>>();
    loop {
        let mut changed = false;
        for (index, node) in functions.iter().enumerate() {
            if reaches_registration.contains(&index) {
                continue;
            }
            let reaches = node.calls.iter().any(|call| {
                let exact = exact_candidates
                    .get(&call.path)
                    .into_iter()
                    .flatten()
                    .copied();
                let use_leaf = call.conservative_leaf
                    || exact_candidates
                        .get(&call.path)
                        .is_none_or(|candidates| candidates.is_empty());
                let leaf = call
                    .path
                    .rsplit("::")
                    .next()
                    .and_then(|name| leaf_candidates.get(name))
                    .into_iter()
                    .flatten()
                    .copied();
                exact
                    .chain(use_leaf.then_some(leaf).into_iter().flatten())
                    .any(|candidate| reaches_registration.contains(&candidate))
            });
            if reaches {
                changed |= reaches_registration.insert(index);
            }
        }
        if !changed {
            break;
        }
    }

    functions
        .into_iter()
        .enumerate()
        .filter(|(index, node)| {
            node.accepts_state
                && node.touches_catalog_runtime
                && reaches_registration.contains(index)
        })
        .map(|(_, node)| {
            format!(
                "catalog-runtime-connector-state-registration: {}|{}",
                node.source_path, node.name
            )
        })
        .collect()
}

fn ebd_5a1_engine_forwarding_facade_violations(
    sources: &BTreeMap<&str, &GuardSource>,
) -> BTreeSet<String> {
    fn type_contains_leaf(ty: &syn::Type, names: &[&str]) -> bool {
        struct Visitor<'a> {
            names: &'a [&'a str],
            found: bool,
        }

        impl<'ast> syn::visit::Visit<'ast> for Visitor<'_> {
            fn visit_path(&mut self, path: &'ast syn::Path) {
                self.found |= path
                    .segments
                    .iter()
                    .any(|segment| self.names.contains(&segment.ident.to_string().as_str()));
                if !self.found {
                    syn::visit::visit_path(self, path);
                }
            }
        }

        let mut visitor = Visitor {
            names,
            found: false,
        };
        syn::visit::Visit::visit_type(&mut visitor, ty);
        visitor.found
    }

    fn runtime_binding_names(
        signature: &syn::Signature,
        source_path: &str,
        inline_modules: &[String],
        aliases: &RustScopedAliases,
    ) -> BTreeSet<String> {
        signature
            .inputs
            .iter()
            .filter_map(|input| {
                let syn::FnArg::Typed(input) = input else {
                    return None;
                };
                let syn::Pat::Ident(binding) = input.pat.as_ref() else {
                    return None;
                };
                (!ebd_5a1_runtime_types_in_type(&input.ty, source_path, inline_modules, aliases)
                    .is_empty()
                    || type_contains_leaf(&input.ty, &["StandaloneCatalogService"]))
                .then(|| binding.ident.to_string())
            })
            .collect()
    }

    fn state_binding_names(signature: &syn::Signature) -> BTreeSet<String> {
        signature
            .inputs
            .iter()
            .filter_map(|input| {
                let syn::FnArg::Typed(input) = input else {
                    return None;
                };
                let syn::Pat::Ident(binding) = input.pat.as_ref() else {
                    return None;
                };
                type_contains_leaf(&input.ty, &["StandaloneState"])
                    .then(|| binding.ident.to_string())
            })
            .collect()
    }

    fn expr_touches_runtime(expr: &syn::Expr, runtime_bindings: &BTreeSet<String>) -> bool {
        match expr {
            syn::Expr::Path(path) => path
                .path
                .segments
                .last()
                .is_some_and(|segment| runtime_bindings.contains(&segment.ident.to_string())),
            syn::Expr::Field(field) => {
                matches!(
                    &field.member,
                    syn::Member::Named(name)
                        if name == "catalog_service"
                ) || expr_touches_runtime(&field.base, runtime_bindings)
            }
            syn::Expr::Call(call) => {
                expr_touches_runtime(&call.func, runtime_bindings)
                    || call
                        .args
                        .iter()
                        .any(|arg| expr_touches_runtime(arg, runtime_bindings))
            }
            syn::Expr::MethodCall(call) => {
                expr_touches_runtime(&call.receiver, runtime_bindings)
                    || call
                        .args
                        .iter()
                        .any(|arg| expr_touches_runtime(arg, runtime_bindings))
            }
            syn::Expr::Reference(reference) => {
                expr_touches_runtime(&reference.expr, runtime_bindings)
            }
            syn::Expr::Return(return_expr) => return_expr
                .expr
                .as_deref()
                .is_some_and(|expr| expr_touches_runtime(expr, runtime_bindings)),
            syn::Expr::Paren(paren) => expr_touches_runtime(&paren.expr, runtime_bindings),
            syn::Expr::Group(group) => expr_touches_runtime(&group.expr, runtime_bindings),
            syn::Expr::Await(await_expr) => {
                expr_touches_runtime(&await_expr.base, runtime_bindings)
            }
            syn::Expr::Try(try_expr) => expr_touches_runtime(&try_expr.expr, runtime_bindings),
            _ => false,
        }
    }

    fn is_thin_forwarder(
        signature: &syn::Signature,
        block: &syn::Block,
        source_path: &str,
        inline_modules: &[String],
        aliases: &RustScopedAliases,
    ) -> bool {
        let runtime_bindings =
            runtime_binding_names(signature, source_path, inline_modules, aliases);
        let state_bindings = state_binding_names(signature);
        let exposes_runtime = !runtime_bindings.is_empty()
            || match &signature.output {
                syn::ReturnType::Default => false,
                syn::ReturnType::Type(_, ty) => {
                    !ebd_5a1_runtime_types_in_type(ty, source_path, inline_modules, aliases)
                        .is_empty()
                        || type_contains_leaf(ty, &["StandaloneCatalogService"])
                }
            };
        let Some(syn::Stmt::Expr(expr, _)) = block.stmts.first() else {
            return false;
        };
        block.stmts.len() == 1
            && (exposes_runtime || !state_bindings.is_empty())
            && expr_touches_runtime(expr, &runtime_bindings)
    }

    struct Visitor<'a> {
        path: &'a str,
        aliases: &'a RustScopedAliases,
        inline_modules: Vec<String>,
        violations: BTreeSet<String>,
    }

    impl Visitor<'_> {
        fn inspect(&mut self, signature: &syn::Signature, block: &syn::Block) {
            if is_thin_forwarder(
                signature,
                block,
                self.path,
                &self.inline_modules,
                self.aliases,
            ) {
                self.violations.insert(format!(
                    "catalog-runtime-engine-forwarding-facade: {}|{}",
                    self.path, signature.ident
                ));
            }
        }
    }

    impl<'ast> syn::visit::Visit<'ast> for Visitor<'_> {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.inspect(&item.sig, &item.block);
                syn::visit::visit_item_fn(self, item);
            }
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                self.inspect(&item.sig, &item.block);
                syn::visit::visit_impl_item_fn(self, item);
            }
        }

        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                syn::visit::visit_item_impl(self, item);
            }
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if ebd_5a1_attrs_are_test_only(&item.attrs) {
                return;
            }
            if let Some((_, items)) = &item.content {
                self.inline_modules.push(item.ident.to_string());
                for nested in items {
                    syn::visit::Visit::visit_item(self, nested);
                }
                self.inline_modules.pop();
            }
        }
    }

    let declared = sources
        .get("src/engine/mod.rs")
        .into_iter()
        .flat_map(|source| rust_module_items(&rust_sanitized_production_text(&source.text)))
        .filter(|item| item.is_external && item.inline_modules.is_empty())
        .map(|item| item.name)
        .collect::<BTreeSet<_>>();
    let mut violations = BTreeSet::new();
    for module in declared {
        for path in [
            format!("src/engine/{module}.rs"),
            format!("src/engine/{module}/mod.rs"),
        ] {
            let Some(source) = sources.get(path.as_str()) else {
                continue;
            };
            let Ok(file) = syn::parse_file(&source.text) else {
                continue;
            };
            let aliases = rust_production_scoped_aliases(&source.text);
            let mut visitor = Visitor {
                path: &source.path,
                aliases: &aliases,
                inline_modules: Vec::new(),
                violations: BTreeSet::new(),
            };
            syn::visit::Visit::visit_file(&mut visitor, &file);
            violations.extend(visitor.violations);
        }
    }
    violations
}

fn ebd_5a1_standalone_state_fields(source: &str) -> BTreeMap<String, usize> {
    struct Visitor {
        fields: BTreeMap<String, usize>,
    }

    impl<'ast> syn::visit::Visit<'ast> for Visitor {
        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            if item.ident == "StandaloneState" && !ebd_5a1_attrs_are_test_only(&item.attrs) {
                for field in &item.fields {
                    if let Some(name) = &field.ident {
                        *self.fields.entry(name.to_string()).or_default() += 1;
                    }
                }
            }
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                syn::visit::visit_item_struct(self, item);
            }
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !ebd_5a1_attrs_are_test_only(&item.attrs) {
                syn::visit::visit_item_mod(self, item);
            }
        }
    }

    let Ok(file) = syn::parse_file(source) else {
        return BTreeMap::new();
    };
    let mut visitor = Visitor {
        fields: BTreeMap::new(),
    };
    syn::visit::Visit::visit_file(&mut visitor, &file);
    visitor.fields
}

fn ebd_5a1_completion_violations(sources: &[GuardSource]) -> BTreeSet<String> {
    let sources = sources
        .iter()
        .filter(|source| source.path != "tests/architecture_guard/ebd_1_engine_boundary.rs")
        .map(|source| (source.path.as_str(), source))
        .collect::<BTreeMap<_, _>>();
    let definitions = sources
        .iter()
        .map(|(path, source)| (*path, ebd_5a1_definitions(&source.text)))
        .collect::<BTreeMap<_, _>>();
    let mut violations = BTreeSet::new();

    for path in EBD_5A1_RETIRED_OWNER_PATHS {
        if sources.contains_key(path) {
            violations.insert(format!("catalog-runtime-old-owner: {path}"));
        }
    }

    if let Some(engine_mod) = sources.get("src/engine/mod.rs") {
        for item in rust_module_items(&rust_sanitized_production_text(&engine_mod.text)) {
            if matches!(item.name.as_str(), "catalog" | "catalog_mgr") {
                violations.insert(format!(
                    "catalog-runtime-old-module: src/engine/mod.rs|{}",
                    item.name
                ));
            }
        }
    }

    for (path, symbol) in EBD_5A1_REQUIRED_OWNERS {
        let owner_count = definitions
            .get(path)
            .and_then(|items| {
                items
                    .items
                    .get(&("struct".to_string(), (*symbol).to_string()))
            })
            .copied()
            .unwrap_or_default();
        if owner_count != 1 {
            violations.insert(format!(
                "catalog-runtime-owner-count: {path}|struct|{symbol}|expected=1 actual={owner_count}"
            ));
        }
        for (source_path, items) in &definitions {
            if source_path == path {
                continue;
            }
            let count = items
                .items
                .iter()
                .filter(|((_, name), _)| name == symbol)
                .map(|(_, count)| count)
                .sum::<usize>();
            if count != 0 {
                violations.insert(format!(
                    "catalog-runtime-secondary-owner: {source_path}|{symbol}|count={count}"
                ));
            }
        }
    }

    for (path, items) in &definitions {
        for symbol in EBD_5A1_RETIRED_OWNER_SYMBOLS {
            let alias_count = items
                .items
                .get(&("type".to_string(), (*symbol).to_string()))
                .copied()
                .unwrap_or_default();
            if alias_count != 0 {
                violations.insert(format!(
                    "catalog-runtime-retired-alias: {path}|{symbol}|count={alias_count}"
                ));
            }
            let owner_count = items
                .items
                .iter()
                .filter(|((kind, name), _)| kind != "type" && name == symbol)
                .map(|(_, count)| count)
                .sum::<usize>();
            if owner_count != 0 {
                violations.insert(format!(
                    "catalog-runtime-retired-owner: {path}|{symbol}|count={owner_count}"
                ));
            }
        }
        for symbol in &items.macros {
            violations.insert(format!(
                "catalog-runtime-macro-secondary-owner: {path}|{symbol}"
            ));
        }
    }

    for source in sources.values() {
        violations.extend(ebd_5a1_runtime_alias_and_wrapper_violations(source));
        for target in ebd_5a1_public_forwarding_reexports(source) {
            violations.insert(format!(
                "catalog-runtime-forwarding-reexport: {}|{target}",
                source.path
            ));
        }
        for function in ebd_5a1_production_function_names(&source.text) {
            if function.contains("catalog_mgr")
                || matches!(
                    function.as_str(),
                    "build_analyzer_provider" | "execute_query_with_catalog_mgr"
                )
            {
                violations.insert(format!(
                    "catalog-runtime-retired-helper: {}|{function}",
                    source.path
                ));
            }
        }
    }
    violations.extend(ebd_5a1_connector_state_registration_violations(&sources));
    violations.extend(ebd_5a1_engine_forwarding_facade_violations(&sources));

    for path in [
        EBD_5A1_MEMORY_OWNER,
        EBD_5A1_REGISTRY_OWNER,
        EBD_5A1_CACHE_OWNER,
        EBD_5A1_SERVICE_OWNER,
    ] {
        let Some(source) = sources.get(path) else {
            continue;
        };
        for dependency in rust_production_canonical_paths(&source.text, path) {
            if dependency.starts_with(&["crate".to_string(), "engine".to_string()])
                || dependency.starts_with(&["crate".to_string(), "sql".to_string()])
                || dependency.starts_with(&["crate".to_string(), "connector".to_string()])
            {
                violations.insert(format!(
                    "catalog-runtime-neutral-owner-dependency: {path}|{}",
                    dependency.join("::")
                ));
            }
        }
        let production = rust_sanitized_production_text(&source.text);
        for forbidden in [
            "CatalogRuntimeMetadata",
            "ConnectorRegistry",
            "ResolvedAnalyzerTable",
            "StandaloneState",
            "TableDef",
        ] {
            if rust_source_tokens(&production)
                .iter()
                .any(|token| token.text == forbidden)
            {
                violations.insert(format!(
                    "catalog-runtime-neutral-owner-dependency: {path}|{forbidden}"
                ));
            }
        }
    }

    let state_fields = sources
        .get("src/engine/mod.rs")
        .map(|source| ebd_5a1_standalone_state_fields(&source.text))
        .unwrap_or_default();
    let service_count = state_fields
        .get("catalog_service")
        .copied()
        .unwrap_or_default();
    if service_count != 1 {
        violations.insert(format!(
            "catalog-runtime-state-field-count: src/engine/mod.rs|catalog_service|expected=1 actual={service_count}"
        ));
    }
    for forbidden in ["catalog", "catalog_mgr"] {
        let count = state_fields.get(forbidden).copied().unwrap_or_default();
        if count != 0 {
            violations.insert(format!(
                "catalog-runtime-state-retired-field: src/engine/mod.rs|{forbidden}|count={count}"
            ));
        }
    }

    violations
}

#[test]
fn ebd_5a1_catalog_runtime_owner_cutover_is_complete() {
    let sources = ebd_4b1_collect_repo_sources();
    let violations = ebd_5a1_completion_violations(&sources);
    assert!(
        violations.is_empty(),
        "EBD-5A1 catalog runtime owner cutover failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn ebd_5a1_detector_rejects_forwarding_alias_macro_and_state_registration() {
    let valid = vec![
        GuardSource::new(
            EBD_5A1_MEMORY_OWNER,
            r#"
pub(crate) struct MemoryCatalog<T> { local: Option<T> }
#[cfg(test)]
struct CatalogRegistry<T>(T);
const COMMENT_NOISE: &str = "struct SchemaCache; state.catalog_mgr.write();";
"#,
        ),
        GuardSource::new(
            EBD_5A1_REGISTRY_OWNER,
            "pub(crate) struct CatalogRegistry<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_CACHE_OWNER,
            "pub(crate) struct SchemaCache<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_SERVICE_OWNER,
            "pub(crate) struct CatalogService<T, M> { marker: std::marker::PhantomData<(T, M)> }",
        ),
        GuardSource::new(
            EBD_5A1_SQL_PROVIDER_OWNER,
            "pub(crate) struct CatalogServiceProvider<'a> { marker: std::marker::PhantomData<&'a ()> }",
        ),
        GuardSource::new(
            "src/engine/mod.rs",
            "struct StandaloneState { catalog_service: Arc<StandaloneCatalogService> }",
        ),
        GuardSource::new(
            "src/connector/fixture.rs",
            r#"
struct LocalCatalogRegistry;
struct LocalSchemaCache;
#[cfg(test)]
fn register_catalog(state: &StandaloneState) {
    state.catalog_mgr.write().unwrap();
}
// pub use crate::catalog::service::CatalogService;
"#,
        ),
    ];
    let valid_violations = ebd_5a1_completion_violations(&valid);
    assert!(
        valid_violations.is_empty(),
        "cfg(test), comments, strings, and unrelated local types must remain accepted: {valid_violations:?}"
    );

    let mut invalid = valid;
    invalid.push(GuardSource::new(
        "src/engine/catalog.rs",
        "pub use crate::catalog::service::CatalogService;",
    ));
    invalid.push(GuardSource::new(
        "src/sql/catalog/legacy_alias.rs",
        "type CatalogMgr<M> = CatalogRegistry<M>;",
    ));
    invalid.push(GuardSource::new(
        "src/sql/catalog/cache_macro.rs",
        "macro_rules! define_cache { () => { struct SchemaCache; } }",
    ));
    invalid.push(GuardSource::new(
        "src/connector/catalog_registration.rs",
        r#"
fn register_catalog(state: &StandaloneState) {
    state.catalog_mgr.write().unwrap();
}
"#,
    ));

    let violations = ebd_5a1_completion_violations(&invalid);
    for expected in [
        "catalog-runtime-old-owner",
        "catalog-runtime-forwarding-reexport",
        "catalog-runtime-retired-alias",
        "catalog-runtime-macro-secondary-owner",
        "catalog-runtime-connector-state-registration",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "EBD-5A1 detector missed {expected}: {violations:?}"
        );
    }
}

#[test]
fn ebd_5a1_detector_rejects_arbitrarily_named_runtime_aliases_and_wrappers() {
    let valid = vec![
        GuardSource::new(
            EBD_5A1_MEMORY_OWNER,
            "pub(crate) struct MemoryCatalog<T> { local: Option<T> }",
        ),
        GuardSource::new(
            EBD_5A1_REGISTRY_OWNER,
            "pub(crate) struct CatalogRegistry<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_CACHE_OWNER,
            "pub(crate) struct SchemaCache<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_SERVICE_OWNER,
            r#"
pub(crate) struct CatalogService<T, M> {
    local: MemoryCatalog<T>,
    registry: CatalogRegistry<M>,
}
"#,
        ),
        GuardSource::new(
            EBD_5A1_SQL_PROVIDER_OWNER,
            r#"
pub(crate) struct CatalogServiceProvider<'a, T, M> {
    service: &'a CatalogService<T, M>,
    current_catalog: Option<&'a str>,
}
"#,
        ),
        GuardSource::new(
            "src/engine/mod.rs",
            r#"
struct StandaloneState {
    catalog_service: Arc<CatalogService<(), ()>>,
    connectors: Arc<()>,
}

struct CatalogRuntime {
    catalog_service: Arc<CatalogService<(), ()>>,
    connectors: Arc<()>,
}

use crate::unrelated::{
    registry::CatalogRegistry,
    service::CatalogService as UnrelatedService,
};
type UnrelatedAlias<M> = CatalogRegistry<M>;
struct UnrelatedHolder<M> {
    inner: UnrelatedService<M>,
}
"#,
        ),
    ];
    let valid_violations = ebd_5a1_completion_violations(&valid);
    assert!(
        valid_violations.is_empty(),
        "real service composition fields must remain accepted: {valid_violations:?}"
    );

    let mut invalid = valid;
    invalid.push(GuardSource::new(
        "src/engine/legacy_registry.rs",
        r#"
use crate::catalog::registry::CatalogRegistry as Registry;
use crate::catalog::{
    memory::MemoryCatalog as Memory,
    service::CatalogService as Service,
};
use crate::sql::catalog::provider::CatalogServiceProvider as Provider;

type LegacyRegistry<M> = Registry<M>;
type LegacyProvider<'a> = Provider<'a>;
struct LegacyMemory<M> { inner: Memory<M> }
struct LegacyService<M> {
    inner: Service<(), M>,
}
"#,
    ));
    invalid.push(GuardSource::new(
        "src/catalog/legacy_cache.rs",
        r#"
use super::schema_cache::SchemaCache as Cache;
struct LegacyCache<M>(Cache<M>);
"#,
    ));
    let violations = ebd_5a1_completion_violations(&invalid);
    for expected in [
        "catalog-runtime-type-alias: src/engine/legacy_registry.rs|LegacyRegistry|CatalogRegistry",
        "catalog-runtime-type-alias: src/engine/legacy_registry.rs|LegacyProvider|CatalogServiceProvider",
        "catalog-runtime-wrapper: src/engine/legacy_registry.rs|LegacyMemory|MemoryCatalog",
        "catalog-runtime-wrapper: src/engine/legacy_registry.rs|LegacyService|CatalogService",
        "catalog-runtime-wrapper: src/catalog/legacy_cache.rs|LegacyCache|SchemaCache",
    ] {
        assert!(
            violations.contains(expected),
            "EBD-5A1 detector missed arbitrary runtime alias/wrapper {expected}: {violations:?}"
        );
    }
}

#[test]
fn ebd_5a1_detector_allows_only_exact_sql_specialization_aliases() {
    let canonical_sources = vec![
        GuardSource::new(
            EBD_5A1_MEMORY_OWNER,
            "pub(crate) struct MemoryCatalog<T> { local: Option<T> }",
        ),
        GuardSource::new(
            EBD_5A1_REGISTRY_OWNER,
            "pub(crate) struct CatalogRegistry<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_CACHE_OWNER,
            "pub(crate) struct SchemaCache<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_SERVICE_OWNER,
            "pub(crate) struct CatalogService<T, M> { marker: std::marker::PhantomData<(T, M)> }",
        ),
        GuardSource::new(
            EBD_5A1_SQL_PROVIDER_OWNER,
            "pub(crate) struct CatalogServiceProvider<'a> { marker: std::marker::PhantomData<&'a ()> }",
        ),
        GuardSource::new(
            "src/sql/catalog/local.rs",
            r#"
use crate::catalog::memory::MemoryCatalog;
use crate::sql::planner::table::TableDef;
pub(crate) type PlannerMemoryCatalog = MemoryCatalog<TableDef>;
"#,
        ),
        GuardSource::new(
            "src/sql/catalog.rs",
            r#"
use crate::catalog::service::CatalogService;
use crate::sql::planner::table::TableDef;
use metadata::CatalogRuntimeMetadata;
pub(crate) type StandaloneCatalogService =
    CatalogService<TableDef, CatalogRuntimeMetadata>;
"#,
        ),
        GuardSource::new(
            "src/engine/mod.rs",
            "struct StandaloneState { catalog_service: Arc<StandaloneCatalogService> }",
        ),
    ];

    let canonical_violations = ebd_5a1_completion_violations(&canonical_sources);
    assert!(
        canonical_violations.is_empty(),
        "the two plan-mandated SQL specialization aliases must be accepted exactly: {canonical_violations:?}"
    );

    for (path, source, expected) in [
        (
            "src/sql/catalog/local.rs",
            r#"
use crate::catalog::memory::MemoryCatalog;
use crate::sql::planner::table::TableDef;
pub(crate) type RenamedPlannerMemoryCatalog = MemoryCatalog<TableDef>;
"#,
            "catalog-runtime-type-alias: src/sql/catalog/local.rs|RenamedPlannerMemoryCatalog|MemoryCatalog",
        ),
        (
            "src/sql/catalog/local.rs",
            r#"
use crate::catalog::memory::MemoryCatalog;
use crate::sql::planner::table::TableDef;
type PlannerMemoryCatalog = MemoryCatalog<TableDef>;
"#,
            "catalog-runtime-type-alias: src/sql/catalog/local.rs|PlannerMemoryCatalog|MemoryCatalog",
        ),
        (
            "src/sql/catalog/local.rs",
            r#"
use crate::catalog::memory::MemoryCatalog;
struct OtherTableDef;
pub(crate) type PlannerMemoryCatalog = MemoryCatalog<OtherTableDef>;
"#,
            "catalog-runtime-type-alias: src/sql/catalog/local.rs|PlannerMemoryCatalog|MemoryCatalog",
        ),
        (
            "src/sql/catalog.rs",
            r#"
use crate::catalog::service::CatalogService;
use crate::sql::planner::table::TableDef;
use metadata::CatalogRuntimeMetadata;
pub(crate) type RenamedStandaloneCatalogService =
    CatalogService<TableDef, CatalogRuntimeMetadata>;
"#,
            "catalog-runtime-type-alias: src/sql/catalog.rs|RenamedStandaloneCatalogService|CatalogService",
        ),
        (
            "src/sql/catalog.rs",
            r#"
use crate::catalog::service::CatalogService;
use crate::sql::planner::table::TableDef;
use metadata::CatalogRuntimeMetadata;
pub type StandaloneCatalogService =
    CatalogService<TableDef, CatalogRuntimeMetadata>;
"#,
            "catalog-runtime-type-alias: src/sql/catalog.rs|StandaloneCatalogService|CatalogService",
        ),
        (
            "src/sql/catalog.rs",
            r#"
use crate::catalog::service::CatalogService;
use crate::sql::planner::table::TableDef;
struct OtherMetadata;
pub(crate) type StandaloneCatalogService = CatalogService<TableDef, OtherMetadata>;
"#,
            "catalog-runtime-type-alias: src/sql/catalog.rs|StandaloneCatalogService|CatalogService",
        ),
    ] {
        let mut invalid = canonical_sources.clone();
        let index = invalid
            .iter()
            .position(|candidate| candidate.path == path)
            .expect("canonical alias fixture");
        invalid[index] = GuardSource::new(path, source);
        let violations = ebd_5a1_completion_violations(&invalid);
        assert!(
            violations.contains(expected),
            "renamed aliases and non-exact interfaces must remain rejected: expected {expected}, got {violations:?}"
        );
    }
}

#[test]
fn ebd_5a1_detector_rejects_cross_function_connector_registration_forwarding() {
    let mut sources = vec![
        GuardSource::new(
            EBD_5A1_MEMORY_OWNER,
            "pub(crate) struct MemoryCatalog<T> { local: Option<T> }",
        ),
        GuardSource::new(
            EBD_5A1_REGISTRY_OWNER,
            "pub(crate) struct CatalogRegistry<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_CACHE_OWNER,
            "pub(crate) struct SchemaCache<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_SERVICE_OWNER,
            "pub(crate) struct CatalogService<T, M> { marker: std::marker::PhantomData<(T, M)> }",
        ),
        GuardSource::new(
            EBD_5A1_SQL_PROVIDER_OWNER,
            "pub(crate) struct CatalogServiceProvider<'a> { marker: std::marker::PhantomData<&'a ()> }",
        ),
        GuardSource::new(
            "src/engine/mod.rs",
            "struct StandaloneState { catalog_service: Arc<StandaloneCatalogService> }",
        ),
    ];
    sources.push(GuardSource::new(
        "src/connector/catalog_forward.rs",
        r#"
fn install_catalog(service: &StandaloneCatalogService) {
    service.register_catalog(build_catalog());
}

fn forward_catalog(state: &StandaloneState) {
    install_catalog(&state.catalog_service);
}
"#,
    ));
    let violations = ebd_5a1_completion_violations(&sources);
    assert!(
        violations.contains(
            "catalog-runtime-connector-state-registration: src/connector/catalog_forward.rs|forward_catalog"
        ),
        "EBD-5A1 detector missed cross-function connector registration forwarding: {violations:?}"
    );
}

#[test]
fn ebd_5a1_detector_rejects_cross_file_connector_registration_forwarding() {
    let valid = vec![
        GuardSource::new(
            EBD_5A1_MEMORY_OWNER,
            "pub(crate) struct MemoryCatalog<T> { local: Option<T> }",
        ),
        GuardSource::new(
            EBD_5A1_REGISTRY_OWNER,
            "pub(crate) struct CatalogRegistry<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_CACHE_OWNER,
            "pub(crate) struct SchemaCache<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_SERVICE_OWNER,
            "pub(crate) struct CatalogService<T, M> { marker: std::marker::PhantomData<(T, M)> }",
        ),
        GuardSource::new(
            EBD_5A1_SQL_PROVIDER_OWNER,
            "pub(crate) struct CatalogServiceProvider<'a> { marker: std::marker::PhantomData<&'a ()> }",
        ),
        GuardSource::new(
            "src/engine/mod.rs",
            "struct StandaloneState { catalog_service: Arc<StandaloneCatalogService> }",
        ),
        GuardSource::new(
            "src/connector/inspect.rs",
            r#"
use crate::unrelated::service::Service;

fn inspect_unrelated(service: &Service) {
    service.inspect();
}

fn inspect_catalog(service: &StandaloneCatalogService) {
    service.catalog_names();
}

fn inspect_state(state: &StandaloneState) {
    inspect_catalog(&state.catalog_service);
}
"#,
        ),
    ];
    let valid_violations = ebd_5a1_completion_violations(&valid);
    assert!(
        valid_violations.is_empty(),
        "cross-file analysis must not report non-registration helpers: {valid_violations:?}"
    );

    let mut invalid = valid;
    invalid.push(GuardSource::new(
        "src/connector/mod.rs",
        r#"
mod iceberg;

fn install_standalone_catalogs(state: &StandaloneState) {
    iceberg::install_catalog(&state.catalog_service);
}
"#,
    ));
    invalid.push(GuardSource::new(
        "src/connector/iceberg.rs",
        r#"
use crate::catalog::service::CatalogService as Service;

pub(super) fn install_catalog(service: &Service<(), ()>) {
    service.register_catalog(build_catalog());
}
"#,
    ));
    let violations = ebd_5a1_completion_violations(&invalid);
    assert!(
        violations.contains(
            "catalog-runtime-connector-state-registration: src/connector/mod.rs|install_standalone_catalogs"
        ),
        "EBD-5A1 detector missed cross-file connector registration forwarding: {violations:?}"
    );
    assert!(
        !violations.contains(
            "catalog-runtime-connector-state-registration: src/connector/inspect.rs|inspect_state"
        ),
        "EBD-5A1 detector misreported non-registration helper forwarding: {violations:?}"
    );
}

#[test]
fn ebd_5a1_detector_resolves_associated_registration_and_same_name_impl_candidates() {
    let valid = vec![
        GuardSource::new(
            EBD_5A1_MEMORY_OWNER,
            "pub(crate) struct MemoryCatalog<T> { local: Option<T> }",
        ),
        GuardSource::new(
            EBD_5A1_REGISTRY_OWNER,
            "pub(crate) struct CatalogRegistry<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_CACHE_OWNER,
            "pub(crate) struct SchemaCache<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_SERVICE_OWNER,
            "pub(crate) struct CatalogService<T, M> { marker: std::marker::PhantomData<(T, M)> }",
        ),
        GuardSource::new(
            EBD_5A1_SQL_PROVIDER_OWNER,
            "pub(crate) struct CatalogServiceProvider<'a> { marker: std::marker::PhantomData<&'a ()> }",
        ),
        GuardSource::new(
            "src/engine/mod.rs",
            "struct StandaloneState { catalog_service: Arc<StandaloneCatalogService> }",
        ),
        GuardSource::new(
            "src/connector/inspect.rs",
            r#"
struct CatalogInspector;
impl CatalogInspector {
    fn inspect(service: &StandaloneCatalogService) {
        service.catalog_names();
    }
}

fn inspect_state(state: &StandaloneState) {
    CatalogInspector::inspect(&state.catalog_service);
}
"#,
        ),
    ];
    let valid_violations = ebd_5a1_completion_violations(&valid);
    assert!(
        valid_violations.is_empty(),
        "non-registration associated functions must remain accepted: {valid_violations:?}"
    );

    let mut invalid = valid;
    invalid.push(GuardSource::new(
        "src/connector/mod.rs",
        r#"
mod iceberg;

fn install_associated(state: &StandaloneState) {
    iceberg::CatalogInstaller::install(&state.catalog_service);
}

fn install_selected(state: &StandaloneState, installer: &iceberg::MethodInstaller) {
    installer.install(&state.catalog_service);
}
"#,
    ));
    invalid.push(GuardSource::new(
        "src/connector/iceberg.rs",
        r#"
pub(super) struct CatalogInstaller;
impl CatalogInstaller {
    pub(super) fn install(service: &StandaloneCatalogService) {
        service.register_catalog(build_catalog());
    }
}

struct CatalogObserver;
impl CatalogObserver {
    fn install(service: &StandaloneCatalogService) {
        service.catalog_names();
    }
}

pub(super) struct MethodInstaller;
impl MethodInstaller {
    pub(super) fn install(&self, service: &StandaloneCatalogService) {
        service.register_catalog(build_catalog());
    }
}

struct MethodObserver;
impl MethodObserver {
    fn install(&self, service: &StandaloneCatalogService) {
        service.catalog_names();
    }
}
"#,
    ));
    let violations = ebd_5a1_completion_violations(&invalid);
    for expected in [
        "catalog-runtime-connector-state-registration: src/connector/mod.rs|install_associated",
        "catalog-runtime-connector-state-registration: src/connector/mod.rs|install_selected",
    ] {
        assert!(
            violations.contains(expected),
            "EBD-5A1 detector missed associated or ambiguous impl registration {expected}: {violations:?}"
        );
    }
    assert!(
        !violations.contains(
            "catalog-runtime-connector-state-registration: src/connector/inspect.rs|inspect_state"
        ),
        "EBD-5A1 detector misreported a non-registration associated function: {violations:?}"
    );
}

#[test]
fn ebd_5a1_detector_rejects_renamed_engine_runtime_forwarding_facades() {
    let mut valid = vec![
        GuardSource::new(
            EBD_5A1_MEMORY_OWNER,
            "pub(crate) struct MemoryCatalog<T> { local: Option<T> }",
        ),
        GuardSource::new(
            EBD_5A1_REGISTRY_OWNER,
            "pub(crate) struct CatalogRegistry<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_CACHE_OWNER,
            "pub(crate) struct SchemaCache<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_SERVICE_OWNER,
            "pub(crate) struct CatalogService<T, M> { marker: std::marker::PhantomData<(T, M)> }",
        ),
        GuardSource::new(
            EBD_5A1_SQL_PROVIDER_OWNER,
            "pub(crate) struct CatalogServiceProvider<'a> { marker: std::marker::PhantomData<&'a ()> }",
        ),
        GuardSource::new(
            "src/engine/mod.rs",
            r#"
mod query_flow;
struct StandaloneState { catalog_service: Arc<StandaloneCatalogService> }
"#,
        ),
        GuardSource::new(
            "src/engine/query_flow.rs",
            r#"
use crate::unrelated::service::Service;

fn inspect_unrelated(service: &Service) {
    service.inspect();
}

fn execute_query(state: &StandaloneState, request: QueryRequest) -> QueryResult {
    let table = state.catalog_service.resolve_table(&request)?;
    let plan = plan_query(table, request)?;
    execute_plan(plan)
}
"#,
        ),
    ];
    let valid_violations = ebd_5a1_completion_violations(&valid);
    assert!(
        valid_violations.is_empty(),
        "multi-step engine business flows must remain accepted: {valid_violations:?}"
    );

    valid[5] = GuardSource::new(
        "src/engine/mod.rs",
        r#"
mod query_flow;
mod catalog_facade;
pub(crate) use catalog_facade::register_catalog;
struct StandaloneState { catalog_service: Arc<StandaloneCatalogService> }
"#,
    );
    valid.push(GuardSource::new(
        "src/engine/catalog_facade.rs",
        r#"
use crate::catalog::service::CatalogService as Service;

pub(crate) fn register_catalog(
    service: &Service<(), ()>,
    catalog: Arc<dyn Catalog>,
) {
    service.register_catalog(catalog);
}
"#,
    ));
    let violations = ebd_5a1_completion_violations(&valid);
    assert!(
        violations.contains(
            "catalog-runtime-engine-forwarding-facade: src/engine/catalog_facade.rs|register_catalog"
        ),
        "EBD-5A1 detector missed renamed engine catalog runtime forwarding facade: {violations:?}"
    );
}

#[test]
fn ebd_5a1_detector_only_aggregates_tainted_ambiguous_method_calls() {
    let sources = vec![
        GuardSource::new(
            EBD_5A1_MEMORY_OWNER,
            "pub(crate) struct MemoryCatalog<T> { local: Option<T> }",
        ),
        GuardSource::new(
            EBD_5A1_REGISTRY_OWNER,
            "pub(crate) struct CatalogRegistry<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_CACHE_OWNER,
            "pub(crate) struct SchemaCache<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_SERVICE_OWNER,
            "pub(crate) struct CatalogService<T, M> { marker: std::marker::PhantomData<(T, M)> }",
        ),
        GuardSource::new(
            EBD_5A1_SQL_PROVIDER_OWNER,
            "pub(crate) struct CatalogServiceProvider<'a> { marker: std::marker::PhantomData<&'a ()> }",
        ),
        GuardSource::new(
            "src/engine/mod.rs",
            "struct StandaloneState { catalog_service: Arc<StandaloneCatalogService> }",
        ),
        GuardSource::new(
            "src/connector/observer.rs",
            r#"
struct Observer;
impl Observer {
    fn install(&self) {}
}

struct CatalogInstaller;
impl CatalogInstaller {
    fn install(service: &StandaloneCatalogService) {
        service.register_catalog(build_catalog());
    }
}

fn inspect_then_observe(state: &StandaloneState, observer: &Observer) {
    state.catalog_service.catalog_names();
    observer.install();
}
"#,
        ),
    ];
    let violations = ebd_5a1_completion_violations(&sources);
    assert!(
        violations.is_empty(),
        "unrelated untainted method calls must not aggregate global registration candidates: {violations:?}"
    );
}

#[test]
fn ebd_5a1_detector_excludes_test_only_impls_but_rejects_production_impls() {
    let valid = vec![
        GuardSource::new(
            EBD_5A1_MEMORY_OWNER,
            "pub(crate) struct MemoryCatalog<T> { local: Option<T> }",
        ),
        GuardSource::new(
            EBD_5A1_REGISTRY_OWNER,
            "pub(crate) struct CatalogRegistry<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_CACHE_OWNER,
            "pub(crate) struct SchemaCache<M> { marker: std::marker::PhantomData<M> }",
        ),
        GuardSource::new(
            EBD_5A1_SERVICE_OWNER,
            "pub(crate) struct CatalogService<T, M> { marker: std::marker::PhantomData<(T, M)> }",
        ),
        GuardSource::new(
            EBD_5A1_SQL_PROVIDER_OWNER,
            "pub(crate) struct CatalogServiceProvider<'a> { marker: std::marker::PhantomData<&'a ()> }",
        ),
        GuardSource::new(
            "src/engine/mod.rs",
            "struct StandaloneState { catalog_service: Arc<StandaloneCatalogService> }",
        ),
        GuardSource::new(
            "src/engine/test_impl.rs",
            r#"
struct OwnerFixture;
#[cfg(test)]
impl OwnerFixture {
    fn execute_query_with_catalog_mgr(&self) {}
}
"#,
        ),
        GuardSource::new(
            "src/connector/test_impl.rs",
            r#"
struct ConnectorFixture;
#[cfg(test)]
impl ConnectorFixture {
    fn install(&self, state: &StandaloneState) {
        state.catalog_service.register_catalog(build_catalog());
    }
}
"#,
        ),
    ];
    let valid_violations = ebd_5a1_completion_violations(&valid);
    assert!(
        valid_violations.is_empty(),
        "entire cfg(test) impl blocks must remain excluded: {valid_violations:?}"
    );

    let mut invalid = valid;
    invalid.push(GuardSource::new(
        "src/engine/production_impl.rs",
        r#"
struct ProductionOwner;
impl ProductionOwner {
    fn execute_query_with_catalog_mgr(&self) {}
}
"#,
    ));
    invalid.push(GuardSource::new(
        "src/connector/production_impl.rs",
        r#"
struct ProductionConnector;
impl ProductionConnector {
    fn install(&self, state: &StandaloneState) {
        state.catalog_service.register_catalog(build_catalog());
    }
}
"#,
    ));
    let violations = ebd_5a1_completion_violations(&invalid);
    for expected in [
        "catalog-runtime-retired-helper: src/engine/production_impl.rs|execute_query_with_catalog_mgr",
        "catalog-runtime-connector-state-registration: src/connector/production_impl.rs|install",
    ] {
        assert!(
            violations.contains(expected),
            "EBD-5A1 detector missed production impl violation {expected}: {violations:?}"
        );
    }
}

#[test]
fn ebd_4a_catalog_identifier_boundary_is_ast_free() {
    let repo = Path::new(manifest_dir());
    let src = src_dir();
    let mut sources = Vec::new();
    for root in [&src, &repo.join("tests")] {
        for path in rs_files(root) {
            let source = rel(&path);
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {source}: {error}"));
            sources.push(GuardSource::new(&source, &text));
        }
    }

    let mut violations = BTreeSet::new();
    let owner = sources.iter().find(|source| source.path == EBD_4A_OWNER);
    if let Some(owner) = owner {
        violations.extend(ebd_4a_audit_owner_items(owner));
    } else {
        violations.insert(format!("catalog-owner-missing: {EBD_4A_OWNER}"));
    }
    for source in sources
        .iter()
        .filter(|source| source.path.starts_with("src/catalog/"))
    {
        if source.path == EBD_4B2B_OWNER {
            violations.extend(ebd_4b1_audit_schema_dependencies(source));
        } else {
            violations.extend(ebd_4a_audit_catalog_dependencies(source));
        }
    }

    let old_owner = repo.join("src/engine/name_resolve.rs");
    if old_owner.exists() {
        violations
            .insert("catalog-old-owner-still-present: src/engine/name_resolve.rs".to_string());
    }
    if let Some(engine_mod) = sources
        .iter()
        .find(|source| source.path == "src/engine/mod.rs")
        && rust_module_items(&engine_mod.text)
            .iter()
            .any(|item| item.name == "name_resolve")
    {
        violations.insert(
            "catalog-old-module-still-declared: src/engine/mod.rs|name_resolve".to_string(),
        );
    }
    violations.extend(ebd_4a_audit_exact_legacy_owner_definitions(&sources));
    violations.extend(ebd_4a_audit_legacy_paths_and_forwarding(&sources));

    assert!(
        violations.is_empty(),
        "EBD-4A catalog identifier boundary failed:\n{}",
        violations.into_iter().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn ebd_4a_detector_rejects_dependencies_and_allows_only_catalog_core() {
    let target_manifest = r#"
[dependencies]
top-level = "1"
[target.'cfg(unix)'.dependencies]
target-only = "1"
[target.'cfg(windows)'.dev-dependencies]
target-dev = "1"
"#
    .parse::<toml::Value>()
    .expect("parse target dependency fixture");
    assert_eq!(
        ebd_4a_dependency_crate_roots_from_manifest(&target_manifest),
        BTreeSet::from([
            "target_dev".to_string(),
            "target_only".to_string(),
            "top_level".to_string(),
        ])
    );

    let valid = GuardSource::new(
        EBD_4A_OWNER,
        r###"
use std::collections::BTreeSet;
use core::fmt;
use alloc::string::String;
use crate::catalog::identifier::TableIdentity;
use super::identifier::LocalTableIdentity;

pub(crate) struct CrateVisible;

// use crate::engine::StandaloneState;
const TEXT: &str = "iceberg::spec::Literal crate::sql::SqlType";
const RAW: &str = r#"arrow::array::ArrayRef"#;
fn local(_: TableIdentity, _: LocalTableIdentity) { let _ = BTreeSet::<String>::new(); }
"###,
    );
    let valid_violations = ebd_4a_audit_catalog_dependencies(&valid);
    assert!(
        valid_violations.is_empty(),
        "valid catalog-core fixture was rejected: {valid_violations:?}"
    );

    let invalid = [
        (
            "use crate::engine::StandaloneState;",
            "catalog-forbidden-StandaloneState-token",
        ),
        ("use iceberg::spec::Literal;", "catalog-forbidden-use"),
        (
            "use iceberg::{spec::Literal as Lit};",
            "catalog-forbidden-use",
        ),
        (
            "fn bad() { let _: Option<arrow::array::ArrayRef> = None; }",
            "catalog-forbidden-external-path",
        ),
        (
            "fn bad() { let _: Option<sqlparser::ast::ObjectName> = None; }",
            "catalog-forbidden-external-path",
        ),
        (
            "#[cfg(test)] mod tests { use crate::connector::ConnectorRegistry; }",
            "catalog-forbidden",
        ),
        (
            "mod nested { use super::super::super::engine::StandaloneState; }",
            "catalog-forbidden-StandaloneState-token",
        ),
        (
            "struct StandaloneState; fn bad(_: StandaloneState) {}",
            "catalog-forbidden-StandaloneState-token",
        ),
    ];
    for (text, expected) in invalid {
        let source = GuardSource::new(EBD_4A_OWNER, text);
        let violations = ebd_4a_audit_catalog_dependencies(&source);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "dependency fixture did not produce {expected}: {text}; got {violations:?}"
        );
    }
}

#[test]
fn ebd_4a_detector_requires_the_exact_canonical_owner_items() {
    let valid = GuardSource::new(
        EBD_4A_OWNER,
        r#"
pub(crate) struct LocalTableIdentity;
pub(crate) struct CatalogNamespaceIdentity;
pub(crate) struct TableIdentity;
pub(crate) fn normalize_identifier() {}
pub(crate) fn normalize_optional_identifier() {}
pub(crate) fn resolve_local_table_name() {}
pub(crate) fn resolve_catalog_namespace_name() {}
pub(crate) fn resolve_catalog_table_name() {}
"#,
    );
    assert!(ebd_4a_audit_owner_items(&valid).is_empty());

    let missing = GuardSource::new(
        EBD_4A_OWNER,
        &valid.text.replace("pub(crate) struct TableIdentity;", ""),
    );
    assert_eq!(
        ebd_4a_audit_owner_items(&missing),
        BTreeSet::from(["catalog-owner-struct-missing: TableIdentity".to_string()])
    );

    let forbidden = GuardSource::new(
        EBD_4A_OWNER,
        &format!(
            "{}\npub(crate) fn resolve_iceberg_table_name_explicit() {{}}",
            valid.text
        ),
    );
    assert_eq!(
        ebd_4a_audit_owner_items(&forbidden),
        BTreeSet::from([
            "catalog-owner-zero-caller-helper-present: resolve_iceberg_table_name_explicit"
                .to_string(),
        ])
    );
}

#[test]
fn ebd_4a_detector_rejects_only_the_exact_legacy_owner_definitions() {
    let legacy = [
        GuardSource::new(
            "src/engine/catalog.rs",
            "mod nested { pub(crate) fn normalize_identifier(_: &str) {} }",
        ),
        GuardSource::new(
            "src/engine/catalog_mgr/metadata.rs",
            "pub(crate) type TableIdentity = crate::catalog::identifier::TableIdentity;",
        ),
        GuardSource::new(
            "src/engine/mod.rs",
            "mod nested { pub(crate) type ResolvedLocalTableName = crate::catalog::identifier::LocalTableIdentity; }",
        ),
    ];
    assert_eq!(
        ebd_4a_audit_exact_legacy_owner_definitions(&legacy),
        BTreeSet::from([
            "catalog-legacy-owner-definition: src/engine/catalog.rs|function normalize_identifier"
                .to_string(),
            "catalog-legacy-owner-definition: src/engine/catalog_mgr/metadata.rs|type TableIdentity"
                .to_string(),
            "catalog-legacy-owner-definition: src/engine/mod.rs|type ResolvedLocalTableName"
                .to_string(),
        ])
    );

    let unrelated = [
        GuardSource::new(
            "src/sql/optimizer/example.rs",
            "fn normalize_identifier(_: &str) {}",
        ),
        GuardSource::new(
            "src/connector/starrocks/txn_log.rs",
            "struct TableIdentity;",
        ),
    ];
    assert!(ebd_4a_audit_exact_legacy_owner_definitions(&unrelated).is_empty());
}

#[test]
fn ebd_4a_detector_rejects_legacy_paths_and_non_private_forwarding() {
    let sources = vec![
        GuardSource::new(
            "src/catalog/mod.rs",
            r###"
use crate::catalog::identifier::TableIdentity;
// pub use crate::catalog::identifier::TableIdentity;
const TEXT: &str = "pub(crate) use crate::catalog::identifier::TableIdentity";
const RAW: &str = r#"crate::engine::catalog::normalize_identifier"#;
"###,
        ),
        GuardSource::new(
            "src/catalog/legacy.rs",
            r#"
pub use crate::catalog::identifier::TableIdentity;
pub(crate) use crate::catalog::identifier::{LocalTableIdentity, resolve_local_table_name};
pub(super) use crate::catalog::identifier::CatalogNamespaceIdentity as Namespace;
mod nested {
    pub(crate) use crate::catalog::identifier::normalize_identifier;
}
#[cfg(test)]
mod tests {
    pub(super) use crate::catalog::identifier::TableIdentity as TestIdentity;
}
"#,
        ),
        GuardSource::new(
            "src/server/example.rs",
            r#"
use crate::engine::catalog::normalize_identifier;
use crate::engine::{ResolvedLocalTableName as Local, name_resolve::*};
#[cfg(test)]
mod tests {
    use crate::engine::name_resolve as legacy;
    fn inspect() { let _ = legacy::resolve_local_table_name; }
}
"#,
        ),
        GuardSource::new(
            "src/catalog/nested_forward.rs",
            "mod nested { pub(crate) use crate::catalog::identifier::normalize_identifier; }",
        ),
        GuardSource::new(
            "src/catalog/test_forward.rs",
            "#[cfg(test)] mod tests { pub(super) use crate::catalog::identifier::TableIdentity as TestIdentity; }",
        ),
        GuardSource::new(
            "src/server/test_legacy.rs",
            "#[cfg(test)] mod tests { use crate::engine::name_resolve as legacy; fn inspect() { let _ = legacy::resolve_local_table_name; } }",
        ),
    ];
    let violations = ebd_4a_audit_legacy_paths_and_forwarding(&sources);
    assert!(
        violations
            .iter()
            .any(|item| item.contains("|pub|crate::catalog::identifier::TableIdentity"))
    );
    assert!(
        violations.iter().any(|item| item
            .contains("|pub(crate)|crate::catalog::identifier::LocalTableIdentity"))
    );
    assert!(violations.iter().any(|item| {
        item.contains("|pub(super)|crate::catalog::identifier::CatalogNamespaceIdentity")
    }));
    assert!(violations.iter().any(
        |item| item.contains("src/catalog/legacy.rs") && item.contains("normalize_identifier")
    ));
    assert!(violations.iter().any(|item| {
        item.contains("src/catalog/nested_forward.rs") && item.contains("normalize_identifier")
    }));
    assert!(violations.iter().any(|item| {
        item.contains("src/catalog/test_forward.rs") && item.contains("TableIdentity")
    }));
    assert!(
        violations
            .iter()
            .any(|item| item.contains("src/server/example.rs|crate::engine::name_resolve"))
    );
    assert!(
        violations
            .iter()
            .any(|item| { item.contains("src/server/test_legacy.rs|crate::engine::name_resolve") })
    );

    let allowed_local_helpers = [
        GuardSource::new(
            "src/sql/optimizer/rbo/rules/example.rs",
            "fn normalize_identifier(raw: &str) -> String { raw.to_string() }",
        ),
        GuardSource::new(
            "src/connector/starrocks/txn_log.rs",
            "struct TableIdentity { table: String }",
        ),
    ];
    assert!(
        ebd_4a_audit_legacy_paths_and_forwarding(&allowed_local_helpers).is_empty(),
        "unrelated local helpers must not be treated as legacy EBD-4A owners"
    );
}

fn assert_growth_axis_rejected(actual: EngineBoundarySnapshot, expected_prefix: &str) {
    let violations = engine_boundary_violations(&actual, &EMPTY_BASELINE);
    assert!(
        violations
            .iter()
            .any(|item| item.starts_with(expected_prefix)),
        "missing {expected_prefix} in {violations:?}"
    );
}

#[test]
fn ebd_1_exact_baseline_rejects_each_growth_axis() {
    let clean = EngineBoundarySnapshot::default();
    assert!(engine_boundary_violations(&clean, &EMPTY_BASELINE).is_empty());

    let mut file_growth = clean.clone();
    file_growth
        .engine_files
        .insert("src/engine/junk_drawer.rs".to_string());
    assert_growth_axis_rejected(file_growth, "engine-file-unexpected:");

    let mut dependency_growth = clean.clone();
    dependency_growth
        .external_engine_dependencies
        .entry("src/sql/new_consumer.rs".to_string())
        .or_default()
        .insert("crate::engine::catalog::TableDef".to_string());
    assert_growth_axis_rejected(dependency_growth, "engine-dependency-unexpected:");

    let mut forwarding_growth = clean.clone();
    forwarding_growth.forwarding_reexports.insert(
        "src/catalog/legacy.rs|crate::catalog::legacy|pub(crate)|catalog|crate::engine::catalog"
            .to_string(),
    );
    assert_growth_axis_rejected(forwarding_growth, "forwarding-reexport-unexpected:");

    let mut frontend_growth = clean;
    frontend_growth
        .lower_layer_frontend_dependencies
        .insert("src/sql/new_consumer.rs|crate::frontend::Frontend".to_string());
    assert_growth_axis_rejected(frontend_growth, "frontend-reverse-dependency:");
}

#[test]
fn ebd_1_detector_rejects_new_engine_dependencies_and_forwarding() {
    let sources = vec![
        GuardSource::new(
            "src/sql/example.rs",
            r#"
use crate::engine::{catalog::TableDef, StandaloneState as State};
use crate::engine::mv as legacy_mv;
fn inspect(_: &State) { let _ = legacy_mv::table_ref::IcebergTableRef::default; }
"#,
        ),
        GuardSource::new(
            "src/catalog/legacy.rs",
            "pub(crate) use crate::engine::catalog::*;",
        ),
    ];
    let actual =
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &sources);
    assert_eq!(
        actual.external_engine_dependencies,
        BTreeMap::from([
            (
                "src/catalog/legacy.rs".to_string(),
                BTreeSet::from(["crate::engine::catalog::*".to_string()]),
            ),
            (
                "src/sql/example.rs".to_string(),
                BTreeSet::from([
                    "crate::engine::StandaloneState".to_string(),
                    "crate::engine::catalog::TableDef".to_string(),
                    "crate::engine::mv".to_string(),
                ]),
            ),
        ])
    );
    assert_eq!(
        actual.standalone_state_dependencies,
        BTreeMap::from([(
            "src/sql/example.rs".to_string(),
            BTreeSet::from(["crate::engine::StandaloneState".to_string()]),
        )])
    );
    assert_eq!(
        actual.forwarding_reexports,
        BTreeSet::from([
            "src/catalog/legacy.rs|crate::catalog::legacy|pub(crate)|*|crate::engine::catalog::*"
                .to_string(),
        ])
    );
    assert!(actual.engine_files.is_empty());
    assert!(actual.engine_module_declarations.is_empty());
    assert!(actual.lower_layer_frontend_dependencies.is_empty());
    assert!(!engine_boundary_violations(&actual, &EMPTY_BASELINE).is_empty());
}

#[test]
fn ebd_1_detector_ignores_test_noise_and_resolves_relative_aliases() {
    let source = GuardSource::new(
        "src/sql/nested/example.rs",
        r#"
// use crate::engine::catalog::TableDef;
const TEXT: &str = "crate::engine::StandaloneState";
#[cfg(test)]
mod tests { use crate::engine::StandaloneState; }
use crate::engine::catalog as legacy_catalog;
fn production() { let _ = legacy_catalog::normalize_identifier("x"); }
"#,
    );
    let actual =
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &[source]);
    let paths = actual
        .external_engine_dependencies
        .get("src/sql/nested/example.rs")
        .expect("production alias dependency");
    assert_eq!(
        paths,
        &BTreeSet::from(["crate::engine::catalog".to_string()])
    );
    assert!(actual.standalone_state_dependencies.is_empty());
}

#[test]
fn ebd_1_detector_resolves_forwarding_aliases_and_inline_relative_paths() {
    let sources = vec![
        GuardSource::new(
            "src/catalog/legacy.rs",
            r#"
use crate::engine as legacy;
pub use legacy::catalog::TableDef;
"#,
        ),
        GuardSource::new(
            "src/engine/catalog.rs",
            r#"
mod nested {
    pub(crate) use super::super::super::sql::catalog::TableDef;
}
"#,
        ),
    ];
    let actual =
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &sources);
    assert_eq!(
        actual.forwarding_reexports,
        BTreeSet::from([
            "src/catalog/legacy.rs|crate::catalog::legacy|pub|TableDef|crate::engine::catalog::TableDef".to_string(),
            "src/engine/catalog.rs|crate::engine::catalog::nested|pub(crate)|TableDef|crate::sql::catalog::TableDef".to_string(),
        ])
    );
}

#[test]
fn ebd_1_detector_rejects_standalone_state_associated_items_only() {
    let source = GuardSource::new(
        "src/sql/example.rs",
        r#"
fn production() {
    let _ = crate::engine::StandaloneState::new();
    let _ = crate::engine::StandaloneState::default();
    let _ = crate::engine::StandaloneStateFactory::new();
}
"#,
    );
    let actual =
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &[source]);
    assert_eq!(
        actual
            .standalone_state_dependencies
            .get("src/sql/example.rs"),
        Some(&BTreeSet::from([
            "crate::engine::StandaloneState::default".to_string(),
            "crate::engine::StandaloneState::new".to_string(),
        ]))
    );
}

#[test]
fn ebd_1_detector_distinguishes_path_affecting_module_attributes() {
    let plain = GuardSource::new("src/engine/mod.rs", "mod aggregate;");
    let direct = GuardSource::new(
        "src/engine/mod.rs",
        "#[path = \"aggregate.rs\"] mod aggregate;",
    );
    let conditional = GuardSource::new(
        "src/engine/mod.rs",
        "#[cfg_attr(feature = \"alternate\", path = \"alternate.rs\")] mod aggregate;",
    );

    let collect = |source| {
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &[source])
            .engine_module_declarations
    };
    let plain = collect(plain);
    let direct = collect(direct);
    let conditional = collect(conditional);

    assert_eq!(
        plain,
        BTreeSet::from(["src/engine/mod.rs||external|path=default|aggregate".to_string()])
    );
    assert_eq!(
        direct,
        BTreeSet::from([
            "src/engine/mod.rs||external|path=direct:src/engine/aggregate.rs|aggregate".to_string()
        ])
    );
    assert_eq!(
        conditional,
        BTreeSet::from([
            "src/engine/mod.rs||external|path=default;cfg:#[cfg_attr(feature=\"alternate\",path=\"alternate.rs\")]=>src/engine/alternate.rs|aggregate".to_string()
        ])
    );
}

#[test]
fn ebd_1_detector_distinguishes_forwarding_export_aliases() {
    let source = GuardSource::new(
        "src/catalog/legacy.rs",
        r#"
pub use crate::engine::catalog::TableDef as FirstTableDef;
pub use crate::engine::catalog::TableDef as SecondTableDef;
"#,
    );
    let actual =
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &[source]);

    assert_eq!(
        actual.forwarding_reexports,
        BTreeSet::from([
            "src/catalog/legacy.rs|crate::catalog::legacy|pub|FirstTableDef|crate::engine::catalog::TableDef".to_string(),
            "src/catalog/legacy.rs|crate::catalog::legacy|pub|SecondTableDef|crate::engine::catalog::TableDef".to_string(),
        ])
    );
}

#[test]
fn ebd_1_detector_distinguishes_forwarding_export_scopes() {
    let source = GuardSource::new(
        "src/catalog/legacy.rs",
        r#"
mod first {
    pub(crate) use crate::engine::catalog::TableDef;
}
mod second {
    pub(crate) use crate::engine::catalog::TableDef;
}
"#,
    );
    let actual =
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &[source]);

    assert_eq!(
        actual.forwarding_reexports,
        BTreeSet::from([
            "src/catalog/legacy.rs|crate::catalog::legacy::first|pub(crate)|TableDef|crate::engine::catalog::TableDef".to_string(),
            "src/catalog/legacy.rs|crate::catalog::legacy::second|pub(crate)|TableDef|crate::engine::catalog::TableDef".to_string(),
        ])
    );
}

#[test]
fn ebd_1_detector_preserves_parent_visible_forwarding_name_through_alias() {
    let collect = |alias: &str| {
        let source = GuardSource::new(
            "src/catalog/legacy.rs",
            &format!(
                r#"
mod boundary {{
    mod aliases {{
        pub(crate) use crate::engine::catalog::TableDef as {alias};
    }}
    pub(super) use aliases::{alias};
}}
"#
            ),
        );
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &[source])
            .forwarding_reexports
    };

    let legacy = collect("Legacy");
    let renamed = collect("RenamedLegacy");
    assert_eq!(
        legacy,
        BTreeSet::from([
            "src/catalog/legacy.rs|crate::catalog::legacy::boundary|pub(super)|Legacy|crate::engine::catalog::TableDef".to_string(),
            "src/catalog/legacy.rs|crate::catalog::legacy::boundary::aliases|pub(crate)|Legacy|crate::engine::catalog::TableDef".to_string(),
        ])
    );
    assert_eq!(
        renamed,
        BTreeSet::from([
            "src/catalog/legacy.rs|crate::catalog::legacy::boundary|pub(super)|RenamedLegacy|crate::engine::catalog::TableDef".to_string(),
            "src/catalog/legacy.rs|crate::catalog::legacy::boundary::aliases|pub(crate)|RenamedLegacy|crate::engine::catalog::TableDef".to_string(),
        ])
    );
    assert_ne!(
        legacy, renamed,
        "renaming the public alias must change the key"
    );
}

#[test]
fn ebd_1_detector_resolves_module_qualified_forwarding_alias() {
    let source = GuardSource::new(
        "src/catalog/legacy.rs",
        r#"
mod aliases {
    pub(crate) use crate::engine::catalog::TableDef as Legacy;
}
mod exports {
    pub(crate) use super::aliases::Legacy;
}
"#,
    );
    let actual =
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &[source]);

    assert_eq!(
        actual.forwarding_reexports,
        BTreeSet::from([
            "src/catalog/legacy.rs|crate::catalog::legacy::aliases|pub(crate)|Legacy|crate::engine::catalog::TableDef".to_string(),
            "src/catalog/legacy.rs|crate::catalog::legacy::exports|pub(crate)|Legacy|crate::engine::catalog::TableDef".to_string(),
        ])
    );
}

#[test]
fn ebd_1_detector_resolves_intermediate_module_qualified_forwarding_alias() {
    let source = GuardSource::new(
        "src/catalog/legacy.rs",
        r#"
mod aliases {
    pub(crate) use crate::engine as legacy;
}
mod exports {
    pub(crate) use super::aliases::legacy::catalog::TableDef;
}
"#,
    );
    let actual =
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &[source]);

    assert_eq!(
        actual.forwarding_reexports,
        BTreeSet::from([
            "src/catalog/legacy.rs|crate::catalog::legacy::aliases|pub(crate)|legacy|crate::engine"
                .to_string(),
            "src/catalog/legacy.rs|crate::catalog::legacy::exports|pub(crate)|TableDef|crate::engine::catalog::TableDef"
                .to_string(),
        ])
    );
}

#[test]
fn ebd_1_detector_distinguishes_path_cfg_attr_predicates() {
    let collect = |feature: &str| {
        let source = GuardSource::new(
            "src/engine/mod.rs",
            &format!(r#"#[cfg_attr(feature = "{feature}", path = "shared.rs")] mod aggregate;"#),
        );
        collect_engine_boundary_snapshot(Path::new("fixture-root-that-does-not-exist"), &[source])
            .engine_module_declarations
    };

    let feature_a = collect("a");
    let feature_b = collect("b");
    assert_eq!(
        feature_a,
        BTreeSet::from([
            "src/engine/mod.rs||external|path=default;cfg:#[cfg_attr(feature=\"a\",path=\"shared.rs\")]=>src/engine/shared.rs|aggregate".to_string(),
        ])
    );
    assert_eq!(
        feature_b,
        BTreeSet::from([
            "src/engine/mod.rs||external|path=default;cfg:#[cfg_attr(feature=\"b\",path=\"shared.rs\")]=>src/engine/shared.rs|aggregate".to_string(),
        ])
    );
    assert_ne!(feature_a, feature_b);

    let test_only = GuardSource::new(
        "src/engine/mod.rs",
        r#"#[cfg_attr(test, path = "test_only.rs")] mod aggregate;"#,
    );
    assert_eq!(
        collect_engine_boundary_snapshot(
            Path::new("fixture-root-that-does-not-exist"),
            &[test_only],
        )
        .engine_module_declarations,
        BTreeSet::from(["src/engine/mod.rs||external|path=default|aggregate".to_string()])
    );
}
