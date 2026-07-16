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
        path: "src/engine/catalog.rs",
        target_owner: "catalog",
        migration_task: "EBD-4",
    },
    EngineFileOwner {
        path: "src/engine/catalog_mgr/catalog.rs",
        target_owner: "catalog",
        migration_task: "EBD-5A",
    },
    EngineFileOwner {
        path: "src/engine/catalog_mgr/iceberg.rs",
        target_owner: "catalog",
        migration_task: "EBD-5A",
    },
    EngineFileOwner {
        path: "src/engine/catalog_mgr/internal.rs",
        target_owner: "catalog",
        migration_task: "EBD-5A",
    },
    EngineFileOwner {
        path: "src/engine/catalog_mgr/metadata.rs",
        target_owner: "catalog",
        migration_task: "EBD-5A",
    },
    EngineFileOwner {
        path: "src/engine/catalog_mgr/mod.rs",
        target_owner: "catalog",
        migration_task: "EBD-5A",
    },
    EngineFileOwner {
        path: "src/engine/catalog_mgr/provider.rs",
        target_owner: "catalog",
        migration_task: "EBD-5A",
    },
    EngineFileOwner {
        path: "src/engine/catalog_mgr/schema_cache.rs",
        target_owner: "catalog",
        migration_task: "EBD-5A",
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
        path: "src/engine/dictionary/maintenance.rs",
        target_owner: "dictionary",
        migration_task: "EBD-8",
    },
    EngineFileOwner {
        path: "src/engine/dictionary/mod.rs",
        target_owner: "dictionary",
        migration_task: "EBD-8",
    },
    EngineFileOwner {
        path: "src/engine/dictionary/model.rs",
        target_owner: "dictionary",
        migration_task: "EBD-8",
    },
    EngineFileOwner {
        path: "src/engine/dictionary/rebuild.rs",
        target_owner: "dictionary",
        migration_task: "EBD-8",
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
    "src/engine/catalog_mgr/mod.rs||external|path=default|catalog",
    "src/engine/catalog_mgr/mod.rs||external|path=default|iceberg",
    "src/engine/catalog_mgr/mod.rs||external|path=default|internal",
    "src/engine/catalog_mgr/mod.rs||external|path=default|metadata",
    "src/engine/catalog_mgr/mod.rs||external|path=default|provider",
    "src/engine/catalog_mgr/mod.rs||external|path=default|schema_cache",
    "src/engine/dictionary/mod.rs||external|path=default|maintenance",
    "src/engine/dictionary/mod.rs||external|path=default|model",
    "src/engine/dictionary/mod.rs||external|path=default|rebuild",
    "src/engine/mod.rs||external|path=default|aggregate",
    "src/engine/mod.rs||external|path=default|backend_ops",
    "src/engine/mod.rs||external|path=default|backend_resolver",
    "src/engine/mod.rs||external|path=default|catalog",
    "src/engine/mod.rs||external|path=default|catalog_mgr",
    "src/engine/mod.rs||external|path=default|delete_flow",
    "src/engine/mod.rs||external|path=default|delete_predicate_translate",
    "src/engine/mod.rs||external|path=default|dictionary",
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
            "crate::engine::execute_query_with_catalog_mgr",
            "crate::engine::iceberg_writer::invalidate_iceberg_caches",
        ],
    ),
    (
        "src/connector/iceberg/catalog/registry.rs",
        &["crate::engine::catalog::ColumnDef"],
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
            "crate::engine::catalog::InMemoryCatalog",
            "crate::engine::catalog_mgr::iceberg::IcebergCatalog::new",
            "crate::engine::catalog_mgr::internal::InternalCatalog::new",
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
        "src/connector/starrocks/table/catalog.rs",
        &[
            "crate::engine::catalog::ColumnDef",
            "crate::engine::catalog::InMemoryCatalog",
            "crate::engine::catalog::PhysicalTableLayout",
            "crate::engine::catalog::ScanSource",
            "crate::engine::catalog::StarRocksTabletRef",
            "crate::engine::catalog::TableDef",
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
            "crate::engine::catalog::InMemoryCatalog",
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
            "crate::engine::catalog::ColumnDef",
            "crate::engine::dictionary::maintenance::mark_starrocks_table_stale",
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
            "crate::engine::catalog::DEFAULT_DATABASE",
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
        "src/engine/dictionary/maintenance.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/dictionary/mod.rs",
        &["crate::engine::StandaloneState"],
    ),
    (
        "src/engine/dictionary/rebuild.rs",
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

const FORWARDING_REEXPORTS: &[&str] = &[
    "src/engine/catalog.rs|crate::engine::catalog|pub|CatalogProvider|crate::sql::catalog::CatalogProvider",
    "src/engine/catalog.rs|crate::engine::catalog|pub|ColumnDef|crate::sql::catalog::ColumnDef",
    "src/engine/catalog.rs|crate::engine::catalog|pub|PhysicalTableLayout|crate::sql::catalog::PhysicalTableLayout",
    "src/engine/catalog.rs|crate::engine::catalog|pub|ScanSource|crate::sql::catalog::ScanSource",
    "src/engine/catalog.rs|crate::engine::catalog|pub|StarRocksTabletRef|crate::sql::catalog::StarRocksTabletRef",
    "src/engine/catalog.rs|crate::engine::catalog|pub|TableDef|crate::sql::catalog::TableDef",
    "src/engine/dictionary/model.rs|crate::engine::dictionary::model|pub(crate)|DictionaryOwner|crate::sql::common::dictionary::DictionaryOwner",
    "src/engine/dictionary/model.rs|crate::engine::dictionary::model|pub(crate)|DictionarySnapshot|crate::sql::common::dictionary::DictionarySnapshot",
    "src/engine/dictionary/model.rs|crate::engine::dictionary::model|pub(crate)|DictionaryState|crate::sql::common::dictionary::DictionaryState",
    "src/engine/dictionary/model.rs|crate::engine::dictionary::model|pub(crate)|DictionaryValue|crate::sql::common::dictionary::DictionaryValue",
    "src/engine/dictionary/model.rs|crate::engine::dictionary::model|pub(crate)|DictionaryWatermark|crate::sql::common::dictionary::DictionaryWatermark",
    "src/engine/dictionary/model.rs|crate::engine::dictionary::model|pub(crate)|QueryDictionarySelection|crate::sql::common::dictionary::QueryDictionarySelection",
    "src/engine/dictionary/model.rs|crate::engine::dictionary::model|pub(crate)|StarRocksTabletWatermark|crate::sql::common::dictionary::StarRocksTabletWatermark",
    "src/engine/mod.rs|crate::engine|pub|CatalogProvider|crate::sql::catalog::CatalogProvider",
    "src/engine/mod.rs|crate::engine|pub|ColumnDef|crate::sql::catalog::ColumnDef",
    "src/engine/mod.rs|crate::engine|pub|ScanSource|crate::sql::catalog::ScanSource",
    "src/engine/mod.rs|crate::engine|pub|TableDef|crate::sql::catalog::TableDef",
];

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
        "dictionary",
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
        actual.engine_files.len() >= 88,
        "EBD-1 must scan the full engine tree, found only {} files",
        actual.engine_files.len()
    );
    assert!(
        !actual.external_engine_dependencies.is_empty(),
        "EBD-1 engine dependency scan must be non-vacuous"
    );
    assert!(
        !actual.forwarding_reexports.is_empty(),
        "EBD-1 must retain the known forwarding debt until its owner task removes it"
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
            if segments
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
        let mut audit = CratePathAudit {
            source: &source.path,
            violations: BTreeSet::new(),
        };
        syn::visit::Visit::visit_file(&mut audit, &file);
        violations.extend(audit.violations);
    }
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
    if let Some(catalog) = sources
        .iter()
        .find(|source| source.path == "src/sql/catalog.rs")
    {
        violations.extend(ebd_4b1_audit_column_def(catalog));
    } else {
        violations
            .insert("catalog-schema-column-def-owner-missing: src/sql/catalog.rs".to_string());
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
    if let Some(catalog) = sources
        .iter()
        .find(|source| source.path == "src/sql/catalog.rs")
    {
        violations.extend(ebd_4b2a_audit_column_def(catalog));
        violations.extend(ebd_4b2a_audit_catalog_raw_iceberg_literals(catalog));
    } else {
        violations
            .insert("catalog-default-column-def-owner-missing: src/sql/catalog.rs".to_string());
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
    let expected_counts = [88, 86, 224, 58, 17];
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
        violations.extend(ebd_4a_audit_catalog_dependencies(source));
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
