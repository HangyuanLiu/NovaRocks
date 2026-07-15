use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use super::{
    cfg_attr_generated_path_values, cfg_attribute_requires_test, manifest_dir,
    path_attribute_value, production_rs_files_from_entries, rel, rs_files,
    rust_canonical_use_segments_in_scope, rust_module_items, rust_production_canonical_paths,
    rust_production_scoped_use_statements, rust_sanitized_production_text,
    rust_source_module_segments, rust_use_visibility, src_dir,
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
        path: "src/engine/name_resolve.rs",
        target_owner: "catalog",
        migration_task: "EBD-4",
    },
    EngineFileOwner {
        path: "src/engine/parquet.rs",
        target_owner: "formats",
        migration_task: "EBD-3A",
    },
    EngineFileOwner {
        path: "src/engine/procedure.rs",
        target_owner: "sql",
        migration_task: "EBD-2",
    },
    EngineFileOwner {
        path: "src/engine/query_options.rs",
        target_owner: "split:frontend,runtime",
        migration_task: "EBD-3B",
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
        path: "src/engine/sql_expr.rs",
        target_owner: "sql",
        migration_task: "EBD-2",
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
        path: "src/engine/stream_load.rs",
        target_owner: "formats",
        migration_task: "EBD-3A",
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
    "src/engine/mod.rs||external|path=default|name_resolve",
    "src/engine/mod.rs||external|path=default|parquet",
    "src/engine/mod.rs||external|path=default|procedure",
    "src/engine/mod.rs||external|path=default|query_options",
    "src/engine/mod.rs||external|path=default|query_prep",
    "src/engine/mod.rs||external|path=default|query_stats",
    "src/engine/mod.rs||external|path=default|sql_expr",
    "src/engine/mod.rs||external|path=default|statement",
    "src/engine/mod.rs||external|path=default|statistics",
    "src/engine/mod.rs||external|path=default|stream_load",
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
        "src/connector/iceberg/catalog/add_files.rs",
        &["crate::engine::catalog::normalize_identifier"],
    ),
    (
        "src/connector/iceberg/catalog/backend.rs",
        &["crate::engine::catalog::normalize_identifier"],
    ),
    (
        "src/connector/iceberg/catalog/registry.rs",
        &[
            "crate::engine::catalog::ColumnDef",
            "crate::engine::catalog::normalize_identifier",
            "crate::engine::parquet::parse_datetime_string_to_nanos",
            "crate::engine::sql_expr::latin1_string_to_bytes",
            "crate::engine::sql_expr::literal_to_i128_for_integer",
        ],
    ),
    (
        "src/connector/iceberg/catalog/schema_update.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::backend_resolver::TargetBackend",
            "crate::engine::backend_resolver::resolve_existing_table_target",
            "crate::engine::catalog::normalize_identifier",
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
        "src/connector/iceberg/catalog/views.rs",
        &["crate::engine::catalog::normalize_identifier"],
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
        "src/connector/iceberg/partition_spec.rs",
        &["crate::engine::catalog::normalize_identifier"],
    ),
    (
        "src/connector/iceberg/variant_write.rs",
        &["crate::engine::catalog::normalize_identifier"],
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
            "crate::engine::catalog::normalize_identifier",
        ],
    ),
    (
        "src/connector/starrocks/table/ddl.rs",
        &[
            "crate::engine::StandaloneState",
            "crate::engine::StatementResult",
            "crate::engine::catalog::normalize_identifier",
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
            "crate::engine::QueryResult",
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
            "crate::engine::QueryResult",
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
            "crate::engine::QueryResult",
            "crate::engine::QueryResultColumn",
            "crate::engine::StandaloneState",
            "crate::engine::StatementResult",
            "crate::engine::catalog::normalize_identifier",
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
            "crate::engine::record_batch_to_chunk",
        ],
    ),
    (
        "src/connector/starrocks/table/mv_refresh.rs",
        &[
            "crate::engine::QueryResult",
            "crate::engine::ResolvedLocalTableName",
            "crate::engine::StandaloneState",
            "crate::engine::StatementResult",
            "crate::engine::catalog::normalize_identifier",
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
            "crate::engine::record_batch_to_chunk",
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
        "src/connector/starrocks/table/schema_adapter.rs",
        &["crate::engine::catalog::normalize_identifier"],
    ),
    (
        "src/connector/starrocks/table/txn.rs",
        &[
            "crate::engine::ResolvedLocalTableName",
            "crate::engine::StandaloneState",
            "crate::engine::StatementResult",
            "crate::engine::build_local_insert_batch",
            "crate::engine::catalog::ColumnDef",
            "crate::engine::catalog::normalize_identifier",
            "crate::engine::dictionary::maintenance::mark_starrocks_table_stale",
            "crate::engine::execute_query",
            "crate::engine::mv::agg_state::mv_agg_state",
            "crate::engine::parquet::normalize_map_entries_nullability",
            "crate::engine::record_batch_to_chunk",
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
        "src/server/encoding.rs",
        &[
            "crate::engine::QueryResult",
            "crate::engine::QueryResultColumn",
        ],
    ),
    (
        "src/server/mod.rs",
        &[
            "crate::engine::QueryResult",
            "crate::engine::StandaloneNovaRocks",
            "crate::engine::StandaloneOptions",
            "crate::engine::StatementResult",
            "crate::engine::catalog::DEFAULT_DATABASE",
            "crate::engine::catalog::normalize_identifier",
            "crate::engine::mv_maintenance::MaintenanceCoordinatorConfig",
            "crate::engine::mv_maintenance::start_maintenance_coordinator_for_server",
            "crate::engine::mv_scheduler::RefreshCoordinatorConfig",
            "crate::engine::mv_scheduler::start_refresh_coordinator_for_server",
            "crate::engine::query_options::StandaloneQueryOptions",
            "crate::engine::sql_expr::literal_from_batch",
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
        "src/sql/parser/dialect/create_table.rs",
        &[
            "crate::engine::catalog::normalize_identifier",
            "crate::engine::parquet::parse_date_string_to_days",
            "crate::engine::parquet::parse_datetime_string_to_micros",
            "crate::engine::parquet::parse_datetime_string_to_nanos",
        ],
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
    "src/engine/mod.rs|crate::engine|pub|QueryResultColumn|crate::runtime::query_result::QueryResultColumn",
    "src/engine/mod.rs|crate::engine|pub|QueryResult|crate::runtime::query_result::QueryResult",
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

fn forwarding_export_name(import: &str, target: &[String]) -> Option<String> {
    let imported = import.split_once('|').map_or(import, |(_, path)| path);
    if let Some((_, alias)) = imported.rsplit_once(" as ") {
        return Some(alias.to_string());
    }
    target.last().cloned()
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

    for import in rust_production_scoped_use_statements(&production)
        .into_iter()
        .filter(|import| rust_use_visibility(&import.import) != "private")
    {
        let visibility = rust_use_visibility(&import.import);
        let Some(target) = rust_canonical_use_segments_in_scope(
            &import.import,
            &source.path,
            &import.inline_modules,
        ) else {
            continue;
        };
        let source_engine = source_is_engine(&source.path);
        let target_engine = is_engine_path(&target);
        if source_engine != target_engine {
            let Some(export_scope) = forwarding_export_scope(&source.path, &import.inline_modules)
            else {
                continue;
            };
            let Some(export_name) = forwarding_export_name(&import.import, &target) else {
                continue;
            };
            snapshot.forwarding_reexports.insert(format!(
                "{}|{}|{}|{}|{}",
                source.path,
                export_scope,
                visibility,
                export_name,
                target.join("::")
            ));
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

fn module_path_metadata(source_path: &str, attributes: &[String]) -> String {
    let direct = attributes
        .iter()
        .filter_map(|attribute| path_attribute_value(attribute))
        .map(|target| normalized_module_target(source_path, &target))
        .collect::<BTreeSet<_>>();
    let conditional = attributes
        .iter()
        .flat_map(|attribute| cfg_attr_generated_path_values(attribute))
        .map(|target| normalized_module_target(source_path, &target))
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
        actual.engine_files.len() >= 90,
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
    let violations = engine_boundary_violations(&actual, &EMPTY_BASELINE);
    assert!(
        violations
            .iter()
            .any(|item| item.contains("crate::engine::catalog")),
        "grouped engine import must be rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("StandaloneState")),
        "StandaloneState alias must be rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|item| item.contains("forwarding-reexport")),
        "public forwarding must be rejected: {violations:?}"
    );
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
            "src/engine/mod.rs||external|path=default;cfg:src/engine/alternate.rs|aggregate"
                .to_string()
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
