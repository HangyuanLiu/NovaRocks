use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::datatypes::DataType;

use crate::plan_nodes;
use crate::sql::analysis::ExprKind;
use crate::sql::codegen::expr_compiler::ExprCompiler;
use crate::sql::codegen::fragment_builder::{
    PlanFragmentBuilder, add_iceberg_equality_delete_required_columns,
    effective_iceberg_scan_column_names, iceberg_scan_table_handle_for_codegen, iceberg_table_info,
    synthetic_iceberg_table_id,
};
use crate::sql::codegen::helpers::typed_expr_display_name_without_qualifiers;
use crate::sql::codegen::nodes;
use crate::sql::codegen::resolve::{ColumnBinding, ExprScope, ResolvedTable};
use crate::sql::codegen::type_infer;
use crate::sql::codegen::{FragmentId, OutputColumn};
use crate::sql::optimizer::operator::{PhysicalProjectOp, PhysicalScanOp, ScanDictionaryColumn};

pub(crate) fn lower_distributed_plan() {
    unimplemented!("lower_distributed_plan is added by a later IR slice")
}

pub(crate) struct LoweringCtx<'b, 'a> {
    builder: &'b mut PlanFragmentBuilder<'a>,
}

impl<'b, 'a> LoweringCtx<'b, 'a> {
    pub(crate) fn new(builder: &'b mut PlanFragmentBuilder<'a>) -> Self {
        Self { builder }
    }

    pub(crate) fn lower_scan(
        &mut self,
        scan_node_id: i32,
        scan_tuple_id: i32,
        op: &PhysicalScanOp,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope), String> {
        let builder = &mut *self.builder;
        let table = builder.refresh_scan_table_for_codegen(&op.table)?;

        let mut scope = ExprScope::new();
        let qualifier = op.alias.as_deref().or(Some(&table.name));
        let mut slot_to_column = HashMap::new();
        let mut iceberg_metadata_pseudo_column_slots = BTreeSet::new();

        // Determine which columns to emit
        let planned_scan = match &table.source {
            crate::sql::catalog::ScanSource::StarRocks { db_id, table_id } => {
                let planner = builder.connectors.scan_planner("starrocks")?;
                let table_handle =
                    crate::connector::starrocks::table::StarRocksTableScanPlanner::table_handle_from_source(
                        &op.database,
                        &table.name,
                        *db_id,
                        *table_id,
                    );
                let scan = planner.begin_scan(
                    table_handle,
                    crate::connector::scan_planning::BeginScanContext,
                )?;
                let splits = planner
                    .plan_splits(&scan, crate::connector::scan_planning::SplitPlanningContext)?;
                Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
            }
            crate::sql::catalog::ScanSource::IcebergDataFiles {
                table: iceberg_table,
                files,
                ..
            } => {
                let planner = builder.connectors.scan_planner("iceberg")?;
                let column_names = effective_iceberg_scan_column_names(&table);
                let table_handle = iceberg_scan_table_handle_for_codegen(
                    &op.table.source,
                    iceberg_table,
                    files.clone(),
                    column_names,
                );
                let scan = planner.begin_scan(
                    table_handle,
                    crate::connector::scan_planning::BeginScanContext,
                )?;
                let splits = planner
                    .plan_splits(&scan, crate::connector::scan_planning::SplitPlanningContext)?;
                Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
            }
            _ => None,
        };
        let mut required: Option<std::collections::HashSet<String>> = op
            .required_columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.to_lowercase()).collect());
        if let Some(required) = required.as_mut() {
            add_iceberg_equality_delete_required_columns(required, &table, planned_scan.as_ref())?;
            for variant_column in &op.variant_columns {
                required.insert(variant_column.source_column.to_lowercase());
            }
        }
        let scan_table_id = match &table.source {
            crate::sql::catalog::ScanSource::StarRocks { table_id, .. } => Some(*table_id),
            _ => iceberg_table_info(&table.source)
                .is_some()
                .then_some(synthetic_iceberg_table_id(scan_node_id)),
        };
        if let Some(table_id) = scan_table_id {
            builder
                .desc_builder
                .add_table_for_scan(table_id, &op.database, &table);
        }

        // Build a quick lookup so the column registration loop below can
        // recognise base-table columns that the dict rewriter retargeted
        // to a hidden `__nr_dict_<t>_<c>` Int32 slot. For those columns
        // we allocate ONE slot (at the source column's storage position)
        // named after the dict column and typed Int32 — keeping the
        // single-slot-per-column contract the StarRocks lake scan
        // expects (see `src/lower/node/lake_scan.rs`'s
        // `dict_int_to_string` self-map handling).
        let dict_source_to_target: HashMap<String, &ScanDictionaryColumn> = op
            .dict_columns
            .iter()
            .map(|dc| (dc.source_column.to_ascii_lowercase(), dc))
            .collect();
        // Track dict slot ids by source column so the second loop over
        // `op.dict_columns` doesn't re-allocate a slot for the same
        // column. Also accumulates the `(dict_slot_id, dict_col)` pairs
        // that feed the TGlobalDict / dict_string_id_to_int_ids payload
        // construction further down.
        let mut dict_slot_for_source: HashMap<String, i32> = HashMap::new();
        let mut dict_slot_to_dict: Vec<(i32, &ScanDictionaryColumn)> = Vec::new();
        let mut physical_slot_by_column: HashMap<String, i32> = HashMap::new();
        for (idx, col) in table.columns.iter().enumerate() {
            // The dict rewriter renames the source string column to the
            // dict column name in `op.columns` / `op.required_columns`,
            // so check membership using BOTH names when a dict mapping
            // exists for this base column.
            let dict_target = dict_source_to_target.get(&col.name.to_lowercase());
            if let Some(ref req) = required {
                let keep = req.contains(&col.name.to_lowercase())
                    || dict_target
                        .map(|dc| req.contains(&dc.dict_column.to_lowercase()))
                        .unwrap_or(false);
                if !keep {
                    continue;
                }
            }
            let slot_id = builder.alloc_slot();
            // Bug B contract: slot keeps the SOURCE column's storage
            // name (so the lake scan finds the column by name in the
            // tablet schema) and Int32 type (the BE reads it as Utf8 via
            // `build_scan_schema_for_global_dict_encoding` when a
            // TGlobalDict is registered for the slot, then encodes
            // string -> dict id). The dict_column NAME is exposed only
            // in the FE codegen scope below, NOT in the slot descriptor
            // — the BE never sees `__nr_dict_t_s` as a column name.
            let slot_type = match dict_target {
                Some(_) => DataType::Int32,
                None => col.data_type.clone(),
            };
            let nullable = col.nullable;
            builder.desc_builder.add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &slot_type,
                nullable,
                idx as i32,
            );
            slot_to_column.insert(slot_id, col.name.clone());
            physical_slot_by_column.insert(col.name.to_lowercase(), slot_id);
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id,
                data_type: slot_type.clone(),
                type_desc: None,
                nullable,
            };
            // G1: pick up the per-column ColumnId from `op.columns` so the
            // scope's by-id index is populated for base-table reads. This is
            // what lets the optimizer's `DistributionSpec::HashPartitioned`
            // (which is now a `Vec<ColumnId>`) resolve directly against the
            // scan's child scope without having to round-trip through the
            // display name. The dict-renamed `OutputColumn` carries the
            // source column's id so the lookup still hits.
            let col_id = op
                .columns
                .iter()
                .find(|oc| {
                    let lc = oc.name.to_ascii_lowercase();
                    lc == col.name.to_ascii_lowercase()
                        || dict_target
                            .map(|dc| lc == dc.dict_column.to_ascii_lowercase())
                            .unwrap_or(false)
                })
                .map(|oc| oc.column_id)
                .unwrap_or(crate::sql::column_id::ColumnId::UNSET);
            scope.add_column_with_id(
                col_id,
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                binding.clone(),
            );
            // Also register the dict column name in the scope so the
            // post-rewrite `ColumnRef("__nr_dict_t_s")` resolves to this
            // same slot. The scan tuple holds a SINGLE slot for this
            // column; both names refer to it.
            if let Some(dict_col) = dict_target {
                scope.add_column_with_id(
                    col_id,
                    qualifier.map(|s| s.to_string()),
                    dict_col.dict_column.clone(),
                    binding.clone(),
                );
            }
            if let Some(dict_col) = dict_target {
                dict_slot_for_source.insert(col.name.to_ascii_lowercase(), slot_id);
                dict_slot_to_dict.push((slot_id, *dict_col));
            }
        }

        // Iceberg metadata pseudo-columns: register in ExprScope and emit as
        // output slots so SELECT _file/_pos and v3 row-lineage references
        // resolve in codegen and flow through to the HDFS_SCAN_NODE tuple
        // descriptor. Lowering picks up the slot by name to populate
        // IcebergVirtualSpec.
        //
        // Note: these pseudo-columns are NOT in `scan.columns`, so the column
        // pruning rule never adds them to `required_columns`. Always register
        // them regardless of `required`; the lowering layer only synthesises
        // the values for slots that are actually in the tuple descriptor.
        let meta_col_offset = table.columns.len();
        for (meta_idx, col) in table
            .iceberg_row_lineage_metadata_columns
            .iter()
            .enumerate()
        {
            let col_pos = (meta_col_offset + meta_idx) as i32;
            let slot_id = builder.alloc_slot();
            builder.desc_builder.add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                col_pos,
            );
            slot_to_column.insert(slot_id, col.name.clone());
            iceberg_metadata_pseudo_column_slots.insert(slot_id);
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: None,
                nullable: col.nullable,
            };
            let col_id = op
                .columns
                .iter()
                .find(|oc| oc.name.eq_ignore_ascii_case(&col.name))
                .map(|oc| oc.column_id)
                .unwrap_or(crate::sql::column_id::ColumnId::UNSET);
            scope.add_column_with_id(
                col_id,
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                binding,
            );
        }

        let variant_col_offset =
            table.columns.len() + table.iceberg_row_lineage_metadata_columns.len();
        let mut variant_path_columns = Vec::with_capacity(op.variant_columns.len());
        for (variant_idx, variant_column) in op.variant_columns.iter().enumerate() {
            let source_slot_id = physical_slot_by_column
                .get(&variant_column.source_column.to_lowercase())
                .copied()
                .ok_or_else(|| {
                    format!(
                        "scan `{}.{}` variant_columns references unknown source column `{}`",
                        op.database, table.name, variant_column.source_column
                    )
                })?;
            let output_slot_id = builder.alloc_slot();
            let requested_type = type_infer::arrow_type_to_type_desc(
                &variant_column.requested_type,
            )
            .map_err(|err| {
                format!(
                    "scan `{}.{}` variant column `{}` has unsupported requested type {:?}: {err}",
                    op.database,
                    table.name,
                    variant_column.synthetic_column,
                    variant_column.requested_type
                )
            })?;
            let nullable = op
                .columns
                .iter()
                .find(|column| column.column_id == variant_column.synthetic_column_id)
                .map(|column| column.nullable)
                .unwrap_or(true);
            builder.desc_builder.add_slot_with_type_desc(
                output_slot_id,
                scan_tuple_id,
                &variant_column.synthetic_column,
                requested_type.clone(),
                nullable,
                (variant_col_offset + variant_idx) as i32,
            );
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id: output_slot_id,
                data_type: variant_column.requested_type.clone(),
                type_desc: Some(requested_type.clone()),
                nullable,
            };
            scope.add_column_with_id(
                variant_column.synthetic_column_id,
                qualifier.map(|s| s.to_string()),
                variant_column.synthetic_column.clone(),
                binding,
            );
            variant_path_columns.push(plan_nodes::TVariantPathColumn::new(
                Some(source_slot_id),
                Some(output_slot_id),
                Some(variant_column.source_column.clone()),
                Some(variant_column.synthetic_column.clone()),
                Some(variant_column.canonical_path.clone()),
                Some(requested_type),
                Some(variant_column.strict),
            ));
        }

        // Compile predicates pushed down by the optimizer
        let pushed_conjuncts = if op.predicates.is_empty() {
            vec![]
        } else {
            let mut conjuncts = Vec::new();
            for pred in &op.predicates {
                let mut compiler = ExprCompiler::new(builder.slot_allocator(), &scope);
                conjuncts.push(compiler.compile_typed(pred)?);
            }
            conjuncts
        };

        // Dict-encoded scan columns (Task 5/7/8 plan hints). The slot for
        // each dict column was already allocated in the table-column loop
        // above (where its storage `col_pos` is recorded). Here we just
        // build the BE-facing payload: a self-map `dict_slot → dict_slot`
        // for `TLakeScanNode.dict_string_id_to_int_ids` (the BE replaces
        // the dict slot in the scan layout with this id, so the storage
        // reader keeps the same slot) and a TGlobalDict for each. The
        // dict_columns hint is still consulted later to detect a planning
        // bug on non-StarRocks scans. `dict_columns` is empty in all
        // production paths today.
        let mut string_to_dict_slot: BTreeMap<i32, i32> = BTreeMap::new();
        for dict_col in &op.dict_columns {
            let dict_slot_id = dict_slot_for_source
                .get(&dict_col.source_column.to_ascii_lowercase())
                .copied()
                .ok_or_else(|| {
                    format!(
                        "scan `{}.{}` dict_columns references unknown source column `{}`",
                        op.database, table.name, dict_col.source_column
                    )
                })?;
            // Self-map: the BE's `lake_scan.rs` rewrites every dict int
            // slot in the layout to its mapped string slot before issuing
            // the storage read. With the FE-only fix the FE no longer
            // emits a separate string slot — the dict slot itself is the
            // storage slot, declared Int32 in desc_tbl but read as Utf8
            // via the query global dict path (see
            // `build_scan_schema_for_global_dict_encoding`). A self-map
            // keeps the BE's layout swap a no-op while preserving the
            // "dict-encoded" semantics on the FE/wire contract.
            string_to_dict_slot.insert(dict_slot_id, dict_slot_id);
        }

        let resolved = ResolvedTable {
            database: op.database.clone(),
            table: table.clone(),
            planned_scan,
            alias: op.alias.clone(),
        };
        builder.desc_builder.add_tuple(scan_tuple_id, scan_table_id);

        let min_max_predicates =
            nodes::scan_file_min_max_predicates_from_state(&pushed_conjuncts, &slot_to_column);
        let change_op_slot = nodes::planned_change_op_slot_from_state(
            &iceberg_metadata_pseudo_column_slots,
            &slot_to_column,
        );
        let mut scan_plan_node = nodes::build_scan_node(
            builder.connectors,
            scan_node_id,
            scan_tuple_id,
            &resolved,
            pushed_conjuncts.clone(),
            min_max_predicates,
            change_op_slot,
        )?;

        if !variant_path_columns.is_empty() {
            if let Some(hdfs) = scan_plan_node.hdfs_scan_node.as_mut() {
                hdfs.variant_path_columns = Some(variant_path_columns);
            } else {
                return Err(format!(
                    "scan `{}.{}` has variant_columns but is not an iceberg/HDFS scan",
                    op.database, table.name
                ));
            }
        }

        // StarRocks lake scans carry the dict slot self-map on the wire via
        // `TLakeScanNode.dict_string_id_to_int_ids`. Iceberg/HDFS scans have no
        // such thrift field and don't need one: the dict slot is already an
        // Int32 storage slot, and the per-fragment `TGlobalDict` payloads
        // emitted below feed `lower_hdfs_scan_node`'s encode map directly
        // (the parquet reader reads Utf8 and encodes to dict ids). So for an
        // iceberg `hdfs_scan_node` we leave the thrift node untouched. Any
        // other scan kind receiving dict_columns is a planning bug.
        if !string_to_dict_slot.is_empty() {
            if let Some(lake) = scan_plan_node.lake_scan_node.as_mut() {
                lake.dict_string_id_to_int_ids = Some(string_to_dict_slot);
            } else if scan_plan_node.hdfs_scan_node.is_some() {
                // iceberg/HDFS: dicts flow via query_global_dicts in lowering.
            } else {
                return Err(format!(
                    "scan `{}.{}` has dict_columns but is neither a StarRocks lake scan nor an iceberg/HDFS scan",
                    op.database, op.table.name,
                ));
            }
        }

        // Emit per-dict-column TGlobalDict payloads onto EVERY fragment in
        // the current stack (the leaf scan's fragment plus every parent
        // fragment that consumes its output through an exchange). The
        // dict slot id is consistent across fragments, so a Decode
        // operator inserted above the exchange — which lives in a parent
        // fragment — must also receive the TGlobalDict via its own
        // fragment's `query_global_dicts`. Without this, the BE's
        // `lower_decode_node` fails with `missing query global dict for
        // encoded slot_id=<N>` (each fragment builds its own
        // QueryGlobalDictMap from its own TGlobalDict list).
        let current_frag = builder.current_fragment_id()?;
        let dict_fragments: Vec<FragmentId> = if builder.fragment_stack.is_empty() {
            vec![current_frag]
        } else {
            builder.fragment_stack.clone()
        };
        for (dict_slot_id, dict_col) in &dict_slot_to_dict {
            let snapshot = dict_col.dictionary.as_ref();
            let mut ids = Vec::with_capacity(snapshot.values.len());
            let mut strings = Vec::with_capacity(snapshot.values.len());
            for value in &snapshot.values {
                ids.push(value.id);
                strings.push(value.bytes.clone());
            }
            let global_dict = crate::data::TGlobalDict::new(
                Some(*dict_slot_id),
                Some(strings),
                Some(ids),
                Some(snapshot.version),
            );
            for fragment_id in &dict_fragments {
                builder
                    .query_global_dicts_per_fragment
                    .entry(*fragment_id)
                    .or_default()
                    .push(global_dict.clone());
            }
            // Track the slot -> dict association so any operator that
            // allocates a new slot inheriting this slot's values
            // (Aggregate group-by, Project column ref, etc.) can
            // re-register the dict on the new slot id. The downstream
            // `Decode` resolves by NAME against the new slot, then the
            // BE's `lower_decode_node` needs a TGlobalDict keyed by that
            // new slot id — registered via `propagate_dict_to_slot`.
            builder
                .slot_to_global_dict
                .insert(*dict_slot_id, global_dict);
        }

        builder.scan_tables.push(nodes::PlannedScanTable {
            scan_node_id,
            scan_tuple_id,
            resolved,
            min_max_conjuncts: pushed_conjuncts,
            slot_to_column,
            iceberg_metadata_pseudo_column_slots,
        });

        Ok((scan_plan_node, scope))
    }

    pub(crate) fn lower_project(
        &mut self,
        project_node_id: i32,
        project_tuple_id: i32,
        op: &PhysicalProjectOp,
        child_scope: &ExprScope,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope, Vec<OutputColumn>), String> {
        let builder = &mut *self.builder;
        let mut output_columns = Vec::new();
        let mut slot_map = BTreeMap::new();
        let mut project_scope = ExprScope::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(builder.slot_allocator(), child_scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            let data_type = item.expr.data_type.clone();
            let nullable = item.expr.nullable;
            let name = item.output_name.clone();
            let slot_id = builder.alloc_slot();
            let slot_type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("project expr `{name}` compiled to empty TExpr"))?;
            builder.desc_builder.add_slot_with_type_desc(
                slot_id,
                project_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                output_columns.len() as i32,
            );
            slot_map.insert(slot_id, texpr);
            output_columns.push(OutputColumn {
                name: name.clone(),
                data_type: data_type.clone(),
                nullable,
            });

            let binding = ColumnBinding {
                tuple_id: project_tuple_id,
                slot_id,
                data_type: data_type.clone(),
                type_desc: Some(slot_type_desc.clone()),
                nullable,
            };
            project_scope.add_column_with_id(
                item.output_column_id,
                op.output_qualifier.clone(),
                name.clone(),
                binding.clone(),
            );

            let unqualified_display = typed_expr_display_name_without_qualifiers(&item.expr);
            if !unqualified_display.eq_ignore_ascii_case(&name) {
                let _ = unqualified_display;
            }

            // Propagate the dict registration on a ColumnRef passthrough:
            // the new slot inherits the source slot's dict, so a parent
            // fragment's Decode (post-exchange) finds the matching dict
            // in its own `query_global_dicts`.
            if let ExprKind::ColumnRef { column_id, .. } = item.expr.kind
                && let Some(child_binding) = child_scope.resolve_by_id(column_id)
            {
                let source_slot_id = child_binding.slot_id;
                builder.propagate_dict_to_slot(source_slot_id, slot_id);
            }
        }

        builder.desc_builder.add_tuple(project_tuple_id, None);
        let project_plan_node =
            nodes::build_project_node(project_node_id, project_tuple_id, slot_map);

        Ok((project_plan_node, project_scope, output_columns))
    }
}
