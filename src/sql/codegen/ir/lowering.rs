use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use arrow::datatypes::DataType;

use crate::exprs;
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::plan_nodes;
use crate::sql::analysis::{ExprKind, OutputColumn as AnalysisOutputColumn};
use crate::sql::catalog::{CatalogProvider, ScanSource, TableDef};
use crate::sql::codegen::descriptors::DescriptorTableBuilder;
use crate::sql::codegen::expr_compiler::{self, ExprCompiler};
use crate::sql::codegen::fragment_builder::{
    PlanFragmentBuilder, add_iceberg_equality_delete_required_columns, build_result_sink,
    effective_iceberg_scan_column_names, iceberg_scan_table_handle_for_codegen, iceberg_table_info,
    output_columns_for_boundary, result_root_boundary_schema_report, synthetic_iceberg_table_id,
};
use crate::sql::codegen::helpers::{
    agg_call_display_name, agg_call_display_name_without_qualifiers, typed_expr_display_name,
    typed_expr_display_name_without_qualifiers,
};
use crate::sql::codegen::nodes;
use crate::sql::codegen::resolve::{ColumnBinding, ExprScope, ResolvedTable};
use crate::sql::codegen::type_infer;
use crate::sql::codegen::{
    FragmentBuildResult, FragmentId, MultiFragmentBuildResult, OutputColumn,
};
use crate::sql::optimizer::operator::{
    AggMode, PhysicalHashAggregateOp, PhysicalProjectOp, PhysicalScanOp, PhysicalSortOp,
    ScanDictionaryColumn,
};
use crate::types;

pub(crate) fn lower_distributed_plan(
    dp: &super::fragment::DistributedPlan,
    catalog: &dyn CatalogProvider,
    connectors: &crate::connector::ConnectorRegistry,
) -> Result<MultiFragmentBuildResult, String> {
    let _ = catalog;
    let root_fragment = validate_m0_root_fragment(dp)?;

    let mut state = OwnedLoweringState::new(connectors, None, dp.root_fragment_id);
    let lowered = {
        let mut ctx = LoweringCtx::new(&mut state);
        ctx.lower_node(&root_fragment.root)?
    };

    let desc_tbl =
        std::mem::replace(&mut state.desc_builder, DescriptorTableBuilder::new()).build();
    let exec_params =
        nodes::build_exec_params_multi_with_refresh_context(connectors, &state.scan_tables, None)?;
    let output_exprs =
        result_output_exprs_for_columns(&lowered.scope, &root_fragment.output_columns)?;
    let output_columns = output_columns_for_boundary(&root_fragment.output_columns);
    let root_node_id = lowered
        .plan_nodes
        .first()
        .map(|node| node.node_id)
        .unwrap_or(-1);
    let boundary_schemas = vec![result_root_boundary_schema_report(
        dp.root_fragment_id,
        root_node_id,
        &output_columns,
    )];
    let root_dicts = state
        .query_global_dicts_per_fragment
        .remove(&dp.root_fragment_id)
        .filter(|dicts| !dicts.is_empty());

    let root_fragment = FragmentBuildResult {
        fragment_id: dp.root_fragment_id,
        plan: plan_nodes::TPlan::new(lowered.plan_nodes),
        desc_tbl: desc_tbl.clone(),
        exec_params: exec_params.clone(),
        output_sink: build_result_sink(),
        output_exprs,
        output_columns,
        direct_exec: None,
        boundary_schemas: boundary_schemas.clone(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
        query_global_dicts: root_dicts,
        query_global_dict_exprs: None,
    };

    Ok(MultiFragmentBuildResult {
        fragment_results: vec![root_fragment],
        root_fragment_id: dp.root_fragment_id,
        edges: Vec::new(),
        boundary_schemas,
        rf_plan: None,
    })
}

fn validate_m0_root_fragment(
    dp: &super::fragment::DistributedPlan,
) -> Result<&super::fragment::PlanFragment, String> {
    if dp.fragments.len() != 1 {
        return Err(format!(
            "lower_distributed_plan M0 supports exactly one fragment, got {}",
            dp.fragments.len()
        ));
    }

    let fragment = &dp.fragments[0];
    if fragment.fragment_id != dp.root_fragment_id {
        return Err(format!(
            "lower_distributed_plan M0 root fragment id={} does not match only fragment id={}",
            dp.root_fragment_id, fragment.fragment_id
        ));
    }
    if !matches!(fragment.sink, super::fragment::DataSink::Result) {
        return Err("lower_distributed_plan M0 supports only result sink".to_string());
    }
    ensure_unpartitioned("data_partition", &fragment.data_partition)?;
    ensure_unpartitioned("output_partition", &fragment.output_partition)?;
    if fragment.output_exprs.is_some() {
        return Err("lower_distributed_plan M0 does not support fragment output_exprs".to_string());
    }

    Ok(fragment)
}

fn ensure_unpartitioned(
    label: &str,
    partition: &super::fragment::DataPartition,
) -> Result<(), String> {
    if !matches!(
        partition.kind,
        super::fragment::PartitionKind::Unpartitioned
    ) || !partition.exprs.is_empty()
    {
        return Err(format!(
            "lower_distributed_plan M0 supports only unpartitioned {label}"
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub(in crate::sql::codegen) struct AggregateSlotContract {
    pub(in crate::sql::codegen) data_type: DataType,
    pub(in crate::sql::codegen) type_desc: types::TTypeDesc,
}

pub(in crate::sql::codegen) fn aggregate_slot_contract_for_phase(
    need_finalize: bool,
    result_type: &DataType,
    intermediate_type: Option<&DataType>,
    display_name: &str,
) -> Result<AggregateSlotContract, String> {
    let data_type = if need_finalize {
        result_type.clone()
    } else {
        intermediate_type
            .cloned()
            .unwrap_or_else(|| result_type.clone())
    };
    let type_desc = type_infer::arrow_type_to_type_desc(&data_type)
        .map_err(|e| format!("aggregate `{display_name}` output type descriptor failed: {e}"))?;
    Ok(AggregateSlotContract {
        data_type,
        type_desc,
    })
}

pub(in crate::sql::codegen) trait LoweringStateAccess<'a> {
    fn connectors(&self) -> &'a crate::connector::ConnectorRegistry;
    fn mv_refresh_ctx(
        &self,
    ) -> Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext>;
    fn desc_builder(&mut self) -> &mut DescriptorTableBuilder;
    fn scan_tables(&mut self) -> &mut Vec<nodes::PlannedScanTable>;
    fn fragment_stack(&self) -> &[FragmentId];
    fn query_global_dicts_per_fragment(
        &mut self,
    ) -> &mut HashMap<FragmentId, Vec<crate::data::TGlobalDict>>;
    fn slot_to_global_dict(&self) -> &HashMap<i32, crate::data::TGlobalDict>;
    fn slot_to_global_dict_mut(&mut self) -> &mut HashMap<i32, crate::data::TGlobalDict>;
    fn alloc_slot(&mut self) -> i32;
    fn slot_allocator(&self) -> expr_compiler::SlotAllocator;

    fn current_fragment_id(&self) -> Result<FragmentId, String> {
        self.fragment_stack()
            .last()
            .copied()
            .ok_or_else(|| "no active fragment id in lowering state".to_string())
    }

    fn refresh_scan_table_for_codegen(&self, table: &TableDef) -> Result<TableDef, String> {
        refresh_scan_table_for_codegen(self.mv_refresh_ctx(), table)
    }

    fn propagate_dict_to_slot(&mut self, source_slot_id: i32, new_slot_id: i32) {
        if source_slot_id == new_slot_id {
            return;
        }
        let Some(source_dict) = self.slot_to_global_dict().get(&source_slot_id).cloned() else {
            return;
        };
        let new_dict = crate::data::TGlobalDict::new(
            Some(new_slot_id),
            source_dict.strings.clone(),
            source_dict.ids.clone(),
            source_dict.version,
        );
        let fragments: Vec<FragmentId> = if self.fragment_stack().is_empty() {
            self.current_fragment_id()
                .ok()
                .map(|fragment_id| vec![fragment_id])
                .unwrap_or_default()
        } else {
            self.fragment_stack().to_vec()
        };
        for fragment_id in fragments {
            self.query_global_dicts_per_fragment()
                .entry(fragment_id)
                .or_default()
                .push(new_dict.clone());
        }
        self.slot_to_global_dict_mut().insert(new_slot_id, new_dict);
    }
}

impl<'a> LoweringStateAccess<'a> for PlanFragmentBuilder<'a> {
    fn connectors(&self) -> &'a crate::connector::ConnectorRegistry {
        self.connectors
    }

    fn mv_refresh_ctx(
        &self,
    ) -> Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext> {
        self.mv_refresh_ctx
    }

    fn desc_builder(&mut self) -> &mut DescriptorTableBuilder {
        &mut self.desc_builder
    }

    fn scan_tables(&mut self) -> &mut Vec<nodes::PlannedScanTable> {
        &mut self.scan_tables
    }

    fn fragment_stack(&self) -> &[FragmentId] {
        &self.fragment_stack
    }

    fn query_global_dicts_per_fragment(
        &mut self,
    ) -> &mut HashMap<FragmentId, Vec<crate::data::TGlobalDict>> {
        &mut self.query_global_dicts_per_fragment
    }

    fn slot_to_global_dict(&self) -> &HashMap<i32, crate::data::TGlobalDict> {
        &self.slot_to_global_dict
    }

    fn slot_to_global_dict_mut(&mut self) -> &mut HashMap<i32, crate::data::TGlobalDict> {
        &mut self.slot_to_global_dict
    }

    fn alloc_slot(&mut self) -> i32 {
        PlanFragmentBuilder::alloc_slot(self)
    }

    fn slot_allocator(&self) -> expr_compiler::SlotAllocator {
        PlanFragmentBuilder::slot_allocator(self)
    }

    fn current_fragment_id(&self) -> Result<FragmentId, String> {
        PlanFragmentBuilder::current_fragment_id(self)
    }

    fn refresh_scan_table_for_codegen(&self, table: &TableDef) -> Result<TableDef, String> {
        PlanFragmentBuilder::refresh_scan_table_for_codegen(self, table)
    }

    fn propagate_dict_to_slot(&mut self, source_slot_id: i32, new_slot_id: i32) {
        PlanFragmentBuilder::propagate_dict_to_slot(self, source_slot_id, new_slot_id)
    }
}

pub(crate) struct OwnedLoweringState<'a> {
    connectors: &'a crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    desc_builder: DescriptorTableBuilder,
    scan_tables: Vec<nodes::PlannedScanTable>,
    next_slot_id: Rc<RefCell<i32>>,
    fragment_stack: Vec<FragmentId>,
    query_global_dicts_per_fragment: HashMap<FragmentId, Vec<crate::data::TGlobalDict>>,
    slot_to_global_dict: HashMap<i32, crate::data::TGlobalDict>,
}

impl<'a> OwnedLoweringState<'a> {
    fn new(
        connectors: &'a crate::connector::ConnectorRegistry,
        mv_refresh_ctx: Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
        root_fragment_id: FragmentId,
    ) -> Self {
        Self {
            connectors,
            mv_refresh_ctx,
            desc_builder: DescriptorTableBuilder::new(),
            scan_tables: Vec::new(),
            next_slot_id: Rc::new(RefCell::new(1)),
            fragment_stack: vec![root_fragment_id],
            query_global_dicts_per_fragment: HashMap::new(),
            slot_to_global_dict: HashMap::new(),
        }
    }
}

impl<'a> LoweringStateAccess<'a> for OwnedLoweringState<'a> {
    fn connectors(&self) -> &'a crate::connector::ConnectorRegistry {
        self.connectors
    }

    fn mv_refresh_ctx(
        &self,
    ) -> Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext> {
        self.mv_refresh_ctx
    }

    fn desc_builder(&mut self) -> &mut DescriptorTableBuilder {
        &mut self.desc_builder
    }

    fn scan_tables(&mut self) -> &mut Vec<nodes::PlannedScanTable> {
        &mut self.scan_tables
    }

    fn fragment_stack(&self) -> &[FragmentId] {
        &self.fragment_stack
    }

    fn query_global_dicts_per_fragment(
        &mut self,
    ) -> &mut HashMap<FragmentId, Vec<crate::data::TGlobalDict>> {
        &mut self.query_global_dicts_per_fragment
    }

    fn slot_to_global_dict(&self) -> &HashMap<i32, crate::data::TGlobalDict> {
        &self.slot_to_global_dict
    }

    fn slot_to_global_dict_mut(&mut self) -> &mut HashMap<i32, crate::data::TGlobalDict> {
        &mut self.slot_to_global_dict
    }

    fn alloc_slot(&mut self) -> i32 {
        let mut next = self.next_slot_id.borrow_mut();
        let slot_id = *next;
        *next += 1;
        slot_id
    }

    fn slot_allocator(&self) -> expr_compiler::SlotAllocator {
        Rc::clone(&self.next_slot_id)
    }
}

pub(in crate::sql::codegen) struct LoweringCtx<'s, 'a, S: LoweringStateAccess<'a> + ?Sized> {
    state: &'s mut S,
    _marker: std::marker::PhantomData<&'a ()>,
}

struct LoweredDistributedNode {
    plan_nodes: Vec<plan_nodes::TPlanNode>,
    scope: ExprScope,
    #[allow(dead_code)]
    output_columns: Vec<AnalysisOutputColumn>,
}

impl<'s, 'a, S: LoweringStateAccess<'a> + ?Sized> LoweringCtx<'s, 'a, S> {
    pub(crate) fn new(state: &'s mut S) -> Self {
        Self {
            state,
            _marker: std::marker::PhantomData,
        }
    }

    fn lower_node(
        &mut self,
        node: &super::node::DistributedPlanNode,
    ) -> Result<LoweredDistributedNode, String> {
        match &node.body {
            super::node::DistributedPlanNodeBody::Scan(scan) => {
                if !node.children.is_empty() {
                    return Err(format!(
                        "DistributedPlan Scan node_id={} expected 0 children, got {}",
                        node.node_id,
                        node.children.len()
                    ));
                }
                let scan_tuple_id = first_tuple_id(node, "Scan")?;
                let op = scan_body_to_physical_op(scan);
                let (scan_plan_node, scope) = self.lower_scan(node.node_id, scan_tuple_id, &op)?;
                Ok(LoweredDistributedNode {
                    plan_nodes: vec![scan_plan_node],
                    scope,
                    output_columns: op.columns.clone(),
                })
            }
            super::node::DistributedPlanNodeBody::Project(project) => {
                if node.children.len() != 1 {
                    return Err(format!(
                        "DistributedPlan Project node_id={} expected 1 child, got {}",
                        node.node_id,
                        node.children.len()
                    ));
                }
                let child = self.lower_node(&node.children[0])?;
                let project_tuple_id = first_tuple_id(node, "Project")?;
                let op = project_body_to_physical_op(project);
                let (project_plan_node, scope, _output_columns) =
                    self.lower_project(node.node_id, project_tuple_id, &op, &child.scope)?;
                let mut plan_nodes = vec![project_plan_node];
                plan_nodes.extend(child.plan_nodes);
                Ok(LoweredDistributedNode {
                    plan_nodes,
                    scope,
                    output_columns: project_body_output_columns(project),
                })
            }
            super::node::DistributedPlanNodeBody::Sort(sort) => {
                if node.children.len() != 1 {
                    return Err(format!(
                        "DistributedPlan Sort node_id={} expected 1 child, got {}",
                        node.node_id,
                        node.children.len()
                    ));
                }
                let child = self.lower_node(&node.children[0])?;
                let op = sort_body_to_physical_op(sort);
                let sort_plan_node = self.lower_sort(
                    node.node_id,
                    &op,
                    &child.scope,
                    &node.children[0].tuple_ids,
                    &sort.output_columns,
                    sort.offset,
                )?;
                let mut plan_nodes = vec![sort_plan_node];
                plan_nodes.extend(child.plan_nodes);
                Ok(LoweredDistributedNode {
                    plan_nodes,
                    scope: child.scope,
                    output_columns: sort.output_columns.clone(),
                })
            }
            super::node::DistributedPlanNodeBody::HashAggregate(agg) => {
                if node.children.len() != 1 {
                    return Err(format!(
                        "DistributedPlan HashAggregate node_id={} expected 1 child, got {}",
                        node.node_id,
                        node.children.len()
                    ));
                }
                let child = self.lower_node(&node.children[0])?;
                let agg_tuple_id = first_tuple_id(node, "HashAggregate")?;
                let op = hash_aggregate_body_to_physical_op(agg);
                let (agg_plan_node, scope) =
                    self.lower_hash_aggregate(node.node_id, agg_tuple_id, &op, &child.scope)?;
                let mut plan_nodes = vec![agg_plan_node];
                plan_nodes.extend(child.plan_nodes);
                Ok(LoweredDistributedNode {
                    plan_nodes,
                    scope,
                    output_columns: agg.output_columns.clone(),
                })
            }
        }
    }

    pub(crate) fn lower_scan(
        &mut self,
        scan_node_id: i32,
        scan_tuple_id: i32,
        op: &PhysicalScanOp,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope), String> {
        let state = &mut *self.state;
        let table = state.refresh_scan_table_for_codegen(&op.table)?;

        let mut scope = ExprScope::new();
        let qualifier = op.alias.as_deref().or(Some(&table.name));
        let mut slot_to_column = HashMap::new();
        let mut iceberg_metadata_pseudo_column_slots = BTreeSet::new();

        // Determine which columns to emit
        let planned_scan = match &table.source {
            crate::sql::catalog::ScanSource::StarRocks { db_id, table_id } => {
                let planner = state.connectors().scan_planner("starrocks")?;
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
                let planner = state.connectors().scan_planner("iceberg")?;
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
            state
                .desc_builder()
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
            let slot_id = state.alloc_slot();
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
            state.desc_builder().add_slot(
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
            let slot_id = state.alloc_slot();
            state.desc_builder().add_slot(
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
            let output_slot_id = state.alloc_slot();
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
            state.desc_builder().add_slot_with_type_desc(
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
                let mut compiler = ExprCompiler::new(state.slot_allocator(), &scope);
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
        state.desc_builder().add_tuple(scan_tuple_id, scan_table_id);

        let min_max_predicates =
            nodes::scan_file_min_max_predicates_from_state(&pushed_conjuncts, &slot_to_column);
        let change_op_slot = nodes::planned_change_op_slot_from_state(
            &iceberg_metadata_pseudo_column_slots,
            &slot_to_column,
        );
        let mut scan_plan_node = nodes::build_scan_node(
            state.connectors(),
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
        let current_frag = state.current_fragment_id()?;
        let dict_fragments: Vec<FragmentId> = if state.fragment_stack().is_empty() {
            vec![current_frag]
        } else {
            state.fragment_stack().to_vec()
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
                state
                    .query_global_dicts_per_fragment()
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
            state
                .slot_to_global_dict_mut()
                .insert(*dict_slot_id, global_dict);
        }

        state.scan_tables().push(nodes::PlannedScanTable {
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
        let state = &mut *self.state;
        let mut output_columns = Vec::new();
        let mut slot_map = BTreeMap::new();
        let mut project_scope = ExprScope::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            let data_type = item.expr.data_type.clone();
            let nullable = item.expr.nullable;
            let name = item.output_name.clone();
            let slot_id = state.alloc_slot();
            let slot_type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("project expr `{name}` compiled to empty TExpr"))?;
            state.desc_builder().add_slot_with_type_desc(
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
                state.propagate_dict_to_slot(source_slot_id, slot_id);
            }
        }

        state.desc_builder().add_tuple(project_tuple_id, None);
        let project_plan_node =
            nodes::build_project_node(project_node_id, project_tuple_id, slot_map);

        Ok((project_plan_node, project_scope, output_columns))
    }

    pub(crate) fn lower_hash_aggregate(
        &mut self,
        agg_node_id: i32,
        agg_tuple_id: i32,
        op: &PhysicalHashAggregateOp,
        child_scope: &ExprScope,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope), String> {
        let state = &mut *self.state;
        let need_finalize = matches!(op.mode, AggMode::Single | AggMode::Global);

        let mut agg_scope = ExprScope::new();
        let mut grouping_exprs = Vec::new();

        // Compile GROUP BY expressions (same for all modes — the child scope
        // has the correct columns for both scan-level and Local-output contexts).
        for (idx, gb_expr) in op.group_by.iter().enumerate() {
            let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
            let texpr = compiler.compile_typed(gb_expr)?;
            let data_type = gb_expr.data_type.clone();
            let nullable = gb_expr.nullable;
            let name = typed_expr_display_name(gb_expr);
            let slot_id = state.alloc_slot();
            let slot_type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("group by expr `{name}` compiled to empty TExpr"))?;
            state.desc_builder().add_slot_with_type_desc(
                slot_id,
                agg_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: agg_tuple_id,
                slot_id,
                data_type: data_type.clone(),
                type_desc: Some(slot_type_desc),
                nullable,
            };
            let gb_column_id = op
                .output_columns
                .get(idx)
                .map(|col| col.column_id)
                .unwrap_or_else(|| match &gb_expr.kind {
                    ExprKind::ColumnRef { column_id, .. } => *column_id,
                    _ => crate::sql::column_id::ColumnId::UNSET,
                });
            agg_scope.add_column_with_id(gb_column_id, None, name, binding.clone());
            if let ExprKind::ColumnRef {
                qualifier: Some(ref q),
                ref column,
                ..
            } = gb_expr.kind
            {
                let _ = (q, column, binding);
            }
            // Propagate dict registration through the aggregate's group-
            // by output: when the group-by is a passthrough ColumnRef of
            // a dict-encoded source slot, the new agg output slot also
            // carries dict ids. Re-register the TGlobalDict on the new
            // slot so a downstream Decode (in this or a parent fragment
            // post-exchange) resolves its `dict_id_to_string_ids` key.
            if let ExprKind::ColumnRef { column_id, .. } = gb_expr.kind
                && let Some(child_binding) = child_scope.resolve_by_id(column_id)
            {
                let source_slot_id = child_binding.slot_id;
                state.propagate_dict_to_slot(source_slot_id, slot_id);
            }
            grouping_exprs.push(texpr);
        }

        // Compile aggregate function expressions — mode-dependent.
        let agg_start_col = op.group_by.len();
        let mut aggregate_functions = Vec::new();

        debug_assert_eq!(
            op.is_merge.len(),
            op.aggregates.len(),
            "PhysicalHashAggregate (node_id={}): is_merge.len() = {}, aggregates.len() = {}",
            agg_node_id,
            op.is_merge.len(),
            op.aggregates.len(),
        );

        for (idx, agg_call) in op.aggregates.iter().enumerate() {
            let texpr = if op.is_merge[idx] {
                // Global (merge) phase: the child scope contains the Local's
                // output.  Each intermediate aggregate column sits at position
                // group_by.len() + idx in the child scope's ordered columns.
                let child_columns: Vec<_> = child_scope.iter_columns().collect();
                let child_col_idx = agg_start_col + idx;
                let (_, binding) = child_columns.get(child_col_idx).ok_or_else(|| {
                    format!(
                        "Global agg: child scope missing intermediate column at index {}",
                        child_col_idx
                    )
                })?;
                let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
                compiler.compile_merge_aggregate_call(
                    agg_call,
                    binding.slot_id,
                    binding.tuple_id,
                    &binding.data_type,
                )?
            } else {
                // Single or Local: compile against child scope normally.
                let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
                compiler.compile_aggregate_call_typed(agg_call).map_err(|err| {
                    let available = child_scope
                        .iter_columns()
                        .map(|(name, _)| name.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(
                        "failed to compile aggregate `{}` in {:?} mode against child scope [{}]: {}",
                        agg_call_display_name(agg_call),
                        op.mode,
                        available,
                        err
                    )
                })?
            };

            let nullable = true;
            let name = agg_call_display_name(agg_call);
            let intermediate_type = texpr
                .nodes
                .first()
                .and_then(|root| root.fn_.as_ref())
                .and_then(|func| func.aggregate_fn.as_ref())
                .and_then(|agg_fn| arrow_type_from_desc(&agg_fn.intermediate_type));
            let slot_contract = aggregate_slot_contract_for_phase(
                need_finalize,
                &agg_call.result_type,
                intermediate_type.as_ref(),
                &name,
            )?;
            let data_type = slot_contract.data_type.clone();
            let slot_type_desc = slot_contract.type_desc.clone();
            let slot_id = state.alloc_slot();
            let col_pos = (agg_start_col + idx) as i32;
            state.desc_builder().add_slot_with_type_desc(
                slot_id,
                agg_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                col_pos,
            );
            let binding = ColumnBinding {
                tuple_id: agg_tuple_id,
                slot_id,
                data_type,
                type_desc: Some(slot_type_desc),
                nullable,
            };
            agg_scope.add_column_with_id(
                agg_call.output_column_id,
                None,
                name.clone(),
                binding.clone(),
            );
            let unqualified_name = agg_call_display_name_without_qualifiers(agg_call);
            if !unqualified_name.eq_ignore_ascii_case(&name) {
                let _ = unqualified_name;
            }
            aggregate_functions.push(texpr);
        }

        state.desc_builder().add_tuple(agg_tuple_id, None);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            aggregate_functions,
            need_finalize,
        );

        Ok((agg_plan_node, agg_scope))
    }

    pub(crate) fn lower_sort(
        &mut self,
        sort_node_id: i32,
        op: &PhysicalSortOp,
        child_scope: &ExprScope,
        child_tuple_ids: &[i32],
        output_columns: &[AnalysisOutputColumn],
        offset: Option<i64>,
    ) -> Result<plan_nodes::TPlanNode, String> {
        let state = &mut *self.state;

        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            ordering_exprs.push(texpr);
            is_asc.push(item.asc);
            nulls_first_list.push(item.nulls_first);
        }

        // Compile analytic-partition exprs (set when this Sort precedes a
        // Window). Emitting them as TSortNode.analytic_partition_exprs tells
        // the pipeline engine to run sort locally per partition instead of
        // doing a global merge — matching StarRocks's parallel analytic
        // sort behaviour. Empty for plain ORDER BY.
        let analytic_partition_exprs = if op.analytic_partition_exprs.is_empty() {
            None
        } else {
            let mut out = Vec::with_capacity(op.analytic_partition_exprs.len());
            for expr in &op.analytic_partition_exprs {
                let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
                out.push(compiler.compile_typed(expr)?);
            }
            Some(out)
        };

        let sort_info = plan_nodes::TSortInfo::new(
            ordering_exprs,
            is_asc,
            nulls_first_list,
            slot_ref_exprs_for_columns(child_scope, output_columns, "Sort")?,
        );
        let sort_tuple_slot_exprs = sort_info.sort_tuple_slot_exprs.clone();

        let mut sort_plan_node = nodes::default_plan_node();
        sort_plan_node.node_id = sort_node_id;
        sort_plan_node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
        sort_plan_node.num_children = 1;
        sort_plan_node.limit = -1;
        sort_plan_node.row_tuples = child_tuple_ids.to_vec();
        sort_plan_node.nullable_tuples = vec![];
        sort_plan_node.compact_data = true;
        sort_plan_node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: false,
            offset,
            ordering_exprs: None,
            is_asc_order: None,
            is_default_limit: None,
            nulls_first: None,
            sort_tuple_slot_exprs,
            has_outer_join_child: None,
            sql_sort_keys: None,
            analytic_partition_exprs,
            partition_exprs: None,
            partition_limit: None,
            topn_type: None,
            build_runtime_filters: None,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            late_materialization: None,
            enable_parallel_merge: None,
            analytic_partition_skewed: None,
            pre_agg_exprs: None,
            pre_agg_output_slot_id: None,
            pre_agg_insert_local_shuffle: None,
            parallel_merge_late_materialize_mode: None,
            per_pipeline: None,
        });

        Ok(sort_plan_node)
    }
}

fn first_tuple_id(
    node: &super::node::DistributedPlanNode,
    operator_name: &str,
) -> Result<i32, String> {
    node.tuple_ids.first().copied().ok_or_else(|| {
        format!(
            "DistributedPlan {operator_name} node_id={} has no output tuple id",
            node.node_id
        )
    })
}

fn scan_body_to_physical_op(body: &super::body::ScanBody) -> PhysicalScanOp {
    PhysicalScanOp {
        database: body.database.clone(),
        table: body.table.clone(),
        alias: body.alias.clone(),
        columns: body.columns.clone(),
        predicates: body.predicates.clone(),
        required_columns: body.required_columns.clone(),
        dict_columns: body.dict_columns.clone(),
        variant_columns: body.variant_columns.clone(),
        mv_rewritten_from: body.mv_rewritten_from.clone(),
    }
}

fn project_body_to_physical_op(body: &super::body::ProjectBody) -> PhysicalProjectOp {
    PhysicalProjectOp {
        items: body.items.clone(),
        output_qualifier: body.output_qualifier.clone(),
    }
}

fn sort_body_to_physical_op(body: &super::body::SortBody) -> PhysicalSortOp {
    PhysicalSortOp {
        items: body.items.clone(),
        analytic_partition_exprs: body.analytic_partition_exprs.clone(),
    }
}

fn hash_aggregate_body_to_physical_op(
    body: &super::body::HashAggregateBody,
) -> PhysicalHashAggregateOp {
    PhysicalHashAggregateOp {
        mode: body.mode,
        group_by: body.group_by.clone(),
        aggregates: body.aggregates.clone(),
        output_columns: body.output_columns.clone(),
        is_merge: body.is_merge.clone(),
    }
}

fn project_body_output_columns(body: &super::body::ProjectBody) -> Vec<AnalysisOutputColumn> {
    body.items
        .iter()
        .map(|item| AnalysisOutputColumn {
            column_id: item.output_column_id,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        })
        .collect()
}

fn result_output_exprs_for_columns(
    scope: &ExprScope,
    output_columns: &[AnalysisOutputColumn],
) -> Result<Option<Vec<exprs::TExpr>>, String> {
    slot_ref_exprs_for_columns(scope, output_columns, "result sink")
}

pub(in crate::sql::codegen) fn slot_ref_exprs_for_columns(
    scope: &ExprScope,
    output_columns: &[AnalysisOutputColumn],
    context: &str,
) -> Result<Option<Vec<exprs::TExpr>>, String> {
    if output_columns.is_empty() {
        return Ok(None);
    }

    let mut exprs = Vec::with_capacity(output_columns.len());
    for column in output_columns {
        let binding = scope.resolve_by_id(column.column_id).ok_or_else(|| {
            format!(
                "{} cannot resolve output column `{}` id={}",
                context, column.name, column.column_id.0
            )
        })?;
        let type_desc = expr_compiler::binding_type_desc(binding)?;
        exprs.push(expr_compiler::build_slot_ref_texpr(
            binding.slot_id,
            binding.tuple_id,
            type_desc,
        ));
    }
    Ok(Some(exprs))
}

fn refresh_scan_table_for_codegen(
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    table: &TableDef,
) -> Result<TableDef, String> {
    match &table.source {
        ScanSource::IcebergVersionTable {
            table: iceberg_table,
            snapshot_id,
        } => {
            let refresh_ctx = mv_refresh_ctx
                .ok_or_else(|| "Iceberg version scan requires MV refresh context".to_string())?;
            let mut out = table.clone();
            out.source = refresh_ctx.version_scan_source(iceberg_table, *snapshot_id)?;
            Ok(out)
        }
        ScanSource::IcebergMvTargetState(scan) => {
            let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "Iceberg target-state scan requires MV refresh context".to_string()
            })?;
            let mut out = table.clone();
            let projected = nodes::projected_target_state_column_names(scan);
            out.columns.retain(|column| {
                projected
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&column.name))
            });
            out.iceberg_row_lineage_metadata_columns.retain(|column| {
                projected
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&column.name))
            });
            if projected
                .iter()
                .any(|name| name.eq_ignore_ascii_case("_row_id"))
                && !out
                    .columns
                    .iter()
                    .chain(out.iceberg_row_lineage_metadata_columns.iter())
                    .any(|column| column.name.eq_ignore_ascii_case("_row_id"))
            {
                out.iceberg_row_lineage_metadata_columns
                    .push(crate::sql::catalog::ColumnDef {
                        name: "_row_id".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    });
            }
            out.source = refresh_ctx.target_state_scan_source(scan)?;
            nodes::reject_target_state_equality_deletes(&out.source)?;
            Ok(out)
        }
        _ => Ok(table.clone()),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::connector::ConnectorRegistry;
    use crate::connector::iceberg::IcebergMetadataTableType;
    use crate::plan_nodes::TPlanNodeType;
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{
        CatalogProvider, ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::codegen::fragment_builder::PlanFragmentBuilder;
    use crate::sql::codegen::ir::{
        DataPartition, DataSink, DistributedPlan, PartitionKind, build_distributed_plan,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{Operator, PhysicalProjectOp, PhysicalScanOp};
    use crate::sql::optimizer::physical_plan::{PhysicalPlanNode, PlanExecutionProps};
    use crate::sql::optimizer::statistics::Statistics;

    #[test]
    fn build_via_distributed_plan_lowers_project_over_scan() {
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        let result = PlanFragmentBuilder::build_via_distributed_plan(
            &project_over_metadata_scan_plan(),
            &catalog,
            &connectors,
            "test_db",
        )
        .expect("build_via_distributed_plan");

        assert_eq!(result.root_fragment_id, 0);
        assert_eq!(result.fragment_results.len(), 1);
        let root = result
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == result.root_fragment_id)
            .expect("root fragment");
        let node_types: Vec<TPlanNodeType> =
            root.plan.nodes.iter().map(|node| node.node_type).collect();
        assert_eq!(
            node_types,
            vec![TPlanNodeType::PROJECT_NODE, TPlanNodeType::HDFS_SCAN_NODE]
        );
        assert!(
            root.desc_tbl.tuple_descriptors.len() >= 2,
            "project and scan tuples should be registered"
        );
        assert!(
            root.desc_tbl
                .slot_descriptors
                .as_ref()
                .expect("slot descriptors")
                .len()
                >= 2,
            "project and scan slots should be registered"
        );
    }

    #[test]
    fn lower_distributed_plan_rejects_non_m0_fragment_shape() {
        let mut extra_fragment = distributed_project_scan_plan();
        let mut duplicate = extra_fragment.fragments[0].clone();
        duplicate.fragment_id = 1;
        extra_fragment.fragments.push(duplicate);
        assert_lowering_err(
            &extra_fragment,
            "lower_distributed_plan M0 supports exactly one fragment",
        );

        let mut root_mismatch = distributed_project_scan_plan();
        root_mismatch.root_fragment_id = 99;
        assert_lowering_err(
            &root_mismatch,
            "lower_distributed_plan M0 root fragment id=99 does not match only fragment id=0",
        );

        let mut noop_sink = distributed_project_scan_plan();
        noop_sink.fragments[0].sink = DataSink::Noop;
        assert_lowering_err(
            &noop_sink,
            "lower_distributed_plan M0 supports only result sink",
        );

        let mut random_partition = distributed_project_scan_plan();
        random_partition.fragments[0].data_partition = DataPartition {
            kind: PartitionKind::Random,
            exprs: vec![],
        };
        assert_lowering_err(
            &random_partition,
            "lower_distributed_plan M0 supports only unpartitioned data_partition",
        );

        let mut output_exprs = distributed_project_scan_plan();
        output_exprs.fragments[0].output_exprs = Some(vec![]);
        assert_lowering_err(
            &output_exprs,
            "lower_distributed_plan M0 does not support fragment output_exprs",
        );
    }

    struct DummyCatalog;

    impl CatalogProvider for DummyCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used by distributed-plan lowering test".to_string())
        }
    }

    fn distributed_project_scan_plan() -> DistributedPlan {
        build_distributed_plan(&project_over_metadata_scan_plan()).expect("build DistributedPlan")
    }

    fn assert_lowering_err(dp: &DistributedPlan, expected: &str) {
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        let err = match super::lower_distributed_plan(dp, &catalog, &connectors) {
            Ok(_) => panic!("expected lowering error containing `{expected}`"),
            Err(err) => err,
        };
        assert!(
            err.contains(expected),
            "expected `{expected}` in lowering error `{err}`"
        );
    }

    fn project_over_metadata_scan_plan() -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let scan = physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: metadata_table_def(),
                alias: Some("t".to_string()),
                columns: vec![k.clone()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k],
        );

        let project_output = output_col(1, "k", DataType::Int64, false);
        physical_node(
            Operator::PhysicalProject(PhysicalProjectOp {
                items: vec![ProjectItem {
                    expr: column_ref_expr(1, "k", DataType::Int64, false),
                    output_name: "k".to_string(),
                    output_column_id: ColumnId::new_for_test(1),
                }],
                output_qualifier: None,
            }),
            vec![scan],
            vec![project_output],
        )
    }

    fn physical_node(
        op: Operator,
        children: Vec<PhysicalPlanNode>,
        output_columns: Vec<OutputColumn>,
    ) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op,
            children,
            stats: Statistics::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    fn metadata_table_def() -> TableDef {
        TableDef {
            name: "t$snapshots".to_string(),
            columns: vec![column_def("k", DataType::Int64, false)],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergMetadataTable {
                table: iceberg_table_info(),
                metadata_table_type: IcebergMetadataTableType::Snapshots,
                serialized_table: "{}".to_string(),
                cloud_properties: Default::default(),
                metadata_payload: None,
            },
        }
    }

    fn iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "t".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///warehouse/t".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn column_def(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_col(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn column_ref_expr(id: u32, column: &str, data_type: DataType, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: column.to_string(),
            },
            data_type,
            nullable,
        }
    }
}
