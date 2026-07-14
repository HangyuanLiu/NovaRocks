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

// The node-output catalog is the planner-native record of every non-aggregate
// physical node's execution output. CGO-9C Task 1 populates it and converts the
// native encoder onto exact reads of it; Tasks 2-5 (fragment/stream projection,
// write/router, guards) consume the occurrence mapping. Until every field and
// accessor is read, allow the not-yet-consumed surface, mirroring `boundary.rs`.
#![allow(dead_code)]

//! Planner-native node execution-output contract for a sealed distributed plan.
//!
//! Where [`super::boundary`] records *which columns cross each plan seam*, this
//! module records *what each execution node outputs*. It finalizes the output
//! columns of the non-aggregate physical nodes whose output the native encoder
//! historically re-derived or repaired: joins (`HashJoin` / `NestLoopJoin`),
//! `Scan`, set operations (`SetOp`), and `Sort`. Aggregate output shape/type is
//! deliberately excluded here (CGO-9C Task 4 owns it).
//!
//! Non-join covered outputs (`Scan`, `SetOp`, `Sort`) are *read* from the
//! planner's already-computed physical payloads. Join outputs (`HashJoin` /
//! `NestLoopJoin`) are *reconciled against the join's children* rather than read
//! verbatim: a join's payload `output_columns` carry the planner-logical columns
//! selected for the join, but after fragmentation and scan column pruning those
//! ids can diverge from what the children actually produce at execution (for
//! example a marker/anti join whose probe scan pruned metadata columns, or a
//! join whose logical output lists a column no child emits). The BE builds the
//! join's output chunk from the concatenation of its children's schemas, so this
//! module recomputes the join's execution output from the children's outputs
//! (per join type, preserving the nullable side and any internal marker column)
//! and keeps the payload list only when it already matches. This is the
//! planner-side successor of the native encoder's now-removed join output
//! repair; the encoder maps the sealed contract 1:1.
//!
//! Every covered output is finalized with **unique wire column ids**: the BE
//! rejects duplicate `OutputColumn.column_id`s in a node schema, so a repeated
//! logical [`ColumnId`] within one covered node output is deduplicated here
//! (keeping the first occurrence). Re-materializing a column at several output
//! positions (`SELECT a, a`) is a boundary/projection concern, not a covered
//! node concern.
//!
//! Finalization also *validates* that each covered node carries a complete
//! output (non-empty after reconciliation, and, for `SetOp`, a per-child schema
//! that lines up with the node's children) and fails fast otherwise.
//!
//! Occurrence identity reuses the boundary catalog's work: a covered node that
//! is a fragment root whose fragment-level sink boundary carries exactly that
//! node's output (result / change-stream router / by-ordinal Iceberg write)
//! reuses the boundary's [`ExecutionColumnId`] occurrences. Every other node
//! occurrence is *internal* and is numbered from the SAME query-scoped
//! [`ExecutionColumnIdAllocator`], continued from where boundary derivation
//! stopped. The allocator is never rebuilt and occurrence identity is never
//! derived from [`ColumnId`] (which is shared across occurrences).
//!
//! This module depends only on planner and arrow types: no protobuf, no
//! coordinator, no runtime handles.

use std::collections::{BTreeMap, HashSet};
use std::fmt;

use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::common::expr::JoinKind;
use crate::sql::planner::payload::{PlanGenerateSeriesNode, PlanProjectNode, PlanScanNode};

use super::boundary::{
    BoundaryCatalog, BoundaryContract, ExecutionColumnId, ExecutionColumnIdAllocator,
};
use super::{DistributedNode, DistributedNodeKind, ExchangeReceiver, FragmentId, PlanFragment};

/// Fragment root nodes keyed by fragment id, used to resolve an exchange
/// receiver's execution output to what its source fragment actually sends.
type FragmentRoots<'a> = BTreeMap<FragmentId, &'a DistributedNode>;

/// The physical node kinds whose execution output this contract finalizes.
///
/// Aggregate nodes are intentionally absent: their output shape and
/// intermediate types stay in CGO-9C Task 4.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum NodeExecutionKind {
    Scan,
    HashJoin,
    NestLoopJoin,
    SetOp,
    Sort,
}

impl fmt::Display for NodeExecutionKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Scan => "scan",
            Self::HashJoin => "hash-join",
            Self::NestLoopJoin => "nest-loop-join",
            Self::SetOp => "set-op",
            Self::Sort => "sort",
        })
    }
}

/// One output column of a finalized execution node, carrying both its
/// query-scoped occurrence identity and its logical planner provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeExecutionColumn {
    /// Query-scoped occurrence identity (reused from the boundary catalog when
    /// this node output participates in a boundary, otherwise freshly allocated).
    pub execution_column_id: ExecutionColumnId,
    /// Logical planner provenance (shared across occurrences of the column).
    pub column_id: ColumnId,
    /// Position of this column within *this* node's output (0-based).
    pub output_ordinal: usize,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_internal: bool,
}

/// The finalized execution output of a single covered node, keyed by the node's
/// `(fragment_id, node_id)` identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeExecutionOutput {
    pub fragment_id: FragmentId,
    pub node_id: i32,
    pub kind: NodeExecutionKind,
    pub columns: Vec<NodeExecutionColumn>,
}

/// The full set of finalized node execution outputs for a sealed distributed
/// plan, in deterministic derivation order (fragment declaration order, then a
/// pre-order walk of each fragment's node tree).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeOutputCatalog {
    outputs: Vec<NodeExecutionOutput>,
    index: BTreeMap<(FragmentId, i32), usize>,
}

impl NodeOutputCatalog {
    /// All finalized node outputs in canonical derivation order.
    pub(crate) fn outputs(&self) -> &[NodeExecutionOutput] {
        &self.outputs
    }

    /// The finalized output of the node identified by `(fragment_id, node_id)`,
    /// or `None` if that node is not a covered kind.
    pub(crate) fn output_for(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&NodeExecutionOutput> {
        self.index
            .get(&(fragment_id, node_id))
            .map(|&index| &self.outputs[index])
    }
}

/// A reason node-output finalization refused to seal the plan.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::sql::planner::distributed) enum NodeOutputError {
    /// A covered node carries no output columns. The encoder used to fall back
    /// to a child- or type-derived schema here; finalization fails fast instead.
    MissingOutputColumns {
        fragment_id: FragmentId,
        node_id: i32,
        kind: NodeExecutionKind,
    },
    /// A `SetOp` node's per-child output schema list does not line up with its
    /// children (arity mismatch). The encoder used to re-derive a child's
    /// columns here; finalization fails fast instead.
    SetOpChildArityMismatch {
        fragment_id: FragmentId,
        node_id: i32,
        children: usize,
        child_output_columns: usize,
    },
    /// Two distinct nodes share a `(fragment_id, node_id)` identity, so the
    /// output cannot be keyed unambiguously. Structural invariants keep node ids
    /// unique within a fragment; this re-checks rather than silently overwriting.
    DuplicateNodeKey {
        fragment_id: FragmentId,
        node_id: i32,
    },
    /// A join reconciliation reached a child node kind whose execution output
    /// cannot be derived (mirrors the native encoder's fail-fast for the same
    /// kinds). No valid plan places such a node under a join.
    NonDerivableChildOutput {
        fragment_id: FragmentId,
        node_id: i32,
        kind: &'static str,
    },
    /// A unary passthrough node reached while deriving a join's execution output
    /// does not have exactly one child, so its output cannot be forwarded.
    PassthroughArityMismatch {
        fragment_id: FragmentId,
        node_id: i32,
        children: usize,
    },
}

impl fmt::Display for NodeOutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingOutputColumns {
                fragment_id,
                node_id,
                kind,
            } => write!(
                formatter,
                "distributed plan {kind} node fragment_id={fragment_id} node_id={node_id} has no execution output columns"
            ),
            Self::SetOpChildArityMismatch {
                fragment_id,
                node_id,
                children,
                child_output_columns,
            } => write!(
                formatter,
                "distributed plan set-op node fragment_id={fragment_id} node_id={node_id} declares {child_output_columns} child output schemas but has {children} children"
            ),
            Self::DuplicateNodeKey {
                fragment_id,
                node_id,
            } => write!(
                formatter,
                "distributed plan node fragment_id={fragment_id} node_id={node_id} is declared more than once"
            ),
            Self::NonDerivableChildOutput {
                fragment_id,
                node_id,
                kind,
            } => write!(
                formatter,
                "distributed plan {kind} node fragment_id={fragment_id} node_id={node_id} has no derivable execution output as a join child"
            ),
            Self::PassthroughArityMismatch {
                fragment_id,
                node_id,
                children,
            } => write!(
                formatter,
                "distributed plan passthrough node fragment_id={fragment_id} node_id={node_id} expected one child for output columns but has {children}"
            ),
        }
    }
}

/// Return the covered kind and finalized execution-output columns of a node, or
/// `None` for a node whose output this contract does not finalize.
///
/// Non-join covered outputs (`Scan`, `SetOp`, `Sort`) are read from the
/// planner-computed payload. Join outputs are *reconciled against the node's
/// children* (see [`derive_join_execution_output`]) rather than read verbatim,
/// because a join's payload `output_columns` can list ids no child produces at
/// execution. Every covered output is deduplicated by column id so the wire
/// schema the encoder emits has unique `OutputColumn.column_id`s.
fn covered_node_output(
    node: &DistributedNode,
    fragment_roots: &FragmentRoots<'_>,
) -> Result<Option<(NodeExecutionKind, Vec<OutputColumn>)>, NodeOutputError> {
    let (kind, columns) = match &node.payload {
        DistributedNodeKind::Scan(scan) => (
            NodeExecutionKind::Scan,
            scan_execution_output_columns(scan)?,
        ),
        DistributedNodeKind::HashJoin(join) => (
            NodeExecutionKind::HashJoin,
            derive_join_execution_output(
                join.join_type,
                &join.output_columns,
                node,
                fragment_roots,
            )?,
        ),
        DistributedNodeKind::NestLoopJoin(join) => (
            NodeExecutionKind::NestLoopJoin,
            derive_join_execution_output(
                join.join_type,
                &join.output_columns,
                node,
                fragment_roots,
            )?,
        ),
        DistributedNodeKind::SetOp(set_op) => {
            (NodeExecutionKind::SetOp, set_op.output_columns.clone())
        }
        DistributedNodeKind::Sort(sort) => (NodeExecutionKind::Sort, sort.output_columns.clone()),
        _ => return Ok(None),
    };
    Ok(Some((kind, deduplicate_output_columns_by_id(columns))))
}

/// Reconcile a join's execution output against its children.
///
/// The BE materializes a join's output chunk from the concatenation of its
/// children's execution schemas (with the null-able side made nullable for outer
/// joins, and only one side kept for semi/anti joins). The join's payload
/// `output_columns` are the planner-logical columns selected for it, but after
/// fragmentation and scan column pruning those ids can reference columns no child
/// emits. This recomputes the join output from the children's actual outputs and
/// keeps the payload list only when it already lists exactly the derived ids in
/// order (which preserves the payload's names/nullability); otherwise the derived
/// output wins. A join with anything other than two children keeps its payload
/// list verbatim (there is nothing to reconcile against).
///
/// This mirrors the semantics of the native encoder's now-removed
/// `normalize_join_output_columns` / `derive_join_output_columns`.
fn derive_join_execution_output(
    join_type: JoinKind,
    requested: &[OutputColumn],
    node: &DistributedNode,
    fragment_roots: &FragmentRoots<'_>,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    let [left, right] = node.children.as_slice() else {
        return Ok(requested.to_vec());
    };
    let left = node_execution_output_columns(left, fragment_roots)?;
    let right = node_execution_output_columns(right, fragment_roots)?;
    let derived = join_output_columns_from_children(join_type, left, right);
    if requested.is_empty() || !same_output_column_ids(requested, &derived) {
        Ok(derived)
    } else {
        Ok(requested.to_vec())
    }
}

/// Compute the logical execution-output columns a distributed node produces at
/// the BE. Used to reconcile a join against its children, so it must match the
/// BE's per-node output exactly (the native encoder's `encoded_node_output_columns`
/// is the wire-side twin of this walk). Fails fast on a node kind whose execution
/// output cannot be derived, mirroring that encoder.
fn node_execution_output_columns(
    node: &DistributedNode,
    fragment_roots: &FragmentRoots<'_>,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    match &node.payload {
        DistributedNodeKind::Scan(scan) => scan_execution_output_columns(scan),
        DistributedNodeKind::Values(values) => Ok(values.columns.clone()),
        // Unary passthrough nodes forward their child's execution output.
        DistributedNodeKind::Filter(_)
        | DistributedNodeKind::Sort(_)
        | DistributedNodeKind::TopN(_)
        | DistributedNodeKind::AssertOneRow(_) => {
            unary_passthrough_output_columns(node, fragment_roots)
        }
        DistributedNodeKind::Project(project) => Ok(project_execution_output_columns(project)),
        DistributedNodeKind::HashAggregate(aggregate) => {
            // Prefer the aggregate's visible `output_columns` (a subset-by-id,
            // possibly reordered, of the full layout — the projection introduced
            // by #551 "Project visible aggregate output columns"), falling back to
            // the full group-key + aggregate layout only when it is empty. The BE
            // aggregate emits exactly this set, so a parent join derived from the
            // full layout would declare columns the child never produces. This
            // mirrors the encoder's `encoded_node_output_columns` aggregate arm.
            if aggregate.output_columns.is_empty() {
                Ok(aggregate.output_layout.full_output_columns())
            } else {
                Ok(aggregate.output_columns.clone())
            }
        }
        DistributedNodeKind::Window(window) => Ok(window.output_columns.clone()),
        DistributedNodeKind::GenerateSeries(generate_series) => {
            Ok(vec![generate_series_output_column(generate_series)])
        }
        DistributedNodeKind::TableFunction(table_function) => {
            let mut columns = unary_passthrough_output_columns(node, fragment_roots)?;
            columns.extend(table_function.output_columns.iter().cloned());
            Ok(columns)
        }
        DistributedNodeKind::SetOp(set_op) => Ok(set_op.output_columns.clone()),
        DistributedNodeKind::ChangeEventExpand(expand) => Ok(expand.output_columns.clone()),
        DistributedNodeKind::HashJoin(join) => {
            derive_join_execution_output(join.join_type, &join.output_columns, node, fragment_roots)
        }
        DistributedNodeKind::NestLoopJoin(join) => {
            derive_join_execution_output(join.join_type, &join.output_columns, node, fragment_roots)
        }
        DistributedNodeKind::Exchange(exchange) => {
            exchange_execution_output_columns(exchange, fragment_roots)
        }
        DistributedNodeKind::Repeat(_) => Err(NodeOutputError::NonDerivableChildOutput {
            fragment_id: node.fragment_id,
            node_id: node.node_id,
            kind: "repeat",
        }),
    }
}

/// An exchange receiver delivers exactly what its source fragment sends, so its
/// execution output is the source fragment root's execution output restricted to
/// the ids the receiver actually carries. The receiver's *declared*
/// `output_columns` can over-list ids the source pruned away (for example a probe
/// scan that only materializes its `required_columns`); intersecting with the
/// source root output drops those stale ids while keeping the receiver's declared
/// order and column metadata. Fixing up the receiver's own declared columns is a
/// separate concern (the encoder's exchange-receiver patch); this only computes
/// what a *parent join* actually sees.
fn exchange_execution_output_columns(
    exchange: &ExchangeReceiver,
    fragment_roots: &FragmentRoots<'_>,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    let Some(source_root) = fragment_roots.get(&exchange.source_fragment_id) else {
        // No source fragment in this catalog (should not happen for a sealed
        // plan); fall back to the declared columns rather than fail.
        return Ok(exchange.output_columns.clone());
    };
    let source_ids: HashSet<ColumnId> = node_execution_output_columns(source_root, fragment_roots)?
        .into_iter()
        .map(|column| column.column_id)
        .collect();
    let projected: Vec<OutputColumn> = exchange
        .output_columns
        .iter()
        .filter(|column| source_ids.contains(&column.column_id))
        .cloned()
        .collect();
    // A correct plan always has a non-empty intersection; if the source output
    // could not be reconciled against the declared columns, keep the declared
    // columns verbatim rather than emit an empty exchange output.
    if projected.is_empty() {
        Ok(exchange.output_columns.clone())
    } else {
        Ok(projected)
    }
}

/// A scan produces the payload columns it materializes, restricted to
/// `required_columns` when those prune the projected set (matching the BE read
/// plan). `required_columns` is `None`/empty when the scan materializes every
/// projected column.
///
/// `required_columns` is expected to name a subset of the projected columns. If
/// it matches none of them — an inconsistent scan (e.g. a projected column
/// renamed away from its binding) that a later binding stage rejects with a
/// precise message — fall back to the full projection rather than manufacture an
/// empty execution output here.
fn scan_execution_output_columns(
    scan: &PlanScanNode,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    let required = match &scan.required_columns {
        Some(required) if !required.is_empty() => required,
        _ => return Ok(scan.columns.clone()),
    };
    let required: HashSet<String> = required
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect();
    let pruned: Vec<OutputColumn> = scan
        .columns
        .iter()
        .filter(|column| required.contains(&column.name.to_ascii_lowercase()))
        .cloned()
        .collect();
    if pruned.is_empty() {
        Ok(scan.columns.clone())
    } else {
        Ok(pruned)
    }
}

/// A project produces exactly one output column per item.
fn project_execution_output_columns(project: &PlanProjectNode) -> Vec<OutputColumn> {
    project
        .items
        .iter()
        .map(|item| OutputColumn {
            column_id: item.output_column_id,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        })
        .collect()
}

fn generate_series_output_column(generate_series: &PlanGenerateSeriesNode) -> OutputColumn {
    OutputColumn {
        column_id: generate_series.output_column_id,
        name: if generate_series.column_name.is_empty() {
            "generate_series".to_string()
        } else {
            generate_series.column_name.clone()
        },
        data_type: DataType::Int64,
        nullable: false,
        is_internal: false,
    }
}

fn unary_passthrough_output_columns(
    node: &DistributedNode,
    fragment_roots: &FragmentRoots<'_>,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    let [child] = node.children.as_slice() else {
        return Err(NodeOutputError::PassthroughArityMismatch {
            fragment_id: node.fragment_id,
            node_id: node.node_id,
            children: node.children.len(),
        });
    };
    node_execution_output_columns(child, fragment_roots)
}

/// Concatenate the children's outputs per join type: outer joins make the
/// null-able side nullable; semi/anti joins keep only the surviving side.
fn join_output_columns_from_children(
    join_type: JoinKind,
    left: Vec<OutputColumn>,
    right: Vec<OutputColumn>,
) -> Vec<OutputColumn> {
    match join_type {
        JoinKind::Inner | JoinKind::Cross => {
            let mut output = left;
            output.extend(right);
            output
        }
        JoinKind::LeftOuter => {
            let mut output = left;
            output.extend(nullable_output_columns(right));
            output
        }
        JoinKind::RightOuter => {
            let mut output = nullable_output_columns(left);
            output.extend(right);
            output
        }
        JoinKind::FullOuter => {
            let mut output = nullable_output_columns(left);
            output.extend(nullable_output_columns(right));
            output
        }
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left,
        JoinKind::RightSemi | JoinKind::RightAnti => right,
    }
}

fn nullable_output_columns(mut columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    for column in &mut columns {
        column.nullable = true;
    }
    columns
}

fn same_output_column_ids(left: &[OutputColumn], right: &[OutputColumn]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| left.column_id == right.column_id)
}

/// Keep the first occurrence of each column id so the wire schema the encoder
/// emits has unique `OutputColumn.column_id`s (the BE rejects duplicates).
fn deduplicate_output_columns_by_id(columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    let mut seen = HashSet::with_capacity(columns.len());
    columns
        .into_iter()
        .filter(|column| seen.insert(column.column_id))
        .collect()
}

/// Derive the authoritative node execution-output catalog from already-sealed
/// fragments, the boundary catalog, and the continued occurrence allocator.
///
/// Runs inside `seal_draft` after `build_boundary_catalog`, so the boundary
/// occurrences already exist and the `allocator` is positioned one past the last
/// boundary occurrence. Non-join covered outputs are read from planner-computed
/// payloads; join outputs are reconciled against their children (see
/// [`covered_node_output`]). It never rebuilds the allocator.
///
/// Determinism: fragments are visited in declaration order and each fragment's
/// node tree in pre-order, so the single `allocator` assigns internal occurrence
/// ids in a fully input-determined sequence.
pub(in crate::sql::planner::distributed) fn build_node_output_catalog(
    fragments: &[PlanFragment],
    boundaries: &BoundaryCatalog,
    allocator: &mut ExecutionColumnIdAllocator,
) -> Result<NodeOutputCatalog, NodeOutputError> {
    // Fragment-level sink boundaries (result / Iceberg write input / change
    // stream router input) are keyed by fragment id and describe exactly the
    // fragment root's output. A covered root node reuses these occurrences.
    let sink_boundary_by_fragment: BTreeMap<FragmentId, &BoundaryContract> = boundaries
        .contracts()
        .iter()
        .filter(|contract| contract.node_id.is_none())
        .map(|contract| (contract.fragment_id, contract))
        .collect();

    // Fragment roots keyed by id, so an exchange receiver can resolve its
    // execution output to what its source fragment actually sends.
    let fragment_roots: FragmentRoots<'_> = fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, &fragment.root))
        .collect();

    let mut outputs = Vec::new();
    let mut index = BTreeMap::new();
    for fragment in fragments {
        let root_sink_boundary = sink_boundary_by_fragment
            .get(&fragment.fragment_id)
            .copied();
        visit_node(
            &fragment.root,
            fragment.fragment_id,
            true,
            root_sink_boundary,
            &fragment_roots,
            allocator,
            &mut outputs,
            &mut index,
        )?;
    }

    Ok(NodeOutputCatalog { outputs, index })
}

#[allow(clippy::too_many_arguments)]
fn visit_node(
    node: &DistributedNode,
    fragment_id: FragmentId,
    is_fragment_root: bool,
    root_sink_boundary: Option<&BoundaryContract>,
    fragment_roots: &FragmentRoots<'_>,
    allocator: &mut ExecutionColumnIdAllocator,
    outputs: &mut Vec<NodeExecutionOutput>,
    index: &mut BTreeMap<(FragmentId, i32), usize>,
) -> Result<(), NodeOutputError> {
    if let Some((kind, columns)) = covered_node_output(node, fragment_roots)? {
        validate_node_output(fragment_id, node, kind, &columns)?;

        // Reuse the boundary occurrences only when this node is the fragment
        // root and the fragment's sink boundary carries exactly this node's
        // output (same length and per-ordinal logical column id). This holds for
        // result and change-stream router sinks and for by-ordinal Iceberg write
        // input; a reordered write projection or an exchange producer does not
        // match and is numbered as internal.
        let reuse = if is_fragment_root {
            root_sink_boundary.filter(|boundary| boundary_matches_node_output(boundary, &columns))
        } else {
            None
        };

        let execution_columns = assign_occurrences(&columns, reuse, allocator);
        let node_key = (fragment_id, node.node_id);
        if index.contains_key(&node_key) {
            return Err(NodeOutputError::DuplicateNodeKey {
                fragment_id,
                node_id: node.node_id,
            });
        }
        index.insert(node_key, outputs.len());
        outputs.push(NodeExecutionOutput {
            fragment_id,
            node_id: node.node_id,
            kind,
            columns: execution_columns,
        });
    }

    for child in &node.children {
        visit_node(
            child,
            fragment_id,
            false,
            root_sink_boundary,
            fragment_roots,
            allocator,
            outputs,
            index,
        )?;
    }
    Ok(())
}

/// Validate that a covered node carries a complete finalized output. The checks
/// are structural (non-empty after reconciliation/dedup, and — for `SetOp` — a
/// per-child schema whose arity matches the node's children).
fn validate_node_output(
    fragment_id: FragmentId,
    node: &DistributedNode,
    kind: NodeExecutionKind,
    columns: &[OutputColumn],
) -> Result<(), NodeOutputError> {
    ensure_output_columns_present(fragment_id, node.node_id, kind, columns)?;

    if let DistributedNodeKind::SetOp(set_op) = &node.payload {
        if set_op.child_output_columns.len() != node.children.len() {
            return Err(NodeOutputError::SetOpChildArityMismatch {
                fragment_id,
                node_id: node.node_id,
                children: node.children.len(),
                child_output_columns: set_op.child_output_columns.len(),
            });
        }
        for child_columns in &set_op.child_output_columns {
            ensure_output_columns_present(fragment_id, node.node_id, kind, child_columns)?;
        }
    }
    Ok(())
}

fn ensure_output_columns_present(
    fragment_id: FragmentId,
    node_id: i32,
    kind: NodeExecutionKind,
    columns: &[OutputColumn],
) -> Result<(), NodeOutputError> {
    if columns.is_empty() {
        return Err(NodeOutputError::MissingOutputColumns {
            fragment_id,
            node_id,
            kind,
        });
    }
    Ok(())
}

/// Whether a fragment-level sink boundary carries exactly the given node output:
/// same length and the same logical column id at every ordinal.
fn boundary_matches_node_output(boundary: &BoundaryContract, columns: &[OutputColumn]) -> bool {
    boundary.columns.len() == columns.len()
        && boundary
            .columns
            .iter()
            .zip(columns.iter())
            .all(|(boundary_column, column)| boundary_column.column_id == column.column_id)
}

/// Number each node output column as an occurrence: reuse the matching boundary
/// occurrence when `reuse` is set, otherwise allocate a fresh id from the shared
/// query-scoped allocator. When `reuse` is set it is guaranteed (by
/// [`boundary_matches_node_output`]) to have the same length as `columns`.
fn assign_occurrences(
    columns: &[OutputColumn],
    reuse: Option<&BoundaryContract>,
    allocator: &mut ExecutionColumnIdAllocator,
) -> Vec<NodeExecutionColumn> {
    columns
        .iter()
        .enumerate()
        .map(|(output_ordinal, column)| {
            let execution_column_id = match reuse {
                Some(boundary) => boundary.columns[output_ordinal].execution_column_id,
                None => allocator.allocate(),
            };
            NodeExecutionColumn {
                execution_column_id,
                column_id: column.column_id,
                output_ordinal,
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                is_internal: column.is_internal,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::NodeExecutionKind;
    use crate::runtime_filter::model::graph::RuntimeFilterGraph;
    use crate::sql::analysis::{JoinKind, OutputColumn};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan,
        ExchangeFlavor, ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
        PlanFragment,
    };
    use crate::sql::planner::payload::{PlanScanNode, PlanSortNode, PlanValuesNode};
    use crate::sql::planner::physical::{
        AggMode, AggregateOutputLayout, JoinDistribution, PhysicalHashAggregateNode,
        PhysicalHashJoinNode, PhysicalNestLoopJoinNode, PhysicalPlanStats, PhysicalSetOpNode,
        PlanSetOpKind, PlannerConfidence,
    };

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn output_col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn internal_col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: true,
        }
    }

    fn node_in(
        fragment_id: u32,
        node_id: i32,
        children: Vec<DistributedNode>,
        payload: DistributedNodeKind,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children,
            stats: stats(),
            payload,
        }
    }

    fn node(
        node_id: i32,
        children: Vec<DistributedNode>,
        payload: DistributedNodeKind,
    ) -> DistributedNode {
        node_in(0, node_id, children, payload)
    }

    fn values_node_in(
        fragment_id: u32,
        node_id: i32,
        columns: Vec<OutputColumn>,
    ) -> DistributedNode {
        node_in(
            fragment_id,
            node_id,
            Vec::new(),
            DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns,
            }),
        )
    }

    fn values_node(node_id: i32, columns: Vec<OutputColumn>) -> DistributedNode {
        values_node_in(0, node_id, columns)
    }

    /// A `HashAggregate` payload whose full group-key + aggregate layout is
    /// `full_layout` but whose visible `output_columns` (what the BE emits) is
    /// `visible`.
    fn hash_aggregate_payload(
        group_key: Vec<OutputColumn>,
        aggregate: Vec<OutputColumn>,
        visible: Vec<OutputColumn>,
    ) -> DistributedNodeKind {
        DistributedNodeKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Single,
            group_by: Vec::new(),
            aggregates: Vec::new(),
            is_merge: Vec::new(),
            output_layout: AggregateOutputLayout::new(group_key, aggregate),
            output_columns: visible,
        }))
    }

    fn scan_payload(columns: Vec<OutputColumn>) -> DistributedNodeKind {
        scan_payload_with_required(columns, None)
    }

    fn scan_payload_with_required(
        columns: Vec<OutputColumn>,
        required_columns: Option<Vec<String>>,
    ) -> DistributedNodeKind {
        DistributedNodeKind::Scan(PlanScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: Vec::new(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::StarRocks {
                    db_id: 1,
                    table_id: 2,
                },
            },
            alias: None,
            columns,
            predicates: Vec::new(),
            required_columns,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        })
    }

    fn sort_payload(output_columns: Vec<OutputColumn>) -> DistributedNodeKind {
        DistributedNodeKind::Sort(PlanSortNode {
            items: Vec::new(),
            analytic_partition_by: Vec::new(),
            output_columns,
            offset: None,
            partition_limit: None,
            topn_type: None,
        })
    }

    fn hash_join_payload(output_columns: Vec<OutputColumn>) -> DistributedNodeKind {
        hash_join_payload_typed(JoinKind::Inner, output_columns)
    }

    fn hash_join_payload_typed(
        join_type: JoinKind,
        output_columns: Vec<OutputColumn>,
    ) -> DistributedNodeKind {
        DistributedNodeKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type,
            eq_conditions: Vec::new(),
            other_condition: None,
            distribution: JoinDistribution::Unknown,
            execution_mode: None,
            build_runtime_filters: Vec::new(),
            output_columns,
        }))
    }

    fn nest_loop_join_payload(output_columns: Vec<OutputColumn>) -> DistributedNodeKind {
        DistributedNodeKind::NestLoopJoin(PhysicalNestLoopJoinNode {
            join_type: JoinKind::Inner,
            condition: None,
            output_columns,
        })
    }

    fn set_op_payload(
        output_columns: Vec<OutputColumn>,
        child_output_columns: Vec<Vec<OutputColumn>>,
    ) -> DistributedNodeKind {
        DistributedNodeKind::SetOp(PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns,
            child_output_columns,
        })
    }

    /// Seal a single-fragment result plan whose root is `root` and whose
    /// fragment output columns are `output_columns`.
    fn seal_single_fragment(
        root: DistributedNode,
        output_columns: Vec<OutputColumn>,
    ) -> Result<DistributedPlan, String> {
        DistributedPlanDraftBuilder::new(
            vec![PlanFragment {
                fragment_id: 0,
                root,
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            Some(0),
            Vec::new(),
            RuntimeFilterGraph::default(),
        )
        .seal()
    }

    // ----- RED: the seal must reject stale/missing/inconsistent node outputs --

    #[test]
    fn seal_rejects_hash_join_node_with_no_execution_output() {
        // A join's execution output is reconciled from its children. When both
        // children produce nothing, the derived output is empty and the seal must
        // fail fast rather than emit an empty join schema.
        let root = node(
            1,
            vec![values_node(2, Vec::new()), values_node(3, Vec::new())],
            hash_join_payload(Vec::new()),
        );
        let error = seal_single_fragment(root, Vec::new())
            .expect_err("a join node without derivable execution output must not seal");
        assert!(error.contains("hash-join"), "{error}");
        assert!(error.contains("no execution output columns"), "{error}");
    }

    #[test]
    fn seal_rejects_nest_loop_join_node_with_no_execution_output() {
        let root = node(
            1,
            vec![values_node(2, Vec::new()), values_node(3, Vec::new())],
            nest_loop_join_payload(Vec::new()),
        );
        let error = seal_single_fragment(root, Vec::new())
            .expect_err("a nest-loop join without derivable execution output must not seal");
        assert!(error.contains("nest-loop-join"), "{error}");
        assert!(error.contains("no execution output columns"), "{error}");
    }

    #[test]
    fn seal_rejects_scan_node_with_no_execution_output() {
        let root = node(1, Vec::new(), scan_payload(Vec::new()));
        let error = seal_single_fragment(root, Vec::new())
            .expect_err("a scan without execution output must not seal");
        assert!(error.contains("scan"), "{error}");
        assert!(error.contains("no execution output columns"), "{error}");
    }

    #[test]
    fn seal_rejects_set_op_node_with_no_execution_output() {
        let root = node(
            1,
            vec![
                values_node(2, vec![output_col(1, "a")]),
                values_node(3, vec![output_col(1, "a")]),
            ],
            set_op_payload(
                Vec::new(),
                vec![vec![output_col(1, "a")], vec![output_col(1, "a")]],
            ),
        );
        let error = seal_single_fragment(root, Vec::new())
            .expect_err("a set-op without execution output must not seal");
        assert!(error.contains("set-op"), "{error}");
        assert!(error.contains("no execution output columns"), "{error}");
    }

    #[test]
    fn seal_rejects_sort_node_with_no_execution_output() {
        let root = node(
            1,
            vec![values_node(2, vec![output_col(1, "a")])],
            sort_payload(Vec::new()),
        );
        let error = seal_single_fragment(root, Vec::new())
            .expect_err("a sort without execution output must not seal");
        assert!(error.contains("sort"), "{error}");
        assert!(error.contains("no execution output columns"), "{error}");
    }

    #[test]
    fn seal_rejects_set_op_with_child_output_arity_mismatch() {
        let output_columns = vec![output_col(5, "a")];
        let root = node(
            1,
            vec![
                values_node(2, vec![output_col(1, "a")]),
                values_node(3, vec![output_col(2, "b")]),
            ],
            // Two children but only one declared child output schema.
            set_op_payload(output_columns.clone(), vec![vec![output_col(1, "a")]]),
        );
        let error = seal_single_fragment(root, output_columns).expect_err(
            "a set-op whose child schema arity disagrees with its children must not seal",
        );
        assert!(error.contains("set-op"), "{error}");
        assert!(
            error.contains("declares 1 child output schemas but has 2 children"),
            "{error}"
        );
    }

    // ----- GREEN: the seal finalizes covered node outputs --------------------

    /// A result-root Sort over a Scan child. Both covered nodes are cataloged;
    /// their kinds and logical columns follow the planner-computed payloads.
    fn sort_over_scan_plan() -> DistributedPlan {
        let columns = vec![output_col(1, "k"), output_col(2, "v")];
        let scan = node(2, Vec::new(), scan_payload(columns.clone()));
        let sort = node(1, vec![scan], sort_payload(columns.clone()));
        seal_single_fragment(sort, columns).expect("sort-over-scan plan seals")
    }

    #[test]
    fn sealed_plan_catalogs_every_covered_node_output() {
        let plan = sort_over_scan_plan();
        let catalog = plan.node_outputs();

        assert_eq!(catalog.outputs().len(), 2);

        let sort = catalog.output_for(0, 1).expect("sort node output");
        assert_eq!(sort.kind, NodeExecutionKind::Sort);
        assert_eq!(
            sort.columns
                .iter()
                .map(|column| (
                    column.column_id.0,
                    column.name.as_str(),
                    column.output_ordinal
                ))
                .collect::<Vec<_>>(),
            vec![(1, "k", 0), (2, "v", 1)]
        );

        let scan = catalog.output_for(0, 2).expect("scan node output");
        assert_eq!(scan.kind, NodeExecutionKind::Scan);
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn output_for_returns_none_for_uncovered_nodes() {
        let plan = sort_over_scan_plan();
        // node 3 does not exist; a covered lookup returns None rather than
        // guessing.
        assert!(plan.node_outputs().output_for(0, 3).is_none());
    }

    #[test]
    fn plan_without_covered_nodes_has_an_empty_catalog() {
        let columns = vec![output_col(1, "k")];
        let plan = seal_single_fragment(values_node(1, columns.clone()), columns)
            .expect("values-only plan seals");
        assert!(plan.node_outputs().outputs().is_empty());
        assert!(plan.node_outputs().output_for(0, 1).is_none());
    }

    #[test]
    fn result_root_covered_node_reuses_boundary_occurrence_ids() {
        let plan = sort_over_scan_plan();
        let result_boundary = plan
            .boundaries()
            .contracts()
            .iter()
            .find(|contract| contract.node_id.is_none())
            .expect("result boundary");
        let boundary_ids = result_boundary
            .columns
            .iter()
            .map(|column| column.execution_column_id)
            .collect::<Vec<_>>();

        // The Sort is the fragment root and its output matches the result
        // boundary, so it reuses the boundary occurrences verbatim.
        let sort = plan.node_outputs().output_for(0, 1).expect("sort output");
        let sort_ids = sort
            .columns
            .iter()
            .map(|column| column.execution_column_id)
            .collect::<Vec<_>>();
        assert_eq!(sort_ids, boundary_ids);
    }

    #[test]
    fn internal_covered_node_gets_fresh_occurrence_ids_continuing_after_boundaries() {
        let plan = sort_over_scan_plan();
        // Two boundary occurrences (the two result columns) are numbered 1 and 2;
        // the internal Scan's occurrences continue densely from 3.
        let scan = plan.node_outputs().output_for(0, 2).expect("scan output");
        let scan_ids = scan
            .columns
            .iter()
            .map(|column| column.execution_column_id.value())
            .collect::<Vec<_>>();
        assert_eq!(scan_ids, vec![3, 4]);
    }

    #[test]
    fn same_logical_column_gets_distinct_occurrences_at_root_and_internal_node() {
        let plan = sort_over_scan_plan();
        let sort = plan.node_outputs().output_for(0, 1).expect("sort output");
        let scan = plan.node_outputs().output_for(0, 2).expect("scan output");

        // Column id 1 flows through both nodes ...
        assert_eq!(sort.columns[0].column_id, scan.columns[0].column_id);
        // ... but each occurrence has its own execution column id.
        assert_ne!(
            sort.columns[0].execution_column_id,
            scan.columns[0].execution_column_id
        );
    }

    #[test]
    fn producer_fragment_covered_root_gets_fresh_occurrence_ids() {
        // A producer fragment (Noop sink feeding an Exchange) has no
        // fragment-level sink boundary, so its covered root cannot reuse a
        // boundary occurrence: the Exchange send/receive boundaries are distinct
        // occurrences of the same logical columns. The root is numbered fresh,
        // continuing after every boundary occurrence.
        let columns = vec![output_col(1, "k"), output_col(2, "v")];
        let exchange_node_id = 20;
        let producer = PlanFragment {
            fragment_id: 1,
            root: node_in(
                1,
                10,
                vec![
                    values_node_in(1, 11, vec![output_col(1, "k")]),
                    values_node_in(1, 12, vec![output_col(2, "v")]),
                ],
                hash_join_payload(columns.clone()),
            ),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let consumer = PlanFragment {
            fragment_id: 0,
            root: node(
                exchange_node_id,
                Vec::new(),
                DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: 1,
                    output_columns: columns.clone(),
                    output_qualifier: None,
                    flavor: ExchangeFlavor::Distribution,
                }),
            ),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = DistributedPlanDraftBuilder::new(
            vec![producer, consumer],
            Some(0),
            vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![1, 2],
            }],
            RuntimeFilterGraph::default(),
        )
        .seal()
        .expect("producer/consumer stream plan seals");

        // Boundaries number six occurrences (result 1-2, send 3-4, receive 5-6);
        // the producer-root join is internal to no boundary and continues from 7.
        let join = plan.node_outputs().output_for(1, 10).expect("join output");
        let boundary_ids = plan
            .boundaries()
            .contracts()
            .iter()
            .flat_map(|contract| {
                contract
                    .columns
                    .iter()
                    .map(|column| column.execution_column_id.value())
            })
            .collect::<Vec<_>>();
        let join_ids = join
            .columns
            .iter()
            .map(|column| column.execution_column_id.value())
            .collect::<Vec<_>>();
        assert!(
            join_ids.iter().all(|id| !boundary_ids.contains(id)),
            "producer root occurrences must be distinct from every boundary occurrence: join={join_ids:?} boundaries={boundary_ids:?}"
        );
        let max_boundary_id = boundary_ids.iter().copied().max().unwrap_or(0);
        assert_eq!(join_ids, vec![max_boundary_id + 1, max_boundary_id + 2]);
    }

    #[test]
    fn set_op_output_is_cataloged_from_its_own_output_columns() {
        let output_columns = vec![output_col(5, "a"), output_col(6, "b")];
        let root = node(
            1,
            vec![
                values_node(2, vec![output_col(1, "a"), output_col(2, "b")]),
                values_node(3, vec![output_col(3, "a"), output_col(4, "b")]),
            ],
            set_op_payload(
                output_columns.clone(),
                vec![
                    vec![output_col(1, "a"), output_col(2, "b")],
                    vec![output_col(3, "a"), output_col(4, "b")],
                ],
            ),
        );
        let plan = seal_single_fragment(root, output_columns).expect("set-op plan seals");
        let set_op = plan.node_outputs().output_for(0, 1).expect("set-op output");
        assert_eq!(set_op.kind, NodeExecutionKind::SetOp);
        assert_eq!(
            set_op
                .columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![5, 6]
        );
    }

    #[test]
    fn seal_deduplicates_repeated_column_ids_in_covered_output() {
        // The BE rejects a node schema with duplicate `OutputColumn.column_id`s,
        // so a covered node output that repeats a logical column id is
        // deduplicated (first occurrence kept) at seal time. Re-materializing a
        // column at several positions (`SELECT c1, c1`) is a boundary/projection
        // concern, not a covered node concern.
        let scan_columns = vec![
            output_col(1, "c1"),
            output_col(2, "c2"),
            output_col(1, "c1"),
        ];
        let root = node(1, Vec::new(), scan_payload(scan_columns));
        let plan = seal_single_fragment(root, vec![output_col(1, "c1"), output_col(2, "c2")])
            .expect("duplicate-column scan plan seals");
        let scan = plan.node_outputs().output_for(0, 1).expect("scan output");
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![1, 2],
            "the repeated logical column id is dropped, keeping the first occurrence"
        );
    }

    #[test]
    fn seal_prunes_scan_covered_output_to_required_columns() {
        // `PruneScanColumns` writes `required_columns` without shrinking the
        // projected `columns`, and the BE materializes only the required set. The
        // catalog is the authoritative execution-output record, so a scan's
        // covered output must be the required-columns-pruned set, not the full
        // projection.
        let scan = scan_payload_with_required(
            vec![
                output_col(1, "k"),
                output_col(2, "v"),
                output_col(3, "_file"),
                output_col(4, "_pos"),
            ],
            Some(vec!["k".to_string(), "v".to_string()]),
        );
        let root = node(1, Vec::new(), scan);
        let plan = seal_single_fragment(root, vec![output_col(1, "k"), output_col(2, "v")])
            .expect("required-columns scan plan seals");
        let scan = plan.node_outputs().output_for(0, 1).expect("scan output");
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![1, 2],
            "the scan covered output is pruned to required_columns [k, v]; _file/_pos are dropped"
        );
    }

    #[test]
    fn seal_reconciles_stale_join_output_from_children() {
        // The join payload lists a column (99) no child produces. The seal must
        // ignore the stale payload and take the children-derived output instead,
        // so the sealed join output references only columns in the execution chunk.
        let root = node(
            1,
            vec![
                values_node(2, vec![output_col(1, "a")]),
                values_node(3, vec![output_col(2, "b")]),
            ],
            hash_join_payload(vec![
                output_col(1, "a"),
                output_col(2, "b"),
                output_col(99, "stale"),
            ]),
        );
        let plan = seal_single_fragment(root, vec![output_col(1, "a"), output_col(2, "b")])
            .expect("stale-join plan seals by reconciling against children");
        let join = plan.node_outputs().output_for(0, 1).expect("join output");
        assert_eq!(
            join.columns
                .iter()
                .map(|column| (column.column_id.0, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "a"), (2, "b")],
            "the stale id 99 is dropped; the sealed output is the children-derived schema"
        );
    }

    #[test]
    fn seal_reconciles_marker_join_output_to_pruned_probe_and_nullable_build() {
        // Models a NOT-IN/marker `LEFT OUTER` join: the probe scan projects
        // [k, v, _file, _pos] but only materializes [k, v] (required_columns), and
        // the build side carries a nullable internal marker column. The join
        // payload still lists the pruned probe metadata columns (3, 4). The seal
        // must derive the join output from the children: probe [k, v] plus the
        // nullable build [v, __match_0], dropping the pruned 3/4 and keeping the
        // internal marker made nullable by the outer join.
        let probe = node(
            2,
            Vec::new(),
            scan_payload_with_required(
                vec![
                    output_col(1, "k"),
                    output_col(2, "v"),
                    output_col(3, "_file"),
                    output_col(4, "_pos"),
                ],
                Some(vec!["k".to_string(), "v".to_string()]),
            ),
        );
        let build = values_node(3, vec![output_col(10, "v"), internal_col(13, "__match_0")]);
        let root = node(
            1,
            vec![probe, build],
            hash_join_payload_typed(
                JoinKind::LeftOuter,
                vec![
                    output_col(1, "k"),
                    output_col(2, "v"),
                    output_col(3, "_file"),
                    output_col(4, "_pos"),
                    output_col(10, "v"),
                    internal_col(13, "__match_0"),
                ],
            ),
        );
        let plan = seal_single_fragment(
            root,
            vec![
                output_col(1, "k"),
                output_col(2, "v"),
                output_col(10, "v"),
                internal_col(13, "__match_0"),
            ],
        )
        .expect("marker-join plan seals by reconciling against children");
        let join = plan.node_outputs().output_for(0, 1).expect("join output");
        assert_eq!(
            join.columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![1, 2, 10, 13],
            "pruned probe metadata columns 3/4 are dropped; probe [k, v] and build [v, marker] survive"
        );
        let marker = join
            .columns
            .iter()
            .find(|column| column.column_id.0 == 13)
            .expect("marker column is retained");
        assert!(marker.is_internal, "the marker column stays internal");
        assert!(
            marker.nullable,
            "the outer-join build side (with the marker) is made nullable"
        );
    }

    #[test]
    fn seal_derives_join_output_from_aggregate_child_visible_columns_not_full_layout() {
        // A join whose direct child is a HashAggregate must derive from the
        // aggregate's *visible* `output_columns` (the projected subset the BE
        // actually emits), not from its full group-key + aggregate layout. Here
        // the full layout is [1:g, 2:c, 3:s] but only [1:g, 3:s] are visible, so
        // the sealed Inner-join output must be [1, 3, 5] (visible [1, 3] plus the
        // build's [5]) -- deriving from the full layout would wrongly declare the
        // hidden aggregate column 2 as [1, 2, 3, 5].
        let aggregate = node(
            2,
            vec![values_node(4, vec![output_col(1, "g")])],
            hash_aggregate_payload(
                vec![output_col(1, "g")],
                vec![output_col(2, "c"), output_col(3, "s")],
                vec![output_col(1, "g"), output_col(3, "s")],
            ),
        );
        let build = values_node(3, vec![output_col(5, "x")]);
        let root = node(
            1,
            vec![aggregate, build],
            hash_join_payload(vec![
                output_col(1, "g"),
                output_col(3, "s"),
                output_col(5, "x"),
            ]),
        );
        let plan = seal_single_fragment(
            root,
            vec![output_col(1, "g"), output_col(3, "s"), output_col(5, "x")],
        )
        .expect("join-over-aggregate plan seals from the aggregate's visible output");
        let join = plan.node_outputs().output_for(0, 1).expect("join output");
        assert_eq!(
            join.columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![1, 3, 5],
            "the hidden aggregate column 2 must not appear; only visible [1, 3] plus build [5]"
        );
    }

    #[test]
    fn node_output_catalog_derivation_is_deterministic() {
        assert_eq!(
            sort_over_scan_plan().node_outputs(),
            sort_over_scan_plan().node_outputs()
        );
    }

    #[test]
    fn output_module_has_no_runtime_or_protobuf_dependency() {
        let source = include_str!("output.rs");
        for pattern in [
            concat!("crate::", "coordinator"),
            concat!("pro", "st"),
            concat!("crate::", "proto"),
            concat!("crate::", "runtime::"),
            concat!("crate::", "thrift"),
            concat!("crate::", "sql::codegen"),
        ] {
            assert!(
                !source.contains(pattern),
                "output module must not reference `{pattern}`"
            );
        }
    }
}
