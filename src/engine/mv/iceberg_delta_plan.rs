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
//! Plan-rewrite pass for IVM-A1: locate the single base-table Scan leaf in a
//! codegen'd MV SELECT ExecPlan and swap it for `IcebergDeltaScan`. The MV
//! contract that A11 enforces — exactly one base table per MV — keeps this
//! pass valid for the projection/filter MV shape A1 supports. Aggregate and
//! join MVs are deferred to A2/A3.

use crate::exec::node::iceberg_delta_scan::IcebergDeltaScanNode;
use crate::exec::node::{ExecNode, ExecNodeKind};

/// Locator of a Scan leaf within an `ExecNode` tree. Stored as the
/// child-index path from the root. Stable across `.clone()` because the
/// rewrite uses the same `ExecNode` between locate and swap.
#[derive(Clone, Debug)]
pub(crate) struct ScanLeafLocator {
    pub path: Vec<usize>,
}

/// Walk the ExecPlan top-down and return the unique Scan leaf.
///
/// A1 contract: a base MV SELECT references exactly one Iceberg base table,
/// so the projection/filter ExecPlan has exactly one Scan leaf. Anything
/// else is a programming error and we fail fast.
pub(crate) fn find_unique_base_scan_leaf(root: &ExecNode) -> Result<ScanLeafLocator, String> {
    let mut found: Vec<ScanLeafLocator> = Vec::new();
    collect_scan_leaves(root, &mut Vec::new(), &mut found);
    match found.len() {
        0 => Err("ivm-a1 leaf-swap: no Scan leaf found in MV ExecPlan".to_string()),
        1 => Ok(found.into_iter().next().unwrap()),
        n => Err(format!(
            "ivm-a1 leaf-swap: expected exactly one Scan leaf in MV ExecPlan, found {n}"
        )),
    }
}

fn collect_scan_leaves(node: &ExecNode, stack: &mut Vec<usize>, out: &mut Vec<ScanLeafLocator>) {
    if matches!(node.kind, ExecNodeKind::Scan(_)) {
        out.push(ScanLeafLocator {
            path: stack.clone(),
        });
        return;
    }
    for (i, child) in children_of(node).into_iter().enumerate() {
        stack.push(i);
        collect_scan_leaves(child, stack, out);
        stack.pop();
    }
}

fn children_of(node: &ExecNode) -> Vec<&ExecNode> {
    match &node.kind {
        ExecNodeKind::Project(p) => vec![&p.input],
        ExecNodeKind::Filter(f) => vec![&f.input],
        ExecNodeKind::Limit(l) => vec![&l.input],
        ExecNodeKind::Repeat(r) => vec![&r.input],
        ExecNodeKind::AssertNumRows(a) => vec![&a.input],
        ExecNodeKind::Sort(s) => vec![&s.input],
        ExecNodeKind::TableFunction(t) => vec![&t.input],
        ExecNodeKind::Fetch(f) => vec![&f.input],
        ExecNodeKind::UnionAll(u) => u.inputs.iter().collect(),
        _ => Vec::new(),
    }
}

fn children_of_mut(node: &mut ExecNode) -> Vec<&mut ExecNode> {
    match &mut node.kind {
        ExecNodeKind::Project(p) => vec![&mut p.input],
        ExecNodeKind::Filter(f) => vec![&mut f.input],
        ExecNodeKind::Limit(l) => vec![&mut l.input],
        ExecNodeKind::Repeat(r) => vec![&mut r.input],
        ExecNodeKind::AssertNumRows(a) => vec![&mut a.input],
        ExecNodeKind::Sort(s) => vec![&mut s.input],
        ExecNodeKind::TableFunction(t) => vec![&mut t.input],
        ExecNodeKind::Fetch(f) => vec![&mut f.input],
        ExecNodeKind::UnionAll(u) => u.inputs.iter_mut().collect(),
        _ => Vec::new(),
    }
}

fn locate_mut<'a>(root: &'a mut ExecNode, path: &[usize]) -> Result<&'a mut ExecNode, String> {
    let mut cur = root;
    for &i in path {
        cur = children_of_mut(cur)
            .into_iter()
            .nth(i)
            .ok_or_else(|| "ivm-a1 leaf-swap: stale locator path".to_string())?;
    }
    Ok(cur)
}

/// Swap the Scan node at `locator` with the supplied `IcebergDeltaScanNode`.
/// The caller MUST ensure the delta scan's `output_chunk_schema` matches the
/// original Scan's slot layout — otherwise upstream operators receive wrong
/// columns at runtime.
pub(crate) fn swap_base_scan_with_delta_scan(
    root: &mut ExecNode,
    locator: &ScanLeafLocator,
    delta_node: IcebergDeltaScanNode,
) -> Result<(), String> {
    let target = locate_mut(root, &locator.path)?;
    let scan_meta = match &target.kind {
        ExecNodeKind::Scan(scan) => scan.clone(),
        _ => {
            return Err(
                "ivm-a1 leaf-swap: locator no longer points at a Scan node".to_string(),
            );
        }
    };
    let scan_slots: Vec<_> = scan_meta.output_chunk_schema().slot_ids().to_vec();
    let delta_slots: Vec<_> = delta_node.output_chunk_schema.slot_ids().to_vec();
    if scan_slots != delta_slots {
        return Err(format!(
            "ivm-a1 leaf-swap: schema slot mismatch (scan={scan_slots:?}, delta={delta_slots:?})"
        ));
    }
    target.kind = ExecNodeKind::IcebergDeltaScan(delta_node);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::node::union_all::UnionAllNode;

    fn empty_union_all() -> ExecNode {
        ExecNode {
            kind: ExecNodeKind::UnionAll(UnionAllNode {
                inputs: Vec::new(),
                node_id: 0,
            }),
        }
    }

    #[test]
    fn fail_when_no_scan_leaf() {
        let node = empty_union_all();
        let err = find_unique_base_scan_leaf(&node).unwrap_err();
        assert!(err.contains("no Scan leaf"));
    }
}
