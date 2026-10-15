// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact finite vectors for wire occurrence indexes. Sparse source IDs never
//! determine allocation sizes; original traversal determines slot numbering.

use super::{NATIVE_V1_MAX_TREE_DEPTH, WireLayout, WireLayoutError as Error, WireSlotId};
use novarocks_physical_plan::{
    Fragment, JoinKind, NodeId, NodeKind, PhysicalNode, TableFunctionOutput, ValueId, ValueOrigin,
};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::alloc::Layout;

pub(super) struct Context<'a, 'c, E> {
    pub request: &'a mut dyn FnMut(Layout) -> Result<(), E>,
    pub work: &'a mut CompileCheckpoints<'c>,
}
impl<E: From<CompileControlError>> Context<'_, '_, E> {
    fn step(&mut self) -> Result<(), E> {
        self.work.step().map_err(Into::into)
    }
    fn vector<T>(&mut self, count: usize) -> Result<Vec<T>, E> {
        let layout =
            Layout::array::<T>(count).map_err(|_| CompileControlError::ResourceExhausted)?;
        if layout.size() != 0 {
            (self.request)(layout)?;
        }
        self.work.flush()?;
        let mut out = Vec::new();
        out.try_reserve_exact(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.step()?;
        Ok(out)
    }
    fn find<T, K: Ord>(&mut self, values: &[(K, T)], key: &K) -> Result<Option<usize>, E> {
        let (mut low, mut high) = (0, values.len());
        while low < high {
            self.step()?;
            let middle = low + (high - low) / 2;
            match values[middle].0.cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(middle)),
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
            }
        }
        Ok(None)
    }
    fn sort<T: Ord>(&mut self, values: &mut [T]) -> Result<(), E> {
        crate::physical_encode::fragment_type_owner::sort_ord_observed(values, self.work)
            .map_err(Into::into)
    }
}
fn add(a: usize, b: usize) -> Result<usize, CompileControlError> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn bad(fragment: &Fragment, node: NodeId, reason: &str) -> Error {
    Error::MechanicalOutputMismatch {
        fragment: fragment.id(),
        node,
        reason: reason.into(),
    }
}
fn ordinal(fragment: &Fragment, node: NodeId, index: usize) -> Result<u32, Error> {
    u32::try_from(index).map_err(|_| {
        Error::OutputOccurrenceSpaceExhausted {
            fragment: fragment.id(),
            node,
        }
        .into()
    })
}
fn slot(fragment: &Fragment, next: &mut i32) -> Result<WireSlotId, Error> {
    if *next <= 0 {
        return Err(Error::SlotSpaceExhausted {
            fragment: fragment.id(),
        }
        .into());
    }
    let slot = WireSlotId(*next);
    *next = next.checked_add(1).ok_or(Error::SlotSpaceExhausted {
        fragment: fragment.id(),
    })?;
    Ok(slot)
}

#[derive(Clone, Copy)]
struct NodeState {
    remaining: usize,
    depth: usize,
    wire_depth: usize,
}
/// One graph body supplies pure expansion checks and the observed constructor.
pub(super) fn depths<E: From<Error> + From<CompileControlError>>(
    fragment: &Fragment,
    c: &mut Context<'_, '_, E>,
) -> Result<Vec<(NodeId, usize)>, E> {
    let mut states = c.vector(fragment.nodes().len())?;
    let mut edge_count = 0usize;
    for (id, node) in fragment.nodes() {
        c.step()?;
        edge_count = add(edge_count, node.inputs.len())?;
        states.push((
            *id,
            NodeState {
                remaining: 0,
                depth: 0,
                wire_depth: 0,
            },
        ));
    }
    let mut edges = c.vector(edge_count)?;
    for (id, node) in fragment.nodes() {
        for input in &node.inputs {
            c.step()?;
            if c.find(&states, input)?.is_none() {
                return Err(Error::MissingInputNode {
                    fragment: fragment.id(),
                    node: *id,
                    input: *input,
                }
                .into());
            }
            edges.push((*input, *id));
        }
    }
    c.sort(&mut edges)?;
    let mut write = 0usize;
    for read in 0..edges.len() {
        c.step()?;
        if write == 0 || edges[read] != edges[write - 1] {
            edges[write] = edges[read];
            write += 1;
        }
    }
    edges.truncate(write);
    for (_, parent) in &edges {
        let index = c
            .find(&states, parent)?
            .ok_or_else(|| bad(fragment, *parent, "dependency node disappeared"))?;
        states[index].1.remaining = add(states[index].1.remaining, 1)?;
    }
    let mut ready = c.vector(states.len())?;
    for (index, (_, state)) in states.iter().enumerate() {
        c.step()?;
        if state.remaining == 0 {
            ready.push(index);
        }
    }
    let mut order = c.vector(states.len())?;
    while let Some(index) = ready.pop() {
        c.step()?;
        let id = states[index].0;
        let node = &fragment.nodes()[&id];
        let mut depth = 1usize;
        for input in &node.inputs {
            let input = c
                .find(&states, input)?
                .ok_or_else(|| bad(fragment, id, "input state disappeared"))?;
            depth = depth.max(add(states[input].1.depth, 1)?);
        }
        if depth > NATIVE_V1_MAX_TREE_DEPTH {
            return Err(Error::TreeDepthExceeded {
                fragment: fragment.id(),
                depth,
                max: NATIVE_V1_MAX_TREE_DEPTH,
            }
            .into());
        }
        states[index].1.depth = depth;
        order.push(index);
        let (mut low, mut high) = (0, edges.len());
        while low < high {
            c.step()?;
            let middle = low + (high - low) / 2;
            if edges[middle].0 < id {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        while low < edges.len() && edges[low].0 == id {
            c.step()?;
            let parent = c
                .find(&states, &edges[low].1)?
                .ok_or_else(|| bad(fragment, id, "dependent state disappeared"))?;
            states[parent].1.remaining -= 1;
            if states[parent].1.remaining == 0 {
                ready.push(parent);
            }
            low += 1;
        }
    }
    if order.len() != states.len() {
        return Err(bad(fragment, fragment.root(), "node graph contains a cycle").into());
    }
    let root = c
        .find(&states, &fragment.root())?
        .ok_or(Error::MissingProjectionNode {
            fragment: fragment.id(),
            node: fragment.root(),
        })?;
    states[root].1.wire_depth = 1;
    for index in order.into_iter().rev() {
        c.step()?;
        let (id, state) = states[index];
        if state.wire_depth == 0 {
            continue;
        }
        for input in &fragment.nodes()[&id].inputs {
            let input = c
                .find(&states, input)?
                .ok_or_else(|| bad(fragment, id, "wire input state disappeared"))?;
            states[input].1.wire_depth = states[input].1.wire_depth.max(add(state.wire_depth, 1)?);
        }
    }
    let mut count = 0usize;
    for (_, state) in &states {
        c.step()?;
        if state.wire_depth != 0 {
            count = add(count, 1)?;
        }
    }
    let mut out = c.vector(count)?;
    for (id, state) in states {
        c.step()?;
        if state.wire_depth != 0 {
            out.push((id, state.wire_depth));
        }
    }
    Ok(out)
}

fn reserved<E: From<Error> + From<CompileControlError>>(
    fragment: &Fragment,
    c: &mut Context<'_, '_, E>,
) -> Result<Vec<(ValueId, WireSlotId)>, E> {
    use novarocks_spi::connector::write_stack::{
        ROOT_WRITE_RESULT_COLUMN_COUNT, WRITE_RELATION_COLUMN_COUNT, root_write_result_column_id,
        write_relation_column_id,
    };
    let mut count = 0usize;
    for node in fragment.nodes().values() {
        c.step()?;
        match &node.kind {
            NodeKind::TableWriter { target } => {
                count = add(
                    count,
                    target
                        .output_schema
                        .fields
                        .len()
                        .min(WRITE_RELATION_COLUMN_COUNT),
                )?
            }
            NodeKind::TableFinish(spec) => {
                count = add(
                    count,
                    spec.input_schema
                        .fields
                        .len()
                        .min(WRITE_RELATION_COLUMN_COUNT),
                )?;
                count = add(
                    count,
                    spec.output_schema
                        .fields
                        .len()
                        .min(ROOT_WRITE_RESULT_COLUMN_COUNT),
                )?;
            }
            _ => {}
        }
    }
    let mut values = c.vector(count)?;
    for node in fragment.nodes().values() {
        c.step()?;
        let mut emit = |schema: &novarocks_physical_plan::WriterRelationSchema,
                        count: usize,
                        id: fn(usize) -> u32,
                        c: &mut Context<'_, '_, E>|
         -> Result<(), E> {
            for (ordinal, field) in schema.fields.iter().take(count).enumerate() {
                c.step()?;
                values.push((
                    field.value,
                    WireSlotId(i32::try_from(id(ordinal)).map_err(|_| {
                        bad(
                            fragment,
                            node.id,
                            "write relation column id exceeds the wire slot space",
                        )
                    })?),
                ));
            }
            Ok(())
        };
        match &node.kind {
            NodeKind::TableWriter { target } => emit(
                &target.output_schema,
                WRITE_RELATION_COLUMN_COUNT,
                write_relation_column_id,
                c,
            )?,
            NodeKind::TableFinish(spec) => {
                emit(
                    &spec.input_schema,
                    WRITE_RELATION_COLUMN_COUNT,
                    write_relation_column_id,
                    c,
                )?;
                emit(
                    &spec.output_schema,
                    ROOT_WRITE_RESULT_COLUMN_COUNT,
                    root_write_result_column_id,
                    c,
                )?;
            }
            _ => {}
        }
    }
    c.sort(&mut values)?;
    let mut write = 0usize;
    for read in 0..values.len() {
        c.step()?;
        if write != 0 && values[read].0 == values[write - 1].0 {
            if values[read].1 != values[write - 1].1 {
                return Err(bad(
                    fragment,
                    fragment.root(),
                    "one value is two different write relation columns",
                )
                .into());
            }
        } else {
            values[write] = values[read];
            write += 1;
        }
    }
    values.truncate(write);
    Ok(values)
}
fn output<E: From<Error> + From<CompileControlError>>(
    fragment: &Fragment,
    node: NodeId,
    ordinal: u32,
    values: &[((NodeId, u32), WireSlotId)],
    c: &mut Context<'_, '_, E>,
) -> Result<WireSlotId, E> {
    let index = c
        .find(values, &(node, ordinal))?
        .ok_or(Error::MissingOutputOccurrence {
            fragment: fragment.id(),
            node,
            occurrence: ordinal,
        })?;
    Ok(values[index].1)
}
fn child<'a>(
    fragment: &'a Fragment,
    node: &PhysicalNode,
    ordinal: usize,
) -> Result<&'a PhysicalNode, Error> {
    let input = node.inputs.get(ordinal).ok_or_else(|| {
        bad(
            fragment,
            node.id,
            "wire output contract requires another child",
        )
    })?;
    fragment
        .nodes()
        .get(input)
        .ok_or(Error::MissingInputNode {
            fragment: fragment.id(),
            node: node.id,
            input: *input,
        })
        .map_err(Into::into)
}
fn same<E: From<Error> + From<CompileControlError>>(
    fragment: &Fragment,
    node: &PhysicalNode,
    index: usize,
    value: ValueId,
    c: &mut Context<'_, '_, E>,
) -> Result<(), E> {
    c.step()?;
    if node.output.columns.get(index) != Some(&value) {
        return Err(bad(
            fragment,
            node.id,
            "final output order differs from the backend v1 mechanical output",
        )
        .into());
    }
    Ok(())
}
fn assigned<E: From<Error> + From<CompileControlError>>(
    fragment: &Fragment,
    node_id: NodeId,
    outputs: &mut [((NodeId, u32), WireSlotId)],
    next: &mut i32,
    visiting: &mut [(NodeId, u8)],
    reserved: &[(ValueId, WireSlotId)],
    c: &mut Context<'_, '_, E>,
) -> Result<(), E> {
    let index = c
        .find(visiting, &node_id)?
        .ok_or(Error::MissingProjectionNode {
            fragment: fragment.id(),
            node: node_id,
        })?;
    if visiting[index].1 == 2 {
        return Ok(());
    }
    if visiting[index].1 == 1 {
        return Err(bad(fragment, node_id, "node graph contains a cycle").into());
    }
    visiting[index].1 = 1;
    let node = &fragment.nodes()[&node_id];
    for input in &node.inputs {
        c.step()?;
        assigned(fragment, *input, outputs, next, visiting, reserved, c)?;
    }
    let mut slots = c.vector(node.output.columns.len())?;
    match &node.kind {
        NodeKind::Filter { .. }
        | NodeKind::Sort { .. }
        | NodeKind::TopN { .. }
        | NodeKind::Limit { .. }
        | NodeKind::AssertOneRow(_) => {
            let base = child(fragment, node, 0)?;
            if base.output.columns.len() != node.output.columns.len() {
                return Err(bad(
                    fragment,
                    node_id,
                    "wire output width differs from final output width",
                )
                .into());
            }
            for (index, value) in base.output.columns.iter().enumerate() {
                same(fragment, node, index, *value, c)?;
                slots.push(output(
                    fragment,
                    base.id,
                    ordinal(fragment, base.id, index)?,
                    outputs,
                    c,
                )?);
            }
        }
        NodeKind::Window(spec) => {
            let base = child(fragment, node, 0)?;
            for (index, value) in base.output.columns.iter().enumerate() {
                same(fragment, node, index, *value, c)?;
                slots.push(output(
                    fragment,
                    base.id,
                    ordinal(fragment, base.id, index)?,
                    outputs,
                    c,
                )?);
            }
            for result in &spec.expressions {
                same(fragment, node, slots.len(), result.output, c)?;
                slots.push(slot(fragment, next)?);
            }
        }
        NodeKind::Repeat {
            grouping_values,
            grouping_outputs,
            ..
        } => {
            let base = child(fragment, node, 0)?;
            let mut replacements = c.vector(grouping_values.len())?;
            for (index, (source, target)) in grouping_values.iter().enumerate() {
                c.step()?;
                replacements.push((*source, index, *target));
            }
            c.sort(&mut replacements)?;
            for (index, value) in base.output.columns.iter().enumerate() {
                let (mut low, mut high) = (0, replacements.len());
                while low < high {
                    c.step()?;
                    let middle = low + (high - low) / 2;
                    if replacements[middle].0 <= *value {
                        low = middle + 1;
                    } else {
                        high = middle;
                    }
                }
                let expected = if low != 0 && replacements[low - 1].0 == *value {
                    replacements[low - 1].2
                } else {
                    *value
                };
                same(fragment, node, index, expected, c)?;
                slots.push(output(
                    fragment,
                    base.id,
                    ordinal(fragment, base.id, index)?,
                    outputs,
                    c,
                )?);
            }
            for result in grouping_outputs {
                same(fragment, node, slots.len(), result.output, c)?;
                slots.push(slot(fragment, next)?);
            }
        }
        NodeKind::TableFunction {
            outputs: columns, ..
        } => {
            let base = child(fragment, node, 0)?;
            for (index, value) in base.output.columns.iter().enumerate() {
                same(fragment, node, index, *value, c)?;
                slots.push(output(
                    fragment,
                    base.id,
                    ordinal(fragment, base.id, index)?,
                    outputs,
                    c,
                )?);
            }
            let mut count = 0usize;
            for column in columns {
                c.step()?;
                if matches!(column, TableFunctionOutput::FunctionResult { .. }) {
                    count = add(count, 1)?;
                }
            }
            let mut results = c.vector(count)?;
            for (index, column) in columns.iter().enumerate() {
                c.step()?;
                if let TableFunctionOutput::FunctionResult {
                    result_ordinal,
                    value,
                } = column
                {
                    results.push((*result_ordinal, index, *value));
                }
            }
            c.sort(&mut results)?;
            for (_, _, value) in results {
                same(fragment, node, slots.len(), value, c)?;
                slots.push(slot(fragment, next)?);
            }
        }
        NodeKind::HashJoin {
            kind,
            null_extended,
            ..
        }
        | NodeKind::NestLoopJoin {
            kind,
            null_extended,
            ..
        } => {
            let left = child(fragment, node, 0)?;
            let right = child(fragment, node, 1)?;
            let (include_left, include_right) = match kind {
                JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
                    (true, false)
                }
                JoinKind::RightSemi | JoinKind::RightAnti => (false, true),
                _ => (true, true),
            };
            let count = add(
                if include_left {
                    left.output.columns.len()
                } else {
                    0
                },
                if include_right {
                    right.output.columns.len()
                } else {
                    0
                },
            )?;
            let mut source = c.vector(count)?;
            for base in [include_left.then_some(left), include_right.then_some(right)]
                .into_iter()
                .flatten()
            {
                for (index, value) in base.output.columns.iter().enumerate() {
                    c.step()?;
                    source.push((
                        *value,
                        source.len(),
                        output(
                            fragment,
                            base.id,
                            ordinal(fragment, base.id, index)?,
                            outputs,
                            c,
                        )?,
                    ));
                }
            }
            c.sort(&mut source)?;
            for (index, entry) in source.iter_mut().enumerate() {
                c.step()?;
                entry.1 = index;
            }
            let mut nulls = c.vector(null_extended.len())?;
            for value in null_extended {
                c.step()?;
                nulls.push((*value, ()));
            }
            c.sort(&mut nulls)?;
            for value in &node.output.columns {
                c.step()?;
                let source_value = if c.find(&nulls, value)?.is_some() {
                    match fragment
                        .values()
                        .get(value)
                        .map(|definition| &definition.origin)
                    {
                        Some(ValueOrigin::NullExtended { of, .. }) => *of,
                        _ => *value,
                    }
                } else {
                    *value
                };
                let (mut low, mut high) = (0, source.len());
                while low < high {
                    c.step()?;
                    let middle = low + (high - low) / 2;
                    if source[middle].0 < source_value {
                        low = middle + 1;
                    } else {
                        high = middle;
                    }
                }
                if low == source.len() || source[low].0 != source_value {
                    return Err(bad(
                        fragment,
                        node_id,
                        "join output value has no remaining v1 source occurrence",
                    )
                    .into());
                }
                let head = source[low].1;
                if head == source.len() || source[head].0 != source_value {
                    return Err(bad(
                        fragment,
                        node_id,
                        "join output value has no remaining v1 source occurrence",
                    )
                    .into());
                }
                slots.push(source[head].2);
                source[low].1 += 1;
            }
        }
        _ => {
            for _ in &node.output.columns {
                c.step()?;
                slots.push(slot(fragment, next)?);
            }
        }
    }
    if slots.len() != node.output.columns.len() {
        return Err(bad(
            fragment,
            node_id,
            "wire output width differs from final output width",
        )
        .into());
    }
    for (index, mut slot) in slots.into_iter().enumerate() {
        c.step()?;
        if let Some(index) = c.find(reserved, &node.output.columns[index])? {
            slot = reserved[index].1;
        }
        let ordinal = ordinal(fragment, node_id, index)?;
        let index =
            c.find(outputs, &(node_id, ordinal))?
                .ok_or(Error::MissingOutputOccurrence {
                    fragment: fragment.id(),
                    node: node_id,
                    occurrence: ordinal,
                })?;
        outputs[index].1 = slot;
    }
    let index = c.find(visiting, &node_id)?.expect("original visiting node");
    visiting[index].1 = 2;
    Ok(())
}

fn produced(node: &PhysicalNode) -> impl Iterator<Item = ValueId> + '_ {
    let spec = match &node.kind {
        NodeKind::TableFinish(spec) => Some(spec),
        _ => None,
    };
    spec.into_iter().flat_map(|spec| {
        spec.final_aggregates.iter().map(|call| call.output).chain(
            spec.grouped_unpivot.iter().flat_map(|unpivot| {
                [
                    unpivot.grouping_output,
                    unpivot.passthrough_output,
                    unpivot.value_output,
                ]
                .into_iter()
                .chain(unpivot.literal_outputs.iter().copied())
            }),
        )
    })
}
fn directory<K: Ord + Copy, E: From<CompileControlError>>(
    mut entries: Vec<(K, usize, WireSlotId)>,
    c: &mut Context<'_, '_, E>,
) -> Result<(Vec<(K, (usize, usize))>, Vec<WireSlotId>), E> {
    c.sort(&mut entries)?;
    let mut groups = 0usize;
    let mut previous = None;
    for (key, _, _) in &entries {
        c.step()?;
        if previous != Some(*key) {
            groups = add(groups, 1)?;
            previous = Some(*key);
        }
    }
    let mut index = c.vector(groups)?;
    let mut backing = c.vector(entries.len())?;
    let mut start = 0;
    while start < entries.len() {
        let key = entries[start].0;
        let mut end = start + 1;
        while end < entries.len() {
            c.step()?;
            if entries[end].0 != key {
                break;
            }
            end += 1;
        }
        index.push((key, (start, end - start)));
        for (_, _, slot) in &entries[start..end] {
            c.step()?;
            backing.push(*slot);
        }
        start = end;
    }
    Ok((index, backing))
}
pub(super) fn construct<E: From<Error> + From<CompileControlError>>(
    fragment: &Fragment,
    c: &mut Context<'_, '_, E>,
) -> Result<WireLayout, E> {
    depths(fragment, c)?;
    let mut output_count = 0usize;
    let mut input_count = 0usize;
    let mut produced_count = 0usize;
    for node in fragment.nodes().values() {
        c.step()?;
        output_count = add(output_count, node.output.columns.len())?;
        for input in &node.inputs {
            c.step()?;
            input_count = add(
                input_count,
                fragment
                    .nodes()
                    .get(input)
                    .ok_or(Error::MissingInputNode {
                        fragment: fragment.id(),
                        node: node.id,
                        input: *input,
                    })?
                    .output
                    .columns
                    .len(),
            )?;
        }
        for _ in produced(node) {
            c.step()?;
            produced_count = add(produced_count, 1)?;
        }
    }
    let mut outputs = c.vector(output_count)?;
    let mut visiting = c.vector(fragment.nodes().len())?;
    let mut published = c.vector(output_count)?;
    for (id, node) in fragment.nodes() {
        c.step()?;
        visiting.push((*id, 0u8));
        for (index, value) in node.output.columns.iter().enumerate() {
            c.step()?;
            outputs.push(((*id, ordinal(fragment, *id, index)?), WireSlotId(0)));
            published.push(((*id, *value), ()));
        }
    }
    c.sort(&mut published)?;
    let reserved = reserved(fragment, c)?;
    let mut next = 1i32;
    for node in fragment.nodes().keys() {
        c.step()?;
        assigned(
            fragment,
            *node,
            &mut outputs,
            &mut next,
            &mut visiting,
            &reserved,
            c,
        )?;
    }
    let mut candidates = c.vector(produced_count)?;
    let mut order = 0usize;
    for node in fragment.nodes().values() {
        for value in produced(node) {
            c.step()?;
            if c.find(&published, &(node.id, value))?.is_none() {
                candidates.push(((node.id, value), order));
            }
            order = add(order, 1)?;
        }
    }
    c.sort(&mut candidates)?;
    let mut write = 0usize;
    for read in 0..candidates.len() {
        c.step()?;
        if write == 0 || candidates[read].0 != candidates[write - 1].0 {
            candidates[write] = candidates[read];
            write += 1;
        }
    }
    candidates.truncate(write);
    let mut ordered = c.vector(candidates.len())?;
    for (key, order) in candidates {
        c.step()?;
        ordered.push((order, key));
    }
    c.sort(&mut ordered)?;
    let mut internal = c.vector(ordered.len())?;
    for (_, key) in ordered {
        c.step()?;
        internal.push((key, slot(fragment, &mut next)?));
    }
    c.sort(&mut internal)?;
    let mut inputs = c.vector(input_count)?;
    let mut edge_inputs = c.vector(input_count)?;
    for node in fragment.nodes().values() {
        for (input_ordinal, input) in node.inputs.iter().enumerate() {
            c.step()?;
            let input_ordinal =
                u32::try_from(input_ordinal).map_err(|_| Error::InputOrdinalSpaceExhausted {
                    fragment: fragment.id(),
                    node: node.id,
                })?;
            for (index, value) in fragment.nodes()[input].output.columns.iter().enumerate() {
                c.step()?;
                let slot = output(
                    fragment,
                    *input,
                    ordinal(fragment, *input, index)?,
                    &outputs,
                    c,
                )?;
                inputs.push(((node.id, *value), inputs.len(), slot));
                edge_inputs.push(((node.id, input_ordinal, *value), edge_inputs.len(), slot));
            }
        }
    }
    let (input_slots, input_backing) = directory(inputs, c)?;
    let (input_edge_slots, input_edge_backing) = directory(edge_inputs, c)?;
    Ok(WireLayout {
        fragment: fragment.id(),
        output_slots: outputs,
        internal_slots: internal,
        input_slots,
        input_backing,
        input_edge_slots,
        input_edge_backing,
    })
}

pub(super) fn project<E: From<Error> + From<CompileControlError>>(
    layout: &WireLayout,
    fragment: &Fragment,
    node: NodeId,
    projection: &[ValueId],
    c: &mut Context<'_, '_, E>,
) -> Result<Vec<WireSlotId>, E> {
    c.step()?;
    if fragment.id() != layout.fragment {
        return Err(Error::FragmentMismatch {
            expected: layout.fragment,
            actual: fragment.id(),
        }
        .into());
    }
    let source = fragment
        .nodes()
        .get(&node)
        .ok_or(Error::MissingProjectionNode {
            fragment: layout.fragment,
            node,
        })?;
    let mut available = c.vector(source.output.columns.len())?;
    for (index, value) in source.output.columns.iter().enumerate() {
        c.step()?;
        available.push((
            *value,
            index,
            output(
                fragment,
                node,
                ordinal(fragment, node, index)?,
                &layout.output_slots,
                c,
            )?,
        ));
    }
    c.sort(&mut available)?;
    for (index, entry) in available.iter_mut().enumerate() {
        c.step()?;
        entry.1 = index;
    }
    let mut target = c.vector(projection.len())?;
    for value in projection {
        let (mut low, mut high) = (0, available.len());
        while low < high {
            c.step()?;
            let middle = low + (high - low) / 2;
            if available[middle].0 < *value {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if low == available.len() || available[low].0 != *value {
            return Err(Error::ProjectionOccurrenceMissing {
                fragment: layout.fragment,
                node,
                value: *value,
            }
            .into());
        }
        let head = available[low].1;
        if head == available.len() || available[head].0 != *value {
            return Err(Error::ProjectionOccurrenceMissing {
                fragment: layout.fragment,
                node,
                value: *value,
            }
            .into());
        }
        target.push(available[head].2);
        available[low].1 += 1;
        c.step()?;
    }
    Ok(target)
}
