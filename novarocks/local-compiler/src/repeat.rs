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

//! Exact grouping-set projection into the existing local Repeat vocabulary.
//! This is a pure compiler author, not an operator implementation or MEM grant.

use arrow_schema::{DataType, Schema};
use novarocks_local_program::{LayoutCompileError, ProgramNodeId, ProgramNodeKind, StaticLayout};
use novarocks_physical_plan::{
    Fragment, FragmentPackage, GroupingOutput, NodeKind, PhysicalNode, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueTypeError,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::Arc,
};

#[derive(Debug)]
pub(crate) enum RepeatLoweringError {
    Control(CompileControlError),
    ValueType(ValueTypeError),
    Layout(LayoutCompileError),
    Invalid(&'static str),
}
impl From<CompileControlError> for RepeatLoweringError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValueTypeError> for RepeatLoweringError {
    fn from(error: ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}
impl From<LayoutCompileError> for RepeatLoweringError {
    fn from(error: LayoutCompileError) -> Self {
        match error {
            LayoutCompileError::Control(cause) => Self::Control(cause),
            error => Self::Layout(error),
        }
    }
}
impl fmt::Display for RepeatLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::ValueType(error) => error.fmt(f),
            Self::Layout(error) => error.fmt(f),
            Self::Invalid(message) => write!(f, "invalid Repeat lowering: {message}"),
        }
    }
}
impl Error for RepeatLoweringError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::ValueType(error) => Some(error),
            Self::Layout(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}

pub(crate) struct RepeatInputPort<'a> {
    pub node: &'a PhysicalNode,
    pub slots: &'a [SlotId],
    /// Representatives were proved by the already-lowered child author.
    pub representatives: &'a BTreeMap<ValueId, usize>,
}
pub(crate) struct RepeatChannelPlan {
    pub slots: Arc<[SlotId]>,
    pub port: BTreeMap<ValueId, usize>,
}

struct Shape<'a> {
    child: &'a PhysicalNode,
    replacements: BTreeMap<ValueId, ValueId>,
    keys: BTreeSet<ValueId>,
    sets: Vec<BTreeSet<ValueId>>,
    outputs: &'a [GroupingOutput],
}

fn exact_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), RepeatLoweringError> {
    work.flush()?;
    let same = left.exactly_equals_observed::<RepeatLoweringError>(right, || {
        // The shared type walk observes prospective borrowed work; a zero
        // checkpoint does not mislabel it as completed local work.
        work.control()
            .checkpoint(CompilePhase::LowerProgram, 0)
            .map_err(Into::into)
    })?;
    work.flush()?;
    if same {
        Ok(())
    } else {
        Err(RepeatLoweringError::Invalid(
            "Repeat complete value types differ",
        ))
    }
}

fn shape<'a>(
    fragment: &'a Fragment,
    node: &'a PhysicalNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Shape<'a>, RepeatLoweringError> {
    let NodeKind::Repeat {
        rollup_keys,
        grouping_sets,
        grouping_values,
        grouping_outputs,
    } = &node.kind
    else {
        return Err(RepeatLoweringError::Invalid("Repeat source kind differs"));
    };
    if node.inputs.len() != 1 || grouping_sets.is_empty() {
        return Err(RepeatLoweringError::Invalid(
            "Repeat requires one input and nonempty grouping sets",
        ));
    }
    let child = fragment
        .nodes()
        .get(&node.inputs[0])
        .ok_or(RepeatLoweringError::Invalid("missing Repeat child"))?;
    let mut visible = BTreeSet::new();
    for value in &child.output.columns {
        visible.insert(*value);
        work.step()?;
    }
    let mut keys = BTreeSet::new();
    for key in rollup_keys {
        let valid = visible.contains(key) && keys.insert(*key);
        work.step()?;
        if !valid {
            return Err(RepeatLoweringError::Invalid(
                "Repeat rollup keys differ from child values",
            ));
        }
    }
    let mut sets = Vec::new();
    for set in grouping_sets {
        let mut members = BTreeSet::new();
        for key in set {
            let valid = keys.contains(key);
            members.insert(*key);
            work.step()?;
            if !valid {
                return Err(RepeatLoweringError::Invalid(
                    "Repeat grouping set contains a foreign key",
                ));
            }
        }
        sets.push(members);
        work.step()?;
    }
    let mut replacements = BTreeMap::new();
    for &(input, output) in grouping_values {
        let old = replacements.insert(input, output);
        let source = fragment.values().get(&input);
        let target = fragment.values().get(&output);
        work.step()?;
        let (Some(source), Some(target)) = (source, target) else {
            return Err(RepeatLoweringError::Invalid(
                "missing Repeat nullable value",
            ));
        };
        let valid = old.is_none()
            && keys.contains(&input)
            && target.ty.nullable
            && matches!(target.origin, ValueOrigin::NullExtended { node: owner, of } if owner == node.id && of == input);
        work.step()?;
        if !valid {
            return Err(RepeatLoweringError::Invalid(
                "Repeat nullable replacement has inconsistent source",
            ));
        }
        work.flush()?;
        // Full type clones may recursively own dictionary boxes. Their
        // internal work/allocation is an honest opaque boundary here.
        let mut expected = source.ty.clone();
        expected.nullable = true;
        work.flush()?;
        exact_type(&expected, &target.ty, work)?;
    }
    for key in &keys {
        let mut absent = false;
        for set in &sets {
            absent |= !set.contains(key);
            work.step()?;
        }
        let valid = absent == replacements.contains_key(key);
        work.step()?;
        if !valid {
            return Err(RepeatLoweringError::Invalid(
                "Repeat nullable replacements do not cover omitted keys",
            ));
        }
    }
    let width = child
        .output
        .columns
        .len()
        .checked_add(grouping_outputs.len())
        .ok_or(RepeatLoweringError::Invalid("Repeat output width overflow"))?;
    if node.output.columns.len() != width {
        return Err(RepeatLoweringError::Invalid(
            "Repeat output occurrence width differs",
        ));
    }
    for (ordinal, source) in child.output.columns.iter().enumerate() {
        let expected = replacements.get(source).copied().unwrap_or(*source);
        let same = node.output.columns[ordinal] == expected;
        work.step()?;
        if !same {
            return Err(RepeatLoweringError::Invalid(
                "Repeat ordered nullable output differs",
            ));
        }
    }
    for (index, output) in grouping_outputs.iter().enumerate() {
        let ordinal = child.output.columns.len() + index;
        let definition = fragment
            .values()
            .get(&output.output)
            .ok_or(RepeatLoweringError::Invalid("missing GROUPING output"))?;
        let valid = output.arguments.len() <= 63
            && node.output.columns[ordinal] == output.output
            && matches!(definition.origin, ValueOrigin::NodeOutput { node: owner, output_ordinal } if owner == node.id && usize::try_from(output_ordinal).ok() == Some(ordinal));
        work.step()?;
        if !valid {
            return Err(RepeatLoweringError::Invalid(
                "GROUPING output source or argument count differs",
            ));
        }
        exact_type(
            &definition.ty,
            &FunctionValueType::new(DataType::Int64, false),
            work,
        )?;
        for argument in &output.arguments {
            let valid = visible.contains(argument);
            work.step()?;
            if !valid {
                return Err(RepeatLoweringError::Invalid(
                    "GROUPING argument is absent from child",
                ));
            }
        }
    }
    Ok(Shape {
        child,
        replacements,
        keys,
        sets,
        outputs: grouping_outputs,
    })
}

pub(crate) fn plan_repeat_channels(
    fragment: &Fragment,
    node: &PhysicalNode,
    input: RepeatInputPort<'_>,
    next_slot: &mut u64,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RepeatChannelPlan, RepeatLoweringError> {
    let shape = shape(fragment, node, work)?;
    if shape.child.id != input.node.id || input.slots.len() != input.node.output.columns.len() {
        return Err(RepeatLoweringError::Invalid(
            "Repeat input channel shape differs",
        ));
    }
    let mut slots = Vec::new();
    let mut port = BTreeMap::new();
    for (ordinal, (&value, &slot)) in input
        .node
        .output
        .columns
        .iter()
        .zip(input.slots)
        .enumerate()
    {
        let representative =
            input
                .representatives
                .get(&value)
                .copied()
                .ok_or(RepeatLoweringError::Invalid(
                    "Repeat child has no proved representative",
                ))?;
        let same_source = input.node.output.columns.get(representative) == Some(&value);
        let output = shape.replacements.get(&value).copied().unwrap_or(value);
        work.step()?;
        if !same_source {
            return Err(RepeatLoweringError::Invalid(
                "Repeat input representative differs",
            ));
        }
        // A nullable replacement acts on every occurrence of the same proved
        // child value, preserving each occurrence's distinct original slot.
        slots.push(slot);
        port.entry(output).or_insert(ordinal);
        work.step()?;
    }
    for output in shape.outputs {
        let slot = u32::try_from(*next_slot)
            .map_err(|_| RepeatLoweringError::Invalid("Repeat slot identity exhausted"))?;
        *next_slot = next_slot
            .checked_add(1)
            .ok_or(RepeatLoweringError::Invalid(
                "Repeat slot identity exhausted",
            ))?;
        port.insert(output.output, slots.len());
        slots.push(SlotId::new(slot));
        work.step()?;
    }
    work.flush()?;
    let slots = Arc::from(slots);
    work.flush()?;
    Ok(RepeatChannelPlan { slots, port })
}

pub(crate) fn lower_repeat(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    input: (ProgramNodeId, &StaticLayout),
    planned_slots: &[SlotId],
    control: &dyn PureCompileControl,
) -> Result<(ProgramNodeKind, StaticLayout), RepeatLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(package, node, local, input, planned_slots, &mut work);
    if matches!(&result, Err(RepeatLoweringError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn lower_core(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    input: (ProgramNodeId, &StaticLayout),
    planned_slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ProgramNodeKind, StaticLayout), RepeatLoweringError> {
    let fragment = package.fragment();
    let shape = shape(fragment, node, work)?;
    let (child, layout) = input;
    if layout.slots().len() != shape.child.output.columns.len()
        || planned_slots.len() != node.output.columns.len()
    {
        return Err(RepeatLoweringError::Invalid(
            "Repeat materialized channel width differs",
        ));
    }
    let mut result_names = package
        .result()
        .filter(|result| result.output.columns.len() == node.output.columns.len());
    if let Some(result) = result_names {
        for (actual, expected) in node.output.columns.iter().zip(&result.output.columns) {
            let same = actual == expected;
            work.step()?;
            if !same {
                result_names = None;
                break;
            }
        }
    }
    let mut fields = Vec::new();
    let mut original_fields = layout.metadata_materializations().map(|_| Vec::new());
    for (ordinal, value) in shape.child.output.columns.iter().enumerate() {
        let source = fragment
            .values()
            .get(value)
            .ok_or(RepeatLoweringError::Invalid("missing Repeat input value"))?;
        let field = layout
            .schema()
            .fields()
            .get(ordinal)
            .ok_or(RepeatLoweringError::Invalid("missing Repeat input field"))?;
        work.flush()?;
        let actual = FunctionValueType::try_from_field(field)?;
        work.flush()?;
        exact_type(&actual, &source.ty, work)?;
        let same_slot = planned_slots[ordinal] == layout.slots()[ordinal];
        work.step()?;
        if !same_slot {
            return Err(RepeatLoweringError::Invalid(
                "Repeat passthrough slot changed",
            ));
        }
        let output_type = &fragment
            .values()
            .get(&node.output.columns[ordinal])
            .ok_or(RepeatLoweringError::Invalid("missing Repeat output value"))?
            .ty;
        let name = result_names
            .and_then(|result| result.fields.get(ordinal))
            .map(|field| field.alias.as_deref().unwrap_or(&field.name));
        work.flush()?;
        // Preserve the original header/metadata, changing only the authored
        // nullable root and, when exact result binding applies, its label.
        if let Some(original_fields) = original_fields.as_mut() {
            let mut output_field = novarocks_type_contract::owned_resources::metadata_materialization::OriginalFieldMaterialization::clone_from_source(field, layout.metadata_materializations()).with_nullable_owned(output_type.nullable);
            if let Some(name) = name {
                output_field = output_field.with_name_owned(name);
            }
            original_fields.push(output_field);
        } else {
            let mut output_field = field.as_ref().clone().with_nullable(output_type.nullable);
            if let Some(name) = name {
                output_field = output_field.with_name(name);
            }
            fields.push(output_field);
        }
        work.flush()?;
        work.step()?;
    }
    let mut grouping_slot_ids = Vec::new();
    let mut grouping_list = Vec::new();
    for (index, output) in shape.outputs.iter().enumerate() {
        let ordinal = shape.child.output.columns.len() + index;
        let mut values = Vec::new();
        for set in &shape.sets {
            let mut bits = 0_i64;
            for argument in &output.arguments {
                // Ordered GROUPING arguments, last argument in the low bit.
                bits = (bits << 1) | i64::from(!set.contains(argument));
                work.step()?;
            }
            values.push(bits);
            work.step()?;
        }
        grouping_list.push(values);
        grouping_slot_ids.push(planned_slots[ordinal]);
        let ty = &fragment.values()[&output.output].ty;
        work.flush()?;
        let name = if let Some(result) = result_names {
            let field = result
                .fields
                .get(ordinal)
                .ok_or(RepeatLoweringError::Invalid("missing Repeat result field"))?;
            field.alias.as_deref().unwrap_or(&field.name).to_owned()
        } else {
            format!("local_{}_{}", local.index(), ordinal)
        };
        work.flush()?;
        if let Some(original_fields) = original_fields.as_mut() {
            let field = novarocks_type_contract::owned_resources::metadata_materialization::materialize_value_field(ty, name);
            work.flush()?;
            original_fields.push(novarocks_type_contract::owned_resources::metadata_materialization::OriginalFieldMaterialization::Materialized(field?));
        } else {
            let field = ty.try_to_field(name);
            work.flush()?;
            fields.push(field?);
        }
        work.step()?;
    }
    let mut null_slot_ids = Vec::new();
    for set in &shape.sets {
        let mut slots = Vec::new();
        for (ordinal, value) in shape.child.output.columns.iter().enumerate() {
            let omitted = shape.keys.contains(value) && !set.contains(value);
            if omitted {
                slots.push(layout.slots()[ordinal]);
            }
            work.step()?;
        }
        null_slot_ids.push(slots);
        work.step()?;
    }
    work.flush()?;
    let (schema, original_schema) = match (original_fields, layout.metadata_materializations()) {
        (Some(fields), Some(source)) => {
            let inherited = source.field_namespace();
            let namespace = match package.original_metadata_namespace() {
                Some(package_source) => inherited.join_original_observed(package_source, work)?,
                None => inherited,
            };
            let original = novarocks_type_contract::owned_resources::metadata_materialization::TypedSchemaMaterializations::from_original_fields(fields, namespace).into_original_schema();
            (original.schema_owner().schema().clone(), Some(original))
        }
        _ => (Arc::new(Schema::new(fields)), None),
    };
    let slots: Arc<[SlotId]> = Arc::from(planned_slots);
    work.flush()?;
    let output_layout = match original_schema {
        Some(source) => {
            StaticLayout::try_new_materialized_for_compile(source, slots, work.control())?
        }
        None => StaticLayout::try_new_for_compile(schema, slots, work.control())?,
    };
    let kind = ProgramNodeKind::Repeat {
        input: child,
        repeat_times: shape.sets.len(),
        null_slot_ids,
        grouping_slot_ids,
        grouping_list,
    };
    Ok((kind, output_layout))
}
