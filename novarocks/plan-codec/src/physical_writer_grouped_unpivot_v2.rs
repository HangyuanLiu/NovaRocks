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

//! Writer grouped Unpivot payload with original value/expression/pool loans.
//! The mandatory Fragment writer owner retains grouping, target, type and
//! selected collection policy. This component is not a complete TableFinish.

pub use crate::physical_node_v2::{
    NodeCodecError as WriterGroupedUnpivotCodecError,
    NodeProjectionFacts as WriterGroupedUnpivotProjectionFacts,
    NodeProjectionLimits as WriterGroupedUnpivotProjectionLimits,
};
use crate::{
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::*,
    physical_unpivot_v2::{
        Expressions, address, decode_constant, delegate_counts, encode_constant,
    },
    physical_value_v2::EncodedValues,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
use std::mem::size_of;
type Error = WriterGroupedUnpivotCodecError;

fn prepare_encode(
    input: &p::WriterGroupedUnpivotSpec,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    limits: WriterGroupedUnpivotProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<WriterGroupedUnpivotProjectionFacts, Error> {
    let same = std::ptr::eq(values.types(), expressions.types())
        && std::ptr::eq(values.original_control(), expressions.control());
    work.step()?;
    if !same {
        return Err(invalid(
            "grouped Unpivot namespaces do not retain the same types and control",
        ));
    }
    let mut known = add(
        size_of::<p::WriterGroupedUnpivotSpec>(),
        add(
            bytes::<p::WriteTargetOrdinal>(input.statistics_target_ordinals.len())?,
            add(
                bytes::<p::ValueId>(input.literal_outputs.len())?,
                bytes::<p::WriterGroupedUnpivotMapping>(input.mappings.len())?,
            )?,
        )?,
    )?;
    let mut model = Model {
        items: add(
            input.statistics_target_ordinals.len(),
            add(input.literal_outputs.len(), input.mappings.len())?,
        )?,
        refs: add(4, add(input.literal_outputs.len(), input.mappings.len())?)?,
        ..Model::default()
    };
    count_prefix(0, model.items, source, known, limits, work)?;
    floor(source, values.retained_floor(work)?, work)?;
    floor(source, expressions.floor(work)?, work)?;
    model.request::<u32>(input.statistics_target_ordinals.len(), 1)?;
    model.request::<u32>(input.literal_outputs.len(), 1)?;
    model.request::<wire::WriterGroupedUnpivotMapping>(input.mappings.len(), 1)?;
    let (mut scalars, mut pools) = (0, 0);
    for mapping in &input.mappings {
        model.items = add(model.items, mapping.constants.len())?;
        known = add(known, bytes::<p::UnpivotConstant>(mapping.constants.len())?)?;
        model.request::<wire::UnpivotConstant>(mapping.constants.len(), 1)?;
        count_prefix(0, model.items, source, known, limits, work)?;
        for constant in &mapping.constants {
            match constant {
                p::UnpivotConstant::Scalar(_) => scalars = add(scalars, 1)?,
                _ => pools = add(pools, 1)?,
            }
            work.step()?;
        }
        work.step()?;
    }
    floor(source, known, work)?;
    delegate_counts(&mut model, scalars, pools, expressions)?;
    let facts = model.facts(source, values.count(), limits, work)?;
    for id in [
        input.grouping_input,
        input.grouping_output,
        input.passthrough_output,
        input.value_output,
    ] {
        reference(id.get(), values, work)?;
    }
    for id in &input.literal_outputs {
        reference(id.get(), values, work)?;
    }
    for mapping in &input.mappings {
        reference(mapping.input.get(), values, work)?;
        for constant in &mapping.constants {
            check_constant(constant, expressions, work)?;
        }
    }
    Ok(facts)
}

fn required(id: Option<u32>) -> Result<u32, Error> {
    id.ok_or_else(|| invalid("grouped Unpivot value ID is absent"))
}
fn target_ordinal(raw: u32) -> Result<p::WriteTargetOrdinal, Error> {
    p::WriteTargetOrdinal::try_new(raw).map_err(Error::Identity)
}
fn check_constant(
    constant: &p::UnpivotConstant,
    expressions: &impl Expressions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    match constant {
        p::UnpivotConstant::Scalar(id) => expressions.scalar(id.get(), work),
        p::UnpivotConstant::Int32List(reference) | p::UnpivotConstant::Utf8Map(reference) => {
            address(*reference, expressions, work)
        }
    }
}
fn prepare_decode(
    input: &wire::WriterGroupedUnpivot,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source: usize,
    limits: WriterGroupedUnpivotProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<WriterGroupedUnpivotProjectionFacts, Error> {
    let values = expressions.values();
    // Presence is not an ID-zero default; preserve the ordinary node's order.
    for id in [
        input.grouping_input_value_id,
        input.grouping_output_value_id,
        input.passthrough_output_value_id,
        input.value_output_id,
    ] {
        let result = required(id);
        work.step()?;
        result?;
    }
    let mut known = add(
        size_of::<wire::WriterGroupedUnpivot>(),
        add(
            bytes::<u32>(input.statistics_target_ordinals.capacity())?,
            add(
                bytes::<u32>(input.literal_output_ids.capacity())?,
                bytes::<wire::WriterGroupedUnpivotMapping>(input.mappings.capacity())?,
            )?,
        )?,
    )?;
    let mut model = Model {
        items: add(
            input.statistics_target_ordinals.len(),
            add(input.literal_output_ids.len(), input.mappings.len())?,
        )?,
        refs: add(
            4,
            add(input.literal_output_ids.len(), input.mappings.len())?,
        )?,
        ..Model::default()
    };
    count_prefix(0, model.items, source, known, limits, work)?;
    floor(source, values.retained_floor(work)?, work)?;
    floor(source, expressions.floor(work)?, work)?;
    model.request::<p::WriteTargetOrdinal>(input.statistics_target_ordinals.len(), 2)?;
    model.request::<p::ValueId>(input.literal_output_ids.len(), 2)?;
    model.request::<p::WriterGroupedUnpivotMapping>(input.mappings.len(), 2)?;
    for raw in &input.statistics_target_ordinals {
        let result = target_ordinal(*raw);
        work.step()?;
        result?;
    }
    let (mut scalars, mut pools) = (0, 0);
    for mapping in &input.mappings {
        model.items = add(model.items, mapping.constants.len())?;
        known = add(
            known,
            bytes::<wire::UnpivotConstant>(mapping.constants.capacity())?,
        )?;
        model.request::<p::UnpivotConstant>(mapping.constants.len(), 2)?;
        count_prefix(0, model.items, source, known, limits, work)?;
        let result = target_ordinal(mapping.write_target_ordinal);
        work.step()?;
        result?;
        for constant in &mapping.constants {
            let kind = constant
                .kind
                .as_ref()
                .ok_or_else(|| invalid("grouped Unpivot constant kind is absent"));
            work.step()?;
            match kind? {
                wire::unpivot_constant::Kind::ScalarExprId(_) => scalars = add(scalars, 1)?,
                _ => pools = add(pools, 1)?,
            }
        }
        work.step()?;
    }
    floor(source, known, work)?;
    delegate_counts(&mut model, scalars, pools, expressions)?;
    let facts = model.facts(source, values.count(), limits, work)?;
    for id in [
        input.grouping_input_value_id,
        input.grouping_output_value_id,
        input.passthrough_output_value_id,
        input.value_output_id,
    ] {
        reference(required(id)?, values, work)?;
    }
    for id in &input.literal_output_ids {
        reference(*id, values, work)?;
    }
    for mapping in &input.mappings {
        let id = required(mapping.input_value_id);
        work.step()?;
        reference(id?, values, work)?;
        for constant in &mapping.constants {
            let decoded = decode_constant(constant);
            work.step()?;
            check_constant(&decoded?, expressions, work)?;
        }
    }
    Ok(facts)
}
fn emit_encode(
    input: &p::WriterGroupedUnpivotSpec,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::WriterGroupedUnpivot, Error> {
    let mut statistics_target_ordinals = reserve(input.statistics_target_ordinals.len(), work)?;
    for ordinal in &input.statistics_target_ordinals {
        statistics_target_ordinals.push(ordinal.get());
        work.step()?;
    }
    let literal_output_ids = encode_ids(&input.literal_outputs, work)?;
    let mut mappings = reserve(input.mappings.len(), work)?;
    for mapping in &input.mappings {
        let mut constants = reserve(mapping.constants.len(), work)?;
        for constant in &mapping.constants {
            constants.push(encode_constant(constant));
            work.step()?;
        }
        mappings.push(wire::WriterGroupedUnpivotMapping {
            write_target_ordinal: mapping.write_target_ordinal.get(),
            input_value_id: Some(mapping.input.get()),
            constants,
        });
        work.step()?;
    }
    let output = wire::WriterGroupedUnpivot {
        statistics_target_ordinals,
        grouping_input_value_id: Some(input.grouping_input.get()),
        grouping_output_value_id: Some(input.grouping_output.get()),
        passthrough_output_value_id: Some(input.passthrough_output.get()),
        value_output_id: Some(input.value_output.get()),
        literal_output_ids,
        mappings,
        max_output_rows: input.max_output_rows,
        max_output_bytes: input.max_output_bytes,
    };
    work.step()?;
    Ok(output)
}
fn emit_decode(
    input: &wire::WriterGroupedUnpivot,
    work: &mut CompileCheckpoints<'_>,
) -> Result<p::WriterGroupedUnpivotSpec, Error> {
    let mut statistics_target_ordinals = reserve(input.statistics_target_ordinals.len(), work)?;
    for ordinal in &input.statistics_target_ordinals {
        statistics_target_ordinals.push(target_ordinal(*ordinal)?);
        work.step()?;
    }
    let statistics_target_ordinals = boxed(statistics_target_ordinals, work)?;
    let literal_outputs = decode_ids(&input.literal_output_ids, work)?;
    let mut mappings = reserve(input.mappings.len(), work)?;
    for mapping in &input.mappings {
        let mut constants = reserve(mapping.constants.len(), work)?;
        for constant in &mapping.constants {
            let decoded = decode_constant(constant);
            work.step()?;
            constants.push(decoded?);
        }
        mappings.push(p::WriterGroupedUnpivotMapping {
            write_target_ordinal: target_ordinal(mapping.write_target_ordinal)?,
            input: p::ValueId::new(required(mapping.input_value_id)?),
            constants: boxed(constants, work)?,
        });
        work.step()?;
    }
    let output = p::WriterGroupedUnpivotSpec {
        statistics_target_ordinals,
        grouping_input: p::ValueId::new(required(input.grouping_input_value_id)?),
        grouping_output: p::ValueId::new(required(input.grouping_output_value_id)?),
        passthrough_output: p::ValueId::new(required(input.passthrough_output_value_id)?),
        value_output: p::ValueId::new(required(input.value_output_id)?),
        literal_outputs,
        mappings: boxed(mappings, work)?,
        max_output_rows: input.max_output_rows,
        max_output_bytes: input.max_output_bytes,
    };
    work.step()?;
    Ok(output)
}

/// Both immutable namespace loans remain live through emission. A checked
/// address is not a proof of writer target membership or collection policy.
pub struct PreparedWriterGroupedUnpivotEncode<'input, 'namespace, 'loan, 'source, 'control> {
    input: &'input p::WriterGroupedUnpivotSpec,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    facts: WriterGroupedUnpivotProjectionFacts,
}
impl PreparedWriterGroupedUnpivotEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &WriterGroupedUnpivotProjectionFacts {
        &self.facts
    }
    pub fn emit(
        self,
    ) -> Result<
        (
            wire::WriterGroupedUnpivot,
            WriterGroupedUnpivotProjectionFacts,
        ),
        Error,
    > {
        let mut work =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        debug_assert!(std::ptr::eq(
            self.values.original_control(),
            self.expressions.control()
        ));
        let result = emit_encode(self.input, &mut work).map(|output| (output, self.facts));
        finish(work, result)
    }
}
pub fn prepare_writer_grouped_unpivot_encode<'input, 'namespace, 'loan, 'source, 'control>(
    input: &'input p::WriterGroupedUnpivotSpec,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: WriterGroupedUnpivotProjectionLimits,
) -> Result<PreparedWriterGroupedUnpivotEncode<'input, 'namespace, 'loan, 'source, 'control>, Error>
{
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = prepare_encode(
        input,
        values,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
    );
    let facts = finish(work, result)?;
    Ok(PreparedWriterGroupedUnpivotEncode {
        input,
        values,
        expressions,
        facts,
    })
}

pub struct PreparedWriterGroupedUnpivotDecode<'input, 'namespace, 'loan, 'wire, 'control> {
    input: &'input wire::WriterGroupedUnpivot,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    facts: WriterGroupedUnpivotProjectionFacts,
}
impl PreparedWriterGroupedUnpivotDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &WriterGroupedUnpivotProjectionFacts {
        &self.facts
    }
    pub fn emit(
        self,
    ) -> Result<
        (
            p::WriterGroupedUnpivotSpec,
            WriterGroupedUnpivotProjectionFacts,
        ),
        Error,
    > {
        let mut work =
            CompileCheckpoints::try_new(self.expressions.control(), CompilePhase::Decode)?;
        let result = emit_decode(self.input, &mut work).map(|output| (output, self.facts));
        finish(work, result)
    }
}
pub fn prepare_writer_grouped_unpivot_decode<'input, 'namespace, 'loan, 'wire, 'control>(
    input: &'input wire::WriterGroupedUnpivot,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: WriterGroupedUnpivotProjectionLimits,
) -> Result<PreparedWriterGroupedUnpivotDecode<'input, 'namespace, 'loan, 'wire, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(expressions.control(), CompilePhase::Decode)?;
    let result = prepare_decode(input, expressions, source_retained_bytes, limits, &mut work);
    let facts = finish(work, result)?;
    Ok(PreparedWriterGroupedUnpivotDecode {
        input,
        expressions,
        facts,
    })
}
pub fn encode_writer_grouped_unpivot(
    input: &p::WriterGroupedUnpivotSpec,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: WriterGroupedUnpivotProjectionLimits,
) -> Result<
    (
        wire::WriterGroupedUnpivot,
        WriterGroupedUnpivotProjectionFacts,
    ),
    Error,
> {
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let facts = prepare_encode(
            input,
            values,
            expressions,
            source_retained_bytes,
            limits,
            &mut work,
        )?;
        Ok((emit_encode(input, &mut work)?, facts))
    })();
    finish(work, result)
}
pub fn decode_writer_grouped_unpivot(
    input: &wire::WriterGroupedUnpivot,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: WriterGroupedUnpivotProjectionLimits,
) -> Result<
    (
        p::WriterGroupedUnpivotSpec,
        WriterGroupedUnpivotProjectionFacts,
    ),
    Error,
> {
    let mut work = CompileCheckpoints::try_new(expressions.control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = prepare_decode(input, expressions, source_retained_bytes, limits, &mut work)?;
        Ok((emit_decode(input, &mut work)?, facts))
    })();
    finish(work, result)
}

#[cfg(test)]
mod tests;
