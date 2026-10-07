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

//! Lower one admitted stream receiver into the existing ExchangeSource owner.
//! The receiver's layout ordinal `i` is the cut's import `i`; the address is
//! taken from the exact inbound cut, never from a legacy native node ID.

use crate::{assert_rows::reserve_vec, lowering::FragmentCompileError};
use arrow_schema::{Field, Schema};
use novarocks_local_program::{CompiledExchangeInput, ProgramNodeKind, StaticLayout};
use novarocks_physical_plan::{EdgeKind, FragmentPackage, NodeKind, PhysicalNode};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use novarocks_types::SlotId;
use std::{sync::Arc, time::Duration};

pub(crate) struct LoweredExchangeSource {
    pub kind: ProgramNodeKind,
    pub layout: StaticLayout,
    pub input: CompiledExchangeInput,
}

pub(crate) fn lower_exchange_source(
    package: &FragmentPackage,
    node: &PhysicalNode,
    slots: &[SlotId],
    exchange_wait: Duration,
    control: &dyn PureCompileControl,
) -> Result<LoweredExchangeSource, FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(package, node, slots, exchange_wait, &mut work);
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn lower_core(
    package: &FragmentPackage,
    node: &PhysicalNode,
    slots: &[SlotId],
    exchange_wait: Duration,
    work: &mut CompileCheckpoints<'_>,
) -> Result<LoweredExchangeSource, FragmentCompileError> {
    let NodeKind::ExchangeSource { edge, imports } = &node.kind else {
        return Err(FragmentCompileError::Invalid("ExchangeSource kind differs"));
    };
    if !node.inputs.is_empty()
        || slots.len() != node.output.columns.len()
        || imports.len() != node.output.columns.len()
    {
        return Err(FragmentCompileError::Invalid(
            "ExchangeSource input, import or channel width differs",
        ));
    }
    let mut found = None;
    for cut in package.cuts().inbound.iter() {
        let addressed = cut.destination_node == node.id;
        work.step()?;
        if addressed && found.replace(cut).is_some() {
            return Err(FragmentCompileError::Invalid(
                "ExchangeSource has more than one inbound cut",
            ));
        }
    }
    let cut = found.ok_or(FragmentCompileError::Invalid(
        "ExchangeSource has no inbound cut",
    ))?;
    if cut.edge != *edge {
        return Err(FragmentCompileError::Invalid(
            "ExchangeSource edge differs from its inbound cut",
        ));
    }
    if cut.kind != EdgeKind::Stream || cut.change_stream_writer.is_some() {
        return Err(FragmentCompileError::Unsupported {
            node: Some(node.id),
            feature: "inbound CTE or change-stream edge",
        });
    }
    // A writer-result receiver presents the writer relation positionally,
    // named by its frozen relation fields; the finish admission proved that
    // it feeds exactly one finish.
    let relation = cut.writer_result.as_ref().map(|relation| &relation.fields);
    if relation.is_some_and(|fields| fields.len() != node.output.columns.len()) {
        return Err(FragmentCompileError::Invalid(
            "writer-result cut width differs from its receiver",
        ));
    }
    // Receive binding is positional: wire column `i` is the cut's import `i`,
    // and the received occurrence `i` is that import's destination value.
    if cut.imports.len() != imports.len() {
        return Err(FragmentCompileError::Invalid(
            "ExchangeSource imports differ from its inbound cut",
        ));
    }
    for ((import, (source, destination)), output) in cut
        .imports
        .iter()
        .zip(imports.iter())
        .zip(node.output.columns.iter())
    {
        let exact = import.source.value == *source
            && import.destination == *destination
            && destination == output;
        work.step()?;
        if !exact {
            return Err(FragmentCompileError::Invalid(
                "ExchangeSource occurrence differs from its inbound cut import",
            ));
        }
    }
    let result = package
        .result()
        .filter(|result| result.output.columns == node.output.columns);
    let mut fields: Vec<Field> = Vec::new();
    reserve_vec(&mut fields, slots.len(), work)?;
    for (ordinal, value) in node.output.columns.iter().enumerate() {
        let ty = &package
            .fragment()
            .values()
            .get(value)
            .ok_or(FragmentCompileError::Invalid(
                "missing ExchangeSource output type",
            ))?
            .ty;
        // Full result labels are authoritative only when this receiver's
        // entire ordered output is the result port's: a Sort, TopN, Filter or
        // Limit root publishes the layout it reads unchanged.
        let name = match (relation, result) {
            (Some(fields), _) => {
                let field = &fields[ordinal];
                let same = field.destination == *value && field.ty == *ty;
                work.step()?;
                if !same {
                    return Err(FragmentCompileError::Invalid(
                        "writer-result cut field differs from its received occurrence",
                    ));
                }
                field.name.to_string()
            }
            (None, Some(result)) => {
                let field = result
                    .fields
                    .get(ordinal)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing ExchangeSource result label",
                    ))?;
                field.alias.as_deref().unwrap_or(&field.name).to_owned()
            }
            (None, None) => format!("local_exchange_{}_{}", node.id.get(), ordinal),
        };
        work.flush()?;
        let field = ty.try_to_field(name);
        work.flush()?;
        fields.push(field?);
        work.step()?;
    }
    work.flush()?;
    let layout = StaticLayout::try_new_for_compile(
        Arc::new(Schema::new(fields)),
        Arc::from(slots),
        work.control(),
    )?;
    work.flush()?;
    Ok(LoweredExchangeSource {
        kind: ProgramNodeKind::ExchangeSource {
            timeout: exchange_wait,
            runtime_filters: Vec::new(),
            hash_partition_exprs: Vec::new(),
        },
        layout,
        input: CompiledExchangeInput {
            receiver_node: node.id.get(),
            edge: edge.get(),
            source_fragment: cut.source_fragment.get(),
        },
    })
}
