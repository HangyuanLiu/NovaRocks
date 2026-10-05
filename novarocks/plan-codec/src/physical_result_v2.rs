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

//! Complete result vocabulary projection, without Fragment/Package certification.
//! Exact types and namespace loans remain with their original owners. Numerical
//! source/request facts are not a host allocation grant or an opaque time bound.

use crate::{
    borrowed_type_resources::verify_type_binding,
    physical_node_v2::*,
    physical_type_v2::{self, DecodedTypeTable, TypeCodecError},
    physical_value_v2::{DecodedValues, EncodedValues, ValueCodecError},
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::mem::size_of;
type Error = NodeCodecError;
fn type_error(error: TypeCodecError) -> Error {
    ValueCodecError::from(error).into()
}
fn required<T>(
    value: Option<T>,
    text: &'static str,
    w: &mut CompileCheckpoints<'_>,
) -> Result<T, Error> {
    let result = value.ok_or_else(|| invalid(text));
    w.step()?;
    result
}
trait Namespace: Values {
    fn ty<'a>(
        &'a self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a FunctionValueType, Error>;
}
impl Namespace for EncodedValues<'_, '_, '_> {
    fn ty<'a>(
        &'a self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a FunctionValueType, Error> {
        let found = self.value_observed(id, w)?;
        w.step()?;
        found
            .map(|v| &v.ty)
            .ok_or_else(|| invalid("result value reference is unknown"))
    }
}
impl Namespace for DecodedValues<'_, '_, '_> {
    fn ty<'a>(
        &'a self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a FunctionValueType, Error> {
        let found = self.value_observed(id, w)?;
        w.step()?;
        found
            .map(|v| &v.ty)
            .ok_or_else(|| invalid("result value reference is unknown"))
    }
}
fn decoded_type<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    w.flush()?;
    let ty = types.value_type(id);
    w.step()?;
    w.flush()?;
    ty.ok_or_else(|| invalid("result type reference is unknown"))
}
fn remaining(
    model: &Model,
    source: usize,
    values: usize,
    limits: NodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<usize, Error> {
    let facts = model.facts(source, values, limits, w)?;
    limits
        .max_work
        .checked_sub(facts.cumulative_work_upper_bound)
        .ok_or_else(|| CompileControlError::ResourceExhausted.into())
}
fn compare(
    left: &FunctionValueType,
    right: &FunctionValueType,
    model: &mut Model,
    source: usize,
    values: usize,
    limits: NodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let room = remaining(model, source, values, limits, w)?;
    let compared = verify_type_binding(left, right, source, room, w).map_err(type_error)?;
    model.delegated_work = add(model.delegated_work, compared.work_upper_bound())?;
    model.facts(source, values, limits, w)?;
    let matches = compared.matches();
    w.step()?;
    if !matches {
        return Err(invalid("result full type differs from original namespace"));
    }
    Ok(())
}
fn physical_floor(input: &p::ResultPort, ids: &[u32]) -> Result<usize, Error> {
    add(
        size_of::<p::ResultPort>(),
        add(
            bytes::<p::ValueId>(input.output.columns.len())?,
            add(
                bytes::<p::ResultField>(input.fields.len())?,
                bytes::<u32>(ids.len())?,
            )?,
        )?,
    )
}
fn wire_floor(input: &wire::ResultPort, output: &wire::OutputPort) -> Result<usize, Error> {
    add(
        size_of::<wire::ResultPort>(),
        add(
            bytes::<u32>(output.value_ids.capacity())?,
            bytes::<wire::ResultField>(input.fields.capacity())?,
        )?,
    )
}
fn text_request(
    model: &mut Model,
    text: &str,
    copies: usize,
    source: usize,
    values: usize,
    limits: NodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    model.request::<u8>(text.len(), copies)?;
    model.facts(source, values, limits, w)?;
    Ok(())
}
fn preflight_encode(
    input: Option<&p::ResultPort>,
    ids: &[u32],
    values: &EncodedValues<'_, '_, '_>,
    source: usize,
    limits: NodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, Error> {
    let mut model = Model::default();
    let known_namespace = values.retained_floor(w)?;
    let Some(input) = input else {
        let empty = ids.is_empty();
        w.step()?;
        if !empty {
            return Err(invalid("absent result has field type IDs"));
        }
        count_prefix(0, 0, source, known_namespace, limits, w)?;
        return model.facts(source, values.count(), limits, w);
    };
    let mut known = add(known_namespace, physical_floor(input, ids)?)?;
    model.items = add(input.output.columns.len(), input.fields.len())?;
    model.refs = model.items;
    count_prefix(0, model.items, source, known, limits, w)?;
    model.facts(source, values.count(), limits, w)?;
    let same_width = ids.len() == input.fields.len();
    w.step()?;
    if !same_width {
        return Err(invalid("result field type ID count differs"));
    }
    model.request::<u32>(input.output.columns.len(), 1)?;
    model.request::<wire::ResultField>(input.fields.len(), 1)?;
    let roots = values.types().source_counts().0;
    model.delegated_work = add(
        model.delegated_work,
        mul(input.fields.len(), add(roots, 8)?)?,
    )?;
    model.facts(source, values.count(), limits, w)?;
    for field in &input.fields {
        known = add(
            known,
            add(
                field.name.len(),
                field.alias.as_ref().map_or(0, |v| v.len()),
            )?,
        )?;
        floor(source, known, w)?;
        text_request(
            &mut model,
            &field.name,
            1,
            source,
            values.count(),
            limits,
            w,
        )?;
        if let Some(alias) = &field.alias {
            text_request(&mut model, alias, 1, source, values.count(), limits, w)?;
        }
        w.step()?;
    }
    for value in &input.output.columns {
        values.ty(value.get(), w)?;
    }
    for (field, id) in input.fields.iter().zip(ids) {
        let referenced = values
            .types()
            .value_type_observed(*id, w)
            .map_err(type_error)?;
        w.step()?;
        let referenced = referenced.ok_or_else(|| invalid("result type reference is unknown"))?;
        compare(
            &field.ty,
            referenced,
            &mut model,
            source,
            values.count(),
            limits,
            w,
        )?;
        compare(
            &field.ty,
            values.ty(field.value.get(), w)?,
            &mut model,
            source,
            values.count(),
            limits,
            w,
        )?;
        // Direct Dictionary Boxes are owned by this actual ResultField FVT;
        // shared FieldRef children are neither copied nor billed as deep clones.
        // Admit the sole owner's clone-preflight bound before that actual
        // walk. Keep this early ceiling in the final facts for tight replay.
        model.delegated_work = add(
            model.delegated_work,
            physical_type_v2::value_type_clone_preflight_work_upper_bound(),
        )?;
        model.facts(source, values.count(), limits, w)?;
        let clone =
            physical_type_v2::preflight_value_type_clone(&field.ty, w).map_err(type_error)?;
        known = add(known, clone.allocation_request_bytes_upper_bound())?;
        floor(source, known, w)?;
        model.facts(source, values.count(), limits, w)?;
    }
    model.facts(source, values.count(), limits, w)
}
fn preflight_decode(
    input: Option<&wire::ResultPort>,
    values: &DecodedValues<'_, '_, '_>,
    source: usize,
    limits: NodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, Error> {
    let mut model = Model::default();
    let known_namespace = values.retained_floor(w)?;
    let Some(input) = input else {
        count_prefix(0, 0, source, known_namespace, limits, w)?;
        return model.facts(source, values.count(), limits, w);
    };
    required(input.fragment_id, "result fragment ID is absent", w)?;
    let output = required(input.output.as_ref(), "result output port is absent", w)?;
    required(output.node_id, "result output node ID is absent", w)?;
    let mut known = add(known_namespace, wire_floor(input, output)?)?;
    model.items = add(output.value_ids.len(), input.fields.len())?;
    model.refs = model.items;
    count_prefix(0, model.items, source, known, limits, w)?;
    model.request::<p::ValueId>(output.value_ids.len(), 2)?;
    model.request::<p::ResultField>(input.fields.len(), 2)?;
    let lookup = crate::btree_resources_v2::lookup_work(values.types().value_types().len())
        .map_err(invalid)?;
    model.delegated_work = mul(input.fields.len(), mul(lookup, 2)?)?;
    model.facts(source, values.count(), limits, w)?;
    for field in &input.fields {
        known = add(
            known,
            add(
                field.name.capacity(),
                field.alias.as_ref().map_or(0, String::capacity),
            )?,
        )?;
        floor(source, known, w)?;
        text_request(
            &mut model,
            &field.name,
            2,
            source,
            values.count(),
            limits,
            w,
        )?;
        if let Some(alias) = &field.alias {
            text_request(&mut model, alias, 2, source, values.count(), limits, w)?;
        }
        required(field.value_id, "result field value ID is absent", w)?;
        required(field.value_type_id, "result field type ID is absent", w)?;
        w.step()?;
    }
    for value in &output.value_ids {
        values.ty(*value, w)?;
    }
    for field in &input.fields {
        let id = required(field.value_type_id, "result field type ID is absent", w)?;
        let ty = decoded_type(values.types(), id, w)?;
        let value = required(field.value_id, "result field value ID is absent", w)?;
        compare(
            ty,
            values.ty(value, w)?,
            &mut model,
            source,
            values.count(),
            limits,
            w,
        )?;
        // This covers both the original preflight and the later emit clone;
        // the actual loops still observe only their completed work.
        model.delegated_work = add(
            model.delegated_work,
            mul(
                physical_type_v2::value_type_clone_preflight_work_upper_bound(),
                2,
            )?,
        )?;
        model.facts(source, values.count(), limits, w)?;
        let clone = physical_type_v2::preflight_value_type_clone(ty, w).map_err(type_error)?;
        model.requests = add(model.requests, clone.allocation_requests_upper_bound())?;
        model.requested = add(
            model.requested,
            clone.allocation_request_bytes_upper_bound(),
        )?;
        model.facts(source, values.count(), limits, w)?;
    }
    model.facts(source, values.count(), limits, w)
}
pub(crate) fn copy_string(input: &str, w: &mut CompileCheckpoints<'_>) -> Result<String, Error> {
    let mut bytes = reserve::<u8>(input.len(), w)?;
    for byte in input.bytes() {
        bytes.push(byte);
        w.step()?;
    }
    // The library UTF8 check consumes the actual immutable copied extent;
    // its work is bracketed opaque, not synthetic completed-byte callbacks.
    w.flush()?;
    let result = String::from_utf8(bytes).map_err(|_| invalid("copied result text is not UTF8"));
    w.step()?;
    w.flush()?;
    result
}
pub(crate) fn copy_box(input: &str, w: &mut CompileCheckpoints<'_>) -> Result<Box<str>, Error> {
    let copied = copy_string(input, w)?;
    w.flush()?;
    let output = copied.into_boxed_str();
    w.flush()?;
    Ok(output)
}
fn emit_encode(
    input: Option<&p::ResultPort>,
    ids: &[u32],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Option<wire::ResultPort>, Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let mut columns = reserve(input.output.columns.len(), w)?;
    for value in &input.output.columns {
        columns.push(value.get());
        w.step()?;
    }
    let mut fields = reserve(input.fields.len(), w)?;
    for (field, id) in input.fields.iter().zip(ids) {
        let name = copy_string(&field.name, w)?;
        let alias = field
            .alias
            .as_ref()
            .map(|v| copy_string(v, w))
            .transpose()?;
        fields.push(wire::ResultField {
            name,
            alias,
            value_id: Some(field.value.get()),
            value_type_id: Some(*id),
        });
        w.step()?;
    }
    Ok(Some(wire::ResultPort {
        fragment_id: Some(input.fragment.get()),
        output: Some(wire::OutputPort {
            node_id: Some(input.output.node.get()),
            value_ids: columns,
        }),
        fields,
    }))
}
fn emit_decode(
    input: Option<&wire::ResultPort>,
    types: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Option<p::ResultPort>, Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let output = required(input.output.as_ref(), "result output port is absent", w)?;
    let mut columns = reserve(output.value_ids.len(), w)?;
    for value in &output.value_ids {
        columns.push(p::ValueId::new(*value));
        w.step()?;
    }
    let mut fields = reserve(input.fields.len(), w)?;
    for field in &input.fields {
        let ty = decoded_type(
            types,
            required(field.value_type_id, "result field type ID is absent", w)?,
            w,
        )?;
        let ty = physical_type_v2::clone_value_type_observed(ty, w).map_err(type_error)?;
        let name = copy_box(&field.name, w)?;
        let alias = field.alias.as_ref().map(|v| copy_box(v, w)).transpose()?;
        fields.push(p::ResultField {
            name,
            alias,
            value: p::ValueId::new(required(
                field.value_id,
                "result field value ID is absent",
                w,
            )?),
            ty,
        });
        w.step()?;
    }
    Ok(Some(p::ResultPort {
        fragment: p::FragmentId::new(required(
            input.fragment_id,
            "result fragment ID is absent",
            w,
        )?),
        output: p::OutputPort {
            node: p::NodeId::new(required(
                output.node_id,
                "result output node ID is absent",
                w,
            )?),
            columns: boxed(columns, w)?,
        },
        fields: boxed(fields, w)?,
    }))
}
pub(crate) struct PreparedResultEncode<'input, 'values, 'loan, 'source, 'control> {
    input: Option<&'input p::ResultPort>,
    ids: &'input [u32],
    values: &'values EncodedValues<'loan, 'source, 'control>,
    control: &'control dyn PureCompileControl,
    facts: NodeProjectionFacts,
}
impl PreparedResultEncode<'_, '_, '_, '_, '_> {
    pub(crate) fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub(crate) fn emit(self) -> Result<(Option<wire::ResultPort>, NodeProjectionFacts), Error> {
        let mut w = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        // Retain the exact original namespace until the component is published.
        let _values = self.values;
        let result = emit_encode(self.input, self.ids, &mut w).map(|v| (v, self.facts));
        finish(w, result)
    }
}
pub(crate) fn prepare_result_encode<'input, 'values, 'loan, 'source, 'control>(
    input: Option<&'input p::ResultPort>,
    ids: &'input [u32],
    values: &'values EncodedValues<'loan, 'source, 'control>,
    source: usize,
    limits: NodeProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedResultEncode<'input, 'values, 'loan, 'source, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let same = std::ptr::eq(control, values.original_control());
        w.step()?;
        if !same {
            return Err(invalid(
                "result control differs from original value namespace",
            ));
        }
        preflight_encode(input, ids, values, source, limits, &mut w)
    })();
    let facts = finish(w, result)?;
    Ok(PreparedResultEncode {
        input,
        ids,
        values,
        control,
        facts,
    })
}
pub(crate) struct PreparedResultDecode<'input, 'values, 'loan, 'wire, 'control> {
    input: Option<&'input wire::ResultPort>,
    values: &'values DecodedValues<'loan, 'wire, 'control>,
    control: &'control dyn PureCompileControl,
    facts: NodeProjectionFacts,
}
impl PreparedResultDecode<'_, '_, '_, '_, '_> {
    pub(crate) fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub(crate) fn emit(self) -> Result<(Option<p::ResultPort>, NodeProjectionFacts), Error> {
        let mut w = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = emit_decode(self.input, self.values.types(), &mut w).map(|v| (v, self.facts));
        finish(w, result)
    }
}
pub(crate) fn prepare_result_decode<'input, 'values, 'loan, 'wire, 'control>(
    input: Option<&'input wire::ResultPort>,
    values: &'values DecodedValues<'loan, 'wire, 'control>,
    source: usize,
    limits: NodeProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedResultDecode<'input, 'values, 'loan, 'wire, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let same = std::ptr::eq(control, values.original_control());
        w.step()?;
        if !same {
            return Err(invalid(
                "result control differs from original value namespace",
            ));
        }
        preflight_decode(input, values, source, limits, &mut w)
    })();
    let facts = finish(w, result)?;
    Ok(PreparedResultDecode {
        input,
        values,
        control,
        facts,
    })
}
#[cfg(test)]
#[path = "physical_result_v2/tests.rs"]
mod tests;
