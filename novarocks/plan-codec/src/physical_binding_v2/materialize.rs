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

use super::*;
use crate::{
    allocation_exit_v2::reserve_exit,
    physical_node_v2 as resources,
    physical_type_v2::{
        DecodedTypeTable, clone_value_type_observed, preflight_value_type_clone,
        value_type_clone_preflight_work_upper_bound,
    },
};
use novarocks_type_contract::{
    FunctionArgumentType, FunctionId, FunctionKind, FunctionOverloadId, FunctionValueType,
};
use std::{alloc::Layout, mem::size_of};

type Error = BindingCodecError;
fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn numeric(error: resources::NodeCodecError) -> Error {
    match error {
        resources::NodeCodecError::Control(cause) => Error::Control(cause),
        _ => Error::Control(CompileControlError::ResourceExhausted),
    }
}
pub(crate) fn add(a: usize, b: usize) -> Result<usize, Error> {
    resources::add(a, b).map_err(numeric)
}
pub(crate) fn mul(a: usize, b: usize) -> Result<usize, Error> {
    resources::mul(a, b).map_err(numeric)
}
pub(crate) fn bytes<T>(n: usize) -> Result<usize, Error> {
    resources::bytes::<T>(n).map_err(numeric)
}
pub(crate) fn cap(n: usize, maximum: usize) -> Result<(), Error> {
    if n > maximum {
        Err(CompileControlError::ResourceExhausted.into())
    } else {
        Ok(())
    }
}
pub(crate) fn finish<T>(w: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.finish()?;
    result
}
pub(crate) fn completed<T>(
    result: Result<T, Error>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.step()?;
    result
}

/// One existing Physical signature carrier; this is not a second signature,
/// installed capability, occurrence effect or original-request authority.
#[derive(Debug)]
pub enum MaterializedFunctionBinding {
    Scalar(BoundFunction),
    Table(BoundTableFunction),
}
impl MaterializedFunctionBinding {
    pub fn as_source(&self) -> BindingSource<'_> {
        match self {
            Self::Scalar(value) => BindingSource::Scalar(value),
            Self::Table(value) => BindingSource::Table(value),
        }
    }
}
/// Owned signatures remain associated with their actual receiving namespace.
/// Into-definitions deliberately ends these source loans and grants no proof.
pub struct MaterializedFunctionBindings<'loan, 'source> {
    definitions: Box<[(u32, MaterializedFunctionBinding)]>,
    headers: &'loan PreparedFunctionBindingHeaders<'source>,
    source_invoice: usize,
    retained_bytes: usize,
    facts: BindingProjectionFacts,
}
impl<'loan, 'source> MaterializedFunctionBindings<'loan, 'source> {
    pub fn definitions(&self) -> &[(u32, MaterializedFunctionBinding)] {
        &self.definitions
    }
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
    /// Count-sized source lookup, never a dense allocation based on maximum ID.
    /// The consuming caller admits repeated count work and owns its footer.
    pub fn definition_observed(
        &self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&MaterializedFunctionBinding>, Error> {
        let same = std::ptr::eq(w.control(), self.headers.original_control());
        w.step()?;
        if !same {
            return Err(shape(
                "materialized binding lookup has a different original control",
            ));
        }
        for (candidate, definition) in &self.definitions {
            let matches = *candidate == id;
            w.step()?;
            if matches {
                return Ok(Some(definition));
            }
        }
        Ok(None)
    }
    pub fn into_definitions(self) -> Box<[(u32, MaterializedFunctionBinding)]> {
        self.definitions
    }
    /// The exact original namespace loan, not an equal reconstructed table.
    pub(crate) fn headers(&self) -> &'loan PreparedFunctionBindingHeaders<'source> {
        self.headers
    }
    /// Only this owned output; excludes the already invoiced source namespace.
    pub(crate) fn retained_output_floor(&self) -> Result<usize, Error> {
        add(size_of::<Self>(), self.retained_bytes)
    }
    /// A necessary retained composition floor, not full backing or a MEM grant.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        add(self.source_invoice, self.retained_output_floor()?)
    }
}
/// Preparation allocates no output and keeps all original header/type/control
/// loans through the only consuming materialization operation.
pub struct PreparedFunctionBindingsMaterialization<'loan, 'source> {
    headers: &'loan PreparedFunctionBindingHeaders<'source>,
    source_invoice: usize,
    retained_bytes: usize,
    facts: BindingProjectionFacts,
}
impl PreparedFunctionBindingsMaterialization<'_, '_> {
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
}
pub(crate) struct Model {
    pub(crate) facts: BindingProjectionFacts,
    pub(crate) items: usize,
    lookup: usize,
    source: usize,
    known: usize,
    pub(crate) retained: usize,
    extra_work: usize,
}
impl Model {
    fn new(headers: &PreparedFunctionBindingHeaders<'_>, source: usize) -> Result<Self, Error> {
        Ok(Self::for_composition(
            headers.as_wire().len(),
            add(headers.type_table().value_types().len(), 1)?,
            source,
            headers.retained_invoice_floor()?,
        ))
    }
    /// One cumulative model for an owning composition. The caller supplies the
    /// real original namespace lookup ceiling and necessary source floor.
    pub(crate) fn for_composition(
        definitions: usize,
        lookup_work: usize,
        source: usize,
        known: usize,
    ) -> Self {
        Self {
            facts: BindingProjectionFacts {
                definition_count: definitions,
                type_reference_count: 0,
                allocation_requests_upper_bound: 0,
                request_bytes_upper_bound: 0,
                coexisting_source_and_request_bytes_upper_bound: 0,
                cumulative_work_upper_bound: 0,
            },
            items: 0,
            lookup: lookup_work,
            source,
            known,
            retained: 0,
            extra_work: 0,
        }
    }
    pub(crate) fn request<T>(&mut self, count: usize, times: usize) -> Result<(), Error> {
        let size = bytes::<T>(count)?;
        self.charge_requests(usize::from(size != 0) * times, mul(size, times)?, size)
    }
    /// Temporary requests coexist during construction but are not backing of
    /// the published output. They use the same cumulative admission model.
    pub(crate) fn temporary_request<T>(&mut self, count: usize, times: usize) -> Result<(), Error> {
        let size = bytes::<T>(count)?;
        self.charge_requests(usize::from(size != 0) * times, mul(size, times)?, 0)
    }
    /// Charge actual layout requests without treating their conservative
    /// upper bound as a measured necessary floor of retained private storage.
    pub(crate) fn request_layouts(&mut self, layout: Layout, requests: usize) -> Result<(), Error> {
        self.charge_requests(
            usize::from(layout.size() != 0) * requests,
            mul(layout.size(), requests)?,
            0,
        )
    }
    fn charge_requests(
        &mut self,
        requests: usize,
        bytes: usize,
        retained: usize,
    ) -> Result<(), Error> {
        self.retained = add(self.retained, retained)?;
        self.facts.request_bytes_upper_bound = add(self.facts.request_bytes_upper_bound, bytes)?;
        self.facts.allocation_requests_upper_bound =
            add(self.facts.allocation_requests_upper_bound, requests)?;
        Ok(())
    }
    /// Delegated work is admitted here; this numerical ceiling does not
    /// synthesize observer callbacks or reset the caller's resource budget.
    pub(crate) fn add_work(&mut self, work: usize) -> Result<(), Error> {
        self.extra_work = add(self.extra_work, work)?;
        Ok(())
    }
    pub(crate) fn check(&mut self, limits: BindingProjectionLimits) -> Result<(), Error> {
        cap(self.facts.definition_count, limits.max_definitions)?;
        cap(self.facts.type_reference_count, limits.max_type_references)?;
        cap(
            self.facts.request_bytes_upper_bound,
            limits.max_request_bytes,
        )?;
        cap(
            self.facts.allocation_requests_upper_bound,
            limits.max_allocation_requests,
        )?;
        self.facts.coexisting_source_and_request_bytes_upper_bound =
            add(self.source, self.facts.request_bytes_upper_bound)?;
        cap(
            self.facts.coexisting_source_and_request_bytes_upper_bound,
            limits.max_coexisting_source_and_request_bytes,
        )?;
        // The sole clone grammar's maximum admits its preflight before that
        // bounded traversal. Keep the conservative ceiling in final facts so
        // exact-envelope replay cannot rely on an unreported early ceiling.
        let clone_work = value_type_clone_preflight_work_upper_bound();
        self.facts.cumulative_work_upper_bound = add(
            add(128, mul(32, add(self.items, self.facts.definition_count)?)?)?,
            add(
                mul(
                    self.facts.type_reference_count,
                    add(self.lookup, clone_work)?,
                )?,
                add(
                    mul(self.facts.request_bytes_upper_bound, 4)?,
                    add(self.facts.allocation_requests_upper_bound, self.extra_work)?,
                )?,
            )?,
        )?;
        cap(self.facts.cumulative_work_upper_bound, limits.max_work)?;
        if self.source < self.known {
            return Err(shape(
                "binding materialization source invoice omits original namespace",
            ));
        }
        Ok(())
    }
}
fn value<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    w.flush()?;
    let found = types.value_type(id);
    w.step()?;
    w.flush()?;
    found.ok_or_else(|| shape("materialized binding value type is absent"))
}
fn count_clone(
    types: &DecodedTypeTable,
    id: u32,
    model: &mut Model,
    limits: BindingProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    // Coarse bounded-type work was included before entering the sole grammar.
    model.check(limits)?;
    let source = value(types, id, w)?;
    model.count_owned_type_clone(source, limits, w)
}
impl Model {
    /// Caller admits this occurrence's reference count before visiting the sole
    /// clone grammar. Repeated owned roots are charged once per actual copy.
    pub(crate) fn count_owned_type_clone(
        &mut self,
        source: &FunctionValueType,
        limits: BindingProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.check(limits)?;
        w.flush()?;
        let clone = preflight_value_type_clone(source, w)?;
        w.step()?;
        w.flush()?;
        self.facts.allocation_requests_upper_bound = add(
            self.facts.allocation_requests_upper_bound,
            clone.allocation_requests_upper_bound(),
        )?;
        self.facts.request_bytes_upper_bound = add(
            self.facts.request_bytes_upper_bound,
            clone.allocation_request_bytes_upper_bound(),
        )?;
        self.retained = add(self.retained, clone.allocation_request_bytes_upper_bound())?;
        // The original facts must fit the original shared maximum; no second
        // datatype grammar or recursive measurement is constructed here.
        cap(
            clone.work_upper_bound(),
            value_type_clone_preflight_work_upper_bound(),
        )?;
        self.check(limits)
    }
}

fn preflight(
    headers: &PreparedFunctionBindingHeaders<'_>,
    source: usize,
    limits: BindingProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(BindingProjectionFacts, usize), Error> {
    let mut model = Model::new(headers, source)?;
    model.request::<(u32, MaterializedFunctionBinding)>(headers.as_wire().len(), 2)?;
    model.check(limits)?;
    for definition in headers.as_wire() {
        model.items = add(model.items, definition.arguments.len())?;
        model.request::<FunctionArgumentType>(definition.arguments.len(), 2)?;
        model.request::<u8>(definition.function_id.len(), 1)?;
        model.request::<u8>(definition.overload_id.len(), 1)?;
        model.check(limits)?;
        for argument in &definition.arguments {
            match &argument.kind {
                Some(wire::function_argument_type::Kind::ValueTypeId(_)) => {
                    model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?
                }
                Some(wire::function_argument_type::Kind::Lambda(lambda)) => {
                    model.items = add(model.items, lambda.parameter_value_type_ids.len())?;
                    model.facts.type_reference_count = add(
                        model.facts.type_reference_count,
                        add(lambda.parameter_value_type_ids.len(), 1)?,
                    )?;
                    model.request::<FunctionValueType>(lambda.parameter_value_type_ids.len(), 2)?;
                }
                None => {
                    w.step()?;
                    return Err(shape("materialized binding argument is absent"));
                }
            }
            model.check(limits)?;
            w.step()?;
        }
        match &definition.result {
            Some(wire::function_binding_definition::Result::ScalarValueTypeId(_)) => {
                model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?
            }
            Some(wire::function_binding_definition::Result::Relation(relation)) => {
                model.items = add(model.items, relation.value_type_ids.len())?;
                model.facts.type_reference_count = add(
                    model.facts.type_reference_count,
                    relation.value_type_ids.len(),
                )?;
                model.request::<FunctionValueType>(relation.value_type_ids.len(), 2)?;
            }
            None => {
                w.step()?;
                return Err(shape("materialized binding result is absent"));
            }
        }
        model.check(limits)?;
        w.step()?;
    }
    // Full cumulative own/delegate/output requests precede every clone walk
    // and the first output allocation; all IDs retain original source order.
    for definition in headers.as_wire() {
        for argument in &definition.arguments {
            match &argument.kind {
                Some(wire::function_argument_type::Kind::ValueTypeId(id)) => {
                    count_clone(headers.type_table(), *id, &mut model, limits, w)?
                }
                Some(wire::function_argument_type::Kind::Lambda(lambda)) => {
                    for id in &lambda.parameter_value_type_ids {
                        count_clone(headers.type_table(), *id, &mut model, limits, w)?;
                        w.step()?;
                    }
                    count_clone(
                        headers.type_table(),
                        lambda
                            .result_value_type_id
                            .ok_or_else(|| shape("materialized Lambda result is absent"))?,
                        &mut model,
                        limits,
                        w,
                    )?;
                }
                None => return Err(shape("materialized binding argument changed")),
            }
            w.step()?;
        }
        match &definition.result {
            Some(wire::function_binding_definition::Result::ScalarValueTypeId(id)) => {
                count_clone(headers.type_table(), *id, &mut model, limits, w)?
            }
            Some(wire::function_binding_definition::Result::Relation(relation)) => {
                for id in &relation.value_type_ids {
                    count_clone(headers.type_table(), *id, &mut model, limits, w)?;
                    w.step()?;
                }
            }
            None => return Err(shape("materialized binding result changed")),
        }
        w.step()?;
    }
    Ok((model.facts, model.retained))
}
pub fn prepare_function_bindings_materialization<'loan, 'source>(
    headers: &'loan PreparedFunctionBindingHeaders<'source>,
    source_retained_bytes: usize,
    limits: BindingProjectionLimits,
) -> Result<PreparedFunctionBindingsMaterialization<'loan, 'source>, Error> {
    let mut w = CompileCheckpoints::try_new(headers.original_control(), CompilePhase::Decode)?;
    let result =
        preflight(headers, source_retained_bytes, limits, &mut w).map(|(facts, retained_bytes)| {
            PreparedFunctionBindingsMaterialization {
                headers,
                source_invoice: source_retained_bytes,
                retained_bytes,
                facts,
            }
        });
    finish(w, result)
}
pub(crate) fn reserve<T>(count: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(count)?;
    w.flush()?;
    let mut values = Vec::new();
    reserve_exit::<Error>(values.try_reserve_exact(count), w)?;
    w.step()?;
    w.flush()?;
    Ok(values)
}
pub(crate) fn boxed<T>(values: Vec<T>, w: &mut CompileCheckpoints<'_>) -> Result<Box<[T]>, Error> {
    w.flush()?;
    let values = values.into_boxed_slice();
    w.step()?;
    w.flush()?;
    Ok(values)
}
fn clone_type(
    types: &DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, Error> {
    let source = value(types, id, w)?;
    w.flush()?;
    let value = clone_value_type_observed(source, w)?;
    w.step()?;
    w.flush()?;
    Ok(value)
}
pub fn materialize_function_bindings<'loan, 'source>(
    token: PreparedFunctionBindingsMaterialization<'loan, 'source>,
) -> Result<MaterializedFunctionBindings<'loan, 'source>, Error> {
    let mut w =
        CompileCheckpoints::try_new(token.headers.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let mut definitions = reserve(token.headers.as_wire().len(), &mut w)?;
        for raw in token.headers.as_wire() {
            // These original constructors own exact identity validation/copy.
            // Header bounds guarantee at most 1024 trusted UTF8 bytes each.
            w.flush()?;
            let function = completed(
                FunctionId::try_new(&raw.function_id)
                    .map_err(|_| shape("materialized function identity is invalid")),
                &mut w,
            )?;
            w.flush()?;
            w.flush()?;
            let overload = completed(
                FunctionOverloadId::try_new(&raw.overload_id)
                    .map_err(|_| shape("materialized overload identity is invalid")),
                &mut w,
            )?;
            w.flush()?;
            let mut arguments = reserve(raw.arguments.len(), &mut w)?;
            for argument in &raw.arguments {
                let value = match &argument.kind {
                    Some(wire::function_argument_type::Kind::ValueTypeId(id)) => {
                        FunctionArgumentType::Value(clone_type(
                            token.headers.type_table(),
                            *id,
                            &mut w,
                        )?)
                    }
                    Some(wire::function_argument_type::Kind::Lambda(lambda)) => {
                        let mut parameters =
                            reserve(lambda.parameter_value_type_ids.len(), &mut w)?;
                        for id in &lambda.parameter_value_type_ids {
                            parameters.push(clone_type(token.headers.type_table(), *id, &mut w)?);
                            w.step()?;
                        }
                        FunctionArgumentType::Lambda {
                            parameter_types: boxed(parameters, &mut w)?,
                            result_type: clone_type(
                                token.headers.type_table(),
                                lambda
                                    .result_value_type_id
                                    .ok_or_else(|| shape("materialized Lambda result is absent"))?,
                                &mut w,
                            )?,
                        }
                    }
                    None => return Err(shape("materialized binding argument changed")),
                };
                arguments.push(value);
                w.step()?;
            }
            let arguments = boxed(arguments, &mut w)?;
            let binding = match (&raw.result, wire::FunctionKind::try_from(raw.kind)) {
                (
                    Some(wire::function_binding_definition::Result::ScalarValueTypeId(id)),
                    Ok(kind),
                ) => {
                    let kind = match kind {
                        wire::FunctionKind::Scalar => FunctionKind::Scalar,
                        wire::FunctionKind::Aggregate => FunctionKind::Aggregate,
                        wire::FunctionKind::Window => FunctionKind::Window,
                        _ => return Err(shape("materialized scalar result has a different kind")),
                    };
                    MaterializedFunctionBinding::Scalar(BoundFunction::from_exact_signature(
                        function,
                        overload,
                        kind,
                        arguments,
                        clone_type(token.headers.type_table(), *id, &mut w)?,
                    ))
                }
                (
                    Some(wire::function_binding_definition::Result::Relation(relation)),
                    Ok(wire::FunctionKind::Table),
                ) => {
                    let mut result = reserve(relation.value_type_ids.len(), &mut w)?;
                    for id in &relation.value_type_ids {
                        result.push(clone_type(token.headers.type_table(), *id, &mut w)?);
                        w.step()?;
                    }
                    MaterializedFunctionBinding::Table(BoundTableFunction::from_exact_signature(
                        function,
                        overload,
                        arguments,
                        boxed(result, &mut w)?,
                    ))
                }
                _ => return Err(shape("materialized binding kind and result differ")),
            };
            definitions.push((raw.id, binding));
            w.step()?;
        }
        Ok(MaterializedFunctionBindings {
            definitions: boxed(definitions, &mut w)?,
            headers: token.headers,
            source_invoice: token.source_invoice,
            retained_bytes: token.retained_bytes,
            facts: token.facts,
        })
    })();
    finish(w, result)
}
#[cfg(test)]
#[path = "materialize_tests.rs"]
mod tests;
