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

use super::{
    CallRequestCodecError as E, CallRequestProjectionFacts as Facts,
    CallRequestProjectionLimits as Limits, finish,
};
use crate::{
    allocation_exit_v2::reserve_exit,
    borrowed_type_resources::verify_type_binding,
    physical_type_v2::{DecodedTypeTable, clone_value_type_observed, preflight_value_type_clone},
};
use novarocks_physical_plan::{
    ConstantPolicy, ConstantPoolId, ConstantPools, ConstantReference, ExprId,
    PhysicalCallDefinition, PhysicalCallRequest, PhysicalCallSite, StaticFunctionArgument,
};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, MAX_VALUE_TYPE_DEPTH,
    MAX_VALUE_TYPE_NODES, PureCompileControl,
};
use std::{alloc::Layout, mem::size_of};

/// This token loans the original DTO, checked type roots and admitted pools.
/// Its raw output still requires the original Fragment coverage constructor.
pub struct PreparedCallRequestsDecode<'loan> {
    source: &'loan wire::FragmentCallRequests,
    types: &'loan DecodedTypeTable,
    _pools: &'loan ConstantPools,
    control: &'loan dyn PureCompileControl,
    facts: Facts,
    _keys: Vec<PhysicalCallDefinition>,
}
impl PreparedCallRequestsDecode<'_> {
    pub fn facts(&self) -> &Facts {
        &self.facts
    }
}
fn invalid(message: &'static str) -> E {
    E::InvalidShape(message)
}
fn resource() -> E {
    E::Control(CompileControlError::ResourceExhausted)
}
fn add(a: usize, b: usize) -> Result<usize, E> {
    a.checked_add(b).ok_or_else(resource)
}
fn mul(a: usize, b: usize) -> Result<usize, E> {
    a.checked_mul(b).ok_or_else(resource)
}
fn bytes<T>(n: usize) -> Result<usize, E> {
    Layout::array::<T>(n)
        .map(|l| l.size())
        .map_err(|_| resource())
}
fn gate(value: usize, maximum: usize) -> Result<(), E> {
    if value > maximum {
        Err(resource())
    } else {
        Ok(())
    }
}
// A numerical refusal is already a primary resource cause; never observe a
// later callback which could replace it. Ordinary source-shape outcomes still
// record their completed calculation before the encompassing footer.
fn observed<T>(result: Result<T, E>, w: &mut CompileCheckpoints<'_>) -> Result<T, E> {
    if matches!(&result, Err(E::Control(_))) {
        return result;
    }
    w.step()?;
    result
}
fn source_gate(source: usize, known: usize) -> Result<(), E> {
    if source < known {
        Err(invalid(
            "call request source invoice omits original backing",
        ))
    } else {
        Ok(())
    }
}
fn required<T: Copy>(value: Option<T>, message: &'static str) -> Result<T, E> {
    value.ok_or_else(|| invalid(message))
}
fn definition(input: &wire::OriginalCallRequest) -> Result<PhysicalCallDefinition, E> {
    use wire::call_request_definition::Kind;
    match input.definition.as_ref().and_then(|d| d.kind.as_ref()) {
        Some(Kind::ExpressionDefinitionId(id)) => {
            Ok(PhysicalCallDefinition::Expression(ExprId::new(*id)))
        }
        Some(Kind::Relational(site)) => {
            let site = crate::physical_semantics_v2::decode_call_site(site)?;
            if matches!(site, PhysicalCallSite::Expression(_)) {
                return Err(invalid(
                    "expression use is not a relational request definition",
                ));
            }
            Ok(PhysicalCallDefinition::Relational(site))
        }
        None => Err(invalid("call request definition is absent")),
    }
}
fn policy(input: Option<&wire::SourceConstantPolicy>) -> Result<ConstantPolicy, E> {
    let p = input.ok_or_else(|| invalid("call request constant policy is absent"))?;
    Ok(ConstantPolicy {
        max_rows: required(p.max_rows, "constant policy max_rows is absent")?,
        max_array_nodes: required(
            p.max_array_nodes,
            "constant policy max_array_nodes is absent",
        )?,
        max_logical_elements: required(
            p.max_logical_elements,
            "constant policy max_logical_elements is absent",
        )?,
        max_retained_buffer_bytes: required(
            p.max_retained_buffer_bytes,
            "constant policy max_retained_buffer_bytes is absent",
        )?,
        max_type_depth: required(p.max_type_depth, "constant policy max_type_depth is absent")?,
        max_type_nodes: required(p.max_type_nodes, "constant policy max_type_nodes is absent")?,
        max_dictionary_depth: required(
            p.max_dictionary_depth,
            "constant policy max_dictionary_depth is absent",
        )?,
        max_metadata_bytes: required(
            p.max_metadata_bytes,
            "constant policy max_metadata_bytes is absent",
        )?,
        max_library_validation_work: required(
            p.max_library_validation_work,
            "constant policy max_library_validation_work is absent",
        )?,
        max_library_validation_bytes: required(
            p.max_library_validation_bytes,
            "constant policy max_library_validation_bytes is absent",
        )?,
    })
}
fn reference(input: &wire::ConstantReference) -> Result<ConstantReference, E> {
    Ok(ConstantReference {
        pool: ConstantPoolId::new(required(
            input.pool_id,
            "request constant pool ID is absent",
        )?),
        ordinal: input.row_ordinal,
    })
}
fn value<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, E> {
    w.flush()?;
    let found = types.value_type(id);
    w.step()?;
    w.flush()?;
    found.ok_or_else(|| invalid("request value type ID is absent from the original table"))
}
fn reserve<T>(n: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, E> {
    bytes::<T>(n)?;
    w.flush()?;
    let mut output = Vec::new();
    if n != 0 {
        let result = output.try_reserve_exact(n);
        reserve_exit::<E>(result, w)?;
    }
    Ok(output)
}
fn boxed<T>(output: Vec<T>, w: &mut CompileCheckpoints<'_>) -> Result<Box<[T]>, E> {
    // Conversion can replace an over-capacity Vec allocation. Both requests
    // and their overlap are admitted, independently of allocator usable size.
    w.flush()?;
    let output = output.into_boxed_slice();
    w.step()?;
    w.flush()?;
    Ok(output)
}

struct Model {
    facts: Facts,
    arguments: usize,
    parameters: usize,
    comparisons: usize,
    known: usize,
    maximum_type_backing: usize,
    source: usize,
    types: usize,
    pools: usize,
}
impl Model {
    fn request<T>(&mut self, n: usize, copies: usize) -> Result<(), E> {
        let b = bytes::<T>(n)?;
        self.facts.request_bytes_upper_bound =
            add(self.facts.request_bytes_upper_bound, mul(b, copies)?)?;
        if b != 0 {
            self.facts.allocation_requests_upper_bound =
                add(self.facts.allocation_requests_upper_bound, copies)?;
        }
        Ok(())
    }
    fn admit(&mut self, limits: Limits) -> Result<(), E> {
        // Original checked clone grammar bounds its fixed borrowed scratch and
        // all possible visited owned Dictionary nodes BEFORE that helper runs.
        // This deliberately conservative per-reference envelope is not an
        // invented type cap or a claim about allocator/library cooperation.
        let scratch =
            bytes::<Option<(&arrow::datatypes::DataType, usize)>>(MAX_VALUE_TYPE_DEPTH + 1)?;
        let clone = add(scratch, add(mul(MAX_VALUE_TYPE_NODES, 16)?, 64)?)?;
        let height = (usize::BITS - self.facts.definition_count.leading_zeros()) as usize;
        let index = mul(
            self.facts.definition_count,
            add(64, mul(add(height, 1)?, 32)?)?,
        )?;
        let lookups = add(add(self.types, self.pools)?, 16)?;
        let references = mul(
            self.facts.type_reference_count,
            add(clone, mul(lookups, 4)?)?,
        )?;
        let source_visits = mul(add(self.types, self.pools)?, 32)?;
        let arguments = mul(self.arguments, 64)?;
        let parameters = mul(self.parameters, 16)?;
        let movement = mul(self.facts.request_bytes_upper_bound, 4)?;
        let total = add(index, source_visits)?;
        let total = add(total, add(arguments, parameters)?)?;
        let total = add(total, add(references, self.comparisons)?)?;
        self.facts.cumulative_work_upper_bound = add(128, add(total, movement)?)?;
        self.facts.coexisting_source_and_request_bytes_upper_bound = add(
            add(self.source, size_of::<PreparedCallRequestsDecode<'_>>())?,
            self.facts.request_bytes_upper_bound,
        )?;
        gate(self.facts.definition_count, limits.max_definitions)?;
        gate(self.facts.type_reference_count, limits.max_type_references)?;
        gate(
            self.facts.request_bytes_upper_bound,
            limits.max_request_bytes,
        )?;
        gate(
            self.facts.allocation_requests_upper_bound,
            limits.max_allocation_requests,
        )?;
        gate(
            self.facts.coexisting_source_and_request_bytes_upper_bound,
            limits.max_coexisting_source_and_request_bytes,
        )?;
        gate(self.facts.cumulative_work_upper_bound, limits.max_work)?;
        source_gate(self.source, add(self.known, self.maximum_type_backing)?)
    }
    fn type_reference(
        &mut self,
        ty: &FunctionValueType,
        limits: Limits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        let facts = preflight_value_type_clone(ty, w)?;
        self.facts.allocation_requests_upper_bound = add(
            self.facts.allocation_requests_upper_bound,
            facts.allocation_requests_upper_bound(),
        )?;
        self.facts.request_bytes_upper_bound = add(
            self.facts.request_bytes_upper_bound,
            facts.allocation_request_bytes_upper_bound(),
        )?;
        self.maximum_type_backing = self
            .maximum_type_backing
            .max(facts.allocation_request_bytes_upper_bound());
        // The numerical helper also describes the ensuing sole clone. The
        // admitted grammar envelope above covers its complete own walk.
        observed(self.admit(limits), w)
    }
}
fn preflight(
    source: &wire::FragmentCallRequests,
    types: &DecodedTypeTable,
    pools: &ConstantPools,
    invoice: usize,
    limits: Limits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Facts, E> {
    let n = source.entries.len();
    let type_count = types.value_types().len();
    let pool_count = pools.entries().len();
    let raw = add(
        size_of::<wire::FragmentCallRequests>(),
        bytes::<wire::OriginalCallRequest>(source.entries.capacity())?,
    )?;
    let type_roots = mul(
        type_count,
        add(size_of::<u32>(), size_of::<FunctionValueType>())?,
    )?;
    let type_roots = add(size_of::<DecodedTypeTable>(), type_roots)?;
    let pool_roots = mul(
        pool_count,
        add(
            size_of::<ConstantPoolId>(),
            size_of::<novarocks_physical_plan::ConstantPool>(),
        )?,
    )?;
    let pool_roots = add(size_of::<ConstantPools>(), pool_roots)?;
    let known = add(raw, add(type_roots, pool_roots)?)?;
    let mut model = Model {
        facts: Facts {
            definition_count: n,
            type_reference_count: 0,
            allocation_requests_upper_bound: 0,
            request_bytes_upper_bound: 0,
            coexisting_source_and_request_bytes_upper_bound: 0,
            cumulative_work_upper_bound: 0,
        },
        arguments: 0,
        parameters: 0,
        comparisons: 0,
        known,
        maximum_type_backing: 0,
        source: invoice,
        types: type_count,
        pools: pool_count,
    };
    model.request::<PhysicalCallDefinition>(n, 1)?;
    model.request::<(PhysicalCallDefinition, PhysicalCallRequest)>(n, 1)?;
    observed(model.admit(limits), w)?;
    // Visit the loaned pool namespace once. Maximum individual backing is an
    // alias-safe NECESSARY floor, not a complete host invoice or dedup upper.
    let mut maximum_pool_backing = 0;
    for pool in pools.entries().values() {
        let retained = usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
            .map_err(|_| resource())?;
        maximum_pool_backing = maximum_pool_backing.max(add(
            novarocks_physical_plan::ConstantPool::backing_allocation_layout().size(),
            retained,
        )?);
        observed(
            source_gate(invoice, add(model.known, maximum_pool_backing)?),
            w,
        )?;
    }
    model.known = add(model.known, maximum_pool_backing)?;
    for entry in &source.entries {
        let key = definition(entry);
        w.step()?;
        key?;
        let logical = required(
            entry.logical_argument_count,
            "request logical argument count is absent",
        );
        w.step()?;
        let logical = usize::try_from(logical?).map_err(|_| resource())?;
        let valid = logical <= entry.arguments.len();
        w.step()?;
        if !valid {
            return Err(invalid(
                "request logical argument count exceeds actual channels",
            ));
        }
        let p = policy(entry.constant_policy.as_ref());
        w.step()?;
        p?;
        model.arguments = add(model.arguments, entry.arguments.len())?;
        model.known = add(
            model.known,
            bytes::<wire::OriginalFunctionArgument>(entry.arguments.capacity())?,
        )?;
        model.request::<StaticFunctionArgument>(entry.arguments.len(), 2)?;
        observed(model.admit(limits), w)?;
        for argument in &entry.arguments {
            match argument.kind.as_ref() {
                Some(wire::original_function_argument::Kind::Value(input)) => {
                    model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
                    observed(model.admit(limits), w)?;
                    let id = required(
                        input.value_type_id,
                        "request argument value type ID is absent",
                    );
                    w.step()?;
                    let ty = value(types, id?, w)?;
                    model.type_reference(ty, limits, w)?;
                    if let Some(input) = &input.constant {
                        let address = observed(reference(input), w)?;
                        w.flush()?;
                        let found = pools.resolve_source_observed(address, w).map_err(E::from);
                        let found = observed(found, w);
                        if matches!(&found, Err(E::Control(_))) {
                            return found.map(|_| model.facts);
                        }
                        w.flush()?;
                        let found = found?;
                        let remaining = limits
                            .max_work
                            .checked_sub(model.facts.cumulative_work_upper_bound)
                            .ok_or_else(resource)?;
                        let compared =
                            verify_type_binding(ty, found.value_type(), invoice, remaining, w)?;
                        model.comparisons = add(model.comparisons, compared.work_upper_bound())?;
                        observed(model.admit(limits), w)?;
                        let matches = compared.matches();
                        w.step()?;
                        if !matches {
                            return Err(novarocks_physical_plan::ConstantReferenceError::SourceTypeMismatch(address).into());
                        }
                    }
                }
                Some(wire::original_function_argument::Kind::Lambda(input)) => {
                    let count = add(input.parameter_value_type_ids.len(), 1)?;
                    model.facts.type_reference_count =
                        add(model.facts.type_reference_count, count)?;
                    model.parameters = add(model.parameters, input.parameter_value_type_ids.len())?;
                    model.known = add(
                        model.known,
                        bytes::<u32>(input.parameter_value_type_ids.capacity())?,
                    )?;
                    model.request::<FunctionValueType>(input.parameter_value_type_ids.len(), 2)?;
                    observed(model.admit(limits), w)?;
                    for &id in &input.parameter_value_type_ids {
                        let ty = value(types, id, w)?;
                        model.type_reference(ty, limits, w)?;
                        w.step()?;
                    }
                    let id = required(
                        input.result_value_type_id,
                        "request lambda result type ID is absent",
                    );
                    w.step()?;
                    let ty = value(types, id?, w)?;
                    model.type_reference(ty, limits, w)?;
                }
                None => {
                    w.step()?;
                    return Err(invalid("request argument kind is absent"));
                }
            }
            w.step()?;
        }
        if let Some(id) = entry.expected_result_value_type_id {
            model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
            observed(model.admit(limits), w)?;
            let ty = value(types, id, w)?;
            model.type_reference(ty, limits, w)?;
        }
        w.step()?;
    }
    Ok(model.facts)
}

// Same bounded heap-index geometry as the original BindingIndex, on complete
// definition keys rather than dense/max-ID storage. Each comparison/move is
// observed; there is no uninterruptible std sort or hidden key dictionary.
fn sift(
    keys: &mut [PhysicalCallDefinition],
    mut root: usize,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    loop {
        let left = add(mul(root, 2)?, 1)?;
        w.step()?;
        if left >= keys.len() {
            return Ok(());
        }
        let right = add(left, 1)?;
        let mut child = left;
        if right < keys.len() {
            let greater = keys[right] > keys[left];
            w.step()?;
            if greater {
                child = right;
            }
        }
        let greater = keys[child] > keys[root];
        w.step()?;
        if !greater {
            return Ok(());
        }
        keys.swap(root, child);
        w.step()?;
        root = child;
    }
}
fn index(
    source: &wire::FragmentCallRequests,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<PhysicalCallDefinition>, E> {
    let mut keys = reserve(source.entries.len(), w)?;
    for entry in &source.entries {
        let key = definition(entry);
        w.step()?;
        keys.push(key?);
    }
    for root in (0..keys.len() / 2).rev() {
        sift(&mut keys, root, w)?;
    }
    for end in (1..keys.len()).rev() {
        keys.swap(0, end);
        w.step()?;
        sift(&mut keys[..end], 0, w)?;
    }
    for pair in keys.windows(2) {
        let duplicate = pair[0] == pair[1];
        w.step()?;
        if duplicate {
            return Err(invalid("call request definition is duplicated"));
        }
    }
    Ok(keys)
}
pub fn prepare_call_requests_decode<'loan>(
    source: Option<&'loan wire::FragmentCallRequests>,
    types: &'loan DecodedTypeTable,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: Limits,
    control: &'loan dyn PureCompileControl,
) -> Result<PreparedCallRequestsDecode<'loan>, E> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let source = source.ok_or_else(|| invalid("fragment call request table is absent"))?;
        let facts = preflight(
            source,
            types,
            pools,
            source_retained_bytes,
            limits,
            &mut work,
        )?;
        let keys = index(source, &mut work)?;
        work.step()?;
        Ok(PreparedCallRequestsDecode {
            source,
            types,
            _pools: pools,
            control,
            facts,
            _keys: keys,
        })
    })();
    finish(work, result)
}
pub fn decode_call_requests(
    token: PreparedCallRequestsDecode<'_>,
) -> Result<Vec<(PhysicalCallDefinition, PhysicalCallRequest)>, E> {
    let mut work = CompileCheckpoints::try_new(token.control, CompilePhase::Decode)?;
    let result = (|| {
        let mut output = reserve(token.source.entries.len(), &mut work)?;
        for entry in &token.source.entries {
            let key = definition(entry);
            work.step()?;
            let mut arguments = reserve(entry.arguments.len(), &mut work)?;
            for argument in &entry.arguments {
                let argument = match argument.kind.as_ref() {
                    Some(wire::original_function_argument::Kind::Value(input)) => {
                        let id = required(
                            input.value_type_id,
                            "request argument value type ID is absent",
                        );
                        work.step()?;
                        let ty = clone_value_type_observed(
                            value(token.types, id?, &mut work)?,
                            &mut work,
                        )?;
                        let constant = match &input.constant {
                            Some(input) => {
                                let address = reference(input);
                                work.step()?;
                                Some(address?)
                            }
                            None => None,
                        };
                        StaticFunctionArgument::Value {
                            value_type: ty,
                            constant,
                        }
                    }
                    Some(wire::original_function_argument::Kind::Lambda(input)) => {
                        let mut parameters =
                            reserve(input.parameter_value_type_ids.len(), &mut work)?;
                        for &id in &input.parameter_value_type_ids {
                            parameters.push(clone_value_type_observed(
                                value(token.types, id, &mut work)?,
                                &mut work,
                            )?);
                            work.step()?;
                        }
                        let id = required(
                            input.result_value_type_id,
                            "request lambda result type ID is absent",
                        );
                        work.step()?;
                        let result_type = clone_value_type_observed(
                            value(token.types, id?, &mut work)?,
                            &mut work,
                        )?;
                        StaticFunctionArgument::Lambda {
                            parameter_types: boxed(parameters, &mut work)?,
                            result_type,
                        }
                    }
                    None => {
                        work.step()?;
                        return Err(invalid("request argument kind is absent"));
                    }
                };
                arguments.push(argument);
                work.step()?;
            }
            let expected_result_type = match entry.expected_result_value_type_id {
                Some(id) => Some(clone_value_type_observed(
                    value(token.types, id, &mut work)?,
                    &mut work,
                )?),
                None => None,
            };
            let logical = required(
                entry.logical_argument_count,
                "request logical argument count is absent",
            );
            work.step()?;
            let logical_argument_count = usize::try_from(logical?).map_err(|_| resource())?;
            let constant_policy = policy(entry.constant_policy.as_ref());
            work.step()?;
            output.push((
                key?,
                PhysicalCallRequest {
                    arguments: boxed(arguments, &mut work)?,
                    logical_argument_count,
                    expected_result_type,
                    constant_policy: constant_policy?,
                },
            ));
            work.step()?;
        }
        work.step()?;
        Ok(output)
    })();
    finish(work, result)
}
#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;
