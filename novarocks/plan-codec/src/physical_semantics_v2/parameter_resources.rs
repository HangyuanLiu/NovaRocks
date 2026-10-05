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

//! Bounded receiving materialization from the actual admitted parameter DTO.
//! Request bounds precede stock Box/BTree allocations; real host allocator
//! grants and raw Prost admission remain separate mandatory owners.

use super::{SemanticsCodecError, finish_projection, parameters};
use crate::btree_resources_v2;
use novarocks_proto_models::physical_semantics_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, MAX_SEMANTIC_PARAMETERS, PureCompileControl,
    SemanticParameterError, SemanticParameterId, SemanticParameterValue, SemanticParameters,
};
use std::{alloc::Layout, mem::size_of};

type Error = SemanticsCodecError;

#[derive(Clone, Copy, Debug)]
pub struct ParameterProjectionLimits {
    pub max_parameters: usize,
    pub max_timezone_request_bytes: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParameterProjectionFacts {
    pub parameter_count: usize,
    pub timezone_request_bytes_upper_bound: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}

fn invalid(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("semantic parameter resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("semantic parameter resource product overflow"))
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|l| l.size())
        .map_err(|_| invalid("semantic parameter allocation layout is unrepresentable"))
}
fn cap(value: usize, maximum: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let accepted = value <= maximum;
    w.step()?;
    if accepted {
        Ok(())
    } else {
        Err(invalid("semantic parameter projection envelope exceeded"))
    }
}
fn floor(source: usize, known: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let accepted = source >= known;
    w.step()?;
    if accepted {
        Ok(())
    } else {
        Err(invalid(
            "semantic parameter source invoice omits original backing",
        ))
    }
}
fn preflight(
    input: &wire::SemanticParameters,
    source: usize,
    limits: ParameterProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<ParameterProjectionFacts, Error> {
    let n = input.entries.len();
    // Preserve the original receiving boundary, including its precedence over
    // any per-entry duplicate or malformed-value error.
    let too_many = n > MAX_SEMANTIC_PARAMETERS;
    w.step()?;
    if too_many {
        return Err(SemanticParameterError::TooManyParameters.into());
    }
    cap(n, limits.max_parameters, w)?;
    let mut known = add(
        size_of::<wire::SemanticParameters>(),
        bytes::<wire::SemanticParameter>(input.entries.capacity())?,
    )?;
    floor(source, known, w)?;
    // Rust's insertion-only table has at most n node requests: every created
    // node retains at least one entry, and this owner never removes/rebuilds.
    // The locked layout author covers the actual public key/value layouts.
    let tree = btree_resources_v2::insertion_only::<SemanticParameterId, SemanticParameterValue>(n)
        .map_err(|error| match error {
            btree_resources_v2::BTreeResourceError::SourceModel(message) => invalid(message),
            btree_resources_v2::BTreeResourceError::Arithmetic(_) => {
                invalid("semantic parameter resource product overflow")
            }
        })?;
    let node_bytes = tree.request_bytes_upper_bound;
    // At every visited B=6 level, eight node-layout byte units cover two
    // key/value/edge array moves, both <=12-child parent-link repairs, node
    // initialization and root headers in insert_fit/split/insert_recursing.
    // Charge this for EVERY entry at EVERY possible level, independent of the
    // much smaller actual split count. Search work is separately admitted.
    let own = add(256, add(mul(n, 128)?, tree.cumulative_work_upper_bound)?)?;
    // This prefix admission precedes the first entries walk, not only the
    // eventual output allocations. String lengths/capacities are O(1) facts.
    cap(own, limits.max_work, w)?;
    let mut strings = 0;
    let mut string_bytes = 0;
    for entry in &input.entries {
        if let Some(wire::semantic_parameter::Value::TimeZone(zone)) = &entry.value {
            known = add(known, zone.capacity())?;
            // The sole decoder rejects >255 before cloning. Do not prevalidate
            // empty/control chars or change an earlier constructor error.
            if !zone.is_empty() && zone.len() <= 255 {
                strings = add(strings, 1)?;
                string_bytes = add(string_bytes, bytes::<u8>(zone.len())?)?;
            }
        }
        w.step()?;
    }
    floor(source, known, w)?;
    let requested = add(node_bytes, string_bytes)?;
    let facts = ParameterProjectionFacts {
        parameter_count: n,
        timezone_request_bytes_upper_bound: string_bytes,
        allocation_requests_upper_bound: add(tree.allocation_requests_upper_bound, strings)?,
        allocation_request_bytes_upper_bound: requested,
        coexisting_source_and_request_bytes_upper_bound: add(source, requested)?,
        cumulative_work_upper_bound: add(own, mul(string_bytes, 4)?)?,
    };
    cap(
        facts.timezone_request_bytes_upper_bound,
        limits.max_timezone_request_bytes,
        w,
    )?;
    cap(
        facts.allocation_requests_upper_bound,
        limits.max_allocation_requests,
        w,
    )?;
    cap(
        facts.allocation_request_bytes_upper_bound,
        limits.max_allocation_request_bytes,
        w,
    )?;
    cap(
        facts.coexisting_source_and_request_bytes_upper_bound,
        limits.max_coexisting_source_and_request_bytes,
        w,
    )?;
    cap(facts.cumulative_work_upper_bound, limits.max_work, w)?;
    Ok(facts)
}

/// A source loan frozen after the complete numerical admission. Consumption
/// uses that same original caller control and the original constructor.
pub struct PreparedSemanticParametersDecode<'source, 'control> {
    input: &'source wire::SemanticParameters,
    control: &'control dyn PureCompileControl,
    facts: ParameterProjectionFacts,
}
impl PreparedSemanticParametersDecode<'_, '_> {
    pub fn facts(&self) -> &ParameterProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(SemanticParameters, ParameterProjectionFacts), Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = parameters::decode_parameters_observed(self.input, &mut work)
            .map(|table| (table, self.facts));
        finish_projection(work, result)
    }
}

pub fn prepare_semantic_parameters_decode<'source, 'control>(
    input: &'source wire::SemanticParameters,
    source_retained_bytes: usize,
    limits: ParameterProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedSemanticParametersDecode<'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = preflight(input, source_retained_bytes, limits, &mut work);
    let facts = finish_projection(work, result)?;
    Ok(PreparedSemanticParametersDecode {
        input,
        control,
        facts,
    })
}

pub fn decode_semantic_parameters(
    input: &wire::SemanticParameters,
    source_retained_bytes: usize,
    limits: ParameterProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<(SemanticParameters, ParameterProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let facts = preflight(input, source_retained_bytes, limits, &mut work)?;
        let table = parameters::decode_parameters_observed(input, &mut work)?;
        Ok((table, facts))
    })();
    finish_projection(work, result)
}

fn preflight_encode(
    input: &SemanticParameters,
    source: usize,
    limits: ParameterProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<ParameterProjectionFacts, Error> {
    let n = input.entries().len();
    let too_many = n > MAX_SEMANTIC_PARAMETERS;
    w.step()?;
    if too_many {
        return Err(SemanticParameterError::TooManyParameters.into());
    }
    cap(n, limits.max_parameters, w)?;
    // The actual BTree may have spare slots and headers; these are necessary
    // occupied-entry lower floors, never a claim of exact retained backing.
    let mut known = add(
        size_of::<SemanticParameters>(),
        mul(
            n,
            add(
                size_of::<SemanticParameterId>(),
                size_of::<SemanticParameterValue>(),
            )?,
        )?,
    )?;
    floor(source, known, w)?;
    let array = bytes::<wire::SemanticParameter>(n)?;
    let lookup = btree_resources_v2::lookup_work(n).map_err(invalid)?;
    let own = add(256, add(mul(n, add(64, lookup)?)?, mul(array, 4)?)?)?;
    cap(own, limits.max_work, w)?;
    let mut strings = 0;
    let mut string_bytes = 0;
    // Iteration follows the original immutable table. A pull is an opaque
    // bounded-height BTree operation, not a second index or cloned table.
    let mut entries = input.entries().iter();
    loop {
        w.flush()?;
        let next = entries.next();
        w.step()?;
        w.flush()?;
        let Some((_, value)) = next else {
            break;
        };
        if let SemanticParameterValue::TimeZone(zone) = value {
            known = add(known, zone.len())?;
            // Only calculate copy requests; semantic spelling remains with
            // the original owner and sole encoder grammar.
            if !zone.is_empty() && zone.len() <= 255 {
                strings = add(strings, 1)?;
                string_bytes = add(string_bytes, bytes::<u8>(zone.len())?)?;
            }
        }
        w.step()?;
    }
    floor(source, known, w)?;
    let requested = add(array, string_bytes)?;
    let facts = ParameterProjectionFacts {
        parameter_count: n,
        timezone_request_bytes_upper_bound: string_bytes,
        allocation_requests_upper_bound: add(usize::from(n != 0), strings)?,
        allocation_request_bytes_upper_bound: requested,
        coexisting_source_and_request_bytes_upper_bound: add(source, requested)?,
        // Both the source-count pass and actual emission iterate the table.
        cumulative_work_upper_bound: add(own, add(mul(n, lookup)?, mul(string_bytes, 4)?)?)?,
    };
    cap(
        facts.timezone_request_bytes_upper_bound,
        limits.max_timezone_request_bytes,
        w,
    )?;
    cap(
        facts.allocation_requests_upper_bound,
        limits.max_allocation_requests,
        w,
    )?;
    cap(
        facts.allocation_request_bytes_upper_bound,
        limits.max_allocation_request_bytes,
        w,
    )?;
    cap(
        facts.coexisting_source_and_request_bytes_upper_bound,
        limits.max_coexisting_source_and_request_bytes,
        w,
    )?;
    cap(facts.cumulative_work_upper_bound, limits.max_work, w)?;
    Ok(facts)
}

pub struct PreparedSemanticParametersEncode<'source, 'control> {
    input: &'source SemanticParameters,
    control: &'control dyn PureCompileControl,
    facts: ParameterProjectionFacts,
}
impl PreparedSemanticParametersEncode<'_, '_> {
    pub fn facts(&self) -> &ParameterProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::SemanticParameters, ParameterProjectionFacts), Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = parameters::encode_parameters_observed(self.input, &mut work)
            .map(|wire| (wire, self.facts));
        finish_projection(work, result)
    }
}
pub fn prepare_semantic_parameters_encode<'source, 'control>(
    input: &'source SemanticParameters,
    source_retained_bytes: usize,
    limits: ParameterProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedSemanticParametersEncode<'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = preflight_encode(input, source_retained_bytes, limits, &mut work);
    let facts = finish_projection(work, result)?;
    Ok(PreparedSemanticParametersEncode {
        input,
        control,
        facts,
    })
}
pub fn encode_semantic_parameters(
    input: &SemanticParameters,
    source_retained_bytes: usize,
    limits: ParameterProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<(wire::SemanticParameters, ParameterProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let facts = preflight_encode(input, source_retained_bytes, limits, &mut work)?;
        let wire = parameters::encode_parameters_observed(input, &mut work)?;
        Ok((wire, facts))
    })();
    finish_projection(work, result)
}

#[cfg(test)]
mod tests;
