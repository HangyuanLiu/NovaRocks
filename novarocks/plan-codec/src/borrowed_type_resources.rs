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

//! Borrowed complete-type comparison admission. The mandatory source invoice
//! covers both original inline roots, Fields, strings and raw metadata backing,
//! including deleted buckets. Scratch is fixed host storage, not a MEM grant.

use crate::physical_type_v2::TypeCodecError;
use arrow::datatypes::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, FunctionValueType, MAX_VALUE_TYPE_NODES,
    NR_LOGICAL_TYPE_KEY, ValueTypeVisit, arrow_data_types_exact_borrowed_observed,
    validate_value_type_structure_with_scratch_observed,
};
use std::mem;

#[derive(Clone, Copy, Debug)]
pub(crate) struct BoundTypeComparisonFacts {
    work_upper_bound: usize,
    flags_match: bool,
}
impl BoundTypeComparisonFacts {
    pub(crate) const fn work_upper_bound(&self) -> usize {
        self.work_upper_bound
    }
    pub(crate) const fn flags_match(&self) -> bool {
        self.flags_match
    }
}

/// One admitted numerical pass and its ensuing exact owner comparison. The
/// caller can accumulate this bound without repeating either operation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct VerifiedTypeBinding {
    facts: BoundTypeComparisonFacts,
    matches: bool,
}
impl VerifiedTypeBinding {
    pub(crate) const fn matches(&self) -> bool {
        self.matches
    }
    pub(crate) const fn work_upper_bound(&self) -> usize {
        self.facts.work_upper_bound()
    }
}

fn shape(message: &'static str) -> TypeCodecError {
    TypeCodecError::InvalidShape(message)
}
fn add(left: usize, right: usize) -> Result<usize, TypeCodecError> {
    left.checked_add(right)
        .ok_or_else(|| shape("borrowed type comparison work sum overflow"))
}
fn mul(left: usize, right: usize) -> Result<usize, TypeCodecError> {
    left.checked_mul(right)
        .ok_or_else(|| shape("borrowed type comparison work product overflow"))
}
fn cap(bound: usize, maximum: usize) -> Result<(), TypeCodecError> {
    if bound > maximum {
        return Err(TypeCodecError::Control(
            CompileControlError::ResourceExhausted,
        ));
    }
    Ok(())
}

#[derive(Default)]
struct Metrics {
    types: usize,
    fields: usize,
    edges: usize,
    entries: usize,
    squared_entries: usize,
    model_visits: usize,
}
impl Metrics {
    fn bound(&self, source: usize, prefix: usize) -> Result<usize, TypeCodecError> {
        // This is the borrowed exact Walk's source model. For equal entry
        // counts K, one left map scan and <=K right scans, repeated key bytes,
        // matched values and names are covered by B*(3K+3) per Field. Unequal
        // counts stop before iteration. Timestamp zones add <=B per type.
        // B is the whole original source, so alias occurrences and deleted
        // buckets remain covered without HashMap.capacity or deduplication.
        let source_visits = add(
            add(mul(3, self.entries)?, mul(3, self.fields)?)?,
            self.types,
        )?;
        let candidates = add(mul(2, self.squared_entries)?, mul(2, self.entries)?)?;
        // Closed node/Field headers include tags, nullability, dictionary
        // attributes, child counts and Union IDs, independently of B.
        let headers = add(
            add(mul(4, self.types)?, mul(8, self.fields)?)?,
            mul(2, self.edges)?,
        )?;
        // Each Field event in the sole grammar is immediately followed by its
        // original fixed-key logical-domain lookup. This preflight includes
        // those opaque bucket/string probes as well as its numerical visits.
        let probes = mul(self.fields, add(source, NR_LOGICAL_TYPE_KEY.len())?)?;
        add(
            add(prefix, self.model_visits)?,
            add(
                add(mul(source, source_visits)?, candidates)?,
                add(headers, probes)?,
            )?,
        )
    }

    fn observe(
        &mut self,
        event: ValueTypeVisit<'_>,
        source: usize,
        prefix: usize,
        maximum: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeCodecError> {
        let opaque_boundary = matches!(
            event,
            ValueTypeVisit::Field(_) | ValueTypeVisit::ChildEdge(_)
        );
        match event {
            ValueTypeVisit::TypeNode(_) => self.types = add(self.types, 1)?,
            ValueTypeVisit::ChildEdge(_) => self.edges = add(self.edges, 1)?,
            ValueTypeVisit::Field(field) => {
                let entries = field.metadata().len();
                self.fields = add(self.fields, 1)?;
                self.entries = add(self.entries, entries)?;
                self.squared_entries = add(self.squared_entries, mul(entries, entries)?)?;
            }
        }
        self.model_visits = add(self.model_visits, 1)?;
        cap(self.bound(source, prefix)?, maximum)?;
        work.step()?;
        if opaque_boundary {
            // Field: entry before the grammar's actual metadata probe.
            // ChildEdge: exit after that completed probe. No predicted-byte
            // loop substitutes for library work or a resource grant.
            work.flush()?;
        }
        Ok(())
    }
}

/// Admit one exact FVT relation. The caller owns entry/ordinary/success finish
/// on this original meter. This only returns numerical facts and flag equality;
/// it does not prove datatype equality or root logical/carrier authorization.
pub(crate) fn preflight_type_binding(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundTypeComparisonFacts, TypeCodecError> {
    work.flush()?;
    // Distinct immutable source roots occupy distinct inline storage, even
    // when their nested Field/type allocations alias. This is only a known
    // floor; it cannot prove the trusted invoice's complete retained backing.
    let roots = if std::ptr::eq(left, right) { 1 } else { 2 };
    let minimum = mul(roots, mem::size_of::<FunctionValueType>())?;
    let source_covers_roots = source_retained_bytes >= minimum;
    work.step()?;
    if !source_covers_roots {
        return Err(shape(
            "borrowed type source invoice omits original inline roots",
        ));
    }
    let flags_match = left.nullable == right.nullable && left.logical_type == right.logical_type;
    work.step()?;
    // The sole FVT equality rule stops on these flags before traversing a
    // datatype. No scratch is initialized and no metadata is inspected then.
    if !flags_match {
        let work_upper_bound = add(source_retained_bytes, 2)?;
        cap(work_upper_bound, max_work)?;
        work.step()?;
        work.flush()?;
        return Ok(BoundTypeComparisonFacts {
            work_upper_bound,
            flags_match,
        });
    }
    let scratch_bytes = mem::size_of::<[Option<(&DataType, usize)>; MAX_VALUE_TYPE_NODES]>();
    let prefix = add(add(source_retained_bytes, scratch_bytes)?, 2)?;
    cap(prefix, max_work)?;
    work.step()?;
    work.flush()?;
    let mut scratch = [None; MAX_VALUE_TYPE_NODES];
    work.flush()?;
    let mut metrics = Metrics::default();
    validate_value_type_structure_with_scratch_observed::<TypeCodecError>(
        &left.data_type,
        &mut scratch,
        |event| metrics.observe(event, source_retained_bytes, prefix, max_work, work),
    )?;
    let work_upper_bound = metrics.bound(source_retained_bytes, prefix)?;
    work.flush()?;
    Ok(BoundTypeComparisonFacts {
        work_upper_bound,
        flags_match,
    })
}

/// Gate once, then call the original borrowed exact datatype author. Even the
/// same immutable FVT pointer is compared; only the original root-flag mismatch
/// rule permits an early false. No source clone, index or heap scratch exists.
pub(crate) fn verify_type_binding(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedTypeBinding, TypeCodecError> {
    let facts = preflight_type_binding(left, right, source_retained_bytes, max_work, work)?;
    if !facts.flags_match() {
        return Ok(VerifiedTypeBinding {
            facts,
            matches: false,
        });
    }
    work.flush()?;
    let compared =
        arrow_data_types_exact_borrowed_observed(&left.data_type, &right.data_type, || {
            work.step().map_err(TypeCodecError::from)
        });
    let compared = match compared {
        Err(TypeCodecError::Control(error)) => return Err(TypeCodecError::Control(error)),
        result => result,
    };
    // Also observe the actual comparison's ordinary refusal exit. Publication
    // and the enclosing caller's ordinary/success finish still occur later.
    work.flush()?;
    Ok(VerifiedTypeBinding {
        facts,
        matches: compared?,
    })
}

#[cfg(test)]
#[path = "borrowed_type_resources/tests.rs"]
mod tests;
