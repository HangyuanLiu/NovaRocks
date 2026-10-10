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

#[derive(Clone, Copy, Default)]
enum Arithmetic {
    #[default]
    Plain,
    Parent,
}
impl Arithmetic {
    fn add(self, left: usize, right: usize) -> Result<usize, TypeCodecError> {
        match self {
            Self::Plain => add(left, right),
            Self::Parent => left.checked_add(right).ok_or(TypeCodecError::Control(
                CompileControlError::ResourceExhausted,
            )),
        }
    }
    fn mul(self, left: usize, right: usize) -> Result<usize, TypeCodecError> {
        match self {
            Self::Plain => mul(left, right),
            Self::Parent => left.checked_mul(right).ok_or(TypeCodecError::Control(
                CompileControlError::ResourceExhausted,
            )),
        }
    }
}

#[derive(Default)]
struct Metrics {
    arithmetic: Arithmetic,
    types: usize,
    fields: usize,
    edges: usize,
    entries: usize,
    squared_entries: usize,
    model_visits: usize,
}
impl Metrics {
    fn bound(&self, source: usize, prefix: usize) -> Result<usize, TypeCodecError> {
        let arithmetic = self.arithmetic;
        // This is the borrowed exact Walk's source model. For equal entry
        // counts K, one left map scan and <=K right scans, repeated key bytes,
        // matched values and names are covered by B*(3K+3) per Field. Unequal
        // counts stop before iteration. Timestamp zones add <=B per type.
        // B is the whole original source, so alias occurrences and deleted
        // buckets remain covered without HashMap.capacity or deduplication.
        let source_visits = arithmetic.add(
            arithmetic.add(
                arithmetic.mul(3, self.entries)?,
                arithmetic.mul(3, self.fields)?,
            )?,
            self.types,
        )?;
        let candidates = arithmetic.add(
            arithmetic.mul(2, self.squared_entries)?,
            arithmetic.mul(2, self.entries)?,
        )?;
        // Closed node/Field headers include tags, nullability, dictionary
        // attributes, child counts and Union IDs, independently of B.
        let headers = arithmetic.add(
            arithmetic.add(
                arithmetic.mul(4, self.types)?,
                arithmetic.mul(8, self.fields)?,
            )?,
            arithmetic.mul(2, self.edges)?,
        )?;
        // Each Field event in the sole grammar is immediately followed by its
        // original fixed-key logical-domain lookup. This preflight includes
        // those opaque bucket/string probes as well as its numerical visits.
        let probes = arithmetic.mul(
            self.fields,
            arithmetic.add(source, NR_LOGICAL_TYPE_KEY.len())?,
        )?;
        arithmetic.add(
            arithmetic.add(prefix, self.model_visits)?,
            arithmetic.add(
                arithmetic.add(arithmetic.mul(source, source_visits)?, candidates)?,
                arithmetic.add(headers, probes)?,
            )?,
        )
    }

    fn observe<E: From<TypeCodecError>>(
        &mut self,
        event: ValueTypeVisit<'_>,
        source: usize,
        prefix: usize,
        maximum: usize,
        admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        let arithmetic = self.arithmetic;
        let opaque_boundary = matches!(
            event,
            ValueTypeVisit::Field(_) | ValueTypeVisit::ChildEdge(_)
        );
        match event {
            ValueTypeVisit::TypeNode(_) => self.types = arithmetic.add(self.types, 1)?,
            ValueTypeVisit::ChildEdge(_) => self.edges = arithmetic.add(self.edges, 1)?,
            ValueTypeVisit::Field(field) => {
                let entries = field.metadata().len();
                self.fields = arithmetic.add(self.fields, 1)?;
                self.entries = arithmetic.add(self.entries, entries)?;
                self.squared_entries =
                    arithmetic.add(self.squared_entries, arithmetic.mul(entries, entries)?)?;
            }
        }
        self.model_visits = arithmetic.add(self.model_visits, 1)?;
        let work_upper_bound = self.bound(source, prefix)?;
        cap(work_upper_bound, maximum)?;
        admit(BoundTypeComparisonFacts {
            work_upper_bound,
            flags_match: true,
        })?;
        work.step().map_err(TypeCodecError::from)?;
        if opaque_boundary {
            // Field: entry before the grammar's actual metadata probe.
            // ChildEdge: exit after that completed probe. No predicted-byte
            // loop substitutes for library work or a resource grant.
            work.flush().map_err(TypeCodecError::from)?;
        }
        Ok(())
    }
}

/// The sole closed-root prefix bound, without observations or scratch setup.
/// This does not prove datatype equality or authorize a logical/carrier domain.
pub(crate) fn type_binding_prefix_work_upper_bound(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
) -> Result<BoundTypeComparisonFacts, TypeCodecError> {
    type_binding_prefix_core(left, right, source_retained_bytes, Arithmetic::Plain)
}

/// The same root arithmetic under caller-owned numerical admission.
pub(crate) fn type_binding_prefix_work_upper_bound_in(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
) -> Result<BoundTypeComparisonFacts, TypeCodecError> {
    type_binding_prefix_core(left, right, source_retained_bytes, Arithmetic::Parent)
}

fn type_binding_prefix_core(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    arithmetic: Arithmetic,
) -> Result<BoundTypeComparisonFacts, TypeCodecError> {
    // Only closed root flags are inspected. No datatype walk, metadata probe
    // or scratch initialization precedes this numerical admission.
    let flags_match = left.nullable == right.nullable && left.logical_type == right.logical_type;
    let scratch_bytes = if flags_match {
        mem::size_of::<[Option<(&DataType, usize)>; MAX_VALUE_TYPE_NODES]>()
    } else {
        0
    };
    Ok(BoundTypeComparisonFacts {
        work_upper_bound: arithmetic
            .add(arithmetic.add(source_retained_bytes, scratch_bytes)?, 2)?,
        flags_match,
    })
}

enum ComparisonWalkError<E> {
    Grammar(TypeCodecError),
    Admission(E),
}
impl<E> From<novarocks_type_contract::ValueTypeError> for ComparisonWalkError<E> {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Grammar(error.into())
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
    preflight_type_binding_admitted(
        left,
        right,
        source_retained_bytes,
        max_work,
        &mut |_| Ok(()),
        work,
    )
}

/// Expose the sole comparison model before its next observation or opaque
/// metadata probe. The caller accumulates actual prefixes, never a guessed B multiplier.
pub(crate) fn preflight_type_binding_admitted<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: usize,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundTypeComparisonFacts, E> {
    preflight_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        max_work,
        admit,
        work,
        Arithmetic::Plain,
    )
}

pub(crate) fn preflight_type_binding_admitted_in<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: usize,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundTypeComparisonFacts, E> {
    preflight_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        max_work,
        admit,
        work,
        Arithmetic::Parent,
    )
}

fn preflight_type_binding_admitted_core<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: usize,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
    arithmetic: Arithmetic,
) -> Result<BoundTypeComparisonFacts, E> {
    // Distinct immutable source roots occupy distinct inline storage, even
    // when their nested Field/type allocations alias. This is only a known
    // floor; it cannot prove the trusted invoice's complete retained backing.
    let roots = if std::ptr::eq(left, right) { 1 } else { 2 };
    let minimum = mul(roots, mem::size_of::<FunctionValueType>())?;
    let source_covers_roots = source_retained_bytes >= minimum;
    let prefix = type_binding_prefix_core(left, right, source_retained_bytes, arithmetic);
    let prefix = match prefix {
        Err(TypeCodecError::Control(cause)) if source_covers_roots => {
            return Err(TypeCodecError::Control(cause).into());
        }
        result => result,
    };
    // An ordinary missing-source floor keeps its original precedence and
    // observations. For a valid source, already known work exhaustion must
    // precede the first opaque boundary, including a caller's pending tail.
    if source_covers_roots && let Ok(prefix) = &prefix {
        cap(prefix.work_upper_bound(), max_work)?;
        admit(*prefix)?;
    }
    work.flush().map_err(TypeCodecError::from)?;
    work.step().map_err(TypeCodecError::from)?;
    if !source_covers_roots {
        return Err(shape("borrowed type source invoice omits original inline roots").into());
    }
    work.step().map_err(TypeCodecError::from)?;
    // Preserve an ordinary arithmetic error's original completed root steps.
    let prefix = prefix?;
    cap(prefix.work_upper_bound(), max_work)?;
    admit(prefix)?;
    work.step().map_err(TypeCodecError::from)?;
    work.flush().map_err(TypeCodecError::from)?;
    // The sole FVT equality rule stops on these flags before traversing a
    // datatype. No scratch is initialized and no metadata is inspected then.
    if !prefix.flags_match() {
        return Ok(prefix);
    }
    let prefix_work = prefix.work_upper_bound();
    let mut scratch = [None; MAX_VALUE_TYPE_NODES];
    work.flush().map_err(TypeCodecError::from)?;
    let mut metrics = Metrics {
        arithmetic,
        ..Metrics::default()
    };
    match validate_value_type_structure_with_scratch_observed::<ComparisonWalkError<E>>(
        &left.data_type,
        &mut scratch,
        |event| {
            metrics
                .observe(
                    event,
                    source_retained_bytes,
                    prefix_work,
                    max_work,
                    admit,
                    work,
                )
                .map_err(ComparisonWalkError::Admission)
        },
    ) {
        Ok(()) => {}
        Err(ComparisonWalkError::Grammar(error)) => return Err(error.into()),
        Err(ComparisonWalkError::Admission(error)) => return Err(error),
    }
    let work_upper_bound = metrics.bound(source_retained_bytes, prefix_work)?;
    let facts = BoundTypeComparisonFacts {
        work_upper_bound,
        flags_match: prefix.flags_match(),
    };
    admit(facts)?;
    work.flush().map_err(TypeCodecError::from)?;
    Ok(facts)
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
    verify_type_binding_admitted(
        left,
        right,
        source_retained_bytes,
        max_work,
        &mut |_| Ok(()),
        work,
    )
}

/// One actual numerical pass and its exact comparison, using the original
/// body. Every growing prefix is synchronously admitted by the parent.
pub(crate) fn verify_type_binding_admitted<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: usize,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedTypeBinding, E> {
    verify_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        max_work,
        admit,
        work,
        Arithmetic::Plain,
    )
}

pub(crate) fn verify_type_binding_admitted_in<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: usize,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedTypeBinding, E> {
    verify_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        max_work,
        admit,
        work,
        Arithmetic::Parent,
    )
}

fn verify_type_binding_admitted_core<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: usize,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
    arithmetic: Arithmetic,
) -> Result<VerifiedTypeBinding, E> {
    let facts = preflight_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        max_work,
        admit,
        work,
        arithmetic,
    )?;
    if !facts.flags_match() {
        return Ok(VerifiedTypeBinding {
            facts,
            matches: false,
        });
    }
    work.flush().map_err(TypeCodecError::from)?;
    let compared =
        arrow_data_types_exact_borrowed_observed(&left.data_type, &right.data_type, || {
            work.step().map_err(TypeCodecError::from)
        });
    let compared = match compared {
        Err(TypeCodecError::Control(error)) => return Err(TypeCodecError::Control(error).into()),
        result => result,
    };
    // Observe the actual comparison's ordinary refusal exit. Publication and
    // the enclosing caller's ordinary/success finish still occur later.
    work.flush().map_err(TypeCodecError::from)?;
    Ok(VerifiedTypeBinding {
        facts,
        matches: compared?,
    })
}

#[cfg(test)]
#[path = "borrowed_type_resources/tests.rs"]
mod tests;
