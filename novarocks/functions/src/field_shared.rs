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
//! ONE original FIELD equality and first-match update, shared by both shells.
use crate::KernelFailure;
use arrow_array::{Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array};
use arrow_ord::cmp::eq;
use arrow_schema::DataType;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};
use std::sync::Arc;
// Arrow's equality uses IEEE totalOrder for floats (-0 != +0 and NaN == NaN).
// FIELD uses scalar equality, matching its typed comparison contract.
fn field_equal(first: &ArrayRef, candidate: &ArrayRef) -> Result<BooleanArray, KernelFailure> {
    macro_rules! float_equal {
        ($array:ty) => {{
            let left = first
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| field_contract("field float downcast failed"))?;
            let right = candidate
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| field_contract("field float downcast failed"))?;
            Ok(BooleanArray::from_iter((0..left.len()).map(|row| {
                (!left.is_null(row) && !right.is_null(row))
                    .then(|| left.value(row) == right.value(row))
            })))
        }};
    }
    match first.data_type() {
        DataType::Float32 => float_equal!(Float32Array),
        DataType::Float64 => float_equal!(Float64Array),
        _ => eq(
            &first.as_ref() as &dyn arrow_array::Datum,
            &candidate.as_ref() as &dyn arrow_array::Datum,
        )
        .map_err(|source| KernelFailure::from_domain(FieldFailure::Arrow(source))),
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Observation {
    Step,
    OpaqueBoundary,
}
fn observe<E>(
    observer: &mut Option<&mut dyn FnMut(Observation) -> Result<(), E>>,
    event: Observation,
) -> Result<(), E> {
    if let Some(observer) = observer.as_deref_mut() {
        observer(event)?;
    }
    Ok(())
}
/// Exact candidate-index representability, independent of capacity admission.
pub fn validate_arity(arity: usize) -> Result<(), FieldArityError> {
    if arity < 2 || arity - 1 > i32::MAX as usize {
        return Err(FieldArityError { arity });
    }
    Ok(())
}
#[derive(Debug)]
pub struct FieldArityError {
    arity: usize,
}
impl std::fmt::Display for FieldArityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = self.arity;
        f.write_str("field requires a value and an INT-bounded candidate list")
    }
}
impl std::error::Error for FieldArityError {}
impl FailureProjection for FieldArityError {
    fn failure_fact(&self) -> FailureFact {
        field_fact(FailureClass::Invariant, 365)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for FieldArityError {}
#[derive(Debug)]
enum FieldFailure {
    Contract(BoundedDetail),
    Arrow(arrow_schema::ArrowError),
    Ordinal(std::num::TryFromIntError),
}
impl std::fmt::Display for FieldFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => detail.fmt(f),
            Self::Arrow(source) => source.fmt(f),
            Self::Ordinal(source) => {
                write!(f, "validated FIELD candidate index is not Int32: {source}")
            }
        }
    }
}
impl std::error::Error for FieldFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(_) => None,
            Self::Arrow(source) => Some(source),
            Self::Ordinal(source) => Some(source),
        }
    }
}
fn field_fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
    )
}
impl FailureProjection for FieldFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Arrow(arrow_schema::ArrowError::MemoryError(_)) => {
                field_fact(FailureClass::Resource, 366)
            }
            Self::Arrow(_) => field_fact(FailureClass::Invariant, 367),
            _ => field_fact(FailureClass::Invariant, 365),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for FieldFailure {}
fn field_contract(detail: impl std::fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(FieldFailure::Contract(BoundedDetail::truncating_display(
        detail,
    )))
}
/// A borrowed first value preserves the caller's actual invocation domain.
/// The caller retains argument evaluation and candidate sequencing authority.
pub struct FieldState<'a> {
    first: &'a ArrayRef,
    indices: Vec<i32>,
    candidates: usize,
}
impl<'a> FieldState<'a> {
    pub fn new_observed<E>(
        first: &'a ArrayRef,
        arity: usize,
        mut observer: Option<&mut dyn FnMut(Observation) -> Result<(), E>>,
    ) -> Result<Result<Self, KernelFailure>, E> {
        if let Err(error) = validate_arity(arity) {
            return Ok(Err(KernelFailure::from_domain(error)));
        }
        observe(&mut observer, Observation::OpaqueBoundary)?;
        let indices = vec![0_i32; first.len()];
        observe(&mut observer, Observation::OpaqueBoundary)?;
        Ok(Ok(Self {
            first,
            indices,
            candidates: arity - 1,
        }))
    }
    pub fn new(first: &'a ArrayRef, arity: usize) -> Result<Self, KernelFailure> {
        match Self::new_observed::<std::convert::Infallible>(first, arity, None) {
            Ok(out) => out,
            Err(never) => match never {},
        }
    }
    pub fn step_observed<E>(
        &mut self,
        index: usize,
        candidate: &ArrayRef,
        mut observer: Option<&mut dyn FnMut(Observation) -> Result<(), E>>,
    ) -> Result<Result<(), KernelFailure>, E> {
        if index >= self.candidates {
            return Ok(Err(field_contract(
                "FIELD candidate index is outside its prepared argument list",
            )));
        }
        let output_index = match i32::try_from(index + 1) {
            Ok(index) => index,
            Err(source) => {
                return Ok(Err(KernelFailure::from_domain(FieldFailure::Ordinal(
                    source,
                ))));
            }
        };
        // These guards are the original raw data contract. No later candidate
        // is requested until the current caller has received this result.
        if candidate.len() != self.first.len() {
            return Ok(Err(field_contract("field frozen argument length mismatch")));
        }
        if self.first.data_type() == &DataType::Null {
            return Ok(Ok(()));
        }
        if candidate.data_type() != self.first.data_type() {
            return Ok(Err(field_contract(format_args!(
                "field frozen argument mismatch: {:?}/{} vs {:?}/{}",
                self.first.data_type(),
                self.first.len(),
                candidate.data_type(),
                candidate.len()
            ))));
        }
        if observer.is_some() {
            // Observe only the already-demanded primitive/text payload before
            // the original opaque Arrow comparator, never a second equality.
            for array in [self.first, candidate] {
                for row in 0..array.len() {
                    observe(&mut observer, Observation::Step)?;
                    if array.is_null(row) {
                        continue;
                    }
                    let text = match array.data_type() {
                        DataType::Utf8 => array
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .map(|a| a.value(row)),
                        DataType::LargeUtf8 => array
                            .as_any()
                            .downcast_ref::<arrow_array::LargeStringArray>()
                            .map(|a| a.value(row)),
                        _ => None,
                    };
                    if let Some(text) = text {
                        for _ in text.bytes() {
                            observe(&mut observer, Observation::Step)?;
                        }
                    }
                }
            }
        }
        observe(&mut observer, Observation::OpaqueBoundary)?;
        let equal = match field_equal(self.first, candidate) {
            Ok(equal) => equal,
            Err(error) => return Ok(Err(error)),
        };
        observe(&mut observer, Observation::OpaqueBoundary)?;
        for (row, output) in self.indices.iter_mut().enumerate() {
            if *output == 0 && !equal.is_null(row) && equal.value(row) {
                *output = output_index;
            }
            observe(&mut observer, Observation::Step)?;
        }
        Ok(Ok(()))
    }
    pub fn step(&mut self, index: usize, candidate: &ArrayRef) -> Result<(), KernelFailure> {
        match self.step_observed::<std::convert::Infallible>(index, candidate, None) {
            Ok(out) => out,
            Err(never) => match never {},
        }
    }
    pub fn finish_observed<E>(
        self,
        mut observer: Option<&mut dyn FnMut(Observation) -> Result<(), E>>,
    ) -> Result<ArrayRef, E> {
        observe(&mut observer, Observation::OpaqueBoundary)?;
        let output = Arc::new(Int32Array::from(self.indices)) as ArrayRef;
        observe(&mut observer, Observation::OpaqueBoundary)?;
        Ok(output)
    }
    pub fn finish(self) -> ArrayRef {
        match self.finish_observed::<std::convert::Infallible>(None) {
            Ok(out) => out,
            Err(never) => match never {},
        }
    }
}

#[cfg(test)]
mod source_properties {
    use super::*;
    #[test]
    fn field_actual_arrow_source_and_invalid_candidate_index_are_unmaskable_and_bounded() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use std::error::Error;
        let first: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let second: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["ResourceExhausted"]));
        // The actual SDK comparison, below the frozen-input guard, produces this
        // source. The selected and direct hosts must preserve its nominal cause.
        let failure = field_equal(&first, &second).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 367);
        let source = failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .source()
            .unwrap();
        assert!(source.is::<arrow_schema::ArrowError>());
        let mut state = FieldState::new(&first, 2).unwrap();
        let failure = state.step(usize::MAX, &first).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 365);
        assert_eq!(
            state
                .finish()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            0
        );
    }
}
