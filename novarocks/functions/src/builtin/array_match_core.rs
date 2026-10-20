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
//! ONE original ALL_MATCH and ANY_MATCH normalizers and row algorithms.
use super::array_literal_core::CollectionObservation;
use arrow_array::{Array, ArrayRef, BooleanArray, ListArray};
use arrow_schema::DataType;
use std::sync::Arc;
#[derive(Debug)]
pub enum MatchFailure<E> {
    Domain(crate::KernelFailure),
    Control(E),
}
impl<E> From<crate::largeint::LargeIntCarrierFailure> for MatchFailure<E> {
    fn from(source: crate::largeint::LargeIntCarrierFailure) -> Self {
        Self::Domain(crate::KernelFailure::from_domain(source))
    }
}
#[derive(Debug)]
enum MatchDomainFailure {
    Contract(novarocks_failure_contract::BoundedDetail),
    Conversion(arrow_schema::ArrowError),
}
impl std::fmt::Display for MatchDomainFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => f.write_str(detail.as_str()),
            Self::Conversion(source) => {
                write!(f, "array match boolean conversion failed: {source}")
            }
        }
    }
}
impl std::error::Error for MatchDomainFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Conversion(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for MatchDomainFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self {
            Self::Contract(_) => (FailureClass::Invariant, 295),
            Self::Conversion(arrow_schema::ArrowError::MemoryError(_)) => {
                (FailureClass::Resource, 296)
            }
            Self::Conversion(arrow_schema::ArrowError::OffsetOverflowError(_)) => {
                (FailureClass::Representation, 297)
            }
            Self::Conversion(arrow_schema::ArrowError::IoError(..)) => {
                (FailureClass::Unexpected, 298)
            }
            // This default safe BOOLEAN cast makes unparseable values NULL;
            // CastError therefore reports an unsupported declared carrier.
            Self::Conversion(_) => (FailureClass::Invariant, 299),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        if class == FailureClass::Representation {
            fact.with_representation(novarocks_failure_contract::RepresentationFacts {
                boundary: novarocks_failure_contract::RepresentationBoundary::SingleOutput,
                layout: novarocks_failure_contract::RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(59).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for MatchDomainFailure {}
fn contract<E>(detail: impl std::fmt::Display) -> MatchFailure<E> {
    MatchFailure::Domain(crate::KernelFailure::from_domain(
        MatchDomainFailure::Contract(
            novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        ),
    ))
}
fn conversion<E>(source: arrow_schema::ArrowError) -> MatchFailure<E> {
    MatchFailure::Domain(crate::KernelFailure::from_domain(
        MatchDomainFailure::Conversion(source),
    ))
}
#[derive(Clone, Copy, Debug)]
pub enum Operation {
    All,
    Any,
}
pub fn all_match(arr: &ArrayRef, chunk_len: usize) -> Result<ArrayRef, crate::KernelFailure> {
    match all_match_observed(arr, chunk_len, &mut |_| {
        Ok::<(), std::convert::Infallible>(())
    }) {
        Ok(out) => Ok(out),
        Err(MatchFailure::Domain(cause)) => Err(cause),
        Err(MatchFailure::Control(never)) => match never {},
    }
}
pub fn all_match_observed<E>(
    arr: &ArrayRef,
    chunk_len: usize,
    observe: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, MatchFailure<E>> {
    observe(CollectionObservation::OpaqueBoundary).map_err(MatchFailure::Control)?;
    let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        contract(format_args!(
            "all_match expects ListArray, got {:?}",
            arr.data_type()
        ))
    })?;

    let mut values = list.values().clone();
    if values.data_type() != &DataType::Boolean {
        if crate::largeint::is_largeint_data_type(values.data_type()) {
            let typed = crate::largeint::as_fixed_size_binary_array(
                &values,
                "all_match LARGEINT to BOOLEAN",
            )?;
            let mut out = Vec::with_capacity(typed.len());
            for idx in 0..typed.len() {
                observe(CollectionObservation::Step).map_err(MatchFailure::Control)?;
                if typed.is_null(idx) {
                    out.push(None);
                } else {
                    out.push(Some(crate::largeint::value_at(typed, idx)? != 0));
                }
            }
            values = Arc::new(BooleanArray::from(out)) as ArrayRef;
        } else {
            observe(CollectionObservation::OpaqueBoundary).map_err(MatchFailure::Control)?;
            values = arrow_cast::cast(&values, &DataType::Boolean).map_err(conversion)?;
        }
    }
    let values = values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| contract("all_match failed to downcast values to BooleanArray"))?;

    let offsets = list.value_offsets();
    let mut out = Vec::with_capacity(chunk_len);
    for row in 0..chunk_len {
        observe(CollectionObservation::Step).map_err(MatchFailure::Control)?;
        let row_idx = super::map_lookup_core::row_index(row, list.len());
        if list.is_null(row_idx) {
            out.push(None);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut has_null = false;
        let mut all_true = true;
        for idx in start..end {
            observe(CollectionObservation::Step).map_err(MatchFailure::Control)?;
            if values.is_null(idx) {
                has_null = true;
                continue;
            }
            if !values.value(idx) {
                all_true = false;
                break;
            }
        }

        if !all_true {
            out.push(Some(false));
        } else if has_null {
            out.push(None);
        } else {
            out.push(Some(true));
        }
    }

    observe(CollectionObservation::OpaqueBoundary).map_err(MatchFailure::Control)?;
    let out = Arc::new(BooleanArray::from(out)) as ArrayRef;
    observe(CollectionObservation::OpaqueBoundary).map_err(MatchFailure::Control)?;
    Ok(out)
}

pub fn any_match(arr: &ArrayRef, chunk_len: usize) -> Result<ArrayRef, crate::KernelFailure> {
    match any_match_observed(arr, chunk_len, &mut |_| {
        Ok::<(), std::convert::Infallible>(())
    }) {
        Ok(out) => Ok(out),
        Err(MatchFailure::Domain(cause)) => Err(cause),
        Err(MatchFailure::Control(never)) => match never {},
    }
}
pub fn any_match_observed<E>(
    arr: &ArrayRef,
    chunk_len: usize,
    observe: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, MatchFailure<E>> {
    observe(CollectionObservation::OpaqueBoundary).map_err(MatchFailure::Control)?;
    let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        contract(format_args!(
            "any_match expects ListArray, got {:?}",
            arr.data_type()
        ))
    })?;

    let mut values = list.values().clone();
    if values.data_type() != &DataType::Boolean {
        if crate::largeint::is_largeint_data_type(values.data_type()) {
            let typed = crate::largeint::as_fixed_size_binary_array(
                &values,
                "any_match LARGEINT to BOOLEAN",
            )?;
            let mut out = Vec::with_capacity(typed.len());
            for idx in 0..typed.len() {
                observe(CollectionObservation::Step).map_err(MatchFailure::Control)?;
                if typed.is_null(idx) {
                    out.push(None);
                } else {
                    out.push(Some(crate::largeint::value_at(typed, idx)? != 0));
                }
            }
            values = Arc::new(BooleanArray::from(out)) as ArrayRef;
        } else {
            observe(CollectionObservation::OpaqueBoundary).map_err(MatchFailure::Control)?;
            values = arrow_cast::cast(&values, &DataType::Boolean).map_err(conversion)?;
        }
    }
    let values = values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| contract("any_match failed to downcast values to BooleanArray"))?;

    let offsets = list.value_offsets();
    let mut out = Vec::with_capacity(chunk_len);
    for row in 0..chunk_len {
        observe(CollectionObservation::Step).map_err(MatchFailure::Control)?;
        let row_idx = super::map_lookup_core::row_index(row, list.len());
        if list.is_null(row_idx) {
            out.push(None);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut has_null = false;
        let mut found_true = false;
        for idx in start..end {
            observe(CollectionObservation::Step).map_err(MatchFailure::Control)?;
            if values.is_null(idx) {
                has_null = true;
                continue;
            }
            if values.value(idx) {
                found_true = true;
                break;
            }
        }

        if found_true {
            out.push(Some(true));
        } else if has_null {
            out.push(None);
        } else {
            out.push(Some(false));
        }
    }

    observe(CollectionObservation::OpaqueBoundary).map_err(MatchFailure::Control)?;
    let out = Arc::new(BooleanArray::from(out)) as ArrayRef;
    observe(CollectionObservation::OpaqueBoundary).map_err(MatchFailure::Control)?;
    Ok(out)
}
