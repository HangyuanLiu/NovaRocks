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

//! Borrowed original TDigest failure formatting and observed host frontiers.
use std::fmt;
#[derive(Clone, Copy, Debug)]
pub enum ApproxPercentileObservation {
    Step,
    SortBegin(usize),
    SortEnd,
    JsonBegin(usize),
    JsonEnd,
}
pub enum ApproxPercentileDataRecipe<'a> {
    Static(&'static str),
    Arguments(fmt::Arguments<'a>),
}
impl fmt::Display for ApproxPercentileDataRecipe<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(s) => f.write_str(s),
            Self::Arguments(a) => f.write_fmt(*a),
        }
    }
}

pub trait ApproxPercentileFailureSink {
    type Error;
    fn data(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> Self::Error;
    fn encoding(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> Self::Error;
    fn kernel(&mut self, source: crate::KernelFailure) -> Self::Error;
    fn json(&mut self, source: serde_json::Error) -> Self::Error;
    fn allocation<E: crate::allocation_failure::AllocationRefusalSource>(
        &mut self,
        recipe: ApproxPercentileDataRecipe<'_>,
        source: E,
    ) -> Self::Error;
    fn observe(&mut self, observation: ApproxPercentileObservation) -> Result<(), Self::Error>;
}
pub struct RequiredApproxPercentileFailure;
impl ApproxPercentileFailureSink for RequiredApproxPercentileFailure {
    type Error = crate::KernelFailure;
    fn data(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> Self::Error {
        value_failure(recipe, crate::DataFailureReason::InvalidValue)
    }
    fn encoding(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> Self::Error {
        value_failure(recipe, crate::DataFailureReason::InvalidEncoding)
    }
    fn kernel(&mut self, source: crate::KernelFailure) -> Self::Error {
        source
    }
    fn json(&mut self, source: serde_json::Error) -> Self::Error {
        json_failure(source)
    }
    fn allocation<E: crate::allocation_failure::AllocationRefusalSource>(
        &mut self,
        recipe: ApproxPercentileDataRecipe<'_>,
        source: E,
    ) -> Self::Error {
        crate::KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
            &recipe.to_string(),
            source,
        ))
    }
    fn observe(&mut self, _: ApproxPercentileObservation) -> Result<(), Self::Error> {
        Ok(())
    }
}
#[derive(Debug)]
struct ApproxPercentileFailure {
    reason: Option<crate::DataFailureReason>,
    detail: novarocks_failure_contract::BoundedDetail,
    source: Option<serde_json::Error>,
}
impl fmt::Display for ApproxPercentileFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}
impl std::error::Error for ApproxPercentileFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}
impl novarocks_failure_contract::FailureProjection for ApproxPercentileFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        match self.reason {
            Some(reason) => reason.failure_fact(),
            None => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(398).unwrap()),
            ),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ApproxPercentileFailure {}
pub(crate) fn contract(detail: impl fmt::Display) -> crate::KernelFailure {
    crate::KernelFailure::from_domain(ApproxPercentileFailure {
        reason: None,
        detail: novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        source: None,
    })
}
pub(crate) fn value_failure(
    detail: impl fmt::Display,
    reason: crate::DataFailureReason,
) -> crate::KernelFailure {
    crate::KernelFailure::from_domain(ApproxPercentileFailure {
        reason: Some(reason),
        detail: novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        source: None,
    })
}
pub(crate) fn json_failure(source: serde_json::Error) -> crate::KernelFailure {
    crate::KernelFailure::from_domain(ApproxPercentileFailure {
        reason: Some(crate::DataFailureReason::InvalidEncoding),
        detail: novarocks_failure_contract::BoundedDetail::truncating_display(&source),
        source: Some(source),
    })
}

#[derive(Debug)]
enum ApproxExtent {
    Count {
        required: usize,
        source: std::num::TryFromIntError,
    },
    Bytes {
        required: u128,
        source: Option<std::alloc::LayoutError>,
    },
}
impl fmt::Display for ApproxExtent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count { required, .. } => write!(
                f,
                "percentile field count {required} exceeds its u32 format"
            ),
            Self::Bytes { required, .. } => write!(
                f,
                "percentile storage requires {required} bytes beyond its addressable layout"
            ),
        }
    }
}
impl std::error::Error for ApproxExtent {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Count { source, .. } => Some(source),
            Self::Bytes { source, .. } => source
                .as_ref()
                .map(|source| source as &(dyn std::error::Error + 'static)),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ApproxExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let (code, required, limit) = match self {
            Self::Count { required, .. } => (399, *required as u128, u32::MAX as u64),
            Self::Bytes { required, .. } => (400, *required, isize::MAX as u64),
        };
        let code = std::num::NonZeroU16::new(code).unwrap();
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, code),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::MinimumUnit,
            layout: RepresentationLayout {
                domain: domain::KERNEL,
                code,
            },
            required: u64::try_from(required).ok(),
            extent_limit: Some(limit),
            representable_prefix: None,
        })
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ApproxExtent {}
pub(crate) fn wire_count(required: usize) -> Result<u32, crate::KernelFailure> {
    u32::try_from(required).map_err(|source| {
        crate::KernelFailure::from_domain(ApproxExtent::Count { required, source })
    })
}
pub(crate) fn storage_count<T>(count: u128) -> Result<usize, crate::KernelFailure> {
    let required = count * std::mem::size_of::<T>() as u128;
    let count = usize::try_from(count).map_err(|_| {
        crate::KernelFailure::from_domain(ApproxExtent::Bytes {
            required,
            source: None,
        })
    })?;
    std::alloc::Layout::array::<T>(count).map_err(|source| {
        crate::KernelFailure::from_domain(ApproxExtent::Bytes {
            required,
            source: Some(source),
        })
    })?;
    Ok(count)
}
