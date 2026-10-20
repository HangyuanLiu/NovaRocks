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

//! Original DS HLL input diagnostic facts, synchronously borrowed.
use crate::datasketches_hll_failure::{HllFailureSink, HllObservation};
use crate::percentile_input::{PercentileInputDiagnostic, PercentilePayloadFailure};
use crate::sketch_hash::{SketchHashFailure, SketchHashFailureSink};
use arrow_schema::DataType;
use std::fmt;
#[derive(Clone, Copy, Debug)]
pub enum DsHllInputContext {
    CountDistinct,
    Merge,
}
impl DsHllInputContext {
    pub const fn label(self) -> &'static str {
        match self {
            Self::CountDistinct => "ds_hll_count_distinct",
            Self::Merge => "ds_hll_merge",
        }
    }
}
pub enum DsHllInputDetail<'a> {
    Downcast(&'static str),
    LogK(&'a DataType),
    Target(&'a DataType),
    EmptyStruct,
    Output(&'a DataType),
    Uninitialized,
    Payload(PercentilePayloadFailure<'a>),
}
pub struct DsHllInputRecipe<'a> {
    pub context: DsHllInputContext,
    pub detail: DsHllInputDetail<'a>,
}
impl fmt::Display for DsHllInputRecipe<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let context = self.context.label();
        match &self.detail {
            DsHllInputDetail::Downcast(kind) => write!(f, "{context}: failed to downcast {kind}"),
            DsHllInputDetail::LogK(ty) => write!(
                f,
                "{context}: ds_hll log_k expects integer input, got {:?}",
                ty
            ),
            DsHllInputDetail::Target(ty) => write!(
                f,
                "{context}: ds_hll target type expects string input, got {:?}",
                ty
            ),
            DsHllInputDetail::EmptyStruct => write!(f, "{context}: ds_hll input struct is empty"),
            DsHllInputDetail::Output(ty) => write!(
                f,
                "ds_hll output type must be Binary or Int64, got {:?}",
                ty
            ),
            DsHllInputDetail::Uninitialized => f.write_str("ds_hll handle is not initialized"),
            DsHllInputDetail::Payload(error) => fmt::Display::fmt(
                &error.message(PercentileInputDiagnostic::LegacyLabel(context)),
                f,
            ),
        }
    }
}
#[derive(Clone, Copy, Debug)]
enum DsHllInputFailureKind {
    Downcast,
    LogKType,
    TargetType,
    EmptyStruct,
    OutputType,
    Uninitialized,
    PayloadShape,
}
#[derive(Debug)]
pub struct DsHllInputFailure {
    context: DsHllInputContext,
    kind: DsHllInputFailureKind,
    detail: novarocks_failure_contract::BoundedDetail,
}
impl DsHllInputRecipe<'_> {
    /// Snapshot only the semantic shape and bounded diagnostic; no Arrow type
    /// graph or field owner escapes the synchronous source inspection.
    pub fn into_failure(self) -> DsHllInputFailure {
        let kind = match &self.detail {
            DsHllInputDetail::Downcast(_) => DsHllInputFailureKind::Downcast,
            DsHllInputDetail::LogK(_) => DsHllInputFailureKind::LogKType,
            DsHllInputDetail::Target(_) => DsHllInputFailureKind::TargetType,
            DsHllInputDetail::EmptyStruct => DsHllInputFailureKind::EmptyStruct,
            DsHllInputDetail::Output(_) => DsHllInputFailureKind::OutputType,
            DsHllInputDetail::Uninitialized => DsHllInputFailureKind::Uninitialized,
            DsHllInputDetail::Payload(_) => DsHllInputFailureKind::PayloadShape,
        };
        DsHllInputFailure {
            context: self.context,
            kind,
            detail: novarocks_failure_contract::BoundedDetail::truncating_display(&self),
        }
    }
}
impl DsHllInputFailure {
    pub fn context(&self) -> DsHllInputContext {
        self.context
    }
}
impl fmt::Display for DsHllInputFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.detail.as_str())
    }
}
impl std::error::Error for DsHllInputFailure {}
impl novarocks_failure_contract::FailureProjection for DsHllInputFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let code = match self.kind {
            DsHllInputFailureKind::Downcast => 184,
            DsHllInputFailureKind::LogKType => 185,
            DsHllInputFailureKind::TargetType => 186,
            DsHllInputFailureKind::EmptyStruct => 187,
            DsHllInputFailureKind::OutputType => 188,
            DsHllInputFailureKind::Uninitialized => 189,
            DsHllInputFailureKind::PayloadShape => 191,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for DsHllInputFailure {}

#[derive(Debug)]
pub struct DsHllOutputExtent {
    total: u128,
    largest: u128,
}
impl fmt::Display for DsHllOutputExtent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DS HLL Binary output extent {} (largest payload {}) exceeds Int32 offsets",
            self.total, self.largest
        )
    }
}
impl std::error::Error for DsHllOutputExtent {}
impl novarocks_failure_contract::FailureProjection for DsHllOutputExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        use std::num::NonZeroU16;
        let minimum = self.largest > i32::MAX as u128;
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(192).unwrap()),
        )
        .with_representation(RepresentationFacts {
            boundary: if minimum {
                RepresentationBoundary::MinimumUnit
            } else {
                RepresentationBoundary::SingleOutput
            },
            layout: RepresentationLayout {
                domain: domain::KERNEL,
                code: NonZeroU16::new(30).unwrap(),
            },
            required: u64::try_from(if minimum { self.largest } else { self.total }).ok(),
            extent_limit: Some(i32::MAX as u64),
            representable_prefix: None,
        })
    }
}
impl novarocks_failure_contract::OuterFailureProjection for DsHllOutputExtent {}
#[derive(Default)]
pub(super) struct DsHllOutputTally {
    total: u128,
    largest: u128,
}
impl DsHllOutputTally {
    pub(super) fn append(&mut self, bytes: usize) -> Result<(), crate::KernelFailure> {
        self.total = self.total.checked_add(bytes as u128).ok_or_else(|| {
            crate::KernelFailure::from_domain(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        self.largest = self.largest.max(bytes as u128);
        if self.total > i32::MAX as u128 {
            return Err(crate::KernelFailure::from_domain(DsHllOutputExtent {
                total: self.total,
                largest: self.largest,
            }));
        }
        Ok(())
    }
}

pub trait DsHllFailureSink: HllFailureSink {
    fn kernel(&mut self, cause: crate::KernelFailure) -> Self::Error;
    type Temporary;
    fn reserve_temporary(&mut self, bytes: usize) -> Result<Self::Temporary, Self::Error>;
    fn input(&mut self, recipe: DsHllInputRecipe<'_>) -> Self::Error;
}
/// Raw aggregate evaluation preserves value failures and outer failures separately.
#[derive(Debug)]
pub enum DsHllAggregateFailure {
    Value(crate::datasketches_hll_failure::HllValueFailure),
    Kernel(crate::KernelFailure),
}
impl From<crate::datasketches_hll_failure::ClassifiedHllFailure> for DsHllAggregateFailure {
    fn from(source: crate::datasketches_hll_failure::ClassifiedHllFailure) -> Self {
        use crate::datasketches_hll_failure::ClassifiedHllFailure;
        match source {
            ClassifiedHllFailure::Value(source) => Self::Value(source),
            ClassifiedHllFailure::Outer(source) => {
                Self::Kernel(crate::KernelFailure::from_domain(source))
            }
        }
    }
}
impl fmt::Display for DsHllAggregateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(source) => source.fmt(f),
            Self::Kernel(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for DsHllAggregateFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Value(source) => Some(source),
            Self::Kernel(source) => Some(source),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for DsHllAggregateFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Value(source) => source.failure_fact(),
            Self::Kernel(source) => source.failure_fact(),
        }
    }
}
pub struct RawDsHllFailure;
impl HllFailureSink for RawDsHllFailure {
    type Error = DsHllAggregateFailure;
    fn failure(
        &mut self,
        recipe: crate::datasketches_hll_failure::HllFailureRecipe,
    ) -> Self::Error {
        recipe.classify().into()
    }
    fn invariant(
        &mut self,
        recipe: crate::datasketches_hll_failure::HllInvariantRecipe,
    ) -> Self::Error {
        DsHllAggregateFailure::Kernel(crate::KernelFailure::from_domain(recipe))
    }
    fn observe(&mut self, _: HllObservation) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl DsHllFailureSink for RawDsHllFailure {
    fn kernel(&mut self, cause: crate::KernelFailure) -> Self::Error {
        DsHllAggregateFailure::Kernel(cause)
    }
    type Temporary = ();
    fn reserve_temporary(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn input(&mut self, recipe: DsHllInputRecipe<'_>) -> Self::Error {
        DsHllAggregateFailure::Kernel(crate::KernelFailure::from_domain(recipe.into_failure()))
    }
}
pub struct HashSink<'a, F: DsHllFailureSink> {
    pub sink: &'a mut F,
    pub context: DsHllInputContext,
}
impl<F: DsHllFailureSink> SketchHashFailureSink for HashSink<'_, F> {
    type Error = F::Error;
    fn hash_contract(&mut self, error: SketchHashFailure<'_>) -> Self::Error {
        self.sink.kernel(error.into_kernel(self.context.label()))
    }
    fn observe_hash(&mut self, event: HllObservation) -> Result<(), Self::Error> {
        self.sink.observe(event)
    }
}

#[cfg(test)]
mod output_extent_tests {
    use super::*;
    use novarocks_failure_contract::{
        FailureClass, FailureProjection, RepresentationBoundary, domain,
    };

    #[test]
    fn binary_output_separates_single_payload_and_aggregate_offset_extents() {
        let limit = i32::MAX as usize;
        let Some(too_large) = limit.checked_add(1) else {
            return;
        };
        let mut batch = DsHllOutputTally::default();
        batch.append(limit).unwrap();
        batch.append(0).unwrap();
        let batch = batch.append(1).unwrap_err().failure_fact();
        let unit = DsHllOutputTally::default()
            .append(too_large)
            .unwrap_err()
            .failure_fact();
        for (fact, boundary) in [
            (batch, RepresentationBoundary::SingleOutput),
            (unit, RepresentationBoundary::MinimumUnit),
        ] {
            assert_eq!(fact.class(), FailureClass::Representation);
            assert_eq!(fact.reason().domain(), domain::KERNEL.get());
            assert_eq!(fact.reason().code(), 192);
            let representation = fact.representation().unwrap();
            assert_eq!(representation.boundary, boundary);
            assert_eq!(representation.layout.domain, domain::KERNEL);
            assert_eq!(representation.layout.code.get(), 30);
            assert_eq!(representation.required, Some(i32::MAX as u64 + 1));
            assert_eq!(representation.extent_limit, Some(i32::MAX as u64));
        }
    }
}
