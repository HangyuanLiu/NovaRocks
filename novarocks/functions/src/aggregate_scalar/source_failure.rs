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

//! Facts authored while inspecting scalar carriers and constructing their exact output.
use crate::KernelFailure;
use arrow_schema::ArrowError;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    OuterFailureProjection, RepresentationBoundary, RepresentationFacts, RepresentationLayout,
    domain,
};
use std::{fmt, num::NonZeroU16};
#[derive(Clone, Copy, Debug)]
pub(super) enum ContractKind {
    Downcast,
    TrackedType,
    OutputType,
    ValueType,
    StructWidth,
    MapEntries,
    Comparison,
    Offsets,
}
#[derive(Debug)]
struct ScalarContractFailure {
    kind: ContractKind,
    detail: BoundedDetail,
}
impl fmt::Display for ScalarContractFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}
impl std::error::Error for ScalarContractFailure {}
impl FailureProjection for ScalarContractFailure {
    fn failure_fact(&self) -> FailureFact {
        let code = match self.kind {
            ContractKind::Downcast => 312,
            ContractKind::TrackedType => 313,
            ContractKind::OutputType => 314,
            ContractKind::ValueType => 315,
            ContractKind::StructWidth => 316,
            ContractKind::MapEntries => 317,
            ContractKind::Comparison => 318,
            ContractKind::Offsets => 335,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
        )
    }
}
impl OuterFailureProjection for ScalarContractFailure {}
pub(super) fn contract(kind: ContractKind, detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(ScalarContractFailure {
        kind,
        detail: BoundedDetail::truncating_display(detail),
    })
}
#[derive(Debug)]
struct ScalarUtf8Contract(std::str::Utf8Error);
impl fmt::Display for ScalarUtf8Contract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tracked UTF-8 output contains invalid bytes: {}", self.0)
    }
}
impl std::error::Error for ScalarUtf8Contract {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for ScalarUtf8Contract {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(319).unwrap()),
        )
    }
}
impl OuterFailureProjection for ScalarUtf8Contract {}
pub(super) fn utf8(source: std::string::FromUtf8Error) -> KernelFailure {
    // Keep the original UTF-8 validation fact, not the rejected payload's Vec.
    KernelFailure::from_domain(ScalarUtf8Contract(source.utf8_error()))
}
#[derive(Clone, Copy, Debug)]
pub(super) enum OffsetLayout {
    Utf8,
    Binary,
    LargeBinary,
    List,
    Map,
}
impl OffsetLayout {
    fn limit(self) -> u64 {
        match self {
            Self::LargeBinary => i64::MAX as u64,
            _ => i32::MAX as u64,
        }
    }
    fn code(self) -> u16 {
        match self {
            Self::Utf8 => 60,
            Self::Binary => 61,
            Self::LargeBinary => 62,
            Self::List => 63,
            Self::Map => 64,
        }
    }
}
#[derive(Debug)]
struct ScalarOutputExtent {
    layout: OffsetLayout,
    total: Option<u128>,
    largest: u128,
}
impl fmt::Display for ScalarOutputExtent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "scalar {:?} output extent ", self.layout)?;
        match self.total {
            Some(total) => write!(f, "{total}")?,
            None => f.write_str("beyond the u128 range")?,
        };
        write!(
            f,
            " (largest value {}) exceeds offset limit {}",
            self.largest,
            self.layout.limit()
        )
    }
}
impl std::error::Error for ScalarOutputExtent {}
impl FailureProjection for ScalarOutputExtent {
    fn failure_fact(&self) -> FailureFact {
        let minimum = self.largest > u128::from(self.layout.limit());
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(320).unwrap()),
        )
        .with_representation(RepresentationFacts {
            boundary: if minimum {
                RepresentationBoundary::MinimumUnit
            } else {
                RepresentationBoundary::SingleOutput
            },
            layout: RepresentationLayout {
                domain: domain::KERNEL,
                code: NonZeroU16::new(self.layout.code()).unwrap(),
            },
            required: if minimum {
                u64::try_from(self.largest).ok()
            } else {
                self.total.and_then(|total| u64::try_from(total).ok())
            },
            extent_limit: Some(self.layout.limit()),
            representable_prefix: None,
        })
    }
}
impl OuterFailureProjection for ScalarOutputExtent {}
pub(super) struct OutputTally {
    layout: OffsetLayout,
    total: u128,
    largest: u128,
}
impl OutputTally {
    pub(super) fn new(layout: OffsetLayout) -> Self {
        Self {
            layout,
            total: 0,
            largest: 0,
        }
    }
    pub(super) fn total(&self) -> u128 {
        self.total
    }
    pub(super) fn append(&mut self, extent: usize) -> Result<(), KernelFailure> {
        self.append_u128(extent as u128)
    }
    pub(super) fn append_u128(&mut self, extent: u128) -> Result<(), KernelFailure> {
        let total = self.total.checked_add(extent);
        let largest = self.largest.max(extent);
        if total.is_none_or(|total| total > u128::from(self.layout.limit())) {
            return Err(KernelFailure::from_domain(ScalarOutputExtent {
                layout: self.layout,
                total,
                largest,
            }));
        }
        // Success establishes both a concrete total and the actual format limit.
        self.total = total.unwrap();
        self.largest = largest;
        Ok(())
    }
}
#[derive(Clone, Copy, Debug)]
pub(super) enum Construction {
    Decimal128,
    Decimal256,
    List,
    Struct,
    MapEntries,
    Map,
}
#[derive(Debug)]
struct ScalarConstructionFailure {
    operation: Construction,
    source: ArrowError,
}
impl fmt::Display for ScalarConstructionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "scalar {:?} output construction failed: {}",
            self.operation, self.source
        )
    }
}
impl std::error::Error for ScalarConstructionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for ScalarConstructionFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self.source {
            ArrowError::MemoryError(_) => (FailureClass::Resource, 321),
            ArrowError::IoError(..) => (FailureClass::Unexpected, 322),
            // Each variable-offset append was validated before construction.
            // Frozen constructors do not convert active numeric values.
            _ => (FailureClass::Invariant, 323),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
        )
    }
}
impl OuterFailureProjection for ScalarConstructionFailure {}
pub(super) fn construction(operation: Construction, source: ArrowError) -> KernelFailure {
    KernelFailure::from_domain(ScalarConstructionFailure { operation, source })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scalar_output_offsets_keep_exact_minimum_and_whole_output_extents() {
        for layout in [
            OffsetLayout::Utf8,
            OffsetLayout::Binary,
            OffsetLayout::LargeBinary,
            OffsetLayout::List,
            OffsetLayout::Map,
        ] {
            let Ok(limit) = usize::try_from(layout.limit()) else {
                continue;
            };
            let Some(oversized) = limit.checked_add(1) else {
                continue;
            };
            let mut tally = OutputTally::new(layout);
            tally.append(limit).unwrap();
            tally.append(0).unwrap();
            let batch = tally.append(1).unwrap_err().failure_fact();
            tally
                .append(0)
                .expect("a rejected extent did not advance the format tally");
            let unit = OutputTally::new(layout)
                .append(oversized)
                .unwrap_err()
                .failure_fact();
            for (fact, boundary) in [
                (batch, RepresentationBoundary::SingleOutput),
                (unit, RepresentationBoundary::MinimumUnit),
            ] {
                assert_eq!(fact.class(), FailureClass::Representation);
                let facts = fact.representation().unwrap();
                assert_eq!(facts.boundary, boundary);
                assert_eq!(facts.required, Some(layout.limit() + 1));
                assert_eq!(facts.extent_limit, Some(layout.limit()));
                assert_eq!(facts.representable_prefix, None);
            }
        }
    }
}

#[cfg(test)]
mod scalar_builder_source_properties {
    use super::*;
    use crate::aggregate_scalar::{AggScalarValue, ScalarWork, build_scalar_array};
    use arrow_schema::{DataType, Field};
    use std::{error::Error, sync::Arc};
    #[test]
    fn scalar_output_rejected_schema_is_bounded_and_does_not_retain_its_field_owner() {
        let field = Arc::new(Field::new(
            "invalid value resource cancelled".repeat(100),
            DataType::Int64,
            true,
        ));
        let weak = Arc::downgrade(&field);
        let ty = DataType::FixedSizeList(field.clone(), 1);
        let failure = build_scalar_array(&ty, vec![None], &mut ScalarWork::new(None)).unwrap_err();
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Invariant);
        assert_eq!(fact.reason().code(), 314);
        assert!(failure.to_string().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
        drop(ty);
        drop(field);
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact(), fact);
    }
    #[test]
    fn scalar_output_struct_constructor_returns_actual_arrow_error_instead_of_panicking() {
        let ty = DataType::Struct(vec![Field::new("required", DataType::Int64, false)].into());
        let failure = build_scalar_array(
            &ty,
            vec![Some(AggScalarValue::Struct(vec![None]))],
            &mut ScalarWork::new(None),
        )
        .unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        let mut source = failure.source();
        let mut found = false;
        while let Some(actual) = source {
            found |= actual.is::<ArrowError>();
            source = actual.source();
        }
        assert!(found, "actual Arrow constructor error must remain a source");
    }
}

/// These failures end a whole aggregate output/comparison, with no selected
/// row ordinal. Data classification does not make this a maskable row error.
#[derive(Debug)]
enum WholeScalarValueFailure {
    Narrow {
        target: &'static str,
        source: std::num::TryFromIntError,
    },
    UnorderedFloat,
}
impl fmt::Display for WholeScalarValueFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Narrow { target, .. } => write!(f, "{target} overflow"),
            Self::UnorderedFloat => f.write_str("float comparison is not ordered"),
        }
    }
}
impl std::error::Error for WholeScalarValueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Narrow { source, .. } => Some(source),
            Self::UnorderedFloat => None,
        }
    }
}
impl FailureProjection for WholeScalarValueFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Narrow { .. } => crate::DataFailureReason::NumericOutOfRange.failure_fact(),
            Self::UnorderedFloat => crate::DataFailureReason::InvalidValue.failure_fact(),
        }
    }
}
impl OuterFailureProjection for WholeScalarValueFailure {}
pub(super) fn whole_narrow(
    target: &'static str,
    source: std::num::TryFromIntError,
) -> KernelFailure {
    KernelFailure::from_domain(WholeScalarValueFailure::Narrow { target, source })
}
pub(super) fn whole_unordered_float() -> KernelFailure {
    KernelFailure::from_domain(WholeScalarValueFailure::UnorderedFloat)
}

#[cfg(test)]
mod numeric_source_properties {
    use super::*;
    use crate::aggregate_scalar::{AggScalarValue, ScalarWork, build_scalar_array};
    use arrow_schema::DataType;
    use std::error::Error;
    #[test]
    fn scalar_output_narrowing_preserves_actual_numeric_source() {
        for (ty, value) in [
            (DataType::Int8, 128),
            (DataType::Int16, 32768),
            (DataType::Int32, i64::from(i32::MAX) + 1),
        ] {
            let error = build_scalar_array(
                &ty,
                vec![Some(AggScalarValue::Int64(value))],
                &mut ScalarWork::new(None),
            )
            .unwrap_err();
            assert_eq!(
                error.failure_fact(),
                crate::DataFailureReason::NumericOutOfRange.failure_fact()
            );
            let mut source = error.source();
            let mut found = false;
            while let Some(actual) = source {
                found |= actual.is::<std::num::TryFromIntError>();
                source = actual.source();
            }
            assert!(
                found,
                "the actual failed narrowing must remain in the source chain"
            );
        }
    }
}

#[cfg(test)]
mod atomic_source_lifecycle_properties {
    use super::*;
    use crate::aggregate_scalar::{AggScalarValue, ScalarWork, build_scalar_array};
    use crate::kernel_input::EvaluationCheckpoints;
    use arrow_schema::DataType;
    struct NoFooter;
    impl crate::KernelEvaluationControl for NoFooter {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            panic!("originating whole-value failure must not start an optional footer")
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("no wait after whole-value failure")
        }
    }
    #[test]
    fn actual_scalar_numeric_and_extent_causes_survive_every_optional_lifecycle_footer() {
        let numeric = build_scalar_array(
            &DataType::Int8,
            vec![Some(AggScalarValue::Int64(128))],
            &mut ScalarWork::new(None),
        )
        .unwrap_err();
        let mut extent = OutputTally::new(OffsetLayout::Utf8);
        extent.append(i32::MAX as usize).unwrap();
        let extent = extent.append(1).unwrap_err();
        for failure in [numeric, extent] {
            assert_eq!(
                EvaluationCheckpoints::new(&NoFooter).finish_result::<()>(Err(failure.clone())),
                Err(failure.clone())
            );
            assert_eq!(
                crate::evaluation_failure::finish_evaluation_lifecycle::<()>(
                    Err(failure.clone().into()),
                    || panic!("optional aggregate completion")
                ),
                Err(crate::EvaluationFailure::Kernel(failure.clone()))
            );
            assert_eq!(
                crate::window_invocation_data::finish_window_lifecycle::<()>(
                    Err(failure.clone().into()),
                    || panic!("optional window completion")
                ),
                Err(crate::WindowEvaluationFailure::Kernel(failure))
            );
        }
    }
    #[test]
    fn a_previously_observed_refusal_precedes_a_later_whole_value_failure() {
        struct Refusal;
        impl crate::KernelEvaluationControl for Refusal {
            fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
                Err(KernelFailure::DeadlineExceeded)
            }
            fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
                panic!("no wait")
            }
        }
        let numeric = build_scalar_array(
            &DataType::Int8,
            vec![Some(AggScalarValue::Int64(128))],
            &mut ScalarWork::new(None),
        )
        .unwrap_err();
        let mut work = EvaluationCheckpoints::new(&Refusal);
        assert_eq!(work.flush(), Err(KernelFailure::DeadlineExceeded));
        assert_eq!(
            work.finish_result::<()>(Err(numeric)),
            Err(KernelFailure::DeadlineExceeded)
        );
    }
}
