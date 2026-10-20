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

//! The copy author distinguishes physical backing from finite Arrow formats.
//! Neither a format check nor an allocation layout authorizes memory capacity.
use super::CopyError;
use crate::KernelFailure;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Clone, Copy, Debug)]
pub(crate) enum CopyLayout {
    Byte32 = 44,
    Byte64 = 45,
    List32 = 46,
    List64 = 47,
    ViewBufferTable = 48,
    DenseUnion = 50,
    Dictionary = 51,
    RunEnd = 52,
    TakeIndex = 53,
}
impl CopyLayout {
    pub(crate) fn bytes(large: bool) -> Self {
        if large { Self::Byte64 } else { Self::Byte32 }
    }
    pub(crate) fn list(large: bool) -> Self {
        if large { Self::List64 } else { Self::List32 }
    }
}
#[derive(Debug)]
pub struct CopyExtentFailure(Box<ExtentCause>);
#[derive(Debug)]
enum ExtentCause {
    Arithmetic {
        required: Option<u128>,
    },
    Backing {
        required: u128,
        source: Option<std::alloc::LayoutError>,
    },
    Format {
        layout: CopyLayout,
        required: Option<u128>,
        maximum: usize,
    },
}
impl fmt::Display for CopyExtentFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            ExtentCause::Arithmetic { required } => write!(
                f,
                "copy physical request arithmetic is not addressable (required {required:?})"
            ),
            ExtentCause::Backing { required, .. } => write!(
                f,
                "copy physical backing requires {required} bytes beyond the Rust allocation extent"
            ),
            ExtentCause::Format {
                layout,
                required,
                maximum,
            } => write!(
                f,
                "copy {layout:?} output requires {required:?} units beyond its format limit {maximum}"
            ),
        }
    }
}
impl Error for CopyExtentFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.0.as_ref() {
            ExtentCause::Backing {
                source: Some(source),
                ..
            } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for CopyExtentFailure {
    fn failure_fact(&self) -> FailureFact {
        match self.0.as_ref() {
            ExtentCause::Arithmetic { .. } => FailureFact::new(FailureClass::Resource, reason(257)),
            ExtentCause::Backing { required, .. } => {
                FailureFact::new(FailureClass::Representation, reason(258)).with_representation(
                    RepresentationFacts {
                        boundary: RepresentationBoundary::SingleOutput,
                        layout: RepresentationLayout {
                            domain: domain::KERNEL,
                            code: crate::allocation_failure::RUST_ARRAY_BACKING_LAYOUT,
                        },
                        required: u64::try_from(*required).ok(),
                        extent_limit: Some(isize::MAX as u64),
                        representable_prefix: None,
                    },
                )
            }
            // A layout-preserving copy of already-valid same-format inputs
            // proves an insufficient complete-output API, not an impossible
            // logical value. Do not infer MinimumUnit from a one-row call.
            ExtentCause::Format {
                layout,
                required,
                maximum,
            } => FailureFact::new(FailureClass::Representation, reason(259)).with_representation(
                RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: NonZeroU16::new(*layout as u16).unwrap(),
                    },
                    required: required.and_then(|n| u64::try_from(n).ok()),
                    extent_limit: u64::try_from(*maximum).ok(),
                    representable_prefix: None,
                },
            ),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CopyExtentFailure {}
fn reason(code: u16) -> FailureReason {
    FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap())
}
impl CopyError {
    pub(crate) fn arithmetic(required: Option<u128>) -> Self {
        Self::Extent(CopyExtentFailure(Box::new(ExtentCause::Arithmetic {
            required,
        })))
    }
    pub(crate) fn backing_bytes(required: u128) -> Self {
        Self::Extent(CopyExtentFailure(Box::new(ExtentCause::Backing {
            required,
            source: None,
        })))
    }
    pub(crate) fn layout<T>(count: usize, source: std::alloc::LayoutError) -> Self {
        Self::Extent(CopyExtentFailure(Box::new(ExtentCause::Backing {
            required: count as u128 * std::mem::size_of::<T>() as u128,
            source: Some(source),
        })))
    }
    pub(crate) fn format_extent(
        layout: CopyLayout,
        required: Option<u128>,
        maximum: usize,
    ) -> Self {
        Self::Extent(CopyExtentFailure(Box::new(ExtentCause::Format {
            layout,
            required,
            maximum,
        })))
    }
    pub(crate) fn unsupported(ty: &arrow_schema::DataType) -> Self {
        Self::Unsupported(BoundedDetail::truncating_display(format_args!(
            "constant broadcast does not support Arrow carrier {ty:?}"
        )))
    }
    pub fn into_kernel(self) -> KernelFailure {
        match self {
            Self::Control(source) => source,
            source => KernelFailure::from_domain(source),
        }
    }
}
impl FailureProjection for CopyError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Extent(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
            Self::Invalid(_) => FailureFact::new(FailureClass::Invariant, reason(260)),
            Self::Unsupported(_) => FailureFact::new(FailureClass::Invariant, reason(261)),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CopyError {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn copy_extent_producer_distinguishes_physical_backing_and_arrow_formats() {
        let source = std::alloc::Layout::array::<u64>(usize::MAX).unwrap_err();
        let error = CopyError::layout::<u64>(usize::MAX, source);
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert!(fact.resource().is_none());
        let extent = fact.representation().unwrap();
        assert_eq!(extent.required, None, "the actual byte extent exceeds u64");
        assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
        assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(extent.representable_prefix, None);
        let arithmetic = CopyError::arithmetic(None).failure_fact();
        assert_eq!(arithmetic.class(), FailureClass::Resource);
        assert!(
            arithmetic.representation().is_none(),
            "opaque accounting arithmetic proves no physical format boundary"
        );
        assert!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<std::alloc::LayoutError>()
        );
        let error =
            crate::selected_copy::byte_interleave_payload_extent(i32::MAX as usize + 1, false)
                .unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(
            fact.representation().unwrap().required,
            Some(i32::MAX as u64 + 1)
        );
        assert_eq!(
            fact.representation().unwrap().extent_limit,
            Some(i32::MAX as u64)
        );
        assert_eq!(
            fact.representation().unwrap().layout.code.get(),
            CopyLayout::Byte32 as u16
        );
        assert_eq!(fact.resource(), None);
        let error = crate::selected_copy::limit(
            i16::MAX as usize + 1,
            i16::MAX as usize,
            CopyLayout::RunEnd,
        )
        .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Representation);
        assert_eq!(
            error
                .failure_fact()
                .representation()
                .unwrap()
                .layout
                .code
                .get(),
            CopyLayout::RunEnd as u16
        );
    }
    #[test]
    fn copy_extent_metadata_failure_drops_original_nested_type_and_keeps_control_identity() {
        use std::sync::Arc;
        let field = Arc::new(arrow_schema::Field::new(
            "x".repeat(1025),
            arrow_schema::DataType::Int64,
            true,
        ));
        let weak = Arc::downgrade(&field);
        let ty = arrow_schema::DataType::FixedSizeList(field, 1);
        let error = CopyError::unsupported(&ty);
        drop(ty);
        assert!(weak.upgrade().is_none());
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert!(error.to_string().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
        let error = CopyError::Invalid("cancelled resource exhausted numeric overflow");
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        let original = KernelFailure::Cancelled;
        assert_eq!(CopyError::Control(original.clone()).into_kernel(), original);
    }
    #[test]
    fn copy_extent_actual_broadcast_keeps_exact_format_fact_without_allocating_the_output() {
        let source = arrow_array::StringArray::from(vec![Some("xy")]);
        let error =
            crate::selected_copy::preflight_broadcast(&source, 0, i32::MAX as usize, |_| Ok(()))
                .unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(
            fact.representation().unwrap().required,
            Some(i32::MAX as u64 * 2)
        );
        assert_eq!(
            fact.representation().unwrap().boundary,
            RepresentationBoundary::SingleOutput
        );
        assert_eq!(fact.resource(), None);
        assert_eq!(
            fact.representation().unwrap().extent_limit,
            Some(i32::MAX as u64)
        );
        assert_eq!(fact.representation().unwrap().representable_prefix, None);
    }
}
