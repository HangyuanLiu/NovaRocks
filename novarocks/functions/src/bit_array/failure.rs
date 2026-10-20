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
//! Safe bit-array casts have no row-error channel: invalid values become NULL.
//! Carrier/unsupported conversion errors violate the admitted call contract.
use crate::KernelFailure;
use arrow_schema::ArrowError;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Debug)]
pub struct BitArrayError(Box<BitCause>);
#[derive(Debug)]
enum BitCause {
    CastArgument {
        index: usize,
        source: ArrowError,
    },
    ArgumentCarrier {
        index: usize,
        expected: &'static str,
    },
    CastOutput(ArrowError),
    LargeIntSlice(std::array::TryFromSliceError),
}
impl BitArrayError {
    pub(super) fn cast_argument(index: usize, source: ArrowError) -> Self {
        Self(Box::new(BitCause::CastArgument { index, source }))
    }
    pub(super) fn carrier(index: usize, expected: &'static str) -> Self {
        Self(Box::new(BitCause::ArgumentCarrier { index, expected }))
    }
    pub(super) fn cast_output(source: ArrowError) -> Self {
        Self(Box::new(BitCause::CastOutput(source)))
    }
    pub(super) fn slice(source: std::array::TryFromSliceError) -> Self {
        Self(Box::new(BitCause::LargeIntSlice(source)))
    }
    pub fn into_kernel(self) -> KernelFailure {
        KernelFailure::from_domain(self)
    }
}
impl fmt::Display for BitArrayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            BitCause::CastArgument { index, source } => {
                write!(f, "bit argument {index} cast to BIGINT failed: {source}")
            }
            BitCause::ArgumentCarrier { index, expected } => {
                write!(f, "bit argument {index} is not {expected}")
            }
            BitCause::CastOutput(source) => write!(f, "bit output cast failed: {source}"),
            BitCause::LargeIntSlice(source) => write!(
                f,
                "bit LARGEINT carrier has an invalid byte width: {source}"
            ),
        }
    }
}
impl Error for BitArrayError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.0.as_ref() {
            BitCause::CastArgument { source, .. } | BitCause::CastOutput(source) => Some(source),
            BitCause::LargeIntSlice(source) => Some(source),
            BitCause::ArgumentCarrier { .. } => None,
        }
    }
}
fn reason(code: u16) -> FailureReason {
    FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap())
}
impl FailureProjection for BitArrayError {
    fn failure_fact(&self) -> FailureFact {
        let source = match self.0.as_ref() {
            BitCause::CastArgument { source, .. } | BitCause::CastOutput(source) => source,
            _ => return FailureFact::new(FailureClass::Invariant, reason(266)),
        };
        match source {
            ArrowError::MemoryError(_) => FailureFact::new(FailureClass::Resource, reason(262)),
            ArrowError::OffsetOverflowError(_) => {
                FailureFact::new(FailureClass::Representation, reason(263)).with_representation(
                    RepresentationFacts {
                        boundary: RepresentationBoundary::SingleOutput,
                        layout: RepresentationLayout {
                            domain: domain::KERNEL,
                            code: NonZeroU16::new(54).unwrap(),
                        },
                        required: None,
                        extent_limit: None,
                        representable_prefix: None,
                    },
                )
            }
            ArrowError::IoError(_, _) => FailureFact::new(FailureClass::Unexpected, reason(264)),
            _ => FailureFact::new(FailureClass::Invariant, reason(265)),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for BitArrayError {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bit_safe_cast_keeps_the_actual_arrow_source_and_ignores_its_words() {
        let cause =
            BitArrayError::cast_output(ArrowError::MemoryError("numeric invalid encoding".into()));
        assert_eq!(cause.failure_fact().class(), FailureClass::Resource);
        assert!(cause.source().unwrap().is::<ArrowError>());
        let cause = BitArrayError::cast_argument(
            1,
            ArrowError::CastError("resource exhausted cancelled".into()),
        );
        assert_eq!(cause.failure_fact().class(), FailureClass::Invariant);
        let kernel = cause.into_kernel();
        assert_eq!(kernel.failure_fact().class(), FailureClass::Invariant);
        assert!(kernel.source().is_some());
    }
    #[test]
    fn bit_real_safe_cast_turns_invalid_input_into_null_and_preserves_values() {
        let array: arrow_array::ArrayRef =
            std::sync::Arc::new(arrow_array::StringArray::from(vec![
                Some("13"),
                Some("overflow"),
                None,
            ]));
        let out = super::super::to_i64_array(&array, 0).unwrap();
        assert_eq!(
            out,
            arrow_array::Int64Array::from(vec![Some(13), None, None])
        );
        let unsupported: arrow_array::ArrayRef =
            std::sync::Arc::new(arrow_array::StructArray::new_empty_fields(1, None));
        let error = super::super::to_i64_array(&unsupported, 0);
        assert!(error.is_err());
    }
}
