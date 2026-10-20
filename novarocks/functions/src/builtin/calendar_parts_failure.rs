// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Calendar extraction and its safe output projection own these facts.
use super::calendar_extended::CalendarCarrierError;
use crate::KernelFailure;
use arrow_schema::ArrowError;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Debug)]
struct CalendarPartFailure(Box<PartCause>);
#[derive(Debug)]
enum PartCause {
    Carrier(BoundedDetail),
    Contract(BoundedDetail),
    Output(ArrowError),
}
pub(super) fn carrier(error: CalendarCarrierError<'_>) -> KernelFailure {
    // Format boundedly at the producer: no borrowed or owned DataType graph
    // escapes this call and no full diagnostic String is created first.
    let detail = match error {
        CalendarCarrierError::UnsupportedDatetime(ty) => BoundedDetail::truncating_display(
            format_args!("unsupported datetime input type: {ty:?}"),
        ),
        CalendarCarrierError::UnsupportedDate(ty) => {
            BoundedDetail::truncating_display(format_args!("unsupported date input type: {ty:?}"))
        }
        CalendarCarrierError::Downcast(detail) => BoundedDetail::truncating(detail),
    };
    KernelFailure::from_domain(CalendarPartFailure(Box::new(PartCause::Carrier(detail))))
}
pub(super) fn contract(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(CalendarPartFailure(Box::new(PartCause::Contract(
        BoundedDetail::truncating_display(detail),
    ))))
}
pub(super) fn output(source: ArrowError) -> KernelFailure {
    KernelFailure::from_domain(CalendarPartFailure(Box::new(PartCause::Output(source))))
}
impl fmt::Display for CalendarPartFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            PartCause::Carrier(detail) | PartCause::Contract(detail) => {
                f.write_str(detail.as_str())
            }
            PartCause::Output(source) => write!(f, "calendar array cast failed: {source}"),
        }
    }
}
impl Error for CalendarPartFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.0.as_ref() {
            PartCause::Output(source) => Some(source),
            _ => None,
        }
    }
}
fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
    )
}
impl FailureProjection for CalendarPartFailure {
    fn failure_fact(&self) -> FailureFact {
        let source = match self.0.as_ref() {
            PartCause::Carrier(_) => return fact(FailureClass::Invariant, 273),
            PartCause::Contract(_) => return fact(FailureClass::Invariant, 274),
            PartCause::Output(source) => source,
        };
        match source {
            ArrowError::MemoryError(_) => fact(FailureClass::Resource, 275),
            ArrowError::OffsetOverflowError(_) => fact(FailureClass::Representation, 276)
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: NonZeroU16::new(56).unwrap(),
                    },
                    required: None,
                    extent_limit: None,
                    representable_prefix: None,
                }),
            ArrowError::IoError(..) => fact(FailureClass::Unexpected, 277),
            _ => fact(FailureClass::Invariant, 278),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CalendarPartFailure {}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn calendar_extraction_failure_drops_type_storage_and_keeps_actual_cast_source() {
        use std::sync::Arc;
        let field = Arc::new(arrow_schema::Field::new(
            "x".repeat(1025),
            arrow_schema::DataType::Int64,
            true,
        ));
        let weak = Arc::downgrade(&field);
        let ty = arrow_schema::DataType::Struct(vec![field].into());
        let error = carrier(CalendarCarrierError::UnsupportedDatetime(&ty));
        drop(ty);
        assert!(weak.upgrade().is_none());
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert!(error.to_string().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
        let error = output(ArrowError::MemoryError("invalid numeric encoding".into()));
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        let mut source: &dyn Error = &error;
        while !source.is::<ArrowError>() {
            source = source.source().unwrap();
        }
        assert!(source.to_string().contains("invalid numeric encoding"));
    }
}
