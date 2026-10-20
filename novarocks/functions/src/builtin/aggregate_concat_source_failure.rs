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
//! GROUP_CONCAT intermediate construction owns the actual Arrow constructor.
use crate::KernelFailure;
use arrow_schema::ArrowError;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, OuterFailureProjection, domain,
};
use std::{fmt, num::NonZeroU16};
#[derive(Debug)]
struct ConcatConstructionFailure(ArrowError);
impl fmt::Display for ConcatConstructionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "group_concat intermediate construction failed: {}",
            self.0
        )
    }
}
impl std::error::Error for ConcatConstructionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for ConcatConstructionFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self.0 {
            ArrowError::MemoryError(_) => (FailureClass::Resource, 336),
            _ => (FailureClass::Invariant, 337),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
        )
    }
}
impl OuterFailureProjection for ConcatConstructionFailure {}
pub(super) fn constructor(source: ArrowError) -> KernelFailure {
    KernelFailure::from_domain(ConcatConstructionFailure(source))
}
