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

use super::KernelFailure;

/// Test observation retains the actual reference error. Fixture setup and
/// panics have no domain fact and cannot authorize a runtime Data match.
#[derive(Debug)]
pub(super) enum ReferenceFailure {
    Source(Box<dyn std::error::Error>),
    Panicked(String),
    Fixture(String),
}
impl From<String> for ReferenceFailure {
    fn from(message: String) -> Self {
        Self::Fixture(message)
    }
}
impl From<&str> for ReferenceFailure {
    fn from(message: &str) -> Self {
        Self::Fixture(message.into())
    }
}
impl std::fmt::Display for ReferenceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(source) => source.fmt(f),
            Self::Panicked(payload) => write!(f, "{}{payload}", super::LEGACY_PANIC_PREFIX),
            Self::Fixture(message) => f.write_str(message),
        }
    }
}
impl ReferenceFailure {
    pub(super) fn matches_kernel(&self, cause: &KernelFailure) -> bool {
        use novarocks_failure_contract::{BoundedDetail, FailureProjection};
        self.original().is_some_and(|(fact, diagnostic)| {
            fact == cause.failure_fact()
                && BoundedDetail::truncating_display(&diagnostic)
                    == BoundedDetail::truncating_display(cause)
        })
    }
    pub(super) fn original(&self) -> Option<(novarocks_failure_contract::FailureFact, String)> {
        use crate::runtime::fragment::ExecutionFailure;
        use novarocks_failure_contract::FailureProjection;
        let Self::Source(original) = self else {
            return None;
        };
        let mut source: &dyn std::error::Error = original.as_ref();
        let mut execution = None;
        loop {
            // The reference runtime's explicit envelope is the authority.
            // Follow its real source to compare the producer diagnostic without
            // phase context; do not classify arbitrary fixture text or Arrow.
            if let Some(error) = source.downcast_ref::<ExecutionFailure>() {
                execution = Some(error);
            }
            if let Some(error) = source.downcast_ref::<KernelFailure>() {
                return Some((error.failure_fact(), error.to_string()));
            }
            match source.source() {
                Some(next) => source = next,
                None => break,
            }
        }
        execution.map(|error| (error.failure_fact(), error.detail().to_owned()))
    }
}
