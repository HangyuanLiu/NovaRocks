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

//! The Worker-owned refusal vocabulary for runtime-filter contracts.
//!
//! Native decode and transport adapters may construct this error, but task
//! lifecycle ownership decides how the bounded contract refusal affects a
//! context or task.  Keeping the vocabulary here prevents an adapter-local
//! error type from becoming a second runtime-filter authority.

/// Why a runtime-filter contract path refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterContractErrorCode {
    /// The contribution, install, session binding, or terminal projection is
    /// structurally illegal or disagrees with the attempt it names.
    InvalidContract,
    /// The participant that would have to answer is not the one this attempt
    /// installed, so there is nothing left to bind.
    ParticipantClosed,
}

/// One runtime-filter contract refusal, with the detail its producer wrote.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterContractError {
    code: RuntimeFilterContractErrorCode,
    detail: String,
}

impl RuntimeFilterContractError {
    pub fn new(code: RuntimeFilterContractErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub fn invalid_contract(detail: impl Into<String>) -> Self {
        Self::new(RuntimeFilterContractErrorCode::InvalidContract, detail)
    }

    pub const fn code(&self) -> RuntimeFilterContractErrorCode {
        self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for RuntimeFilterContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.detail)
    }
}

impl std::error::Error for RuntimeFilterContractError {}

#[cfg(test)]
mod tests {
    use super::{RuntimeFilterContractError, RuntimeFilterContractErrorCode};

    #[test]
    fn refusal_retains_its_exact_code_and_detail() {
        let error = RuntimeFilterContractError::new(
            RuntimeFilterContractErrorCode::ParticipantClosed,
            "participant already released",
        );

        assert_eq!(
            error.code(),
            RuntimeFilterContractErrorCode::ParticipantClosed
        );
        assert_eq!(error.detail(), "participant already released");
        assert_eq!(
            error.to_string(),
            "ParticipantClosed: participant already released"
        );
    }
}
