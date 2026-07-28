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

//! Neutral native report transport port.

use crate::common::engine_error::EngineError;

pub const NATIVE_REPORT_ROLE_REJECTED_ERROR_CODE: &str = "NativeReportRoleRejected";
pub const NATIVE_REPORT_CONTRACT_VIOLATION_ERROR_CODE: &str = "NativeReportContractViolation";
pub const NATIVE_REPORT_FAILED_ERROR_CODE: &str = "NativeReportFailed";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeReportHandlerError {
    status_code: i32,
    message: String,
    error_code: String,
}

impl NativeReportHandlerError {
    pub fn role_rejected(message: impl Into<String>) -> Self {
        Self {
            status_code: crate::common::engine_error::REPORT_EXEC_STATUS_ERROR,
            message: message.into(),
            error_code: NATIVE_REPORT_ROLE_REJECTED_ERROR_CODE.to_string(),
        }
    }

    pub fn query_gone(message: impl Into<String>) -> Self {
        Self {
            status_code: crate::common::engine_error::REPORT_EXEC_STATUS_QUERY_GONE,
            message: message.into(),
            error_code: crate::common::engine_error::EngineErrorCode::WriteCoordinatorGone
                .as_str()
                .to_string(),
        }
    }

    pub fn contract_violation(message: impl Into<String>) -> Self {
        Self {
            status_code: crate::common::engine_error::REPORT_EXEC_STATUS_ERROR,
            message: message.into(),
            error_code: NATIVE_REPORT_CONTRACT_VIOLATION_ERROR_CODE.to_string(),
        }
    }

    pub fn failed(message: impl Into<String>) -> Self {
        Self {
            status_code: crate::common::engine_error::REPORT_EXEC_STATUS_ERROR,
            message: message.into(),
            error_code: NATIVE_REPORT_FAILED_ERROR_CODE.to_string(),
        }
    }

    pub const fn status_code(&self) -> i32 {
        self.status_code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn error_code(&self) -> &str {
        &self.error_code
    }
}

impl From<EngineError> for NativeReportHandlerError {
    fn from(error: EngineError) -> Self {
        Self {
            status_code: error.to_report_status_code(),
            message: error.to_user_message(),
            error_code: error.to_report_error_code().to_string(),
        }
    }
}

impl std::fmt::Display for NativeReportHandlerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for NativeReportHandlerError {}

pub trait NativeReportHandler: Send + Sync + 'static {
    fn handle_native_report(
        &self,
        report: crate::proto::novarocks::ExecStatusReport,
    ) -> Result<(), NativeReportHandlerError>;
}
