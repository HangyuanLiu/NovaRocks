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

use std::sync::Arc;

use novarocks::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use novarocks::query_execution::report::{NativeReportHandler, NativeReportHandlerError};
use novarocks::query_execution::write::{NativeExecutionReport, decode_native_execution_report};

use super::query_registry::FrontendQueryRegistry;

#[derive(Clone)]
pub struct FrontendCoordinatorReportHandler {
    registry: Arc<FrontendQueryRegistry>,
}

impl FrontendCoordinatorReportHandler {
    pub(crate) fn new(registry: Arc<FrontendQueryRegistry>) -> Self {
        Self { registry }
    }

    pub fn handle_native_report(
        &self,
        report: NativeExecutionReport,
    ) -> Result<(), DistributedQueryError> {
        self.registry.record_report(report)
    }
}

impl NativeReportHandler for FrontendCoordinatorReportHandler {
    fn handle_native_report(
        &self,
        report: novarocks::proto::novarocks::ExecStatusReport,
    ) -> Result<(), NativeReportHandlerError> {
        let report = decode_native_execution_report(report)
            .map_err(NativeReportHandlerError::contract_violation)?;
        FrontendCoordinatorReportHandler::handle_native_report(self, report).map_err(|error| {
            match error.kind() {
                DistributedQueryErrorKind::Rejected => {
                    NativeReportHandlerError::query_gone(error.message())
                }
                DistributedQueryErrorKind::ContractViolation => {
                    NativeReportHandlerError::contract_violation(error.message())
                }
                DistributedQueryErrorKind::Failed => {
                    NativeReportHandlerError::failed(error.message())
                }
            }
        })
    }
}
