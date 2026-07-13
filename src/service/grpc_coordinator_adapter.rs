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

use crate::common::engine_error::EngineError;
use crate::coordinator::ports::{CoordinatorObserver, CoordinatorReportHandler};
use crate::runtime::endpoint::RuntimeEndpoint;

pub(crate) struct PrometheusCoordinatorObserver;

impl CoordinatorObserver for PrometheusCoordinatorObserver {
    fn fragment_scheduled(&self) {
        crate::service::metrics_http::observe_fragment_scheduled();
    }
}

pub(crate) struct LegacyCoordinatorReportHandler;

impl CoordinatorReportHandler for LegacyCoordinatorReportHandler {
    fn handle_exec_status_report(
        &self,
        report: crate::proto::novarocks::ExecStatusReport,
    ) -> Result<(), EngineError> {
        let failure =
            failed_query_from_native_report(&report).map_err(EngineError::protocol_decode)?;
        let profile_report_accepted =
            crate::coordinator::profile::record_native_standalone_query_profile_report(&report)
                .map_err(EngineError::protocol_decode)?;
        match crate::runtime::write_coordinator::lookup_native_writer_report(&report)
            .map_err(EngineError::protocol_decode)?
        {
            crate::runtime::write_coordinator::WriterReportLookup::Expected => {
                let result = crate::runtime::write_report::report_from_native(report)
                    .map_err(EngineError::protocol_decode)
                    .and_then(
                        crate::runtime::write_coordinator::handle_fragment_report_exec_status,
                    );
                match result {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        if let Some(failure) = failure {
                            mark_failed_query_report(failure);
                        }
                        Err(err)
                    }
                }
            }
            crate::runtime::write_coordinator::WriterReportLookup::UnknownWriter { query_id } => {
                if !report.iceberg_commits.is_empty() {
                    let message = format!(
                        "unknown writer report with write metadata for query {}/{}, fragment {}/{}, backend {}",
                        query_id.hi,
                        query_id.lo,
                        report
                            .fragment_instance_id
                            .as_ref()
                            .map(|id| id.hi)
                            .unwrap_or_default(),
                        report
                            .fragment_instance_id
                            .as_ref()
                            .map(|id| id.lo)
                            .unwrap_or_default(),
                        report.backend_num,
                    );
                    crate::runtime::write_coordinator::mark_query_failed(
                        &query_id,
                        message.clone(),
                    );
                    return Err(EngineError::distributed_write_output_mismatch(
                        "reportExecStatus",
                        message,
                    ));
                }
                if let Some(failure) = failure {
                    crate::runtime::write_coordinator::mark_query_failed(
                        &query_id,
                        failure.error.clone(),
                    );
                    mark_failed_query_report(failure);
                }
                Ok(())
            }
            crate::runtime::write_coordinator::WriterReportLookup::UnknownQuery { query_id } => {
                if let Some(failure) = failure {
                    mark_failed_query_report(failure);
                    Ok(())
                } else if profile_report_accepted {
                    Ok(())
                } else {
                    Err(EngineError::write_coordinator_gone(query_id))
                }
            }
        }
    }
}

struct FailedQueryReport {
    query_id: crate::runtime::query_context::QueryId,
    finst_id: crate::common::types::UniqueId,
    error: String,
}

fn failed_query_from_native_report(
    report: &crate::proto::novarocks::ExecStatusReport,
) -> Result<Option<FailedQueryReport>, String> {
    let Some(status) = report.status.as_ref() else {
        return Ok(None);
    };
    if status.code == 0 {
        return Ok(None);
    }
    let query = report
        .query_id
        .as_ref()
        .ok_or_else(|| "ExecStatusReport missing query_id".to_string())?;
    let finst = report
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "ExecStatusReport missing fragment_instance_id".to_string())?;
    let error = if status.message.is_empty() {
        format!("status={}", status.code)
    } else {
        status.message.clone()
    };
    Ok(Some(FailedQueryReport {
        query_id: crate::runtime::query_context::QueryId {
            hi: query.hi,
            lo: query.lo,
        },
        finst_id: crate::common::types::UniqueId {
            hi: finst.hi,
            lo: finst.lo,
        },
        error,
    }))
}

fn mark_failed_query_report(report: FailedQueryReport) {
    crate::service::fragment_control::mark_query_failed_from_report(
        report.query_id,
        report.finst_id,
        report.error,
    );
}

pub(crate) fn coordinator_report_endpoint() -> Result<RuntimeEndpoint, String> {
    let cfg = crate::novarocks_config::config()
        .map_err(|e| format!("cannot read coordinator config: {e}"))?;
    let advertised_host = crate::common::network::advertise_host().ok();
    let bound_port = crate::service::grpc_server::grpc_server_bound_port().ok();
    select_coordinator_report_endpoint(
        &cfg.server.host,
        cfg.server.grpc_port,
        advertised_host.as_deref(),
        bound_port,
    )
}

fn select_coordinator_report_endpoint(
    configured_host: &str,
    configured_port: u16,
    advertised_host: Option<&str>,
    bound_port: Option<u16>,
) -> Result<RuntimeEndpoint, String> {
    RuntimeEndpoint::new(
        advertised_host.unwrap_or(configured_host),
        i32::from(bound_port.unwrap_or(configured_port)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_endpoint_prefers_advertised_host_and_bound_port() {
        let endpoint = select_coordinator_report_endpoint(
            "configured.internal",
            9070,
            Some("advertised.internal"),
            Some(19070),
        )
        .expect("report endpoint");

        assert_eq!(endpoint.host(), "advertised.internal");
        assert_eq!(endpoint.port(), 19070);
    }

    #[test]
    fn report_endpoint_falls_back_to_configured_host_and_port() {
        let endpoint = select_coordinator_report_endpoint("configured.internal", 9070, None, None)
            .expect("report endpoint");

        assert_eq!(endpoint.host(), "configured.internal");
        assert_eq!(endpoint.port(), 9070);
    }
}
