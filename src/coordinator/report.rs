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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

use crate::common::engine_error::EngineError;
use crate::common::types::UniqueId;
use crate::coordinator::ports::CoordinatorReportHandler;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::{exchange, result_buffer};

pub(crate) struct CoordinatorExecStatusReportHandler;

impl CoordinatorReportHandler for CoordinatorExecStatusReportHandler {
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
    query_id: QueryId,
    finst_id: UniqueId,
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
        query_id: QueryId {
            hi: query.hi,
            lo: query.lo,
        },
        finst_id: UniqueId {
            hi: finst.hi,
            lo: finst.lo,
        },
        error,
    }))
}

fn mark_failed_query_report(report: FailedQueryReport) {
    mark_query_failed_from_report(report.query_id, report.finst_id, report.error);
}

#[derive(Default)]
struct StandaloneQueryFailureRegistry {
    active: BTreeSet<(i64, i64)>,
    failures: BTreeMap<(i64, i64), String>,
}

fn standalone_query_failures() -> &'static Mutex<StandaloneQueryFailureRegistry> {
    static REGISTRY: OnceLock<Mutex<StandaloneQueryFailureRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(StandaloneQueryFailureRegistry::default()))
}

fn query_failure_key(query_id: &UniqueId) -> (i64, i64) {
    (query_id.hi, query_id.lo)
}

fn record_standalone_query_failure(query_id: QueryId, error: String) {
    let key = (query_id.hi, query_id.lo);
    let mut guard = standalone_query_failures()
        .lock()
        .expect("standalone query failure registry lock");
    if guard.active.contains(&key) {
        guard.failures.entry(key).or_insert(error);
    }
}

pub(crate) fn take_standalone_query_failure(query_id: &UniqueId) -> Option<String> {
    standalone_query_failures()
        .lock()
        .expect("standalone query failure registry lock")
        .failures
        .remove(&query_failure_key(query_id))
}

pub(crate) struct StandaloneQueryFailureGuard {
    key: (i64, i64),
}

impl StandaloneQueryFailureGuard {
    pub(crate) fn register(query_id: &UniqueId) -> Self {
        let key = query_failure_key(query_id);
        let mut guard = standalone_query_failures()
            .lock()
            .expect("standalone query failure registry lock");
        guard.failures.remove(&key);
        guard.active.insert(key);
        Self { key }
    }
}

impl Drop for StandaloneQueryFailureGuard {
    fn drop(&mut self) {
        let mut guard = standalone_query_failures()
            .lock()
            .expect("standalone query failure registry lock");
        guard.active.remove(&self.key);
        guard.failures.remove(&self.key);
    }
}

pub(crate) fn mark_query_failed_from_report(query_id: QueryId, finst_id: UniqueId, error: String) {
    record_standalone_query_failure(query_id, error.clone());
    let mgr = query_context_manager();
    let mut finsts = mgr.cancel_query(query_id, error.clone());
    if !finsts.contains(&finst_id) {
        finsts.push(finst_id);
    }
    for id in finsts {
        result_buffer::close_error(id, error.clone());
        exchange::cancel_fragment(id.hi, id.lo);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::common::{Status, UniqueId as ProtoUniqueId};
    use crate::proto::novarocks::{ExecStatusReport, ProfileNode, RuntimeProfileTree};
    use crate::runtime::exchange::{ExchangeKey, set_expected_senders, snapshot_receiver_state};
    use crate::runtime::result_buffer::{FetchErrorKind, TryFetchResult};

    fn report(query: UniqueId, finst: UniqueId) -> ExecStatusReport {
        ExecStatusReport {
            query_id: Some(ProtoUniqueId {
                hi: query.hi,
                lo: query.lo,
            }),
            fragment_instance_id: Some(ProtoUniqueId {
                hi: finst.hi,
                lo: finst.lo,
            }),
            status: Some(Status::default()),
            done: true,
            ..Default::default()
        }
    }

    #[test]
    fn failure_registry_is_first_wins_and_raii_scoped() {
        let query = UniqueId { hi: 520_001, lo: 1 };
        let runtime_query = QueryId {
            hi: query.hi,
            lo: query.lo,
        };
        record_standalone_query_failure(runtime_query, "inactive".to_string());
        assert_eq!(take_standalone_query_failure(&query), None);

        {
            let _guard = StandaloneQueryFailureGuard::register(&query);
            record_standalone_query_failure(runtime_query, "first".to_string());
            record_standalone_query_failure(runtime_query, "second".to_string());
            assert_eq!(
                take_standalone_query_failure(&query),
                Some("first".to_string())
            );
            record_standalone_query_failure(runtime_query, "drop-clears".to_string());
        }
        assert_eq!(take_standalone_query_failure(&query), None);
    }

    #[test]
    fn profile_only_unknown_query_report_is_accepted() {
        let query = UniqueId { hi: 520_002, lo: 1 };
        let finst = UniqueId { hi: 520_002, lo: 2 };
        let _profile_guard =
            crate::coordinator::profile::StandaloneQueryProfileGuard::register(&query);
        let mut report = report(query, finst);
        report.profile = Some(RuntimeProfileTree {
            root: Some(ProfileNode {
                name: "root".to_string(),
                node_id: 7,
                ..Default::default()
            }),
        });

        CoordinatorExecStatusReportHandler
            .handle_exec_status_report(report)
            .expect("active profile-only report must be accepted");
    }

    #[test]
    fn report_failure_fans_out_to_query_peers_and_result_buffer() {
        let query_id = QueryId { hi: 520_003, lo: 1 };
        let finst_a = UniqueId { hi: 520_003, lo: 2 };
        let finst_b = UniqueId { hi: 520_003, lo: 3 };
        let key_a = ExchangeKey {
            finst_id_hi: finst_a.hi,
            finst_id_lo: finst_a.lo,
            node_id: 51,
        };
        let key_b = ExchangeKey {
            finst_id_hi: finst_b.hi,
            finst_id_lo: finst_b.lo,
            node_id: 52,
        };
        let mgr = query_context_manager();
        mgr.register_finst(finst_a, query_id);
        mgr.register_finst(finst_b, query_id);
        result_buffer::create_sender(finst_a);
        set_expected_senders(key_a, 1);
        set_expected_senders(key_b, 1);

        mark_query_failed_from_report(query_id, finst_a, "remote failure".to_string());

        let TryFetchResult::Error(err) = result_buffer::try_fetch(finst_a) else {
            panic!("report failure must close the result buffer");
        };
        assert!(matches!(err.kind, FetchErrorKind::Failed));
        assert!(err.message.contains("remote failure"));
        assert!(snapshot_receiver_state(key_a).is_none());
        assert!(snapshot_receiver_state(key_b).is_none());

        mgr.unregister_finst(finst_a);
        mgr.unregister_finst(finst_b);
    }

    #[test]
    fn report_failure_closes_unregistered_finst_buffer() {
        let query_id = QueryId { hi: 520_004, lo: 1 };
        let finst_id = UniqueId { hi: 520_004, lo: 2 };
        result_buffer::create_sender(finst_id);
        let mgr = query_context_manager();
        mgr.register_finst(finst_id, query_id);
        mgr.unregister_finst(finst_id);

        mark_query_failed_from_report(
            query_id,
            finst_id,
            "standalone final reportExecStatus failed: coordinator unreachable".to_string(),
        );

        let TryFetchResult::Error(err) = result_buffer::try_fetch(finst_id) else {
            panic!("final report failure must be observable through result_buffer");
        };
        assert!(matches!(err.kind, FetchErrorKind::Failed));
        assert!(err.message.contains("coordinator unreachable"));
    }
}
