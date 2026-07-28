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

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::sync::{Arc, Mutex};

use novarocks::UniqueId;
use novarocks::query_execution::backend::LiveBackendTarget;
use novarocks::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent, QueryId,
};
use novarocks::query_execution::fragment_transport::FragmentDispatcher;
use novarocks::query_execution::write::NativeExecutionReport;

type QueryKey = (i64, i64);

struct ActiveQuery {
    intent: DistributedQueryIntent,
    dispatcher: Arc<dyn FragmentDispatcher>,
    scheduled_backends: BTreeMap<usize, u64>,
    attempted: BTreeMap<usize, Vec<UniqueId>>,
    writer_instances: BTreeMap<UniqueId, i32>,
    reports: Vec<NativeExecutionReport>,
    writer_report_indexes: BTreeMap<UniqueId, usize>,
    reports_sealed: bool,
    final_report_instances: BTreeSet<UniqueId>,
    profile_report_instances: BTreeSet<UniqueId>,
    has_failed_final_report: bool,
    first_failure: Option<String>,
    submissions_inflight: usize,
    cancellation_requested: bool,
    cancellation_dispatched: bool,
}

#[derive(Default)]
struct BackendTopologyState {
    initialized: bool,
    revision: u64,
    live_generations: BTreeMap<usize, u64>,
}

#[derive(Default)]
pub(crate) struct FrontendQueryRegistry {
    active: Mutex<BTreeMap<QueryKey, ActiveQuery>>,
    backend_topology: Mutex<BackendTopologyState>,
}

impl FrontendQueryRegistry {
    pub(crate) fn register(
        self: &Arc<Self>,
        query_id: QueryId,
        intent: DistributedQueryIntent,
        dispatcher: Arc<dyn FragmentDispatcher>,
    ) -> Result<ActiveQueryGuard, DistributedQueryError> {
        let key = query_key(query_id);
        let mut active = self.active.lock().expect("frontend query registry lock");
        match active.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(ActiveQuery {
                    intent,
                    dispatcher,
                    scheduled_backends: BTreeMap::new(),
                    attempted: BTreeMap::new(),
                    writer_instances: BTreeMap::new(),
                    reports: Vec::new(),
                    writer_report_indexes: BTreeMap::new(),
                    reports_sealed: false,
                    final_report_instances: BTreeSet::new(),
                    profile_report_instances: BTreeSet::new(),
                    has_failed_final_report: false,
                    first_failure: None,
                    submissions_inflight: 0,
                    cancellation_requested: false,
                    cancellation_dispatched: false,
                });
            }
            Entry::Occupied(_) => {
                return Err(contract_violation(format!(
                    "frontend query {}/{} is already active",
                    query_id.high(),
                    query_id.low()
                )));
            }
        }
        Ok(ActiveQueryGuard {
            registry: Arc::clone(self),
            key,
        })
    }

    pub(crate) fn record_attempt(
        &self,
        query_id: QueryId,
        backend_idx: usize,
        finst_id: UniqueId,
    ) -> Result<(), DistributedQueryError> {
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| inactive_query(query_id))?;
        if let Some(message) = query.first_failure.as_ref() {
            return Err(failed(message.clone()));
        }
        if query.cancellation_requested {
            return Err(failed("frontend query cancellation is already requested"));
        }
        query
            .attempted
            .entry(backend_idx)
            .or_default()
            .push(finst_id);
        query.submissions_inflight += 1;
        Ok(())
    }

    pub(crate) fn set_scheduled_backend_ownership(
        &self,
        query_id: QueryId,
        backend_ownership: &[(usize, u64)],
    ) -> Result<(), DistributedQueryError> {
        let topology = self
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        if topology.initialized {
            for &(backend_idx, start_epoch) in backend_ownership {
                match topology.live_generations.get(&backend_idx) {
                    Some(current_epoch) if *current_epoch == start_epoch => {}
                    Some(current_epoch) => {
                        return Err(DistributedQueryError::new(
                            DistributedQueryErrorKind::Rejected,
                            format!(
                                "scheduled backend {backend_idx} generation {start_epoch} is stale; current generation is {current_epoch}"
                            ),
                        ));
                    }
                    None => {
                        return Err(DistributedQueryError::new(
                            DistributedQueryErrorKind::Rejected,
                            format!(
                                "scheduled backend {backend_idx} is no longer live in the current frontend topology"
                            ),
                        ));
                    }
                }
            }
        }
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| inactive_query(query_id))?;
        if !query.scheduled_backends.is_empty() {
            return Err(contract_violation(
                "frontend query scheduled backend ownership is already registered",
            ));
        }
        for &(backend_idx, start_epoch) in backend_ownership {
            if query
                .scheduled_backends
                .insert(backend_idx, start_epoch)
                .is_some()
            {
                return Err(contract_violation(
                    "frontend query scheduled backend ownership contains duplicate backend ids",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn replace_live_backends(&self, revision: u64, backends: &[LiveBackendTarget]) {
        let mut topology = self
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        if topology.initialized && revision < topology.revision {
            return;
        }
        topology.initialized = true;
        topology.revision = revision;
        topology.live_generations = backends
            .iter()
            .map(|target| (target.backend_idx(), target.start_epoch()))
            .collect();
    }

    #[cfg(test)]
    pub(crate) fn set_scheduled_backends(
        &self,
        query_id: QueryId,
        backend_ids: &[usize],
    ) -> Result<(), DistributedQueryError> {
        let ownership = backend_ids
            .iter()
            .map(|&backend_idx| (backend_idx, 0))
            .collect::<Vec<_>>();
        self.set_scheduled_backend_ownership(query_id, &ownership)
    }

    pub(crate) fn finish_attempt(&self, query_id: QueryId) -> Result<(), DistributedQueryError> {
        let cancellation = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            let query = active
                .get_mut(&query_key(query_id))
                .ok_or_else(|| inactive_query(query_id))?;
            query.submissions_inflight =
                query.submissions_inflight.checked_sub(1).ok_or_else(|| {
                    contract_violation("frontend query submission accounting underflow")
                })?;
            cancellation_if_ready(query)
        };
        dispatch_cancellation(cancellation);
        Ok(())
    }

    pub(crate) fn record_report(
        &self,
        report: NativeExecutionReport,
    ) -> Result<(), DistributedQueryError> {
        let query_id = report.query_id();
        let fragment_instance_id = report.fragment_instance_id();
        let backend_num = report.backend_num();
        let has_write_metadata = report.has_write_metadata();
        let report_failure = report.failure_message().map(ToString::to_string);
        let (cancellation, report_error) = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            let query = active
                .get_mut(&query_key(query_id))
                .ok_or_else(|| inactive_query(query_id))?;
            if query.reports_sealed {
                return Err(DistributedQueryError::new(
                    DistributedQueryErrorKind::Rejected,
                    "frontend query report aggregation is already sealed",
                ));
            }
            let was_attempted = query
                .attempted
                .values()
                .any(|instances| instances.contains(&fragment_instance_id));
            if !was_attempted {
                return Err(contract_violation(format!(
                    "frontend query received a report for unattempted fragment instance {}/{}",
                    fragment_instance_id.hi, fragment_instance_id.lo
                )));
            }
            let expected_writer = query
                .writer_instances
                .get(&fragment_instance_id)
                .is_some_and(|expected_backend_num| *expected_backend_num == backend_num);
            let unexpected_writer_error = (query.intent == DistributedQueryIntent::Write
                && !expected_writer
                && has_write_metadata)
                .then(|| {
                    format!(
                        "unknown writer report with write metadata for query {}/{}, fragment {}/{}",
                        query_id.high(),
                        query_id.low(),
                        fragment_instance_id.hi,
                        fragment_instance_id.lo
                    )
                });
            let mut conflicting_writer_error = None;
            if report.is_final() {
                query.has_failed_final_report |= report.is_failed();
                match query.intent {
                    DistributedQueryIntent::Write if expected_writer => {
                        query.final_report_instances.insert(fragment_instance_id);
                        if let Some(&index) = query.writer_report_indexes.get(&fragment_instance_id)
                        {
                            if !query.reports[index].same_write_report(&report) {
                                let message =
                                    "frontend query received conflicting final writer output"
                                        .to_string();
                                conflicting_writer_error = Some(message);
                            }
                        } else {
                            query
                                .writer_report_indexes
                                .insert(fragment_instance_id, query.reports.len());
                            query.reports.push(report);
                        }
                    }
                    DistributedQueryIntent::Write => {}
                    DistributedQueryIntent::Profile if report.has_profile() => {
                        query.profile_report_instances.insert(fragment_instance_id);
                        if let Some(existing) = query.reports.iter_mut().find(|existing| {
                            existing.fragment_instance_id() == report.fragment_instance_id()
                        }) {
                            *existing = report;
                        } else {
                            query.reports.push(report);
                        }
                    }
                    DistributedQueryIntent::Result | DistributedQueryIntent::Profile => {}
                }
            }
            if let Some(message) = unexpected_writer_error
                .as_ref()
                .or(conflicting_writer_error.as_ref())
            {
                query.first_failure.get_or_insert_with(|| message.clone());
            }
            if let Some(message) = report_failure {
                query.first_failure.get_or_insert(message);
            }
            (
                query
                    .first_failure
                    .is_some()
                    .then(|| request_cancellation(query))
                    .flatten(),
                unexpected_writer_error.or(conflicting_writer_error),
            )
        };
        dispatch_cancellation(cancellation);
        match report_error {
            Some(message) => Err(contract_violation(message)),
            None => Ok(()),
        }
    }

    pub(crate) fn set_writer_instances(
        &self,
        query_id: QueryId,
        writer_instances: &[(UniqueId, i32)],
    ) -> Result<(), DistributedQueryError> {
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| inactive_query(query_id))?;
        if query.intent != DistributedQueryIntent::Write && !writer_instances.is_empty() {
            return Err(contract_violation(
                "non-write frontend query has writer registrations",
            ));
        }
        for &(fragment_instance_id, backend_num) in writer_instances {
            if query
                .writer_instances
                .insert(fragment_instance_id, backend_num)
                .is_some()
            {
                return Err(contract_violation(
                    "frontend query has duplicate writer fragment instances",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn first_failure(&self, query_id: QueryId) -> Option<String> {
        self.active
            .lock()
            .expect("frontend query registry lock")
            .get(&query_key(query_id))
            .and_then(|query| query.first_failure.clone())
    }

    pub(crate) fn preserve_failure_context(
        &self,
        query_id: QueryId,
        message: String,
    ) -> Result<(), DistributedQueryError> {
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| inactive_query(query_id))?;
        match query.first_failure.as_mut() {
            Some(primary) if message.starts_with(primary.as_str()) => *primary = message,
            Some(_) => {}
            None => query.first_failure = Some(message),
        }
        Ok(())
    }

    pub(crate) fn latch_failure_and_cancel(
        &self,
        query_id: QueryId,
        message: impl Into<String>,
    ) -> Result<String, DistributedQueryError> {
        let (message, cancellation) = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            let query = active
                .get_mut(&query_key(query_id))
                .ok_or_else(|| inactive_query(query_id))?;
            let message = query
                .first_failure
                .get_or_insert_with(|| message.into())
                .clone();
            (message, request_cancellation(query))
        };
        dispatch_cancellation(cancellation);
        Ok(message)
    }

    pub(crate) fn seal_and_take_completion(
        &self,
        query_id: QueryId,
    ) -> Result<(Option<String>, Vec<NativeExecutionReport>), DistributedQueryError> {
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| inactive_query(query_id))?;
        if query.reports_sealed {
            return Err(contract_violation(
                "frontend query reports are already sealed",
            ));
        }
        query.reports_sealed = true;
        query.writer_report_indexes.clear();
        Ok((
            query.first_failure.clone(),
            std::mem::take(&mut query.reports),
        ))
    }

    pub(crate) fn report_progress(
        &self,
        query_id: QueryId,
        expected_instances: &[UniqueId],
    ) -> Result<(usize, Option<String>, bool), DistributedQueryError> {
        let active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get(&query_key(query_id))
            .ok_or_else(|| inactive_query(query_id))?;
        let completed_instances = match query.intent {
            DistributedQueryIntent::Profile => &query.profile_report_instances,
            DistributedQueryIntent::Result | DistributedQueryIntent::Write => {
                &query.final_report_instances
            }
        };
        let final_count = expected_instances
            .iter()
            .filter(|instance| completed_instances.contains(instance))
            .count();
        Ok((
            final_count,
            query.first_failure.clone(),
            query.has_failed_final_report,
        ))
    }

    pub(crate) fn backend_failed(&self, backend_idx: usize, message: String) -> Vec<QueryId> {
        let (affected, cancellations) = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            let mut affected = Vec::new();
            let mut cancellations = Vec::new();
            for (&(high, low), query) in active.iter_mut() {
                if query.reports_sealed || !query.scheduled_backends.contains_key(&backend_idx) {
                    continue;
                }
                if query.first_failure.is_none() {
                    query.first_failure = Some(message.clone());
                    affected.push(QueryId::new(high, low));
                }
                if let Some(cancellation) = request_cancellation(query) {
                    cancellations.push(cancellation);
                }
            }
            (affected, cancellations)
        };

        for (dispatcher, attempted) in cancellations {
            for (backend_idx, finst_ids) in attempted {
                dispatcher.cancel_fragments(backend_idx, &finst_ids);
            }
        }
        affected
    }

    pub(crate) fn backend_restarted(
        &self,
        backend_idx: usize,
        old_epoch: u64,
        message: String,
    ) -> Vec<QueryId> {
        let (affected, cancellations) = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            let mut affected = Vec::new();
            let mut cancellations = Vec::new();
            for (&(high, low), query) in active.iter_mut() {
                if query.reports_sealed
                    || query.scheduled_backends.get(&backend_idx) != Some(&old_epoch)
                {
                    continue;
                }
                if query.first_failure.is_none() {
                    query.first_failure = Some(message.clone());
                    affected.push(QueryId::new(high, low));
                }
                if let Some(cancellation) = request_cancellation(query) {
                    cancellations.push(cancellation);
                }
            }
            (affected, cancellations)
        };

        for (dispatcher, attempted) in cancellations {
            for (backend_idx, finst_ids) in attempted {
                dispatcher.cancel_fragments(backend_idx, &finst_ids);
            }
        }
        affected
    }

    pub(crate) fn backend_has_active_queries(&self, backend_idx: usize) -> bool {
        self.active
            .lock()
            .expect("frontend query registry lock")
            .values()
            .any(|query| {
                !query.reports_sealed && query.scheduled_backends.contains_key(&backend_idx)
            })
    }

    fn unregister(&self, key: QueryKey) {
        self.active
            .lock()
            .expect("frontend query registry lock")
            .remove(&key);
    }
}

pub(crate) struct ActiveQueryGuard {
    registry: Arc<FrontendQueryRegistry>,
    key: QueryKey,
}

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        self.registry.unregister(self.key);
    }
}

type DispatcherCancellation = (Arc<dyn FragmentDispatcher>, Vec<(usize, Vec<UniqueId>)>);

fn request_cancellation(query: &mut ActiveQuery) -> Option<DispatcherCancellation> {
    query.cancellation_requested = true;
    cancellation_if_ready(query)
}

fn cancellation_if_ready(query: &mut ActiveQuery) -> Option<DispatcherCancellation> {
    if !query.cancellation_requested
        || query.cancellation_dispatched
        || query.submissions_inflight != 0
    {
        return None;
    }
    query.cancellation_dispatched = true;
    Some((
        Arc::clone(&query.dispatcher),
        query
            .attempted
            .iter()
            .map(|(&backend_idx, finst_ids)| (backend_idx, finst_ids.clone()))
            .collect(),
    ))
}

fn dispatch_cancellation(cancellation: Option<DispatcherCancellation>) {
    if let Some((dispatcher, attempted)) = cancellation {
        for (backend_idx, finst_ids) in attempted {
            dispatcher.cancel_fragments(backend_idx, &finst_ids);
        }
    }
}

fn query_key(query_id: QueryId) -> QueryKey {
    (query_id.high(), query_id.low())
}

fn inactive_query(query_id: QueryId) -> DistributedQueryError {
    DistributedQueryError::new(
        DistributedQueryErrorKind::Rejected,
        format!(
            "frontend query {}/{} is not active",
            query_id.high(),
            query_id.low()
        ),
    )
}

fn contract_violation(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}
