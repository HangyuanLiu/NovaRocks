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

//! The typed connector scan source and its scan operation.
//!
//! Responsibilities:
//! - Turns one validated `ConnectorTableScanSource` plus one installed typed
//!   provider into an execution `ScanSource`/`ScanOp` pair.
//! - Drives the runtime split stream: one split becomes one page source, whose
//!   pages become `Chunk`s through the execution-owned page adapter.
//! - Owns terminal cleanup for the page sources it opened, mirroring the
//!   opaque `ConnectorReadScanSource` reader-group discipline.
//!
//! Key exported interfaces:
//! - Types: `TypedConnectorScanSource`, `TypedConnectorScanOp`.
//! - Functions: `complete_all_scan_dynamic_filter`.
//!
//! Current limitations:
//! - The scan produces no morsel of its own yet. `build_morsels` is empty with
//!   `has_more` still true, which is exactly "this scan may still receive
//!   work"; the queue-driven morsel that schedules that work is a later task.
//! - The dynamic filter handed to the provider is the truthful unconstrained
//!   one until this fragment's runtime-filter consumer contracts are decoded.
//!   `ScanSource::with_runtime_filter_contracts` is the one seam a live,
//!   backend-driven filter is substituted through.
//!
//! Provider neutrality: this file holds protocol-validated carriers and trait
//! objects only. It never matches a provider variant and never downcasts, so it
//! compiles with no provider crate in the dependency graph.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use crate::runtime_filter::typed_dynamic_filter::scan_dynamic_filter;
use novarocks_execution::connector::{
    ConnectorPageAdapter, PageConversion, SplitPoll, SplitQueue, TaskAttemptSplitQueues,
};
use novarocks_execution::exec::node::runtime_filter::RuntimeFilterConsumerBinding;
use novarocks_execution::exec::node::scan::{
    BoundScanRanges, IncrementalScanRange, RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanOp,
    ScanSource,
};
use novarocks_execution::exec::node::{BoxedExecIter, ExecResult};
use novarocks_execution::runtime::profile::{ProfileUnit, RuntimeProfile};
use novarocks_execution::runtime_filter::{RuntimeFilterConsumerContract, RuntimeFilterSessionRef};
use novarocks_proto::connector_read::{
    ConnectorTableScanSource, ScheduledSplit, TypedConnectorPageSourceProvider,
    ValidatedColumnHandle, WireDynamicFilter,
};
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_spi::connector::read_stack::{CompleteAllDynamicFilter, ConnectorSession};
use novarocks_types::SlotId;

/// How long a driver parks on an empty, non-terminal split queue before it
/// re-checks cancellation, the deadline, and the terminal latch.
///
/// A wake always arrives through the queue's observable; this bound exists so a
/// cancelled or expired attempt is noticed even if a wake is lost, never as the
/// primary way progress is made.
const SPLIT_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// How long a driver waits on a page source that reports itself blocked.
///
/// The page contract has no wake-up, so a blocked source can only be polled
/// again. Sleeping briefly keeps an idle turn from becoming a spin without
/// turning "nothing right now" into end of stream.
const BLOCKED_PAGE_SOURCE_BACKOFF: Duration = Duration::from_millis(1);

/// The truthful dynamic filter for a scan that has no runtime feedback.
///
/// It covers exactly the columns the scan's dynamic-filter bindings name, and
/// reports an unconstrained, complete, non-awaitable predicate. A blocked or
/// awaitable filter here would fabricate feedback that no one produces.
pub fn complete_all_scan_dynamic_filter(scan: &ConnectorTableScanSource) -> Arc<WireDynamicFilter> {
    // A binding always names a variable this scan assigns: the protocol carrier
    // rejects one that does not, so this lookup is total for a validated scan.
    let assigned: BTreeMap<&str, &ValidatedColumnHandle> = scan
        .assignments()
        .iter()
        .map(|assignment| (assignment.variable(), assignment.column()))
        .collect();
    let covered: BTreeSet<ValidatedColumnHandle> = scan
        .dynamic_filters()
        .iter()
        .filter_map(|binding| {
            assigned
                .get(binding.variable())
                .map(|column| (*column).clone())
        })
        .collect();
    Arc::new(CompleteAllDynamicFilter::new(covered))
}

/// Everything one typed scan needs, shared by the source, the op, and every
/// iterator the op hands out.
struct TypedConnectorScanShared {
    scan: ConnectorTableScanSource,
    provider: Arc<dyn TypedConnectorPageSourceProvider>,
    session: ConnectorSession,
    /// The attempt's runtime-filter session, absent when this attempt installed
    /// none. Retained because the consumer contracts that make a live filter
    /// possible are decoded after this source is built.
    runtime_filter: Option<RuntimeFilterSessionRef>,
    /// Deadline and cancellation, exactly as the opaque connector path uses
    /// them: checked before every open and on every driver turn.
    request: ConnectorRequestContext,
    plan_node_id: i32,
    /// Ordered output slot ids. `slot_ids[i]` names page channel `i`.
    slot_ids: Vec<SlotId>,
    dynamic_filter: Arc<WireDynamicFilter>,
}

impl TypedConnectorScanShared {
    /// Fail fast on a cancelled or expired attempt, before any provider call.
    fn check_liveness(&self, action: &str) -> Result<(), String> {
        if self.request.cancellation().is_cancelled() {
            return Err(format!("typed connector scan {action} was cancelled"));
        }
        if Instant::now() >= self.request.deadline() {
            return Err(format!("typed connector scan {action} deadline elapsed"));
        }
        Ok(())
    }
}

/// A physical source for one typed connector scan node of one task attempt.
///
/// The source carries no split: splits arrive at runtime through the task's
/// per-plan-node queue, so binding it never waits for enumeration.
pub struct TypedConnectorScanSource {
    shared: Arc<TypedConnectorScanShared>,
    queues: Arc<TaskAttemptSplitQueues>,
}

impl TypedConnectorScanSource {
    pub fn new(
        scan: ConnectorTableScanSource,
        provider: Arc<dyn TypedConnectorPageSourceProvider>,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        queues: Arc<TaskAttemptSplitQueues>,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        runtime_filter: Option<RuntimeFilterSessionRef>,
    ) -> Self {
        let dynamic_filter = complete_all_scan_dynamic_filter(&scan);
        Self {
            shared: Arc::new(TypedConnectorScanShared {
                scan,
                provider,
                session,
                runtime_filter,
                request,
                plan_node_id,
                slot_ids,
                dynamic_filter,
            }),
            queues,
        }
    }

    /// The single seam for a live, backend-driven dynamic filter.
    ///
    /// Runtime-filter production and its wait policy belong to the backend
    /// runtime-filter owner, not to this scan: when that owner can hand over a
    /// filter, it is substituted here and nothing else in this file changes. No
    /// other code path may synthesize a filter.
    pub fn with_backend_dynamic_filter(mut self, dynamic_filter: Arc<WireDynamicFilter>) -> Self {
        let shared = Arc::get_mut(&mut self.shared)
            .expect("typed connector scan source is not shared before it is bound");
        shared.dynamic_filter = dynamic_filter;
        self
    }

    /// Rebuild this source around one live dynamic filter.
    ///
    /// Every field but the filter is carried over: the scan carrier, the
    /// installed provider, the session, and the request all belong to this
    /// fragment instance and are unaffected by which filter the page sources
    /// consult.
    fn with_substituted_filter(&self, dynamic_filter: Arc<WireDynamicFilter>) -> Self {
        Self {
            shared: Arc::new(TypedConnectorScanShared {
                scan: self.shared.scan.clone(),
                provider: Arc::clone(&self.shared.provider),
                session: self.shared.session.clone(),
                runtime_filter: self.shared.runtime_filter.clone(),
                request: self.shared.request.clone(),
                plan_node_id: self.shared.plan_node_id,
                slot_ids: self.shared.slot_ids.clone(),
                dynamic_filter,
            }),
            queues: Arc::clone(&self.queues),
        }
    }
}

impl ScanSource for TypedConnectorScanSource {
    fn bind(&self, ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String> {
        match ranges {
            // Typed scans carry no frozen range: their work arrives as splits.
            BoundScanRanges::None => {}
            BoundScanRanges::SchemaSelection { .. } => {
                return Err(
                    "typed connector scan source requires an empty range binding".to_string(),
                );
            }
        }
        // Created empty on first use, and born closed when the attempt is
        // already terminal, so a late bind observes termination instead of
        // parking on a queue nobody will ever serve.
        let queue = self.queues.queue(self.shared.plan_node_id);
        let waiter = Arc::new(SplitWaiter::default());
        // Weak, so dropping the op leaves an inert observer rather than keeping
        // this op's state alive for as long as the attempt's queue lives.
        let woken = Arc::downgrade(&waiter);
        queue.observable().add_observer(Arc::new(move || {
            if let Some(waiter) = woken.upgrade() {
                waiter.wake();
            }
        }));
        Ok(Arc::new(TypedConnectorScanOp {
            shared: Arc::clone(&self.shared),
            queue,
            waiter,
            sources: Arc::new(TypedPageSourceGroup::default()),
        }))
    }

    fn profile_name(&self) -> Option<String> {
        Some("TypedConnectorScan".to_string())
    }

    /// Subscribe to the live filter this fragment's consumer contracts describe.
    ///
    /// `DynamicFilterBinding.filter_id` on the scan carrier is the runtime
    /// filter's binding id, so a contract is matched to a binding by that id
    /// alone. With no session or no contract this scan keeps the truthful
    /// unconstrained filter it was built with rather than claiming feedback it
    /// never receives.
    fn with_runtime_filter_contracts(
        &self,
        contracts: &[RuntimeFilterConsumerBinding],
    ) -> Result<Option<Arc<dyn ScanSource>>, String> {
        let Some(session) = self.shared.runtime_filter.as_ref() else {
            return Ok(None);
        };
        let by_filter_id: BTreeMap<u32, RuntimeFilterConsumerContract> = contracts
            .iter()
            .map(|binding| {
                (
                    binding.contract.binding_id().get(),
                    binding.contract.clone(),
                )
            })
            .collect();
        if by_filter_id.is_empty() {
            return Ok(None);
        }
        let dynamic_filter = scan_dynamic_filter(&self.shared.scan, Some(session), &by_filter_id)
            .map_err(|error| error.to_string())?;
        Ok(Some(Arc::new(self.with_substituted_filter(dynamic_filter))))
    }
}

/// One bound typed connector scan.
pub struct TypedConnectorScanOp {
    shared: Arc<TypedConnectorScanShared>,
    queue: Arc<SplitQueue>,
    waiter: Arc<SplitWaiter>,
    sources: Arc<TypedPageSourceGroup>,
}

impl ScanOp for TypedConnectorScanOp {
    fn terminate(&self) -> Result<(), String> {
        // Page sources first: stop the I/O this scan started before waking the
        // drivers that would otherwise start more.
        let closed = self.sources.terminate();
        // Idempotent, drops anything still queued, and wakes every waiter once.
        self.queue.close();
        // A queue close notifies through the observable, but a driver parked
        // between two polls must also be woken directly.
        self.waiter.wake();
        closed
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        // Deliberately empty: a task scan may legally start before its first
        // split arrives, and may receive none at all. `has_more` stays true
        // until this plan node's queue can never produce another split, which
        // is what keeps the operator alive instead of finishing at zero rows.
        Ok(ScanMorsels::new(Vec::new(), !self.queue.is_exhausted()))
    }

    fn supports_incremental_scan_ranges(&self) -> bool {
        // The morsel set of a typed scan can still grow after the plan was
        // frozen. This says only that; the growth itself arrives as splits on
        // the task-update queue, never as a legacy incremental scan range.
        true
    }

    fn build_incremental_morsels(
        &self,
        _scan_ranges: &[IncrementalScanRange],
    ) -> Result<ScanMorsels, String> {
        Err(
            "typed connector scan receives splits through its task-update split queue, \
             not through incremental scan ranges"
                .to_string(),
        )
    }

    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        _runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        // The execution-layer runtime filters are not the connector's dynamic
        // filter: the connector consults the one this source was built with,
        // through `with_backend_dynamic_filter`. Applying an execution filter
        // here would push a predicate the provider never agreed to.
        match morsel {
            // The neutral morsel: this scan's work unit is its split queue, so
            // the morsel carries no scheduling identity of its own yet.
            ScanMorsel::Empty => {}
            ScanMorsel::FileRange { .. } => {
                return Err(
                    "typed connector scan received a file-range morsel it does not own".to_string(),
                );
            }
            ScanMorsel::ConnectorScanUnit { .. } => {
                return Err(
                    "typed connector scan received an opaque prepared-unit morsel".to_string(),
                );
            }
            ScanMorsel::Schema { .. } => {
                return Err("typed connector scan received a schema morsel".to_string());
            }
        }
        Ok(Box::new(TypedConnectorSplitIter {
            shared: Arc::clone(&self.shared),
            queue: Arc::clone(&self.queue),
            waiter: Arc::clone(&self.waiter),
            sources: Arc::clone(&self.sources),
            current: None,
            profile,
            finished: false,
        }))
    }

    fn profile_name(&self) -> Option<String> {
        Some("TypedConnectorScan".to_string())
    }
}

/// The chunk stream of one driver over this scan's split queue.
struct TypedConnectorSplitIter {
    shared: Arc<TypedConnectorScanShared>,
    queue: Arc<SplitQueue>,
    waiter: Arc<SplitWaiter>,
    sources: Arc<TypedPageSourceGroup>,
    /// The split currently being read. `None` between two splits.
    current: Option<RegisteredPageSource>,
    profile: Option<RuntimeProfile>,
    finished: bool,
}

impl TypedConnectorSplitIter {
    fn open_page_source(&mut self, split: &ScheduledSplit) -> Result<(), String> {
        self.shared.check_liveness("page source open")?;
        let page_source = self
            .shared
            .provider
            .create_page_source(
                &self.shared.session,
                self.shared.scan.table(),
                split.split(),
                split.sequence_id(),
                self.shared.scan.assignments(),
                &self.shared.dynamic_filter,
            )
            .map_err(|error| {
                format!(
                    "create typed connector page source for sequence {}: {error}",
                    split.sequence_id()
                )
            })?;
        let adapter = ConnectorPageAdapter::new(self.shared.slot_ids.clone(), page_source);
        self.current = Some(self.sources.register(adapter)?);
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add("TypedConnectorPageSourcesOpened", ProfileUnit::Unit, 1);
        }
        Ok(())
    }

    fn close_current(&mut self) -> Result<(), String> {
        match self.current.take() {
            Some(source) => source.close(),
            None => Ok(()),
        }
    }

    /// End this stream on a primary failure, still releasing the open source.
    fn fail(&mut self, primary: String) -> ExecResult {
        self.finished = true;
        match self.close_current() {
            Ok(()) => Err(primary),
            Err(cleanup) => Err(format!("{primary} (cleanup: {cleanup})")),
        }
    }
}

impl Iterator for TypedConnectorSplitIter {
    type Item = ExecResult;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }
            if let Err(error) = self.shared.check_liveness("split driver") {
                return Some(self.fail(error));
            }
            if self.sources.is_terminal() {
                // `terminate` already closed everything and no further I/O may
                // start, so this driver ends without touching the provider.
                self.finished = true;
                return None;
            }

            if let Some(source) = self.current.as_ref() {
                match source.pull() {
                    Err(error) => return Some(self.fail(error)),
                    Ok(PageConversion::Chunk(chunk)) => return Some(Ok(chunk)),
                    Ok(PageConversion::Idle) => {
                        // Nothing right now, which is not end of stream. Only a
                        // source that says it is waiting earns a sleep.
                        if source.is_blocked() {
                            std::thread::sleep(BLOCKED_PAGE_SOURCE_BACKOFF);
                        }
                        continue;
                    }
                    Ok(PageConversion::Finished) => {
                        if let Err(error) = self.close_current() {
                            return Some(self.fail(error));
                        }
                        if let Some(profile) = self.profile.as_ref() {
                            profile.counter_add("TypedConnectorSplitsRead", ProfileUnit::Unit, 1);
                        }
                        continue;
                    }
                }
            }

            // Read the wake generation before polling, so a split that arrives
            // between the poll and the park is never slept through.
            let generation = self.waiter.generation();
            match self.queue.poll() {
                SplitPoll::Ready(split) => {
                    if let Err(error) = self.open_page_source(&split) {
                        return Some(self.fail(error));
                    }
                    continue;
                }
                SplitPoll::Blocked => {
                    self.waiter.wait(generation, SPLIT_WAIT_POLL_INTERVAL);
                    continue;
                }
                // The only end of stream: drained after the terminal marker, or
                // closed.
                SplitPoll::Exhausted => {
                    self.finished = true;
                    return self.close_current().err().map(Err);
                }
            }
        }
    }
}

impl Drop for TypedConnectorSplitIter {
    fn drop(&mut self) {
        // A dropped driver must still release its page source. Terminal
        // cleanup normally does it; this is the final safety net.
        let _ = self.close_current();
    }
}

/// A wake latch for drivers parked on an empty, non-terminal split queue.
///
/// The queue publishes state changes through observers, which cannot park a
/// thread by themselves; this turns one into a wake.
#[derive(Default)]
struct SplitWaiter {
    generation: Mutex<u64>,
    signal: Condvar,
}

impl SplitWaiter {
    fn wake(&self) {
        let mut generation = self.generation.lock().expect("split waiter lock");
        *generation = generation.wrapping_add(1);
        drop(generation);
        self.signal.notify_all();
    }

    fn generation(&self) -> u64 {
        *self.generation.lock().expect("split waiter lock")
    }

    /// Park until the generation moves past `seen`, or until `timeout`.
    fn wait(&self, seen: u64, timeout: Duration) {
        let generation = self.generation.lock().expect("split waiter lock");
        if *generation != seen {
            return;
        }
        let _unused = self
            .signal
            .wait_timeout(generation, timeout)
            .expect("split waiter lock");
    }
}

/// Fragment-local ownership of the page sources one typed scan opened.
///
/// Terminal scan lifecycle closes them explicitly; adapter `Drop` is only a
/// final safety net. This mirrors the opaque connector reader group so both
/// paths have one termination discipline.
#[derive(Default)]
struct TypedPageSourceGroup {
    state: Mutex<TypedPageSourceGroupState>,
}

#[derive(Default)]
struct TypedPageSourceGroupState {
    phase: TypedPageSourcePhase,
    next_id: usize,
    open: BTreeMap<usize, SharedPageSource>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum TypedPageSourcePhase {
    #[default]
    Open,
    Terminating,
    Closed,
}

/// One adapter slot. `None` once closed, which is what makes closing
/// idempotent no matter who wins the race.
type SharedPageSource = Arc<Mutex<Option<ConnectorPageAdapter>>>;

impl TypedPageSourceGroup {
    fn register(
        self: &Arc<Self>,
        adapter: ConnectorPageAdapter,
    ) -> Result<RegisteredPageSource, String> {
        let slot: SharedPageSource = Arc::new(Mutex::new(Some(adapter)));
        let id = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "typed connector page source group lock poisoned".to_string())?;
            if state.phase != TypedPageSourcePhase::Open {
                return Err(format!(
                    "typed connector page source group is {:?}",
                    state.phase
                ));
            }
            let id = state.next_id;
            state.next_id = state.next_id.saturating_add(1);
            state.open.insert(id, Arc::clone(&slot));
            id
        };
        Ok(RegisteredPageSource {
            slot,
            group: Arc::downgrade(self),
            id,
        })
    }

    fn unregister(&self, id: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.open.remove(&id);
        }
    }

    fn is_terminal(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.phase != TypedPageSourcePhase::Open)
            .unwrap_or(true)
    }

    /// Close every open page source exactly once and refuse any new one.
    fn terminate(&self) -> Result<(), String> {
        let open = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "typed connector page source group lock poisoned".to_string())?;
            if state.phase != TypedPageSourcePhase::Open {
                return Ok(());
            }
            state.phase = TypedPageSourcePhase::Terminating;
            std::mem::take(&mut state.open)
                .into_values()
                .collect::<Vec<_>>()
        };
        let mut cleanup_errors = Vec::new();
        for slot in open {
            if let Err(error) = close_slot(&slot) {
                cleanup_errors.push(error);
            }
        }
        match self.state.lock() {
            Ok(mut state) => state.phase = TypedPageSourcePhase::Closed,
            Err(_) => {
                cleanup_errors.push("typed connector page source group lock poisoned".to_string());
            }
        }
        if cleanup_errors.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "typed connector page source cleanup failed: {}",
                cleanup_errors.join("; ")
            ))
        }
    }
}

/// Close one slot's adapter if it is still open. Idempotent.
fn close_slot(slot: &SharedPageSource) -> Result<(), String> {
    let mut guard = slot
        .lock()
        .map_err(|_| "typed connector page source lock poisoned".to_string())?;
    match guard.as_mut() {
        Some(adapter) => {
            let result = adapter.close().map_err(|error| error.to_string());
            // Dropped only after its own `close` ran, so the source is closed
            // exactly once whether termination or the driver got here first.
            *guard = None;
            result
        }
        None => Ok(()),
    }
}

/// A page source owned by the group and read by exactly one driver.
struct RegisteredPageSource {
    slot: SharedPageSource,
    group: Weak<TypedPageSourceGroup>,
    id: usize,
}

impl RegisteredPageSource {
    fn pull(&self) -> Result<PageConversion, String> {
        let mut guard = self
            .slot
            .lock()
            .map_err(|_| "typed connector page source lock poisoned".to_string())?;
        match guard.as_mut() {
            // A source terminal cleanup already closed is finished, not idle:
            // the driver must not ask the provider for another page.
            None => Ok(PageConversion::Finished),
            Some(adapter) => adapter.pull().map_err(|error| error.to_string()),
        }
    }

    fn is_blocked(&self) -> bool {
        self.slot
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(ConnectorPageAdapter::source_is_blocked))
            .unwrap_or(false)
    }

    fn close(self) -> Result<(), String> {
        let result = close_slot(&self.slot);
        if let Some(group) = self.group.upgrade() {
            group.unregister(self.id);
        }
        result
    }
}

impl Drop for RegisteredPageSource {
    fn drop(&mut self) {
        let _ = close_slot(&self.slot);
        if let Some(group) = self.group.upgrade() {
            group.unregister(self.id);
        }
    }
}

/// Wire fixtures shared by this module's tests and the typed scan decoder's.
///
/// They live here because the carrier they build is this module's input; the
/// decoder tests assert what the decoder does with exactly that carrier.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;

    use novarocks_proto::connector_read::encode_value_type;
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::read_stack::ConnectorValueType;

    pub(crate) fn unconstrained() -> dto::TupleDomain {
        dto::TupleDomain {
            none: false,
            column_domains: Vec::new(),
        }
    }

    pub(crate) fn iceberg_column_handle(field_id: i32) -> dto::IcebergColumnHandle {
        dto::IcebergColumnHandle {
            base_column_identity: Some(dto::ColumnIdentity {
                field_id,
                name: format!("c{field_id}"),
                category: dto::ColumnIdentityCategory::Primitive as i32,
                children: Vec::new(),
            }),
            base_type_json: "\"long\"".to_owned(),
            field_id_path: Vec::new(),
            type_json: "\"long\"".to_owned(),
            nullable: true,
            comment: None,
        }
    }

    pub(crate) fn column_handle(field_id: i32) -> dto::ColumnHandle {
        dto::ColumnHandle {
            handle: Some(dto::column_handle::Handle::Iceberg(iceberg_column_handle(
                field_id,
            ))),
        }
    }

    pub(crate) fn schema_table_name() -> dto::SchemaTableName {
        dto::SchemaTableName {
            schema_name: "db".to_owned(),
            table_name: "t".to_owned(),
        }
    }

    pub(crate) fn iceberg_table_handle() -> dto::IcebergTableHandle {
        dto::IcebergTableHandle {
            schema_table_name: Some(schema_table_name()),
            snapshot_id: Some(11),
            table_schema_json: "{\"type\":\"struct\"}".to_owned(),
            spec_id: Some(0),
            partition_spec_jsons: BTreeMap::from([(0, "{\"spec-id\":0}".to_owned())]),
            format_version: 2,
            unenforced_predicate: Some(unconstrained()),
            enforced_predicate: Some(unconstrained()),
            limit: None,
            projected_columns: vec![iceberg_column_handle(1)],
            name_mapping_json: None,
            table_location: "s3://bucket/warehouse/db/t".to_owned(),
            storage_properties: BTreeMap::new(),
        }
    }

    pub(crate) fn catalog_table_handle() -> dto::CatalogTableHandle {
        dto::CatalogTableHandle {
            catalog_name: "test.typed".to_owned(),
            instance_incarnation: vec![1; 16],
            transaction: Some(dto::ConnectorTransactionHandle {
                handle: Some(dto::connector_transaction_handle::Handle::Iceberg(
                    dto::HiveTransactionHandle {
                        auto_commit: true,
                        uuid: vec![2; 16],
                    },
                )),
            }),
            relation: Some(dto::catalog_table_handle::Relation::Table(
                dto::ConnectorTableHandle {
                    handle: Some(dto::connector_table_handle::Handle::Iceberg(
                        iceberg_table_handle(),
                    )),
                },
            )),
        }
    }

    /// A provider that is installed but never asked to read anything. It lets
    /// a decode test prove the binding resolves without opening any file.
    struct UnusedProvider;

    impl novarocks_proto::connector_read::TypedConnectorPageSourceProvider for UnusedProvider {
        fn create_page_source(
            &self,
            _session: &novarocks_spi::connector::read_stack::ConnectorSession,
            _table: &novarocks_proto::connector_read::CatalogTableHandle,
            _split: &novarocks_proto::connector_read::ValidatedConnectorSplit,
            _scheduled_split_sequence_id: u64,
            _columns: &[novarocks_proto::connector_read::ScanAssignment],
            _dynamic_filter: &std::sync::Arc<novarocks_proto::connector_read::WireDynamicFilter>,
        ) -> Result<
            Box<dyn novarocks_spi::connector::read_stack::ConnectorPageSource>,
            novarocks_spi::connector::ConnectorError,
        > {
            Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "this fixture provider is never read from",
            ))
        }
    }

    impl novarocks_proto::connector_read::TypedConnectorProviderFactory for UnusedProvider {
        fn create_page_source_provider(
            &self,
            _request: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<
            std::sync::Arc<dyn novarocks_proto::connector_read::TypedConnectorPageSourceProvider>,
            novarocks_spi::connector::ConnectorError,
        > {
            Ok(std::sync::Arc::new(UnusedProvider))
        }

        fn create_system_table_provider(
            &self,
            _request: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<
            std::sync::Arc<dyn novarocks_proto::connector_read::TypedConnectorSystemTableProvider>,
            novarocks_spi::connector::ConnectorError,
        > {
            Ok(std::sync::Arc::new(UnusedProvider))
        }
    }

    impl novarocks_proto::connector_read::TypedConnectorSystemTableProvider for UnusedProvider {
        fn create_system_page_source(
            &self,
            _session: &novarocks_spi::connector::read_stack::ConnectorSession,
            _table: &novarocks_proto::connector_read::CatalogTableHandle,
            _columns: &[novarocks_proto::connector_read::ScanAssignment],
        ) -> Result<
            Box<dyn novarocks_spi::connector::read_stack::ConnectorPageSource>,
            novarocks_spi::connector::ConnectorError,
        > {
            Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "this fixture provider is never read from",
            ))
        }
    }

    /// The runtime bundle a typed decode needs, wired to the same binding
    /// generation `catalog_table_handle` names.
    pub(crate) fn typed_scan_runtime() -> crate::fragment::decode::plan::context::TypedScanRuntime {
        use novarocks_types::{AttemptId, QueryId};

        let key = novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: novarocks_spi::connector::ConnectorInstanceId::try_from_canonical(
                "test.typed",
            )
            .expect("canonical instance id"),
            incarnation: novarocks_spi::connector::ConnectorInstanceIncarnation::from_bytes(
                [1; 16],
            ),
        };
        let providers =
            std::sync::Arc::new(crate::connector::TypedConnectorProviderRegistry::new());
        let unused = std::sync::Arc::new(UnusedProvider);
        providers
            .install(&key, crate::connector::TypedConnectorProviders::new(unused))
            .expect("fixture install");

        let execution_id = novarocks_proto::lifecycle::QueryExecutionId::new(
            QueryId::new(1, 2),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("execution id");
        let queues = novarocks_execution::connector::SplitQueueRegistry::new().open_attempt(
            novarocks_execution::connector::TaskAttemptKey::new(
                execution_id,
                novarocks_types::UniqueId::new(9, 1),
            ),
            novarocks_execution::connector::SplitQueueConfig::default(),
        );
        let session = novarocks_spi::connector::read_stack::ConnectorSession::try_new(
            "q1",
            "novarocks",
            "UTC",
            "en_US",
            std::time::SystemTime::UNIX_EPOCH,
        )
        .expect("session");
        crate::fragment::decode::plan::context::TypedScanRuntime::new(
            providers, queues, session, None,
        )
    }

    pub(crate) fn scan_source_proto() -> dto::ConnectorTableScanSource {
        dto::ConnectorTableScanSource {
            table: Some(catalog_table_handle()),
            assignments: vec![dto::ScanAssignment {
                variable: "id".to_owned(),
                column: Some(column_handle(1)),
                value_type: Some(encode_value_type(ConnectorValueType::BigInt)),
            }],
            enforced_predicate: Some(unconstrained()),
            unenforced_predicate: Some(unconstrained()),
            remaining_expression: None,
            dynamic_filters: Vec::new(),
            max_batch_rows: 1024,
            max_batch_bytes: 1 << 20,
        }
    }

    pub(crate) fn split_proto(plan_node_id: i32, sequence_id: u64) -> dto::ScheduledSplit {
        dto::ScheduledSplit {
            sequence_id,
            plan_node_id,
            split: Some(dto::ConnectorSplit {
                split_weight_raw: 100,
                remotely_accessible: true,
                addresses: Vec::new(),
                affinity_key: None,
                retained_size_in_bytes: 64,
                category: Some(dto::connector_split::Category::Data(dto::DataSplit {
                    provider: Some(dto::data_split::Provider::Iceberg(dto::IcebergSplit {
                        path: format!("s3://bucket/warehouse/db/t/data-{sequence_id}.parquet"),
                        start: 0,
                        length: 128,
                        file_size: 128,
                        file_record_count: 4,
                        file_format: dto::IcebergFileFormat::Parquet as i32,
                        partition_spec_id: 0,
                        partition_data_json: "{\"partitionValues\":[]}".to_owned(),
                        deletes: Vec::new(),
                        file_statistics_domain: Some(unconstrained()),
                        data_sequence_number: Some(1),
                        file_first_row_id: Some(0),
                        decryption_data: None,
                    })),
                })),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    use arrow::array::{ArrayRef, Int64Array};
    use novarocks_execution::connector::{SplitQueueConfig, SplitQueueRegistry, TaskAttemptKey};
    use novarocks_proto::FieldPath;
    use novarocks_proto::connector_read::{
        CatalogTableHandle, ScanAssignment, ValidatedConnectorSplit,
    };
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::read_stack::{
        ConnectorPageSource, PageSourceMetrics, SourcePage,
    };
    use novarocks_spi::connector::{ConnectorCancellation, ConnectorError, ConnectorErrorKind};
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId, UniqueId};

    use super::*;

    fn scan_source() -> ConnectorTableScanSource {
        ConnectorTableScanSource::parse(
            test_support::scan_source_proto(),
            FieldPath::root("typed_connector_read"),
        )
        .expect("valid typed scan source")
    }

    const NODE: i32 = 7;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct AlwaysCancelled;

    impl ConnectorCancellation for AlwaysCancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }

    /// A page source scripted turn by turn. A `None` entry is an idle turn, not
    /// termination: only running out of script finishes it.
    struct ScriptedPageSource {
        pages: Vec<Option<SourcePage>>,
        cursor: usize,
        finished: bool,
        closes: Arc<AtomicUsize>,
    }

    impl ConnectorPageSource for ScriptedPageSource {
        fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
            if self.cursor >= self.pages.len() {
                self.finished = true;
                return Ok(None);
            }
            let page = self.pages[self.cursor].take();
            self.cursor += 1;
            Ok(page)
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn metrics(&self) -> PageSourceMetrics {
            PageSourceMetrics::default()
        }

        fn memory_usage_bytes(&self) -> u64 {
            0
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    /// A page-source provider that hands out one scripted source per split.
    struct ScriptedProvider {
        script: Mutex<Vec<Vec<Option<SourcePage>>>>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        fail_open: bool,
    }

    impl ScriptedProvider {
        fn new(script: Vec<Vec<Option<SourcePage>>>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                opens: Arc::new(AtomicUsize::new(0)),
                closes: Arc::new(AtomicUsize::new(0)),
                fail_open: false,
            })
        }
    }

    impl TypedConnectorPageSourceProvider for ScriptedProvider {
        fn create_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
            _split: &ValidatedConnectorSplit,
            _scheduled_split_sequence_id: u64,
            _columns: &[ScanAssignment],
            _dynamic_filter: &Arc<WireDynamicFilter>,
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            if self.fail_open {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "scripted open failure",
                ));
            }
            self.opens.fetch_add(1, Ordering::AcqRel);
            let mut script = self.script.lock().expect("script lock");
            let pages = if script.is_empty() {
                Vec::new()
            } else {
                script.remove(0)
            };
            Ok(Box::new(ScriptedPageSource {
                pages,
                cursor: 0,
                finished: false,
                closes: Arc::clone(&self.closes),
            }))
        }
    }

    /// Records how many columns the dynamic filter it was handed covers.
    struct FilterRecordingProvider {
        observed: Arc<Mutex<Option<usize>>>,
    }

    impl TypedConnectorPageSourceProvider for FilterRecordingProvider {
        fn create_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
            _split: &ValidatedConnectorSplit,
            _scheduled_split_sequence_id: u64,
            _columns: &[ScanAssignment],
            dynamic_filter: &Arc<WireDynamicFilter>,
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            *self.observed.lock().expect("observed lock") =
                Some(dynamic_filter.columns_covered().len());
            Ok(Box::new(ScriptedPageSource {
                pages: Vec::new(),
                cursor: 0,
                finished: false,
                closes: Arc::new(AtomicUsize::new(0)),
            }))
        }
    }

    fn int_page(values: Vec<i64>) -> SourcePage {
        let positions = values.len();
        let column: ArrayRef = Arc::new(Int64Array::from(values));
        SourcePage::try_new(positions, vec![column]).expect("valid page")
    }

    fn scheduled_split(sequence_id: u64) -> ScheduledSplit {
        ScheduledSplit::parse(
            test_support::split_proto(NODE, sequence_id),
            FieldPath::root("scheduled_split"),
        )
        .expect("valid scheduled split")
    }

    fn attempt_queues() -> Arc<TaskAttemptSplitQueues> {
        let registry = SplitQueueRegistry::new();
        registry.open_attempt(
            TaskAttemptKey::new(
                QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).expect("attempt"))
                    .expect("execution id"),
                UniqueId::new(3, 4),
            ),
            SplitQueueConfig::default(),
        )
    }

    fn request(cancellation: Arc<dyn ConnectorCancellation>) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            cancellation,
            1 << 20,
            1 << 22,
        )
        .expect("request context")
    }

    fn session() -> ConnectorSession {
        ConnectorSession::try_new("q-1", "test", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("session")
    }

    fn source_with(
        provider: Arc<ScriptedProvider>,
        queues: Arc<TaskAttemptSplitQueues>,
        cancellation: Arc<dyn ConnectorCancellation>,
    ) -> TypedConnectorScanSource {
        TypedConnectorScanSource::new(
            scan_source(),
            provider,
            session(),
            request(cancellation),
            queues,
            NODE,
            vec![SlotId::new(1)],
            None,
        )
    }

    fn bind(source: &TypedConnectorScanSource) -> Arc<dyn ScanOp> {
        source
            .bind(BoundScanRanges::None)
            .expect("bind typed connector scan")
    }

    #[test]
    fn typed_scan_starts_with_zero_splits_and_does_not_end_the_stream() {
        let queues = attempt_queues();
        let source = source_with(
            ScriptedProvider::new(Vec::new()),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);

        let morsels = op.build_morsels().expect("build morsels");
        assert!(morsels.morsels.is_empty());
        // Still open: a scan that received nothing yet has not finished.
        assert!(morsels.has_more);
        assert!(op.supports_incremental_scan_ranges());

        // The terminal marker alone is a clean, empty end of stream.
        queues
            .queue(NODE)
            .offer_splits(NODE, Vec::new(), true)
            .expect("terminal marker");
        let morsels = op.build_morsels().expect("build morsels after the marker");
        assert!(!morsels.has_more);
        let rows = op
            .execute_iter(ScanMorsel::Empty, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive an empty typed scan");
        assert!(rows.is_empty());
    }

    #[test]
    fn typed_scan_reads_a_split_that_arrives_after_the_driver_started() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![Some(int_page(vec![1, 2, 3]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);

        let mut iter = op
            .execute_iter(ScanMorsel::Empty, None, None)
            .expect("driver");
        // The driver parks on an empty queue; the split arrives only now.
        let offering = {
            let queues = Arc::clone(&queues);
            std::thread::spawn(move || {
                let queue = queues.queue(NODE);
                queue
                    .offer_splits(NODE, vec![scheduled_split(1)], true)
                    .expect("late split");
            })
        };

        let chunk = iter
            .next()
            .expect("the late split produces a chunk")
            .expect("chunk");
        assert_eq!(chunk.len(), 3);
        assert!(iter.next().is_none());
        offering.join().expect("offering thread");
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);
        // One split, one page source, closed when the split finished.
        assert_eq!(provider.closes.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_scan_treats_an_idle_page_as_not_end_of_stream() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![
            None,
            Some(int_page(vec![10])),
            None,
            Some(int_page(vec![20, 30])),
        ]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");

        let rows = op
            .execute_iter(ScanMorsel::Empty, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive across idle turns");
        // Both pages arrive: the idle turns between them ended nothing.
        assert_eq!(
            rows.iter()
                .map(novarocks_execution::exec::chunk::Chunk::len)
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn typed_scan_reads_every_queued_split_before_exhaustion_ends_it() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![
            vec![Some(int_page(vec![1]))],
            vec![Some(int_page(vec![2, 3]))],
        ]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1), scheduled_split(2)], true)
            .expect("two splits");

        let rows = op
            .execute_iter(ScanMorsel::Empty, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive both splits");
        assert_eq!(rows.len(), 2);
        assert_eq!(provider.opens.load(Ordering::Acquire), 2);
        assert_eq!(provider.closes.load(Ordering::Acquire), 2);
        assert!(queues.queue(NODE).is_exhausted());
    }

    #[test]
    fn typed_scan_terminate_closes_the_page_source_and_the_queue_exactly_once() {
        let queues = attempt_queues();
        let provider =
            ScriptedProvider::new(vec![vec![Some(int_page(vec![1])), Some(int_page(vec![2]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        let queue = queues.queue(NODE);
        let closes = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&closes);
        queue.observable().add_observer(Arc::new(move || {
            observed.fetch_add(1, Ordering::AcqRel);
        }));
        queue
            .offer_splits(NODE, vec![scheduled_split(1)], false)
            .expect("one split");
        let woken_by_offer = closes.load(Ordering::Acquire);

        let mut iter = op
            .execute_iter(ScanMorsel::Empty, None, None)
            .expect("driver");
        iter.next()
            .expect("first page")
            .expect("first page is a chunk");
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);

        op.terminate().expect("terminate");
        op.terminate().expect("terminate is idempotent");
        op.terminate().expect("terminate is idempotent");
        assert_eq!(provider.closes.load(Ordering::Acquire), 1);
        assert_eq!(closes.load(Ordering::Acquire), woken_by_offer + 1);
        assert!(queue.is_closed());

        // After terminal the driver ends without another provider call.
        assert!(iter.next().is_none());
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_scan_after_terminate_opens_no_new_page_source() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![Some(int_page(vec![1]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");
        op.terminate().expect("terminate before any read");

        let rows = op
            .execute_iter(ScanMorsel::Empty, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("a terminated scan yields nothing");
        assert!(rows.is_empty());
        assert_eq!(provider.opens.load(Ordering::Acquire), 0);
    }

    #[test]
    fn typed_scan_fails_fast_on_a_cancelled_attempt() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![Some(int_page(vec![1]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(AlwaysCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");

        let error = op
            .execute_iter(ScanMorsel::Empty, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect_err("a cancelled attempt must not read");
        assert!(error.contains("cancelled"), "unexpected error: {error}");
        assert_eq!(provider.opens.load(Ordering::Acquire), 0);
    }

    #[test]
    fn typed_scan_rejects_a_morsel_it_does_not_own() {
        let source = source_with(
            ScriptedProvider::new(Vec::new()),
            attempt_queues(),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        assert!(
            op.execute_iter(
                ScanMorsel::ConnectorScanUnit {
                    index: 0,
                    row_position: None,
                },
                None,
                None,
            )
            .is_err()
        );
        assert!(
            op.build_incremental_morsels(&[IncrementalScanRange::Empty { has_more: None }])
                .is_err()
        );
    }

    #[test]
    fn typed_scan_hands_the_substituted_dynamic_filter_to_the_provider() {
        let queues = attempt_queues();
        let observed = Arc::new(Mutex::new(None));
        let provider = Arc::new(FilterRecordingProvider {
            observed: Arc::clone(&observed),
        });
        // The seam: a backend-driven filter replaces the default one, and the
        // provider is handed exactly what was substituted.
        let mut covered = BTreeSet::new();
        covered.insert(
            ScanAssignment::parse(
                dto::ScanAssignment {
                    variable: "id".to_owned(),
                    column: Some(test_support::column_handle(1)),
                    value_type: Some(novarocks_proto::connector_read::encode_value_type(
                        novarocks_spi::connector::read_stack::ConnectorValueType::BigInt,
                    )),
                },
                FieldPath::root("assignment"),
            )
            .expect("valid assignment")
            .column()
            .clone(),
        );
        let source = TypedConnectorScanSource::new(
            scan_source(),
            provider,
            session(),
            request(Arc::new(NeverCancelled)),
            Arc::clone(&queues),
            NODE,
            vec![SlotId::new(1)],
            None,
        )
        .with_backend_dynamic_filter(Arc::new(CompleteAllDynamicFilter::new(covered)));
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");

        let _ = op
            .execute_iter(ScanMorsel::Empty, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive the scan");
        assert_eq!(
            *observed.lock().expect("observed lock"),
            Some(1),
            "the provider must see the substituted filter's covered columns"
        );
    }

    #[test]
    fn typed_scan_dynamic_filter_covers_only_the_scans_bound_columns() {
        let mut proto = test_support::scan_source_proto();
        proto.dynamic_filters = vec![dto::DynamicFilterBinding {
            filter_id: 3,
            variable: "id".to_owned(),
        }];
        let scan = ConnectorTableScanSource::parse(proto, FieldPath::root("scan"))
            .expect("valid typed scan source");
        let filter = complete_all_scan_dynamic_filter(&scan);
        assert_eq!(filter.columns_covered().len(), 1);
        // Truthful and unconstrained: never blocked, never awaitable.
        assert!(filter.current_predicate().is_all());
        assert!(filter.is_complete());
        assert!(!filter.is_awaitable());
        assert!(!filter.is_blocked());

        // A scan with no binding covers nothing at all.
        assert!(
            complete_all_scan_dynamic_filter(&scan_source())
                .columns_covered()
                .is_empty()
        );
    }
}
