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

//! The split-assignment resource one execution round owns.
//!
//! It exists for exactly one round: aborting the round closes every split
//! source and sender, and a replacement round builds a new one under a new
//! attempt id rather than resuming this. Nothing here survives a round, which
//! is what keeps a replaced attempt from inheriting a sequence space.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_proto_codec::connector_read::ConnectorReadCodec;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_spi::connector::read_stack::ConnectorReadSplitSource;

use super::super::connector_domain::CatalogHandle;
use super::driver::{
    AssignmentTarget, SplitAssignmentDriver, SplitAssignmentDriverError, SplitAssignmentStop,
    TaskUpdateRetryPolicy,
};
use super::transport::TaskUpdateTransport;

/// How many splits one batch pulls from a source.
///
/// Bounded well below the wire limit so a slow task's queue drains between
/// batches instead of filling in one shot.
pub(crate) const DEFAULT_PUMP_BATCH_SIZE: usize = 256;

/// How long the pump waits when no source could make progress.
const IDLE_PUMP_BACKOFF: std::time::Duration = std::time::Duration::from_millis(2);

/// One typed scan's split source, with the plan node it feeds.
pub(crate) struct RoundSplitSource {
    pub(crate) plan_node_id: i32,
    pub(crate) source: Box<dyn ConnectorReadSplitSource>,
    pub(crate) codec: Arc<dyn ConnectorReadCodec>,
}

/// The per-round owner of every split source and the driver that drains them.
pub(crate) struct RoundSplitAssignment {
    driver: SplitAssignmentDriver,
    sources: Vec<RoundSplitSource>,
    stop: SplitAssignmentStop,
    /// Closing owns source cleanup. It is deliberately independent from
    /// `stop`: the coordinator signals stop before it joins the worker, and
    /// that worker must still release every source after observing the signal.
    closed: bool,
}

impl RoundSplitAssignment {
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        transport: Arc<dyn TaskUpdateTransport>,
        tasks: BTreeMap<i32, Vec<AssignmentTarget>>,
        max_queued_splits_per_task: u64,
        sources: Vec<RoundSplitSource>,
        retry_policy: TaskUpdateRetryPolicy,
    ) -> Self {
        let stop = SplitAssignmentStop::default();
        Self {
            driver: SplitAssignmentDriver::new(
                execution_id,
                transport,
                tasks,
                max_queued_splits_per_task,
                sources
                    .iter()
                    .map(|source| (source.plan_node_id, Arc::clone(&source.codec)))
                    .collect(),
                retry_policy,
                stop.clone(),
            ),
            sources,
            stop,
            closed: false,
        }
    }

    /// A handle that closes this round's assignment from another thread.
    ///
    /// Cancellation reaches the coordinator on the statement thread while the
    /// pump may be blocked in a batch request, so the stop signal has to be
    /// observable without holding the round.
    pub(crate) fn stop_handle(&self) -> SplitAssignmentStop {
        self.stop.clone()
    }

    /// Drain every source until each plan node has sent its terminal marker.
    ///
    /// A source that yields nothing right now is retried after the other
    /// sources get a turn, so one slow enumeration cannot starve the rest.
    pub(crate) fn pump_to_completion(&mut self) -> Result<(), SplitAssignmentDriverError> {
        while !self.stop.is_stopped() {
            let mut progressed = false;
            let mut pending = false;
            for index in 0..self.sources.len() {
                if self.stop.is_stopped() {
                    break;
                }
                let plan_node_id = self.sources[index].plan_node_id;
                if self.driver.is_terminal_for(plan_node_id) {
                    continue;
                }
                pending = true;
                if self.driver.is_backpressured(plan_node_id) {
                    continue;
                }
                let source = self.sources[index].source.as_mut();
                if self
                    .driver
                    .pump(plan_node_id, source, DEFAULT_PUMP_BATCH_SIZE)?
                {
                    progressed = true;
                }
            }
            if !pending {
                return Ok(());
            }
            if !progressed {
                // Either every remaining task is at its queue ceiling, or every
                // source had nothing right now. Both resolve on their own, so
                // wait briefly rather than spin a core. The statement deadline
                // and the stop handle are what end a round that never
                // progresses; this loop deliberately has no deadline of its
                // own, because inventing one would cancel a legitimately slow
                // enumeration.
                std::thread::sleep(IDLE_PUMP_BACKOFF);
            }
        }
        Ok(())
    }

    /// Idempotent. Closes the driver and every source exactly once.
    pub(crate) fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.stop.stop();
        self.driver.close();
        for entry in &mut self.sources {
            // A source close may race an outstanding batch; the connector
            // contract makes it idempotent, and a batch that already completed
            // normally may still be returned to whoever asked for it.
            let _ = entry.source.close();
            // Acceptance evidence: a pre-ControlReady replan must close the
            // old round's sources rather than reuse them, and this is the only
            // place that can show it happened exactly once.
            emit_split_source_close_marker(entry.plan_node_id);
        }
    }
}

impl Drop for RoundSplitAssignment {
    fn drop(&mut self) {
        self.close();
    }
}

/// Emit the split-source close marker, behind the connector-reader test gate.
///
/// It prints scheduling identity only: never a relation name, a file path, or
/// any part of a split's contents.
pub(crate) fn emit_split_source_close_marker(plan_node_id: i32) {
    // Debug-only, matching the backend's own reader-marker gate: a release
    // build must not be able to print execution evidence at all.
    if !cfg!(debug_assertions)
        || std::env::var_os("NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER").is_none()
    {
        return;
    }
    println!("NOVAROCKS_CONNECTOR_SPLIT_SOURCE_CLOSE plan_node={plan_node_id}");
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

pub(crate) type RoundSplitAssignmentStop = SplitAssignmentStop;

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks_proto_codec::connector_read::{
        CatalogTableHandle, ConnectorReadCodecError, ValidatedColumnHandle,
        ValidatedConnectorSplit, ValidatedTransactionHandle,
    };
    use novarocks_spi::connector::ConnectorError;
    use novarocks_spi::connector::read_stack::{
        ConnectorReadDynamicFilterSnapshot, ConnectorReadSplit, ConnectorReadSplitSource,
        ConnectorSplitBatch,
    };
    use novarocks_types::{AttemptId, QueryId};

    use crate::query_execution::connector_domain::TaskUpdateRequest;
    use crate::query_execution::split_assignment::{TaskUpdateOutcome, TaskUpdateTransportError};

    use super::*;

    struct NeverSend;

    impl TaskUpdateTransport for NeverSend {
        fn send(
            &self,
            _execution_id: QueryExecutionId,
            _target: &AssignmentTarget,
            _request: &TaskUpdateRequest,
            _timeout: std::time::Duration,
            _stop: &SplitAssignmentStop,
        ) -> Result<TaskUpdateOutcome, TaskUpdateTransportError> {
            panic!("closing a round must not send a task update")
        }
    }

    struct CloseCountingSource {
        close_calls: Arc<AtomicUsize>,
    }

    impl ConnectorReadSplitSource for CloseCountingSource {
        fn next_batch(
            &mut self,
            _max_size: usize,
            _dynamic_filter: &ConnectorReadDynamicFilterSnapshot,
        ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
            panic!("close lifecycle tests must not enumerate splits")
        }

        fn is_finished(&self) -> bool {
            false
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct InertCodec;

    impl ConnectorReadCodec for InertCodec {
        fn owner(&self) -> &str {
            "round-close-test"
        }

        fn decode_relation(
            &self,
            _relation: &CatalogTableHandle,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadRelation,
            ConnectorReadCodecError,
        > {
            unreachable!("close lifecycle tests must not decode relations")
        }

        fn encode_relation(
            &self,
            _relation: &novarocks_spi::connector::read_stack::ConnectorReadRelation,
        ) -> Result<
            novarocks_proto_models::connector_read::CatalogTableHandle,
            ConnectorReadCodecError,
        > {
            unreachable!("close lifecycle tests must not encode relations")
        }

        fn decode_column(
            &self,
            _column: &ValidatedColumnHandle,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            ConnectorReadCodecError,
        > {
            unreachable!("close lifecycle tests must not decode columns")
        }

        fn encode_column(
            &self,
            _column: &novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
        ) -> Result<novarocks_proto_models::connector_read::ColumnHandle, ConnectorReadCodecError>
        {
            unreachable!("close lifecycle tests must not encode columns")
        }

        fn decode_transaction(
            &self,
            _transaction: &ValidatedTransactionHandle,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
            ConnectorReadCodecError,
        > {
            unreachable!("close lifecycle tests must not decode transactions")
        }

        fn encode_transaction(
            &self,
            _transaction: &novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
        ) -> Result<
            novarocks_proto_models::connector_read::ConnectorTransactionHandle,
            ConnectorReadCodecError,
        > {
            unreachable!("close lifecycle tests must not encode transactions")
        }

        fn decode_split(
            &self,
            _split: &ValidatedConnectorSplit,
        ) -> Result<ConnectorReadSplit, ConnectorReadCodecError> {
            unreachable!("close lifecycle tests must not decode splits")
        }

        fn encode_split(
            &self,
            _split: &ConnectorReadSplit,
        ) -> Result<novarocks_proto_models::connector_read::ConnectorSplit, ConnectorReadCodecError>
        {
            unreachable!("close lifecycle tests must not encode splits")
        }
    }

    fn assignment(close_calls: Arc<AtomicUsize>) -> RoundSplitAssignment {
        let execution_id = QueryExecutionId::new(
            QueryId::new(9, 9),
            AttemptId::new(1).expect("attempt id must be valid"),
        )
        .expect("execution id must be valid");
        RoundSplitAssignment::new(
            execution_id,
            Arc::new(NeverSend),
            BTreeMap::new(),
            1,
            vec![RoundSplitSource {
                plan_node_id: 7,
                source: Box::new(CloseCountingSource { close_calls }),
                codec: Arc::new(InertCodec),
            }],
            TaskUpdateRetryPolicy::default(),
        )
    }

    #[test]
    fn stop_handle_is_visible_to_the_pump() {
        let stop = RoundSplitAssignmentStop::default();
        assert!(!stop.is_stopped());
        stop.stop();
        assert!(stop.is_stopped());
    }

    #[test]
    fn close_releases_sources_after_an_external_stop() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let mut assignment = assignment(Arc::clone(&close_calls));

        assignment.stop_handle().stop();
        assignment.close();

        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn close_releases_each_source_only_once() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let mut assignment = assignment(Arc::clone(&close_calls));

        assignment.close();
        assignment.close();

        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }
}
