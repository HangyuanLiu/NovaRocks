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

use std::num::{NonZeroU64, NonZeroUsize};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::{
    ConnectorError, ConnectorPredicateDisposition, ConnectorReadSessionLease,
    ConnectorRequestContext, ConnectorScanHandle, ConnectorSplit, ConnectorStaticPredicate,
};

#[derive(Clone)]
pub struct ConnectorScan {
    pub handle: ConnectorScanHandle,
    pub output_schema: SchemaRef,
    pub predicate_dispositions: Vec<ConnectorPredicateDisposition>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadSelector {
    Current,
    SnapshotId(i64),
    TimestampMicros(i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorBatchBudget {
    pub max_rows: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
}

#[derive(Clone)]
pub struct ConnectorBeginScanRequest {
    pub projection: Vec<usize>,
    pub static_predicates: Vec<ConnectorStaticPredicate>,
    pub selector: ConnectorReadSelector,
    pub limit: Option<u64>,
    pub batch: ConnectorBatchBudget,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorSplitPlanningRequest {
    pub target_parallelism: NonZeroUsize,
    pub max_split_bytes: Option<NonZeroU64>,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorSplitPlanningMetrics {
    pub candidate_units_considered: u64,
    pub candidate_units_pruned: u64,
}

#[derive(Clone, Debug)]
pub struct ConnectorSplitPlanningResult {
    pub splits: Vec<ConnectorSplit>,
    pub metrics: ConnectorSplitPlanningMetrics,
    /// FE-local prepared remote session. It never enters any execution carrier.
    pub session: Option<ConnectorReadSessionLease>,
}

impl ConnectorSplitPlanningResult {
    pub fn try_new(
        splits: Vec<ConnectorSplit>,
        metrics: ConnectorSplitPlanningMetrics,
    ) -> Result<Self, ConnectorError> {
        if metrics.candidate_units_pruned > metrics.candidate_units_considered {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector split planning metrics report more pruned units than considered units",
            ));
        }
        Ok(Self {
            splits,
            metrics,
            session: None,
        })
    }

    pub fn try_new_with_session(
        splits: Vec<ConnectorSplit>,
        metrics: ConnectorSplitPlanningMetrics,
        session: ConnectorReadSessionLease,
    ) -> Result<Self, ConnectorError> {
        let mut result = Self::try_new(splits, metrics)?;
        result.session = Some(session);
        Ok(result)
    }
}

#[derive(Clone)]
pub struct ConnectorOpenReaderRequest {
    pub expected_schema: SchemaRef,
    pub batch: ConnectorBatchBudget,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorReaderMetricsSnapshot {
    pub bytes_read: u64,
    pub read_requests: u64,
    pub rows_decoded: u64,
    pub batches_delivered: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub io_time_ns: u64,
    pub decode_time_ns: u64,
    pub row_groups_read: u64,
    pub row_groups_pruned: u64,
    pub delayed_materialization_ranges: u64,
}

impl ConnectorReaderMetricsSnapshot {
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            bytes_read: self.bytes_read.saturating_add(other.bytes_read),
            read_requests: self.read_requests.saturating_add(other.read_requests),
            rows_decoded: self.rows_decoded.saturating_add(other.rows_decoded),
            batches_delivered: self
                .batches_delivered
                .saturating_add(other.batches_delivered),
            cache_hits: self.cache_hits.saturating_add(other.cache_hits),
            cache_misses: self.cache_misses.saturating_add(other.cache_misses),
            io_time_ns: self.io_time_ns.saturating_add(other.io_time_ns),
            decode_time_ns: self.decode_time_ns.saturating_add(other.decode_time_ns),
            row_groups_read: self.row_groups_read.saturating_add(other.row_groups_read),
            row_groups_pruned: self
                .row_groups_pruned
                .saturating_add(other.row_groups_pruned),
            delayed_materialization_ranges: self
                .delayed_materialization_ranges
                .saturating_add(other.delayed_materialization_ranges),
        }
    }

    pub fn saturating_delta_since(self, previous: Self) -> Self {
        Self {
            bytes_read: self.bytes_read.saturating_sub(previous.bytes_read),
            read_requests: self.read_requests.saturating_sub(previous.read_requests),
            rows_decoded: self.rows_decoded.saturating_sub(previous.rows_decoded),
            batches_delivered: self
                .batches_delivered
                .saturating_sub(previous.batches_delivered),
            cache_hits: self.cache_hits.saturating_sub(previous.cache_hits),
            cache_misses: self.cache_misses.saturating_sub(previous.cache_misses),
            io_time_ns: self.io_time_ns.saturating_sub(previous.io_time_ns),
            decode_time_ns: self.decode_time_ns.saturating_sub(previous.decode_time_ns),
            row_groups_read: self
                .row_groups_read
                .saturating_sub(previous.row_groups_read),
            row_groups_pruned: self
                .row_groups_pruned
                .saturating_sub(previous.row_groups_pruned),
            delayed_materialization_ranges: self
                .delayed_materialization_ranges
                .saturating_sub(previous.delayed_materialization_ranges),
        }
    }
}

pub trait ConnectorBatchReader: Send {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;

    fn close(&mut self) -> Result<(), ConnectorError>;

    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        ConnectorReaderMetricsSnapshot::default()
    }
}
