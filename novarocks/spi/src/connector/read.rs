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
    ConnectorError, ConnectorInstanceId, ConnectorRequestContext, ConnectorScanHandle,
    ConnectorSplit, ConnectorTableHandle,
};

#[derive(Clone)]
pub struct ConnectorScan {
    pub handle: ConnectorScanHandle,
    pub output_schema: SchemaRef,
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

#[derive(Clone)]
pub struct ConnectorOpenReaderRequest {
    pub expected_schema: SchemaRef,
    pub batch: ConnectorBatchBudget,
    pub context: ConnectorRequestContext,
}

pub trait ConnectorRead: Send + Sync {
    fn instance_id(&self) -> &ConnectorInstanceId;

    fn begin_scan(
        &self,
        table: &ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError>;

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<Vec<ConnectorSplit>, ConnectorError>;

    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError>;
}

pub trait ConnectorBatchReader: Send {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;

    fn close(&mut self) -> Result<(), ConnectorError>;
}
