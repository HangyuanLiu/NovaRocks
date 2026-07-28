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

#![cfg(feature = "connector-conformance")]

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::conformance::assert_batch_reader_contract;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorError,
    ConnectorErrorKind, ConnectorInstance, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceRequest,
    ConnectorOpenReaderRequest, ConnectorProviderId, ConnectorRead, ConnectorScan,
    ConnectorScanHandle, ConnectorSplit, ConnectorSplitPlanningRequest, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableRequest,
};

struct OwnerRead {
    instance_id: ConnectorInstanceId,
}

impl OwnerRead {
    fn new(instance_id: &str) -> Self {
        Self {
            instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
        }
    }
}

impl ConnectorRead for OwnerRead {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        unreachable!("instance construction must not begin a scan")
    }

    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<Vec<ConnectorSplit>, ConnectorError> {
        unreachable!("instance construction must not plan splits")
    }

    fn open_reader(
        &self,
        _split: &ConnectorSplit,
        _request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        unreachable!("instance construction must not open a reader")
    }
}

struct OwnerMetadata {
    instance_id: ConnectorInstanceId,
}

impl OwnerMetadata {
    fn new(instance_id: &str) -> Self {
        Self {
            instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
        }
    }
}

impl ConnectorMetadata for OwnerMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> Result<bool, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }

    fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }

    fn list_tables(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }

    fn load_table(
        &self,
        _request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }
}

fn descriptor(instance_id: &str) -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("file").expect("provider ID"),
        instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
    }
}

#[test]
fn read_only_instances_are_valid_without_metadata_discovery() {
    let instance =
        ConnectorInstance::try_new(descriptor("file"), None, Arc::new(OwnerRead::new("file")))
            .expect("read-only compat provider");

    assert!(instance.metadata().is_none());
    assert_eq!(instance.read().instance_id().as_str(), "file");
}

#[test]
fn instance_rejects_a_read_capability_owned_by_another_instance() {
    assert_eq!(
        ConnectorInstance::try_new(
            descriptor("file"),
            None,
            Arc::new(OwnerRead::new("foreign")),
        )
        .err()
        .expect("a host must not attach a foreign read capability")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn instance_rejects_a_metadata_capability_owned_by_another_instance() {
    assert_eq!(
        ConnectorInstance::try_new(
            descriptor("file"),
            Some(Arc::new(OwnerMetadata::new("foreign"))),
            Arc::new(OwnerRead::new("file")),
        )
        .err()
        .expect("a host must not attach foreign metadata")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

struct FixtureReader {
    batches: VecDeque<RecordBatch>,
    close_calls: usize,
}

struct ScriptedReader {
    responses: VecDeque<Option<RecordBatch>>,
}

impl ScriptedReader {
    fn new(responses: impl IntoIterator<Item = Option<RecordBatch>>) -> Self {
        Self {
            responses: responses.into_iter().collect(),
        }
    }
}

impl ConnectorBatchReader for ScriptedReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        Ok(self.responses.pop_front().flatten())
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

impl FixtureReader {
    fn new(batches: impl IntoIterator<Item = RecordBatch>) -> Self {
        Self {
            batches: batches.into_iter().collect(),
            close_calls: 0,
        }
    }
}

impl ConnectorBatchReader for FixtureReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        Ok(self.batches.pop_front())
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.close_calls += 1;
        Ok(())
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn batch(schema: SchemaRef, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).expect("fixture batch")
}

fn budget() -> ConnectorBatchBudget {
    ConnectorBatchBudget {
        max_rows: NonZeroUsize::new(2).expect("nonzero rows"),
        max_bytes: NonZeroUsize::new(1024).expect("nonzero bytes"),
    }
}

#[test]
fn batch_reader_conformance_accepts_schema_matched_stable_eos() {
    let expected_schema = schema();
    let mut reader = FixtureReader::new([
        batch(expected_schema.clone(), vec![1, 2]),
        batch(expected_schema.clone(), vec![3]),
    ]);

    let batches = assert_batch_reader_contract(&mut reader, &expected_schema, budget())
        .expect("reader with matching schema and stable EOS");

    assert_eq!(batches.len(), 2);
    assert_eq!(reader.close_calls, 2);
}

#[test]
fn batch_reader_conformance_rejects_a_schema_drift() {
    let expected_schema = schema();
    let wrong_schema = Arc::new(Schema::new(vec![Field::new(
        "other_value",
        DataType::Int64,
        false,
    )]));
    let mut reader = FixtureReader::new([batch(wrong_schema, vec![1])]);

    assert_eq!(
        assert_batch_reader_contract(&mut reader, &expected_schema, budget())
            .expect_err("a reader must not drift from its declared output schema")
            .kind(),
        ConnectorErrorKind::CorruptData
    );
}

#[test]
fn batch_reader_conformance_rejects_a_batch_after_eos() {
    let expected_schema = schema();
    let mut reader = ScriptedReader::new([
        Some(batch(expected_schema.clone(), vec![1])),
        None,
        Some(batch(expected_schema.clone(), vec![2])),
    ]);

    assert_eq!(
        assert_batch_reader_contract(&mut reader, &expected_schema, budget())
            .expect_err("a provider must not resume after reporting EOS")
            .kind(),
        ConnectorErrorKind::CorruptData
    );
}
