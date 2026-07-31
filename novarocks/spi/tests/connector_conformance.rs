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

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::conformance::assert_batch_reader_contract;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorControlBinding,
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBinding, ConnectorExecutionBindingKey,
    ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorListTablesRequest,
    ConnectorMetadata, ConnectorNamespaceRequest, ConnectorOpenReaderRequest, ConnectorProviderId,
    ConnectorReadExecution, ConnectorReaderMetricsSnapshot, ConnectorScan, ConnectorScanHandle,
    ConnectorScanPlanning, ConnectorSplit, ConnectorSplitPlanningRequest, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableRequest,
};

struct OwnerExecution {
    key: ConnectorExecutionBindingKey,
}

impl OwnerExecution {
    fn new(instance_id: &str) -> Self {
        Self {
            key: ConnectorExecutionBindingKey {
                instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
                incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
            },
        }
    }
}

impl ConnectorReadExecution for OwnerExecution {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn open_reader(
        &self,
        _split: &ConnectorSplit,
        _request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        unreachable!("instance construction must not open a reader")
    }
}

struct OwnerPlanning {
    instance_id: ConnectorInstanceId,
}

impl ConnectorScanPlanning for OwnerPlanning {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _: &ConnectorTableHandle,
        _: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        unreachable!("control binding construction must not begin a scan")
    }

    fn plan_splits(
        &self,
        _: &ConnectorScanHandle,
        _: ConnectorSplitPlanningRequest,
    ) -> Result<Vec<ConnectorSplit>, ConnectorError> {
        unreachable!("control binding construction must not plan splits")
    }
}

struct OwnerDistribution {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorExecutionDistribution for OwnerDistribution {
    fn declaration(
        &self,
        _: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        ConnectorExecutionDeclaration::try_new(
            self.descriptor.clone(),
            self.incarnation,
            bytes::Bytes::new(),
        )
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
fn execution_bindings_are_valid_without_control_capabilities() {
    let key = ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse("file").expect("instance ID"),
        incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
    };
    let binding = ConnectorExecutionBinding::try_new(
        ConnectorProviderId::parse("file").expect("provider ID"),
        key.clone(),
        Arc::new(OwnerExecution::new("file")),
    )
    .expect("read-only execution binding");

    assert_eq!(binding.key(), &key);
}

#[test]
fn execution_binding_rejects_a_read_capability_owned_by_another_generation() {
    let key = ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse("file").expect("instance ID"),
        incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
    };
    assert_eq!(
        ConnectorExecutionBinding::try_new(
            ConnectorProviderId::parse("file").expect("provider ID"),
            key,
            Arc::new(OwnerExecution::new("foreign")),
        )
        .err()
        .expect("a host must not attach a foreign read capability")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn control_binding_rejects_metadata_owned_by_another_instance() {
    let descriptor = descriptor("file");
    assert_eq!(
        ConnectorControlBinding::try_new(
            descriptor.clone(),
            ConnectorInstanceIncarnation::from_bytes([1; 16]),
            Arc::new(OwnerMetadata::new("foreign")),
            Arc::new(OwnerPlanning {
                instance_id: descriptor.instance_id.clone(),
            }),
            Arc::new(OwnerDistribution {
                descriptor,
                incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
            }),
            None,
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

#[test]
fn reader_metrics_snapshot_add_and_delta_are_saturating() {
    let first = ConnectorReaderMetricsSnapshot {
        bytes_read: 10,
        rows_decoded: 2,
        ..ConnectorReaderMetricsSnapshot::default()
    };
    let second = ConnectorReaderMetricsSnapshot {
        bytes_read: 7,
        rows_decoded: 3,
        ..ConnectorReaderMetricsSnapshot::default()
    };
    let total = first.saturating_add(second);
    assert_eq!(total.bytes_read, 17);
    assert_eq!(total.rows_decoded, 5);
    assert_eq!(
        total
            .saturating_delta_since(ConnectorReaderMetricsSnapshot {
                bytes_read: 20,
                rows_decoded: 1,
                ..ConnectorReaderMetricsSnapshot::default()
            })
            .bytes_read,
        0
    );
}
