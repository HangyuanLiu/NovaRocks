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

//! Test-only Native connector-write fixtures for fragment-plan decode tests.
//!
//! These helpers are compiled only through `test-support`. They construct
//! generated Native carriers and pair them with SPI-owned provider values, so
//! role tests exercise the same decode edge as production without making the
//! Backend package a generated-wire fixture owner.

use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use bytes::Bytes;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::connector_common::encode_connector_payload_message;
use novarocks_proto_models::connector_write as write_dto;
use novarocks_proto_models::{catalog as catalog_dto, plan};
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution, ConnectorWriterHandle, ProviderWriteRuntime, RootWriteResultSchema,
    WriteRuntimeAdapter, WriterMultiplexSchema,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionWriteBinding, ConnectorFieldPath, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorWriteFragmentWireEncoder,
    ConnectorWriteHandleWireDecoder,
};
use novarocks_types::{QueryExecutionId, UniqueId};

pub const TEST_WRITE_CATALOG: &str = "write_catalog";
const TEST_WRITE_PROVIDER: &str = "test";
const TEST_WRITE_CODEC_REVISION: u32 = 1;

#[derive(Debug)]
pub struct StubProviderRuntime {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
}

impl ProviderWriteRuntime for StubProviderRuntime {
    type CommitHandle = ();
    type WriterHandle = String;
    type CommitFragment = String;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}

pub fn test_write_catalog_handle() -> CatalogHandle {
    CatalogHandle::new(
        ConnectorInstanceId::try_from_canonical(TEST_WRITE_CATALOG).expect("canonical instance id"),
        CatalogVersion::from_bytes([9; 32]),
    )
}

pub fn test_write_adapter() -> WriteRuntimeAdapter<StubProviderRuntime> {
    let handle = test_write_catalog_handle();
    WriteRuntimeAdapter::new(Arc::new(StubProviderRuntime {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse(TEST_WRITE_PROVIDER).expect("provider id"),
            instance_id: handle.catalog_name().clone(),
        },
        catalog_handle: handle,
    }))
}

struct StubHandleDecoder {
    adapter: WriteRuntimeAdapter<StubProviderRuntime>,
}

impl ConnectorWriteHandleWireDecoder for StubHandleDecoder {
    fn owner(&self) -> &str {
        TEST_WRITE_CATALOG
    }

    fn decode_writer_handle_payload(
        &self,
        payload: &ConnectorEncodedPayload,
    ) -> Result<ConnectorWriterHandle, novarocks_spi::connector::ConnectorCodecError> {
        let expected = synthetic_header(ConnectorCodecCategory::WriteHandle);
        payload
            .header()
            .validate_expected::<novarocks_spi::connector::ConnectorCodecError>(
                expected.provider_id(),
                expected.catalog(),
                expected.category(),
                expected.codec_revision(),
            )?;
        let value = std::str::from_utf8(payload.payload())
            .map_err(|error| {
                novarocks_spi::connector::ConnectorCodecError::new(
                    ConnectorFieldPath::root("writer_handle").field("provider_payload"),
                    novarocks_spi::connector::ConnectorCodecErrorKind::InvalidValue,
                    format!("synthetic writer handle is not UTF-8: {error}"),
                )
            })?
            .to_owned();
        if value.is_empty() {
            return Err(novarocks_spi::connector::ConnectorCodecError::new(
                ConnectorFieldPath::root("writer_handle").field("provider_payload"),
                novarocks_spi::connector::ConnectorCodecErrorKind::InvalidValue,
                "synthetic writer handle must not be empty",
            ));
        }
        Ok(self.adapter.wrap_writer_handle(value))
    }
}

struct StubFragmentEncoder {
    adapter: WriteRuntimeAdapter<StubProviderRuntime>,
}

impl ConnectorWriteFragmentWireEncoder for StubFragmentEncoder {
    fn owner(&self) -> &str {
        TEST_WRITE_CATALOG
    }

    fn encode_commit_fragment_payload(
        &self,
        fragment: &ConnectorCommitFragment,
    ) -> Result<ConnectorEncodedPayload, novarocks_spi::connector::ConnectorCodecError> {
        let value = self
            .adapter
            .commit_fragment(fragment)
            .cloned()
            .map_err(|error| {
                novarocks_spi::connector::ConnectorCodecError::new(
                    ConnectorFieldPath::root("commit_fragment"),
                    novarocks_spi::connector::ConnectorCodecErrorKind::InvalidValue,
                    error.to_string(),
                )
            })?;
        Ok(ConnectorEncodedPayload::new(
            synthetic_header(ConnectorCodecCategory::CommitFragment),
            Bytes::from(value),
        ))
    }
}

fn synthetic_header(category: ConnectorCodecCategory) -> ConnectorEnvelopeHeader {
    ConnectorEnvelopeHeader::new(
        ConnectorProviderId::parse(TEST_WRITE_PROVIDER).expect("provider id"),
        test_write_catalog_handle(),
        category,
        ConnectorCodecRevision::try_new(TEST_WRITE_CODEC_REVISION).expect("codec revision"),
    )
}

/// Records every writer it opened, so a test can prove each driver received
/// its own writer and physical context.
pub struct RecordingWriteExecution {
    catalog_handle: CatalogHandle,
    opened: Mutex<Vec<(u32, u32, u32)>>,
    terminals: Arc<Mutex<WriterTerminals>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriterTerminals {
    pub finished: usize,
    pub aborted: usize,
    pub appended_rows: usize,
}

impl RecordingWriteExecution {
    pub fn new() -> Self {
        Self {
            catalog_handle: test_write_catalog_handle(),
            opened: Mutex::new(Vec::new()),
            terminals: Arc::new(Mutex::new(WriterTerminals::default())),
        }
    }

    pub fn opened(&self) -> Vec<(u32, u32, u32)> {
        self.opened.lock().expect("opened writers").clone()
    }
}

struct RecordingWriter {
    terminals: Arc<Mutex<WriterTerminals>>,
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for RecordingWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.terminals
            .lock()
            .expect("writer terminals")
            .appended_rows += batch.num_rows();
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        self.terminals.lock().expect("writer terminals").finished += 1;
        Ok(Vec::new())
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        self.terminals.lock().expect("writer terminals").aborted += 1;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ConnectorWriteExecution for RecordingWriteExecution {
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    async fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        self.opened.lock().expect("opened writers").push((
            request.physical.driver_id(),
            request.physical.writer_ordinal(),
            request.target.get(),
        ));
        Ok(Box::new(RecordingWriter {
            terminals: Arc::clone(&self.terminals),
        }))
    }
}

pub fn test_write_binding(
    execution: Arc<RecordingWriteExecution>,
) -> ConnectorExecutionWriteBinding {
    ConnectorExecutionWriteBinding::new(
        execution,
        Arc::new(StubHandleDecoder {
            adapter: test_write_adapter(),
        }),
        Arc::new(StubFragmentEncoder {
            adapter: test_write_adapter(),
        }),
    )
}

struct NoVendedStorage;

impl novarocks_spi::connector::ConnectorStorageResolver for NoVendedStorage {
    fn resolve_vended_s3(
        &self,
        _: &novarocks_spi::connector::StorageAccessRequest,
    ) -> Result<novarocks_spi::connector::ResolvedVendedS3Access, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "test fixture has no vended storage lease",
        ))
    }
}

pub fn never_cancelled() -> novarocks_spi::connector::ConnectorStopView {
    novarocks_spi::connector::ConnectorStopOwner::new().view()
}

pub fn test_request_context() -> novarocks_spi::connector::ConnectorRequestContext {
    novarocks_spi::connector::ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(60),
        never_cancelled(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .expect("request context")
}

pub fn test_write_scan_runtime(
    execution_id: QueryExecutionId,
    fragment_instance_id: UniqueId,
    execution: Arc<RecordingWriteExecution>,
) -> novarocks_worker::TypedScanRuntime {
    let binding = test_write_binding(execution);
    let expected = test_write_catalog_handle();
    let queues = novarocks_execution::connector::SplitQueueRegistry::new().open_attempt(
        novarocks_execution::connector::TaskAttemptKey::new(execution_id, fragment_instance_id),
        novarocks_execution::connector::SplitQueueConfig::default(),
    );
    let session = novarocks_spi::connector::read_stack::ConnectorSession::try_new(
        "write-test",
        "novarocks",
        "UTC",
        "en_US",
        std::time::SystemTime::UNIX_EPOCH,
    )
    .expect("session");
    novarocks_worker::TypedScanRuntime::new(
        execution_id,
        Arc::new(|_| Err("no query-leased test read runtime".to_owned())),
        Arc::new(move |handle| {
            if handle == &expected {
                Ok(binding.clone())
            } else {
                Err(format!(
                    "no query-leased catalog runtime exists for {}",
                    handle.catalog_name().as_str()
                ))
            }
        }),
        queues,
        session,
        Arc::new(|| Ok(None)),
        Arc::new(novarocks_worker::read_attempt::TypedReadAttemptContext::new()),
        Arc::new(NoVendedStorage),
        novarocks_worker::ScanStreamHost::new(
            novarocks_worker::ScanPreparationConfig::default(),
            crate::backend_test_support::test_scan_stream_runtime(),
        ),
    )
}

pub fn wire_catalog_handle(name: &str) -> catalog_dto::CatalogHandle {
    catalog_dto::CatalogHandle {
        catalog_name: name.to_string(),
        version: vec![9; 32],
    }
}

pub fn iceberg_writer_handle(value: String) -> write_dto::ConnectorWriterHandle {
    let payload = ConnectorEncodedPayload::new(
        synthetic_header(ConnectorCodecCategory::WriteHandle),
        Bytes::from(value),
    );
    write_dto::ConnectorWriterHandle {
        provider_payload: Some(encode_connector_payload_message(&payload)),
    }
}

pub fn table_writer_payload(
    output_expr: novarocks_proto_models::expr::Expr,
    target_schema: Vec<novarocks_proto_models::common::OutputColumn>,
) -> plan::TableWriterNode {
    plan::TableWriterNode {
        catalog_handle: Some(wire_catalog_handle(TEST_WRITE_CATALOG)),
        write_target_ordinal: 0,
        handle: Some(iceberg_writer_handle(
            "9c2f1f66-1f0f-4c9a-9a1a-3f1d2c0b7a11".to_string(),
        )),
        input: Some(plan::ConnectorWriteInputBinding {
            kind: Some(plan::connector_write_input_binding::Kind::RootOutputByOrdinal(true)),
        }),
        writer_ordinal: 0,
        output_exprs: vec![output_expr],
        target_schema,
        writer_multiplex_schema: Some(writer_multiplex_schema(&WriterMultiplexSchema::empty())),
        partial_aggregate_plan: Some(plan::WriterPartialAggregatePlan { calls: Vec::new() }),
    }
}

pub fn writer_multiplex_schema(schema: &WriterMultiplexSchema) -> plan::WriterMultiplexSchema {
    use arrow::datatypes::DataType;
    use novarocks_type_contract::FunctionValueType as V;
    // The fixed prefix is independently declared by the writer relation contract.
    let mut fields = schema
        .arrow_schema()
        .fields()
        .iter()
        .zip(schema.slot_ids())
        .zip([
            (DataType::Int8, false),
            (DataType::Int32, false),
            (DataType::Int64, true),
            (DataType::Binary, true),
        ])
        .map(|((field, slot), (ty, nullable))| (slot, field.name().as_str(), V::new(ty, nullable)))
        .collect::<Vec<_>>();
    fields.extend(schema.auxiliary_channels().iter().map(|channel| {
        (
            channel.slot_id(),
            channel.name(),
            channel.value_type().clone(),
        )
    }));
    let (columns, logical_types) = relation_fields(&fields);
    plan::WriterMultiplexSchema {
        contract_version: schema.contract_version(),
        columns,
        logical_types,
    }
}

pub fn root_result_schema(schema: &RootWriteResultSchema) -> plan::RootWriteResultSchema {
    use novarocks_type_contract::FunctionValueType as V;
    let source = schema;
    let schema = source.arrow_schema();
    let types = vec![
        (DataType::Int8, false),
        (DataType::Int32, true),
        (DataType::Int64, true),
        (DataType::Binary, true),
        (
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                false,
            ))),
            true,
        ),
        (DataType::Utf8, true),
        (DataType::Binary, true),
        (
            DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            arrow::datatypes::Field::new("key", DataType::Utf8, false),
                            arrow::datatypes::Field::new("value", DataType::Utf8, false),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        ),
    ];
    let fields = schema
        .fields()
        .iter()
        .zip(source.slot_ids())
        .zip(types)
        .map(|((field, slot), (ty, nullable))| (slot, field.name().as_str(), V::new(ty, nullable)))
        .collect::<Vec<_>>();
    let (columns, logical_types) = relation_fields(&fields);
    plan::RootWriteResultSchema {
        contract_version: source.contract_version(),
        columns,
        logical_types,
    }
}

pub fn writer_node(
    node_id: i32,
    writer: plan::TableWriterNode,
    children: Vec<plan::DistributedNode>,
) -> plan::DistributedNode {
    plan::DistributedNode {
        node_id,
        fragment_id: 1,
        tuple_ids: Vec::new(),
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children,
        payload: Some(plan::distributed_node::Payload::TableWriter(writer)),
    }
}

pub fn finish_node(
    node_id: i32,
    expected_target_ordinals: Vec<u32>,
    children: Vec<plan::DistributedNode>,
) -> plan::DistributedNode {
    plan::DistributedNode {
        node_id,
        fragment_id: 1,
        tuple_ids: Vec::new(),
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children,
        payload: Some(plan::distributed_node::Payload::TableFinish(
            plan::TableFinishNode {
                expected_target_ordinals,
                writer_multiplex_schema: Some(writer_multiplex_schema(
                    &WriterMultiplexSchema::empty(),
                )),
                root_result_schema: Some(root_result_schema(&RootWriteResultSchema::new())),
                final_aggregate_plan: Some(plan::WriterFinalAggregatePlan {
                    calls: Vec::new(),
                    unpivot: None,
                }),
            },
        )),
    }
}

pub(crate) fn relation_fields(
    fields: &[(u32, &str, novarocks_type_contract::FunctionValueType)],
) -> (
    Vec<plan::ArrowPhysicalColumn>,
    Vec<novarocks_proto_models::value_type::LogicalTypeDefinition>,
) {
    use novarocks_plan_codec::execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
    };
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(
            &self,
            _: CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    let roots = fields
        .iter()
        .enumerate()
        .map(|(ordinal, (_, _, ty))| ExecutionTypeRoot {
            id: ordinal as u32,
            value: ty,
        })
        .collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
    let table = ExecutionTypeTableBuilder::new(&roots)
        .encode_observed::<std::convert::Infallible>(
            ExecutionTypeTableLimits::for_validated_root_count(roots.len()).unwrap(),
            &mut |_| {
                Control
                    .checkpoint(CompilePhase::Encode, 0)
                    .map_err(Into::into)
            },
            &mut work,
        )
        .unwrap()
        .into_wire();
    let columns = fields
        .iter()
        .zip(table.value_bindings)
        .map(|((slot, name, ty), binding)| plan::ArrowPhysicalColumn {
            slot_id: *slot,
            is_internal: true,
            field: Some(plan::ArrowPhysicalField {
                name: (*name).into(),
                nullable: ty.nullable(),
                r#type: binding.value_layout.map(Box::new),

                logical_type_id: binding.logical_type_id,
            }),
        })
        .collect();
    work.finish().unwrap();
    (columns, table.logical_types)
}
