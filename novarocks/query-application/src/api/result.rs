// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Query-application-owned result delivery contracts.
//!
//! A row result begins with one schema delivery, continues with zero or more
//! exactly sequenced Backend-encoded segment deliveries, and ends only with an explicitly
//! acknowledged success EOF. Every delivery is move-only and reports whether
//! the protocol consumer completed, failed, or dropped it. Each segment keeps
//! its original full-window alias through actual consumption and backing exit.

use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_execution_contract::ResultPacketSequence;
use novarocks_result_contract::{
    ClientRowProfile, ClientRowStreamCursor, RootOutputKind, ValidatedClientBody,
};
use novarocks_types::{QueryExecutionId, QueryId, schema::SqlType};
use tokio::sync::{mpsc, oneshot, watch};

use super::root_delivery::{RetainedRootReply, RootReplyView};
use super::{QueryExecutionError, QueryExecutionErrorKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultField {
    name: Arc<str>,
    data_type: DataType,
    nullable: bool,
    logical_type: Option<SqlType>,
}

impl ResultField {
    pub fn new(
        name: impl Into<Arc<str>>,
        data_type: DataType,
        nullable: bool,
        logical_type: Option<SqlType>,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            logical_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    pub const fn logical_type(&self) -> Option<&SqlType> {
        self.logical_type.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultSchema {
    fields: Arc<[ResultField]>,
}

/// Fully materialized immediate query output owned by the query application.
///
/// Unlike distributed [`QueryResultStream`] delivery, an immediate result has
/// already been produced by a synchronous application command. It retains
/// only Arrow batches and application-visible column metadata: execution
/// chunks and their slot mappings are an execution implementation detail and
/// must not escape into a client-session contract.
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<ResultField>,
    pub batches: Vec<RecordBatch>,
}

impl QueryResult {
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    /// Empty schema, empty batches. Used as the no-op output when an IVM
    /// branch (insert or delete) has zero input files or rows.
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            batches: Vec::new(),
        }
    }
}

/// Builds one bounded, fully materialized immediate result from product-owned
/// columns and Arrow arrays. Product adapters retain the meaning of their
/// columns and cells; this function owns the shared query-session schema and
/// batch projection. The arrays are checked against
/// [`LocalResultBound::V1`](super::LocalResultBound::V1) before the result is
/// published; sources with no structural bound push rows through
/// [`LocalTableBuilder`](super::LocalTableBuilder) instead.
pub fn build_arrow_query_result(
    columns: Vec<ResultField>,
    arrays: Vec<ArrayRef>,
) -> Result<QueryResult, String> {
    if columns.len() != arrays.len() {
        return Err("immediate result column and array counts must match".to_owned());
    }
    super::LocalResultBound::V1.check_arrays(&arrays)?;
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| Field::new(column.name(), column.data_type().clone(), column.nullable()))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|error| format!("build immediate result batch failed: {error}"))?;
    Ok(QueryResult {
        columns,
        batches: vec![batch],
    })
}

pub fn build_string_query_result(
    column_name: &str,
    rows: Vec<String>,
) -> Result<QueryResult, String> {
    let mut table =
        super::LocalTableBuilder::try_new(&[(column_name, false)], super::LocalResultBound::V1)?;
    for row in rows {
        table.push_row(&[Some(row)])?;
    }
    table
        .finish()
        .map_err(|error| format!("build immediate text result failed: {error}"))
}

/// Builds a bounded immediate table whose protocol-visible cells are required
/// UTF-8 values.
///
/// Command adapters retain their row semantics; this helper owns only the
/// common Arrow/schema projection into the query-session result contract.
/// It rejects ragged rows before appending them, so a product cannot publish
/// a schema that disagrees with its visible cells.
pub fn build_utf8_query_result(
    column_names: &[&str],
    rows: Vec<Vec<String>>,
) -> Result<QueryResult, String> {
    preflight_text_column_count(column_names.len())?;
    let columns = column_names
        .iter()
        .map(|name| (*name, false))
        .collect::<Vec<_>>();
    let mut table = super::LocalTableBuilder::try_new(&columns, super::LocalResultBound::V1)?;
    for row in rows {
        if row.len() != columns.len() {
            return Err(
                "immediate tabular result contains a row with the wrong column count".to_owned(),
            );
        }
        let row = row.into_iter().map(Some).collect::<Vec<_>>();
        table.push_row(&row)?;
    }
    table.finish()
}

/// Builds a bounded immediate UTF-8 table with the supplied per-column
/// nullability. Product command adapters retain ownership of the row and
/// column semantics; this helper owns the common Arrow/schema projection.
/// Each row is checked against [`LocalResultBound::V1`](super::LocalResultBound::V1)
/// before it is appended.
pub fn build_utf8_table_query_result(
    columns: &[(&str, bool)],
    rows: Vec<Vec<Option<String>>>,
) -> Result<QueryResult, String> {
    let mut table = super::LocalTableBuilder::try_new(columns, super::LocalResultBound::V1)?;
    for row in rows {
        table.push_row(&row)?;
    }
    table.finish()
}

/// Builds a bounded immediate table whose protocol-visible cells are nullable
/// UTF-8 values.
///
/// Product command adapters retain ownership of their row semantics; this
/// helper owns only the common Arrow/schema projection into the query-session
/// result contract. It rejects ragged rows before appending them, so a
/// product cannot publish a schema that disagrees with its visible cells.
pub fn build_nullable_utf8_query_result(
    column_names: &[&str],
    rows: Vec<Vec<Option<String>>>,
) -> Result<QueryResult, String> {
    preflight_text_column_count(column_names.len())?;
    let columns = column_names
        .iter()
        .map(|name| (*name, true))
        .collect::<Vec<_>>();
    build_utf8_table_query_result(&columns, rows)
}

fn preflight_text_column_count(columns: usize) -> Result<(), String> {
    let bound = super::LocalResultBound::V1.columns;
    if columns > bound {
        return Err(format!(
            "local result has {columns} columns, beyond its {bound} column bound"
        ));
    }
    Ok(())
}

impl ResultSchema {
    pub fn new(fields: impl Into<Arc<[ResultField]>>) -> Self {
        Self {
            fields: fields.into(),
        }
    }

    pub fn fields(&self) -> &[ResultField] {
        &self.fields
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn arrow_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(
            self.fields
                .iter()
                .map(|field| Field::new(field.name(), field.data_type().clone(), field.nullable()))
                .collect::<Vec<_>>(),
        ))
    }
}

pub enum ExecutionOutput {
    Rows(QueryResultStream),
    /// The product owner finalized its effect without a row stream. Commit and
    /// publication evidence remains inside that product-specific lifecycle.
    Completion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResultDeliveryDisposition {
    Completed,
    Failed(QueryExecutionError),
    Dropped,
}

pub(crate) type ResultDeliveryReceipt = oneshot::Receiver<ResultDeliveryDisposition>;

struct DeliverySignal {
    sender: Option<oneshot::Sender<ResultDeliveryDisposition>>,
}

impl DeliverySignal {
    fn channel() -> (Self, ResultDeliveryReceipt) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                sender: Some(sender),
            },
            receiver,
        )
    }

    fn finish(&mut self, disposition: ResultDeliveryDisposition) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(disposition);
        }
    }
}

impl Drop for DeliverySignal {
    fn drop(&mut self) {
        self.finish(ResultDeliveryDisposition::Dropped);
    }
}

/// Move-only schema delivery that starts a row result.
/// How the rows of one result stream reach its consumer. It is fixed when the
/// stream is created, before the schema is published, so a protocol writer
/// chooses its framing before it writes any metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultRowCarrier {
    /// Rows arrive as decoded Arrow batches.
    DecodedBatches,
    /// Rows arrive as Backend-encoded root items of this output kind, in
    /// order; ClientRows items carry this frozen row profile.
    Relayed {
        kind: RootOutputKind,
        client_rows: Option<ClientRowProfile>,
    },
}

impl ResultRowCarrier {
    /// A relayed carrier whose row profile matches its output kind.
    pub fn relayed(
        kind: RootOutputKind,
        client_rows: Option<ClientRowProfile>,
    ) -> Result<Self, QueryExecutionError> {
        if client_rows.is_some() != (kind == RootOutputKind::ClientRows) {
            return Err(invalid_result_delivery(
                "a relayed result carrier's row profile must match its output kind",
            ));
        }
        Ok(Self::Relayed { kind, client_rows })
    }
}

pub struct SchemaDelivery {
    query_id: QueryId,
    schema: ResultSchema,
    carrier: ResultRowCarrier,
    signal: DeliverySignal,
}

impl SchemaDelivery {
    pub(crate) fn new(
        query_id: QueryId,
        schema: ResultSchema,
        carrier: ResultRowCarrier,
    ) -> (Self, ResultDeliveryReceipt) {
        let (signal, receipt) = DeliverySignal::channel();
        (
            Self {
                query_id,
                schema,
                carrier,
                signal,
            },
            receipt,
        )
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub const fn schema(&self) -> &ResultSchema {
        &self.schema
    }

    /// How this stream's rows will arrive.
    pub const fn row_carrier(&self) -> ResultRowCarrier {
        self.carrier
    }

    pub fn complete(mut self) {
        self.signal.finish(ResultDeliveryDisposition::Completed);
    }

    pub fn fail(mut self, error: QueryExecutionError) {
        self.signal.finish(ResultDeliveryDisposition::Failed(error));
    }
}

/// Successful EOF for one exact execution. Only the logical execution actor
/// may construct this after it has irreversibly committed logical success.
/// The consumer disposition settles visible transport only; it cannot revoke
/// or replace that logical conclusion.
pub struct EndDelivery {
    execution_id: QueryExecutionId,
    sequence: ResultPacketSequence,
    root_output_rows: Option<u64>,
    signal: DeliverySignal,
}

impl EndDelivery {
    pub(crate) fn success_eof(
        execution_id: QueryExecutionId,
        sequence: ResultPacketSequence,
    ) -> (Self, ResultDeliveryReceipt) {
        Self::success_eof_with_root_rows(execution_id, sequence, None)
    }

    pub(crate) fn success_eof_with_root_rows(
        execution_id: QueryExecutionId,
        sequence: ResultPacketSequence,
        root_output_rows: Option<u64>,
    ) -> (Self, ResultDeliveryReceipt) {
        let (signal, receipt) = DeliverySignal::channel();
        (
            Self {
                execution_id,
                sequence,
                root_output_rows,
                signal,
            },
            receipt,
        )
    }

    /// Checked row count of the original root plan, carried by its locally
    /// consumed V1 End.
    pub const fn root_output_rows(&self) -> Option<u64> {
        self.root_output_rows
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn sequence(&self) -> ResultPacketSequence {
        self.sequence
    }

    /// Records successful transport of the actor-authorized EOF.
    pub fn complete(mut self) {
        self.signal.finish(ResultDeliveryDisposition::Completed);
    }

    /// Records failed transport of the actor-authorized EOF. The protocol or
    /// application owner decides its externally visible outcome separately.
    pub fn fail(mut self, error: QueryExecutionError) {
        self.signal.finish(ResultDeliveryDisposition::Failed(error));
    }
}

/// Move-only ownership of one validated Backend-encoded root data item and
/// the window alias covering its backing. Completing it is the in-order
/// delivery receipt that lets the relay acknowledge the item. A shared window
/// owner may retain the backing through that receipt handoff for a closing cut;
/// capacity remains held until its last actual owner exits.
pub struct RootSegmentDelivery {
    execution_id: QueryExecutionId,
    sequence: ResultPacketSequence,
    reply: Option<Arc<RetainedRootReply>>,
    client_rows: Option<(ClientRowProfile, ClientRowStreamCursor)>,
    rows: u64,
    resident_window: Option<super::RootRelayResidentWindow>,
    signal: DeliverySignal,
}

impl RootSegmentDelivery {
    /// `reply` must hold Data. ClientRows carries its frozen profile and the
    /// row cursor before this body, against which the body already validated.
    pub(crate) fn try_new(
        execution_id: QueryExecutionId,
        sequence: ResultPacketSequence,
        reply: impl Into<Arc<RetainedRootReply>>,
        client_rows: Option<(ClientRowProfile, ClientRowStreamCursor)>,
        rows: u64,
    ) -> Result<(Self, ResultDeliveryReceipt), QueryExecutionError> {
        let reply = reply.into();
        let body = match reply.outcome() {
            RootReplyView::Data { body, .. } => body,
            _ => {
                return Err(invalid_result_delivery(
                    "a root segment delivery requires a data item",
                ));
            }
        };
        if client_rows.is_some() != (reply.kind() == RootOutputKind::ClientRows) {
            return Err(invalid_result_delivery(
                "a root segment's row profile must match its output kind",
            ));
        }
        if let Some((profile, before)) = client_rows {
            let validated = before.validate_body(profile, body).map_err(|error| {
                invalid_result_delivery(format!("root segment rows are malformed: {error}"))
            })?;
            if validated.after().completed_rows() - before.completed_rows() != rows {
                return Err(invalid_result_delivery(
                    "a root segment's row count differs from its body",
                ));
            }
        }
        let (signal, receipt) = DeliverySignal::channel();
        Ok((
            Self {
                execution_id,
                sequence,
                reply: Some(reply),
                client_rows,
                rows,
                resident_window: None,
                signal,
            },
            receipt,
        ))
    }

    pub(crate) fn with_resident_window(mut self, window: super::RootRelayResidentWindow) -> Self {
        self.resident_window = Some(window);
        self
    }
    /// Retains the bounded window through between-delivery protocol phases.
    /// This shares ownership and exposes no independently clonable bytes.
    pub fn resident_window(&self) -> Option<super::RootRelayResidentWindow> {
        self.resident_window.clone()
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }
    pub const fn sequence(&self) -> ResultPacketSequence {
        self.sequence
    }
    /// Rows this item completes. Client rows follow validated payload boundaries;
    /// ScalarValueV1 uses its sealed End count, checked by the typed consumer
    /// before the receipt completes. Other internal domains carry zero here.
    pub const fn rows(&self) -> u64 {
        self.rows
    }
    pub fn kind(&self) -> RootOutputKind {
        self.reply().kind()
    }
    fn reply(&self) -> &RetainedRootReply {
        self.reply
            .as_ref()
            .expect("root segment delivery owns its reply before completion")
    }
    /// The item's exact body bytes.
    pub fn body(&self) -> &[u8] {
        match self.reply().outcome() {
            RootReplyView::Data { body, .. } => body,
            _ => unreachable!("a root segment delivery holds a data item"),
        }
    }
    /// ClientRows payload spans, from the row cursor before this body.
    pub fn client_rows(&self) -> Option<ValidatedClientBody<'_>> {
        let (profile, before) = self.client_rows?;
        Some(
            before
                .validate_body(profile, self.body())
                .expect("a root segment validated at construction"),
        )
    }
    /// Complete protocol consumption. The relay then retires its shared owner
    /// before acknowledging this receipt to the Backend.
    pub fn complete(mut self) {
        drop(self.reply.take());
        self.signal.finish(ResultDeliveryDisposition::Completed);
    }
    pub fn fail(mut self, error: QueryExecutionError) {
        drop(self.reply.take());
        self.signal.finish(ResultDeliveryDisposition::Failed(error));
    }
}

impl Drop for RootSegmentDelivery {
    fn drop(&mut self) {
        drop(self.reply.take());
        self.signal.finish(ResultDeliveryDisposition::Dropped);
    }
}

pub enum ResultDelivery {
    /// A Backend-encoded root item, relayed without Arrow decode.
    Segment(RootSegmentDelivery),
    End(EndDelivery),
}

impl ResultDelivery {
    pub const fn execution_id(&self) -> QueryExecutionId {
        match self {
            Self::Segment(delivery) => delivery.execution_id(),
            Self::End(delivery) => delivery.execution_id(),
        }
    }
}

type StreamMessage = ResultDelivery;

enum StreamEvent {
    Failure(QueryExecutionError),
    Message(Option<StreamMessage>),
}

/// Cloneable observation of a logical failure that must interrupt any current
/// schema or batch protocol write. The protocol adapter retains one view while
/// it owns a delivery, instead of waiting for the next stream item.
#[derive(Clone, Debug)]
pub struct ResultFailureView {
    receiver: watch::Receiver<Option<QueryExecutionError>>,
}

impl ResultFailureView {
    pub fn current(&self) -> Option<QueryExecutionError> {
        self.receiver.borrow().clone()
    }

    pub async fn wait(&mut self) -> QueryExecutionError {
        loop {
            if let Some(error) = self.current() {
                return error;
            }
            if self.receiver.changed().await.is_err() {
                return failed_result_delivery_message(
                    "logical result owner disappeared before success EOF",
                );
            }
        }
    }
}

/// Pure bounded transport for one actor-owned result stream.
///
/// Query identity, attempt eligibility, packet sequencing, schema state, and
/// visibility remain exclusively in the logical execution actor. This value
/// only reserves queue capacity and synchronously transfers an already
/// authorized delivery into that slot.
#[derive(Clone)]
pub(crate) struct QueryResultTransport {
    sender: mpsc::Sender<StreamMessage>,
}

impl std::fmt::Debug for QueryResultTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryResultTransport")
            .field("capacity", &self.sender.capacity())
            .field("closed", &self.sender.is_closed())
            .finish()
    }
}

pub(crate) struct ResultQueuePermit {
    permit: mpsc::OwnedPermit<StreamMessage>,
}

impl QueryResultTransport {
    pub(crate) async fn reserve_owned(&self) -> Result<ResultQueuePermit, QueryExecutionError> {
        self.sender
            .clone()
            .reserve_owned()
            .await
            .map(|permit| ResultQueuePermit { permit })
            .map_err(|_| failed_result_delivery_message("result stream consumer disappeared"))
    }

    pub(crate) fn enqueue(&self, permit: ResultQueuePermit, delivery: ResultDelivery) {
        permit.permit.send(delivery);
    }

    pub(crate) async fn closed(&self) {
        self.sender.closed().await;
    }
}

pub struct QueryResultStream {
    query_id: QueryId,
    schema: Option<SchemaDelivery>,
    receiver: mpsc::Receiver<StreamMessage>,
    failure: Option<ResultFailureView>,
    terminal_seen: bool,
}

impl QueryResultStream {
    pub(crate) fn try_channel(
        query_id: QueryId,
        schema: ResultSchema,
        carrier: ResultRowCarrier,
        delivery_capacity: usize,
    ) -> Result<
        (
            QueryResultTransport,
            ResultDeliveryReceipt,
            watch::Sender<Option<QueryExecutionError>>,
            Self,
        ),
        QueryExecutionError,
    > {
        if delivery_capacity == 0 {
            return Err(invalid_result_delivery(
                "result delivery capacity must be nonzero",
            ));
        }
        let (schema_delivery, schema_receipt) =
            SchemaDelivery::new(query_id, schema.clone(), carrier);
        let (sender, receiver) = mpsc::channel(delivery_capacity);
        let (failure_sender, failure) = watch::channel(None);
        Ok((
            QueryResultTransport { sender },
            schema_receipt,
            failure_sender,
            Self {
                query_id,
                schema: Some(schema_delivery),
                receiver,
                failure: Some(ResultFailureView { receiver: failure }),
                terminal_seen: false,
            },
        ))
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub fn begin_schema(&mut self) -> Option<SchemaDelivery> {
        self.schema.take()
    }

    pub fn failure_view(&self) -> Option<ResultFailureView> {
        self.failure.clone()
    }

    pub async fn next(&mut self) -> Result<Option<ResultDelivery>, QueryExecutionError> {
        if self.schema.is_some() {
            return Err(invalid_result_delivery(
                "result schema must begin before reading result batches",
            ));
        }
        if self.terminal_seen {
            return Ok(None);
        }
        let event = if let Some(failure) = self.failure.as_mut() {
            tokio::select! {
                biased;
                failure = failure.wait() => StreamEvent::Failure(failure),
                message = self.receiver.recv() => StreamEvent::Message(message),
            }
        } else {
            StreamEvent::Message(self.receiver.recv().await)
        };
        let message = match event {
            StreamEvent::Failure(error) => {
                self.failure.take();
                self.receiver.close();
                while let Ok(delivery) = self.receiver.try_recv() {
                    drop(delivery);
                }
                self.terminal_seen = true;
                return Err(error);
            }
            StreamEvent::Message(message) => message,
        };
        match message {
            Some(delivery @ ResultDelivery::Segment(_)) => Ok(Some(delivery)),
            Some(delivery @ ResultDelivery::End(_)) => {
                self.terminal_seen = true;
                Ok(Some(delivery))
            }
            None => {
                self.terminal_seen = true;
                Err(failed_result_delivery_message(
                    "result stream closed before success EOF",
                ))
            }
        }
    }
}

fn invalid_result_delivery(message: impl Into<Arc<str>>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, message)
}

fn failed_result_delivery_message(message: impl Into<Arc<str>>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::Failed, message)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, ArrayRef, Int64Array, StringArray},
        datatypes::DataType,
    };
    use novarocks_types::{AttemptId, QueryId};
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
        WorkloadConfig, WorkloadControl,
    };

    use super::*;

    fn execution_id(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(17, 23), AttemptId::new(attempt).unwrap()).unwrap()
    }

    fn result_schema() -> ResultSchema {
        ResultSchema::new(vec![ResultField::new(
            "value",
            DataType::Int64,
            false,
            Some(SqlType::BigInt),
        )])
    }

    fn workload() -> (
        WorkloadControl,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        (control, capacity)
    }

    fn carrier() -> ResultRowCarrier {
        ResultRowCarrier::relayed(
            RootOutputKind::ClientRows,
            Some(
                ClientRowProfile::try_new(
                    novarocks_result_contract::RootProfileV1::SEGMENT_BYTES,
                    novarocks_result_contract::RootProfileV1::ROW_PAYLOAD_BYTES,
                )
                .unwrap(),
            ),
        )
        .unwrap()
    }

    #[test]
    fn immediate_query_result_retains_arrow_batches_without_execution_chunks() {
        let result = build_string_query_result(
            "Explain String",
            vec!["first".to_string(), "second".to_string()],
        )
        .expect("build immediate result");

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].name(), "Explain String");
        assert_eq!(result.row_count(), 2);
        let values = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string output");
        assert_eq!(values.value(0), "first");
        assert_eq!(values.value(1), "second");
    }

    #[test]
    fn generic_immediate_result_projection_preserves_typed_columns() {
        let result = build_arrow_query_result(
            vec![
                ResultField::new("count", DataType::Int64, false, None),
                ResultField::new("active", DataType::Boolean, true, None),
            ],
            vec![
                Arc::new(Int64Array::from(vec![3_i64])) as ArrayRef,
                Arc::new(arrow::array::BooleanArray::from(vec![Some(true)])) as ArrayRef,
            ],
        )
        .expect("build typed immediate result");

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.batches[0].num_rows(), 1);
        assert_eq!(
            result.batches[0].schema().field(0).data_type(),
            &DataType::Int64
        );
        assert_eq!(
            result.batches[0].schema().field(1).data_type(),
            &DataType::Boolean
        );
    }

    #[test]
    fn generic_immediate_result_projection_rejects_mismatched_columns_and_arrays() {
        let error = build_arrow_query_result(
            vec![ResultField::new("count", DataType::Int64, false, None)],
            Vec::new(),
        )
        .expect_err("mismatched immediate result must fail");

        assert_eq!(error, "immediate result column and array counts must match");
    }

    #[test]
    fn nullable_text_table_projection_preserves_schema_cells_and_nulls() {
        let result = build_nullable_utf8_query_result(
            &["job_id", "detail"],
            vec![
                vec![Some("job-1".to_owned()), None],
                vec![Some("job-2".to_owned()), Some("complete".to_owned())],
            ],
        )
        .expect("build nullable text table");

        assert_eq!(result.columns.len(), 2);
        assert!(result.columns.iter().all(ResultField::nullable));
        let detail = result.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("nullable string output");
        assert!(detail.is_null(0));
        assert_eq!(detail.value(1), "complete");
    }

    #[test]
    fn required_text_table_projection_preserves_schema_cells() {
        let result = build_utf8_query_result(
            &["name", "state"],
            vec![vec!["be-1".to_owned(), "Live".to_owned()]],
        )
        .expect("build required text table");

        assert_eq!(result.columns.len(), 2);
        assert!(result.columns.iter().all(|column| !column.nullable()));
        let state = result.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("required string output");
        assert_eq!(state.value(0), "Live");
    }

    #[test]
    fn nullable_text_table_projection_rejects_ragged_rows() {
        let error = build_nullable_utf8_query_result(
            &["job_id", "detail"],
            vec![vec![Some("job-1".to_owned())]],
        )
        .expect_err("ragged rows must not produce a visible schema");
        assert_eq!(
            error,
            "immediate tabular result contains a row with the wrong column count"
        );
    }

    #[test]
    fn text_helpers_refuse_wide_schema_and_ragged_required_rows_before_projection() {
        let columns = vec!["column"; super::super::LocalResultBound::V1.columns + 1];
        for error in [
            build_utf8_query_result(&columns, Vec::new()).unwrap_err(),
            build_nullable_utf8_query_result(&columns, Vec::new()).unwrap_err(),
        ] {
            assert!(error.contains("4097 columns, beyond its 4096 column bound"));
        }
        let error = build_utf8_query_result(&["one"], vec![vec!["x".into(), "y".into()]])
            .expect_err("required row width must be checked before Option projection");
        assert_eq!(
            error,
            "immediate tabular result contains a row with the wrong column count"
        );
    }

    #[test]
    fn mixed_text_table_projection_preserves_per_column_nullability() {
        let result = build_utf8_table_query_result(
            &[("catalog_name", false), ("sql_path", true)],
            vec![vec![Some("lake".to_owned()), None]],
        )
        .expect("build mixed-nullability text table");

        assert!(!result.columns[0].nullable());
        assert!(result.columns[1].nullable());
        assert!(!result.batches[0].schema().field(0).is_nullable());
        assert!(result.batches[0].schema().field(1).is_nullable());
    }

    #[test]
    fn mixed_text_table_projection_rejects_null_in_required_column() {
        let error = build_utf8_table_query_result(&[("catalog_name", false)], vec![vec![None]])
            .expect_err("required column must reject null");
        assert_eq!(
            error,
            "immediate tabular result contains null in a required column"
        );
    }

    #[tokio::test]
    async fn schema_disposition_distinguishes_complete_failure_and_drop() {
        let id = execution_id(1);

        let (complete, complete_receipt) =
            SchemaDelivery::new(id.query_id(), result_schema(), carrier());
        complete.complete();
        assert_eq!(
            complete_receipt.await.unwrap(),
            ResultDeliveryDisposition::Completed
        );

        let expected = QueryExecutionError::new(QueryExecutionErrorKind::Failed, "encode schema");
        let (failed, failed_receipt) =
            SchemaDelivery::new(id.query_id(), result_schema(), carrier());
        failed.fail(expected.clone());
        assert_eq!(
            failed_receipt.await.unwrap(),
            ResultDeliveryDisposition::Failed(expected)
        );

        let (dropped, dropped_receipt) =
            SchemaDelivery::new(id.query_id(), result_schema(), carrier());
        drop(dropped);
        assert_eq!(
            dropped_receipt.await.unwrap(),
            ResultDeliveryDisposition::Dropped
        );
    }

    #[tokio::test]
    async fn stream_transport_only_moves_actor_authorized_deliveries() {
        let (control, capacity) = workload();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let id = execution_id(2);
        let (transport, schema_receipt, _failure_sender, mut stream) =
            QueryResultStream::try_channel(id.query_id(), result_schema(), carrier(), 1).unwrap();

        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("result batches must not precede the schema"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::InvalidRequest);
        let delivered_schema = stream.begin_schema().unwrap();
        assert_eq!(delivered_schema.query_id(), id.query_id());
        delivered_schema.complete();
        assert_eq!(
            schema_receipt.await.unwrap(),
            ResultDeliveryDisposition::Completed
        );

        use novarocks_execution_contract::{
            TaskIdentity,
            root_result::{RootReadOutcome, RootResultData, RootResultReply},
        };
        use novarocks_result_contract::RootProfileId;
        use novarocks_types::{BackendProcessId, StageId, TaskId};
        let window = capacity
            .try_acquire(&root.owner.scope(), ResultWindowClass::Client)
            .unwrap();
        let reply = RetainedRootReply::try_new(
            RootResultReply {
                root_task: TaskIdentity::new(
                    id,
                    StageId::new(1).unwrap(),
                    TaskId::new(1).unwrap(),
                    BackendProcessId::new_v7(),
                ),
                profile: RootProfileId::V1,
                kind: RootOutputKind::ClientRows,
                accepted_consumed: 0,
                outcome: RootReadOutcome::Data(
                    RootResultData::try_new(
                        RootOutputKind::ClientRows,
                        std::num::NonZeroU64::new(1).unwrap(),
                        bytes::Bytes::from_static(&[2, 0, 0, 0, 1, b'7']),
                        None,
                    )
                    .unwrap(),
                ),
            },
            window.retain_alias(),
            4096,
        )
        .unwrap();
        drop(window);
        let (delivery, receipt) = RootSegmentDelivery::try_new(
            id,
            ResultPacketSequence::new(0),
            reply,
            Some((
                match carrier() {
                    ResultRowCarrier::Relayed {
                        client_rows: Some(profile),
                        ..
                    } => profile,
                    _ => unreachable!(),
                },
                ClientRowStreamCursor::default(),
            )),
            1,
        )
        .unwrap();
        let slot = transport.reserve_owned().await.unwrap();
        transport.enqueue(slot, ResultDelivery::Segment(delivery));
        let ResultDelivery::Segment(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("expected batch delivery");
        };
        assert_eq!(delivery.execution_id(), id);
        assert_eq!(delivery.sequence(), ResultPacketSequence::new(0));
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        delivery.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "client disconnected",
        ));
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert!(matches!(
            receipt.await.unwrap(),
            ResultDeliveryDisposition::Failed(_)
        ));

        let (eof, eof_receipt) = EndDelivery::success_eof(id, ResultPacketSequence::new(1));
        let slot = transport.reserve_owned().await.unwrap();
        transport.enqueue(slot, ResultDelivery::End(eof));
        let ResultDelivery::End(eof) = stream.next().await.unwrap().unwrap() else {
            panic!("expected EOF delivery");
        };
        assert_eq!(eof.execution_id(), id);
        assert_eq!(eof.sequence(), ResultPacketSequence::new(1));
        eof.complete();
        assert_eq!(
            eof_receipt.await.unwrap(),
            ResultDeliveryDisposition::Completed
        );
        drop(transport);
        assert!(stream.next().await.unwrap().is_none());
        drop(root);
    }

    #[tokio::test]
    async fn stream_failure_is_terminal_without_success_eof() {
        let id = execution_id(3);
        let (_transport, schema_receipt, failure_sender, mut stream) =
            QueryResultStream::try_channel(id.query_id(), result_schema(), carrier(), 1).unwrap();
        stream.begin_schema().unwrap().complete();
        assert_eq!(
            schema_receipt.await.unwrap(),
            ResultDeliveryDisposition::Completed
        );

        let expected = QueryExecutionError::new(QueryExecutionErrorKind::Failed, "attempt failed");
        failure_sender.send_replace(Some(expected.clone()));
        let actual = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("failed stream must return its terminal error"),
        };
        assert_eq!(actual, expected);
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn owner_loss_drops_queued_success_eof() {
        let id = execution_id(4);
        let (transport, schema_receipt, failure_sender, mut stream) =
            QueryResultStream::try_channel(id.query_id(), result_schema(), carrier(), 1).unwrap();
        stream.begin_schema().unwrap().complete();
        assert_eq!(
            schema_receipt.await.unwrap(),
            ResultDeliveryDisposition::Completed
        );

        let (eof, eof_receipt) = EndDelivery::success_eof(id, ResultPacketSequence::new(0));
        let slot = transport.reserve_owned().await.unwrap();
        transport.enqueue(slot, ResultDelivery::End(eof));
        drop(failure_sender);
        drop(transport);

        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("owner loss must preempt a queued success EOF"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            eof_receipt.await.unwrap(),
            ResultDeliveryDisposition::Dropped
        );
        assert!(stream.next().await.unwrap().is_none());
    }
}
