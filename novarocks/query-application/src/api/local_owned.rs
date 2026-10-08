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

//! Closed ownership of a Local result graph and its pure render cursor.

use super::{LocalResultBound, QueryResult};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_result_contract::{
    ClientRenderSchema, NativeRenderType, RenderColumn, RenderField, RenderPresentation,
    RootProfileV1,
};
use novarocks_result_render::{
    ArrowMysqlTextEncoder, BoundedMysqlTextEncoder, RenderError, RenderTurn, RenderTurnStatus,
};
use novarocks_workload_control::{ResultWindowAlias, ResultWindowClass, WorkScope};
use std::sync::Arc;

// Frozen Local source + conversion + output overlap, not another wallet.
const LOCAL_PEAK_BYTES: u64 = (16 + 1 + 4 + 32 + 32 + 8) * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalColumnKind {
    Utf8,
    Boolean,
    Int32,
    Int64,
}

/// Borrowed metadata cannot publish an Arrow/schema/ResultField alias.
#[derive(Clone, Copy, Debug)]
pub struct LocalColumnView<'a> {
    pub name: &'a str,
    pub nullable: bool,
    pub kind: LocalColumnKind,
}

/// One admitted, closed application producer. Its callback must construct a
/// fresh graph, without retaining or publishing Arrow aliases. This contract
/// is established by the application source audit; it cannot be inferred from
/// an arbitrary QueryResult's capacities. Internal/legacy results do not enter
/// this factory merely because they contain Arrow.
pub struct LocalResultProducer {
    window: ResultWindowAlias,
}
impl LocalResultProducer {
    pub fn try_new(scope: &WorkScope, window: ResultWindowAlias) -> Result<Self, String> {
        if !window.is_for_scope(scope)
            || !matches!(
                window.class(),
                ResultWindowClass::Local | ResultWindowClass::Internal
            )
        {
            return Err("Local producer requires its exact Local or Internal window".into());
        }
        scope.check().map_err(|error| error.to_string())?;
        window
            .check_backing_total(LOCAL_PEAK_BYTES)
            .map_err(|error| error.to_string())?;
        Ok(Self { window })
    }
    /// The complete allowance already exists before the source is called.
    /// Failure/panic destroys source locals before this capability can exit.
    pub fn produce(
        self,
        source: impl FnOnce() -> Result<QueryResult, String>,
    ) -> Result<OwnedLocalResult, String> {
        let result = source()?;
        OwnedLocalResult::seal_exclusive(result, self.window)
    }
}

/// No Clone, raw-batch getter or graph extraction is provided. All copies made
/// by rendering stay inside the same guarded graph.
///
/// ```compile_fail
/// use novarocks_query_application::api::OwnedLocalResult;
/// fn escape(result: OwnedLocalResult) { let _ = result.into_batches(); }
/// ```
pub struct OwnedLocalResult {
    result: QueryResult,
    schema: Arc<ClientRenderSchema>,
    // Fields drop in declaration order: every graph owner precedes the guard.
    window: ResultWindowAlias,
}
struct LocalSourceInput {
    result: QueryResult,
    window: ResultWindowAlias,
}
impl OwnedLocalResult {
    pub(crate) fn seal_exclusive(
        result: QueryResult,
        window: ResultWindowAlias,
    ) -> Result<Self, String> {
        let input = LocalSourceInput { result, window };
        if !matches!(
            input.window.class(),
            ResultWindowClass::Local | ResultWindowClass::Internal
        ) {
            return Err("Local graph requires a Local or Internal window".into());
        }
        let fields = &input.result.columns;
        if fields.is_empty() || fields.len() > RootProfileV1::MAX_COLUMNS {
            return Err("Local result requires a bounded nonempty schema".into());
        }
        let mut backing =
            size_of::<ClientRenderSchema>() + fields.len() * size_of::<RenderColumn>();
        let mut wire = 0usize;
        for field in fields {
            kind(field.data_type(), field.logical_type().is_some())?;
            if field.name().is_empty() || field.name().len() > RootProfileV1::MAX_NAME_BYTES {
                return Err("Local result name exceeds its schema bound".into());
            }
            backing = backing
                .checked_add(field.name().len())
                .ok_or("Local schema size overflows")?;
            wire = wire
                .checked_add(field.name().len())
                .and_then(|n| n.checked_add(48))
                .ok_or("Local schema size overflows")?;
        }
        if backing > RootProfileV1::SCHEMA_BACKING_BYTES || wire > RootProfileV1::SCHEMA_WIRE_BYTES
        {
            return Err("Local render schema exceeds its backing/wire bound".into());
        }
        let mut rows = 0usize;
        let mut buffers = 0usize;
        for batch in &input.result.batches {
            if batch.num_columns() != fields.len() {
                return Err("Local result batch differs from its declared schema".into());
            }
            rows = rows
                .checked_add(batch.num_rows())
                .ok_or("Local row count overflows")?;
            for (array, (arrow_field, field)) in batch
                .columns()
                .iter()
                .zip(batch.schema_ref().fields().iter().zip(fields))
            {
                if array.data_type() != field.data_type()
                    || arrow_field.data_type() != field.data_type()
                    || arrow_field.is_nullable() != field.nullable()
                {
                    return Err("Local result batch differs from its declared schema".into());
                }
                buffers = buffers
                    .checked_add(array.get_buffer_memory_size())
                    .ok_or("Local buffer capacities overflow")?;
            }
        }
        if rows > LocalResultBound::V1.rows
            || input.result.batches.len() > LocalResultBound::V1.rows + 1
        {
            return Err("Local result exceeds its row/batch bound".into());
        }
        // Collector payload plus fixed per-column buffer padding/offset zero
        // occupies at most 1 MiB of the already admitted 8 MiB root share.
        // Source growth checks, not this transfer check, prove construction.
        if buffers > LocalResultBound::V1.bytes + RootProfileV1::SCHEMA_BACKING_BYTES {
            return Err("Local result exceeds its actual buffer capacity bound".into());
        }
        input
            .window
            .check_backing_total(LOCAL_PEAK_BYTES)
            .map_err(|error| error.to_string())?;
        let columns = fields
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                Ok(RenderColumn {
                    source_ordinal: ordinal as u32,
                    source_slot: None,
                    name: field.name().to_owned(),
                    field: RenderField {
                        presentation: RenderPresentation::ScalarText,
                        nullable: field.nullable(),
                        native_type: native(kind(field.data_type(), false)?),
                    },
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let schema = Arc::new(
            ClientRenderSchema::try_new(columns, fields.len())
                .map_err(|error| error.to_string())?,
        );
        let LocalSourceInput { result, window } = input;
        Ok(Self {
            result,
            schema,
            window,
        })
    }
    /// Scalar compatibility fact only; the guarded graph cannot escape to the
    /// transitional LRA adapter. Remove this with P08's old-credit retirement.
    pub(crate) fn legacy_governance_charge(&self) -> Result<u64, String> {
        self.result
            .batches
            .iter()
            .try_fold(0_u64, |sum, batch| {
                sum.checked_add(
                    super::decoded_result_batch_governance_charge(batch)
                        .map_err(|error| error.to_string())?,
                )
                .ok_or_else(|| "Local legacy governance charge overflows".to_owned())
            })
            .map(|bytes| bytes.max(1))
    }
    pub fn row_count(&self) -> usize {
        self.result.row_count()
    }
    pub fn columns(&self) -> impl ExactSizeIterator<Item = LocalColumnView<'_>> {
        self.result.columns.iter().map(|field| LocalColumnView {
            name: field.name(),
            nullable: field.nullable(),
            kind: kind(field.data_type(), false).expect("sealed Local column kind"),
        })
    }
    pub fn into_cursor(self) -> LocalRenderCursor {
        let Self {
            result,
            schema,
            window,
        } = self;
        LocalRenderCursor {
            encoder: None,
            remaining: result.batches.into_iter(),
            schema,
            terminal_error: None,
            _window: window,
        }
    }
}
fn kind(data_type: &DataType, has_logical_type: bool) -> Result<LocalColumnKind, String> {
    if has_logical_type {
        return Err("Local result has an unsupported logical domain".into());
    }
    match data_type {
        DataType::Utf8 => Ok(LocalColumnKind::Utf8),
        DataType::Boolean => Ok(LocalColumnKind::Boolean),
        DataType::Int32 => Ok(LocalColumnKind::Int32),
        DataType::Int64 => Ok(LocalColumnKind::Int64),
        _ => Err("Local result has an unsupported declared type".into()),
    }
}
fn native(kind: LocalColumnKind) -> NativeRenderType {
    match kind {
        LocalColumnKind::Utf8 => NativeRenderType::String,
        LocalColumnKind::Boolean => NativeRenderType::Boolean,
        LocalColumnKind::Int32 => NativeRenderType::SignedInteger(32),
        LocalColumnKind::Int64 => NativeRenderType::SignedInteger(64),
    }
}

/// A move-only cursor that exposes only encoded bytes. Its private renderer,
/// remaining batches, schema and scratch all exit before the final guard.
pub struct LocalRenderCursor {
    encoder: Option<ArrowMysqlTextEncoder>,
    remaining: std::vec::IntoIter<RecordBatch>,
    schema: Arc<ClientRenderSchema>,
    terminal_error: Option<RenderError>,
    _window: ResultWindowAlias,
}
impl LocalRenderCursor {
    pub fn step(&mut self, output: &mut [u8]) -> Result<RenderTurn, RenderError> {
        if let Some(error) = self.terminal_error {
            return Err(error);
        }
        let outcome = self.step_live(output);
        if let Err(error) = outcome {
            self.terminal_error = Some(error);
        }
        outcome
    }
    fn step_live(&mut self, output: &mut [u8]) -> Result<RenderTurn, RenderError> {
        if self.encoder.is_none() {
            let Some(batch) = self.remaining.next() else {
                return Ok(RenderTurn {
                    emitted_bytes: 0,
                    examined_bytes: 0,
                    visited_cells: 0,
                    completed_rows: 0,
                    status: RenderTurnStatus::InputComplete,
                });
            };
            self.encoder = Some(ArrowMysqlTextEncoder::try_new(
                Arc::clone(&self.schema),
                batch,
            )?);
        }
        let mut turn = self
            .encoder
            .as_mut()
            .expect("installed Local encoder")
            .step(output)?;
        if turn.status == RenderTurnStatus::InputComplete {
            self.encoder = None;
            if !self.remaining.as_slice().is_empty() {
                turn.status = RenderTurnStatus::Yielded;
            }
        }
        Ok(turn)
    }
    pub fn retain_physical_guard(&self) -> ResultWindowAlias {
        self._window.clone()
    }
    pub fn cancel(&mut self) {
        if self.terminal_error.is_none() {
            self.terminal_error = Some(RenderError {
                kind: novarocks_result_render::RenderErrorKind::Cancelled,
                output_ordinal: None,
            });
        }
        if let Some(encoder) = &mut self.encoder {
            encoder.cancel();
        }
    }
    pub fn scratch_capacity_bytes(&self) -> usize {
        self.encoder
            .as_ref()
            .map_or(0, BoundedMysqlTextEncoder::scratch_capacity_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    fn fixture() -> (
        WorkloadControl,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        let host = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = host
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        host.mark_ready().unwrap();
        (host, capacity)
    }
    fn produce_text(host: &WorkloadControl, text: &str) -> OwnedLocalResult {
        let (root, window) = host
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let source =
            LocalResultProducer::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
        let result = source
            .produce(|| super::super::build_string_query_result("value", vec![text.to_owned()]))
            .unwrap();
        root.owner.complete();
        root.business.release();
        drop(window);
        result
    }
    #[test]
    fn opaque_graph_cursor_and_last_payload_guard_share_one_position() {
        let (host, capacity) = fixture();
        let result = produce_text(&host, "hello");
        assert_eq!(result.row_count(), 1);
        let column = result.columns().next().unwrap();
        assert_eq!(column.name, "value");
        assert_eq!(column.kind, LocalColumnKind::Utf8);
        let mut cursor = result.into_cursor();
        let physical = cursor.retain_physical_guard();
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        let mut bytes = [0; 256];
        let turn = cursor.step(&mut bytes).unwrap();
        assert_eq!(turn.status, RenderTurnStatus::InputComplete);
        assert_eq!(
            &bytes[..turn.emitted_bytes],
            &[6, 0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o']
        );
        drop(cursor);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(physical);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(host.snapshot().root_responsibilities, 0);
    }
    #[test]
    fn cancellation_latches_before_first_encoder_and_after_a_counting_turn() {
        for after_count in [false, true] {
            let (host, capacity) = fixture();
            let result = produce_text(&host, &"x".repeat(128 * 1024));
            let mut cursor = result.into_cursor();
            let mut output = [0xab; 128];
            if after_count {
                let turn = cursor.step(&mut output).unwrap();
                assert!(turn.examined_bytes <= RootProfileV1::EMIT_BYTES_PER_TURN);
                assert_eq!(turn.emitted_bytes, 0);
            }
            cursor.cancel();
            for _ in 0..2 {
                assert_eq!(
                    cursor.step(&mut output).unwrap_err().kind,
                    novarocks_result_render::RenderErrorKind::Cancelled
                );
            }
            assert!(output.iter().all(|byte| *byte == 0xab));
            assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
            drop(cursor);
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        }
    }
    #[test]
    fn foreign_same_number_scope_and_client_class_refuse_before_source() {
        let (first, _) = fixture();
        let (second, _) = fixture();
        let (root, window) = first
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let other = second
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        assert_eq!(root.owner.scope().id(), other.owner.scope().id());
        assert!(LocalResultProducer::try_new(&other.owner.scope(), window.retain_alias()).is_err());
        let (client_root, client_window) = first
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Client,
            )
            .unwrap();
        assert!(
            LocalResultProducer::try_new(&client_root.owner.scope(), client_window.retain_alias())
                .is_err()
        );
        root.owner.complete();
        root.business.release();
        drop(window);
        other.owner.complete();
        other.business.release();
        client_root.owner.complete();
        client_root.business.release();
        drop(client_window);
    }
    #[test]
    fn declared_local_primitives_keep_exact_metadata_and_shared_render_bytes() {
        use super::super::{ResultField, build_arrow_query_result};
        use arrow::array::{BooleanArray, Int32Array, Int64Array, StringArray};
        let (host, capacity) = fixture();
        let (root, window) = host
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let result = LocalResultProducer::try_new(&root.owner.scope(), window.retain_alias())
            .unwrap()
            .produce(|| {
                build_arrow_query_result(
                    vec![
                        ResultField::new("active", DataType::Boolean, false, None),
                        ResultField::new("small", DataType::Int32, false, None),
                        ResultField::new("large", DataType::Int64, false, None),
                        ResultField::new("text", DataType::Utf8, true, None),
                    ],
                    vec![
                        Arc::new(BooleanArray::from(vec![true])),
                        Arc::new(Int32Array::from(vec![-2])),
                        Arc::new(Int64Array::from(vec![7])),
                        Arc::new(StringArray::from(vec![None::<&str>])),
                    ],
                )
            })
            .unwrap();
        assert_eq!(
            result
                .columns()
                .map(|column| column.kind)
                .collect::<Vec<_>>(),
            vec![
                LocalColumnKind::Boolean,
                LocalColumnKind::Int32,
                LocalColumnKind::Int64,
                LocalColumnKind::Utf8
            ]
        );
        root.owner.complete();
        root.business.release();
        drop(window);
        let mut cursor = result.into_cursor();
        let mut bytes = [0; 256];
        let turn = cursor.step(&mut bytes).unwrap();
        assert_eq!(turn.completed_rows, 1);
        assert_eq!(
            &bytes[..turn.emitted_bytes],
            &[8, 0, 0, 0, 1, b'1', 2, b'-', b'2', 1, b'7', 0xfb]
        );
        drop(cursor);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
    #[test]
    fn empty_input_batch_yields_before_the_following_batch_without_false_end() {
        let (host, _) = fixture();
        let (root, window) = host
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let result = LocalResultProducer::try_new(&root.owner.scope(), window.retain_alias())
            .unwrap()
            .produce(|| {
                let mut empty = super::super::build_string_query_result("value", vec![])?;
                let mut next = super::super::build_string_query_result("value", vec!["a".into()])?;
                empty.batches.append(&mut next.batches);
                Ok(empty)
            })
            .unwrap();
        root.owner.complete();
        root.business.release();
        drop(window);
        let mut cursor = result.into_cursor();
        let mut bytes = [0; 256];
        assert_eq!(
            cursor.step(&mut bytes).unwrap().status,
            RenderTurnStatus::Yielded
        );
        let turn = cursor.step(&mut bytes).unwrap();
        assert_eq!(turn.status, RenderTurnStatus::InputComplete);
        assert_eq!(&bytes[..turn.emitted_bytes], &[2, 0, 0, 0, 1, b'a']);
    }

    #[test]
    fn inconsistent_batch_and_unknown_local_type_fail_without_guessing() {
        use super::super::{ResultField, build_arrow_query_result};
        use arrow::array::{Int16Array, Int64Array};
        let (host, capacity) = fixture();
        for mismatch in [false, true] {
            let (root, window) = host
                .root_admission()
                .try_begin_root_with_result(
                    WorkRequest::new(WorkClass::Management),
                    ResultWindowClass::Local,
                )
                .unwrap();
            let source =
                LocalResultProducer::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
            let result = source.produce(|| {
                if mismatch {
                    let mut result = build_arrow_query_result(
                        vec![ResultField::new("value", DataType::Int64, false, None)],
                        vec![Arc::new(Int64Array::from(vec![1]))],
                    )?;
                    result.columns = vec![ResultField::new("value", DataType::Utf8, false, None)];
                    Ok(result)
                } else {
                    build_arrow_query_result(
                        vec![ResultField::new("value", DataType::Int16, false, None)],
                        vec![Arc::new(Int16Array::from(vec![1]))],
                    )
                }
            });
            assert!(result.is_err());
            root.owner.complete();
            root.business.release();
            drop(window);
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        }
    }
}
