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

//! The `TableFinish` operator: bounded aggregation at DOP 1.
//!
//! One driver on one Root BE consumes every writer row that arrives through the
//! gather Exchange. It validates each row's shape against the frozen relation,
//! checks the target ordinal against the sealed set, hands the canonical bytes
//! to the validator port for a structural check, and charges both the frozen
//! per-fragment and prepared-write-set budgets. Exceeding a budget is a typed
//! rejection, never a truncation.
//!
//! Nothing is emitted before every sender reaches EOS: a prepared write set is
//! complete or it does not exist. On abort the buffered fragments are released
//! immediately rather than held until the driver is dropped.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryBuilder, Int8Array, Int32Array, Int64Array};
use arrow::record_batch::RecordBatch;

use novarocks_spi::connector::write_stack::{
    PreparedWriteSetLedger, RootRowKind, WriteRowCountAccumulator, WriteTargetOrdinal,
    WriterRowKind, row_count_from_wire, row_count_to_wire, target_ordinal_from_wire,
    target_ordinal_to_wire, validate_root_row, validate_writer_row,
};

use crate::exec::chunk::Chunk;
use crate::exec::node::table_finish::TableFinishNode;
use crate::exec::node::table_write_relation::{
    ConnectorCommitFragmentCarrierValidator, root_relation_chunk_schema, root_relation_schema,
};
use crate::exec::operators::table_writer::TableWriteRelationColumns;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for the single-driver table finish operator.
pub struct TableFinishOperatorFactory {
    name: String,
    expected_targets: Arc<Vec<WriteTargetOrdinal>>,
    fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
}

impl TableFinishOperatorFactory {
    pub fn new(node: &TableFinishNode) -> Self {
        let name = if node.node_id >= 0 {
            format!("TABLE_FINISH (id={})", node.node_id)
        } else {
            "TABLE_FINISH".to_string()
        };
        Self {
            name,
            expected_targets: Arc::clone(node.expected_targets()),
            fragment_validator: Arc::clone(node.fragment_validator()),
        }
    }
}

impl OperatorFactory for TableFinishOperatorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        // The builder creates this factory's pipeline at DOP 1, converging every
        // writer input into it first, so any other degree of parallelism means
        // the plan and the pipeline disagree about who owns the complete
        // prepared write set. A second driver would each see a partial set and
        // each believe it was complete, so fail closed at `prepare` rather than
        // silently aggregating a partial set per driver.
        let parallelism_error = (dop.max(1) != 1 || driver_id != 0).then(|| {
            format!(
                "table finish must run at DOP 1 on a single driver, but was created with dop={dop} driver_id={driver_id}"
            )
        });
        Box::new(TableFinishOperator {
            name: self.name.clone(),
            expected_targets: Arc::clone(&self.expected_targets),
            fragment_validator: Arc::clone(&self.fragment_validator),
            parallelism_error,
            rows: WriteRowCountAccumulator::new(),
            ledger: PreparedWriteSetLedger::new(),
            fragments: Vec::new(),
            pending_output: None,
            finishing: false,
            finished: false,
        })
    }
}

struct TableFinishOperator {
    name: String,
    expected_targets: Arc<Vec<WriteTargetOrdinal>>,
    fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    parallelism_error: Option<String>,
    rows: WriteRowCountAccumulator,
    ledger: PreparedWriteSetLedger,
    fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl TableFinishOperator {
    fn release_buffer(&mut self) {
        self.fragments = Vec::new();
        self.pending_output = None;
    }

    /// Read one target ordinal off the signed carrier. A negative value is
    /// corrupt data, never a very large unsigned ordinal.
    fn target_in_sealed_set(&self, raw: i32) -> Result<WriteTargetOrdinal, String> {
        let target = target_ordinal_from_wire(raw)
            .map_err(|error| format!("table finish write target ordinal: {error}"))?;
        // An exact set test, not a bound: the query's expected set need not be
        // dense from zero, so "at or below the highest ordinal" would admit a
        // target this query compiled no writer for.
        if !self.expected_targets.contains(&target) {
            return Err(format!(
                "table finish received a write target ordinal {raw} outside the sealed set of {} targets",
                self.expected_targets.len()
            ));
        }
        Ok(target)
    }

    fn accept_row(
        &mut self,
        columns: &TableWriteRelationColumns<'_>,
        row: usize,
    ) -> Result<(), String> {
        if columns.kinds.is_null(row) {
            return Err("table finish received a writer row with a null kind".to_string());
        }
        let kind = WriterRowKind::from_wire(columns.kinds.value(row)).map_err(|error| {
            format!(
                "table finish writer row kind {}: {error}",
                columns.kinds.value(row)
            )
        })?;
        let row_count = (!columns.row_counts.is_null(row)).then(|| columns.row_counts.value(row));
        let fragment_len =
            (!columns.fragments.is_null(row)).then(|| columns.fragments.value(row).len());
        validate_writer_row(kind, row_count, fragment_len)
            .map_err(|error| format!("table finish writer row shape: {error}"))?;

        if columns.ordinals.is_null(row) {
            return Err(
                "table finish received a writer row with a null write target ordinal".to_string(),
            );
        }
        let target = self.target_in_sealed_set(columns.ordinals.value(row))?;

        match kind {
            WriterRowKind::RowCount => {
                let rows = row_count.ok_or_else(|| {
                    "table finish ROW_COUNT row lost its validated row count".to_string()
                })?;
                // A negative row count is corrupt data, never a huge unsigned
                // one: it would become the statement's affected row count.
                let rows = row_count_from_wire(rows)
                    .map_err(|error| format!("table finish row count: {error}"))?;
                self.rows
                    .add(rows)
                    .map_err(|error| format!("table finish row count: {error}"))
            }
            WriterRowKind::CommitFragment => {
                let encoded = columns.fragments.value(row);
                self.fragment_validator
                    .validate(target, encoded)
                    .map_err(|error| format!("table finish commit fragment carrier: {error}"))?;
                self.ledger
                    .reserve_fragment(encoded.len())
                    .map_err(|error| format!("table finish prepared write set: {error}"))?;
                self.fragments.push((target, encoded.to_vec()));
                Ok(())
            }
        }
    }

    fn build_output(&mut self) -> Result<Chunk, String> {
        let fragments = std::mem::take(&mut self.fragments);
        let rows = fragments.len() + 1;
        let mut kinds = Vec::with_capacity(rows);
        let mut ordinals: Vec<Option<i32>> = Vec::with_capacity(rows);
        let mut row_counts: Vec<Option<i64>> = Vec::with_capacity(rows);
        let mut payloads = BinaryBuilder::new();

        // Accumulation stays checked and unsigned; the narrowing to the
        // relation's signed carrier happens once, here, and errors rather than
        // wrapping.
        let summary_rows = row_count_to_wire(self.rows.get())
            .map_err(|error| format!("table finish summary row count: {error}"))?;
        validate_root_row(RootRowKind::Summary, None, Some(summary_rows), None)
            .map_err(|error| format!("table finish summary row shape: {error}"))?;
        kinds.push(RootRowKind::Summary.to_wire());
        ordinals.push(None);
        row_counts.push(Some(summary_rows));
        payloads.append_null();

        for (target, encoded) in &fragments {
            let target = target_ordinal_to_wire(*target)
                .map_err(|error| format!("table finish target ordinal: {error}"))?;
            validate_root_row(
                RootRowKind::PreparedFragment,
                Some(target),
                None,
                Some(encoded.len()),
            )
            .map_err(|error| format!("table finish prepared fragment row shape: {error}"))?;
            kinds.push(RootRowKind::PreparedFragment.to_wire());
            ordinals.push(Some(target));
            row_counts.push(None);
            payloads.append_value(encoded);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(kinds)),
            Arc::new(Int32Array::from(ordinals)),
            Arc::new(Int64Array::from(row_counts)),
            Arc::new(payloads.finish()) as ArrayRef,
        ];
        let batch = RecordBatch::try_new(root_relation_schema(), columns)
            .map_err(|error| format!("build table finish output batch: {error}"))?;
        Chunk::try_new_with_chunk_schema(batch, root_relation_chunk_schema())
    }
}

impl Operator for TableFinishOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn prepare(&mut self) -> Result<(), String> {
        match self.parallelism_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn cancel(&mut self) {
        self.release_buffer();
        self.finishing = true;
        self.finished = true;
    }

    fn on_driver_failure(&mut self) {
        self.cancel();
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for TableFinishOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished
    }

    /// Nothing is available before every sender reached EOS.
    fn has_output(&self) -> bool {
        self.pending_output.is_some()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }
        if self.finishing {
            return Err("table finish received a writer row after EOS".to_string());
        }
        let columns = TableWriteRelationColumns::try_from_chunk(&chunk)?;
        for row in 0..chunk.len() {
            if let Err(error) = self.accept_row(&columns, row) {
                self.release_buffer();
                return Err(error);
            }
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if self.pending_output.is_none() && self.finishing {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finishing {
            return Ok(());
        }
        let output = match self.build_output() {
            Ok(output) => output,
            Err(error) => {
                self.release_buffer();
                return Err(error);
            }
        };
        self.pending_output = Some(output);
        self.finishing = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::BinaryArray;
    use novarocks_spi::connector::write_stack::{
        MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES,
        MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES,
    };
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

    use super::*;
    use crate::exec::node::ExecNode;
    use crate::exec::node::table_write_relation::{
        root_relation_chunk_schema, writer_relation_chunk_schema, writer_relation_schema,
    };
    use crate::exec::node::values::ValuesNode;
    use crate::exec::operators::table_writer::tests::target;

    /// A row shape the tests can express, including shapes a correct
    /// `TableWriter` would never produce.
    type RawRow = (i8, Option<i32>, Option<i64>, Option<Vec<u8>>);

    #[derive(Default)]
    struct AcceptEveryCarrier {
        calls: AtomicUsize,
    }

    impl ConnectorCommitFragmentCarrierValidator for AcceptEveryCarrier {
        fn validate(
            &self,
            _target: WriteTargetOrdinal,
            _encoded: &[u8],
        ) -> Result<(), ConnectorError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct RejectEveryCarrier;

    impl ConnectorCommitFragmentCarrierValidator for RejectEveryCarrier {
        fn validate(
            &self,
            _target: WriteTargetOrdinal,
            _encoded: &[u8],
        ) -> Result<(), ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "not a canonical carrier of the expected provider",
            ))
        }
    }

    /// One writer input. The finish node is n-ary, so tests build a `Vec`; the
    /// operator's behaviour does not depend on how many inputs converge, only
    /// on the rows that arrive.
    fn values_input() -> Vec<ExecNode> {
        vec![ExecNode {
            kind: crate::exec::node::ExecNodeKind::Values(ValuesNode {
                chunk: Chunk::default(),
                node_id: 1,
            }),
        }]
    }

    fn finish_node(
        targets: usize,
        validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    ) -> TableFinishNode {
        let expected = (0..targets)
            .map(|value| target(u32::try_from(value).expect("bounded")))
            .collect();
        TableFinishNode::try_new(values_input(), 3, expected, validator).expect("table finish node")
    }

    fn factory(targets: usize) -> TableFinishOperatorFactory {
        TableFinishOperatorFactory::new(&finish_node(
            targets,
            Arc::new(AcceptEveryCarrier::default()),
        ))
    }

    fn writer_rows(rows: Vec<RawRow>) -> Chunk {
        let mut kinds = Vec::with_capacity(rows.len());
        let mut ordinals = Vec::with_capacity(rows.len());
        let mut row_counts = Vec::with_capacity(rows.len());
        let mut payloads = BinaryBuilder::new();
        for (kind, ordinal, row_count, payload) in rows {
            kinds.push(kind);
            ordinals.push(ordinal);
            row_counts.push(row_count);
            match payload {
                Some(bytes) => payloads.append_value(&bytes),
                None => payloads.append_null(),
            }
        }
        // The writer relation declares a non-null ordinal, so a deliberately
        // null ordinal has to travel in the root relation's nullable field.
        let has_null_ordinal = ordinals.iter().any(Option::is_none);
        let (schema, chunk_schema) = if has_null_ordinal {
            (root_relation_schema(), root_relation_chunk_schema())
        } else {
            (writer_relation_schema(), writer_relation_chunk_schema())
        };
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(kinds)),
            Arc::new(Int32Array::from(ordinals)),
            Arc::new(Int64Array::from(row_counts)),
            Arc::new(payloads.finish()) as ArrayRef,
        ];
        let batch = RecordBatch::try_new(schema, columns).expect("writer relation batch");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("writer relation chunk")
    }

    fn row_count_row(ordinal: i32, rows: i64) -> RawRow {
        (
            WriterRowKind::RowCount.to_wire(),
            Some(ordinal),
            Some(rows),
            None,
        )
    }

    fn fragment_row(ordinal: i32, bytes: Vec<u8>) -> RawRow {
        (
            WriterRowKind::CommitFragment.to_wire(),
            Some(ordinal),
            None,
            Some(bytes),
        )
    }

    fn finish_columns(chunk: &Chunk) -> (Int8Array, Int32Array, Int64Array, BinaryArray) {
        let kinds = chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("kind")
            .clone();
        let ordinals = chunk.columns()[1]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ordinal")
            .clone();
        let row_counts = chunk.columns()[2]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row count")
            .clone();
        let fragments = chunk.columns()[3]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("fragment")
            .clone();
        (kinds, ordinals, row_counts, fragments)
    }

    fn run(operator: &mut Box<dyn Operator>, chunks: Vec<Chunk>) -> Result<Option<Chunk>, String> {
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        for chunk in chunks {
            processor.push_chunk(&state, chunk)?;
        }
        assert!(
            !processor.has_output(),
            "table finish must emit nothing before EOS"
        );
        processor.set_finishing(&state)?;
        processor.pull_chunk(&state)
    }

    #[test]
    fn table_finish_runs_at_dop_one_and_rejects_any_other_parallelism() {
        let factory = factory(1);
        let mut single = factory.create(1, 0);
        assert!(single.prepare().is_ok());

        let mut wide = factory.create(4, 0);
        let error = wide.prepare().expect_err("DOP > 1 must fail closed");
        assert!(error.contains("must run at DOP 1"));

        let mut second_driver = factory.create(1, 1);
        let error = second_driver
            .prepare()
            .expect_err("a second driver must fail closed");
        assert!(error.contains("must run at DOP 1"));
    }

    #[test]
    fn table_finish_checked_sums_row_counts_from_many_senders_in_any_order() {
        let factory = factory(2);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let output = run(
            &mut operator,
            vec![
                writer_rows(vec![
                    fragment_row(1, b"second-target".to_vec()),
                    row_count_row(1, 7),
                ]),
                writer_rows(vec![row_count_row(0, 5)]),
                writer_rows(vec![
                    row_count_row(0, 0),
                    fragment_row(0, b"first-target".to_vec()),
                ]),
            ],
        )
        .expect("finish")
        .expect("output chunk");

        assert_eq!(output.len(), 3);
        let (kinds, ordinals, row_counts, fragments) = finish_columns(&output);
        assert_eq!(kinds.value(0), RootRowKind::Summary.to_wire());
        assert!(ordinals.is_null(0));
        assert_eq!(row_counts.value(0), 12);
        assert!(fragments.is_null(0));

        let mut prepared = Vec::new();
        for row in 1..output.len() {
            assert_eq!(kinds.value(row), RootRowKind::PreparedFragment.to_wire());
            assert!(row_counts.is_null(row));
            prepared.push((ordinals.value(row), fragments.value(row).to_vec()));
        }
        prepared.sort_unstable();
        assert_eq!(
            prepared,
            vec![
                (0, b"first-target".to_vec()),
                (1, b"second-target".to_vec())
            ]
        );
    }

    #[test]
    fn table_finish_emits_only_a_summary_when_no_writer_staged_anything() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let output = run(&mut operator, vec![writer_rows(vec![row_count_row(0, 0)])])
            .expect("finish")
            .expect("output chunk");
        assert_eq!(output.len(), 1);
        let (kinds, _, row_counts, _) = finish_columns(&output);
        assert_eq!(kinds.value(0), RootRowKind::Summary.to_wire());
        assert_eq!(row_counts.value(0), 0);
    }

    #[test]
    fn table_finish_rejects_inconsistent_null_and_kind_combinations() {
        let payload_mismatch = "payload does not match its row kind";
        let cases: Vec<(Vec<RawRow>, &str)> = vec![
            // ROW_COUNT with no row count
            (
                vec![(WriterRowKind::RowCount.to_wire(), Some(0), None, None)],
                payload_mismatch,
            ),
            // ROW_COUNT carrying a commit fragment
            (
                vec![(
                    WriterRowKind::RowCount.to_wire(),
                    Some(0),
                    Some(1),
                    Some(b"x".to_vec()),
                )],
                payload_mismatch,
            ),
            // COMMIT_FRAGMENT carrying a row count
            (
                vec![(
                    WriterRowKind::CommitFragment.to_wire(),
                    Some(0),
                    Some(1),
                    Some(b"x".to_vec()),
                )],
                payload_mismatch,
            ),
            // COMMIT_FRAGMENT with no fragment
            (
                vec![(WriterRowKind::CommitFragment.to_wire(), Some(0), None, None)],
                payload_mismatch,
            ),
            // both payloads null
            (
                vec![(WriterRowKind::CommitFragment.to_wire(), Some(0), None, None)],
                payload_mismatch,
            ),
            (
                vec![(0, Some(0), Some(1), None)],
                "unknown connector writer row kind",
            ),
            (
                vec![(3, Some(0), Some(1), None)],
                "unknown connector writer row kind",
            ),
            (
                vec![(WriterRowKind::RowCount.to_wire(), None, Some(1), None)],
                "null write target ordinal",
            ),
        ];

        for (rows, expected) in cases {
            let factory = factory(1);
            let mut operator = factory.create(1, 0);
            operator.prepare().expect("prepare");
            let state = RuntimeState::default();
            let error = operator
                .as_processor_mut()
                .expect("processor")
                .push_chunk(&state, writer_rows(rows))
                .expect_err("invalid row shape");
            assert!(
                error.contains(expected),
                "expected {expected:?} in error {error:?}"
            );
        }
    }

    #[test]
    fn table_finish_rejects_a_target_ordinal_outside_the_sealed_set() {
        let factory = factory(2);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, writer_rows(vec![row_count_row(2, 1)]))
            .expect_err("ordinal outside the sealed set");
        assert!(error.contains("outside the sealed set of 2 targets"));
    }

    #[test]
    fn table_finish_rejects_a_non_canonical_carrier_before_it_enters_the_set() {
        let node = finish_node(1, Arc::new(RejectEveryCarrier));
        let factory = TableFinishOperatorFactory::new(&node);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, writer_rows(vec![fragment_row(0, b"junk".to_vec())]))
            .expect_err("foreign carrier");
        assert!(error.contains("not a canonical carrier of the expected provider"));
    }

    #[test]
    fn table_finish_row_count_overflow_is_an_error_not_a_saturating_counter() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        // The accumulator stays unsigned and checked, so two maximal signed
        // row counts fit and the third must fail rather than wrap.
        for _ in 0..2 {
            processor
                .push_chunk(&state, writer_rows(vec![row_count_row(0, i64::MAX)]))
                .expect("within the unsigned accumulator");
        }
        let error = processor
            .push_chunk(&state, writer_rows(vec![row_count_row(0, i64::MAX)]))
            .expect_err("row count overflow");
        assert!(error.contains("row count overflowed"));
    }

    #[test]
    fn table_finish_rejects_a_negative_row_count_as_corrupt_data() {
        // The relation's carrier is signed, so a corrupt negative value is
        // newly expressible. It must be rejected, never reinterpreted as a
        // huge unsigned row count the client would see as truth.
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, writer_rows(vec![row_count_row(0, -1)]))
            .expect_err("a negative row count is corrupt data");
        assert!(
            error.contains("row count is negative"),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn table_finish_rejects_a_negative_target_ordinal_as_corrupt_data() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, writer_rows(vec![row_count_row(-1, 1)]))
            .expect_err("a negative target ordinal is corrupt data");
        assert!(
            error.contains("target ordinal is negative"),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn table_finish_accepts_the_exact_single_fragment_limit_and_rejects_one_more() {
        let state = RuntimeState::default();
        let at_limit = factory(1);
        let mut operator = at_limit.create(1, 0);
        operator.prepare().expect("prepare");
        operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(
                &state,
                writer_rows(vec![fragment_row(
                    0,
                    vec![7; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES],
                )]),
            )
            .expect("the exact single-fragment budget is legal");

        let over_limit = factory(1);
        let mut operator = over_limit.create(1, 0);
        operator.prepare().expect("prepare");
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(
                &state,
                writer_rows(vec![fragment_row(
                    0,
                    vec![7; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1],
                )]),
            )
            .expect_err("over the single-fragment budget");
        assert!(error.contains("exceeds the frozen single-fragment budget"));
    }

    #[test]
    fn table_finish_rejects_the_prepared_write_set_byte_budget() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        let full = MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES / MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES;
        for _ in 0..full {
            processor
                .push_chunk(
                    &state,
                    writer_rows(vec![fragment_row(
                        0,
                        vec![1; MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES],
                    )]),
                )
                .expect("within the set byte budget");
        }
        let error = processor
            .push_chunk(&state, writer_rows(vec![fragment_row(0, vec![1])]))
            .expect_err("over the set byte budget");
        assert!(error.contains("exceeds the frozen byte budget"));
    }

    #[test]
    fn table_finish_rejects_the_prepared_write_set_entry_budget() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        let rows = (0..MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES)
            .map(|_| fragment_row(0, vec![1]))
            .collect::<Vec<_>>();
        processor
            .push_chunk(&state, writer_rows(rows))
            .expect("the exact entry budget is legal");
        let error = processor
            .push_chunk(&state, writer_rows(vec![fragment_row(0, vec![1])]))
            .expect_err("over the entry budget");
        assert!(error.contains("exceeds the frozen entry budget"));
    }

    #[test]
    fn table_finish_emits_nothing_before_eos_and_releases_its_buffer_on_abort() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        {
            let processor = operator.as_processor_mut().expect("processor");
            processor
                .push_chunk(
                    &state,
                    writer_rows(vec![
                        row_count_row(0, 11),
                        fragment_row(0, vec![3; 4096]),
                        fragment_row(0, vec![4; 4096]),
                    ]),
                )
                .expect("accumulate");
            assert!(!processor.has_output());
            assert!(
                processor
                    .pull_chunk(&state)
                    .expect("pull before EOS")
                    .is_none()
            );
        }

        operator.cancel();
        let processor = operator.as_processor_mut().expect("processor");
        assert!(!processor.has_output());
        assert!(
            processor
                .pull_chunk(&state)
                .expect("pull after abort")
                .is_none()
        );
        assert!(operator.is_finished());
    }

    #[test]
    fn table_finish_drops_its_buffer_when_a_row_is_rejected() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        processor
            .push_chunk(&state, writer_rows(vec![fragment_row(0, vec![1; 1024])]))
            .expect("first fragment");
        let error = processor
            .push_chunk(
                &state,
                writer_rows(vec![(
                    WriterRowKind::CommitFragment.to_wire(),
                    Some(0),
                    Some(1),
                    Some(vec![2; 1024]),
                )]),
            )
            .expect_err("invalid row");
        assert!(error.contains("payload does not match its row kind"));
        processor.set_finishing(&state).expect("finish");
        let output = processor
            .pull_chunk(&state)
            .expect("finish after rejection")
            .expect("output chunk");
        // The buffer was released, so only the summary row survives.
        assert_eq!(output.len(), 1);
    }

    #[test]
    fn table_finish_rejects_a_writer_row_that_arrives_after_eos() {
        let factory = factory(1);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        processor.set_finishing(&state).expect("finish");
        let error = processor
            .push_chunk(&state, writer_rows(vec![row_count_row(0, 1)]))
            .expect_err("a row after EOS is a contract violation");
        assert!(error.contains("after EOS"));
    }

    #[test]
    fn table_finish_node_requires_a_non_empty_duplicate_free_target_set() {
        assert!(
            TableFinishNode::try_new(
                values_input(),
                3,
                Vec::new(),
                Arc::new(AcceptEveryCarrier::default()),
            )
            .is_err()
        );
        assert!(
            TableFinishNode::try_new(
                values_input(),
                3,
                vec![target(1), target(1)],
                Arc::new(AcceptEveryCarrier::default()),
            )
            .is_err()
        );
    }

    /// A copy-on-write statement drives one query per rewritten file against a
    /// single write session, and each of those queries compiles exactly one
    /// writer -- the one at that group's own ordinal. Query `k` therefore
    /// expects `[k]`, which is correctly not dense from zero. Denseness belongs
    /// to the session's sealed target set, not to any one query.
    #[test]
    fn table_finish_accepts_a_single_writer_query_at_a_non_zero_ordinal() {
        let node = TableFinishNode::try_new(
            values_input(),
            3,
            vec![target(2)],
            Arc::new(AcceptEveryCarrier::default()),
        )
        .expect("a single-writer query at ordinal 2");
        assert!(node.accepts_target(target(2)));
        // Membership stays exact: a bound check would have admitted every
        // ordinal below the one this query actually feeds.
        assert!(!node.accepts_target(target(0)));
        assert!(!node.accepts_target(target(1)));

        let factory = TableFinishOperatorFactory::new(&node);
        let mut operator = factory.create(1, 0);
        operator.prepare().expect("prepare");
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("processor");
        processor
            .push_chunk(&state, writer_rows(vec![row_count_row(2, 5)]))
            .expect("its own writer's rows are accepted");
        let error = processor
            .push_chunk(&state, writer_rows(vec![row_count_row(0, 1)]))
            .expect_err("a target this query never compiled a writer for");
        assert!(error.contains("outside the sealed set"), "{error}");
    }
}
