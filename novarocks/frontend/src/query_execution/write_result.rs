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

//! Rebuilding the prepared write set from the root result relation.
//!
//! This is the frontend's half of the write data plane. The root backend
//! aggregates every writer's rows and emits them through the ordinary result
//! sink; the frontend fetches them like any other result and turns them back
//! into a complete prepared write set here.
//!
//! Two things make this different from decoding a user query result:
//!
//! * the relation is engine machinery and is never shown to a SQL client, so it
//!   is decoded by column position against the frozen write relation rather
//!   than by the statement's output columns;
//! * a set is complete or it does not exist. A prefix that arrives without an
//!   observed EOF is not "most of a write" -- it is no write at all, and the
//!   only method that can produce a complete set takes the EOF as its
//!   precondition.
//!
//! This is also a trust boundary: the same budgets the writer and the root
//! backend already charged are charged again here, because a frontend that
//! trusted a backend's arithmetic would have no way to notice a backend that
//! got it wrong.

use arrow::array::{Array, BinaryArray, Int8Array, Int32Array, Int64Array};
use novarocks_execution::exec::chunk::Chunk;
use novarocks_spi::connector::write_stack::{
    PreparedWriteSetLedger, RootRowKind, WRITE_RELATION_COLUMN_COUNT,
    WRITE_RELATION_FRAGMENT_INDEX, WRITE_RELATION_KIND_INDEX, WRITE_RELATION_ROW_COUNT_INDEX,
    WRITE_RELATION_TARGET_INDEX, WriteRowCountAccumulator, WriteTargetOrdinal, row_count_from_wire,
    target_ordinal_from_wire, validate_root_row,
};

/// A complete prepared write set, still in canonical carrier form.
///
/// The fragments stay encoded here on purpose: turning them into provider
/// values requires the exact control binding that produced them, and this type
/// exists to be handed to it.
#[derive(Debug)]
pub(crate) struct DecodedPreparedWriteSet {
    row_count: u64,
    fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
}

impl DecodedPreparedWriteSet {
    pub(crate) const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(crate) fn fragments(&self) -> &[(WriteTargetOrdinal, Vec<u8>)] {
        &self.fragments
    }

    pub(crate) fn into_fragments(self) -> Vec<(WriteTargetOrdinal, Vec<u8>)> {
        self.fragments
    }
}

/// Accumulates the root result relation across fetched batches.
pub(crate) struct RootWriteResultDecoder {
    highest_expected_ordinal: u32,
    rows: WriteRowCountAccumulator,
    ledger: PreparedWriteSetLedger,
    fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
    summary: Option<u64>,
}

impl RootWriteResultDecoder {
    /// `expected_targets` is the sealed logical target set from the begin
    /// session. It is dense from zero, so membership is one comparison.
    pub(crate) fn new(expected_targets: &[WriteTargetOrdinal]) -> Result<Self, String> {
        novarocks_spi::connector::write_stack::validate_dense_target_ordinals(expected_targets)
            .map_err(|error| format!("connector write session target set: {error}"))?;
        Ok(Self {
            highest_expected_ordinal: expected_targets
                .iter()
                .map(|target| target.get())
                .max()
                .unwrap_or_default(),
            rows: WriteRowCountAccumulator::new(),
            ledger: PreparedWriteSetLedger::new(),
            fragments: Vec::new(),
            summary: None,
        })
    }

    pub(crate) fn apply_chunk(&mut self, chunk: &Chunk) -> Result<(), String> {
        let columns = chunk.columns();
        if columns.len() != WRITE_RELATION_COLUMN_COUNT {
            return Err(format!(
                "root write result must have {WRITE_RELATION_COLUMN_COUNT} columns, found {}",
                columns.len()
            ));
        }
        let kinds = downcast::<Int8Array>(&columns[WRITE_RELATION_KIND_INDEX], "kind")?;
        let targets = downcast::<Int32Array>(&columns[WRITE_RELATION_TARGET_INDEX], "target")?;
        let counts = downcast::<Int64Array>(&columns[WRITE_RELATION_ROW_COUNT_INDEX], "row count")?;
        let payloads =
            downcast::<BinaryArray>(&columns[WRITE_RELATION_FRAGMENT_INDEX], "commit fragment")?;

        for row in 0..chunk.len() {
            if kinds.is_null(row) {
                return Err("root write result row has no kind".to_string());
            }
            let kind = RootRowKind::from_wire(kinds.value(row))
                .map_err(|error| format!("root write result row kind: {error}"))?;
            let target = (!targets.is_null(row)).then(|| targets.value(row));
            let count = (!counts.is_null(row)).then(|| counts.value(row));
            let payload = (!payloads.is_null(row)).then(|| payloads.value(row));
            validate_root_row(kind, target, count, payload.map(<[u8]>::len))
                .map_err(|error| format!("root write result row shape: {error}"))?;

            match kind {
                RootRowKind::Summary => {
                    // Exactly one summary describes the whole write. A second
                    // one would mean two roots aggregated, and picking either
                    // would report a row count that never happened.
                    if self.summary.is_some() {
                        return Err(
                            "root write result carries more than one summary row".to_string()
                        );
                    }
                    let count = count.expect("validated above");
                    let count = row_count_from_wire(count)
                        .map_err(|error| format!("root write result row count: {error}"))?;
                    self.rows
                        .add(count)
                        .map_err(|error| format!("root write result row count: {error}"))?;
                    self.summary = Some(count);
                }
                RootRowKind::PreparedFragment => {
                    let target = target_ordinal_from_wire(target.expect("validated above"))
                        .map_err(|error| format!("root write result target ordinal: {error}"))?;
                    if target.get() > self.highest_expected_ordinal {
                        return Err(format!(
                            "root write result names write target {} outside the sealed set",
                            target.get()
                        ));
                    }
                    let payload = payload.expect("validated above");
                    self.ledger
                        .reserve_fragment(payload.len())
                        .map_err(|error| format!("prepared write set budget: {error}"))?;
                    self.fragments.push((target, payload.to_vec()));
                }
            }
        }
        Ok(())
    }

    /// Freeze the set. The caller must have observed `FetchOutcome::Eof`; a
    /// prefix, a timeout, an abort, or a decode failure never reaches here,
    /// because none of them proves the root finished emitting.
    pub(crate) fn finish_at_eof(self) -> Result<DecodedPreparedWriteSet, String> {
        let row_count = self.summary.ok_or_else(|| {
            "root write result reached end of stream without a summary row".to_string()
        })?;
        debug_assert_eq!(row_count, self.rows.get());
        Ok(DecodedPreparedWriteSet {
            row_count,
            fragments: self.fragments,
        })
    }
}

fn downcast<'a, T: 'static>(
    column: &'a arrow::array::ArrayRef,
    label: &str,
) -> Result<&'a T, String> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("root write result {label} column has the wrong Arrow type"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BinaryArray, Int8Array, Int32Array, Int64Array};
    use novarocks_execution::exec::node::table_write_relation::root_relation_chunk_schema;

    use super::*;

    struct Row {
        kind: i8,
        target: Option<i32>,
        row_count: Option<i64>,
        fragment: Option<Vec<u8>>,
    }

    fn summary(row_count: i64) -> Row {
        Row {
            kind: RootRowKind::SUMMARY,
            target: None,
            row_count: Some(row_count),
            fragment: None,
        }
    }

    fn fragment(target: i32, bytes: usize) -> Row {
        Row {
            kind: RootRowKind::PREPARED_FRAGMENT,
            target: Some(target),
            row_count: None,
            fragment: Some(vec![7_u8; bytes]),
        }
    }

    fn chunk(rows: Vec<Row>) -> Chunk {
        let kinds: ArrayRef = Arc::new(Int8Array::from(
            rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
        ));
        let targets: ArrayRef = Arc::new(Int32Array::from(
            rows.iter().map(|row| row.target).collect::<Vec<_>>(),
        ));
        let counts: ArrayRef = Arc::new(Int64Array::from(
            rows.iter().map(|row| row.row_count).collect::<Vec<_>>(),
        ));
        let fragments: ArrayRef = Arc::new(BinaryArray::from(
            rows.iter()
                .map(|row| row.fragment.as_deref())
                .collect::<Vec<_>>(),
        ));
        Chunk::try_new_with_columns(
            root_relation_chunk_schema(),
            vec![kinds, targets, counts, fragments],
        )
        .expect("root relation chunk")
    }

    fn targets(count: u32) -> Vec<WriteTargetOrdinal> {
        (0..count)
            .map(|ordinal| WriteTargetOrdinal::try_new(ordinal).expect("bounded ordinal"))
            .collect()
    }

    fn new_decoder(count: u32) -> RootWriteResultDecoder {
        RootWriteResultDecoder::new(&targets(count)).expect("dense target set")
    }

    #[test]
    fn a_complete_set_spans_several_batches_and_keeps_its_fragments() {
        let mut decoder = new_decoder(2);
        decoder
            .apply_chunk(&chunk(vec![summary(42), fragment(0, 8)]))
            .expect("first batch");
        decoder
            .apply_chunk(&chunk(vec![fragment(1, 16), fragment(0, 4)]))
            .expect("second batch");
        let set = decoder.finish_at_eof().expect("complete set");
        assert_eq!(set.row_count(), 42);
        assert_eq!(set.fragments().len(), 3);
        assert_eq!(set.fragments()[0].0.get(), 0);
        assert_eq!(set.fragments()[1].0.get(), 1);
        assert_eq!(set.fragments()[0].1.len(), 8);
    }

    #[test]
    fn a_write_that_staged_nothing_is_still_a_complete_set() {
        let mut decoder = new_decoder(1);
        decoder
            .apply_chunk(&chunk(vec![summary(0)]))
            .expect("batch");
        let set = decoder.finish_at_eof().expect("complete set");
        assert_eq!(set.row_count(), 0);
        assert!(set.fragments().is_empty());
    }

    #[test]
    fn a_prefix_without_a_summary_is_not_a_partial_write_but_no_write() {
        let mut decoder = new_decoder(1);
        decoder
            .apply_chunk(&chunk(vec![fragment(0, 8)]))
            .expect("batch");
        let error = decoder.finish_at_eof().expect_err("no summary");
        assert!(
            error.contains("without a summary row"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn two_summary_rows_would_mean_two_roots_and_are_refused() {
        let mut decoder = new_decoder(1);
        let error = decoder
            .apply_chunk(&chunk(vec![summary(1), summary(2)]))
            .expect_err("two summaries");
        assert!(
            error.contains("more than one summary"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_row_whose_payload_contradicts_its_kind_is_refused() {
        // A summary that also claims a target ordinal.
        let mut decoder = new_decoder(1);
        assert!(
            decoder
                .apply_chunk(&chunk(vec![Row {
                    kind: RootRowKind::SUMMARY,
                    target: Some(0),
                    row_count: Some(1),
                    fragment: None,
                }]))
                .is_err()
        );

        // A fragment row with no fragment.
        let mut decoder = new_decoder(1);
        assert!(
            decoder
                .apply_chunk(&chunk(vec![Row {
                    kind: RootRowKind::PREPARED_FRAGMENT,
                    target: Some(0),
                    row_count: None,
                    fragment: None,
                }]))
                .is_err()
        );

        // An unknown kind.
        let mut decoder = new_decoder(1);
        assert!(
            decoder
                .apply_chunk(&chunk(vec![Row {
                    kind: 9,
                    target: None,
                    row_count: Some(1),
                    fragment: None,
                }]))
                .is_err()
        );
    }

    #[test]
    fn a_fragment_naming_a_target_outside_the_sealed_set_is_refused() {
        let mut decoder = new_decoder(1);
        let error = decoder
            .apply_chunk(&chunk(vec![fragment(1, 4)]))
            .expect_err("foreign target");
        assert!(
            error.contains("outside the sealed set"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_negative_row_count_is_corrupt_rather_than_a_huge_unsigned_one() {
        let mut decoder = new_decoder(1);
        let error = decoder
            .apply_chunk(&chunk(vec![summary(-1)]))
            .expect_err("negative row count");
        assert!(error.contains("negative"), "unexpected error: {error}");
    }

    #[test]
    fn the_frontend_recharges_the_budgets_the_backend_already_charged() {
        use novarocks_spi::connector::write_stack::MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES;

        let mut decoder = new_decoder(1);
        decoder
            .apply_chunk(&chunk(vec![fragment(
                0,
                MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES,
            )]))
            .expect("exactly at the single-fragment bound");

        let mut decoder = new_decoder(1);
        let error = decoder
            .apply_chunk(&chunk(vec![fragment(
                0,
                MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1,
            )]))
            .expect_err("over the single-fragment bound");
        assert!(error.contains("budget"), "unexpected error: {error}");
    }
}
