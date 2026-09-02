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

//! Dataflow write nodes: `TableWriter` and `TableFinish`.
//!
//! NCP-6 replaces the "writer is a terminal sink" model with a Trino-style
//! dataflow. A [`TableWriterNode`] is an ordinary relational operator that
//! *emits a small relation* instead of terminating the plan:
//!
//! ```text
//! writer fragment:  child plan -> TableWriter -> stream edge (gather)
//! finish fragment:  Exchange(s) -> TableFinish -> DataSink::Result
//! ```
//!
//! Every writer emits exactly one `ROW_COUNT` row followed by zero or more
//! `COMMIT_FRAGMENT` rows, each tagged with the writer's
//! [`WriteTargetOrdinal`]. All writer fragments of one query gather into a
//! single finish fragment, whose [`TableFinishNode`] aggregates the rows and
//! emits the query result through the ordinary result sink.
//!
//! The two four-column relations themselves are **not** defined here.
//! `novarocks_spi::connector::write_stack::relation` is their single definition
//! point, because SQL, execution, backend, and frontend must all agree on them.
//! This module only attaches planner identity ([`ColumnId`]) to the SPI Arrow
//! fields; the names, types, and nullability come from SPI verbatim.

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::write_stack::{
    WRITE_RELATION_COLUMN_COUNT, WriteTargetOrdinal, root_output_schema,
    validate_query_target_ordinals, writer_output_schema,
};

use crate::analysis::OutputColumn;
use crate::column_id::ColumnId;
use crate::planner::distributed::output::ConnectorWriteOutputContract;

use super::contract::ConnectorWriteInputBinding;

/// The planner [`ColumnId`] of the write relation column at `index`.
///
/// The number itself is frozen by the write contract, not chosen here: the same
/// value is the execution slot id, because the exchange edge carrying this
/// relation is where the two must agree.
pub(crate) const fn write_relation_column_id(index: usize) -> ColumnId {
    ColumnId(novarocks_spi::connector::write_stack::write_relation_column_id(index))
}

/// The `TableWriter` output relation, as planner output columns.
///
/// Schema facts (names, types, nullability) come from
/// [`writer_output_schema`]; only the [`ColumnId`]s are planner-owned.
pub(crate) fn table_writer_output_columns() -> Vec<OutputColumn> {
    write_relation_output_columns(&writer_output_schema())
}

/// The `TableFinish` output relation, as planner output columns. Schema facts
/// come from [`root_output_schema`].
pub(crate) fn table_finish_output_columns() -> Vec<OutputColumn> {
    write_relation_output_columns(&root_output_schema())
}

/// The stream-edge `output_slot_ids` of the write relations, in field order.
/// The reserved column ids fit `i32` by construction, so this cannot overflow
/// the wire slot-id space.
pub(crate) fn write_relation_output_slot_ids() -> Vec<i32> {
    (0..WRITE_RELATION_COLUMN_COUNT)
        .map(|index| {
            i32::try_from(write_relation_column_id(index).0)
                .expect("reserved write relation column ids fit the wire slot id space")
        })
        .collect()
}

fn write_relation_output_columns(schema: &SchemaRef) -> Vec<OutputColumn> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| OutputColumn {
            column_id: write_relation_column_id(index),
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            // The write relations are engine machinery, never a user-visible
            // SQL projection.
            is_internal: true,
        })
        .collect()
}

/// A dataflow table writer.
///
/// It consumes its child's rows, hands them to the bound connector writer, and
/// emits the writer relation tagged with `write_target_ordinal`. The Arrow/SQL
/// output contract is the same [`ConnectorWriteOutputContract`] the terminal
/// write sink froze: it is pure SQL/Arrow fact and survives NCP-6 unchanged.
#[derive(Clone, Debug)]
pub struct TableWriterNode {
    pub(crate) write_target_ordinal: WriteTargetOrdinal,
    pub(crate) input: ConnectorWriteInputBinding,
    pub(crate) output_contract: ConnectorWriteOutputContract,
}

impl TableWriterNode {
    pub(crate) fn new(
        write_target_ordinal: WriteTargetOrdinal,
        input: ConnectorWriteInputBinding,
        output_contract: ConnectorWriteOutputContract,
    ) -> Self {
        Self {
            write_target_ordinal,
            input,
            output_contract,
        }
    }
}

/// The single per-query write finish operator.
///
/// It gathers the writer relation of every [`TableWriterNode`] in the query and
/// emits the root relation. `expected_target_ordinals` is the set of writer
/// ordinals it must observe; it is the plan-level record of "which logical write
/// targets *this query* feeds", not a routing table.
///
/// That set is not required to be dense from zero, and must not be: a
/// copy-on-write statement drives one query per rewritten file against a single
/// write session, and each of those queries compiles exactly one writer, at that
/// group's own ordinal. Query `k` therefore expects `[k]`. Denseness is a
/// property of the *session's* sealed target set and is enforced there, by
/// `ConnectorWriteSessionPlan::try_new`.
#[derive(Clone, Debug)]
pub struct TableFinishNode {
    pub(crate) expected_target_ordinals: Vec<WriteTargetOrdinal>,
}

impl TableFinishNode {
    /// Build a finish node over one query's expected ordinal set: non-empty,
    /// duplicate-free, and inside the frozen target bound. Cardinality and
    /// duplication are checked by the SPI owner of the ordinal vocabulary and
    /// not restated here; the ascending listing below is this encoding's own
    /// determinism rule.
    pub(crate) fn try_new(
        expected_target_ordinals: Vec<WriteTargetOrdinal>,
    ) -> Result<Self, String> {
        validate_query_target_ordinals(&expected_target_ordinals)
            .map_err(|error| format!("table finish write target ordinals rejected: {error}"))?;
        for pair in expected_target_ordinals.windows(2) {
            if pair[0].get() >= pair[1].get() {
                return Err(format!(
                    "table finish write target ordinals must be listed in strictly ascending order: found {} before {}",
                    pair[0].get(),
                    pair[1].get()
                ));
            }
        }
        Ok(Self {
            expected_target_ordinals,
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn planner_output_columns_mirror_the_spi_write_relations() {
        let writer = table_writer_output_columns();
        let finish = table_finish_output_columns();

        assert_eq!(writer.len(), WRITE_RELATION_COLUMN_COUNT);
        assert_eq!(finish.len(), WRITE_RELATION_COLUMN_COUNT);

        for (index, (column, field)) in writer
            .iter()
            .zip(writer_output_schema().fields())
            .enumerate()
        {
            assert_eq!(column.name, *field.name());
            assert_eq!(column.data_type, *field.data_type());
            assert_eq!(column.nullable, field.is_nullable());
            assert_eq!(column.column_id, write_relation_column_id(index));
            assert!(column.is_internal);
        }
        for (index, (column, field)) in finish.iter().zip(root_output_schema().fields()).enumerate()
        {
            assert_eq!(column.name, *field.name());
            assert_eq!(column.data_type, *field.data_type());
            assert_eq!(column.nullable, field.is_nullable());
            // Both relations share one planner column id per position.
            assert_eq!(column.column_id, writer[index].column_id);
        }

        // Only the writer-ordinal nullability differs between the two relations.
        assert_eq!(
            writer
                .iter()
                .map(|column| column.nullable)
                .collect::<Vec<_>>(),
            vec![false, false, true, true]
        );
        assert_eq!(
            finish
                .iter()
                .map(|column| column.nullable)
                .collect::<Vec<_>>(),
            vec![false, true, true, true]
        );
        // Signed primitives: the FE/BE native `TypeDesc` mapping names only
        // `Int8..Int64`, so an unsigned relation column could not be encoded.
        assert_eq!(
            writer
                .iter()
                .map(|column| column.data_type.clone())
                .collect::<Vec<_>>(),
            vec![
                DataType::Int8,
                DataType::Int32,
                DataType::Int64,
                DataType::Binary
            ]
        );
    }

    #[test]
    fn write_relation_column_ids_fit_the_wire_slot_id_space() {
        let slots = write_relation_output_slot_ids();
        assert_eq!(slots.len(), WRITE_RELATION_COLUMN_COUNT);
        assert!(slots.iter().all(|slot| *slot > 0));
        assert_eq!(slots.last().copied(), Some(i32::MAX));
    }

    #[test]
    fn table_finish_rejects_an_empty_repeated_or_unordered_write_target_set() {
        let ordinal = |value: u32| WriteTargetOrdinal::try_new(value).expect("bounded ordinal");
        assert!(TableFinishNode::try_new(vec![ordinal(0), ordinal(1), ordinal(2)]).is_ok());

        let error = TableFinishNode::try_new(Vec::new()).expect_err("empty ordinals");
        assert!(error.contains("rejected"), "unexpected error: {error}");

        let error =
            TableFinishNode::try_new(vec![ordinal(1), ordinal(1)]).expect_err("repeated ordinal");
        assert!(error.contains("rejected"), "unexpected error: {error}");

        let error = TableFinishNode::try_new(vec![ordinal(1), ordinal(0)])
            .expect_err("descending ordinals");
        assert!(
            error.contains("strictly ascending order"),
            "unexpected error: {error}"
        );
    }

    /// A copy-on-write statement runs one query per rewritten file against one
    /// write session, and query `k` compiles exactly one writer -- the one at
    /// ordinal `k`. Its finish node therefore sees `[k]`, which is correctly not
    /// dense from zero.
    #[test]
    fn table_finish_accepts_a_single_writer_query_at_a_non_zero_ordinal() {
        let ordinal = |value: u32| WriteTargetOrdinal::try_new(value).expect("bounded ordinal");
        let node = TableFinishNode::try_new(vec![ordinal(2)]).expect("single non-zero target");
        assert_eq!(node.expected_target_ordinals, vec![ordinal(2)]);
        // A gap between two targets is the same kind of fact.
        assert!(TableFinishNode::try_new(vec![ordinal(0), ordinal(2)]).is_ok());
    }
}
