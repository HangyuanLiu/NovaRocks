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

//! Input-owned inner-join match enumeration. No output queue or borrowed turn
//! capability survives a step. Lookup is once per input; gather is once per page.
use super::*;
use crate::exec::operators::hashjoin::join_hash_map::gather::{
    MAX_JOIN_OUTPUT_ROWS_PER_BATCH, gather_probe_build_batches,
};

pub(super) struct InnerProbeCursor {
    input: Chunk,
    groups: Option<JoinPairCursor>,
    probe_position: usize,
    build_position: usize,
}
impl HashJoinProbeCore {
    pub(super) fn start_inner(
        &mut self,
        ctx: &StepContext<'_>,
        input: Chunk,
    ) -> ExecutionResult<()> {
        if self
            .build_chunk
            .as_ref()
            .is_none_or(|chunk| chunk.is_empty())
        {
            return Ok(());
        }
        let groups = if self.probe_keys.is_empty() {
            None
        } else {
            let Some(table) = self.build_table.as_ref().filter(|table| !table.is_empty()) else {
                return Ok(());
            };
            let start = std::time::Instant::now();
            let factory = self
                .construction
                .as_deref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let owner = factory.begin_input_construction(&input)?;
            let evaluation =
                crate::exec::expr::ExprEvaluationContext::new(&self.arena, owner.as_ref(), ctx);
            let groups =
                table.lookup_group_ids_evaluated(&self.probe_keys, &input, &mut |id, input| {
                    evaluation.eval(id, input)
                })?;
            self.record_search_ns(start);
            let stats = SearchStats::from_group_ids(&groups);
            self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(stats.lookup_hit_rows);
            self.lookup_miss_rows = self.lookup_miss_rows.saturating_add(stats.lookup_miss_rows);
            if self.residual_predicate.is_some() {
                self.residual_rows_checked = self
                    .residual_rows_checked
                    .saturating_add(stats.lookup_hit_rows);
            }
            Some(JoinPairCursor::new(groups))
        };
        self.inner_cursor = Some(InnerProbeCursor {
            input,
            groups,
            probe_position: 0,
            build_position: 0,
        });
        Ok(())
    }
    pub(crate) fn drive_probe(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if self.inner_cursor.is_none() {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        }
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let mut cursor = self.inner_cursor.take().expect("live inner-join input");
        let result = self.advance_inner(ctx, &mut cursor);
        match result {
            Ok(step) => {
                if matches!(step.next, NextStep::Runnable) {
                    self.inner_cursor = Some(cursor);
                }
                Ok(step)
            }
            Err(error) => {
                ctx.report_failure(error.clone());
                Err(error)
            }
        }
    }
    fn advance_inner(
        &mut self,
        ctx: &StepContext<'_>,
        cursor: &mut InnerProbeCursor,
    ) -> ExecutionResult<StepResult> {
        let build = self
            .build_chunk
            .clone()
            .ok_or_else(|| probe_contract("inner join cursor has no build rows"))?;
        let (mut selection, complete) = if let Some(groups) = cursor.groups.as_mut() {
            let table = self
                .build_table
                .as_ref()
                .ok_or_else(|| probe_contract("inner join cursor has no lookup table"))?;
            let selection = groups
                .next_page(
                    table,
                    std::num::NonZeroUsize::new(MAX_JOIN_OUTPUT_ROWS_PER_BATCH)
                        .expect("positive join page size"),
                )
                .map_err(ExecutionFailure::from_domain)?;
            (selection, groups.is_complete())
        } else {
            let mut selection = JoinSelection::new();
            while cursor.probe_position < cursor.input.len()
                && selection.len() < MAX_JOIN_OUTPUT_ROWS_PER_BATCH
            {
                let probe = u32::try_from(cursor.probe_position)
                    .map_err(|_| ExecutionFailure::from_domain(JoinCursorFailure::RowIdExtent))?;
                let build_row = u32::try_from(cursor.build_position)
                    .map_err(|_| ExecutionFailure::from_domain(JoinCursorFailure::RowIdExtent))?;
                selection.push(probe, build_row);
                cursor.build_position += 1;
                if cursor.build_position == build.len() {
                    cursor.probe_position += 1;
                    cursor.build_position = 0;
                }
            }
            (selection, cursor.probe_position == cursor.input.len())
        };
        if let Some(predicate) = self.residual_predicate {
            self.residual_group_rows_total = self
                .residual_group_rows_total
                .saturating_add(selection.len() as u64);
            let factory = self
                .construction
                .clone()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let arena = self.arena.clone();
            self.compact_selection_by_residual(
                &cursor.input,
                &build,
                &mut selection,
                predicate,
                &mut |id, input| {
                    let owner = factory.begin_input_construction(input)?;
                    crate::exec::expr::ExprEvaluationContext::new(&arena, owner.as_ref(), ctx)
                        .eval(id, input)
                },
                &mut |batch, schema, sources, nullable_slots| {
                    create_join_input(batch, schema, sources, nullable_slots, factory.as_ref())
                },
            )?;
        }
        let next = if complete {
            NextStep::NeedInput
        } else {
            NextStep::Runnable
        };
        if selection.is_empty() {
            return Ok(StepResult::new(None, next, 0));
        }
        let schema = self.join_scope_chunk_schema.clone();
        let gathered = gather_probe_build_batches(
            &cursor.input,
            &build,
            &selection.probe,
            &selection.build,
            &schema.arrow_schema_ref(),
            self.probe_is_left,
            is_all_match_one(&selection, cursor.input.len()),
        )?;
        self.record_out_timings(gathered.timings);
        let [batch] = <[RecordBatch; 1]>::try_from(gathered.batches).map_err(|batches| {
            probe_contract(format_args!(
                "inner join match page produced {} batches, expected one",
                batches.len()
            ))
        })?;
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let output = create_join_input(
            batch,
            schema,
            &[&cursor.input, &build],
            &[],
            factory.as_ref(),
        )?;
        ctx.checkpoint()?;
        self.output_rows = self.output_rows.saturating_add(output.len() as u64);
        Ok(StepResult::new(Some(output), next, 0))
    }
}
