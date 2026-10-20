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

//! The `TableFinish` plan node.
//!
//! `TableFinish` is a single-driver processor on one Root BE. It consumes every
//! writer row, validates row shape and budgets, checked-sums the row count,
//! collects canonical commit fragments by target ordinal, and emits its
//! complete relation into the ordinary RESULT sink only after all of its inputs
//! reached EOS. It performs bounded aggregation and nothing else: it holds no
//! commit handle, decodes no fragment, and never touches external catalog
//! metadata.
//!
//! The node is n-ary. An exchange receiver names exactly one source fragment,
//! so a query with several writer fragments gives the finish node one receiver
//! per writer rather than one shared receiver. The pipeline converges them the
//! way `UnionAll` does, straight onto a single-driver pipeline.

use std::sync::Arc;

use novarocks_spi::connector::write_stack::{WriteTargetOrdinal, validate_query_target_ordinals};

use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::exec::node::table_write_aggregate::WriterFinalAggregatePlan;
#[cfg(novarocks_dev_controls)]
use crate::exec::node::table_write_relation::{
    AllowTableWriteAggregates, TableWriteAggregateGuard,
};
use crate::exec::node::table_write_relation::{
    ConnectorCommitFragmentCarrierValidator, RootWriteResultRelationSchema,
    WriterMultiplexRelationSchema,
};

/// Neutral finish-test facts paired with an actual Task validation binding.
/// No input node or construction-plan authority is retained.
#[cfg(test)]
pub(crate) struct FinishFixture {
    pub node_id: i32,
    expected_targets: Arc<Vec<WriteTargetOrdinal>>,
    binding: TableFinishRuntimeBinding,
    writer_multiplex_schema: WriterMultiplexRelationSchema,
    root_result_schema: RootWriteResultRelationSchema,
    final_aggregate_plan: WriterFinalAggregatePlan,
}

/// Runtime validation authority for a single Task instance of TableFinish.
pub struct TableFinishRuntimeBinding {
    pub(crate) fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
}

impl TableFinishRuntimeBinding {
    /// One Task's validation authority for one compiled finish.
    pub fn new(fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>) -> Self {
        Self {
            fragment_validator,
            #[cfg(novarocks_dev_controls)]
            aggregate_guard: Arc::new(AllowTableWriteAggregates),
        }
    }

    /// Bind the application-owned, query-scoped aggregate rejection guard.
    #[cfg(novarocks_dev_controls)]
    pub fn with_aggregate_guard(mut self, guard: Arc<dyn TableWriteAggregateGuard>) -> Self {
        self.aggregate_guard = guard;
        self
    }
}

#[cfg(test)]
impl FinishFixture {
    pub fn try_new(
        node_id: i32,
        expected_targets: Vec<WriteTargetOrdinal>,
        fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    ) -> Result<Self, ExecPlanBuildError> {
        Self::try_new_with_relations(
            node_id,
            expected_targets,
            fragment_validator,
            WriterMultiplexRelationSchema::empty(),
            RootWriteResultRelationSchema::fixed(),
            WriterFinalAggregatePlan::default(),
        )
    }

    pub fn try_new_with_relations(
        node_id: i32,
        expected_targets: Vec<WriteTargetOrdinal>,
        fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
        writer_multiplex_schema: WriterMultiplexRelationSchema,
        root_result_schema: RootWriteResultRelationSchema,
        final_aggregate_plan: WriterFinalAggregatePlan,
    ) -> Result<Self, ExecPlanBuildError> {
        // The expected set is the targets *this query's* writers feed, so it is
        // validated as a query set: non-empty, inside the frozen bound, and
        // free of duplicates. It is deliberately not required to be dense from
        // zero -- a copy-on-write statement compiles one writer per query, at
        // that group's own ordinal, so query `k` legitimately expects `[k]`.
        // Denseness stays a property of the session's sealed set, where
        // `ConnectorWriteSessionPlan::try_new` enforces it.
        validate_query_target_ordinals(&expected_targets).map_err(|error| {
            ExecPlanBuildError::new(
                ExecPlanInvariant::Node,
                format!("table finish expected write targets: {error}"),
            )
        })?;
        Ok(Self {
            node_id,
            expected_targets: Arc::new(expected_targets),
            binding: TableFinishRuntimeBinding::new(fragment_validator),
            writer_multiplex_schema,
            root_result_schema,
            final_aggregate_plan,
        })
    }

    /// Bind the application-owned, query-scoped aggregate rejection guard.
    #[cfg(novarocks_dev_controls)]
    pub fn with_aggregate_guard(mut self, guard: Arc<dyn TableWriteAggregateGuard>) -> Self {
        self.binding.aggregate_guard = guard;
        self
    }

    pub fn expected_targets(&self) -> &Arc<Vec<WriteTargetOrdinal>> {
        &self.expected_targets
    }

    /// Whether `ordinal` is one of the targets this query's writers feed.
    ///
    /// This is an exact set test rather than a comparison against the highest
    /// ordinal: a query set need not be dense from zero, so "at or below the
    /// highest" would admit a target this query never compiled a writer for.
    pub fn accepts_target(&self, ordinal: WriteTargetOrdinal) -> bool {
        self.expected_targets.contains(&ordinal)
    }

    pub const fn fragment_validator(&self) -> &Arc<dyn ConnectorCommitFragmentCarrierValidator> {
        &self.binding.fragment_validator
    }

    pub const fn writer_multiplex_schema(&self) -> &WriterMultiplexRelationSchema {
        &self.writer_multiplex_schema
    }

    pub const fn root_result_schema(&self) -> &RootWriteResultRelationSchema {
        &self.root_result_schema
    }

    pub const fn final_aggregate_plan(&self) -> &WriterFinalAggregatePlan {
        &self.final_aggregate_plan
    }

    #[cfg(novarocks_dev_controls)]
    pub const fn aggregate_guard(&self) -> &Arc<dyn TableWriteAggregateGuard> {
        &self.binding.aggregate_guard
    }
}

#[cfg(test)]
impl std::fmt::Debug for FinishFixture {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FinishFixture")
            .field("node_id", &self.node_id)
            .field("expected_targets", &self.expected_targets)
            .finish_non_exhaustive()
    }
}
