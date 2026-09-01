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

use novarocks_spi::connector::write_stack::{WriteTargetOrdinal, validate_dense_target_ordinals};

use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::exec::node::ExecNode;
use crate::exec::node::table_write_relation::ConnectorCommitFragmentCarrierValidator;

/// The bounded aggregation stage of one distributed write.
#[derive(Clone)]
pub struct TableFinishNode {
    pub inputs: Vec<ExecNode>,
    pub node_id: i32,
    expected_targets: Arc<Vec<WriteTargetOrdinal>>,
    fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
}

impl TableFinishNode {
    pub fn try_new(
        inputs: Vec<ExecNode>,
        node_id: i32,
        expected_targets: Vec<WriteTargetOrdinal>,
        fragment_validator: Arc<dyn ConnectorCommitFragmentCarrierValidator>,
    ) -> Result<Self, ExecPlanBuildError> {
        if inputs.is_empty() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Node,
                "table finish requires at least one writer input".to_string(),
            ));
        }
        validate_dense_target_ordinals(&expected_targets).map_err(|error| {
            ExecPlanBuildError::new(
                ExecPlanInvariant::Node,
                format!("table finish expected write targets: {error}"),
            )
        })?;
        Ok(Self {
            inputs,
            node_id,
            expected_targets: Arc::new(expected_targets),
            fragment_validator,
        })
    }

    pub fn expected_targets(&self) -> &Arc<Vec<WriteTargetOrdinal>> {
        &self.expected_targets
    }

    /// The highest legal ordinal in the sealed set. The set is dense from zero,
    /// so a single comparison decides membership.
    pub fn highest_expected_ordinal(&self) -> u32 {
        self.expected_targets
            .iter()
            .map(|target| target.get())
            .max()
            .unwrap_or_default()
    }

    pub const fn fragment_validator(&self) -> &Arc<dyn ConnectorCommitFragmentCarrierValidator> {
        &self.fragment_validator
    }
}

impl std::fmt::Debug for TableFinishNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TableFinishNode")
            .field("node_id", &self.node_id)
            .field("inputs", &self.inputs.len())
            .field("expected_targets", &self.expected_targets)
            .finish_non_exhaustive()
    }
}
