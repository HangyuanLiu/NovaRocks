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

use crate::exec::node::table_write_relation::ConnectorCommitFragmentCarrierValidator;
#[cfg(novarocks_dev_controls)]
use crate::exec::node::table_write_relation::{
    AllowTableWriteAggregates, TableWriteAggregateGuard,
};

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
