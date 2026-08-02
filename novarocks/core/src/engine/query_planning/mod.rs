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

//! Application-owned query planning input assembly.
//!
//! SQL consumes `SqlCompileRequest` only.  This module keeps the paired exact
//! table bindings, connector controls, and request context available solely
//! for post-compile preparation and native request assembly.

use std::sync::Arc;

use crate::query_execution::cancellation::QueryCancellationView;
use crate::sql::compiler::{SqlCancellationObservation, SqlCompileRequest};

/// Adapter from application cancellation state to the SQL-owned observation.
#[derive(Clone)]
pub(crate) struct QueryCancellationObservation {
    view: QueryCancellationView,
}

impl QueryCancellationObservation {
    pub(crate) fn new(view: QueryCancellationView) -> Self {
        Self { view }
    }
}

impl SqlCancellationObservation for QueryCancellationObservation {
    fn is_cancelled(&self) -> bool {
        self.view.is_cancelled()
    }
}

pub(crate) fn sql_cancellation_observation(
    view: QueryCancellationView,
) -> Arc<dyn SqlCancellationObservation> {
    Arc::new(QueryCancellationObservation::new(view))
}

/// Application-owned facts used only after SQL has produced a plan.
pub(crate) struct PostCompilePlanningContext<'a> {
    pub(crate) table_bindings: Arc<crate::sql::catalog::provider::QueryTableBindingStore>,
    pub(crate) connector_controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    pub(crate) connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
}

/// One admission's complete planning input. The exact binding store is shared
/// with the catalog and statistics snapshots that fed the compiler, but is
/// structurally unavailable to SQL compilation itself.
pub(crate) struct QueryPlanningInputs<'a> {
    pub(crate) compile_request: SqlCompileRequest<'a>,
    pub(crate) post_compile: PostCompilePlanningContext<'a>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::query_execution::cancellation::{QueryCancellationReason, QueryCancellationSource};
    use crate::sql::compiler::SqlCancellationObservation;

    use super::QueryCancellationObservation;

    #[test]
    fn sqlx2_application_cancellation_adapter_hides_the_reason() {
        let source = QueryCancellationSource::new();
        let observation = QueryCancellationObservation::new(source.view());
        assert!(!observation.is_cancelled());
        source.request(QueryCancellationReason::ServerShutdown);
        assert!(observation.is_cancelled());

        let _: Arc<dyn SqlCancellationObservation> = Arc::new(observation);
    }
}
