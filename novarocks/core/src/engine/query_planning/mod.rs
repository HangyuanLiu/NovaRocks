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
// Design: ADR-0036 (docs/adr/ADR-0036-sql-compiler-dependency-inversion.md)

pub(crate) mod bindings;
pub(crate) mod catalog_materializer;
pub(crate) mod catalog_runtime;
pub(crate) mod delta_scan;
pub(crate) mod write_sink;

use std::{collections::HashMap, sync::Arc};

use crate::query_execution::cancellation::QueryCancellationView;
use crate::sql::compiler::{SqlCancellationObservation, SqlCompileRequest};
use crate::sql::explain::distributed::{
    SqlExplainProfile, SqlFragmentProfile, SqlFragmentProfileView, SqlOperatorMetrics,
    SqlOperatorProfileView,
};

impl SqlOperatorProfileView for crate::query_execution::profile::ActualMetrics {
    fn as_sql_operator_metrics(&self) -> SqlOperatorMetrics {
        SqlOperatorMetrics {
            output_rows: self.output_rows,
            total_time_ns: self.total_time_ns,
            peak_mem_bytes: self.peak_mem_bytes,
            total_time_max_ns: self.total_time_max_ns,
            total_time_min_ns: self.total_time_min_ns,
            build_ht_ns: self.build_ht_ns,
            search_ns: self.search_ns,
            out_build_ns: self.out_build_ns,
            out_probe_ns: self.out_probe_ns,
            dict_input_rows: self.dict_input_rows,
            dict_input_columns: self.dict_input_columns,
            dict_kept_rows: self.dict_kept_rows,
            dict_kept_columns: self.dict_kept_columns,
            dict_hydrated_rows: self.dict_hydrated_rows,
            dict_hydrated_columns: self.dict_hydrated_columns,
            dict_unsupported_columns: self.dict_unsupported_columns,
        }
    }
}

impl SqlFragmentProfileView for crate::query_execution::profile::DistributedProfileSummary {
    fn as_sql_fragment_profile(&self) -> SqlFragmentProfile {
        SqlFragmentProfile {
            operator_active_time_ns: self.operator_active_time_ns,
            driver_blocked_time_ns: self.driver_blocked_time_ns,
            dependency_wait_time_ns: self.dependency_wait_time_ns,
            exchange_wait_time_ns: self.exchange_wait_time_ns,
            network_time_ns: self.network_time_ns,
            scan_io_time_ns: self.scan_io_time_ns,
        }
    }
}

/// Project execution profiles into the SQL-owned EXPLAIN vocabulary at the
/// application boundary. The SQL formatter never inspects runtime profile
/// trees or execution-owned profile structs.
pub(crate) fn sql_explain_profile(
    operators: HashMap<i32, crate::query_execution::profile::ActualMetrics>,
    fragments: HashMap<i32, crate::query_execution::profile::DistributedProfileSummary>,
) -> SqlExplainProfile {
    SqlExplainProfile {
        operators: operators
            .into_iter()
            .map(|(node_id, metrics)| (node_id, metrics.as_sql_operator_metrics()))
            .collect(),
        fragments: fragments
            .into_iter()
            .map(|(node_id, metrics)| (node_id, metrics.as_sql_fragment_profile()))
            .collect(),
    }
}

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
    pub(crate) table_bindings: Arc<bindings::QueryTableBindingStore>,
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
    use std::{collections::HashMap, sync::Arc};

    use crate::query_execution::cancellation::{QueryCancellationReason, QueryCancellationSource};
    use crate::sql::compiler::SqlCancellationObservation;

    use super::{QueryCancellationObservation, sql_explain_profile};

    #[test]
    fn sqlx2_application_cancellation_adapter_hides_the_reason() {
        let source = QueryCancellationSource::new();
        let observation = QueryCancellationObservation::new(source.view());
        assert!(!observation.is_cancelled());
        source.request(QueryCancellationReason::ServerShutdown);
        assert!(observation.is_cancelled());

        let _: Arc<dyn SqlCancellationObservation> = Arc::new(observation);
    }

    #[test]
    fn sqlx2_control_profile_projects_execution_metrics_at_application_boundary() {
        let profile = sql_explain_profile(
            HashMap::from([(
                7,
                crate::query_execution::profile::ActualMetrics {
                    output_rows: 13,
                    total_time_ns: 42_000,
                    peak_mem_bytes: 4096,
                    ..Default::default()
                },
            )]),
            HashMap::from([(
                7,
                crate::query_execution::profile::DistributedProfileSummary {
                    operator_active_time_ns: 1_000,
                    driver_blocked_time_ns: 200,
                    ..Default::default()
                },
            )]),
        );

        let operator = profile.operators.get(&7).expect("projected operator");
        assert_eq!(operator.output_rows, 13);
        assert_eq!(operator.total_time_ns, 42_000);
        assert_eq!(operator.peak_mem_bytes, 4096);

        let fragment = profile.fragments.get(&7).expect("projected fragment");
        assert_eq!(fragment.operator_active_time_ns, 1_000);
        assert_eq!(fragment.driver_blocked_time_ns, 200);
    }
}
