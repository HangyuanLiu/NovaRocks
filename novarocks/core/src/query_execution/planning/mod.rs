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

//! Sealed request-local inputs consumed by query execution after SQL
//! compilation. SQL receives opaque binding tokens only; these modules retain
//! the paired exact connector admission and never reacquire a newer binding.

pub(crate) mod bindings;
pub mod catalog_materializer;
pub(crate) mod delta_scan;
pub mod statistics;
pub mod time_travel;
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

pub fn sql_cancellation_observation(
    view: QueryCancellationView,
) -> Arc<dyn SqlCancellationObservation> {
    Arc::new(QueryCancellationObservation::new(view))
}

pub(crate) struct PostCompilePlanningContext<'a> {
    pub(crate) table_bindings: Arc<bindings::QueryTableBindingStore>,
    pub(crate) connector_controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    pub(crate) connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
}

pub(crate) struct QueryPlanningInputs<'a> {
    pub(crate) compile_request: SqlCompileRequest<'a>,
    pub(crate) post_compile: PostCompilePlanningContext<'a>,
}
