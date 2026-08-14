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

//! Application-owned lookup for provider-sealed change-window scans.
//!
//! The provider admits the physical change read while the refresh owns its
//! exact planning lease. Preparation retrieves only the sealed SPI scan by its
//! query-local token and snapshot window.

use crate::query_execution::planning::bindings::QueryTableBindingStore;
use crate::query_execution::preparation::scan::{ResolvedScanExecution, ScanBindingResolver};
use novarocks_sql::planner::payload::PlanScanNode;
use novarocks_sql::planner::table::{ScanSource, SqlScanKind};

/// Exact query-local delta lookup.  It intentionally accepts neither a
/// refresh context nor a catalog/registry, so it cannot reacquire metadata or
/// a newer connector generation after compilation.
pub(crate) struct QueryTableBindingScanResolver<'a> {
    bindings: &'a QueryTableBindingStore,
}

impl<'a> QueryTableBindingScanResolver<'a> {
    pub(crate) fn new(bindings: &'a QueryTableBindingStore) -> Self {
        Self { bindings }
    }
}

impl ScanBindingResolver for QueryTableBindingScanResolver<'_> {
    fn resolve_scan(
        &self,
        _node_id: i32,
        scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        let ScanSource::Sql(source) = &scan.table.source;
        let SqlScanKind::Delta {
            from_snapshot_id,
            to_snapshot_id,
        } = source.kind
        else {
            return Ok(None);
        };
        let binding = self.bindings.binding(source.binding)?;
        let admitted_scan = binding
            .admitted_change_scans
            .get(&(from_snapshot_id, to_snapshot_id))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "SQL delta scan binding for '{}.{}.{}' has no sealed change-window admission from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}",
                    source.table.catalog, source.table.namespace, source.table.table
                )
            })?;
        Ok(Some(ResolvedScanExecution::SealedConnectorScan(
            admitted_scan,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorChangeWindow, ConnectorChangeWindowAdmission,
        ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorRequestContext, ConnectorScan, ConnectorScanHandle,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::{QueryTableBindingScanResolver, ScanBindingResolver};
    use crate::query_execution::planning::bindings::{
        QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore,
    };
    use crate::query_execution::preparation::scan::ResolvedScanExecution;
    use novarocks_sql::catalog::ResolvedAnalyzerTable;
    use novarocks_sql::planner::payload::PlanScanNode;
    use novarocks_sql::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
    };

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn admitted_scan() -> ConnectorScan {
        let owner = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance ID"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request context");
        ConnectorScan::try_new_change_window(
            owner.clone(),
            ConnectorChangeWindow::new(10, 20),
            ConnectorChangeWindowAdmission::MetadataOnly,
            ConnectorScanHandle::try_new(owner.instance_id, Bytes::from_static(b"change-v1"))
                .expect("scan handle"),
            Arc::new(Schema::empty()),
            Vec::new(),
            &context,
        )
        .expect("sealed scan")
    }

    fn delta_scan(
        binding: novarocks_sql::binding::SqlTableBindingId,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
    ) -> PlanScanNode {
        let source = ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
            },
            SqlScanKind::Delta {
                from_snapshot_id,
                to_snapshot_id,
            },
        ));
        PlanScanNode {
            database: "sales".to_string(),
            table: TableDef {
                name: "orders".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source,
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }
    }

    fn binding_with_delta(binding: novarocks_sql::binding::SqlTableBindingId) -> QueryTableBinding {
        let source = ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
            },
            SqlScanKind::Delta {
                from_snapshot_id: 10,
                to_snapshot_id: 20,
            },
        ));
        QueryTableBinding {
            resolved: ResolvedAnalyzerTable::from_planner(
                Some("ice"),
                "sales",
                TableDef {
                    name: "orders".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source,
                },
            ),
            statistics_pin: None,
            admission:
                crate::query_execution::planning::bindings::QueryTableBindingAdmission::Local,
            scan_materialization: None,
            write_target_admission: None,
            mv_target_read: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            admitted_change_scans: BTreeMap::from([((10, 20), admitted_scan())]),
        }
    }

    #[test]
    fn sqlx2_preparation_delta_resolves_only_its_admitted_window() {
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        let binding = bindings
            .resolve_or_insert_with_id(
                QueryTableBindingKey::snapshot("ice", "sales", "orders", 20),
                |binding| Ok(binding_with_delta(binding)),
            )
            .expect("admit binding");
        let resolver = QueryTableBindingScanResolver::new(&bindings);

        let resolved = resolver
            .resolve_scan(7, &delta_scan(binding, 10, 20))
            .expect("resolve admitted delta")
            .expect("delta scan execution");
        let ResolvedScanExecution::SealedConnectorScan(scan) = resolved else {
            panic!("expected sealed connector scan");
        };
        assert_eq!(
            scan.selection(),
            novarocks_spi::connector::ConnectorScanSelection::ChangeWindow(
                ConnectorChangeWindow::new(10, 20)
            )
        );

        let error = resolver
            .resolve_scan(7, &delta_scan(binding, 9, 20))
            .expect_err("unadmitted window must fail before submission");
        assert!(
            error.contains("no sealed change-window admission"),
            "error={error}"
        );
    }

    #[test]
    fn sqlx2_preparation_delta_rejects_cross_request_token() {
        let first = QueryTableBindingStore::try_new().expect("first binding store");
        let second = QueryTableBindingStore::try_new().expect("second binding store");
        let binding = second
            .resolve_or_insert_with_id(
                QueryTableBindingKey::snapshot("ice", "sales", "orders", 20),
                |binding| Ok(binding_with_delta(binding)),
            )
            .expect("admit second binding");

        let error = QueryTableBindingScanResolver::new(&first)
            .resolve_scan(8, &delta_scan(binding, 10, 20))
            .expect_err("cross-request token must fail before connector preparation");
        assert!(error.contains("different request"), "error={error}");
    }
}
