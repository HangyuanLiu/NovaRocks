#![allow(dead_code)]

use std::sync::Arc;

use crate::connector::stats::{
    ScanSourceIdentity, TableSnapshotRef, TableStatsProvider, TableStatsRequest,
};
use crate::sql::catalog::ScanSource;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::stats_input::{
    BaseTableStatistics, QueryStatsSnapshot, StatsMissingReason, StatsRef,
};

#[derive(Clone, Default)]
pub(crate) struct QueryStatsProviders {
    iceberg: Option<Arc<dyn TableStatsProvider>>,
}

impl QueryStatsProviders {
    pub(crate) fn none() -> Self {
        Self::default()
    }

    pub(crate) fn from_connectors(connectors: &crate::connector::ConnectorRegistry) -> Self {
        let iceberg = connectors
            .table_source("iceberg")
            .ok()
            .and_then(|source| source.stats_provider());
        Self { iceberg }
    }

    pub(crate) fn from_standalone_state(state: &Arc<super::StandaloneState>) -> Self {
        let connectors = state
            .connectors
            .read()
            .expect("standalone connectors read lock");
        Self::from_connectors(&connectors)
    }

    pub(crate) fn from_optional_state(state: Option<&Arc<super::StandaloneState>>) -> Self {
        state
            .map(Self::from_standalone_state)
            .unwrap_or_else(Self::none)
    }
}

pub(crate) struct QueryStatsPlan {
    pub snapshot: QueryStatsSnapshot,
    next_stats_ref: u32,
}

impl QueryStatsPlan {
    fn new(snapshot: QueryStatsSnapshot, next_stats_ref: u32) -> Self {
        Self {
            snapshot,
            next_stats_ref,
        }
    }

    pub(crate) fn add_stats(
        &mut self,
        label: impl Into<String>,
        stats: BaseTableStatistics,
    ) -> StatsRef {
        let stats_ref = StatsRef::new(self.next_stats_ref);
        self.next_stats_ref += 1;
        self.snapshot.insert(stats_ref, label, stats);
        stats_ref
    }
}

pub(crate) struct QueryStatsCollector {
    providers: QueryStatsProviders,
    next_stats_ref: u32,
    snapshot: QueryStatsSnapshot,
}

impl QueryStatsCollector {
    pub(crate) fn new(providers: QueryStatsProviders) -> Self {
        Self {
            providers,
            next_stats_ref: 0,
            snapshot: QueryStatsSnapshot::empty(),
        }
    }

    pub(crate) fn collect(mut self, opt_expr: &mut OptExpr) -> QueryStatsPlan {
        self.walk(opt_expr);
        QueryStatsPlan::new(self.snapshot, self.next_stats_ref)
    }

    fn walk(&mut self, expr: &mut OptExpr) {
        if let Operator::LogicalScan(scan) = &mut expr.op {
            let stats_ref = StatsRef::new(self.next_stats_ref);
            self.next_stats_ref += 1;
            scan.stats_ref = Some(stats_ref);

            let (label, stats) = self.collect_scan(scan);
            self.snapshot.insert(stats_ref, label, stats);
        }

        for child in &mut expr.children {
            self.walk(child);
        }
    }

    fn collect_scan(
        &self,
        scan: &crate::sql::optimizer::operator::ScanOp,
    ) -> (String, BaseTableStatistics) {
        let label = scan_label(scan);
        let Some(request) = table_stats_request(scan) else {
            return (
                label,
                BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                    "scan source does not expose query stats".to_string(),
                )),
            );
        };

        let stats = match &request.source {
            ScanSourceIdentity::IcebergTable { .. } => {
                let Some(provider) = self.providers.iceberg.as_deref() else {
                    return (
                        label,
                        BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                            "iceberg stats provider is not registered".to_string(),
                        )),
                    );
                };
                provider
                    .estimate_table_statistics(&request)
                    .unwrap_or_else(|err| BaseTableStatistics::missing(err.into_missing_reason()))
            }
            ScanSourceIdentity::Unsupported { reason } => BaseTableStatistics::missing(
                StatsMissingReason::ConnectorUnsupported(reason.clone()),
            ),
        };

        (label, stats)
    }
}

fn scan_label(scan: &crate::sql::optimizer::operator::ScanOp) -> String {
    match &scan.table.source {
        ScanSource::IcebergDataFiles { table, .. }
        | ScanSource::IcebergVersionTable { table, .. }
        | ScanSource::IcebergDeltaTable { table, .. } => {
            format!("{}.{}.{}", table.catalog, table.namespace, table.table)
        }
        _ => format!("{}.{}", scan.database, scan.table.name),
    }
}

fn table_stats_request(
    scan: &crate::sql::optimizer::operator::ScanOp,
) -> Option<TableStatsRequest> {
    match &scan.table.source {
        ScanSource::IcebergDataFiles { table, .. } => Some(TableStatsRequest {
            catalog: Some(table.catalog.clone()),
            database: table.namespace.clone(),
            table: table.table.clone(),
            source: ScanSourceIdentity::IcebergTable {
                catalog: table.catalog.clone(),
                namespace: table.namespace.clone(),
                table: table.table.clone(),
            },
            snapshot: Some(TableSnapshotRef::Current),
        }),
        ScanSource::IcebergVersionTable { table, snapshot_id } => Some(TableStatsRequest {
            catalog: Some(table.catalog.clone()),
            database: table.namespace.clone(),
            table: table.table.clone(),
            source: ScanSourceIdentity::IcebergTable {
                catalog: table.catalog.clone(),
                namespace: table.namespace.clone(),
                table: table.table.clone(),
            },
            snapshot: Some(TableSnapshotRef::SnapshotId(*snapshot_id)),
        }),
        ScanSource::IcebergDeltaTable { table, .. } => Some(TableStatsRequest {
            catalog: Some(table.catalog.clone()),
            database: table.namespace.clone(),
            table: table.table.clone(),
            source: ScanSourceIdentity::Unsupported {
                reason: "iceberg delta scan stats are not supported".to_string(),
            },
            snapshot: None,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::stats::StatsProviderError;
    use crate::sql::catalog::{
        IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::{JoinKind, OutputColumn};
    use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ScanOp};
    use crate::sql::optimizer::stats_input::{StatValue, StatsSource};

    #[test]
    fn collector_binds_each_scan_in_the_same_opt_expr_traversal() {
        let mut expr = test_join_with_two_scans();

        let plan = QueryStatsCollector::new(QueryStatsProviders::none()).collect(&mut expr);

        let refs = collect_scan_refs_for_test(&expr);
        assert_eq!(refs.len(), 2);
        assert_ne!(refs[0], refs[1]);
        assert_eq!(plan.snapshot.len(), 2);
    }

    #[test]
    fn table_stats_request_maps_iceberg_current_and_version_sources() {
        let current = table_stats_request(&test_iceberg_scan_op(ScanSource::IcebergDataFiles {
            table: iceberg_info("cat", "db", "tbl"),
            files: vec![],
            cloud_properties: BTreeMap::new(),
            binding: IcebergDataFileBinding::CurrentSnapshot,
        }))
        .expect("current iceberg scan should have stats request");
        assert_eq!(current.catalog.as_deref(), Some("cat"));
        assert_eq!(current.database, "db");
        assert_eq!(current.table, "tbl");
        assert_eq!(current.snapshot, Some(TableSnapshotRef::Current));
        assert_eq!(
            current.source,
            ScanSourceIdentity::IcebergTable {
                catalog: "cat".to_string(),
                namespace: "db".to_string(),
                table: "tbl".to_string(),
            }
        );

        let version = table_stats_request(&test_iceberg_scan_op(ScanSource::IcebergVersionTable {
            table: iceberg_info("cat", "db", "tbl"),
            snapshot_id: 42,
        }))
        .expect("version iceberg scan should have stats request");
        assert_eq!(version.snapshot, Some(TableSnapshotRef::SnapshotId(42)));
        assert_eq!(
            version.source,
            ScanSourceIdentity::IcebergTable {
                catalog: "cat".to_string(),
                namespace: "db".to_string(),
                table: "tbl".to_string(),
            }
        );
    }

    #[test]
    fn table_stats_request_marks_iceberg_delta_unsupported() {
        let delta = table_stats_request(&test_iceberg_scan_op(ScanSource::IcebergDeltaTable {
            table: iceberg_info("cat", "db", "tbl"),
            from_snapshot_id: 1,
            to_snapshot_id: 2,
        }))
        .expect("delta iceberg scan should produce unsupported request");

        assert_eq!(delta.snapshot, None);
        assert_eq!(
            delta.source,
            ScanSourceIdentity::Unsupported {
                reason: "iceberg delta scan stats are not supported".to_string(),
            }
        );
    }

    #[test]
    fn provider_error_becomes_missing_stats_without_blocking_collection() {
        let provider = Arc::new(FailingStatsProvider::default());
        let providers = QueryStatsProviders {
            iceberg: Some(provider.clone()),
        };
        let mut expr = test_iceberg_scan(ScanSource::IcebergDataFiles {
            table: iceberg_info("cat", "db", "tbl"),
            files: vec![],
            cloud_properties: BTreeMap::new(),
            binding: IcebergDataFileBinding::CurrentSnapshot,
        });

        let plan = QueryStatsCollector::new(providers).collect(&mut expr);

        let refs = collect_scan_refs_for_test(&expr);
        let stats_ref = refs[0].expect("scan should be bound");
        let stats = plan.snapshot.get(stats_ref).expect("snapshot entry");
        assert_eq!(
            stats.row_count,
            StatValue::missing(StatsMissingReason::CatalogLoadError(
                "catalog unavailable".to_string()
            ))
        );
        assert_eq!(provider.requests.lock().expect("requests").len(), 1);
    }

    #[test]
    fn add_stats_allocates_from_next_stats_ref_not_snapshot_len() {
        let mut snapshot = QueryStatsSnapshot::empty();
        snapshot.insert(
            StatsRef::new(99),
            "preexisting",
            BaseTableStatistics::missing(StatsMissingReason::NoDataFiles),
        );
        let mut plan = QueryStatsPlan::new(snapshot, 7);

        let stats_ref = plan.add_stats(
            "extra",
            BaseTableStatistics {
                row_count: StatValue::known(
                    5,
                    crate::sql::optimizer::statistics::Confidence::Exact,
                    StatsSource::TestFixture,
                ),
                columns: HashMap::new(),
                source: StatsSource::TestFixture,
            },
        );

        assert_eq!(stats_ref, StatsRef::new(7));
        assert!(plan.snapshot.get(StatsRef::new(7)).is_some());
        assert!(plan.snapshot.get(StatsRef::new(99)).is_some());
    }

    fn test_join_with_two_scans() -> OptExpr {
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![test_scan("left", 1), test_scan("right", 2)],
        )
    }

    fn test_scan(name: &str, column_id: u32) -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: i64::from(column_id),
                },
            },
            alias: None,
            stats_ref: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(column_id),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn test_iceberg_scan(source: ScanSource) -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(test_iceberg_scan_op(source)))
    }

    fn test_iceberg_scan_op(source: ScanSource) -> ScanOp {
        ScanOp {
            database: "db".to_string(),
            table: TableDef {
                name: "tbl".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source,
            },
            alias: None,
            stats_ref: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(10),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }
    }

    fn iceberg_info(catalog: &str, namespace: &str, table: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(1),
            schema_id: 1,
            location: format!("file:///tmp/{table}"),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn collect_scan_refs_for_test(expr: &OptExpr) -> Vec<Option<StatsRef>> {
        let mut refs = Vec::new();
        collect_scan_refs(expr, &mut refs);
        refs
    }

    fn collect_scan_refs(expr: &OptExpr, refs: &mut Vec<Option<StatsRef>>) {
        if let Operator::LogicalScan(scan) = &expr.op {
            refs.push(scan.stats_ref);
        }
        for child in &expr.children {
            collect_scan_refs(child, refs);
        }
    }

    #[derive(Default)]
    struct FailingStatsProvider {
        requests: Mutex<Vec<TableStatsRequest>>,
    }

    impl TableStatsProvider for FailingStatsProvider {
        fn estimate_table_statistics(
            &self,
            request: &TableStatsRequest,
        ) -> Result<BaseTableStatistics, StatsProviderError> {
            self.requests
                .lock()
                .expect("requests")
                .push(request.clone());
            Err(StatsProviderError::Catalog(
                "catalog unavailable".to_string(),
            ))
        }
    }
}
