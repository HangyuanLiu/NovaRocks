use arrow::datatypes::DataType;
use novarocks::engine::statistics::{
    CollectedColumnStatistics, StatisticsColumn, StatisticsEngine, StatisticsInsertObservation,
    StatisticsInsertSource, StatisticsLiteral, StatisticsOverwriteMode, StatisticsRequestContext,
    StatisticsService, StatisticsTableTarget,
};
use novarocks_frontend::FrontendStatisticsService;

struct FakeStatisticsEngine {
    local_columns: Vec<StatisticsColumn>,
}

impl StatisticsEngine for FakeStatisticsEngine {
    fn resolve_table_columns(
        &self,
        _target: &StatisticsTableTarget,
    ) -> Result<Vec<StatisticsColumn>, String> {
        Ok(self.local_columns.clone())
    }

    fn resolve_local_table_columns(
        &self,
        _database: &str,
        _table: &str,
    ) -> Result<Option<Vec<StatisticsColumn>>, String> {
        Ok(Some(self.local_columns.clone()))
    }

    fn collect_table_statistics(
        &self,
        _target: &StatisticsTableTarget,
        _columns: &[String],
    ) -> Result<Vec<novarocks::engine::statistics::CollectedColumnStatistics>, String> {
        Ok(Vec::new())
    }
}

#[test]
fn catalog_provider_returns_owned_rows_and_none_for_missing_table() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine {
        local_columns: vec![
            StatisticsColumn {
                name: "k".to_string(),
                data_type: DataType::Int64,
            },
            StatisticsColumn {
                name: "v".to_string(),
                data_type: DataType::Int64,
            },
        ],
    };
    let source = StatisticsInsertSource::Values(vec![
        vec![StatisticsLiteral::Int(1), StatisticsLiteral::Int(10)],
        vec![StatisticsLiteral::Int(2), StatisticsLiteral::Int(20)],
        vec![StatisticsLiteral::Int(3), StatisticsLiteral::Int(30)],
    ]);
    service
        .observe_insert(
            &engine,
            StatisticsInsertObservation {
                database: "db1",
                table: "t1",
                insert_columns: &[],
                source: &source,
                overwrite_mode: StatisticsOverwriteMode::Append,
            },
        )
        .unwrap();
    let snapshot = service
        .catalog_table_statistics("db1", "t1")
        .unwrap()
        .expect("table stats");
    assert_eq!(snapshot.columns.len(), 2);
    assert_eq!(snapshot.columns[0].row_count, 3);
    assert!(
        service
            .catalog_table_statistics("db1", "missing")
            .unwrap()
            .is_none()
    );
}

#[test]
fn catalog_provider_preserves_nonnumeric_bounds_and_invalid_ndv_rows() {
    struct CollectedRowsEngine {
        rows: Vec<CollectedColumnStatistics>,
    }

    impl StatisticsEngine for CollectedRowsEngine {
        fn resolve_table_columns(
            &self,
            _target: &StatisticsTableTarget,
        ) -> Result<Vec<StatisticsColumn>, String> {
            Ok(self
                .rows
                .iter()
                .map(|row| StatisticsColumn {
                    name: row.column_name.clone(),
                    data_type: DataType::Utf8,
                })
                .collect())
        }

        fn resolve_local_table_columns(
            &self,
            _database: &str,
            _table: &str,
        ) -> Result<Option<Vec<StatisticsColumn>>, String> {
            Ok(None)
        }

        fn collect_table_statistics(
            &self,
            _target: &StatisticsTableTarget,
            _columns: &[String],
        ) -> Result<Vec<CollectedColumnStatistics>, String> {
            Ok(self.rows.clone())
        }
    }

    let service = FrontendStatisticsService::new();
    let engine = CollectedRowsEngine {
        rows: vec![
            CollectedColumnStatistics {
                column_name: "payload".to_string(),
                row_count: 3,
                min: "ten".to_string(),
                max: "thirty".to_string(),
                ndv: String::new(),
            },
            CollectedColumnStatistics {
                column_name: "zero_ndv".to_string(),
                row_count: 0,
                min: "-1".to_string(),
                max: "0".to_string(),
                ndv: "0".to_string(),
            },
            CollectedColumnStatistics {
                column_name: "negative_ndv".to_string(),
                row_count: 0,
                min: "-1".to_string(),
                max: "0".to_string(),
                ndv: "-2".to_string(),
            },
        ],
    };
    service
        .try_handle_statement(
            &engine,
            "ANALYZE TABLE db1.raw_stats",
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap()
        .expect("analyze handled");

    let snapshot = service
        .catalog_table_statistics("db1", "raw_stats")
        .unwrap()
        .expect("raw statistics");
    assert_eq!(snapshot.columns.len(), 3);
    assert_eq!(snapshot.columns[0].min, "ten");
    assert_eq!(snapshot.columns[0].max, "thirty");
    assert_eq!(snapshot.columns[0].ndv, "");
    assert_eq!(snapshot.columns[1].ndv, "0");
    assert_eq!(snapshot.columns[2].ndv, "-2");
}
