use arrow::datatypes::DataType;
use novarocks::engine::statistics::{
    StatisticsColumn, StatisticsEngine, StatisticsInsertObservation, StatisticsInsertSource,
    StatisticsLiteral, StatisticsOverwriteMode, StatisticsService, StatisticsTableTarget,
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
