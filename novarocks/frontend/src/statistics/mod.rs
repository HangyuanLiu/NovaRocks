mod model;
mod observation;
mod provider;
mod query;
mod statement;

use std::sync::RwLock;

use novarocks::engine::statistics::{
    StatisticsEngine, StatisticsInsertObservation, StatisticsRequestContext, StatisticsService,
    StatisticsStatementResult,
};
use novarocks::runtime::query_result::QueryResult;

use self::model::StatisticsState;

pub struct FrontendStatisticsService {
    state: RwLock<StatisticsState>,
}

impl FrontendStatisticsService {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(StatisticsState::default()),
        }
    }
}

impl Default for FrontendStatisticsService {
    fn default() -> Self {
        Self::new()
    }
}

impl StatisticsService for FrontendStatisticsService {
    fn try_handle_statement(
        &self,
        engine: &dyn StatisticsEngine,
        sql: &str,
        context: StatisticsRequestContext<'_>,
    ) -> Result<Option<StatisticsStatementResult>, String> {
        statement::try_handle_statement(self, engine, sql, context)
    }

    fn try_query(
        &self,
        sql: &str,
        query: &sqlparser::ast::Query,
        context: StatisticsRequestContext<'_>,
    ) -> Result<Option<QueryResult>, String> {
        query::try_query(self, sql, query, context.current_database)
    }

    fn observe_query(
        &self,
        query: &sqlparser::ast::Query,
        current_database: &str,
    ) -> Result<(), String> {
        observation::observe_query(self, query, current_database)
    }

    fn observe_insert(
        &self,
        engine: &dyn StatisticsEngine,
        observation: StatisticsInsertObservation<'_>,
    ) -> Result<(), String> {
        let target_columns =
            engine.resolve_local_table_columns(observation.database, observation.table)?;
        let Some(target_columns) = target_columns else {
            return Ok(());
        };
        self::observation::observe_insert(self, observation, &target_columns)
    }

    fn observe_update(&self, sql: &str, current_database: &str) -> Result<(), String> {
        observation::observe_update(self, sql, current_database)
    }

    fn drop_table(&self, database: &str, table: &str) {
        observation::drop_table(self, database, table);
    }

    fn drop_database(&self, database: &str) {
        observation::drop_database(self, database);
    }

    fn catalog_table_statistics(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<novarocks::engine::statistics::CatalogTableStatistics>, String> {
        provider::catalog_table_statistics(self, database, table)
    }
}
