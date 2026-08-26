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

//! Frontend-owned typed executor for durable statistics commands.

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::query_execution::StatementResult;
use crate::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
use crate::statistics_jobs::application::{
    StatisticsApplicationCommand, StatisticsApplicationPort, StatisticsApplicationResult,
    StatisticsColumnIntent, StatisticsTableTarget,
};
use novarocks_parser::ast::{AnalyzeMode, StatisticsStatement};
use novarocks_types::naming::normalize_identifier;

#[derive(Clone)]
pub struct StatisticsCommandExecutor {
    application: Arc<dyn StatisticsApplicationPort>,
}

fn statistics_application_target(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatisticsTableTarget, String> {
    let default_catalog = current_catalog.unwrap_or("default_catalog");
    let (catalog, namespace, table) = match parts {
        [table] => (default_catalog, current_database, table.as_str()),
        [namespace, table] => (default_catalog, namespace.as_str(), table.as_str()),
        [catalog, namespace, table] => (catalog.as_str(), namespace.as_str(), table.as_str()),
        _ => {
            return Err(format!(
                "statistics table name must be table, db.table, or catalog.db.table: {}",
                parts.join(".")
            ));
        }
    };
    Ok(StatisticsTableTarget {
        catalog: normalize_identifier(catalog)?,
        namespace: normalize_identifier(namespace)?,
        table: normalize_identifier(table)?,
    })
}

fn statistics_application_result(
    result: StatisticsApplicationResult,
) -> Result<StatementResult, String> {
    match result {
        StatisticsApplicationResult::JobSubmitted(_)
        | StatisticsApplicationResult::JobCancellationRequested(_) => Ok(StatementResult::Ok),
        StatisticsApplicationResult::AnalyzeJobs(jobs) => statistics_string_result(
            &[
                "job_id",
                "operation_id",
                "state",
                "attempt",
                "catalog",
                "namespace",
                "table",
                "error_kind",
                "error_message",
            ],
            jobs.into_iter()
                .map(|job| {
                    vec![
                        Some(job.job_id.to_string()),
                        Some(job.operation_id.to_string()),
                        Some(job.state),
                        Some(job.attempt.to_string()),
                        Some(job.target.catalog),
                        Some(job.target.namespace),
                        Some(job.target.table),
                        job.error_kind,
                        job.error_message,
                    ]
                })
                .collect(),
        ),
        StatisticsApplicationResult::TableStats(rows) => statistics_string_result(
            &[
                "metric",
                "value",
                "status",
                "basis_version",
                "source",
                "numeric_nature",
                "basis_relation",
            ],
            rows.into_iter()
                .map(|row| {
                    vec![
                        Some(row.metric),
                        row.value,
                        Some(row.status),
                        Some(row.basis_version),
                        Some(row.source),
                        Some(row.numeric_nature),
                        Some(row.basis_relation),
                    ]
                })
                .collect(),
        ),
    }
}

fn statistics_string_result(
    names: &[&str],
    rows: Vec<Vec<Option<String>>>,
) -> Result<StatementResult, String> {
    if rows.iter().any(|row| row.len() != names.len()) {
        return Err("statistics application returned malformed tabular result".to_string());
    }
    let columns = names
        .iter()
        .map(|name| QueryResultColumn {
            name: (*name).to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let arrays = (0..names.len())
        .map(|column| {
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row[column].clone())
                    .collect::<Vec<_>>(),
            )) as arrow::array::ArrayRef
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|error| format!("build statistics application result failed: {error}"))?;
    Ok(StatementResult::Query(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    }))
}

impl StatisticsCommandExecutor {
    pub fn new(application: Arc<dyn StatisticsApplicationPort>) -> Self {
        Self { application }
    }

    pub fn execute(
        &self,
        statement: &StatisticsStatement,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: Option<&crate::common::admitted_query_context::QueryExecutionContext>,
    ) -> Result<StatementResult, String> {
        let command = match statement {
            StatisticsStatement::AnalyzeTable(statement) => {
                if statement.mode != AnalyzeMode::Default || statement.with_sync_mode {
                    return Err(
                        "ANALYZE mode and sync options are not supported by the statistics application"
                            .to_string(),
                    );
                }
                StatisticsApplicationCommand::AnalyzeTable {
                    target: statistics_application_target(
                        &statement
                            .name
                            .parts
                            .iter()
                            .map(|part| part.value.clone())
                            .collect::<Vec<_>>(),
                        current_catalog,
                        current_database,
                    )?,
                    columns: if statement.columns.is_empty() {
                        StatisticsColumnIntent::AllColumns
                    } else {
                        StatisticsColumnIntent::Explicit(
                            statement
                                .columns
                                .iter()
                                .map(|column| column.value.clone())
                                .collect(),
                        )
                    },
                }
            }
            StatisticsStatement::ShowAnalyzeJobs(_) => {
                StatisticsApplicationCommand::ShowAnalyzeJobs
            }
            StatisticsStatement::CancelAnalyze(statement) => {
                StatisticsApplicationCommand::CancelAnalyze {
                    job_id: uuid::Uuid::parse_str(&statement.job_id).map_err(|error| {
                        format!("invalid ANALYZE job ID '{}': {error}", statement.job_id)
                    })?,
                }
            }
            StatisticsStatement::ShowTableStats(statement) => {
                StatisticsApplicationCommand::ShowTableStats {
                    target: statistics_application_target(
                        &statement
                            .name
                            .parts
                            .iter()
                            .map(|part| part.value.clone())
                            .collect::<Vec<_>>(),
                        current_catalog,
                        current_database,
                    )?,
                }
            }
            StatisticsStatement::ShowBasicStatsMeta(_)
            | StatisticsStatement::ShowHistogramStatsMeta(_)
            | StatisticsStatement::DropStats(_)
            | StatisticsStatement::DropHistogram(_)
            | StatisticsStatement::DropMultipleColumnsStats(_) => {
                return Err(
                    "statistics command is not supported by the statistics application".to_string(),
                );
            }
        };
        self.application
            .execute(command, execution)
            .map_err(|error| error.to_string())
            .and_then(statistics_application_result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{Array, StringArray};
    use uuid::Uuid;

    use super::StatisticsCommandExecutor;
    use crate::statistics_jobs::application::{
        StatisticsApplicationCommand, StatisticsApplicationError, StatisticsApplicationPort,
        StatisticsApplicationResult, StatisticsJobView, StatisticsTableStatView,
        StatisticsTableTarget,
    };

    #[derive(Default)]
    struct RecordingStatisticsApplicationPort {
        commands: Mutex<Vec<StatisticsApplicationCommand>>,
    }

    impl RecordingStatisticsApplicationPort {
        fn commands(&self) -> Vec<StatisticsApplicationCommand> {
            self.commands.lock().expect("statistics commands").clone()
        }
    }

    impl StatisticsApplicationPort for RecordingStatisticsApplicationPort {
        fn execute(
            &self,
            command: StatisticsApplicationCommand,
            _execution: Option<&crate::common::admitted_query_context::QueryExecutionContext>,
        ) -> Result<StatisticsApplicationResult, StatisticsApplicationError> {
            self.commands
                .lock()
                .expect("statistics commands")
                .push(command.clone());
            match command {
                StatisticsApplicationCommand::AnalyzeTable { target, .. } => Ok(
                    StatisticsApplicationResult::JobSubmitted(StatisticsJobView {
                        job_id: Uuid::nil(),
                        operation_id: novarocks_spi::connector::LakePublicationId::new_v7(),
                        state: "SUBMITTED".into(),
                        attempt: 0,
                        target,
                        error_kind: None,
                        error_message: None,
                    }),
                ),
                StatisticsApplicationCommand::ShowAnalyzeJobs
                | StatisticsApplicationCommand::CancelAnalyze { .. } => {
                    Ok(StatisticsApplicationResult::AnalyzeJobs(Vec::new()))
                }
                StatisticsApplicationCommand::ShowTableStats { .. } => {
                    Ok(StatisticsApplicationResult::TableStats(vec![
                        StatisticsTableStatView {
                            metric: "row_count".into(),
                            value: Some("42".into()),
                            status: "AVAILABLE".into(),
                            basis_version: "SAME".into(),
                            source: "PROVIDER_ARTIFACT".into(),
                            numeric_nature: "EXACT".into(),
                            basis_relation: "IDENTICAL".into(),
                        },
                    ]))
                }
            }
        }
    }

    #[test]
    fn typed_statistics_statements_use_the_frontend_application_owner() {
        let port = Arc::new(RecordingStatisticsApplicationPort::default());
        let executor =
            StatisticsCommandExecutor::new(Arc::clone(&port) as Arc<dyn StatisticsApplicationPort>);

        let statements = novarocks_parser::parse("ANALYZE TABLE ice.analytics.orders (order_id)")
            .expect("parse typed analyze");
        let [novarocks_parser::ast::Statement::Statistics(statement)] = statements.as_slice()
        else {
            panic!("expected statistics statement");
        };
        assert!(executor.execute(statement, None, "default", None).is_ok());
        let statements = novarocks_parser::parse("SHOW TABLE STATS ice.analytics.orders")
            .expect("parse typed table stats");
        let [novarocks_parser::ast::Statement::Statistics(statement)] = statements.as_slice()
        else {
            panic!("expected statistics statement");
        };
        let show_stats = executor
            .execute(statement, None, "default", None)
            .expect("show typed table stats");
        let crate::runtime::statement_result::StatementResult::Query(show_stats) = show_stats
        else {
            panic!("SHOW TABLE STATS must return a query result");
        };
        assert_eq!(show_stats.columns[0].name, "metric");
        assert_eq!(show_stats.columns[1].name, "value");
        let value = show_stats.chunks[0].batch.column(1);
        let value = value
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("statistics value string column");
        assert_eq!(value.value(0), "42");

        assert_eq!(
            port.commands(),
            vec![
                StatisticsApplicationCommand::AnalyzeTable {
                    target: StatisticsTableTarget {
                        catalog: "ice".into(),
                        namespace: "analytics".into(),
                        table: "orders".into(),
                    },
                    columns: crate::statistics_jobs::application::StatisticsColumnIntent::Explicit(
                        vec!["order_id".into()],
                    ),
                },
                StatisticsApplicationCommand::ShowTableStats {
                    target: StatisticsTableTarget {
                        catalog: "ice".into(),
                        namespace: "analytics".into(),
                        table: "orders".into(),
                    },
                },
            ]
        );
    }
}
