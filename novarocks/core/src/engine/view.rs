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

//! View application ports and core engine adapter.
//!
//! The public traits and DTOs are the dependency-inversion boundary used by
//! `novarocks-frontend`: core exposes only the engine capabilities required by
//! view DDL and rewrite, without leaking `StandaloneState`, connector
//! backends, or parser-internal column definitions.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::engine::StandaloneState;
use crate::runtime::query_result::QueryResult;
/// Shared StarRocks SQL parser contract for view DDL, storage, and rewrite.
pub use crate::sql::parser::dialect::StarRocksDialect as ViewSqlDialect;

#[derive(Clone, Copy, Debug)]
pub struct ViewRequestContext<'a> {
    pub current_catalog: Option<&'a str>,
    pub current_database: &'a str,
}

#[derive(Clone, Debug)]
pub enum ViewStatementResult {
    Ok,
    Query(QueryResult),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ViewTarget {
    pub catalog: String,
    pub database: String,
    pub view: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ViewColumnDefinition {
    pub name: String,
    pub data_type: sqlparser::ast::DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateExternalViewRequest {
    pub target: ViewTarget,
    pub columns: Vec<ViewColumnDefinition>,
    pub sql: String,
    pub comment: Option<String>,
    pub or_replace: bool,
    pub properties: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedExternalView {
    pub sql: String,
    pub dialect: String,
    pub default_database: String,
    pub column_names: Vec<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
}

pub trait ViewService: Send + Sync {
    fn try_handle_statement(
        &self,
        engine: &dyn ViewEngine,
        sql: &str,
        context: ViewRequestContext<'_>,
    ) -> Result<Option<ViewStatementResult>, String>;

    fn rewrite_query(
        &self,
        engine: &dyn ViewEngine,
        query: &mut sqlparser::ast::Query,
        context: ViewRequestContext<'_>,
    ) -> Result<(), String>;

    fn drop_database(&self, catalog: &str, database: &str) -> Result<(), String>;
}

pub trait ViewEngine: Send + Sync {
    fn validate_iceberg_catalog(&self, catalog: &str) -> Result<(), String>;
    fn is_rest_iceberg_catalog(&self, catalog: &str) -> bool;
    fn table_exists(&self, target: &ViewTarget) -> Result<bool, String>;
    fn view_exists(&self, target: &ViewTarget) -> Result<bool, String>;
    fn create_external_view(&self, request: CreateExternalViewRequest) -> Result<(), String>;
    fn drop_external_view(&self, target: &ViewTarget) -> Result<(), String>;
    fn load_external_view(
        &self,
        target: &ViewTarget,
    ) -> Result<Option<ResolvedExternalView>, String>;
    fn list_external_views(&self, catalog: &str, database: &str) -> Result<Vec<String>, String>;
    fn analyze_external_view(
        &self,
        catalog: &str,
        database: &str,
        query: &sqlparser::ast::Query,
    ) -> Result<Vec<ViewColumnDefinition>, String>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyViewService;

impl ViewService for EmptyViewService {
    fn try_handle_statement(
        &self,
        _engine: &dyn ViewEngine,
        sql: &str,
        _context: ViewRequestContext<'_>,
    ) -> Result<Option<ViewStatementResult>, String> {
        let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
        if normalized.starts_with("create view ")
            || normalized.starts_with("create or replace view ")
            || normalized.starts_with("drop view ")
            || normalized.starts_with("show create view ")
            || normalized == "show views"
            || normalized.starts_with("show views ")
        {
            return Err("view service is not injected".to_string());
        }
        Ok(None)
    }

    fn rewrite_query(
        &self,
        _engine: &dyn ViewEngine,
        _query: &mut sqlparser::ast::Query,
        _context: ViewRequestContext<'_>,
    ) -> Result<(), String> {
        Ok(())
    }

    fn drop_database(&self, _catalog: &str, _database: &str) -> Result<(), String> {
        Ok(())
    }
}

impl ViewEngine for StandaloneState {
    fn validate_iceberg_catalog(&self, catalog: &str) -> Result<(), String> {
        self.iceberg_catalogs
            .read()
            .map_err(|error| format!("iceberg catalog registry read lock: {error}"))?
            .get(catalog)
            .map(|_| ())
    }

    fn is_rest_iceberg_catalog(&self, catalog: &str) -> bool {
        self.iceberg_catalogs
            .read()
            .expect("iceberg catalog registry read lock")
            .get(catalog)
            .is_ok_and(|entry| entry.rest_uri.is_some())
    }

    fn table_exists(&self, target: &ViewTarget) -> Result<bool, String> {
        crate::connector::metadata_table_exists(
            &self.connectors.read().expect("connector registry read"),
            crate::connector::query_request_context(None)?,
            &target.catalog,
            &target.database,
            &target.view,
        )
    }

    fn view_exists(&self, target: &ViewTarget) -> Result<bool, String> {
        let registry = self
            .iceberg_catalogs
            .read()
            .map_err(|error| format!("iceberg catalog registry read lock: {error}"))?;
        crate::connector::iceberg::catalog::views::view_exists(
            &registry.get(&target.catalog)?,
            &target.database,
            &target.view,
        )
    }

    fn create_external_view(&self, request: CreateExternalViewRequest) -> Result<(), String> {
        let columns = request
            .columns
            .into_iter()
            .map(|column| {
                Ok(crate::sql::parser::ast::TableColumnDef {
                    name: column.name,
                    data_type: crate::sql::parser::dialect::convert_sql_type(column.data_type)?,
                    nullable: column.nullable,
                    aggregation: None,
                    default: None,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        self.connectors
            .read()
            .expect("connector registry read")
            .catalog_backend("iceberg")?
            .create_view(crate::connector::backend::CreateViewRequest {
                catalog: request.target.catalog,
                namespace: request.target.database,
                view: request.target.view,
                columns,
                view_sql: request.sql,
                comment: request.comment,
                or_replace: request.or_replace,
                properties: request.properties,
            })
    }

    fn drop_external_view(&self, target: &ViewTarget) -> Result<(), String> {
        self.connectors
            .read()
            .expect("connector registry read")
            .catalog_backend("iceberg")?
            .drop_view(&target.catalog, &target.database, &target.view)
    }

    fn load_external_view(
        &self,
        target: &ViewTarget,
    ) -> Result<Option<ResolvedExternalView>, String> {
        let registry = self
            .iceberg_catalogs
            .read()
            .map_err(|error| format!("iceberg catalog registry read lock: {error}"))?;
        let result = crate::connector::iceberg::catalog::views::load_view(
            &registry.get(&target.catalog)?,
            &target.database,
            &target.view,
        );
        match result {
            Ok(view) => Ok(Some(ResolvedExternalView {
                sql: view.sql,
                dialect: view.dialect,
                default_database: view.default_namespace,
                column_names: view.column_names,
                comment: view.comment,
                properties: view.properties,
            })),
            Err(error) if error.contains("unknown view") => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn list_external_views(&self, catalog: &str, database: &str) -> Result<Vec<String>, String> {
        let registry = self
            .iceberg_catalogs
            .read()
            .map_err(|error| format!("iceberg catalog registry read lock: {error}"))?;
        crate::connector::iceberg::catalog::views::list_views(&registry.get(catalog)?, database)
    }

    fn analyze_external_view(
        &self,
        catalog: &str,
        database: &str,
        query: &sqlparser::ast::Query,
    ) -> Result<Vec<ViewColumnDefinition>, String> {
        let catalog_service_snapshot = crate::sql::catalog::StandaloneCatalogService::new(
            Arc::new(RwLock::new(self.catalog_service.local_snapshot())),
            self.catalog_service.registry_snapshot(),
        );
        let connectors_snapshot = self
            .connectors
            .read()
            .expect("standalone connector registry read lock")
            .clone();
        let provider = crate::engine::build_catalog_service_provider(
            Some(catalog),
            &catalog_service_snapshot,
            &connectors_snapshot,
            crate::connector::query_request_context(None)?,
            crate::sql::catalog::TableLookupMode::SchemaOnly,
        );
        let (resolved, _ctes, _factory) = crate::sql::analyzer::analyze(query, &provider, database)
            .map_err(|error| format!("analyze view definition failed: {error}"))?;
        let columns = resolved
            .output_columns
            .into_iter()
            .filter(|column| !column.is_internal)
            .map(|column| {
                Ok(ViewColumnDefinition {
                    name: column.name,
                    data_type: view_sqlparser_data_type(&column.data_type)?,
                    nullable: column.nullable,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        if columns.is_empty() {
            return Err("CREATE VIEW: SELECT produced no output columns".to_string());
        }
        Ok(columns)
    }
}

fn view_sqlparser_data_type(
    data_type: &arrow::datatypes::DataType,
) -> Result<sqlparser::ast::DataType, String> {
    use novarocks_catalog::schema::SqlType;
    use sqlparser::ast::{
        ArrayElemTypeDef, DataType, Ident, ObjectName, ObjectNamePart, StructBracketKind,
        StructField, TimezoneInfo,
    };

    fn custom(name: &str, modifiers: Vec<String>) -> DataType {
        DataType::Custom(
            ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
            modifiers,
        )
    }

    fn convert(data_type: SqlType) -> DataType {
        match data_type {
            SqlType::TinyInt => DataType::TinyInt(None),
            SqlType::SmallInt => DataType::SmallInt(None),
            SqlType::Int => DataType::Int(None),
            SqlType::BigInt => DataType::BigInt(None),
            SqlType::LargeInt => custom("LARGEINT", vec![]),
            SqlType::Float => DataType::Float(sqlparser::ast::ExactNumberInfo::None),
            SqlType::Double => DataType::Double(sqlparser::ast::ExactNumberInfo::None),
            SqlType::Decimal { precision, scale } => {
                custom("DECIMAL128", vec![precision.to_string(), scale.to_string()])
            }
            SqlType::String => DataType::String(None),
            SqlType::Json => DataType::JSON,
            SqlType::Binary => DataType::Varbinary(None),
            SqlType::Bitmap => custom("BITMAP", vec![]),
            SqlType::Hll => custom("HLL", vec![]),
            SqlType::Boolean => DataType::Boolean,
            SqlType::Date => DataType::Date,
            SqlType::DateTime => DataType::Datetime(None),
            SqlType::DateTimeNs => custom("DATETIME_NS", vec![]),
            SqlType::Time => DataType::Time(None, TimezoneInfo::None),
            SqlType::Array(element) => {
                DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(convert(*element))))
            }
            SqlType::Map(key, value) => {
                DataType::Map(Box::new(convert(*key)), Box::new(convert(*value)))
            }
            SqlType::Struct(fields) => DataType::Struct(
                fields
                    .into_iter()
                    .map(|(name, field_type)| StructField {
                        field_name: Some(Ident::new(name)),
                        field_type: convert(field_type),
                        options: None,
                    })
                    .collect(),
                StructBracketKind::AngleBrackets,
            ),
            SqlType::Variant => custom("VARIANT", vec![]),
        }
    }

    Ok(convert(
        crate::engine::iceberg_ctas::arrow_data_type_to_sql_type(data_type)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::dialect::StarRocksDialect;
    use sqlparser::ast as sqlast;
    use sqlparser::parser::Parser;

    #[derive(Default)]
    struct FakeViewEngine;

    impl ViewEngine for FakeViewEngine {
        fn validate_iceberg_catalog(&self, _catalog: &str) -> Result<(), String> {
            unreachable!("empty view service must not access the engine")
        }

        fn is_rest_iceberg_catalog(&self, _catalog: &str) -> bool {
            unreachable!("empty view service must not access the engine")
        }

        fn table_exists(&self, _target: &ViewTarget) -> Result<bool, String> {
            unreachable!("empty view service must not access the engine")
        }

        fn view_exists(&self, _target: &ViewTarget) -> Result<bool, String> {
            unreachable!("empty view service must not access the engine")
        }

        fn create_external_view(&self, _request: CreateExternalViewRequest) -> Result<(), String> {
            unreachable!("empty view service must not access the engine")
        }

        fn drop_external_view(&self, _target: &ViewTarget) -> Result<(), String> {
            unreachable!("empty view service must not access the engine")
        }

        fn load_external_view(
            &self,
            _target: &ViewTarget,
        ) -> Result<Option<ResolvedExternalView>, String> {
            unreachable!("empty view service must not access the engine")
        }

        fn list_external_views(
            &self,
            _catalog: &str,
            _database: &str,
        ) -> Result<Vec<String>, String> {
            unreachable!("empty view service must not access the engine")
        }

        fn analyze_external_view(
            &self,
            _catalog: &str,
            _database: &str,
            _query: &sqlast::Query,
        ) -> Result<Vec<ViewColumnDefinition>, String> {
            unreachable!("empty view service must not access the engine")
        }
    }

    fn parse_query(sql: &str) -> Box<sqlast::Query> {
        let mut parser = Parser::new(&StarRocksDialect).try_with_sql(sql).unwrap();
        match parser.parse_statement().unwrap() {
            sqlast::Statement::Query(q) => q,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn empty_view_service_rejects_view_ddl_but_leaves_queries_unchanged() {
        let service: Arc<dyn ViewService> = Arc::new(EmptyViewService);
        let engine = FakeViewEngine;
        let ctx = ViewRequestContext {
            current_catalog: None,
            current_database: "db",
        };
        assert!(
            service
                .try_handle_statement(&engine, "CREATE VIEW v AS SELECT 1", ctx)
                .unwrap_err()
                .contains("view service is not injected")
        );
        let mut query = parse_query("SELECT * FROM t");
        service.rewrite_query(&engine, &mut query, ctx).unwrap();
        assert_eq!(query.to_string(), "SELECT * FROM t");
    }
}
