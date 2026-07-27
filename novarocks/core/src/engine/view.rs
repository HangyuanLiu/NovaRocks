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

//! View application ports and the temporary session-view registry seam.
//!
//! The public traits and DTOs are the dependency-inversion boundary used by
//! `novarocks-frontend`: core exposes only the engine capabilities required by
//! view DDL and rewrite, without leaking `StandaloneState`, connector
//! backends, or parser-internal column definitions. The private `ViewCatalog`
//! below remains temporarily for the staged FEH-4b caller migration.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sqlparser::ast as sqlast;
use sqlparser::parser::Parser;

use crate::engine::iceberg_view;
use crate::engine::{StandaloneState, StatementResult};
use crate::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
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

/// Error returned by [`ViewCatalog::create`]. The caller owns the frozen
/// user-facing message so the trait stays free of formatting policy.
#[derive(Debug)]
pub(crate) enum ViewCatalogError {
    /// The `(db, name)` key already exists and `or_replace` was false.
    AlreadyExists,
}

/// Read/write seam over the session-view registry. Object-safe, `Send + Sync`.
/// Exposes only registry primitives; name normalization, Iceberg-vs-session
/// routing, result ordering and error text stay in the callers.
pub(crate) trait ViewCatalog: Send + Sync {
    /// Atomic full clone of the registry for the pre-analyzer rewrite walk.
    /// Mirrors the single read-lock snapshot the rewrite used to take.
    fn snapshot(&self) -> HashMap<(String, String), Box<sqlast::Query>>;

    /// Session view names whose database key equals `db` (already normalized
    /// by the caller). Unsorted; the caller sorts to match SHOW VIEWS output.
    fn list_in_database(&self, db: &str) -> Vec<String>;

    /// Atomic check-and-insert. Returns `Err(AlreadyExists)` when the key is
    /// present and `!or_replace`; otherwise inserts/replaces.
    fn create(
        &self,
        db: String,
        name: String,
        query: Box<sqlast::Query>,
        or_replace: bool,
    ) -> Result<(), ViewCatalogError>;

    /// Remove `(db, name)`. Absent key is a silent no-op (matches the previous
    /// unconditional `remove` that ignored `IF EXISTS`).
    fn remove(&self, db: &str, name: &str);

    /// Remove every entry whose database key equals `db` (caller passes the
    /// already-normalized database name).
    fn remove_database(&self, db: &str);
}

/// Core, process-memory `ViewCatalog`. Wraps the same `RwLock<HashMap>` the
/// registry used before FEH-4a. Replaced by a frontend impl in FEH-4b.
#[derive(Default)]
pub(crate) struct InMemoryViewCatalog {
    views: RwLock<HashMap<(String, String), Box<sqlast::Query>>>,
}

impl InMemoryViewCatalog {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl ViewCatalog for InMemoryViewCatalog {
    fn snapshot(&self) -> HashMap<(String, String), Box<sqlast::Query>> {
        self.views.read().expect("view registry read lock").clone()
    }

    fn list_in_database(&self, db: &str) -> Vec<String> {
        self.views
            .read()
            .expect("view registry read lock")
            .keys()
            .filter(|(database, _)| database == db)
            .map(|(_, view)| view.clone())
            .collect()
    }

    fn create(
        &self,
        db: String,
        name: String,
        query: Box<sqlast::Query>,
        or_replace: bool,
    ) -> Result<(), ViewCatalogError> {
        let mut views = self.views.write().expect("view registry write lock");
        if views.contains_key(&(db.clone(), name.clone())) && !or_replace {
            return Err(ViewCatalogError::AlreadyExists);
        }
        views.insert((db, name), query);
        Ok(())
    }

    fn remove(&self, db: &str, name: &str) {
        self.views
            .write()
            .expect("view registry write lock")
            .remove(&(db.to_string(), name.to_string()));
    }

    fn remove_database(&self, db: &str) {
        self.views
            .write()
            .expect("view registry write lock")
            .retain(|(view_db, _), _| view_db != db);
    }
}

pub(crate) fn try_handle_statement(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Option<StatementResult>, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("create view ") || lower.starts_with("create or replace view ") {
        return handle_create_view(state, trimmed, current_catalog, current_database).map(Some);
    }
    if lower.starts_with("drop view ") {
        return handle_drop_view(state, trimmed, current_catalog, current_database).map(Some);
    }
    Ok(None)
}

pub(crate) fn handle_show_create_view(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let view_name = crate::engine::statement::parse_show_create_view(sql)?;
    let Some(target) = crate::engine::iceberg_view::resolve_iceberg_view_target_parts(
        state,
        &view_name.parts,
        current_catalog,
        current_database,
    )?
    else {
        return Err("SHOW CREATE VIEW only supports views in iceberg catalogs".to_string());
    };
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend("iceberg")?;
    let view = backend.load_view(&target.catalog, &target.namespace, &target.view)?;

    let columns = view
        .column_names
        .iter()
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut ddl = format!(
        "CREATE VIEW `{}`.`{}`.`{}` ({})",
        target.catalog, target.namespace, target.view, columns
    );
    if let Some(comment) = &view.comment {
        ddl.push_str(&format!("\nCOMMENT \"{}\"", comment.replace('"', "\\\"")));
    }
    ddl.push_str(&format!("\nAS {};", view.sql));

    let fields = vec![
        Field::new("View", DataType::Utf8, false),
        Field::new("Create View", DataType::Utf8, false),
    ];
    let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(StringArray::from(vec![target.view.clone()])),
        Arc::new(StringArray::from(vec![ddl])),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("build SHOW CREATE VIEW result failed: {e}"))?;
    Ok(StatementResult::Query(QueryResult {
        columns: vec![
            QueryResultColumn {
                name: "View".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                logical_type: None,
            },
            QueryResultColumn {
                name: "Create View".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                logical_type: None,
            },
        ],
        chunks: vec![record_batch_to_chunk(batch)?],
    }))
}

pub(crate) fn handle_show_views(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let from_db = crate::engine::statement::parse_show_views(sql)?;
    let db = from_db.as_deref().unwrap_or(current_database);
    let session_catalog =
        current_catalog.filter(|catalog| !catalog.eq_ignore_ascii_case("default_catalog"));
    let names: Vec<String> = match session_catalog {
        Some(catalog) => {
            let backend = state
                .connectors
                .read()
                .expect("connector registry read")
                .catalog_backend("iceberg")?;
            backend.list_views(catalog, db)?
        }
        None => {
            let db_lower = db.to_ascii_lowercase();
            let mut names = state.session_views.list_in_database(&db_lower);
            names.sort();
            names
        }
    };
    let column_name = format!("Views_in_{db}");
    let fields = vec![Field::new(column_name.clone(), DataType::Utf8, false)];
    let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![Arc::new(StringArray::from(names))];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("build SHOW VIEWS result failed: {e}"))?;
    Ok(StatementResult::Query(QueryResult {
        columns: vec![QueryResultColumn {
            name: column_name,
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        }],
        chunks: vec![record_batch_to_chunk(batch)?],
    }))
}

/// Handle `CREATE VIEW [IF NOT EXISTS] [db.]name AS <query>` by parsing
/// the trailing query AST and registering it in the in-memory view
/// registry on `StandaloneState`. The view is later expanded inline by
/// the analyzer whenever a `FROM <view>` reference resolves to this
/// name. Views live for the lifetime of the standalone process.
fn handle_create_view(
    state: &Arc<StandaloneState>,
    trimmed: &str,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let dialect = ViewSqlDialect;
    let mut parser = Parser::new(&dialect)
        .try_with_sql(trimmed)
        .map_err(|e| format!("CREATE VIEW parse error: {e}"))?;
    let stmt = parser
        .parse_statement()
        .map_err(|e| format!("CREATE VIEW parse error: {e}"))?;
    let sqlparser::ast::Statement::CreateView(create_view) = stmt else {
        return Err("CREATE VIEW: failed to parse statement".to_string());
    };
    // A 3-part name, or a 1/2-part name under an active `SET CATALOG`, routes
    // to the iceberg REST backend. `default_catalog` names fall through to the
    // existing session-view registration below.
    if let Some(target) = iceberg_view::resolve_iceberg_view_target(
        state,
        &create_view.name,
        current_catalog,
        current_database,
    )? {
        return iceberg_view::create_iceberg_view(state, &target, create_view);
    }
    let (db, name) = view_name_parts(&create_view.name, current_database)?;
    state
        .session_views
        .create(
            db.clone(),
            name.clone(),
            create_view.query,
            create_view.or_replace,
        )
        .map_err(|ViewCatalogError::AlreadyExists| format!("view already exists: {db}.{name}"))?;
    Ok(StatementResult::Ok)
}

/// Handle `DROP VIEW [IF EXISTS] [db.]name` by removing the matching
/// entry from the in-memory view registry.
fn handle_drop_view(
    state: &Arc<StandaloneState>,
    trimmed: &str,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let dialect = ViewSqlDialect;
    let mut parser = Parser::new(&dialect)
        .try_with_sql(trimmed)
        .map_err(|e| format!("DROP VIEW parse error: {e}"))?;
    let stmt = parser
        .parse_statement()
        .map_err(|e| format!("DROP VIEW parse error: {e}"))?;
    let sqlparser::ast::Statement::Drop {
        object_type: sqlparser::ast::ObjectType::View,
        names,
        if_exists,
        ..
    } = stmt
    else {
        return Err("DROP VIEW: failed to parse statement".to_string());
    };
    for name in names {
        if let Some(target) = iceberg_view::resolve_iceberg_view_target(
            state,
            &name,
            current_catalog,
            current_database,
        )? {
            iceberg_view::drop_iceberg_view(state, &target, if_exists)?;
            continue;
        }
        let (db, view) = view_name_parts(&name, current_database)?;
        state.session_views.remove(&db, &view);
    }
    Ok(StatementResult::Ok)
}

fn view_name_parts(
    name: &sqlparser::ast::ObjectName,
    current_database: &str,
) -> Result<(String, String), String> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .collect();
    let (db, view) = match parts.as_slice() {
        [view] => (current_database.to_string(), view.clone()),
        [db, view] => (db.clone(), view.clone()),
        [_cat, db, view] => (db.clone(), view.clone()),
        _ => return Err(format!("invalid view name: {name}")),
    };
    Ok((db.to_lowercase(), view.to_lowercase()))
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
        self.connectors
            .read()
            .expect("connector registry read")
            .catalog_backend("iceberg")?
            .table_exists(&target.catalog, &target.database, &target.view)
    }

    fn view_exists(&self, target: &ViewTarget) -> Result<bool, String> {
        self.connectors
            .read()
            .expect("connector registry read")
            .catalog_backend("iceberg")?
            .view_exists(&target.catalog, &target.database, &target.view)
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
        let result = self
            .connectors
            .read()
            .expect("connector registry read")
            .catalog_backend("iceberg")?
            .load_view(&target.catalog, &target.database, &target.view);
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
        self.connectors
            .read()
            .expect("connector registry read")
            .catalog_backend("iceberg")?
            .list_views(catalog, database)
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

    #[test]
    fn create_inserts_then_snapshot_returns_it() {
        let c = InMemoryViewCatalog::new();
        c.create("db".into(), "v".into(), parse_query("SELECT 1 AS a"), false)
            .expect("insert");
        let snap = c.snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap.contains_key(&("db".to_string(), "v".to_string())));
    }

    #[test]
    fn create_rejects_existing_without_or_replace() {
        let c = InMemoryViewCatalog::new();
        c.create("db".into(), "v".into(), parse_query("SELECT 1"), false)
            .expect("first insert");
        let err = c.create("db".into(), "v".into(), parse_query("SELECT 2"), false);
        assert!(matches!(err, Err(ViewCatalogError::AlreadyExists)));
        // unchanged body
        assert_eq!(
            c.snapshot()[&("db".into(), "v".into())].to_string(),
            "SELECT 1"
        );
    }

    #[test]
    fn create_or_replace_overwrites() {
        let c = InMemoryViewCatalog::new();
        c.create("db".into(), "v".into(), parse_query("SELECT 1"), false)
            .expect("first");
        c.create("db".into(), "v".into(), parse_query("SELECT 2"), true)
            .expect("replace");
        assert_eq!(
            c.snapshot()[&("db".into(), "v".into())].to_string(),
            "SELECT 2"
        );
    }

    #[test]
    fn remove_is_silent_noop_when_absent() {
        let c = InMemoryViewCatalog::new();
        c.remove("db", "missing"); // must not panic
        c.create("db".into(), "v".into(), parse_query("SELECT 1"), false)
            .unwrap();
        c.remove("db", "v");
        assert!(c.snapshot().is_empty());
    }

    #[test]
    fn remove_database_only_drops_matching_db() {
        let c = InMemoryViewCatalog::new();
        c.create("a".into(), "v".into(), parse_query("SELECT 1"), false)
            .unwrap();
        c.create("b".into(), "v".into(), parse_query("SELECT 1"), false)
            .unwrap();
        c.remove_database("a");
        let snap = c.snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap.contains_key(&("b".to_string(), "v".to_string())));
    }

    #[test]
    fn list_in_database_returns_matching_views_unsorted() {
        let c = InMemoryViewCatalog::new();
        c.create("db".into(), "v2".into(), parse_query("SELECT 1"), false)
            .unwrap();
        c.create("db".into(), "v1".into(), parse_query("SELECT 1"), false)
            .unwrap();
        c.create("other".into(), "x".into(), parse_query("SELECT 1"), false)
            .unwrap();
        let mut names = c.list_in_database("db");
        names.sort();
        assert_eq!(names, vec!["v1".to_string(), "v2".to_string()]);
    }
}
