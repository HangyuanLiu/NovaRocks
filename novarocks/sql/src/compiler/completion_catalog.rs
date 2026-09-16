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

//! Owned catalog completion state and its fact-only analyzer catalog.

use std::collections::{HashMap, HashSet};

use novarocks_parser::ast::{self, Visit};
use novarocks_types::naming::{TableIdentity, normalize_identifier, resolve_catalog_table_name};

use super::SqlCompileError;
use super::completion::{
    CatalogLookupTarget, CatalogRelationFact, CatalogRelationNeed, CatalogRelationOutcome,
    CompileNeedId,
};
use crate::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable, TableLookupMode,
};
use crate::planner::table::{ScanSource, SqlMetadataTableKind, SqlScanKind};

/// The move-only catalog phase of one SQL compilation.
///
/// The query has already passed SQL-owned pre-analysis, including recursive
/// CTE unrolling. The state owns every value needed to resume analysis and
/// contains no application catalog, callback, or provider object.
pub(super) struct CatalogCompletionState {
    query: Box<ast::Query>,
    current_catalog: Option<Box<str>>,
    current_database: Box<str>,
    lookup_mode: TableLookupMode,
    needs: Box<[CatalogRelationNeed]>,
    next_need_ordinal: u32,
}

impl CatalogCompletionState {
    pub(super) fn try_new(
        query: ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        lookup_mode: TableLookupMode,
        first_need_ordinal: u32,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_with_additional_queries(
            query,
            &[],
            &[],
            current_catalog,
            current_database,
            lookup_mode,
            first_need_ordinal,
        )
    }

    pub(super) fn try_new_with_additional_queries(
        query: ast::Query,
        additional_queries: &[ast::Query],
        additional_relations: &[TableIdentity],
        current_catalog: Option<&str>,
        current_database: &str,
        lookup_mode: TableLookupMode,
        first_need_ordinal: u32,
    ) -> Result<Self, SqlCompileError> {
        let query =
            crate::analyzer::query_prepass::preanalyze(query).map_err(SqlCompileError::Analyze)?;
        let current_catalog = current_catalog
            .map(normalize_identifier)
            .transpose()
            .map_err(SqlCompileError::Compilation)?
            .map(String::into_boxed_str);
        let current_database = normalize_identifier(current_database)
            .map_err(SqlCompileError::Compilation)?
            .into_boxed_str();
        let mut collector = CatalogNeedCollector::new(
            current_catalog.as_deref(),
            &current_database,
            lookup_mode,
            first_need_ordinal,
        );
        collector.collect_query(&query, &CteScope::default())?;
        for additional in additional_queries {
            let additional = crate::analyzer::query_prepass::preanalyze(additional.clone())
                .map_err(SqlCompileError::Analyze)?;
            collector.collect_query(&additional, &CteScope::default())?;
        }
        for relation in additional_relations {
            collector.add_need(
                relation.clone(),
                CatalogLookupTarget::Table { mode: lookup_mode },
            )?;
        }
        let (needs, next_need_ordinal) = collector.finish();
        Ok(Self {
            query: Box::new(query),
            current_catalog,
            current_database,
            lookup_mode,
            needs,
            next_need_ordinal,
        })
    }

    pub(super) fn query(&self) -> &ast::Query {
        &self.query
    }

    pub(super) fn needs(&self) -> &[CatalogRelationNeed] {
        &self.needs
    }

    pub(super) const fn next_need_ordinal(&self) -> u32 {
        self.next_need_ordinal
    }

    pub(super) fn fact_catalog(
        &self,
        facts: &[CatalogRelationFact],
    ) -> Result<FactBackedCatalog, SqlCompileError> {
        FactBackedCatalog::try_new(
            self.current_catalog.as_deref(),
            &self.current_database,
            self.lookup_mode,
            &self.needs,
            facts,
        )
    }
}

#[derive(Clone, Default)]
struct CteScope {
    visible: HashSet<String>,
    pending: HashSet<String>,
}

struct CatalogNeedCollector<'a> {
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    lookup_mode: TableLookupMode,
    next_need_ordinal: u32,
    keys: HashSet<CatalogFactKey>,
    needs: Vec<CatalogRelationNeed>,
}

impl<'a> CatalogNeedCollector<'a> {
    fn new(
        current_catalog: Option<&'a str>,
        current_database: &'a str,
        lookup_mode: TableLookupMode,
        first_need_ordinal: u32,
    ) -> Self {
        Self {
            current_catalog,
            current_database,
            lookup_mode,
            next_need_ordinal: first_need_ordinal,
            keys: HashSet::new(),
            needs: Vec::new(),
        }
    }

    fn finish(self) -> (Box<[CatalogRelationNeed]>, u32) {
        (self.needs.into_boxed_slice(), self.next_need_ordinal)
    }

    fn collect_query(
        &mut self,
        query: &ast::Query,
        outer_scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        let mut body_scope = outer_scope.clone();
        if let Some(with) = &query.with {
            let local_names = with
                .ctes
                .iter()
                .map(|cte| cte.name.value.to_ascii_lowercase())
                .collect::<Vec<_>>();
            for name in &local_names {
                body_scope.pending.insert(name.clone());
            }
            for cte in &with.ctes {
                let name = cte.name.value.to_ascii_lowercase();
                body_scope.pending.remove(&name);
                self.collect_query(&cte.query, &body_scope)?;
                body_scope.visible.insert(name);
            }
        }

        self.collect_set_expr(&query.body, &body_scope)?;
        for order in &query.order_by {
            self.collect_expr(&order.expr, &body_scope)?;
        }
        if let Some(limit) = &query.limit {
            self.collect_expr(limit, &body_scope)?;
        }
        if let Some(offset) = &query.offset {
            self.collect_expr(&offset.value, &body_scope)?;
        }
        if let Some(quantity) = query
            .fetch
            .as_ref()
            .and_then(|fetch| fetch.quantity.as_ref())
        {
            self.collect_expr(quantity, &body_scope)?;
        }
        Ok(())
    }

    fn collect_set_expr(
        &mut self,
        expression: &ast::SetExpr,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        match expression {
            ast::SetExpr::Select(select) => self.collect_select(select, scope),
            ast::SetExpr::Values(values) => {
                for row in &values.rows {
                    for expression in row {
                        self.collect_expr(expression, scope)?;
                    }
                }
                Ok(())
            }
            ast::SetExpr::Query(query) => self.collect_query(query, scope),
            ast::SetExpr::SetOperation(operation) => {
                self.collect_set_expr(&operation.left, scope)?;
                self.collect_set_expr(&operation.right, scope)
            }
        }
    }

    fn collect_select(
        &mut self,
        select: &ast::Select,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        for hint in &select.hints {
            match &hint.value {
                ast::SelectHintValue::Bare => {}
                ast::SelectHintValue::Call { arguments } => {
                    for argument in arguments {
                        self.collect_expr(argument, scope)?;
                    }
                }
                ast::SelectHintValue::Assignment { value } => self.collect_expr(value, scope)?,
            }
        }
        if let ast::SelectQuantifier::Distinct { on, .. } = &select.quantifier {
            for expression in on {
                self.collect_expr(expression, scope)?;
            }
        }
        for item in &select.projection {
            match item {
                ast::SelectItem::UnnamedExpr(expression)
                | ast::SelectItem::ExprWithAlias {
                    expr: expression, ..
                } => {
                    self.collect_expr(expression, scope)?;
                }
                ast::SelectItem::Wildcard { options, .. }
                | ast::SelectItem::QualifiedWildcard { options, .. } => {
                    for replacement in &options.replace {
                        self.collect_expr(&replacement.expr, scope)?;
                    }
                }
            }
        }
        for relation in &select.from {
            self.collect_table_with_joins(relation, scope)?;
        }
        if let Some(selection) = &select.selection {
            self.collect_expr(selection, scope)?;
        }
        self.collect_group_by(&select.group_by, scope)?;
        if let Some(having) = &select.having {
            self.collect_expr(having, scope)?;
        }
        if let Some(qualify) = &select.qualify {
            self.collect_expr(qualify, scope)?;
        }
        for window in &select.windows {
            self.collect_window_spec(&window.specification, scope)?;
        }
        Ok(())
    }

    fn collect_group_by(
        &mut self,
        group_by: &ast::GroupBy,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        let groups: &[Vec<ast::Expr>] = match group_by {
            ast::GroupBy::None => return Ok(()),
            ast::GroupBy::Expressions { expressions, .. }
            | ast::GroupBy::Rollup { expressions, .. }
            | ast::GroupBy::Cube { expressions, .. } => std::slice::from_ref(expressions),
            ast::GroupBy::GroupingSets { sets, .. } => sets,
        };
        for group in groups {
            for expression in group {
                self.collect_expr(expression, scope)?;
            }
        }
        Ok(())
    }

    fn collect_window_spec(
        &mut self,
        window: &ast::WindowSpec,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        for expression in &window.partition_by {
            self.collect_expr(expression, scope)?;
        }
        for order in &window.order_by {
            self.collect_expr(&order.expr, scope)?;
        }
        if let Some(frame) = &window.window_frame {
            self.collect_window_bound(&frame.start_bound, scope)?;
            if let Some(end) = &frame.end_bound {
                self.collect_window_bound(end, scope)?;
            }
        }
        Ok(())
    }

    fn collect_window_bound(
        &mut self,
        bound: &ast::WindowFrameBound,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        match bound {
            ast::WindowFrameBound::CurrentRow(_) => Ok(()),
            ast::WindowFrameBound::Preceding(expression, _)
            | ast::WindowFrameBound::Following(expression, _) => {
                if let Some(expression) = expression {
                    self.collect_expr(expression, scope)?;
                }
                Ok(())
            }
        }
    }

    fn collect_table_with_joins(
        &mut self,
        relation: &ast::TableWithJoins,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        self.collect_table_factor(&relation.relation, scope)?;
        for join in &relation.joins {
            self.collect_table_factor(&join.relation, scope)?;
            if let ast::JoinConstraint::On(expression) = &join.constraint {
                self.collect_expr(expression, scope)?;
            }
        }
        Ok(())
    }

    fn collect_table_factor(
        &mut self,
        factor: &ast::TableFactor,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        match factor {
            ast::TableFactor::Table {
                name,
                metadata,
                version,
                hints,
                ..
            } => {
                if metadata.is_none() && name.parts.len() == 1 {
                    let name = name.parts[0].value.to_ascii_lowercase();
                    if scope.pending.contains(&name) {
                        return Err(SqlCompileError::Compilation(format!(
                            "forward CTE reference is not supported: {name}"
                        )));
                    }
                    if scope.visible.contains(&name) {
                        self.collect_table_decorations(version.as_ref(), hints, scope)?;
                        return Ok(());
                    }
                }
                let relation = self.resolve_name(name)?;
                let target = match metadata {
                    Some(metadata) => CatalogLookupTarget::IcebergMetadata {
                        kind: SqlMetadataTableKind::parse(&metadata.value)
                            .map_err(SqlCompileError::Compilation)?,
                    },
                    None => CatalogLookupTarget::Table {
                        mode: self.lookup_mode,
                    },
                };
                self.add_need(relation, target)?;
                self.collect_table_decorations(version.as_ref(), hints, scope)
            }
            ast::TableFactor::Derived {
                subquery, hints, ..
            } => {
                self.collect_query(subquery, scope)?;
                self.collect_hints(hints, scope)
            }
            ast::TableFactor::TableFunction { expr, hints, .. } => {
                self.collect_internal_table_function_need(expr)?;
                self.collect_expr(expr, scope)?;
                self.collect_hints(hints, scope)
            }
            ast::TableFactor::Unnest { array_exprs, .. } => {
                for expression in array_exprs {
                    self.collect_expr(expression, scope)?;
                }
                Ok(())
            }
            ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.collect_table_with_joins(table_with_joins, scope),
        }
    }

    fn collect_table_decorations(
        &mut self,
        version: Option<&ast::TableVersion>,
        hints: &[ast::TableHint],
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        if let Some(version) = version {
            self.collect_expr(&version.value, scope)?;
        }
        self.collect_hints(hints, scope)
    }

    fn collect_hints(
        &mut self,
        hints: &[ast::TableHint],
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        for hint in hints {
            for argument in &hint.arguments {
                self.collect_expr(argument, scope)?;
            }
            if let Some(target) = &hint.target {
                self.collect_expr(target, scope)?;
            }
        }
        Ok(())
    }

    fn collect_expr(
        &mut self,
        expression: &ast::Expr,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        let mut visitor = ExpressionQueryCollector {
            collector: self,
            scope,
            error: None,
        };
        visitor.visit_expr(expression);
        match visitor.error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn collect_internal_table_function_need(
        &mut self,
        expression: &ast::Expr,
    ) -> Result<(), SqlCompileError> {
        let ast::Expr::FunctionCall(function) = expression else {
            return Ok(());
        };
        let Some(name) = function.name.parts.last() else {
            return Ok(());
        };
        if !name.value.eq_ignore_ascii_case("__nr_ivm_delta") {
            return Ok(());
        }
        let Some(ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::String(identity),
            ..
        })) = function.arguments.first()
        else {
            return Ok(());
        };
        let parts = identity
            .split('.')
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let relation = resolve_catalog_table_name(&parts, None, self.current_database)
            .map_err(SqlCompileError::Compilation)?;
        self.add_need(
            relation,
            CatalogLookupTarget::Table {
                mode: self.lookup_mode,
            },
        )
    }

    fn resolve_name(&self, name: &ast::ObjectName) -> Result<TableIdentity, SqlCompileError> {
        let parts = name
            .parts
            .iter()
            .map(|part| part.value.clone())
            .collect::<Vec<_>>();
        resolve_catalog_table_name(&parts, self.current_catalog, self.current_database)
            .map_err(SqlCompileError::Compilation)
    }

    fn add_need(
        &mut self,
        relation: TableIdentity,
        target: CatalogLookupTarget,
    ) -> Result<(), SqlCompileError> {
        let key = CatalogFactKey::new(relation.clone(), target);
        if !self.keys.insert(key) {
            return Ok(());
        }
        let id = CompileNeedId::new(self.next_need_ordinal);
        self.next_need_ordinal = self.next_need_ordinal.checked_add(1).ok_or_else(|| {
            SqlCompileError::Compilation("catalog completion need identity overflow".to_string())
        })?;
        let need = CatalogRelationNeed::try_new(id, relation, target)
            .map_err(|error| SqlCompileError::Compilation(error.to_string()))?;
        self.needs.push(need);
        Ok(())
    }
}

struct ExpressionQueryCollector<'collector, 'scope, 'context> {
    collector: &'collector mut CatalogNeedCollector<'context>,
    scope: &'scope CteScope,
    error: Option<SqlCompileError>,
}

impl Visit for ExpressionQueryCollector<'_, '_, '_> {
    fn visit_query(&mut self, query: &ast::Query) {
        if self.error.is_none()
            && let Err(error) = self.collector.collect_query(query, self.scope)
        {
            self.error = Some(error);
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CatalogFactKey {
    relation: TableIdentity,
    target: CatalogTargetKey,
}

impl CatalogFactKey {
    fn new(relation: TableIdentity, target: CatalogLookupTarget) -> Self {
        Self {
            relation,
            target: CatalogTargetKey::from(target),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum CatalogTargetKey {
    TableSchemaOnly,
    TableExplainStats,
    IcebergMetadata(SqlMetadataTableKind),
}

impl From<CatalogLookupTarget> for CatalogTargetKey {
    fn from(value: CatalogLookupTarget) -> Self {
        match value {
            CatalogLookupTarget::Table {
                mode: TableLookupMode::SchemaOnly,
            } => Self::TableSchemaOnly,
            CatalogLookupTarget::Table {
                mode: TableLookupMode::ExplainStats,
            } => Self::TableExplainStats,
            CatalogLookupTarget::IcebergMetadata { kind } => Self::IcebergMetadata(kind),
        }
    }
}

enum CatalogEntry {
    Resolved(Box<ResolvedAnalyzerTable>),
    Missing(Box<str>),
}

/// Analyzer catalog backed solely by one exact completion answer batch.
pub(super) struct FactBackedCatalog {
    current_catalog: Option<Box<str>>,
    current_database: Box<str>,
    lookup_mode: TableLookupMode,
    entries: HashMap<CatalogFactKey, CatalogEntry>,
}

impl FactBackedCatalog {
    fn try_new(
        current_catalog: Option<&str>,
        current_database: &str,
        lookup_mode: TableLookupMode,
        needs: &[CatalogRelationNeed],
        facts: &[CatalogRelationFact],
    ) -> Result<Self, SqlCompileError> {
        let needs_by_id = needs
            .iter()
            .map(|need| (need.id(), need))
            .collect::<HashMap<_, _>>();
        let mut entries = HashMap::with_capacity(facts.len());
        for fact in facts {
            let need = needs_by_id.get(&fact.id()).ok_or_else(|| {
                SqlCompileError::Compilation(format!(
                    "catalog completion returned unexpected need {}",
                    fact.id().get()
                ))
            })?;
            if fact.relation() != need.relation() || fact.target() != need.target() {
                return Err(SqlCompileError::Compilation(format!(
                    "catalog completion fact {} does not match its exact request",
                    fact.id().get()
                )));
            }
            let key = CatalogFactKey::new(fact.relation().clone(), fact.target());
            let entry = match fact.outcome() {
                CatalogRelationOutcome::Resolved(table) => {
                    validate_resolved_target(fact, table)?;
                    CatalogEntry::Resolved(table.clone())
                }
                CatalogRelationOutcome::Missing { reason } => CatalogEntry::Missing(reason.clone()),
            };
            if entries.insert(key, entry).is_some() {
                return Err(SqlCompileError::Compilation(format!(
                    "catalog completion repeats lookup for `{}`",
                    fact.relation().fqn()
                )));
            }
        }
        if entries.len() != needs.len() {
            return Err(SqlCompileError::Compilation(
                "catalog completion did not answer every exact request".to_string(),
            ));
        }
        Ok(Self {
            current_catalog: current_catalog.map(Into::into),
            current_database: current_database.into(),
            lookup_mode,
            entries,
        })
    }

    fn resolve(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        target: CatalogLookupTarget,
    ) -> Result<ResolvedAnalyzerTable, String> {
        let parts = match catalog {
            Some(catalog) => vec![catalog.to_string(), database.to_string(), table.to_string()],
            None => vec![database.to_string(), table.to_string()],
        };
        let identity = resolve_catalog_table_name(
            &parts,
            self.current_catalog.as_deref(),
            &self.current_database,
        )?;
        let key = CatalogFactKey::new(identity.clone(), target);
        match self.entries.get(&key) {
            Some(CatalogEntry::Resolved(table)) => Ok(table.as_ref().clone()),
            Some(CatalogEntry::Missing(reason)) => Err(format!(
                "catalog relation `{}` is missing: {reason}",
                identity.fqn()
            )),
            None => Err(format!(
                "catalog relation `{}` was not requested for {target:?}",
                identity.fqn()
            )),
        }
    }
}

impl PlannerTableProvider for FactBackedCatalog {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve(
            catalog,
            database,
            table,
            CatalogLookupTarget::Table {
                mode: self.lookup_mode,
            },
        )
    }

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        Some(self)
    }
}

impl IcebergMetadataTableProvider for FactBackedCatalog {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: SqlMetadataTableKind,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve(
            catalog,
            database,
            table,
            CatalogLookupTarget::IcebergMetadata {
                kind: metadata_table_type,
            },
        )
    }
}

fn validate_resolved_target(
    fact: &CatalogRelationFact,
    table: &ResolvedAnalyzerTable,
) -> Result<(), SqlCompileError> {
    if table.catalog.identity != *fact.relation() {
        return Err(SqlCompileError::Compilation(format!(
            "catalog completion fact {} resolved `{}` instead of `{}`",
            fact.id().get(),
            table.catalog.identity.fqn(),
            fact.relation().fqn()
        )));
    }
    let ScanSource::Sql(source) = &table.planner.source;
    if source.table.catalog != fact.relation().catalog
        || source.table.namespace != fact.relation().namespace
        || source.table.table != fact.relation().table
    {
        return Err(SqlCompileError::Compilation(format!(
            "catalog completion fact {} carries a mismatched planner source identity",
            fact.id().get()
        )));
    }
    let matches_target = match (fact.target(), &source.kind) {
        (CatalogLookupTarget::Table { .. }, SqlScanKind::Metadata { .. }) => false,
        (CatalogLookupTarget::Table { .. }, _) => true,
        (
            CatalogLookupTarget::IcebergMetadata { kind: expected },
            SqlScanKind::Metadata { kind: actual, .. },
        ) => expected == *actual,
        (CatalogLookupTarget::IcebergMetadata { .. }, _) => false,
    };
    if !matches_target {
        return Err(SqlCompileError::Compilation(format!(
            "catalog completion fact {} carries a planner source incompatible with {:?}",
            fact.id().get(),
            fact.target()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::SqlTableBindingId;
    use crate::planner::table::{
        SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };

    fn parse_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("query must parse");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected one query statement");
        };
        query.clone()
    }

    fn state(sql: &str) -> CatalogCompletionState {
        CatalogCompletionState::try_new(
            parse_query(sql),
            Some("session_catalog"),
            "session_db",
            TableLookupMode::SchemaOnly,
            10,
        )
        .expect("catalog needs must collect")
    }

    fn keys(state: &CatalogCompletionState) -> HashSet<CatalogFactKey> {
        state
            .needs()
            .iter()
            .map(|need| CatalogFactKey::new(need.relation().clone(), need.target()))
            .collect()
    }

    fn key(
        catalog: &str,
        namespace: &str,
        table: &str,
        target: CatalogLookupTarget,
    ) -> CatalogFactKey {
        CatalogFactKey::new(TableIdentity::new(catalog, namespace, table), target)
    }

    fn resolved_table(identity: &TableIdentity, kind: SqlScanKind) -> ResolvedAnalyzerTable {
        let planner = TableDef {
            name: identity.table.clone(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::Sql(SqlScanSource::new(
                SqlTableBindingId::new_for_test(1),
                SqlTableIdentity::try_new(
                    identity.catalog.clone(),
                    identity.namespace.clone(),
                    identity.table.clone(),
                )
                .expect("identity must be valid"),
                kind,
            )),
        };
        ResolvedAnalyzerTable::from_planner(Some(&identity.catalog), &identity.namespace, planner)
    }

    #[test]
    fn collects_all_relations_in_one_batch_with_exact_name_resolution() {
        let state = state(
            "SELECT * FROM local_table l \
             JOIN other_db.orders o ON l.id = o.id \
             JOIN remote_catalog.sales.orders r ON o.id = r.id",
        );
        let expected = HashSet::from([
            key(
                "session_catalog",
                "session_db",
                "local_table",
                CatalogLookupTarget::Table {
                    mode: TableLookupMode::SchemaOnly,
                },
            ),
            key(
                "session_catalog",
                "other_db",
                "orders",
                CatalogLookupTarget::Table {
                    mode: TableLookupMode::SchemaOnly,
                },
            ),
            key(
                "remote_catalog",
                "sales",
                "orders",
                CatalogLookupTarget::Table {
                    mode: TableLookupMode::SchemaOnly,
                },
            ),
        ]);
        assert_eq!(keys(&state), expected);
        assert_eq!(state.next_need_ordinal(), 13);
    }

    #[test]
    fn cte_shadowing_and_nested_queries_do_not_issue_catalog_needs_for_ctes() {
        let state = state(
            "WITH orders AS (SELECT * FROM source_catalog.raw.orders), \
                  nested AS (WITH orders AS (SELECT * FROM source_catalog.raw.inner_orders) \
                             SELECT * FROM orders) \
             SELECT * FROM orders \
             JOIN nested ON 1 = 1 \
             JOIN source_catalog.audit.orders a ON 1 = 1 \
             WHERE EXISTS (SELECT 1 FROM source_catalog.audit.events)",
        );
        let table_target = CatalogLookupTarget::Table {
            mode: TableLookupMode::SchemaOnly,
        };
        assert_eq!(
            keys(&state),
            HashSet::from([
                key("source_catalog", "raw", "orders", table_target),
                key("source_catalog", "raw", "inner_orders", table_target),
                key("source_catalog", "audit", "orders", table_target),
                key("source_catalog", "audit", "events", table_target),
            ])
        );
    }

    #[test]
    fn additional_mv_queries_and_targets_share_one_exact_catalog_batch() {
        let main = parse_query("SELECT * FROM iceberg.prod.orders");
        let candidates = [
            parse_query("SELECT * FROM iceberg.prod.orders"),
            parse_query("SELECT * FROM iceberg.raw.lineitem"),
        ];
        let targets = [TableIdentity::new("iceberg", "mv", "orders_rollup")];
        let state = CatalogCompletionState::try_new_with_additional_queries(
            main,
            &candidates,
            &targets,
            Some("iceberg"),
            "prod",
            TableLookupMode::SchemaOnly,
            10,
        )
        .expect("combined catalog batch");
        let target = CatalogLookupTarget::Table {
            mode: TableLookupMode::SchemaOnly,
        };
        assert_eq!(
            keys(&state),
            HashSet::from([
                key("iceberg", "prod", "orders", target),
                key("iceberg", "raw", "lineitem", target),
                key("iceberg", "mv", "orders_rollup", target),
            ])
        );
        assert_eq!(state.next_need_ordinal(), 13);
    }

    #[test]
    fn recursive_cte_is_preanalyzed_before_catalog_collection() {
        let state = state(
            "WITH RECURSIVE seq(n) AS (\
                 SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 2\
             ) SELECT * FROM seq",
        );
        assert!(state.needs().is_empty());
    }

    #[test]
    fn metadata_and_ordinary_lookups_are_distinct_and_each_is_deduplicated() {
        let state = state(
            "SELECT * FROM iceberg.prod.orders$files f1 \
             JOIN iceberg.prod.orders$files f2 ON 1 = 1 \
             JOIN iceberg.prod.orders o ON 1 = 1",
        );
        let identity = TableIdentity::new("iceberg", "prod", "orders");
        assert_eq!(state.needs().len(), 2);
        assert!(keys(&state).contains(&CatalogFactKey::new(
            identity.clone(),
            CatalogLookupTarget::IcebergMetadata {
                kind: SqlMetadataTableKind::Files,
            },
        )));
        assert!(keys(&state).contains(&CatalogFactKey::new(
            identity,
            CatalogLookupTarget::Table {
                mode: TableLookupMode::SchemaOnly,
            },
        )));
    }

    #[test]
    fn same_table_name_in_different_namespaces_remains_distinct() {
        let state = state(
            "SELECT * FROM iceberg.sales.orders s \
             JOIN iceberg.audit.orders a ON s.id = a.id",
        );
        let target = CatalogLookupTarget::Table {
            mode: TableLookupMode::SchemaOnly,
        };
        assert_eq!(state.needs().len(), 2);
        assert!(keys(&state).contains(&key("iceberg", "sales", "orders", target)));
        assert!(keys(&state).contains(&key("iceberg", "audit", "orders", target)));
    }

    #[test]
    fn internal_delta_table_function_collects_its_explicit_base_identity() {
        let state =
            state("SELECT * FROM TABLE(__nr_ivm_delta('iceberg.raw.events', 10, 20)) AS delta");
        assert_eq!(
            keys(&state),
            HashSet::from([key(
                "iceberg",
                "raw",
                "events",
                CatalogLookupTarget::Table {
                    mode: TableLookupMode::SchemaOnly,
                },
            )])
        );
    }

    #[test]
    fn absent_current_catalog_fails_closed_for_unqualified_relation() {
        let result = CatalogCompletionState::try_new(
            parse_query("SELECT * FROM orders"),
            None,
            "db",
            TableLookupMode::SchemaOnly,
            1,
        );
        let Err(error) = result else {
            panic!("an unqualified relation requires an exact current catalog");
        };
        assert!(error.to_string().contains("current catalog"));
    }

    #[test]
    fn fact_catalog_returns_exact_missing_fact_and_never_uses_another_namespace() {
        let state = state(
            "SELECT * FROM iceberg.sales.orders s \
             JOIN iceberg.audit.orders a ON 1 = 1",
        );
        let facts = state
            .needs()
            .iter()
            .map(|need| {
                CatalogRelationFact::missing(need, format!("missing {}", need.relation().fqn()))
                    .expect("missing fact must be valid")
            })
            .collect::<Vec<_>>();
        let catalog = state.fact_catalog(&facts).expect("facts must bind exactly");
        let error = catalog
            .resolve_table_for_analysis(Some("iceberg"), "sales", "orders")
            .expect_err("missing fact must fail closed");
        assert!(error.contains("iceberg.sales.orders"));
        assert!(!error.contains("iceberg.audit.orders"));
        assert!(
            catalog
                .resolve_table_for_analysis(Some("iceberg"), "unknown", "orders")
                .expect_err("an unrequested namespace must fail closed")
                .contains("was not requested")
        );
    }

    #[test]
    fn resolved_fact_must_match_ordinary_vs_metadata_scan_kind() {
        let metadata_state = state("SELECT * FROM iceberg.prod.orders$entries");
        let metadata_need = &metadata_state.needs()[0];
        let ordinary = resolved_table(
            metadata_need.relation(),
            SqlScanKind::Data {
                version: SqlTableVersionSelector::Current,
            },
        );
        assert!(CatalogRelationFact::resolved(metadata_need, ordinary).is_err());

        let ordinary_state = state("SELECT * FROM iceberg.prod.orders");
        let ordinary_need = &ordinary_state.needs()[0];
        let metadata = resolved_table(
            ordinary_need.relation(),
            SqlScanKind::Metadata {
                kind: SqlMetadataTableKind::Files,
                version: SqlTableVersionSelector::Current,
            },
        );
        assert!(CatalogRelationFact::resolved(ordinary_need, metadata).is_err());
    }

    #[test]
    fn metadata_fact_must_match_the_exact_requested_kind() {
        let state = state("SELECT * FROM iceberg.prod.orders$entries");
        let need = &state.needs()[0];
        let wrong_kind = resolved_table(
            need.relation(),
            SqlScanKind::Metadata {
                kind: SqlMetadataTableKind::Files,
                version: SqlTableVersionSelector::Current,
            },
        );
        assert!(CatalogRelationFact::resolved(need, wrong_kind).is_err());
    }

    #[test]
    fn fact_catalog_rejects_mismatched_inner_planner_source_identity() {
        let state = state("SELECT * FROM iceberg.prod.orders");
        let need = &state.needs()[0];
        let wrong_identity = TableIdentity::new("iceberg", "other", "orders");
        let planner = resolved_table(
            &wrong_identity,
            SqlScanKind::Data {
                version: SqlTableVersionSelector::Current,
            },
        )
        .planner;
        let table = ResolvedAnalyzerTable {
            catalog: novarocks_types::schema::CatalogTable {
                identity: need.relation().clone(),
                columns: Vec::new(),
                hidden_columns: Vec::new(),
            },
            planner,
        };
        assert!(matches!(
            CatalogRelationFact::resolved(need, table),
            Err(
                super::super::completion::CompletionProtocolError::CatalogLookupTargetMismatch { .. }
            )
        ));
    }
}
