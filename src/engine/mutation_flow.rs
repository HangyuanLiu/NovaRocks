use std::sync::Arc;

use crate::engine::{StandaloneState, StatementResult};
use crate::sql::parser::ast::UpdateStmt;

pub(crate) fn execute_update_statement(
    state: &Arc<StandaloneState>,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        &stmt.table,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "UPDATE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    let _ = (target, stmt);
    Err(
        "UPDATE execution reaches mutation_flow; Task 3 adds validation and match planning"
            .to_string(),
    )
}
