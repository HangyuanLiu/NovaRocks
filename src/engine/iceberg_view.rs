//! Iceberg-catalog view DDL flows and name-target resolution.
//!
//! A view name routes to an iceberg catalog when it is a 3-part name
//! naming a registered iceberg catalog, or a 1/2-part name while a
//! session catalog (`SET CATALOG`) is active. Everything else stays a
//! session view in `StandaloneState::views`.

use std::sync::Arc;

use crate::engine::catalog::normalize_identifier;
use crate::engine::StandaloneState;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergViewTarget {
    pub catalog: String,
    pub namespace: String,
    pub view: String,
}

/// Resolve a view name (already split into identifier parts) to an iceberg
/// target. `Ok(None)` means "session view" (default catalog). The catalog
/// must exist in the iceberg registry; an unknown catalog is an error.
pub(crate) fn resolve_iceberg_view_target_parts(
    state: &Arc<StandaloneState>,
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Option<IcebergViewTarget>, String> {
    let session_catalog =
        current_catalog.filter(|catalog| !catalog.eq_ignore_ascii_case("default_catalog"));
    let (catalog, namespace, view) = match parts {
        [catalog, db, view] => {
            if catalog.eq_ignore_ascii_case("default_catalog") {
                return Ok(None);
            }
            (catalog.clone(), db.clone(), view.clone())
        }
        [db, view] => match session_catalog {
            Some(catalog) => (catalog.to_string(), db.clone(), view.clone()),
            None => return Ok(None),
        },
        [view] => match session_catalog {
            Some(catalog) => (
                catalog.to_string(),
                current_database.to_string(),
                view.clone(),
            ),
            None => return Ok(None),
        },
        _ => return Err(format!("invalid view name: {}", parts.join("."))),
    };
    let target = IcebergViewTarget {
        catalog: normalize_identifier(&catalog)?,
        namespace: normalize_identifier(&namespace)?,
        view: normalize_identifier(&view)?,
    };
    // Validate catalog existence eagerly so DDL gets a clear error.
    state
        .iceberg_catalogs
        .read()
        .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?
        .get(&target.catalog)?;
    Ok(Some(target))
}

/// Helper for sqlparser names: extract identifier parts then resolve.
pub(crate) fn resolve_iceberg_view_target(
    state: &Arc<StandaloneState>,
    name: &sqlparser::ast::ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Option<IcebergViewTarget>, String> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .collect();
    resolve_iceberg_view_target_parts(state, &parts, current_catalog, current_database)
}
