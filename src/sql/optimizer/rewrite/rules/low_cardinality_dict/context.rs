//! Per-rule context for the low-cardinality dictionary rewrite.
//!
//! Distinct from `crate::sql::optimizer::rewrite::context::RewriteContext`:
//! this struct lives for one application of
//! `LowCardinalityDictionaryRewriteRule` and tracks the dict-eligible
//! scan columns plus the boundaries where dict columns must be decoded
//! back to their string form.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::engine::dictionary::model::DictionarySnapshot;

/// Identifies a base-table column that is participating in dictionary
/// rewrite. `(database, table, column)` are all lowercased to match the
/// normalization the catalog applies.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ScanColumnKey {
    pub database: String,
    pub table: String,
    pub column: String,
}

impl ScanColumnKey {
    pub(crate) fn new(database: &str, table: &str, column: &str) -> Self {
        Self {
            database: database.to_ascii_lowercase(),
            table: table.to_ascii_lowercase(),
            column: column.to_ascii_lowercase(),
        }
    }
}

/// State accumulated by the rule's collector and consumed by the
/// rewriter. Conservative by design: any scan-side dictionary column
/// that reaches a node the rewriter does not understand is decoded
/// before that node.
#[derive(Clone, Debug, Default)]
pub(crate) struct DictionaryRewriteContext {
    scan_columns: BTreeMap<ScanColumnKey, Arc<DictionarySnapshot>>,
    string_to_dict_column: BTreeMap<String, String>,
    /// Map from dict column name back to the original string column
    /// name. Used by the rewriter when materializing a `Decode` node.
    dict_to_string_column: BTreeMap<String, String>,
    /// Reverse lookup from string column to the snapshot used to encode
    /// it. Needed when the rewriter inserts a `Decode` and must locate
    /// the `dict_column` and snapshot mapping.
    string_to_snapshot: BTreeMap<String, Arc<DictionarySnapshot>>,
    /// Decode boundaries collected by the collector (string columns
    /// that must be available in string form to a downstream consumer
    /// such as Sort with non-order-preserving snapshot, Window, set
    /// op, etc.).
    #[allow(dead_code)] // reserved for Task 8 join/union refinements
    decode_boundaries: BTreeSet<String>,
    changed: bool,
}

impl DictionaryRewriteContext {
    /// Generate the synthetic dict column name for `table.column`. The
    /// name is shared between the scan-side hidden Int32 slot and any
    /// dict-column references inserted upstream.
    pub(crate) fn dict_column_name(table: &str, column: &str) -> String {
        format!(
            "__nr_dict_{}_{}",
            table.to_ascii_lowercase(),
            column.to_ascii_lowercase()
        )
    }

    pub(crate) fn register_scan_column(
        &mut self,
        key: ScanColumnKey,
        snapshot: DictionarySnapshot,
    ) {
        let dict_column = Self::dict_column_name(&key.table, &key.column);
        let snapshot = Arc::new(snapshot);
        let string_name = key.column.clone();
        self.scan_columns.insert(key, snapshot.clone());
        self.string_to_dict_column
            .insert(string_name.clone(), dict_column.clone());
        self.dict_to_string_column
            .insert(dict_column.clone(), string_name.clone());
        self.string_to_snapshot.insert(string_name, snapshot);
    }

    pub(crate) fn dict_column_for(&self, column: &str) -> Option<&str> {
        self.string_to_dict_column
            .get(&column.to_ascii_lowercase())
            .map(String::as_str)
    }

    pub(crate) fn string_column_for(&self, dict_column: &str) -> Option<&str> {
        self.dict_to_string_column
            .get(dict_column)
            .map(String::as_str)
    }

    pub(crate) fn snapshot_for_string(&self, column: &str) -> Option<&Arc<DictionarySnapshot>> {
        self.string_to_snapshot.get(&column.to_ascii_lowercase())
    }

    pub(crate) fn mark_changed(&mut self) {
        self.changed = true;
    }

    pub(crate) fn changed(&self) -> bool {
        self.changed
    }

    pub(crate) fn has_any_dict_column(&self) -> bool {
        !self.string_to_dict_column.is_empty()
    }

    pub(crate) fn dict_eligible_columns_for_scan(
        &self,
        database: &str,
        table: &str,
    ) -> Vec<(String, Arc<DictionarySnapshot>)> {
        let database = database.to_ascii_lowercase();
        let table = table.to_ascii_lowercase();
        self.scan_columns
            .iter()
            .filter(|(key, _)| key.database == database && key.table == table)
            .map(|(key, snapshot)| (key.column.clone(), snapshot.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dict_column_name_lowercases() {
        assert_eq!(
            DictionaryRewriteContext::dict_column_name("Customer", "Name"),
            "__nr_dict_customer_name"
        );
    }

    #[test]
    fn dict_column_for_lookup_is_case_insensitive() {
        let mut ctx = DictionaryRewriteContext::default();
        ctx.string_to_dict_column
            .insert("name".to_string(), "__nr_dict_t_name".to_string());
        assert_eq!(ctx.dict_column_for("name"), Some("__nr_dict_t_name"));
        assert_eq!(ctx.dict_column_for("NAME"), Some("__nr_dict_t_name"));
        assert!(ctx.dict_column_for("other").is_none());
    }
}
