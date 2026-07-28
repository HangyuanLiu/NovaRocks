use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct TableKey {
    pub db: String,
    pub table: String,
}

#[derive(Clone, Debug)]
pub(super) struct ColumnStatRow {
    pub key: TableKey,
    pub column_name: String,
    pub partition_name: String,
    pub row_count: i64,
    pub max: String,
    pub min: String,
    pub ndv: String,
}

#[derive(Clone, Debug)]
pub(super) struct HistogramStatRow {
    pub key: TableKey,
    pub column_name: String,
    pub buckets: String,
    pub mcv: String,
}

#[derive(Clone, Debug)]
pub(super) struct MultiColumnStatRow {
    pub key: TableKey,
    pub column_names: String,
}

#[derive(Clone, Debug)]
pub(super) struct AnalyzeStatusRow {
    pub id: i64,
    pub db: String,
    pub table: String,
    pub columns: String,
    pub analyze_type: String,
    pub status: String,
    pub is_new: bool,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ColumnUsage {
    pub columns: BTreeMap<String, BTreeSet<&'static str>>,
}

pub(super) struct StatisticsState {
    pub collect_on_first_load: bool,
    pub table_collect_on_first_load: BTreeMap<TableKey, bool>,
    pub column_stats: Vec<ColumnStatRow>,
    pub histogram_stats: Vec<HistogramStatRow>,
    pub multi_column_stats: Vec<MultiColumnStatRow>,
    pub analyze_status: Vec<AnalyzeStatusRow>,
    pub column_usage: BTreeMap<TableKey, ColumnUsage>,
    pub next_analyze_id: i64,
}

impl Default for StatisticsState {
    fn default() -> Self {
        Self {
            collect_on_first_load: true,
            table_collect_on_first_load: BTreeMap::new(),
            column_stats: Vec::new(),
            histogram_stats: Vec::new(),
            multi_column_stats: Vec::new(),
            analyze_status: Vec::new(),
            column_usage: BTreeMap::new(),
            next_analyze_id: 1,
        }
    }
}
