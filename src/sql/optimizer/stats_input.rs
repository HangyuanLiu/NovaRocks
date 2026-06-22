//! Query-scoped statistics input for the optimizer.
//!
//! No engine, connector, or catalog dependencies belong in this module.

#![allow(dead_code)]

use std::collections::HashMap;

use crate::sql::optimizer::statistics::Confidence;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct StatsRef(u32);

impl StatsRef {
    pub(crate) fn new(value: u32) -> Self {
        Self(value)
    }

    pub(crate) fn as_u32(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatsSource {
    IcebergManifest,
    IcebergPuffin,
    ManagedLakeMetadata,
    StarRocksTableMetadata,
    ConnectorEstimate,
    Derived,
    Fallback,
    TestFixture,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StatsMissingReason {
    NoCurrentSnapshot,
    NoDataFiles,
    ManifestMissingRowCount,
    StatsFileMissing,
    ConnectorUnsupported(String),
    CatalogLoadError(String),
    ColumnNotReported(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum StatValue<T> {
    Known {
        value: T,
        confidence: Confidence,
        source: StatsSource,
    },
    Missing {
        reason: StatsMissingReason,
    },
}

impl<T> StatValue<T> {
    pub(crate) fn known(value: T, confidence: Confidence, source: StatsSource) -> Self {
        Self::Known {
            value,
            confidence,
            source,
        }
    }

    pub(crate) fn missing(reason: StatsMissingReason) -> Self {
        Self::Missing { reason }
    }

    pub(crate) fn known_value(&self) -> Option<&T> {
        match self {
            Self::Known { value, .. } => Some(value),
            Self::Missing { .. } => None,
        }
    }

    pub(crate) fn confidence(&self) -> Confidence {
        match self {
            Self::Known { confidence, .. } => *confidence,
            Self::Missing { .. } => Confidence::Fallback,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BaseColumnStatistics {
    pub nulls_fraction: StatValue<f64>,
    pub average_row_size: StatValue<f64>,
    pub min_value: StatValue<f64>,
    pub max_value: StatValue<f64>,
    pub ndv: StatValue<f64>,
}

impl BaseColumnStatistics {
    pub(crate) fn missing(column: &str) -> Self {
        let reason = StatsMissingReason::ColumnNotReported(column.to_ascii_lowercase());
        Self {
            nulls_fraction: StatValue::missing(reason.clone()),
            average_row_size: StatValue::missing(reason.clone()),
            min_value: StatValue::missing(reason.clone()),
            max_value: StatValue::missing(reason.clone()),
            ndv: StatValue::missing(reason),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BaseTableStatistics {
    pub row_count: StatValue<u64>,
    pub columns: HashMap<String, BaseColumnStatistics>,
    pub source: StatsSource,
}

impl BaseTableStatistics {
    pub(crate) fn missing(reason: StatsMissingReason) -> Self {
        Self {
            row_count: StatValue::missing(reason),
            columns: HashMap::new(),
            source: StatsSource::Fallback,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryStatsEntry {
    pub label: String,
    pub stats: BaseTableStatistics,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct QueryStatsSnapshot {
    entries: HashMap<StatsRef, QueryStatsEntry>,
}

impl QueryStatsSnapshot {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn insert(
        &mut self,
        stats_ref: StatsRef,
        label: impl Into<String>,
        stats: BaseTableStatistics,
    ) {
        self.entries.insert(
            stats_ref,
            QueryStatsEntry {
                label: label.into(),
                stats,
            },
        );
    }

    pub(crate) fn get(&self, stats_ref: StatsRef) -> Option<&BaseTableStatistics> {
        self.entries.get(&stats_ref).map(|entry| &entry.stats)
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn display_rows(&self) -> Vec<String> {
        let mut entries: Vec<_> = self.entries.iter().collect();
        entries.sort_by_key(|(stats_ref, _)| stats_ref.as_u32());

        entries
            .into_iter()
            .map(|(stats_ref, entry)| match &entry.stats.row_count {
                StatValue::Known {
                    value,
                    confidence,
                    source,
                } => format!(
                    "TABLE STATS ref={} table={} rows={} confidence={:?} source={:?}",
                    stats_ref.as_u32(),
                    entry.label,
                    value,
                    confidence,
                    source
                ),
                StatValue::Missing { reason } => format!(
                    "TABLE STATS ref={} table={} rows=missing reason={:?}",
                    stats_ref.as_u32(),
                    entry.label,
                    reason
                ),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_rows_sort_by_numeric_ref() {
        let mut snapshot = QueryStatsSnapshot::empty();
        snapshot.insert(
            StatsRef::new(10),
            "ten",
            BaseTableStatistics::missing(StatsMissingReason::NoDataFiles),
        );
        snapshot.insert(
            StatsRef::new(2),
            "two",
            BaseTableStatistics {
                row_count: StatValue::known(
                    7,
                    crate::sql::optimizer::statistics::Confidence::Exact,
                    StatsSource::IcebergManifest,
                ),
                columns: std::collections::HashMap::new(),
                source: StatsSource::IcebergManifest,
            },
        );

        assert_eq!(
            snapshot.display_rows(),
            vec![
                "TABLE STATS ref=2 table=two rows=7 confidence=Exact source=IcebergManifest"
                    .to_string(),
                "TABLE STATS ref=10 table=ten rows=missing reason=NoDataFiles".to_string(),
            ]
        );
    }

    #[test]
    fn get_returns_base_table_statistics() {
        let mut snapshot = QueryStatsSnapshot::empty();
        snapshot.insert(
            StatsRef::new(1),
            "orders",
            BaseTableStatistics {
                row_count: StatValue::known(42, Confidence::Exact, StatsSource::IcebergManifest),
                columns: std::collections::HashMap::new(),
                source: StatsSource::IcebergManifest,
            },
        );

        assert_eq!(
            snapshot
                .get(StatsRef::new(1))
                .unwrap()
                .row_count
                .known_value(),
            Some(&42)
        );
    }

    #[test]
    fn missing_confidence_falls_back() {
        let value: StatValue<u64> = StatValue::missing(StatsMissingReason::NoCurrentSnapshot);

        assert_eq!(value.confidence(), Confidence::Fallback);
    }

    #[test]
    fn connector_unsupported_preserves_reason() {
        let value: StatValue<u64> =
            StatValue::missing(StatsMissingReason::ConnectorUnsupported("jdbc".to_string()));

        assert_eq!(
            value,
            StatValue::Missing {
                reason: StatsMissingReason::ConnectorUnsupported("jdbc".to_string()),
            }
        );
    }
}
