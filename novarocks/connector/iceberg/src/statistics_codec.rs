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

//! Provider-private encoding for Iceberg published statistics.
//!
//! The payload is stored by the Iceberg provider, while Core only sees the
//! SPI `StatisticsEvidence` reconstructed from this codec.  Keeping it here
//! prevents SQL/optimizer types from becoming part of a persisted provider
//! artifact.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, StatisticsDataVersion, StatisticsEvidence,
    StatisticsMetric, StatisticsMetricSource, StatisticsMetricState, StatisticsMetricValue,
    StatisticsRowCoverage,
};
use serde::{Deserialize, Serialize};

/// Bumped to 2 when the artifact moved from column names to stable field ids.
/// A version-1 payload is unreadable rather than name-matched: once statistics
/// can be read across snapshots, matching by name would silently attach a
/// column's statistics to whatever now bears its name.
const ICEBERG_PROVIDER_STATISTICS_VERSION: u16 = 2;

pub fn statistics_data_version(
    table_uuid: &str,
    snapshot_id: Option<i64>,
) -> Result<StatisticsDataVersion, ConnectorError> {
    StatisticsDataVersion::try_new(Bytes::from(format!(
        "iceberg/v1/{table_uuid}/{}",
        snapshot_id
            .map(|snapshot| snapshot.to_string())
            .unwrap_or_else(|| "empty".to_string())
    )))
}

pub fn statistics_metric_column(metric: &StatisticsMetric) -> Option<&str> {
    match metric {
        StatisticsMetric::RowCount => None,
        StatisticsMetric::NullCount { column }
        | StatisticsMetric::Minimum { column }
        | StatisticsMetric::Maximum { column }
        | StatisticsMetric::AverageSize { column }
        | StatisticsMetric::ThetaNdv { column } => Some(column),
    }
}

/// Rejects evidence that did not come from a complete scan of the pinned
/// table's visible rows.
///
/// This deliberately says nothing about numeric exactness. A full visible-row
/// collection legitimately contains a Theta sketch, which is never exact; the
/// property that makes it publishable is that it observed every visible row.
pub fn ensure_publishable_visible_row_evidence(
    evidence: &StatisticsEvidence,
) -> Result<(), ConnectorError> {
    if evidence.row_coverage() != StatisticsRowCoverage::AllVisibleRows {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg provider statistics require a collection covering all visible rows",
        ));
    }
    for state in evidence.metrics().values() {
        if let StatisticsMetricState::Available(observation) = state
            && *observation.source() != StatisticsMetricSource::VisibleRowScan
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg provider statistics require every published metric to come from a visible-row scan",
            ));
        }
    }
    Ok(())
}

/// Encodes the published artifact, keying every column metric by the stable
/// field id it had in the measured snapshot's schema.
pub fn encode_provider_statistics(
    evidence: &StatisticsEvidence,
    field_ids: &HashMap<String, i32>,
) -> Result<Vec<u8>, ConnectorError> {
    ensure_publishable_visible_row_evidence(evidence)?;
    let field_id = |column: &str| -> Result<i32, ConnectorError> {
        field_ids
            .get(&column.to_ascii_lowercase())
            .copied()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "Iceberg statistics column `{column}` is absent from the measured schema"
                    ),
                )
            })
    };
    let metrics = evidence
        .metrics()
        .iter()
        .map(|(metric, state)| {
            let StatisticsMetricState::Available(observation) = state else {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg provider statistics cannot persist unavailable metrics",
                ));
            };
            let value = match observation.value() {
                StatisticsMetricValue::U64(value) => IcebergStatisticValueV2::U64(*value),
                StatisticsMetricValue::I64(value) => IcebergStatisticValueV2::I64(*value),
                StatisticsMetricValue::F64(value) => IcebergStatisticValueV2::F64(*value),
                StatisticsMetricValue::Bytes(value) => {
                    IcebergStatisticValueV2::Bytes(value.to_vec())
                }
            };
            Ok(match metric {
                StatisticsMetric::RowCount => IcebergProviderStatisticV2::RowCount { value },
                StatisticsMetric::NullCount { column } => IcebergProviderStatisticV2::NullCount {
                    field_id: field_id(column)?,
                    value,
                },
                StatisticsMetric::Minimum { column } => IcebergProviderStatisticV2::Minimum {
                    field_id: field_id(column)?,
                    value,
                },
                StatisticsMetric::Maximum { column } => IcebergProviderStatisticV2::Maximum {
                    field_id: field_id(column)?,
                    value,
                },
                StatisticsMetric::AverageSize { column } => {
                    IcebergProviderStatisticV2::AverageSize {
                        field_id: field_id(column)?,
                        value,
                    }
                }
                StatisticsMetric::ThetaNdv { column } => IcebergProviderStatisticV2::ThetaNdv {
                    field_id: field_id(column)?,
                    value,
                },
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_vec(&IcebergProviderStatisticsV2 {
        version: ICEBERG_PROVIDER_STATISTICS_VERSION,
        data_version: evidence.data_version().as_bytes().to_vec(),
        metrics,
    })
    .map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("encode Iceberg provider statistics: {error}"),
        )
    })
}

/// Decodes a published artifact, matching column metrics by the stable field id
/// they were written under.
///
/// `field_ids` maps each requested column name to its field id **in the schema
/// of the snapshot being queried**. Matching through it is what keeps a renamed
/// column's statistics attached to the column rather than to the name.
///
/// Returns plain values: the basis version and relation belong to whoever
/// resolved this artifact, since the same bytes read from an ancestor snapshot
/// describe a different row set than when read from the queried one.
///
/// A payload written under an older artifact version yields no metrics at all
/// rather than an error — unreadable statistics are missing statistics, not a
/// failed query.
pub fn decode_provider_statistics(
    payload: &[u8],
    field_ids: &HashMap<String, i32>,
    requested: &novarocks_spi::connector::StatisticsMetricRequest,
) -> Result<BTreeMap<StatisticsMetric, StatisticsMetricValue>, ConnectorError> {
    let artifact: IcebergProviderStatisticsV2 =
        serde_json::from_slice(payload).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode Iceberg provider statistics: {error}"),
            )
        })?;
    if artifact.version != ICEBERG_PROVIDER_STATISTICS_VERSION {
        return Ok(BTreeMap::new());
    }

    // Keyed by (metric kind, field id) so a renamed column still matches.
    let mut available: BTreeMap<(MetricKind, Option<i32>), IcebergStatisticValueV2> =
        BTreeMap::new();
    for metric in artifact.metrics {
        let (kind, field_id, value) = match metric {
            IcebergProviderStatisticV2::RowCount { value } => (MetricKind::RowCount, None, value),
            IcebergProviderStatisticV2::NullCount { field_id, value } => {
                (MetricKind::NullCount, Some(field_id), value)
            }
            IcebergProviderStatisticV2::Minimum { field_id, value } => {
                (MetricKind::Minimum, Some(field_id), value)
            }
            IcebergProviderStatisticV2::Maximum { field_id, value } => {
                (MetricKind::Maximum, Some(field_id), value)
            }
            IcebergProviderStatisticV2::AverageSize { field_id, value } => {
                (MetricKind::AverageSize, Some(field_id), value)
            }
            IcebergProviderStatisticV2::ThetaNdv { field_id, value } => {
                (MetricKind::ThetaNdv, Some(field_id), value)
            }
        };
        if available.insert((kind, field_id), value).is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg provider statistics contain a duplicate metric",
            ));
        }
    }

    let mut decoded = BTreeMap::new();
    for metric in requested.metrics() {
        let kind = MetricKind::of(metric);
        let field_id = match statistics_metric_column(metric) {
            // The column does not exist in the queried schema, so no artifact
            // entry can be about it.
            Some(column) => match field_ids.get(&column.to_ascii_lowercase()) {
                Some(field_id) => Some(*field_id),
                None => continue,
            },
            None => None,
        };
        let Some(value) = available.get(&(kind, field_id)) else {
            continue;
        };
        let value = match value {
            IcebergStatisticValueV2::U64(value) => StatisticsMetricValue::U64(*value),
            IcebergStatisticValueV2::I64(value) => StatisticsMetricValue::I64(*value),
            IcebergStatisticValueV2::F64(value) if value.is_finite() => {
                StatisticsMetricValue::F64(*value)
            }
            IcebergStatisticValueV2::F64(_) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg provider statistics contain a non-finite value",
                ));
            }
            IcebergStatisticValueV2::Bytes(value) => {
                StatisticsMetricValue::try_bytes(Bytes::from(value.clone()))?
            }
        };
        decoded.insert(metric.clone(), value);
    }
    Ok(decoded)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum MetricKind {
    RowCount,
    NullCount,
    Minimum,
    Maximum,
    AverageSize,
    ThetaNdv,
}

impl MetricKind {
    fn of(metric: &StatisticsMetric) -> Self {
        match metric {
            StatisticsMetric::RowCount => Self::RowCount,
            StatisticsMetric::NullCount { .. } => Self::NullCount,
            StatisticsMetric::Minimum { .. } => Self::Minimum,
            StatisticsMetric::Maximum { .. } => Self::Maximum,
            StatisticsMetric::AverageSize { .. } => Self::AverageSize,
            StatisticsMetric::ThetaNdv { .. } => Self::ThetaNdv,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct IcebergProviderStatisticsV2 {
    version: u16,
    data_version: Vec<u8>,
    metrics: Vec<IcebergProviderStatisticV2>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "metric", rename_all = "snake_case")]
enum IcebergProviderStatisticV2 {
    RowCount {
        value: IcebergStatisticValueV2,
    },
    NullCount {
        field_id: i32,
        value: IcebergStatisticValueV2,
    },
    Minimum {
        field_id: i32,
        value: IcebergStatisticValueV2,
    },
    Maximum {
        field_id: i32,
        value: IcebergStatisticValueV2,
    },
    AverageSize {
        field_id: i32,
        value: IcebergStatisticValueV2,
    },
    ThetaNdv {
        field_id: i32,
        value: IcebergStatisticValueV2,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum IcebergStatisticValueV2 {
    U64(u64),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use novarocks_spi::connector::{
        StatisticsBasisRelation, StatisticsEvidenceRevision, StatisticsMetricObservation,
        StatisticsMetricRequest, StatisticsNumericNature,
    };

    fn theta() -> StatisticsMetric {
        StatisticsMetric::ThetaNdv {
            column: Arc::from("k"),
        }
    }

    fn version() -> StatisticsDataVersion {
        StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("version")
    }

    fn scanned(
        value: StatisticsMetricValue,
        version: &StatisticsDataVersion,
        nature: StatisticsNumericNature,
    ) -> StatisticsMetricState {
        StatisticsMetricState::Available(StatisticsMetricObservation::new(
            value,
            version.clone(),
            StatisticsMetricSource::VisibleRowScan,
            nature,
            StatisticsBasisRelation::Identical,
        ))
    }

    fn visible_row_evidence(version: &StatisticsDataVersion) -> StatisticsEvidence {
        StatisticsEvidence::try_new(
            version.clone(),
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"run-v1")).expect("revision"),
            StatisticsRowCoverage::AllVisibleRows,
            BTreeMap::from([
                (
                    StatisticsMetric::RowCount,
                    scanned(
                        StatisticsMetricValue::U64(3),
                        version,
                        StatisticsNumericNature::Exact,
                    ),
                ),
                (
                    theta(),
                    scanned(
                        StatisticsMetricValue::F64(3.0),
                        version,
                        StatisticsNumericNature::TwoSidedApproximate,
                    ),
                ),
            ]),
        )
        .expect("evidence")
    }

    /// `k` is field 7 in the schema that produced the artifact.
    fn measured_schema() -> HashMap<String, i32> {
        HashMap::from([("k".to_string(), 7)])
    }

    fn request(metrics: Vec<StatisticsMetric>) -> StatisticsMetricRequest {
        StatisticsMetricRequest::try_new(metrics).expect("request")
    }

    #[test]
    fn provider_artifact_round_trips_only_requested_metrics() {
        let version = version();
        let payload =
            encode_provider_statistics(&visible_row_evidence(&version), &measured_schema())
                .expect("encode");

        let decoded = decode_provider_statistics(
            &payload,
            &measured_schema(),
            &request(vec![
                StatisticsMetric::RowCount,
                theta(),
                StatisticsMetric::NullCount {
                    column: Arc::from("k"),
                },
            ]),
        )
        .expect("decode");

        assert_eq!(
            decoded.get(&StatisticsMetric::RowCount),
            Some(&StatisticsMetricValue::U64(3))
        );
        assert_eq!(
            decoded.get(&theta()),
            Some(&StatisticsMetricValue::F64(3.0))
        );
        assert!(
            !decoded.contains_key(&StatisticsMetric::NullCount {
                column: Arc::from("k")
            }),
            "a metric absent from the artifact is simply not decoded"
        );
    }

    /// The point of keying by field id: the column can be renamed between
    /// publication and the read, and its statistics must follow the column.
    #[test]
    fn a_renamed_column_still_matches_its_own_statistics() {
        let version = version();
        let payload =
            encode_provider_statistics(&visible_row_evidence(&version), &measured_schema())
                .expect("encode");

        // Same field id 7, now called `renamed`.
        let queried_schema = HashMap::from([("renamed".to_string(), 7)]);
        let decoded = decode_provider_statistics(
            &payload,
            &queried_schema,
            &request(vec![StatisticsMetric::ThetaNdv {
                column: Arc::from("renamed"),
            }]),
        )
        .expect("decode");

        assert_eq!(
            decoded.get(&StatisticsMetric::ThetaNdv {
                column: Arc::from("renamed")
            }),
            Some(&StatisticsMetricValue::F64(3.0))
        );
    }

    /// A column added after publication has a field id the artifact never saw.
    /// It must decode to nothing rather than borrow another column's value.
    #[test]
    fn a_column_added_after_publication_has_no_statistics() {
        let version = version();
        let payload =
            encode_provider_statistics(&visible_row_evidence(&version), &measured_schema())
                .expect("encode");

        let queried_schema = HashMap::from([("k".to_string(), 7), ("added".to_string(), 9)]);
        let decoded = decode_provider_statistics(
            &payload,
            &queried_schema,
            &request(vec![StatisticsMetric::ThetaNdv {
                column: Arc::from("added"),
            }]),
        )
        .expect("decode");

        assert!(decoded.is_empty());
    }

    #[test]
    fn an_artifact_from_an_older_format_yields_no_metrics_rather_than_an_error() {
        let stale = serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "data_version": [1, 2, 3],
            "metrics": [],
        }))
        .expect("stale payload");

        let decoded = decode_provider_statistics(
            &stale,
            &measured_schema(),
            &request(vec![StatisticsMetric::RowCount]),
        )
        .expect("an unreadable artifact is missing statistics, not a failure");
        assert!(decoded.is_empty());
    }

    #[test]
    fn a_column_absent_from_the_measured_schema_cannot_be_published() {
        let version = version();
        assert!(
            encode_provider_statistics(&visible_row_evidence(&version), &HashMap::new()).is_err()
        );
    }

    #[test]
    fn a_full_visible_row_collection_containing_a_theta_sketch_is_publishable() {
        let version = version();
        encode_provider_statistics(&visible_row_evidence(&version), &measured_schema())
            .expect("numeric exactness is no longer a publication precondition");
    }

    #[test]
    fn partial_coverage_or_a_non_scan_source_cannot_be_published() {
        let version = version();
        let metrics = BTreeMap::from([(
            StatisticsMetric::RowCount,
            scanned(
                StatisticsMetricValue::U64(3),
                &version,
                StatisticsNumericNature::Exact,
            ),
        )]);

        let partial = StatisticsEvidence::try_new(
            version.clone(),
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"run-v1")).expect("revision"),
            StatisticsRowCoverage::PartialRows,
            metrics,
        )
        .expect("evidence");
        assert!(encode_provider_statistics(&partial, &measured_schema()).is_err());

        let manifest_derived = StatisticsEvidence::try_new(
            version.clone(),
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"run-v1")).expect("revision"),
            StatisticsRowCoverage::AllVisibleRows,
            BTreeMap::from([(
                StatisticsMetric::RowCount,
                StatisticsMetricState::Available(StatisticsMetricObservation::new(
                    StatisticsMetricValue::U64(3),
                    version,
                    StatisticsMetricSource::CurrentManifest,
                    StatisticsNumericNature::Exact,
                    StatisticsBasisRelation::Identical,
                )),
            )]),
        )
        .expect("evidence");
        assert!(encode_provider_statistics(&manifest_derived, &measured_schema()).is_err());
    }
}
