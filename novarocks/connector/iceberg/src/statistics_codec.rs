// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
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

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, StatisticsBasisRelation, StatisticsDataVersion,
    StatisticsEvidence, StatisticsMetric, StatisticsMetricObservation, StatisticsMetricSource,
    StatisticsMetricState, StatisticsMetricValue, StatisticsMissing, StatisticsMissingKind,
    StatisticsNumericNature, StatisticsRowCoverage,
};
use serde::{Deserialize, Serialize};

const ICEBERG_PROVIDER_STATISTICS_VERSION: u16 = 1;

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

pub fn encode_provider_statistics(
    evidence: &StatisticsEvidence,
) -> Result<Vec<u8>, ConnectorError> {
    ensure_publishable_visible_row_evidence(evidence)?;
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
                StatisticsMetricValue::U64(value) => IcebergStatisticValueV1::U64(*value),
                StatisticsMetricValue::I64(value) => IcebergStatisticValueV1::I64(*value),
                StatisticsMetricValue::F64(value) => IcebergStatisticValueV1::F64(*value),
                StatisticsMetricValue::Bytes(value) => {
                    IcebergStatisticValueV1::Bytes(value.to_vec())
                }
            };
            Ok(match metric {
                StatisticsMetric::RowCount => IcebergProviderStatisticV1::RowCount { value },
                StatisticsMetric::NullCount { column } => IcebergProviderStatisticV1::NullCount {
                    column: column.to_string(),
                    value,
                },
                StatisticsMetric::Minimum { column } => IcebergProviderStatisticV1::Minimum {
                    column: column.to_string(),
                    value,
                },
                StatisticsMetric::Maximum { column } => IcebergProviderStatisticV1::Maximum {
                    column: column.to_string(),
                    value,
                },
                StatisticsMetric::AverageSize { column } => {
                    IcebergProviderStatisticV1::AverageSize {
                        column: column.to_string(),
                        value,
                    }
                }
                StatisticsMetric::ThetaNdv { column } => IcebergProviderStatisticV1::ThetaNdv {
                    column: column.to_string(),
                    value,
                },
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_vec(&IcebergProviderStatisticsV1 {
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

pub fn decode_provider_statistics(
    payload: &[u8],
    expected_data_version: &StatisticsDataVersion,
    requested: &novarocks_spi::connector::StatisticsMetricRequest,
) -> Result<BTreeMap<StatisticsMetric, StatisticsMetricState>, ConnectorError> {
    let artifact: IcebergProviderStatisticsV1 =
        serde_json::from_slice(payload).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode Iceberg provider statistics: {error}"),
            )
        })?;
    if artifact.version != ICEBERG_PROVIDER_STATISTICS_VERSION
        || artifact.data_version.as_slice() != expected_data_version.as_bytes().as_ref()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg provider statistics do not match the pinned table version",
        ));
    }
    let mut available = BTreeMap::new();
    for metric in artifact.metrics {
        let (metric, value) = match metric {
            IcebergProviderStatisticV1::RowCount { value } => (StatisticsMetric::RowCount, value),
            IcebergProviderStatisticV1::NullCount { column, value } => (
                StatisticsMetric::NullCount {
                    column: Arc::from(column),
                },
                value,
            ),
            IcebergProviderStatisticV1::Minimum { column, value } => (
                StatisticsMetric::Minimum {
                    column: Arc::from(column),
                },
                value,
            ),
            IcebergProviderStatisticV1::Maximum { column, value } => (
                StatisticsMetric::Maximum {
                    column: Arc::from(column),
                },
                value,
            ),
            IcebergProviderStatisticV1::AverageSize { column, value } => (
                StatisticsMetric::AverageSize {
                    column: Arc::from(column),
                },
                value,
            ),
            IcebergProviderStatisticV1::ThetaNdv { column, value } => (
                StatisticsMetric::ThetaNdv {
                    column: Arc::from(column),
                },
                value,
            ),
        };
        let value = match value {
            IcebergStatisticValueV1::U64(value) => StatisticsMetricValue::U64(value),
            IcebergStatisticValueV1::I64(value) => StatisticsMetricValue::I64(value),
            IcebergStatisticValueV1::F64(value) if value.is_finite() => {
                StatisticsMetricValue::F64(value)
            }
            IcebergStatisticValueV1::F64(_) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg provider statistics contain a non-finite value",
                ));
            }
            IcebergStatisticValueV1::Bytes(value) => {
                StatisticsMetricValue::try_bytes(Bytes::from(value))?
            }
        };
        let observation = StatisticsMetricObservation::new(
            value,
            expected_data_version.clone(),
            StatisticsMetricSource::ProviderArtifact,
            provider_artifact_numeric_nature(&metric),
            // The artifact is keyed by the data version it was measured on, and
            // the caller already rejected a mismatch above, so this artifact
            // describes exactly the queried row set. Reading a statistics file
            // published against an *ancestor* snapshot is STAT-2C's ancestor
            // walk; when that lands it must derive the relation via
            // `statistics_basis::basis_relation` rather than assume identity.
            StatisticsBasisRelation::Identical,
        );
        if available
            .insert(metric, StatisticsMetricState::Available(observation))
            .is_some()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg provider statistics contain a duplicate metric",
            ));
        }
    }
    Ok(requested
        .metrics()
        .iter()
        .map(|metric| {
            let state = available.get(metric).cloned().unwrap_or_else(|| {
                StatisticsMetricState::Missing(StatisticsMissing {
                    kind: StatisticsMissingKind::NotCollected,
                    message: Arc::from(
                        "metric was not present in the published statistics artifact",
                    ),
                })
            });
            (metric.clone(), state)
        })
        .collect())
}

/// A published artifact records values measured by a full visible-row scan, so
/// every metric is exact on its basis — except a Theta sketch, which estimates
/// in both directions no matter how completely it was fed.
fn provider_artifact_numeric_nature(metric: &StatisticsMetric) -> StatisticsNumericNature {
    match metric {
        StatisticsMetric::ThetaNdv { .. } => StatisticsNumericNature::TwoSidedApproximate,
        StatisticsMetric::RowCount
        | StatisticsMetric::NullCount { .. }
        | StatisticsMetric::Minimum { .. }
        | StatisticsMetric::Maximum { .. }
        | StatisticsMetric::AverageSize { .. } => StatisticsNumericNature::Exact,
    }
}

#[derive(Deserialize, Serialize)]
struct IcebergProviderStatisticsV1 {
    version: u16,
    data_version: Vec<u8>,
    metrics: Vec<IcebergProviderStatisticV1>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "metric", rename_all = "snake_case")]
enum IcebergProviderStatisticV1 {
    RowCount {
        value: IcebergStatisticValueV1,
    },
    NullCount {
        column: String,
        value: IcebergStatisticValueV1,
    },
    Minimum {
        column: String,
        value: IcebergStatisticValueV1,
    },
    Maximum {
        column: String,
        value: IcebergStatisticValueV1,
    },
    AverageSize {
        column: String,
        value: IcebergStatisticValueV1,
    },
    ThetaNdv {
        column: String,
        value: IcebergStatisticValueV1,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum IcebergStatisticValueV1 {
    U64(u64),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{StatisticsEvidenceRevision, StatisticsMetricRequest};

    fn theta() -> StatisticsMetric {
        StatisticsMetric::ThetaNdv {
            column: Arc::from("k"),
        }
    }

    fn visible_row_observation(
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
                    visible_row_observation(
                        StatisticsMetricValue::U64(3),
                        version,
                        StatisticsNumericNature::Exact,
                    ),
                ),
                (
                    theta(),
                    visible_row_observation(
                        StatisticsMetricValue::F64(3.0),
                        version,
                        StatisticsNumericNature::TwoSidedApproximate,
                    ),
                ),
            ]),
        )
        .expect("evidence")
    }

    fn observation(state: Option<&StatisticsMetricState>) -> &StatisticsMetricObservation {
        match state {
            Some(StatisticsMetricState::Available(observation)) => observation,
            other => panic!("expected an available metric, got {other:?}"),
        }
    }

    #[test]
    fn provider_artifact_round_trips_only_requested_metrics() {
        let version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("version");
        let evidence = visible_row_evidence(&version);
        let requested = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            theta(),
            StatisticsMetric::NullCount {
                column: Arc::from("k"),
            },
        ])
        .expect("request");

        let decoded = decode_provider_statistics(
            &encode_provider_statistics(&evidence).expect("encode"),
            &version,
            &requested,
        )
        .expect("decode");

        let row_count = observation(decoded.get(&StatisticsMetric::RowCount));
        assert_eq!(row_count.value(), &StatisticsMetricValue::U64(3));
        assert_eq!(
            row_count.numeric_nature(),
            StatisticsNumericNature::Exact,
            "a published artifact records a full visible-row measurement"
        );

        let ndv = observation(decoded.get(&theta()));
        assert_eq!(ndv.value(), &StatisticsMetricValue::F64(3.0));
        assert_eq!(
            ndv.numeric_nature(),
            StatisticsNumericNature::TwoSidedApproximate,
            "a Theta sketch stays approximate even after a full scan"
        );

        for state in [
            decoded.get(&StatisticsMetric::RowCount),
            decoded.get(&theta()),
        ] {
            let observation = observation(state);
            assert_eq!(
                observation.source(),
                &StatisticsMetricSource::ProviderArtifact
            );
            assert_eq!(observation.basis_version(), &version);
            assert_eq!(
                observation.basis_relation(),
                StatisticsBasisRelation::Identical
            );
        }

        assert!(matches!(
            decoded.get(&StatisticsMetric::NullCount {
                column: Arc::from("k")
            }),
            Some(StatisticsMetricState::Missing(StatisticsMissing {
                kind: StatisticsMissingKind::NotCollected,
                ..
            }))
        ));
    }

    #[test]
    fn a_full_visible_row_collection_containing_a_theta_sketch_is_publishable() {
        let version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("version");
        encode_provider_statistics(&visible_row_evidence(&version))
            .expect("numeric exactness is no longer a publication precondition");
    }

    #[test]
    fn partial_coverage_or_a_non_scan_source_cannot_be_published() {
        let version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("version");
        let metrics = BTreeMap::from([(
            StatisticsMetric::RowCount,
            visible_row_observation(
                StatisticsMetricValue::U64(3),
                &version,
                StatisticsNumericNature::Exact,
            ),
        )]);

        let partial = StatisticsEvidence::try_new(
            version.clone(),
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"run-v1")).expect("revision"),
            StatisticsRowCoverage::PartialRows,
            metrics.clone(),
        )
        .expect("evidence");
        assert!(encode_provider_statistics(&partial).is_err());

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
        assert!(encode_provider_statistics(&manifest_derived).is_err());
    }
}
