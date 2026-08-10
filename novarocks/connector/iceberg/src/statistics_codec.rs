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
    ConnectorError, ConnectorErrorKind, StatisticsAccuracy, StatisticsCoverage,
    StatisticsDataVersion, StatisticsEvidence, StatisticsMetric, StatisticsMetricState,
    StatisticsMetricValue, StatisticsMissing, StatisticsMissingKind, StatisticsProvenance,
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

pub fn encode_provider_statistics(
    evidence: &StatisticsEvidence,
) -> Result<Vec<u8>, ConnectorError> {
    if evidence.coverage != StatisticsCoverage::Full
        || evidence.accuracy != StatisticsAccuracy::Exact
        || evidence.provenance != StatisticsProvenance::VisibleRows
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg provider statistics require Full Exact visible-row evidence",
        ));
    }
    let metrics = evidence
        .metrics
        .iter()
        .map(|(metric, state)| {
            let StatisticsMetricState::Available(value) = state else {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg provider statistics cannot persist unavailable metrics",
                ));
            };
            let value = match value {
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
        data_version: evidence.data_version.as_bytes().to_vec(),
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
        if available
            .insert(metric, StatisticsMetricState::Available(value))
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

    #[test]
    fn provider_artifact_round_trips_only_requested_metrics() {
        let version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("version");
        let theta = StatisticsMetric::ThetaNdv {
            column: Arc::from("k"),
        };
        let evidence = StatisticsEvidence {
            data_version: version.clone(),
            evidence_revision: StatisticsEvidenceRevision::try_new(Bytes::from_static(b"run-v1"))
                .expect("revision"),
            coverage: StatisticsCoverage::Full,
            accuracy: StatisticsAccuracy::Exact,
            interval: None,
            provenance: StatisticsProvenance::VisibleRows,
            metrics: BTreeMap::from([
                (
                    StatisticsMetric::RowCount,
                    StatisticsMetricState::Available(StatisticsMetricValue::U64(3)),
                ),
                (
                    theta.clone(),
                    StatisticsMetricState::Available(StatisticsMetricValue::F64(3.0)),
                ),
            ]),
        };
        let requested = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            theta.clone(),
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
        assert_eq!(
            decoded.get(&StatisticsMetric::RowCount),
            Some(&StatisticsMetricState::Available(
                StatisticsMetricValue::U64(3)
            ))
        );
        assert_eq!(
            decoded.get(&theta),
            Some(&StatisticsMetricState::Available(
                StatisticsMetricValue::F64(3.0)
            ))
        );
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
}
