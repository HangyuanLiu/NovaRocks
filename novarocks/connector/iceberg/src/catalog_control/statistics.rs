// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Exact-generation Iceberg statistics capability.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::DataType;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorStatistics, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, StatisticsBasisRelation,
    StatisticsCollection, StatisticsCollectionPlan, StatisticsCollectionRequest,
    StatisticsDataVersion, StatisticsEvidence, StatisticsEvidenceRevision, StatisticsMetric,
    StatisticsMetricObservation, StatisticsMetricSource, StatisticsMetricState,
    StatisticsMetricValue, StatisticsMissing, StatisticsMissingKind, StatisticsNumericNature,
    StatisticsPublishPreparationRequest, StatisticsPublishRequest, StatisticsReadRequest,
    StatisticsReader, StatisticsReceipt, StatisticsReconcileRequest, StatisticsRowCoverage,
    StatisticsScanColumn,
};
use sha2::{Digest, Sha256};

use crate::control_provider::{IcebergControlProvider, IcebergTablePayload};
use crate::manifest::{DataFileWithStats, extract_data_files_with_stats_at};
use crate::reconcile_payload::{
    ICEBERG_STATISTICS_EVIDENCE_VERSION, IcebergStatisticsEvidenceV1, decode_statistics_evidence,
    encode_statistics_evidence,
};
use crate::statistics_ancestry::{AncestorNdv, resolve_ancestor_ndv};
use crate::statistics_basis::basis_relation;
use crate::statistics_codec::{
    encode_provider_statistics, ensure_publishable_visible_row_evidence, statistics_data_version,
    statistics_metric_column,
};
use crate::stats_assembler::{
    StatisticsCoverageMark, puffin_path_for_statistics_operation,
    write_puffin_with_provider_statistics,
};
use crate::theta_sketch::ThetaSketchHandle;

const STATISTICS_OPERATION_KIND: &str = "statistics-publish";
const VISIBLE_ROW_ARTIFACT_VERSION: u8 = 1;
const THETA_PARTIAL_WIRE_VERSION: u8 = 1;
const THETA_PARTIAL_WIRE_HEADER_BYTES: usize = 14;
const MAX_THETA_RETAINED_HASHES: usize = 1 << 12;

impl StatisticsReader for IcebergControlProvider {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> novarocks_spi::connector::ConnectorInstanceIncarnation {
        self.incarnation()
    }

    fn read_statistics(
        &self,
        request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let table_info = base_table_info(&table, "statistics read")?;
        let expected = pinned_data_version(table_info)?;
        if request.data_version != expected {
            return Err(invalid(
                "Iceberg statistics request does not match its resolved table pin",
            ));
        }
        let snapshot_id = table_info.current_snapshot_id.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg table has no current snapshot for statistics",
            )
        })?;
        let physical = self
            .runtime()
            .load_table(&table.namespace, &table.table)
            .map_err(unavailable)?;
        let metadata = physical.table.metadata();
        // Deliberately no currentness check. The query was planned on this
        // snapshot; the table moving on afterwards does not make that snapshot's
        // statistics wrong, it only makes them describe an older state — which
        // is what the per-metric basis facts are for.

        // Manifest-derivable metrics always come from the snapshot being
        // queried, whether or not a statistics file exists. Letting a published
        // artifact supply them would answer with whichever snapshot ANALYZE
        // happened to measure, and letting its absence blank them out was how a
        // table with no Puffin ended up with no statistics at all.
        let table_for_files = physical.table.clone();
        let files = self
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                extract_data_files_with_stats_at(&table_for_files, snapshot_id).await
            })
            .map_err(unavailable)?
            .map_err(unavailable)?;
        let arrow_schema = crate::iceberg::arrow::schema_to_arrow_schema(metadata.current_schema())
            .map_err(|error| corrupt(format!("convert Iceberg statistics schema: {error}")))?;
        let field_ids = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.to_ascii_lowercase(), field.id))
            .collect::<HashMap<_, _>>();
        let data_types = arrow_schema
            .fields()
            .iter()
            .map(|field| (field.name().to_ascii_lowercase(), field.data_type().clone()))
            .collect::<HashMap<_, _>>();
        // Two independent questions that used to share one boolean. Whether the
        // manifest accounts for every row is a coverage fact; whether delete
        // files make a summed number an over-count is a per-metric numeric
        // fact, and it only bends the metrics it actually affects.
        let row_coverage = if files.iter().all(|file| file.record_count.is_some()) {
            StatisticsRowCoverage::AllVisibleRows
        } else {
            StatisticsRowCoverage::PartialRows
        };
        let has_deletes = files.iter().any(|file| !file.delete_files.is_empty());

        let mut metrics: BTreeMap<StatisticsMetric, StatisticsMetricState> = request
            .metrics
            .metrics()
            .iter()
            .filter(|metric| !matches!(metric, StatisticsMetric::ThetaNdv { .. }))
            .cloned()
            .map(|metric| {
                let state = manifest_metric(&metric, &files, &data_types, has_deletes, &expected);
                (metric, state)
            })
            .collect();

        // NDV lives only in Puffin, and Puffin is published against the snapshot
        // that was measured — so each column searches its own ancestry.
        let wanted_ndv: BTreeMap<i32, StatisticsMetric> = request
            .metrics
            .metrics()
            .iter()
            .filter(|metric| matches!(metric, StatisticsMetric::ThetaNdv { .. }))
            .filter_map(|metric| {
                let column = statistics_metric_column(metric)?;
                let field_id = field_ids.get(&column.to_ascii_lowercase())?;
                Some((*field_id, metric.clone()))
            })
            .collect();
        let resolved_ndv = if wanted_ndv.is_empty() {
            HashMap::new()
        } else {
            let table_for_ndv = physical.table.clone();
            let field_set = wanted_ndv.keys().copied().collect::<BTreeSet<_>>();
            self.runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    resolve_ancestor_ndv(
                        table_for_ndv.metadata(),
                        table_for_ndv.file_io(),
                        snapshot_id,
                        &field_set,
                    )
                    .await
                })
                .map_err(unavailable)?
        };
        let row_count_ceiling = row_count_ceiling(&metrics);
        for metric in request.metrics.metrics() {
            if !matches!(metric, StatisticsMetric::ThetaNdv { .. }) {
                continue;
            }
            let resolved = wanted_ndv
                .iter()
                .find(|(_, wanted)| *wanted == metric)
                .and_then(|(field_id, _)| resolved_ndv.get(field_id));
            let state = match resolved {
                Some(resolved) => {
                    let basis_version = statistics_data_version(
                        table_info
                            .table_uuid
                            .as_deref()
                            .expect("pinned data version requires a table UUID"),
                        Some(resolved.basis_snapshot_id),
                    )?;
                    StatisticsMetricState::Available(StatisticsMetricObservation::new(
                        StatisticsMetricValue::F64(cap_ndv(resolved.ndv, row_count_ceiling)),
                        basis_version,
                        StatisticsMetricSource::ProviderArtifact,
                        // A Theta sketch estimates in both directions however
                        // complete its input was.
                        StatisticsNumericNature::TwoSidedApproximate,
                        basis_relation(metadata, resolved.basis_snapshot_id, snapshot_id),
                    ))
                }
                None => StatisticsMetricState::Missing(StatisticsMissing {
                    kind: StatisticsMissingKind::NotCollected,
                    message: Arc::from("no ancestor snapshot published a sketch for this column"),
                }),
            };
            metrics.insert(metric.clone(), state);
        }

        // The revision identifies the exact set of artifacts behind this answer.
        // Ancestors matter: a statistics file may be replaced on any snapshot in
        // the chain, and the cache must not keep serving the previous one.
        let revision = evidence_revision(
            table_info
                .table_uuid
                .as_deref()
                .expect("pinned data version requires a table UUID"),
            snapshot_id,
            &resolved_ndv,
        )?;
        StatisticsEvidence::try_new(expected, revision, row_coverage, metrics)
    }
}

/// Row count usable as the ceiling for an NDV, when one is available.
///
/// A count over a snapshot with delete files is itself an upper bound, so the
/// cap it provides is loose — but a loose conservative bound still beats an NDV
/// that exceeds the whole table.
fn row_count_ceiling(
    metrics: &BTreeMap<StatisticsMetric, StatisticsMetricState>,
) -> Option<RowCount> {
    match metrics.get(&StatisticsMetric::RowCount) {
        Some(StatisticsMetricState::Available(observation)) => match observation.value() {
            StatisticsMetricValue::U64(rows) => Some(RowCount {
                rows: *rows,
                exact: observation.numeric_nature() == StatisticsNumericNature::Exact,
            }),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Clone, Copy)]
struct RowCount {
    rows: u64,
    exact: bool,
}

/// Keeps a published NDV within what the table can hold.
///
/// A table proven empty has no distinct values, so the usual floor of 1 must
/// not apply — otherwise an empty table reports one distinct value per column.
fn cap_ndv(ndv: f64, row_count: Option<RowCount>) -> f64 {
    let Some(RowCount { rows, exact }) = row_count else {
        return ndv;
    };
    if exact && rows == 0 {
        return 0.0;
    }
    ndv.min(rows as f64).max(1.0)
}

/// Direction of a manifest-derived value against the truth on the queried
/// snapshot.
///
/// Manifest sums do not subtract rows removed by delete files, so with deletes
/// present a count over-reports and the bounds widen — each in its own
/// direction.
fn manifest_numeric_nature(
    metric: &StatisticsMetric,
    has_deletes: bool,
) -> StatisticsNumericNature {
    match metric {
        _ if !has_deletes => StatisticsNumericNature::Exact,
        StatisticsMetric::RowCount
        | StatisticsMetric::NullCount { .. }
        | StatisticsMetric::Maximum { .. } => StatisticsNumericNature::UpperBound,
        StatisticsMetric::Minimum { .. } => StatisticsNumericNature::LowerBound,
        // A ratio of two sums that deletes shrink independently; neither
        // direction is provable. NDV never reaches here.
        StatisticsMetric::AverageSize { .. } | StatisticsMetric::ThetaNdv { .. } => {
            StatisticsNumericNature::TwoSidedApproximate
        }
    }
}

fn evidence_revision(
    table_uuid: &str,
    snapshot_id: i64,
    resolved_ndv: &HashMap<i32, AncestorNdv>,
) -> Result<StatisticsEvidenceRevision, ConnectorError> {
    let mut bases = resolved_ndv
        .iter()
        .map(|(field_id, resolved)| (*field_id, resolved.basis_snapshot_id))
        .collect::<Vec<_>>();
    bases.sort_unstable();
    let mut digest = Sha256::new();
    for (field_id, basis) in bases {
        digest.update(field_id.to_be_bytes());
        digest.update(basis.to_be_bytes());
    }
    let digest = digest.finalize();
    let mut rendered = String::with_capacity(32);
    for byte in &digest[..16] {
        rendered.push_str(&format!("{byte:02x}"));
    }
    StatisticsEvidenceRevision::try_new(Bytes::from(format!(
        "iceberg/v2/{table_uuid}/{snapshot_id}/{rendered}"
    )))
}

impl StatisticsCollection for IcebergControlProvider {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> novarocks_spi::connector::ConnectorInstanceIncarnation {
        self.incarnation()
    }

    fn prepare_collection(
        &self,
        request: StatisticsCollectionRequest,
    ) -> Result<StatisticsCollectionPlan, ConnectorError> {
        validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let table_info = base_table_info(&table, "statistics collection")?;
        let expected = pinned_data_version(table_info)?;
        if request.data_version != expected {
            return Err(invalid(
                "Iceberg statistics collection does not match its resolved table pin",
            ));
        }
        let mut projection = request
            .metrics
            .metrics()
            .iter()
            .filter_map(statistics_metric_column)
            .map(|column| {
                table_info
                    .schema
                    .fields
                    .iter()
                    .position(|field| field.name.eq_ignore_ascii_case(column))
                    .ok_or_else(|| {
                        invalid(format!(
                            "Iceberg statistics column `{column}` is absent from the pinned schema"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        projection.sort_unstable();
        projection.dedup();
        let revision = StatisticsEvidenceRevision::try_new(Bytes::from(format!(
            "iceberg/v1/{}/{}/collection/{}",
            table_info
                .table_uuid
                .as_deref()
                .expect("pinned data version requires a table UUID"),
            table_info.current_snapshot_id.unwrap_or_default(),
            uuid::Uuid::from_bytes(request.operation_id.to_bytes())
        )))?;
        let columns = statistics_scan_layout(&table, &projection)?;
        let provider_payload = request.table.payload().clone();
        StatisticsCollectionPlan::try_new(
            request.table,
            request.data_version,
            revision,
            request.metrics,
            columns,
            provider_payload,
        )
    }

    fn prepare_publish(
        &self,
        request: StatisticsPublishPreparationRequest,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let info = base_table_info(&table, "statistics publication")?;
        let expected = pinned_data_version(info)?;
        if *request.result.evidence.data_version() != expected {
            return Err(invalid(
                "Iceberg statistics publication does not match its resolved table pin",
            ));
        }
        let physical = self
            .runtime()
            .load_table(&table.namespace, &table.table)
            .map_err(unavailable)?;
        // The path is derived from the snapshot the collection measured, not
        // from whatever is current now, so a table that advances between
        // preparation and publication still produces the same evidence.
        let snapshot_id = info
            .current_snapshot_id
            .ok_or_else(|| invalid("cannot publish statistics for a table without a snapshot"))?;
        let path = puffin_path_for_statistics_operation(
            physical.table.metadata(),
            snapshot_id,
            request.operation_id.to_bytes(),
        );
        statistics_evidence(self, request.operation_id, &table, &expected, &path)
    }

    fn publish_statistics(
        &self,
        request: StatisticsPublishRequest,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let info = match base_table_info(&table, "statistics publication") {
            Ok(info) => info,
            Err(error) => return Ok(known_uncommitted(error)),
        };
        let expected = pinned_data_version(info)?;
        if *request.result.evidence.data_version() != expected {
            return Ok(known_uncommitted(invalid(
                "Iceberg statistics publication does not match its resolved table pin",
            )));
        }
        // Publishable means "this measurement saw every visible row of the
        // pinned table", not "every number is exact" — the collection always
        // carries a Theta sketch, which is never exact.
        if let Err(error) = ensure_publishable_visible_row_evidence(&request.result.evidence) {
            return Ok(known_uncommitted(error));
        }
        let (artifact_version, theta) =
            decode_visible_row_artifact(request.result.provider_payload())?;
        if artifact_version != expected {
            return Ok(known_uncommitted(invalid(
                "statistics collection artifact does not match its resolved table pin",
            )));
        }

        let physical = self
            .runtime()
            .load_table(&table.namespace, &table.table)
            .map_err(unavailable)?;
        let metadata = physical.table.metadata();
        // Statistics belong to the snapshot they measured. Requiring that
        // snapshot to still be current is what made ANALYZE fail on any table
        // that took a write while it ran; the ancestry walk on the read side is
        // what makes an older target readable.
        let snapshot_id = info.current_snapshot_id.ok_or_else(|| {
            invalid("cannot publish statistics for a table without a current snapshot")
        })?;
        let Some(snapshot) = metadata.snapshot_by_id(snapshot_id) else {
            // The measured snapshot expired while the collection ran; there is
            // nothing left to attach the result to.
            return Ok(known_uncommitted(invalid(
                "the snapshot these statistics measured is no longer present in table metadata",
            )));
        };
        let sequence_number = snapshot.sequence_number();
        let measured_schema = snapshot.schema(metadata).map_err(|error| {
            corrupt(format!(
                "resolve the schema of the measured Iceberg snapshot: {error}"
            ))
        })?;
        let field_ids = measured_schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.to_ascii_lowercase(), field.id))
            .collect::<HashMap<_, _>>();
        let provider_statistics = encode_provider_statistics(&request.result.evidence, &field_ids)?;
        let mut sketches = HashMap::new();
        for (column, sketch) in theta {
            let field_id = field_ids.get(&column.to_ascii_lowercase()).ok_or_else(|| {
                invalid(format!(
                    "statistics artifact column `{column}` is absent from the pinned Iceberg schema"
                ))
            })?;
            sketches.insert(*field_id, sketch);
        }
        let path = puffin_path_for_statistics_operation(
            metadata,
            snapshot_id,
            request.operation_id.to_bytes(),
        );
        let expected_evidence =
            statistics_evidence(self, request.operation_id, &table, &expected, &path)?;
        if request.evidence != expected_evidence {
            return Ok(known_uncommitted(invalid(
                "Iceberg statistics evidence does not match its pinned operation",
            )));
        }

        let file_io = physical.table.file_io().clone();
        let path_for_write = path.clone();
        let written = self
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                write_puffin_with_provider_statistics(
                    &file_io,
                    &path_for_write,
                    snapshot_id,
                    sequence_number,
                    &sketches,
                    Some(&provider_statistics),
                    // ANALYZE scanned every visible row, which is what gives it
                    // precedence over an incremental entry on the same snapshot.
                    StatisticsCoverageMark::AllVisibleRows,
                )
                .await
            })
            .map_err(unavailable)?
            .map_err(unavailable)?;
        let Some(statistics_file) = written else {
            return statistics_receipt(
                self,
                request.operation_id,
                expected,
                request.result.evidence.evidence_revision().clone(),
                Bytes::from(path),
                ExternalMutationEffect::NoOp,
            );
        };
        let table_for_commit = physical.table.clone();
        let catalog = Arc::clone(self.runtime().catalog());
        let committed = self
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::commit::statistics::commit_statistics_file(
                    &table_for_commit,
                    catalog.as_ref(),
                    statistics_file,
                    StatisticsCoverageMark::AllVisibleRows,
                )
                .await
            });
        match committed {
            Ok(Ok(outcome)) => {
                self.runtime()
                    .control_state()
                    .invalidate_table(&table.namespace, &table.table);
                let effect = match outcome {
                    crate::commit::statistics::StatisticsCommitOutcome::Registered => {
                        ExternalMutationEffect::Applied
                    }
                    // A fuller entry already covers this snapshot. Nothing was
                    // written, and nothing needed to be.
                    crate::commit::statistics::StatisticsCommitOutcome::YieldedToFullerCoverage => {
                        ExternalMutationEffect::NoOp
                    }
                };
                statistics_receipt(
                    self,
                    request.operation_id,
                    expected,
                    request.result.evidence.evidence_revision().clone(),
                    Bytes::from(path),
                    effect,
                )
            }
            Ok(Err(error)) | Err(error) => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Internal,
                    format!("commit Iceberg statistics: {error}"),
                ),
                evidence: request.evidence,
            }),
        }
    }

    fn reconcile_statistics(
        &self,
        request: StatisticsReconcileRequest,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        validate_context(&request.context)?;
        if request.evidence.descriptor() != self.descriptor()
            || request.evidence.incarnation() != self.incarnation()
            || request.evidence.schema_version() != ICEBERG_STATISTICS_EVIDENCE_VERSION
            || request.evidence.operation_kind() != STATISTICS_OPERATION_KIND
        {
            return Err(invalid(
                "Iceberg statistics evidence does not match this exact generation",
            ));
        }
        let evidence = decode_statistics_evidence(request.evidence.provider_payload())
            .map_err(|error| invalid(format!("decode Iceberg statistics evidence: {error}")))?;
        let expected = StatisticsDataVersion::try_new(Bytes::from(evidence.data_version))?;
        if !self
            .runtime()
            .table_exists(&evidence.namespace, &evidence.table)
            .map_err(unavailable)?
        {
            return Ok(known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg table disappeared before statistics publication reconciled",
            )));
        }
        self.runtime()
            .control_state()
            .invalidate_table(&evidence.namespace, &evidence.table);
        let physical = self
            .runtime()
            .load_table(&evidence.namespace, &evidence.table)
            .map_err(unavailable)?;
        // Whether the artifact is registered is the authoritative answer, and it
        // stays authoritative however far the table has moved since.
        if physical
            .table
            .metadata()
            .statistics_iter()
            .any(|file| file.statistics_path == evidence.statistics_path)
        {
            return statistics_receipt(
                self,
                request.evidence.operation_id(),
                expected,
                StatisticsEvidenceRevision::try_new(Bytes::from(format!(
                    "iceberg/statistics/v1/{}",
                    evidence.statistics_path
                )))?,
                Bytes::from(evidence.statistics_path),
                ExternalMutationEffect::Applied,
            );
        }
        Ok(known_uncommitted(invalid(
            "Iceberg statistics artifact is not registered in table metadata",
        )))
    }
}

impl ConnectorStatistics for IcebergControlProvider {
    fn collection(&self) -> Option<&dyn StatisticsCollection> {
        Some(self)
    }
}

fn base_table_info<'a>(
    table: &'a IcebergTablePayload,
    operation: &str,
) -> Result<&'a crate::scan_model::IcebergTableInfo, ConnectorError> {
    if table.metadata_table_type.is_some() {
        return Err(invalid(format!(
            "Iceberg {operation} requires a base table handle"
        )));
    }
    table.table_info.as_ref().ok_or_else(|| {
        invalid(format!(
            "Iceberg {operation} requires a resolved base table payload"
        ))
    })
}

fn pinned_data_version(
    info: &crate::scan_model::IcebergTableInfo,
) -> Result<StatisticsDataVersion, ConnectorError> {
    statistics_data_version(
        info.table_uuid
            .as_deref()
            .ok_or_else(|| corrupt("Iceberg table payload is missing its table UUID"))?,
        info.current_snapshot_id,
    )
}

fn statistics_scan_layout(
    table: &IcebergTablePayload,
    projection: &[usize],
) -> Result<Vec<StatisticsScanColumn>, ConnectorError> {
    let info = base_table_info(table, "statistics collection")?;
    let serialized = info.serialized_metadata.as_deref().ok_or_else(|| {
        corrupt("Iceberg statistics collection payload is missing serialized pinned metadata")
    })?;
    let metadata: crate::iceberg::spec::TableMetadata = serde_json::from_str(serialized)
        .map_err(|error| corrupt(format!("decode pinned Iceberg metadata: {error}")))?;
    if metadata.current_schema_id() != info.schema_id {
        return Err(corrupt(
            "Iceberg statistics metadata does not match its pinned schema ID",
        ));
    }
    let schema = crate::iceberg::arrow::schema_to_arrow_schema(metadata.current_schema())
        .map_err(|error| corrupt(format!("convert pinned Iceberg schema: {error}")))?;
    projection
        .iter()
        .map(|&ordinal| {
            let field = schema.fields().get(ordinal).ok_or_else(|| {
                invalid(format!(
                    "Iceberg statistics projection index {ordinal} is outside the pinned schema"
                ))
            })?;
            let data_type = match table
                .logical_type_columns
                .get(&field.name().to_ascii_lowercase())
                .map(String::as_str)
            {
                Some("bitmap") | Some("hll") => DataType::Binary,
                _ => field.data_type().clone(),
            };
            StatisticsScanColumn::try_new(
                ordinal,
                Arc::<str>::from(field.name().as_str()),
                data_type,
                field.is_nullable(),
            )
        })
        .collect()
}

fn manifest_metric(
    metric: &StatisticsMetric,
    files: &[DataFileWithStats],
    data_types: &HashMap<String, DataType>,
    has_deletes: bool,
    basis_version: &StatisticsDataVersion,
) -> StatisticsMetricState {
    // Every manifest-derived value describes the queried snapshot itself, so
    // the basis relation is identity; only the numeric direction varies.
    let available = |value: StatisticsMetricValue| {
        StatisticsMetricState::Available(StatisticsMetricObservation::new(
            value,
            basis_version.clone(),
            StatisticsMetricSource::CurrentManifest,
            manifest_numeric_nature(metric, has_deletes),
            StatisticsBasisRelation::Identical,
        ))
    };
    match metric {
        StatisticsMetric::RowCount => files
            .iter()
            .try_fold(0_u64, |total, file| {
                total.checked_add(u64::try_from(file.record_count?).ok()?)
            })
            .map(|value| available(StatisticsMetricValue::U64(value)))
            .unwrap_or_else(|| incomplete("Iceberg manifest does not report every row count")),
        StatisticsMetric::NullCount { column } => files
            .iter()
            .try_fold(0_u64, |total, file| {
                let count = column_stats(file, column)?.null_count?;
                total.checked_add(u64::try_from(count).ok()?)
            })
            .map(|value| available(StatisticsMetricValue::U64(value)))
            .unwrap_or_else(|| {
                incomplete(format!(
                    "Iceberg manifest does not report a null count for `{column}`"
                ))
            }),
        StatisticsMetric::AverageSize { column } => {
            let total_rows = files.iter().try_fold(0_u64, |total, file| {
                total.checked_add(u64::try_from(file.record_count?).ok()?)
            });
            let total_size = files.iter().try_fold(0_u64, |total, file| {
                total.checked_add(u64::try_from(column_stats(file, column)?.column_size?).ok()?)
            });
            match (total_rows, total_size) {
                (Some(rows), Some(size)) => available(StatisticsMetricValue::F64(if rows == 0 {
                    0.0
                } else {
                    size as f64 / rows as f64
                })),
                _ => missing_column(column),
            }
        }
        StatisticsMetric::Minimum { column } | StatisticsMetric::Maximum { column } => {
            let lower = matches!(metric, StatisticsMetric::Minimum { .. });
            let Some(data_type) = data_types.get(&column.to_ascii_lowercase()) else {
                return missing_column(column);
            };
            let values = files.iter().map(|file| {
                let stats = column_stats(file, column)?;
                let bytes = if lower {
                    stats.lower_bound.as_deref()?
                } else {
                    stats.upper_bound.as_deref()?
                };
                decode_bound_to_f64(bytes, data_type).filter(|value| value.is_finite())
            });
            let reduced = values.fold(Some(None), |state, value| match (state, value) {
                (Some(None), Some(value)) => Some(Some(value)),
                (Some(Some(current)), Some(value)) => Some(Some(if lower {
                    current.min(value)
                } else {
                    current.max(value)
                })),
                _ => None,
            });
            match reduced.flatten() {
                Some(value) => available(StatisticsMetricValue::F64(value)),
                None => missing_column(column),
            }
        }
        // NDV is never manifest-derivable; it comes from Puffin, possibly from
        // an ancestor snapshot, and is assembled by the caller.
        StatisticsMetric::ThetaNdv { .. } => {
            incomplete("Iceberg NDV is resolved from Puffin, not from the manifest")
        }
    }
}

fn column_stats<'a>(
    file: &'a DataFileWithStats,
    column: &str,
) -> Option<&'a crate::scan_model::IcebergColumnStats> {
    file.column_stats
        .as_ref()?
        .iter()
        .find_map(|(name, stats)| name.eq_ignore_ascii_case(column).then_some(stats))
}

fn decode_bound_to_f64(bytes: &[u8], data_type: &DataType) -> Option<f64> {
    match data_type {
        DataType::Boolean => match bytes {
            [0] => Some(0.0),
            [1] => Some(1.0),
            _ => None,
        },
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Date32
        | DataType::Time32(_) => Some(i32::from_le_bytes(bytes.try_into().ok()?) as f64),
        DataType::Int64
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time64(_)
        | DataType::Duration(_) => Some(i64::from_le_bytes(bytes.try_into().ok()?) as f64),
        DataType::Float32 => Some(f32::from_le_bytes(bytes.try_into().ok()?) as f64),
        DataType::Float64 => Some(f64::from_le_bytes(bytes.try_into().ok()?)),
        DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) => {
            if bytes.is_empty() || bytes.len() > 16 {
                return None;
            }
            let mut padded = [if bytes[0] & 0x80 != 0 { 0xff } else { 0 }; 16];
            padded[16 - bytes.len()..].copy_from_slice(bytes);
            Some(i128::from_be_bytes(padded) as f64 / 10_f64.powi(*scale as i32))
        }
        _ => None,
    }
}

fn decode_visible_row_artifact(
    bytes: &[u8],
) -> Result<(StatisticsDataVersion, BTreeMap<String, ThetaSketchHandle>), ConnectorError> {
    let mut cursor = 0usize;
    let version = take(bytes, &mut cursor, 1)?[0];
    if version != VISIBLE_ROW_ARTIFACT_VERSION {
        return Err(corrupt(
            "statistics visible-row artifact has an unsupported version",
        ));
    }
    let data_version_len = u16::from_be_bytes(
        take(bytes, &mut cursor, 2)?
            .try_into()
            .expect("fixed field width"),
    ) as usize;
    let data_version = StatisticsDataVersion::try_new(Bytes::copy_from_slice(take(
        bytes,
        &mut cursor,
        data_version_len,
    )?))?;
    let count = u16::from_be_bytes(
        take(bytes, &mut cursor, 2)?
            .try_into()
            .expect("fixed field width"),
    ) as usize;
    let mut sketches = BTreeMap::new();
    for _ in 0..count {
        let name_len = u16::from_be_bytes(
            take(bytes, &mut cursor, 2)?
                .try_into()
                .expect("fixed field width"),
        ) as usize;
        let name = std::str::from_utf8(take(bytes, &mut cursor, name_len)?)
            .map_err(|_| corrupt("statistics artifact column is not UTF-8"))?;
        if name.is_empty() {
            return Err(corrupt("statistics artifact column name is empty"));
        }
        let sketch_len = u32::from_be_bytes(
            take(bytes, &mut cursor, 4)?
                .try_into()
                .expect("fixed field width"),
        ) as usize;
        let sketch = decode_theta_partial(take(bytes, &mut cursor, sketch_len)?)?;
        if sketches.insert(name.to_string(), sketch).is_some() {
            return Err(corrupt(
                "statistics artifact contains a duplicate Theta column",
            ));
        }
    }
    if cursor != bytes.len() {
        return Err(corrupt(
            "statistics visible-row artifact has trailing bytes",
        ));
    }
    Ok((data_version, sketches))
}

fn decode_theta_partial(bytes: &[u8]) -> Result<ThetaSketchHandle, ConnectorError> {
    if bytes.len() < THETA_PARTIAL_WIRE_HEADER_BYTES || bytes[0] != THETA_PARTIAL_WIRE_VERSION {
        return Err(corrupt("statistics Theta state is invalid"));
    }
    let lg_k = bytes[1];
    if !(5..=12).contains(&lg_k) {
        return Err(corrupt("statistics Theta state has an invalid lg_k"));
    }
    let theta = u64::from_be_bytes(bytes[2..10].try_into().expect("fixed field width"));
    let count = u32::from_be_bytes(bytes[10..14].try_into().expect("fixed field width")) as usize;
    if count > MAX_THETA_RETAINED_HASHES
        || bytes.len() != THETA_PARTIAL_WIRE_HEADER_BYTES + count * 8
    {
        return Err(corrupt("statistics Theta state has an invalid length"));
    }
    let hashes = bytes[THETA_PARTIAL_WIRE_HEADER_BYTES..]
        .chunks_exact(8)
        .map(|chunk| u64::from_be_bytes(chunk.try_into().expect("exact chunks")))
        .collect::<Vec<_>>();
    ThetaSketchHandle::from_compact_parts(lg_k, theta, hashes).map_err(corrupt)
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, count: usize) -> Result<&'a [u8], ConnectorError> {
    let end = cursor
        .checked_add(count)
        .ok_or_else(|| corrupt("statistics artifact length overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| corrupt("statistics artifact is truncated"))?;
    *cursor = end;
    Ok(value)
}

fn statistics_evidence(
    provider: &IcebergControlProvider,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    table: &IcebergTablePayload,
    data_version: &StatisticsDataVersion,
    path: &str,
) -> Result<ExternalMutationEvidence, ConnectorError> {
    let payload = encode_statistics_evidence(&IcebergStatisticsEvidenceV1 {
        version: ICEBERG_STATISTICS_EVIDENCE_VERSION,
        namespace: table.namespace.clone(),
        table: table.table.clone(),
        data_version: data_version.as_bytes().to_vec(),
        statistics_path: path.to_string(),
    })
    .map_err(internal)?;
    ExternalMutationEvidence::try_new(
        ICEBERG_STATISTICS_EVIDENCE_VERSION,
        provider.descriptor().clone(),
        provider.incarnation(),
        operation_id,
        STATISTICS_OPERATION_KIND,
        Bytes::from(payload),
    )
}

fn statistics_receipt(
    provider: &IcebergControlProvider,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    data_version: StatisticsDataVersion,
    revision: StatisticsEvidenceRevision,
    payload: Bytes,
    effect: ExternalMutationEffect,
) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
    Ok(ExternalMutationOutcome::KnownCommitted {
        effect,
        receipt: StatisticsReceipt::try_new(
            provider.descriptor().clone(),
            provider.incarnation(),
            operation_id,
            data_version,
            revision,
            payload,
        )?,
        finalization: ExternalMutationFinalization::Complete,
    })
}

fn known_uncommitted(error: ConnectorError) -> ExternalMutationOutcome<StatisticsReceipt> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: ConnectorMutationFailure::new(failure_kind(error.kind()), error.to_string()),
    }
}

fn failure_kind(kind: ConnectorErrorKind) -> ConnectorMutationFailureKind {
    match kind {
        ConnectorErrorKind::InvalidRequest => ConnectorMutationFailureKind::InvalidRequest,
        ConnectorErrorKind::NotFound => ConnectorMutationFailureKind::NotFound,
        ConnectorErrorKind::PermissionDenied => ConnectorMutationFailureKind::PermissionDenied,
        ConnectorErrorKind::Unsupported => ConnectorMutationFailureKind::Unsupported,
        ConnectorErrorKind::Cancelled => ConnectorMutationFailureKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded => ConnectorMutationFailureKind::DeadlineExceeded,
        ConnectorErrorKind::ResourceExhausted => ConnectorMutationFailureKind::ResourceExhausted,
        ConnectorErrorKind::Unavailable => ConnectorMutationFailureKind::Unavailable,
        ConnectorErrorKind::CorruptData => ConnectorMutationFailureKind::CorruptData,
        ConnectorErrorKind::Internal => ConnectorMutationFailureKind::Internal,
    }
}

fn validate_context(
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

fn missing_column(column: &str) -> StatisticsMetricState {
    StatisticsMetricState::Missing(StatisticsMissing {
        kind: StatisticsMissingKind::NotCollected,
        message: Arc::from(format!(
            "statistics for column `{column}` are not collected"
        )),
    })
}

fn incomplete(message: impl Into<Arc<str>>) -> StatisticsMetricState {
    StatisticsMetricState::Missing(StatisticsMissing {
        kind: StatisticsMissingKind::IncompleteEvidence,
        message: message.into(),
    })
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorMutationOperationId, ConnectorProviderId,
        ConnectorRequestContext,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::control_runtime::IcebergControlRuntime;
    use crate::resources::IcebergControlResources;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("context")
    }

    fn provider() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergControlProvider,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = Arc::new(
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                IcebergControlResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let provider = IcebergControlProvider::new(
            descriptor,
            ConnectorInstanceIncarnation::from_bytes([4; 16]),
            runtime,
        );
        (executor, warehouse, provider)
    }

    fn table_payload() -> IcebergTablePayload {
        IcebergTablePayload {
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_info: None,
            metadata_columns: Vec::new(),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            row_mutation_frozen_source: false,
            logical_type_columns: BTreeMap::new(),
            hidden_columns: Vec::new(),
        }
    }

    #[test]
    fn rejects_non_canonical_theta_state() {
        let mut bytes = vec![THETA_PARTIAL_WIRE_VERSION, 12];
        bytes.extend_from_slice(&u64::MAX.to_be_bytes());
        bytes.extend_from_slice(&2_u32.to_be_bytes());
        bytes.extend_from_slice(&9_u64.to_be_bytes());
        bytes.extend_from_slice(&9_u64.to_be_bytes());
        assert!(decode_theta_partial(&bytes).is_err());
    }

    fn manifest_file(
        record_count: Option<i64>,
        column: &str,
        has_deletes: bool,
    ) -> DataFileWithStats {
        DataFileWithStats {
            path: "data.parquet".to_string(),
            size: 64,
            record_count,
            column_stats: Some(HashMap::from([(
                column.to_string(),
                crate::scan_model::IcebergColumnStats {
                    field_id: Some(1),
                    null_count: Some(0),
                    value_count: record_count,
                    column_size: Some(32),
                    lower_bound: Some(1_i32.to_le_bytes().to_vec()),
                    upper_bound: Some(9_i32.to_le_bytes().to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            partition_values: None,
            manifest_path: None,
            partition_field_values: Vec::new(),
            first_row_id: None,
            data_sequence_number: None,
            delete_files: if has_deletes {
                vec![crate::scan_model::IcebergDeleteFileInfo {
                    path: "delete.parquet".to_string(),
                    file_format: crate::scan_model::IcebergDeleteFileFormat::Parquet,
                    file_content: crate::scan_model::IcebergDeleteFileContent::Position,
                    length: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                    sequence_number: None,
                    partition_spec_id: None,
                    partition_key: None,
                    equality_column_names: Vec::new(),
                    equality_field_ids: Vec::new(),
                }]
            } else {
                Vec::new()
            },
        }
    }

    fn manifest_nature(
        metric: &StatisticsMetric,
        has_deletes: bool,
    ) -> Option<StatisticsNumericNature> {
        let files = vec![manifest_file(Some(4), "k", has_deletes)];
        let data_types = HashMap::from([("k".to_string(), DataType::Int32)]);
        let field_ids = HashMap::from([("k".to_string(), 1_i32)]);
        let ndv = HashMap::from([(1_i32, 3.0_f64)]);
        let basis =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("data version");
        match manifest_metric(metric, &files, &data_types, has_deletes, &basis) {
            StatisticsMetricState::Available(observation) => Some(observation.numeric_nature()),
            _ => None,
        }
    }

    #[test]
    fn manifest_row_count_requires_every_file() {
        let files = vec![manifest_file(None, "k", false)];
        let basis =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("data version");
        assert!(matches!(
            manifest_metric(
                &StatisticsMetric::RowCount,
                &files,
                &HashMap::new(),
                false,
                &basis,
            ),
            StatisticsMetricState::Missing(_)
        ));
    }

    /// A sketch can legitimately estimate above the table's own size, and a
    /// published value can outlive the rows it counted. Neither may reach the
    /// optimizer as-is.
    #[test]
    fn a_published_ndv_is_kept_within_what_the_table_can_hold() {
        let bounded = Some(RowCount {
            rows: 10,
            exact: true,
        });
        assert_eq!(
            cap_ndv(1_000.0, bounded),
            10.0,
            "an NDV cannot exceed the rows"
        );
        assert_eq!(
            cap_ndv(4.0, bounded),
            4.0,
            "an NDV within the table is kept"
        );

        // A count over a snapshot with delete files is itself an upper bound, so
        // the cap is loose — but still worth applying.
        assert_eq!(
            cap_ndv(
                1_000.0,
                Some(RowCount {
                    rows: 10,
                    exact: false
                })
            ),
            10.0
        );

        assert_eq!(
            cap_ndv(7.0, None),
            7.0,
            "with no row count there is nothing to cap against"
        );
    }

    /// The floor of one distinct value is there so a non-empty column never
    /// reports zero. A table proven empty is the one case where zero is right.
    #[test]
    fn an_empty_table_reports_no_distinct_values() {
        assert_eq!(
            cap_ndv(
                3.0,
                Some(RowCount {
                    rows: 0,
                    exact: true
                })
            ),
            0.0
        );
        assert_eq!(
            cap_ndv(
                3.0,
                Some(RowCount {
                    rows: 0,
                    exact: false
                })
            ),
            1.0,
            "a row count that is only an upper bound does not prove emptiness"
        );
    }

    #[test]
    fn without_delete_files_every_manifest_metric_is_exact() {
        let column: Arc<str> = Arc::from("k");
        for metric in [
            StatisticsMetric::RowCount,
            StatisticsMetric::NullCount {
                column: Arc::clone(&column),
            },
            StatisticsMetric::Minimum {
                column: Arc::clone(&column),
            },
            StatisticsMetric::Maximum {
                column: Arc::clone(&column),
            },
            StatisticsMetric::AverageSize {
                column: Arc::clone(&column),
            },
        ] {
            assert_eq!(
                manifest_nature(&metric, false),
                Some(StatisticsNumericNature::Exact),
                "{metric:?} is exact when no rows are hidden by delete files"
            );
        }
        // NDV is not a manifest fact at all: it lives in Puffin and is resolved
        // from the snapshot ancestry, so asking the manifest for it yields
        // nothing rather than a value.
        assert_eq!(
            manifest_nature(
                &StatisticsMetric::ThetaNdv {
                    column: Arc::clone(&column)
                },
                false
            ),
            None
        );
    }

    #[test]
    fn delete_files_bend_each_manifest_metric_in_its_own_direction() {
        let column: Arc<str> = Arc::from("k");
        // Sums do not subtract deleted rows, so they over-report.
        assert_eq!(
            manifest_nature(&StatisticsMetric::RowCount, true),
            Some(StatisticsNumericNature::UpperBound)
        );
        assert_eq!(
            manifest_nature(
                &StatisticsMetric::NullCount {
                    column: Arc::clone(&column)
                },
                true
            ),
            Some(StatisticsNumericNature::UpperBound)
        );
        // The true minimum can only rise and the true maximum can only fall
        // when rows disappear, so the bounds stay valid in opposite directions.
        assert_eq!(
            manifest_nature(
                &StatisticsMetric::Minimum {
                    column: Arc::clone(&column)
                },
                true
            ),
            Some(StatisticsNumericNature::LowerBound)
        );
        assert_eq!(
            manifest_nature(
                &StatisticsMetric::Maximum {
                    column: Arc::clone(&column)
                },
                true
            ),
            Some(StatisticsNumericNature::UpperBound)
        );
    }

    #[test]
    fn delete_files_do_not_reduce_manifest_row_coverage() {
        // Coverage answers "did the measurement account for every visible row",
        // which a full manifest read does whether or not deletes exist. The
        // delete-file effect belongs to numeric nature, not to coverage.
        let files = vec![manifest_file(Some(4), "k", true)];
        assert!(
            files.iter().all(|file| file.record_count.is_some()),
            "the coverage predicate must not consult delete files"
        );
    }

    #[test]
    fn response_loss_evidence_is_deterministic_and_exact_generation_bound() {
        let (_executor, _warehouse, provider) = provider();
        let operation_id = ConnectorMutationOperationId::new();
        let data_version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("data version");
        let first = statistics_evidence(
            &provider,
            operation_id,
            &table_payload(),
            &data_version,
            "s3://warehouse/db/t/metadata/stats.puffin",
        )
        .expect("evidence");
        let second = statistics_evidence(
            &provider,
            operation_id,
            &table_payload(),
            &data_version,
            "s3://warehouse/db/t/metadata/stats.puffin",
        )
        .expect("evidence replay");
        assert_eq!(first, second);
        let decoded = decode_statistics_evidence(first.provider_payload()).expect("decode");
        assert_eq!(decoded.namespace, "db");
        assert_eq!(decoded.table, "t");
        assert_eq!(
            decoded.statistics_path,
            "s3://warehouse/db/t/metadata/stats.puffin"
        );

        let foreign = ExternalMutationEvidence::try_new(
            ICEBERG_STATISTICS_EVIDENCE_VERSION,
            provider.descriptor().clone(),
            ConnectorInstanceIncarnation::from_bytes([5; 16]),
            operation_id,
            STATISTICS_OPERATION_KIND,
            first.provider_payload().clone(),
        )
        .expect("foreign evidence");
        let error = provider
            .reconcile_statistics(StatisticsReconcileRequest {
                evidence: foreign,
                context: context(),
            })
            .expect_err("foreign generation must be rejected before table access");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("exact generation"));
    }

    #[test]
    fn malformed_reconcile_evidence_fails_before_catalog_access() {
        let (_executor, _warehouse, provider) = provider();
        let evidence = ExternalMutationEvidence::try_new(
            ICEBERG_STATISTICS_EVIDENCE_VERSION,
            provider.descriptor().clone(),
            provider.incarnation(),
            ConnectorMutationOperationId::new(),
            STATISTICS_OPERATION_KIND,
            Bytes::from_static(b"not-json"),
        )
        .expect("evidence envelope");
        let error = provider
            .reconcile_statistics(StatisticsReconcileRequest {
                evidence,
                context: context(),
            })
            .expect_err("malformed evidence must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("decode Iceberg statistics evidence")
        );
    }
}
