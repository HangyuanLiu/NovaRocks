// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The original SQL statistics read, outside its borrowed I/O and CPU waiters.
use super::*;
use crate::read_snapshot::cow_capture::{CaptureBudget, ReadFailure};
use novarocks_spi::connector::{
    ConnectorMetadata, ConnectorOriginalResultScope, ConnectorTableMetadata, ConnectorTableRequest,
    OriginalResultCheckClass, StatisticsMetricRequest, StatisticsReadOriginal,
};

pub(super) struct ReadBody {
    provider: Option<IcebergMetadata>,
    request: Option<ConnectorTableRequest>,
    facts: Option<crate::metadata::IcebergPreparedTableFacts>,
    metadata: Option<ConnectorTableMetadata>,
    read_request: Option<StatisticsReadRequest>,
    files: Option<Vec<crate::read_model::IcebergReadFile>>,
    result: Option<Result<StatisticsEvidence, ConnectorError>>,
    pub(super) phase: u8,
    snapshot: Option<Arc<IcebergMetadataCpuOriginal>>,
    upper: u64,
    required: u64,
    payload: Option<IcebergTablePayload>,
    snapshot_id: Option<i64>,
    retiring: bool,
    closed: bool,
    // Last: every first-party/SDK input is disposed before this paid owner.
    original: ConnectorOriginalResultScope,
}
const READ_QUOTA_DETAIL: &str = "statistics read original payload check failed";
#[derive(Debug)]
struct ReadQuotaFailure {
    kind: ConnectorErrorKind,
    original: novarocks_spi::connector::OriginalResultCheckError,
}
impl std::fmt::Display for ReadQuotaFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(READ_QUOTA_DETAIL)
    }
}
impl std::error::Error for ReadQuotaFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}
impl novarocks_failure_contract::FailureProjection for ReadQuotaFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::FailureProjection;
        self.kind.failure_fact()
    }
}
pub(super) fn checked_read(error: ReadFailure) -> ConnectorError {
    use novarocks_spi::connector::ConnectorCowBeginCause;
    match error.into_cause() {
        ConnectorCowBeginCause::Provider(error) => error,
        ConnectorCowBeginCause::OriginalResult(error) => {
            let kind = match error.class() {
                OriginalResultCheckClass::Control => ConnectorErrorKind::Interrupted,
                OriginalResultCheckClass::Admission | OriginalResultCheckClass::Coverage => {
                    ConnectorErrorKind::ResourceExhausted
                }
                OriginalResultCheckClass::Identity => ConnectorErrorKind::InvalidRequest,
            };
            ConnectorError::from_domain_static(
                kind,
                ReadQuotaFailure {
                    kind,
                    original: error,
                },
                READ_QUOTA_DETAIL,
            )
        }
    }
}
impl ReadBody {
    pub(super) fn run(&mut self) {
        // This cumulative prospective bound is checked against the same paid
        // original payload. It is never a second allowance or a window size.
        let original_scope = self.original.clone();
        let budget = match CaptureBudget::new(&original_scope, self.upper) {
            Ok(value) => value,
            Err(error) => {
                self.result = Some(Err(checked_read(error)));
                return;
            }
        };
        let result = match self.phase {
            0 => self.preflight(&budget),
            1 => self.prepare_request(&budget),
            2 => self.preflight_evidence(&budget),
            3 => self.project(&budget),
            _ => Err(invalid("statistics read CPU phase was dispatched twice")),
        };
        self.upper = budget.upper();
        if let Err(error) = result {
            tracing::debug!(
                phase = self.phase,
                retained_upper = self.upper,
                prospective_total = self.required,
                "statistics read CPU phase refused"
            );
            self.result = Some(Err(error));
        }
    }
    fn preflight(&mut self, budget: &CaptureBudget<'_>) -> Result<(), ConnectorError> {
        let table = IcebergMetadata::statistics_read_physical(self.facts.as_ref().unwrap());
        let schema = table.metadata().current_schema();
        let schema_bytes = budget.json_size(schema.as_ref()).map_err(checked_read)?;
        let metadata_bytes = budget.json_size(table.metadata()).map_err(checked_read)?;
        let mut amount = CaptureBudget::json_working_upper(schema_bytes).map_err(checked_read)?;
        // create_table_from_observation serializes the full metadata into the
        // original handle; table_payload decodes that handle into our receiver.
        // The pinned JSON recipe covers these first-party escaped buffers and
        // parsed nodes, not the already-borrowed SDK Table backing.
        amount = plus(
            amount,
            CaptureBudget::json_working_upper(metadata_bytes).map_err(checked_read)?,
        )?;
        amount = plus(
            amount,
            budget.schema_indexes_upper(schema).map_err(checked_read)?,
        )?;
        // The exact physical top-level fields bound the visible selection.
        // Hidden pseudo-columns do not enter SHOW's metric request.
        amount = plus(
            amount,
            metric_request_upper(schema.as_struct().fields().iter().map(|f| f.name.as_str()))?,
        )?;
        self.required = plus(budget.upper(), amount)?;
        Ok(())
    }
    fn prepare_request(&mut self, budget: &CaptureBudget<'_>) -> Result<(), ConnectorError> {
        let facts = self.facts.as_ref().expect("original prepared read facts");
        // Consume the complete original phase receipt before schema, handle,
        // request Vec, name Arc or duplicate-check trees are constructed.
        budget
            .charge(
                self.required
                    .checked_sub(budget.upper())
                    .ok_or_else(|| invalid("statistics request receipt regressed"))?,
            )
            .map_err(checked_read)?;
        self.metadata = Some(
            self.provider
                .as_ref()
                .unwrap()
                .statistics_read_metadata(facts)?,
        );
        let metadata = self.metadata.as_ref().expect("original metadata receiver");
        self.payload = Some(
            self.provider
                .as_ref()
                .unwrap()
                .table_payload(&metadata.table)?,
        );
        self.snapshot_id =
            base_table_info(self.payload.as_ref().unwrap(), "statistics read")?.current_snapshot_id;
        let data_version = metadata
            .statistics_data_version
            .clone()
            .ok_or_else(|| invalid("statistics read metadata has no pinned data version"))?;
        let schema = metadata.schema();
        let columns = || {
            schema.fields().iter().filter(|field| {
                field
                    .metadata()
                    .get(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                    .is_none_or(|value| !value.eq_ignore_ascii_case("true"))
            })
        };
        let count = columns()
            .count()
            .checked_mul(5)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| invalid("statistics read metric count overflow"))?;
        if count.saturating_sub(1) > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_METRICS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "SHOW TABLE STATS exceeds the supported column metric count",
            ));
        }
        let mut metrics = Vec::with_capacity(count);
        metrics.push(StatisticsMetric::RowCount);
        for column in columns() {
            let column: Arc<str> = Arc::from(column.name().as_str());
            metrics.extend([
                StatisticsMetric::NullCount {
                    column: column.clone(),
                },
                StatisticsMetric::Minimum {
                    column: column.clone(),
                },
                StatisticsMetric::Maximum {
                    column: column.clone(),
                },
                StatisticsMetric::AverageSize {
                    column: column.clone(),
                },
                StatisticsMetric::ThetaNdv { column },
            ]);
        }
        self.read_request = Some(StatisticsReadRequest {
            narrowed_read: None,
            table: metadata.table.clone(),
            data_version,
            metrics: StatisticsMetricRequest::try_new(metrics)?,
            context: self.request.as_ref().unwrap().context.clone(),
        });
        Ok(())
    }
    fn preflight_evidence(&mut self, budget: &CaptureBudget<'_>) -> Result<(), ConnectorError> {
        let table = IcebergMetadata::statistics_read_physical(self.facts.as_ref().unwrap());
        let request = self.read_request.as_ref().unwrap();
        let info = base_table_info(self.payload.as_ref().unwrap(), "statistics read")?;
        let amount = evidence_formation_upper(
            budget,
            table.metadata(),
            self.metadata.as_ref().unwrap().schema(),
            request,
            info.table_uuid
                .as_deref()
                .ok_or_else(|| invalid("statistics table UUID is absent"))?,
        )?;
        self.required = plus(budget.upper(), amount)?;
        Ok(())
    }
    fn project(&mut self, budget: &CaptureBudget<'_>) -> Result<(), ConnectorError> {
        let request = self.read_request.as_ref().expect("original read request");
        let facts = self.facts.as_ref().expect("original physical table");
        let table = IcebergMetadata::statistics_read_physical(facts);
        let info = base_table_info(self.payload.as_ref().unwrap(), "statistics read")?;
        let snapshot = info.current_snapshot_id.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg table has no current snapshot for statistics",
            )
        })?;
        // Schema indexes, selected metric BTree nodes, name maps, ancestry
        // entries and escaped diagnostics are first-party new backing. The
        // pinned schema recipe is shared with the original checked reader.
        budget
            .charge(
                self.required
                    .checked_sub(budget.upper())
                    .ok_or_else(|| invalid("statistics evidence receipt regressed"))?,
            )
            .map_err(checked_read)?;
        let files = self
            .files
            .as_ref()
            .expect("original manifest read receiver");
        self.result = Some(project_statistics_evidence(
            table.metadata(),
            info,
            snapshot,
            &request.data_version,
            request,
            files,
        ));
        Ok(())
    }
}
// These are prospective layout facts, not a capacity source. The caller must
// await the original MIO receipt before the corresponding formation phase.
fn plus(a: u64, b: u64) -> Result<u64, ConnectorError> {
    a.checked_add(b)
        .ok_or_else(|| invalid("statistics read geometry overflow"))
}
fn geometry<T>(
    value: Result<
        T,
        crate::read_snapshot::cow_capture::geometry::Failure<
            novarocks_spi::connector::ConnectorCowBeginCause,
        >,
    >,
) -> Result<T, ConnectorError> {
    crate::read_snapshot::cow_capture::recipe(value).map_err(checked_read)
}
fn metric_request_upper<'a>(names: impl Iterator<Item = &'a str>) -> Result<u64, ConnectorError> {
    use crate::read_snapshot::cow_capture::geometry as g;
    use novarocks_spi::connector::ConnectorCowBeginCause as Cause;
    let mut columns = 0usize;
    let mut amount = 0;
    for name in names {
        columns = columns
            .checked_add(1)
            .ok_or_else(|| invalid("statistics column count overflow"))?;
        amount = plus(amount, geometry(g::arc_bytes::<Cause>(name.len()))?)?;
    }
    let count = columns
        .checked_mul(5)
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| invalid("statistics metric count overflow"))?;
    amount = plus(
        amount,
        geometry(g::slots::<Cause, StatisticsMetric>(count))?,
    )?;
    // StatisticsMetricRequest::try_new's two real BTreeSet collections. They
    // are temporary; include both in the cumulative constructor envelope.
    amount = plus(
        amount,
        geometry(g::btree_construct::<Cause, &StatisticsMetric, ()>(count))?,
    )?;
    plus(
        amount,
        geometry(g::btree_construct::<Cause, &str, ()>(count - 1))?,
    )
}
fn evidence_formation_upper(
    budget: &CaptureBudget<'_>,
    metadata: &crate::iceberg::spec::TableMetadata,
    schema: &arrow::datatypes::Schema,
    request: &StatisticsReadRequest,
    uuid: &str,
) -> Result<u64, ConnectorError> {
    use crate::read_snapshot::cow_capture::geometry as g;
    use novarocks_spi::connector::ConnectorCowBeginCause as Cause;
    let bytes = budget
        .json_size(metadata.current_schema().as_ref())
        .map_err(checked_read)?;
    let mut amount = CaptureBudget::json_working_upper(bytes).map_err(checked_read)?;
    // Nominal type declarations are metadata properties, not schema JSON.
    for (key, value) in metadata.properties() {
        amount = plus(
            amount,
            CaptureBudget::json_working_upper(plus(key.len() as u64, value.len() as u64)?)
                .map_err(checked_read)?,
        )?;
    }
    let fields = metadata.current_schema().as_struct();
    amount = plus(
        amount,
        geometry(g::hash_max::<Cause, (String, i32)>(fields.fields().len()))?,
    )?;
    amount = plus(
        amount,
        geometry(g::hash_max::<Cause, (String, DataType)>(
            schema.fields().len(),
        ))?,
    )?;
    for field in fields.fields() {
        amount = plus(amount, field.name.len() as u64)?;
    }
    for field in schema.fields() {
        amount = plus(amount, field.name().len() as u64)?;
        // Arrow's other recursive variants clone their existing Arc fields.
        // Dictionary owns two Boxes and recursively clones their DataTypes.
        fn boxes(ty: &DataType) -> Result<u64, ConnectorError> {
            if let DataType::Dictionary(key, value) = ty {
                plus(
                    (2 * std::mem::size_of::<DataType>()) as u64,
                    plus(boxes(key)?, boxes(value)?)?,
                )
            } else {
                Ok(0)
            }
        }
        amount = plus(amount, boxes(field.data_type())?)?;
    }
    // String -> Bytes reuses the character buffer but may create bytes' own
    // Shared allocation. Use the pinned original layout author; this is not
    // another copy of the String payload or an SDK graph estimate.
    let bytes_shared = u64::try_from(
        novarocks_type_contract::owned_resources::layout::bytes_shared_upper()
            .map_err(|_| invalid("statistics Bytes shared layout is unsupported"))?,
    )
    .map_err(|_| invalid("statistics Bytes shared layout overflow"))?;
    let count = request.metrics.metrics().len();
    let ndv = request
        .metrics
        .metrics()
        .iter()
        .filter(|m| matches!(m, StatisticsMetric::ThetaNdv { .. }))
        .count();
    for part in [
        g::btree_construct::<Cause, StatisticsMetric, StatisticsMetricState>(count),
        g::btree_construct::<Cause, i32, StatisticsMetric>(ndv),
        g::btree_construct::<Cause, i32, ()>(ndv), // wanted field set
        g::btree_construct::<Cause, i32, ()>(ndv), // outstanding clone
        g::slots::<Cause, (i32, i64)>(ndv),        // evidence_revision bases
    ] {
        amount = plus(amount, geometry(part)?)?;
    }
    amount = plus(
        amount,
        geometry(g::mul::<Cause>(
            geometry(g::hash_max::<Cause, (i32, AncestorNdv)>(ndv))?,
            2,
        ))?,
    )?;
    // Each ancestor's temporary blob map is dropped before the next ancestor.
    // Bound that one real map's geometric old/new backing by the largest actual
    // registered file; do not clone any StatisticsFile or Puffin/SDK graph.
    let blobs = metadata
        .statistics_iter()
        .map(|file| file.blob_metadata.len())
        .max()
        .unwrap_or(0);
    amount = plus(
        amount,
        geometry(g::mul::<Cause>(
            geometry(g::hash_max::<Cause, (i32, f64)>(blobs))?,
            2,
        ))?,
    )?;
    // StatsLoader's original corrupt-metadata branch renders one i32 field id
    // and, for parse failure, std's fixed ParseFloatError text. It stops the
    // walk immediately; no SDK error tree is copied or recursively estimated.
    let loader_error = [
        "Theta blob metadata must name exactly one field".len(),
        "Theta blob metadata for field  is missing ndv".len() + 11,
        "Theta ndv for field  is not numeric: ".len()
            + 11
            + "cannot parse float from empty string".len(),
        "Theta ndv for field  must be finite and non-negative".len() + 11,
        "statistics metadata contains duplicate Theta blobs for field ".len() + 11,
    ]
    .into_iter()
    .max()
    .unwrap();
    amount = plus(
        amount,
        geometry(g::format_capacity::<Cause>(loader_error as u64))?,
    )?;
    for metric in request.metrics.metrics() {
        budget.active().map_err(checked_read)?;
        let column = statistics_metric_column(metric).unwrap_or("");
        amount = plus(amount, column.len() as u64)?; // lowercase lookup scratch
        let message = match metric {
            StatisticsMetric::RowCount => "Iceberg manifest does not report every row count".len(),
            StatisticsMetric::NullCount { .. } => {
                "Iceberg manifest does not report a null count for ``".len()
            }
            StatisticsMetric::ThetaNdv { .. } => {
                "no ancestor snapshot published a sketch for this column".len()
            }
            _ => "statistics for column `` are not collected".len(),
        };
        let length = message
            .checked_add(column.len())
            .ok_or_else(|| invalid("statistics diagnostic upper overflow"))?;
        amount = plus(
            amount,
            geometry(g::format_capacity::<Cause>(length as u64))?,
        )?;
        amount = plus(amount, geometry(g::arc_bytes::<Cause>(length))?)?;
        if matches!(metric, StatisticsMetric::ThetaNdv { .. }) {
            amount = plus(amount, bytes_shared)?;
            // statistics_data_version: signed-i64 temporary, then its exact
            // fixed prefix + original UUID + snapshot rendering.
            amount = plus(amount, geometry(g::format_capacity::<Cause>(20))?)?;
            amount = plus(
                amount,
                geometry(g::format_capacity::<Cause>(plus(
                    uuid.len() as u64,
                    ("iceberg/v1//".len() + 20) as u64,
                )?))?,
            )?;
        }
    }
    amount = plus(amount, 32)?; // evidence_revision's digest String
    amount = plus(amount, geometry(g::format_capacity::<Cause>(2))?)?; // one byte formatting scratch
    amount = plus(
        amount,
        geometry(g::format_capacity::<Cause>(plus(
            uuid.len() as u64,
            ("iceberg/v2///".len() + 20 + 32) as u64,
        )?))?,
    )?;
    // The evidence revision owns one independent Bytes carrier.
    plus(amount, bytes_shared)
}
impl IcebergMetadataCpuOriginal {
    pub(crate) fn read_source(
        provider: IcebergMetadata,
        request: ConnectorTableRequest,
        original: ConnectorOriginalResultScope,
    ) -> Arc<Self> {
        Self::new(
            provider.descriptor().clone(),
            provider.incarnation(),
            CpuBody::Read(ReadBody {
                provider: Some(provider),
                request: Some(request),
                original,
                facts: None,
                metadata: None,
                read_request: None,
                files: None,
                result: None,
                phase: 0,
                snapshot: None,
                upper: 0,
                required: 0,
                payload: None,
                snapshot_id: None,
                retiring: false,
                closed: false,
            }),
        )
    }
    async fn read_original(
        self: Arc<Self>,
        cpu: Arc<dyn StatisticsCpuPort>,
        metadata: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> Result<(), ConnectorError> {
        let result = self.drive_read(&cpu, &metadata).await;
        // Ordinary result is received into the original body before this F
        // can exit or be dropped. The CPU port independently preserves health.
        if let Err(error) = result {
            let mut loan = BodyLoan::take(&self);
            let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
                unreachable!()
            };
            if body.result.is_none() {
                body.result = Some(Err(error));
            }
        }
        Ok(())
    }
    async fn drive_read(
        self: &Arc<Self>,
        cpu: &Arc<dyn StatisticsCpuPort>,
        metadata: &Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> Result<(), ConnectorError> {
        {
            let mut loan = BodyLoan::take(self);
            let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
                unreachable!()
            };
            let budget = CaptureBudget::new(&body.original, body.upper).map_err(checked_read)?;
            budget.active().map_err(checked_read)?;
            let preparation = body
                .provider
                .as_ref()
                .unwrap()
                .prepare_table(body.request.as_ref().unwrap().clone())
                .await?;
            body.facts = Some(
                preparation.into_facts::<crate::metadata::IcebergPreparedTableFacts>(
                    &body.provider.as_ref().unwrap().descriptor().instance_id,
                )?,
            );
        }
        let mut input = Some(novarocks_spi::connector::ProviderMetadataCpuInput(
            self.clone(),
        ));
        metadata.submit_metadata_in(&mut input)?;
        std::future::poll_fn(|cx| metadata.poll_finished(cx)).await?;
        let (context, required) = {
            let mut loan = BodyLoan::take(self);
            let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
                unreachable!()
            };
            if body.result.is_some() {
                return Ok(());
            }
            (
                body.request.as_ref().unwrap().context.clone(),
                body.required,
            )
        };
        let required = std::num::NonZeroU64::new(required)
            .ok_or_else(|| invalid("statistics schema quote is empty"))?;
        context.reserve_owned_metadata(required).await?;
        {
            let mut loan = BodyLoan::take(self);
            let original = loan.body.as_mut().unwrap();
            let CpuBody::Read(body) = &mut original.body else {
                unreachable!()
            };
            body.phase = 1;
            original.started = false;
        }
        let mut input = Some(novarocks_spi::connector::ProviderMetadataCpuInput(
            self.clone(),
        ));
        metadata.submit_metadata_in(&mut input)?;
        std::future::poll_fn(|cx| metadata.poll_finished(cx)).await?;
        {
            let mut loan = BodyLoan::take(self);
            let original = loan.body.as_mut().unwrap();
            let CpuBody::Read(body) = &mut original.body else {
                unreachable!()
            };
            if body.result.is_some() {
                return Ok(());
            }
            let request = body
                .read_request
                .as_ref()
                .expect("CPU prepared read request");
            let snapshot = body.snapshot_id.ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "Iceberg table has no current snapshot for statistics",
                )
            })?;
            let table = IcebergMetadata::statistics_read_physical(body.facts.as_ref().unwrap());
            body.snapshot = Some(IcebergMetadataCpuOriginal::read_snapshot_source(
                body.provider.as_ref().unwrap().clone(),
                table.clone(),
                snapshot,
                body.request.as_ref().unwrap().context.clone(),
                body.original.clone(),
                body.upper,
                super::scoped_snapshot::SnapshotReadKind::Statistics,
            ));
        }
        let snapshot = {
            let mut loan = BodyLoan::take(self);
            let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
                unreachable!()
            };
            body.snapshot.as_ref().unwrap().clone()
        };
        super::scoped_snapshot::drive_read_snapshot(&snapshot, metadata).await?;
        {
            let mut loan = BodyLoan::take(self);
            let original = loan.body.as_mut().unwrap();
            let CpuBody::Read(body) = &mut original.body else {
                unreachable!()
            };
            let mut received = None;
            snapshot.take_read_snapshot_in(&mut received)?;
            let received = received.expect("original snapshot receiver")?;
            body.files = Some(received.files);
            body.upper = received.simultaneous_upper;
            body.phase = 2;
            original.started = false;
        }
        run_read(cpu, self).await?;
        let (context, required) = {
            let mut loan = BodyLoan::take(self);
            let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
                unreachable!()
            };
            if body.result.is_some() {
                return Ok(());
            }
            (
                body.request.as_ref().unwrap().context.clone(),
                body.required,
            )
        };
        context
            .reserve_owned_metadata(
                std::num::NonZeroU64::new(required)
                    .ok_or_else(|| invalid("statistics evidence quote is empty"))?,
            )
            .await?;
        {
            let mut loan = BodyLoan::take(self);
            let original = loan.body.as_mut().unwrap();
            let CpuBody::Read(body) = &mut original.body else {
                unreachable!()
            };
            body.phase = 3;
            original.started = false;
        }
        run_read(cpu, self).await
    }
    pub(crate) fn read_future_bytes() -> usize {
        fn bytes<A, B, C, F>(_: fn(A, B, C) -> F) -> usize {
            std::mem::size_of::<F>()
        }
        bytes(Self::read_original)
    }
}
async fn run_read(
    cpu: &Arc<dyn StatisticsCpuPort>,
    original: &Arc<IcebergMetadataCpuOriginal>,
) -> Result<(), ConnectorError> {
    let mut input = Some(StatisticsCpuInput::Read(original.clone()));
    cpu.submit_in(&mut input)?;
    futures::future::poll_fn(|cx| cpu.poll_finished(cx)).await
}
impl StatisticsReadOriginal for IcebergMetadataCpuOriginal {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.incarnation
    }
    fn read(
        self: Arc<Self>,
        cpu: Arc<dyn StatisticsCpuPort>,
        metadata: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> StatisticsSessionFuture<'static, ()> {
        Box::pin(Self::read_original(self, cpu, metadata))
    }
    fn close_original(&self) -> Result<(), ConnectorError> {
        let mut loan = BodyLoan::take(self);
        let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
            return Err(invalid("foreign read close source"));
        };
        if body.closed {
            return Ok(());
        }
        if body.retiring {
            return Err(invalid("statistics read source disposal is unproven"));
        }
        body.retiring = true;
        let inputs = (
            body.snapshot.take(),
            body.files.take(),
            body.read_request.take(),
            body.metadata.take(),
            body.payload.take(),
            body.facts.take(),
            body.request.take(),
            body.provider.take(),
        );
        drop(inputs); // foreign Drop is outside the mutex, original paid scope stays in body
        body.closed = true;
        Ok(())
    }
    fn take_result_in(
        &self,
        target: &mut Option<Result<StatisticsEvidence, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        if target.is_some() {
            return Err(invalid("statistics read receiver is occupied"));
        }
        let mut loan = BodyLoan::take(self);
        let CpuBody::Read(body) = &mut loan.body.as_mut().unwrap().body else {
            return Err(invalid("foreign statistics read body"));
        };
        if body.result.is_none() {
            return Err(invalid("statistics read result is not ready"));
        }
        *target = body.result.take();
        Ok(())
    }
}

pub(super) fn error_backing_bytes() -> Result<usize, ConnectorError> {
    ConnectorError::static_domain_backing_bytes::<ReadQuotaFailure>(READ_QUOTA_DETAIL)
        .map_err(|_| internal("statistics error carrier layout overflow"))
}

#[cfg(test)]
mod failure_geometry_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
    use novarocks_spi::connector::OriginalResultCheckError;
    use std::{
        error::Error,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct OriginalRefusal(Arc<AtomicUsize>);
    impl std::fmt::Display for OriginalRefusal {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("original scope refusal must not be formatted")
        }
    }
    impl Error for OriginalRefusal {}
    impl Drop for OriginalRefusal {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn original_quota_refusal_moves_once_and_retains_its_typed_source() {
        let drops = Arc::new(AtomicUsize::new(0));
        let input = OriginalResultCheckError::new(
            OriginalResultCheckClass::Coverage,
            OriginalRefusal(drops.clone()),
        );
        let error = checked_read(ReadFailure::Original(input.into()));
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        assert_eq!(error.failure_fact().reason().code(), 7);
        assert_eq!(error.message(), READ_QUOTA_DETAIL);
        let shared = error
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        let quota = shared.downcast_ref::<ReadQuotaFailure>().unwrap();
        assert_eq!(quota.original.class(), OriginalResultCheckClass::Coverage);
        assert!(
            quota
                .original
                .source()
                .unwrap()
                .downcast_ref::<OriginalRefusal>()
                .is_some()
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            error_backing_bytes().unwrap(),
            ConnectorError::static_domain_backing_bytes::<ReadQuotaFailure>(READ_QUOTA_DETAIL)
                .unwrap()
        );
        drop(error);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn provider_error_is_moved_without_another_cause_or_diagnostic_carrier() {
        let input = super::super::scoped_snapshot::raw_read_failure(crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::TableNotFound,
            "private original table",
        ));
        let fact = input.failure_fact();
        let original = input
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
            .clone();
        let output = checked_read(ReadFailure::Original(input.into()));
        assert_eq!(output.failure_fact(), fact);
        assert_eq!(
            output
                .source()
                .unwrap()
                .downcast_ref::<SharedFailureCause>(),
            Some(&original)
        );
        assert_eq!(output.message(), original.detail());
    }
}
