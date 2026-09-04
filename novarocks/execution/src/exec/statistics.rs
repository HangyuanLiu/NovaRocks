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

//! Bounded, fragment-local statistics collection.
//!
//! The collector accepts Arrow batches from one fragment and produces a
//! versioned partial payload. Provider evidence, query lifecycle aggregation,
//! and publication deliberately remain outside this module.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Decimal128Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datasketches::theta::{CompactThetaSketch, ThetaSketch, ThetaSketchBuilder};
use novarocks_spi::connector::{
    MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES, StatisticsMetric, StatisticsMetricRequest,
};
use sha2::{Digest, Sha256};

pub const MAX_STATISTICS_THETA_RETAINED_HASHES: usize = 1 << 12;
const MIN_STATISTICS_THETA_LG_K: u8 = 5;
const MAX_STATISTICS_THETA_LG_K: u8 = 12;
const THETA_PARTIAL_WIRE_VERSION: u8 = 2;
const THETA_PARTIAL_WIRE_HEADER_BYTES: usize = 2;
const THETA_COMPACT_MAX_PREAMBLE_BYTES: usize = 24;
const MAX_STATISTICS_THETA_COMPACT_BODY_BYTES: usize =
    THETA_COMPACT_MAX_PREAMBLE_BYTES + MAX_STATISTICS_THETA_RETAINED_HASHES * size_of::<u64>();
const MAX_STATISTICS_THETA_PARTIAL_WIRE_BYTES: usize =
    THETA_PARTIAL_WIRE_HEADER_BYTES + MAX_STATISTICS_THETA_COMPACT_BODY_BYTES;
const STATISTICS_FRAGMENT_PAYLOAD_VERSION: u8 = 2;
const STATISTICS_SCALAR_PARTIAL_MAX_WIRE_BYTES: usize = 8 + 8 + 8 + (1 + 16) * 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsFragmentError {
    message: String,
}

impl StatisticsFragmentError {
    fn contract(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn exhausted(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for StatisticsFragmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatisticsFragmentError {}

#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsScalarPartial {
    row_count: u64,
    null_count: u64,
    total_size: u64,
    minimum: Option<StatisticsScalarBound>,
    maximum: Option<StatisticsScalarBound>,
}

#[derive(Clone, Debug, PartialEq)]
enum StatisticsScalarBound {
    F64(f64),
    LargeInt(i128),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThetaSketchPartial {
    // The V2 Nova carrier owns only lg_k and bounds. DataSketches owns this ordered v3 body.
    lg_k: u8,
    compact_body: Vec<u8>,
}

/// A bounded partial which can be transferred in one terminal fragment report.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StatisticsFragmentPartial {
    table: Option<StatisticsScalarPartial>,
    columns: BTreeMap<Arc<str>, StatisticsScalarPartial>,
    theta: BTreeMap<Arc<str>, ThetaSketchPartial>,
}

/// Per-fragment Arrow collector. It owns no connector/provider or query state.
pub struct StatisticsBatchCollector {
    schema: SchemaRef,
    metrics: StatisticsMetricRequest,
    column_indexes: BTreeMap<Arc<str>, usize>,
    table_rows: u64,
    columns: BTreeMap<Arc<str>, StatisticsScalarAccumulator>,
    theta: BTreeMap<Arc<str>, StatisticsThetaAccumulator>,
}

#[derive(Clone, Debug, Default)]
struct StatisticsScalarAccumulator {
    row_count: u64,
    null_count: u64,
    total_size: u64,
    minimum: Option<StatisticsScalarBound>,
    maximum: Option<StatisticsScalarBound>,
}

#[derive(Debug)]
struct StatisticsThetaAccumulator {
    sketch: ThetaSketch,
}

impl StatisticsBatchCollector {
    pub fn try_new(
        schema: SchemaRef,
        metrics: StatisticsMetricRequest,
    ) -> Result<Self, StatisticsFragmentError> {
        let mut column_indexes = BTreeMap::new();
        for metric in metrics.metrics() {
            let Some(column) = statistics_metric_column(metric) else {
                continue;
            };
            let index = schema
                .fields()
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(column))
                .ok_or_else(|| {
                    StatisticsFragmentError::contract(format!(
                        "statistics scan schema does not contain requested column `{column}`"
                    ))
                })?;
            column_indexes.insert(column.clone(), index);
        }
        let scalar_columns = metrics
            .metrics()
            .iter()
            .filter_map(|metric| match metric {
                StatisticsMetric::NullCount { column }
                | StatisticsMetric::Minimum { column }
                | StatisticsMetric::Maximum { column }
                | StatisticsMetric::AverageSize { column } => Some(column.clone()),
                StatisticsMetric::RowCount | StatisticsMetric::ThetaNdv { .. } => None,
            })
            .collect::<BTreeSet<_>>();
        let theta_columns = metrics
            .metrics()
            .iter()
            .filter_map(|metric| match metric {
                StatisticsMetric::ThetaNdv { column } => Some(column.clone()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let theta_lg_k = statistics_theta_lg_k(&scalar_columns, &theta_columns)?;
        Ok(Self {
            schema,
            metrics,
            column_indexes,
            table_rows: 0,
            columns: scalar_columns
                .into_iter()
                .map(|column| (column, StatisticsScalarAccumulator::default()))
                .collect(),
            theta: theta_columns
                .into_iter()
                .map(|column| {
                    StatisticsThetaAccumulator::try_new(theta_lg_k)
                        .map(|accumulator| (column, accumulator))
                })
                .collect::<Result<_, _>>()?,
        })
    }

    /// Accumulate one batch of the pinned scan's output.
    ///
    /// The batch is checked against the pinned scan by *shape* -- its column
    /// count and each column's type, in order -- rather than by whole-schema
    /// equality. A connector read's chunk identifies its columns by slot id
    /// and names its Arrow fields `slot_<id>` accordingly, while the pinned
    /// plan schema names them as the relation does; both descriptions are
    /// correct and neither is the other. Nullability is excluded for the same
    /// reason: a reader declares every produced column nullable because a page
    /// carries no nullability of its own.
    ///
    /// Position is what the collector actually relies on -- `column_indexes`
    /// resolved each metric's column to an ordinal against the pinned schema
    /// once -- so the type-and-arity check is exactly the assumption being
    /// made, and a batch that is not this scan's output still fails here.
    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<(), StatisticsFragmentError> {
        let batch_schema = batch.schema();
        let pinned_types = self.schema.fields().iter().map(|field| field.data_type());
        let batch_types = batch_schema.fields().iter().map(|field| field.data_type());
        if batch_schema.fields().len() != self.schema.fields().len()
            || !pinned_types.eq(batch_types)
        {
            return Err(StatisticsFragmentError::contract(
                "statistics batch shape differs from the pinned scan schema",
            ));
        }
        let rows = u64::try_from(batch.num_rows()).map_err(|_| {
            StatisticsFragmentError::exhausted("statistics batch row count exceeds u64")
        })?;
        self.table_rows = self
            .table_rows
            .checked_add(rows)
            .ok_or_else(|| StatisticsFragmentError::exhausted("statistics row count overflow"))?;
        for (column, index) in &self.column_indexes {
            let array = batch.column(*index);
            if let Some(accumulator) = self.columns.get_mut(column) {
                accumulator.push(array, rows)?;
            }
            if let Some(accumulator) = self.theta.get_mut(column) {
                accumulator.push(array)?;
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Result<StatisticsFragmentPartial, StatisticsFragmentError> {
        let table = StatisticsScalarPartial::try_new_bounds(self.table_rows, 0, 0, None, None)?;
        let mut partial = StatisticsFragmentPartial::default().with_table(table);
        for (column, accumulator) in self.columns {
            partial = partial.with_column(column, accumulator.finish()?);
        }
        for (column, accumulator) in self.theta {
            partial = partial.with_theta(column, accumulator.finish()?);
        }
        debug_assert!(!self.metrics.metrics().is_empty());
        Ok(partial)
    }

    pub fn finish_fragment_payload(self) -> Result<Bytes, StatisticsFragmentError> {
        self.finish()?.to_payload()
    }
}

impl StatisticsFragmentPartial {
    pub fn with_table(mut self, partial: StatisticsScalarPartial) -> Self {
        self.table = Some(partial);
        self
    }

    pub fn with_column(
        mut self,
        column: impl Into<Arc<str>>,
        partial: StatisticsScalarPartial,
    ) -> Self {
        self.columns.insert(column.into(), partial);
        self
    }

    pub fn with_theta(mut self, column: impl Into<Arc<str>>, partial: ThetaSketchPartial) -> Self {
        self.theta.insert(column.into(), partial);
        self
    }

    /// Encode exactly the existing native terminal-report payload. The payload
    /// carries only fragment-local facts and never evidence revision or
    /// provider credentials.
    pub fn to_payload(&self) -> Result<Bytes, StatisticsFragmentError> {
        let mut bytes = Vec::new();
        bytes.push(STATISTICS_FRAGMENT_PAYLOAD_VERSION);
        match &self.table {
            Some(table) => {
                bytes.push(1);
                encode_scalar_partial(&mut bytes, table);
            }
            None => bytes.push(0),
        }
        encode_scalar_partials(&mut bytes, &self.columns)?;
        encode_theta_partials(&mut bytes, &self.theta)?;
        if bytes.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics fragment report exceeds the SPI payload limit",
            ));
        }
        Ok(Bytes::from(bytes))
    }

    pub fn try_from_payload(bytes: &[u8]) -> Result<Self, StatisticsFragmentError> {
        if bytes.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics fragment report exceeds the SPI payload limit",
            ));
        }
        let mut cursor = 0usize;
        let version = take_bytes(bytes, &mut cursor, 1)?[0];
        if version != STATISTICS_FRAGMENT_PAYLOAD_VERSION {
            return Err(StatisticsFragmentError::contract(
                "statistics fragment report has an unsupported version",
            ));
        }
        let table = match take_bytes(bytes, &mut cursor, 1)?[0] {
            0 => None,
            1 => Some(decode_scalar_partial(bytes, &mut cursor)?),
            _ => {
                return Err(StatisticsFragmentError::contract(
                    "statistics fragment report has an invalid table flag",
                ));
            }
        };
        let columns = decode_scalar_partials(bytes, &mut cursor)?;
        let theta = decode_theta_partials(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(StatisticsFragmentError::contract(
                "statistics fragment report has trailing bytes",
            ));
        }
        Ok(Self {
            table,
            columns,
            theta,
        })
    }
}

impl StatisticsScalarPartial {
    fn try_new_bounds(
        row_count: u64,
        null_count: u64,
        total_size: u64,
        minimum: Option<StatisticsScalarBound>,
        maximum: Option<StatisticsScalarBound>,
    ) -> Result<Self, StatisticsFragmentError> {
        if null_count > row_count {
            return Err(StatisticsFragmentError::contract(
                "statistics null count exceeds row count",
            ));
        }
        if minimum.as_ref().is_some_and(|value| !value.is_valid())
            || maximum.as_ref().is_some_and(|value| !value.is_valid())
            || matches!((&minimum, &maximum), (Some(minimum), Some(maximum)) if minimum.compare(maximum).is_none_or(|order| order.is_gt()))
        {
            return Err(StatisticsFragmentError::contract(
                "statistics scalar bounds must be finite, equally typed, and ordered",
            ));
        }
        Ok(Self {
            row_count,
            null_count,
            total_size,
            minimum,
            maximum,
        })
    }
}

impl StatisticsScalarAccumulator {
    fn push(&mut self, array: &ArrayRef, rows: u64) -> Result<(), StatisticsFragmentError> {
        self.row_count = self
            .row_count
            .checked_add(rows)
            .ok_or_else(|| StatisticsFragmentError::exhausted("statistics row count overflow"))?;
        self.null_count = self
            .null_count
            .checked_add(u64::try_from(array.null_count()).map_err(|_| {
                StatisticsFragmentError::exhausted("statistics null count exceeds u64")
            })?)
            .ok_or_else(|| StatisticsFragmentError::exhausted("statistics null count overflow"))?;
        self.total_size = self
            .total_size
            .checked_add(estimated_value_bytes(array)?)
            .ok_or_else(|| StatisticsFragmentError::exhausted("statistics value size overflow"))?;
        for value in array_scalar_bounds(array)? {
            self.minimum = merge_scalar_bounds(self.minimum.take(), Some(value.clone()), true)?;
            self.maximum = merge_scalar_bounds(self.maximum.take(), Some(value), false)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<StatisticsScalarPartial, StatisticsFragmentError> {
        StatisticsScalarPartial::try_new_bounds(
            self.row_count,
            self.null_count,
            self.total_size,
            self.minimum,
            self.maximum,
        )
    }
}

impl StatisticsThetaAccumulator {
    fn try_new(lg_k: u8) -> Result<Self, StatisticsFragmentError> {
        debug_assert!((MIN_STATISTICS_THETA_LG_K..=MAX_STATISTICS_THETA_LG_K).contains(&lg_k));
        let sketch = ThetaSketchBuilder::default()
            .lg_k(lg_k)
            .build()
            .map_err(|err| {
                StatisticsFragmentError::contract(format!(
                    "failed to create statistics Theta sketch: {err}"
                ))
            })?;
        Ok(Self { sketch })
    }

    fn push(&mut self, array: &ArrayRef) -> Result<(), StatisticsFragmentError> {
        for hash in array_hashes(array)? {
            self.sketch.update(hash as i64);
        }
        Ok(())
    }

    fn finish(self) -> Result<ThetaSketchPartial, StatisticsFragmentError> {
        ThetaSketchPartial::try_from_sketch(self.sketch)
    }
}

fn statistics_theta_lg_k(
    scalar_columns: &BTreeSet<Arc<str>>,
    theta_columns: &BTreeSet<Arc<str>>,
) -> Result<u8, StatisticsFragmentError> {
    if theta_columns.is_empty() {
        return Ok(MAX_STATISTICS_THETA_LG_K);
    }

    // Reserve the exact fixed framing plus the worst-case scalar bounds before
    // dividing the remaining SPI payload budget among the requested sketches.
    // Choosing lg_k per request keeps one terminal fragment report bounded
    // without weakening the connector-wide 64 KiB contract.
    let mut fixed_bytes = 1usize // fragment payload version
        + 1 // table partial flag
        + STATISTICS_SCALAR_PARTIAL_MAX_WIRE_BYTES
        + 2 // scalar partial count
        + 2; // Theta partial count
    for column in scalar_columns {
        fixed_bytes = fixed_bytes
            .checked_add(2 + column.len() + STATISTICS_SCALAR_PARTIAL_MAX_WIRE_BYTES)
            .ok_or_else(|| {
                StatisticsFragmentError::exhausted(
                    "statistics fragment report size accounting overflow",
                )
            })?;
    }
    for column in theta_columns {
        fixed_bytes = fixed_bytes
            .checked_add(
                2 + column.len()
                    + 4
                    + THETA_PARTIAL_WIRE_HEADER_BYTES
                    + THETA_COMPACT_MAX_PREAMBLE_BYTES,
            )
            .ok_or_else(|| {
                StatisticsFragmentError::exhausted(
                    "statistics fragment report size accounting overflow",
                )
            })?;
    }
    let remaining = MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
        .checked_sub(fixed_bytes)
        .ok_or_else(|| {
            StatisticsFragmentError::exhausted(
                "statistics metrics cannot fit in one bounded fragment report",
            )
        })?;
    let hashes_per_sketch = remaining / theta_columns.len() / std::mem::size_of::<u64>();
    let lg_k = hashes_per_sketch
        .checked_ilog2()
        .map(|value| value.min(u32::from(MAX_STATISTICS_THETA_LG_K)) as u8)
        .ok_or_else(|| {
            StatisticsFragmentError::exhausted(
                "statistics metrics leave no room for a bounded Theta sketch",
            )
        })?;
    if lg_k < MIN_STATISTICS_THETA_LG_K {
        return Err(StatisticsFragmentError::exhausted(
            "statistics metrics leave insufficient room for a bounded Theta sketch",
        ));
    }
    Ok(lg_k)
}

impl StatisticsScalarBound {
    fn is_valid(&self) -> bool {
        !matches!(self, Self::F64(value) if !value.is_finite())
    }

    fn compare(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::F64(left), Self::F64(right)) => left.partial_cmp(right),
            (Self::LargeInt(left), Self::LargeInt(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}

impl ThetaSketchPartial {
    fn try_from_sketch(mut sketch: ThetaSketch) -> Result<Self, StatisticsFragmentError> {
        sketch.trim();
        let lg_k = sketch.lg_k();
        let compact = sketch.compact(true);
        if compact.num_retained() > MAX_STATISTICS_THETA_RETAINED_HASHES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics Theta partial exceeds the retained-hash limit",
            ));
        }
        let compact_body = compact.serialize();
        Self::try_new(lg_k, compact_body)
    }

    fn try_new(lg_k: u8, compact_body: Vec<u8>) -> Result<Self, StatisticsFragmentError> {
        if !(MIN_STATISTICS_THETA_LG_K..=MAX_STATISTICS_THETA_LG_K).contains(&lg_k) {
            return Err(StatisticsFragmentError::contract(
                "statistics Theta wire state has an invalid lg_k",
            ));
        }
        let wire_len = THETA_PARTIAL_WIRE_HEADER_BYTES
            .checked_add(compact_body.len())
            .ok_or_else(|| {
                StatisticsFragmentError::exhausted("statistics Theta wire state length overflow")
            })?;
        if wire_len > MAX_STATISTICS_THETA_PARTIAL_WIRE_BYTES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics Theta wire state exceeds the compact-body limit",
            ));
        }
        Ok(Self { lg_k, compact_body })
    }

    fn to_wire_bytes(&self) -> Result<Vec<u8>, StatisticsFragmentError> {
        let wire_len = THETA_PARTIAL_WIRE_HEADER_BYTES
            .checked_add(self.compact_body.len())
            .ok_or_else(|| {
                StatisticsFragmentError::exhausted("statistics Theta wire state length overflow")
            })?;
        if wire_len > MAX_STATISTICS_THETA_PARTIAL_WIRE_BYTES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics Theta wire state exceeds the compact-body limit",
            ));
        }
        let mut bytes = Vec::with_capacity(wire_len);
        bytes.push(THETA_PARTIAL_WIRE_VERSION);
        bytes.push(self.lg_k);
        bytes.extend_from_slice(&self.compact_body);
        Ok(bytes)
    }

    fn try_from_wire_bytes(bytes: &[u8]) -> Result<Self, StatisticsFragmentError> {
        if bytes.len() < THETA_PARTIAL_WIRE_HEADER_BYTES {
            return Err(StatisticsFragmentError::contract(
                "statistics Theta wire state is truncated",
            ));
        }
        if bytes.len() > MAX_STATISTICS_THETA_PARTIAL_WIRE_BYTES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics Theta wire state exceeds the compact-body limit",
            ));
        }
        if bytes[0] != THETA_PARTIAL_WIRE_VERSION {
            return Err(StatisticsFragmentError::contract(
                "statistics Theta wire state has an unsupported version",
            ));
        }
        let lg_k = bytes[1];
        if !(MIN_STATISTICS_THETA_LG_K..=MAX_STATISTICS_THETA_LG_K).contains(&lg_k) {
            return Err(StatisticsFragmentError::contract(
                "statistics Theta wire state has an invalid lg_k",
            ));
        }
        let compact_body = bytes[THETA_PARTIAL_WIRE_HEADER_BYTES..].to_vec();
        let compact = CompactThetaSketch::deserialize(&compact_body).map_err(|err| {
            StatisticsFragmentError::contract(format!(
                "statistics Theta compact body is invalid: {err}"
            ))
        })?;
        if !compact.is_ordered() {
            return Err(StatisticsFragmentError::contract(
                "statistics Theta compact body must be ordered",
            ));
        }
        if compact.num_retained() > MAX_STATISTICS_THETA_RETAINED_HASHES {
            return Err(StatisticsFragmentError::exhausted(
                "statistics Theta wire state exceeds the retained-hash limit",
            ));
        }
        Self::try_new(lg_k, compact_body)
    }
}

fn statistics_metric_column(metric: &StatisticsMetric) -> Option<&Arc<str>> {
    match metric {
        StatisticsMetric::RowCount => None,
        StatisticsMetric::NullCount { column }
        | StatisticsMetric::Minimum { column }
        | StatisticsMetric::Maximum { column }
        | StatisticsMetric::AverageSize { column }
        | StatisticsMetric::ThetaNdv { column } => Some(column),
    }
}

fn estimated_value_bytes(array: &ArrayRef) -> Result<u64, StatisticsFragmentError> {
    let bytes = if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        array
            .iter()
            .flatten()
            .map(|value| value.len() as u64)
            .try_fold(0_u64, |total, value| total.checked_add(value).ok_or(()))
            .map_err(|_| StatisticsFragmentError::exhausted("statistics string size overflow"))?
    } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        array
            .iter()
            .flatten()
            .map(|value| value.len() as u64)
            .try_fold(0_u64, |total, value| total.checked_add(value).ok_or(()))
            .map_err(|_| StatisticsFragmentError::exhausted("statistics string size overflow"))?
    } else {
        u64::try_from(array.get_array_memory_size())
            .map_err(|_| StatisticsFragmentError::exhausted("statistics value size exceeds u64"))?
    };
    Ok(bytes)
}

fn merge_scalar_bounds(
    left: Option<StatisticsScalarBound>,
    right: Option<StatisticsScalarBound>,
    minimum: bool,
) -> Result<Option<StatisticsScalarBound>, StatisticsFragmentError> {
    match (left, right) {
        (Some(left), Some(right)) => {
            let ordering = left.compare(&right).ok_or_else(|| {
                StatisticsFragmentError::contract(
                    "statistics scalar bounds use incompatible physical types",
                )
            })?;
            Ok(Some(
                if (minimum && ordering.is_gt()) || (!minimum && ordering.is_lt()) {
                    right
                } else {
                    left
                },
            ))
        }
        (value @ Some(_), None) | (None, value @ Some(_)) => Ok(value),
        (None, None) => Ok(None),
    }
}

fn array_scalar_bounds(
    array: &ArrayRef,
) -> Result<Vec<StatisticsScalarBound>, StatisticsFragmentError> {
    macro_rules! f64_values {
        ($array:expr) => {
            return Ok($array
                .iter()
                .flatten()
                .map(|value| StatisticsScalarBound::F64(value as f64))
                .collect())
        };
    }
    if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        f64_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        return array
            .iter()
            .flatten()
            .map(|value| {
                let value = value as f64;
                value
                    .is_finite()
                    .then_some(StatisticsScalarBound::F64(value))
                    .ok_or_else(|| {
                        StatisticsFragmentError::contract("statistics numeric value is not finite")
                    })
            })
            .collect();
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        return array
            .iter()
            .flatten()
            .map(|value| {
                value
                    .is_finite()
                    .then_some(StatisticsScalarBound::F64(value))
                    .ok_or_else(|| {
                        StatisticsFragmentError::contract("statistics numeric value is not finite")
                    })
            })
            .collect();
    }
    if let Some(array) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        if array.value_length() != novarocks_types::largeint::LARGEINT_BYTE_WIDTH {
            return Ok(Vec::new());
        }
        return array
            .iter()
            .flatten()
            .map(|value| {
                novarocks_types::largeint::i128_from_be_bytes(value)
                    .map(StatisticsScalarBound::LargeInt)
                    .map_err(|error| {
                        StatisticsFragmentError::contract(format!(
                            "statistics LARGEINT value: {error}"
                        ))
                    })
            })
            .collect();
    }
    Ok(Vec::new())
}

/// Whether [`array_scalar_bounds`] can express a bound for `data_type`.
///
/// A requester must consult this before asking for `Minimum`/`Maximum`.
/// [`StatisticsScalarAccumulator`] serves `NullCount`, `AverageSize`,
/// `Minimum`, and `Maximum` from a single pass, so it cannot refuse a type it
/// has no bound vocabulary for — a `STRING` column still owes the first two.
/// It contributes no bound and stays silent instead. Nothing notices until
/// `finish_visible_row`, which fails the collection when any *requested*
/// metric produced nothing. Asking for a bound that cannot exist therefore
/// discards every other column's statistics for the whole table, rather than
/// leaving that one bound unknown.
pub fn statistics_scalar_bounds_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    ) || novarocks_types::largeint::is_largeint_data_type(data_type)
}

fn array_hashes(array: &ArrayRef) -> Result<Vec<u64>, StatisticsFragmentError> {
    let mut values = Vec::new();
    macro_rules! hash_values {
        ($array:expr) => {{
            for value in $array.iter().flatten() {
                values.push(statistics_value_hash(&value.to_be_bytes()));
            }
            return Ok(values);
        }};
    }
    if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<Decimal128Array>() {
        hash_values!(array);
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        for value in array.iter().flatten() {
            values.push(statistics_value_hash(value.as_bytes()));
        }
        return Ok(values);
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        for value in array.iter().flatten() {
            values.push(statistics_value_hash(value.as_bytes()));
        }
        return Ok(values);
    }
    if let Some(array) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        for value in array.iter().flatten() {
            values.push(statistics_value_hash(value));
        }
        return Ok(values);
    }
    Err(StatisticsFragmentError::contract(
        "statistics Theta collection does not support the requested Arrow type",
    ))
}

fn statistics_value_hash(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 digest has at least eight bytes"),
    )
}

fn take_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    count: usize,
) -> Result<&'a [u8], StatisticsFragmentError> {
    let end = cursor.checked_add(count).ok_or_else(|| {
        StatisticsFragmentError::contract("statistics fragment report length overflow")
    })?;
    let output = bytes.get(*cursor..end).ok_or_else(|| {
        StatisticsFragmentError::contract("statistics fragment report is truncated")
    })?;
    *cursor = end;
    Ok(output)
}

fn encode_scalar_partial(bytes: &mut Vec<u8>, partial: &StatisticsScalarPartial) {
    bytes.extend_from_slice(&partial.row_count.to_be_bytes());
    bytes.extend_from_slice(&partial.null_count.to_be_bytes());
    bytes.extend_from_slice(&partial.total_size.to_be_bytes());
    for value in [&partial.minimum, &partial.maximum] {
        match value {
            Some(StatisticsScalarBound::F64(value)) => {
                bytes.push(1);
                bytes.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            Some(StatisticsScalarBound::LargeInt(value)) => {
                bytes.push(2);
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            None => bytes.push(0),
        }
    }
}

fn decode_scalar_partial(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<StatisticsScalarPartial, StatisticsFragmentError> {
    let read_u64 = |cursor: &mut usize| -> Result<u64, StatisticsFragmentError> {
        Ok(u64::from_be_bytes(
            take_bytes(bytes, cursor, 8)?
                .try_into()
                .expect("fixed scalar field width"),
        ))
    };
    let row_count = read_u64(cursor)?;
    let null_count = read_u64(cursor)?;
    let total_size = read_u64(cursor)?;
    let read_bound =
        |cursor: &mut usize| -> Result<Option<StatisticsScalarBound>, StatisticsFragmentError> {
            match take_bytes(bytes, cursor, 1)?[0] {
                0 => Ok(None),
                1 => Ok(Some(StatisticsScalarBound::F64(f64::from_bits(
                    u64::from_be_bytes(
                        take_bytes(bytes, cursor, 8)?
                            .try_into()
                            .expect("fixed scalar field width"),
                    ),
                )))),
                2 => Ok(Some(StatisticsScalarBound::LargeInt(i128::from_be_bytes(
                    take_bytes(bytes, cursor, 16)?
                        .try_into()
                        .expect("fixed LARGEINT scalar field width"),
                )))),
                _ => Err(StatisticsFragmentError::contract(
                    "statistics scalar partial has an invalid bound flag",
                )),
            }
        };
    StatisticsScalarPartial::try_new_bounds(
        row_count,
        null_count,
        total_size,
        read_bound(cursor)?,
        read_bound(cursor)?,
    )
}

fn encode_scalar_partials(
    bytes: &mut Vec<u8>,
    partials: &BTreeMap<Arc<str>, StatisticsScalarPartial>,
) -> Result<(), StatisticsFragmentError> {
    let count = u16::try_from(partials.len()).map_err(|_| {
        StatisticsFragmentError::exhausted("statistics fragment report has too many scalar columns")
    })?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (column, partial) in partials {
        encode_fragment_column(bytes, column)?;
        encode_scalar_partial(bytes, partial);
    }
    Ok(())
}

fn decode_scalar_partials(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<BTreeMap<Arc<str>, StatisticsScalarPartial>, StatisticsFragmentError> {
    let count = u16::from_be_bytes(
        take_bytes(bytes, cursor, 2)?
            .try_into()
            .expect("fixed count width"),
    ) as usize;
    let mut partials = BTreeMap::new();
    for _ in 0..count {
        let column = decode_fragment_column(bytes, cursor)?;
        let value = decode_scalar_partial(bytes, cursor)?;
        if partials.insert(column, value).is_some() {
            return Err(StatisticsFragmentError::contract(
                "statistics fragment report has duplicate scalar columns",
            ));
        }
    }
    Ok(partials)
}

fn encode_theta_partials(
    bytes: &mut Vec<u8>,
    partials: &BTreeMap<Arc<str>, ThetaSketchPartial>,
) -> Result<(), StatisticsFragmentError> {
    let count = u16::try_from(partials.len()).map_err(|_| {
        StatisticsFragmentError::exhausted("statistics fragment report has too many Theta columns")
    })?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (column, partial) in partials {
        encode_fragment_column(bytes, column)?;
        let theta = partial.to_wire_bytes()?;
        let theta_len = u32::try_from(theta.len()).map_err(|_| {
            StatisticsFragmentError::exhausted("statistics fragment Theta state is too large")
        })?;
        bytes.extend_from_slice(&theta_len.to_be_bytes());
        bytes.extend_from_slice(&theta);
    }
    Ok(())
}

fn decode_theta_partials(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<BTreeMap<Arc<str>, ThetaSketchPartial>, StatisticsFragmentError> {
    let count = u16::from_be_bytes(
        take_bytes(bytes, cursor, 2)?
            .try_into()
            .expect("fixed count width"),
    ) as usize;
    let mut partials = BTreeMap::new();
    for _ in 0..count {
        let column = decode_fragment_column(bytes, cursor)?;
        let theta_len = u32::from_be_bytes(
            take_bytes(bytes, cursor, 4)?
                .try_into()
                .expect("fixed length width"),
        ) as usize;
        let value = ThetaSketchPartial::try_from_wire_bytes(take_bytes(bytes, cursor, theta_len)?)?;
        if partials.insert(column, value).is_some() {
            return Err(StatisticsFragmentError::contract(
                "statistics fragment report has duplicate Theta columns",
            ));
        }
    }
    Ok(partials)
}

fn encode_fragment_column(
    bytes: &mut Vec<u8>,
    column: &Arc<str>,
) -> Result<(), StatisticsFragmentError> {
    let column = column.as_bytes();
    let length = u16::try_from(column.len()).map_err(|_| {
        StatisticsFragmentError::exhausted("statistics fragment report column name is too large")
    })?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(column);
    Ok(())
}

fn decode_fragment_column(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<Arc<str>, StatisticsFragmentError> {
    let length = u16::from_be_bytes(
        take_bytes(bytes, cursor, 2)?
            .try_into()
            .expect("fixed length width"),
    ) as usize;
    let column = std::str::from_utf8(take_bytes(bytes, cursor, length)?).map_err(|_| {
        StatisticsFragmentError::contract("statistics fragment report column is not UTF-8")
    })?;
    if column.is_empty() {
        return Err(StatisticsFragmentError::contract(
            "statistics fragment report has an empty column name",
        ));
    }
    Ok(Arc::from(column))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Decimal128Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datasketches::theta::{CompactThetaSketch, ThetaSketchBuilder};
    use novarocks_spi::connector::{StatisticsMetric, StatisticsMetricRequest};

    use super::{
        MAX_STATISTICS_THETA_COMPACT_BODY_BYTES, MAX_STATISTICS_THETA_PARTIAL_WIRE_BYTES,
        MAX_STATISTICS_THETA_RETAINED_HASHES, StatisticsBatchCollector, StatisticsFragmentPartial,
        THETA_COMPACT_MAX_PREAMBLE_BYTES, THETA_PARTIAL_WIRE_HEADER_BYTES,
        THETA_PARTIAL_WIRE_VERSION, ThetaSketchPartial,
    };

    fn standard_theta_body(lg_k: u8, count: usize, seed: Option<u64>) -> Vec<u8> {
        let mut builder = ThetaSketchBuilder::default().lg_k(lg_k);
        if let Some(seed) = seed {
            builder = builder.seed(seed);
        }
        let mut sketch = builder.build().expect("Theta sketch");
        for value in 0..count {
            sketch.update(value as i64);
        }
        sketch.trim();
        sketch.compact(true).serialize()
    }

    fn theta_wire(version: u8, lg_k: u8, body: &[u8]) -> Vec<u8> {
        let mut wire = Vec::with_capacity(THETA_PARTIAL_WIRE_HEADER_BYTES + body.len());
        wire.push(version);
        wire.push(lg_k);
        wire.extend_from_slice(body);
        wire
    }

    #[test]
    fn bound_support_predicate_agrees_with_the_measured_bounds() {
        use arrow::array::{
            ArrayRef, BooleanArray, Date32Array, FixedSizeBinaryArray, Float64Array, Int32Array,
            StringArray, TimestampMicrosecondArray,
        };

        // The predicate is a requester's only view of what the accumulator can
        // actually measure, and the accumulator reports a type it cannot bound
        // by staying silent rather than by failing. If the two ever disagree,
        // the requester asks for a bound that never arrives and the whole
        // table's collection is discarded at `finish_visible_row`.
        let cases: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Date32Array::from(vec![1])),
            Arc::new(TimestampMicrosecondArray::from(vec![1])),
            // Bounded today only through the LARGEINT width; a decimal and a
            // shorter fixed-size binary are both unbounded, and the predicate
            // has to say so.
            Arc::new(Decimal128Array::from(vec![1i128])),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([[0u8; 16]].into_iter()).expect("largeint"),
            ),
            Arc::new(FixedSizeBinaryArray::try_from_iter([[0u8; 8]].into_iter()).expect("binary")),
        ];

        for array in cases {
            let data_type = array.data_type().clone();
            let measured = !super::array_scalar_bounds(&array)
                .expect("scalar bounds")
                .is_empty();
            assert_eq!(
                measured,
                super::statistics_scalar_bounds_supported(&data_type),
                "{data_type} disagrees about scalar bound support",
            );
        }
    }

    #[test]
    fn fragment_payload_roundtrips_after_arrow_collection() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let metrics = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            StatisticsMetric::NullCount { column: "v".into() },
            StatisticsMetric::ThetaNdv { column: "v".into() },
        ])
        .expect("metrics");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(2),
                Some(2),
            ]))],
        )
        .expect("batch");
        let mut collector = StatisticsBatchCollector::try_new(schema, metrics).expect("collector");
        collector.push_batch(&batch).expect("push batch");
        let payload = collector.finish_fragment_payload().expect("payload");
        let partial =
            StatisticsFragmentPartial::try_from_payload(&payload).expect("decode payload");
        assert_eq!(payload, partial.to_payload().expect("re-encode payload"));
    }

    #[test]
    fn fragment_payload_collects_decimal_theta_ndv() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Decimal128(18, 2),
            true,
        )]));
        let metrics = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            StatisticsMetric::ThetaNdv { column: "d".into() },
        ])
        .expect("metrics");
        let values = Decimal128Array::from(vec![Some(110_i128), Some(220), Some(110), None])
            .with_precision_and_scale(18, 2)
            .expect("decimal values");
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).expect("batch");

        let mut collector = StatisticsBatchCollector::try_new(schema, metrics).expect("collector");
        collector.push_batch(&batch).expect("push decimal batch");
        let payload = collector.finish_fragment_payload().expect("payload");
        let partial =
            StatisticsFragmentPartial::try_from_payload(&payload).expect("decode payload");
        let compact = CompactThetaSketch::deserialize(&partial.theta["d"].compact_body)
            .expect("standard compact body");
        assert_eq!(
            compact.num_retained(),
            2,
            "Theta NDV must deduplicate equal Decimal128 values"
        );
        assert!(compact.is_ordered());
        assert_eq!(payload, partial.to_payload().expect("re-encode payload"));
    }

    #[test]
    fn fragment_payload_budgets_multiple_theta_sketches_within_spi_limit() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("left_key", DataType::Int64, false),
            Field::new("right_key", DataType::Int64, false),
        ]));
        let metrics = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            StatisticsMetric::ThetaNdv {
                column: "left_key".into(),
            },
            StatisticsMetric::ThetaNdv {
                column: "right_key".into(),
            },
        ])
        .expect("metrics");
        let values = (0_i64..5_000).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(values.clone())),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .expect("batch");

        let mut collector = StatisticsBatchCollector::try_new(schema, metrics).expect("collector");
        collector.push_batch(&batch).expect("push batch");
        let payload = collector.finish_fragment_payload().expect("payload");
        assert!(payload.len() <= novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES);
        let partial =
            StatisticsFragmentPartial::try_from_payload(&payload).expect("decode payload");
        assert_eq!(partial.theta["left_key"].lg_k, 11);
        assert_eq!(partial.theta["right_key"].lg_k, 11);
    }

    #[test]
    fn fragment_payload_rejects_trailing_bytes() {
        let error = StatisticsFragmentPartial::try_from_payload(&[2, 0, 0, 0, 0, 0, 7])
            .expect_err("trailing payload must fail");
        assert!(error.to_string().contains("trailing"));
    }

    #[test]
    fn theta_carrier_uses_standard_ordered_compact_body_with_bounded_sizes() {
        for lg_k in 5_u8..=12 {
            let mut sketch = ThetaSketchBuilder::default()
                .lg_k(lg_k)
                .build()
                .expect("Theta sketch");
            for value in 0..(4 * (1_usize << lg_k)) {
                sketch.update(value as i64);
            }
            let partial = ThetaSketchPartial::try_from_sketch(sketch).expect("Theta partial");
            let compact = CompactThetaSketch::deserialize(&partial.compact_body)
                .expect("standard compact body");
            assert!(compact.is_ordered());
            assert!(compact.num_retained() <= 1_usize << lg_k);

            let wire = partial.to_wire_bytes().expect("Theta carrier");
            let per_lg_k_bound = THETA_PARTIAL_WIRE_HEADER_BYTES
                + THETA_COMPACT_MAX_PREAMBLE_BYTES
                + (1_usize << lg_k) * std::mem::size_of::<u64>();
            assert!(wire.len() <= per_lg_k_bound);
            assert_eq!(
                ThetaSketchPartial::try_from_wire_bytes(&wire)
                    .expect("decode Theta carrier")
                    .to_wire_bytes()
                    .expect("re-encode Theta carrier"),
                wire
            );
        }
    }

    #[test]
    fn theta_carrier_accepts_shared_tck_ordered_v3_body_byte_exact() {
        let body = include_bytes!(
            "../../../../tests/datasketches-tck/fixtures/theta/rust_quickselect_n1000_ordered_v3.sk"
        );
        let wire = theta_wire(THETA_PARTIAL_WIRE_VERSION, 12, body);

        assert_eq!(
            ThetaSketchPartial::try_from_wire_bytes(&wire)
                .expect("decode shared TCK carrier")
                .to_wire_bytes()
                .expect("re-encode shared TCK carrier"),
            wire
        );
    }

    #[test]
    fn theta_carrier_rejects_shared_tck_unordered_body() {
        let body = include_bytes!(
            "../../../../tests/datasketches-tck/fixtures/theta/java62_quickselect_n1000_unordered_v3.sk"
        );
        let error = ThetaSketchPartial::try_from_wire_bytes(&theta_wire(
            THETA_PARTIAL_WIRE_VERSION,
            12,
            body,
        ))
        .expect_err("unordered compact body must fail");

        assert!(error.to_string().contains("must be ordered"));
    }

    #[test]
    fn theta_carrier_rejects_truncation_version_lg_k_and_corrupt_body() {
        let body = standard_theta_body(12, 64, None);

        for truncated in [Vec::new(), vec![THETA_PARTIAL_WIRE_VERSION]] {
            let error = ThetaSketchPartial::try_from_wire_bytes(&truncated)
                .expect_err("truncated carrier must fail");
            assert!(error.to_string().contains("truncated"));
        }

        let error = ThetaSketchPartial::try_from_wire_bytes(&theta_wire(1, 12, &body))
            .expect_err("old carrier version must fail");
        assert!(error.to_string().contains("unsupported version"));

        let error = ThetaSketchPartial::try_from_wire_bytes(&theta_wire(
            THETA_PARTIAL_WIRE_VERSION,
            4,
            &body,
        ))
        .expect_err("invalid lg_k must fail");
        assert!(error.to_string().contains("invalid lg_k"));

        let error = ThetaSketchPartial::try_from_wire_bytes(&theta_wire(
            THETA_PARTIAL_WIRE_VERSION,
            12,
            &[0; 8],
        ))
        .expect_err("corrupt standard body must fail");
        assert!(error.to_string().contains("compact body is invalid"));
    }

    #[test]
    fn theta_carrier_rejects_seed_mismatch_and_oversized_body() {
        let custom_seed_body = standard_theta_body(12, 64, Some(123_456_789));
        let error = ThetaSketchPartial::try_from_wire_bytes(&theta_wire(
            THETA_PARTIAL_WIRE_VERSION,
            12,
            &custom_seed_body,
        ))
        .expect_err("seed mismatch must fail");
        assert!(error.to_string().contains("seed"));

        let oversized_body = vec![0; MAX_STATISTICS_THETA_COMPACT_BODY_BYTES + 1];
        let oversized_wire = theta_wire(0, 4, &oversized_body);
        assert_eq!(
            oversized_wire.len(),
            MAX_STATISTICS_THETA_PARTIAL_WIRE_BYTES + 1
        );
        let error = ThetaSketchPartial::try_from_wire_bytes(&oversized_wire)
            .expect_err("oversized body must fail before standard decode");
        assert!(error.to_string().contains("compact-body limit"));
    }

    #[test]
    fn theta_carrier_checks_retained_count_after_standard_decode() {
        let body = standard_theta_body(13, MAX_STATISTICS_THETA_RETAINED_HASHES + 1, None);
        assert!(body.len() <= MAX_STATISTICS_THETA_COMPACT_BODY_BYTES);
        let compact = CompactThetaSketch::deserialize(&body).expect("standard compact body");
        assert!(compact.num_retained() > MAX_STATISTICS_THETA_RETAINED_HASHES);

        let error = ThetaSketchPartial::try_from_wire_bytes(&theta_wire(
            THETA_PARTIAL_WIRE_VERSION,
            12,
            &body,
        ))
        .expect_err("retained count above carrier budget must fail");
        assert!(error.to_string().contains("retained-hash limit"));
    }
}
