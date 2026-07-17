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

use parquet::arrow::arrow_reader::RowSelection;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use std::ops::Range;

use crate::common::scan_predicate::{
    ColumnStats, ScanLayer, ScanPredicate, ScanPredicateDomainKind, ScanPredicateSource,
    ScanPruner, UnitId, prune_units,
};

use super::{
    BoundVariantPathPruningPredicate, MinMaxPredicate, MinMaxPredicateOp, MinMaxPredicateValue,
    variant_residual_value_all_null_for_row_group,
};

pub(crate) struct PageSelectionResult {
    pub(crate) selection: Option<RowSelection>,
    pub(crate) rows_total: usize,
    pub(crate) rows_selected: usize,
    pub(crate) ranges: usize,
    pub(crate) pages_total: u128,
    pub(crate) pages_selected: u128,
    pub(crate) pages_pruned: u128,
    pub(crate) physical_range_predicates: u128,
    pub(crate) physical_discrete_set_predicates: u128,
    pub(crate) physical_envelope_fallback_predicates: u128,
    pub(crate) page_index_missing: u128,
    pub(crate) offset_index_missing: u128,
    pub(crate) predicates_unsupported: u128,
}

struct PageRangeResult {
    ranges: Vec<Range<usize>>,
    pages_total: usize,
    pages_selected: usize,
}

struct PhysicalPageRangeResult {
    page_ranges: PageRangeResult,
    used_envelope_fallback: bool,
}

enum ParquetPageColumnStats {
    Null,
    MinMax {
        min: MinMaxPredicateValue,
        max: MinMaxPredicateValue,
    },
    Keep,
}

impl ColumnStats for ParquetPageColumnStats {
    fn min_max(&self) -> Option<(MinMaxPredicateValue, MinMaxPredicateValue)> {
        match self {
            Self::MinMax { min, max } => Some((min.clone(), max.clone())),
            Self::Null | Self::Keep => None,
        }
    }

    fn contains(&self, value: &MinMaxPredicateValue) -> Option<bool> {
        match self {
            Self::Null => Some(false),
            Self::Keep => Some(true),
            Self::MinMax { min, max } => {
                let predicate = MinMaxPredicate::Eq {
                    column: String::new(),
                    value: value.clone(),
                };
                page_may_satisfy_min_max_predicate(min, max, &predicate)
            }
        }
    }

    fn may_satisfy_range(
        &self,
        op: MinMaxPredicateOp,
        value: &MinMaxPredicateValue,
    ) -> Result<Option<bool>, String> {
        match self {
            Self::Null => Ok(Some(false)),
            Self::Keep => Ok(Some(true)),
            Self::MinMax { min, max } => {
                let predicate = page_min_max_predicate_from_parts(String::new(), op, value.clone());
                Ok(page_may_satisfy_min_max_predicate(min, max, &predicate))
            }
        }
    }
}

struct ParquetPagePruner<'a> {
    column_index: &'a ColumnIndexMetaData,
    units: Vec<UnitId>,
}

impl<'a> ParquetPagePruner<'a> {
    fn new(column_index: &'a ColumnIndexMetaData, units: Vec<UnitId>) -> Self {
        Self {
            column_index,
            units,
        }
    }
}

impl ScanPruner for ParquetPagePruner<'_> {
    fn layer(&self) -> ScanLayer {
        ScanLayer::Page
    }

    fn accepts_domain(&self, kind: ScanPredicateDomainKind) -> bool {
        matches!(
            kind,
            ScanPredicateDomainKind::Range | ScanPredicateDomainKind::DiscreteSet
        )
    }

    fn units(&self) -> &[UnitId] {
        &self.units
    }

    fn column_stats<'a>(
        &'a self,
        _column: &str,
        unit: UnitId,
    ) -> Option<Box<dyn ColumnStats + 'a>> {
        parquet_page_stats(self.column_index, unit).map(|stats| Box::new(stats) as _)
    }
}

fn empty_page_selection_result() -> PageSelectionResult {
    PageSelectionResult {
        selection: None,
        rows_total: 0,
        rows_selected: 0,
        ranges: 0,
        pages_total: 0,
        pages_selected: 0,
        pages_pruned: 0,
        physical_range_predicates: 0,
        physical_discrete_set_predicates: 0,
        physical_envelope_fallback_predicates: 0,
        page_index_missing: 0,
        offset_index_missing: 0,
        predicates_unsupported: 0,
    }
}

pub(crate) fn build_row_selection_for_row_groups(
    metadata: &ParquetMetaData,
    row_groups: &[usize],
    min_max_predicates: &[MinMaxPredicate],
    variant_predicates: &[BoundVariantPathPruningPredicate],
    columns: &[String],
    case_sensitive: bool,
) -> PageSelectionResult {
    let scan_predicates = min_max_predicates
        .iter()
        .cloned()
        .map(|predicate| {
            ScanPredicate::from_min_max_predicate(predicate, ScanPredicateSource::Static)
        })
        .collect::<Vec<_>>();

    build_row_selection_for_scan_predicates(
        metadata,
        row_groups,
        &scan_predicates,
        variant_predicates,
        columns,
        case_sensitive,
    )
}

pub(crate) fn build_row_selection_for_scan_predicates(
    metadata: &ParquetMetaData,
    row_groups: &[usize],
    scan_predicates: &[ScanPredicate],
    variant_predicates: &[BoundVariantPathPruningPredicate],
    columns: &[String],
    case_sensitive: bool,
) -> PageSelectionResult {
    let mut result = empty_page_selection_result();

    if scan_predicates.is_empty() && variant_predicates.is_empty() {
        return result;
    }

    let Some(column_index) = metadata.column_index() else {
        let total_rows = row_groups
            .iter()
            .map(|&rg_idx| metadata.row_group(rg_idx).num_rows().max(0) as usize)
            .sum::<usize>();
        result.rows_total = total_rows;
        result.rows_selected = total_rows;
        result.page_index_missing = row_groups.len() as u128;
        return result;
    };
    let Some(offset_index) = metadata.offset_index() else {
        let total_rows = row_groups
            .iter()
            .map(|&rg_idx| metadata.row_group(rg_idx).num_rows().max(0) as usize)
            .sum::<usize>();
        result.rows_total = total_rows;
        result.rows_selected = total_rows;
        result.offset_index_missing = row_groups.len() as u128;
        return result;
    };

    let mut global_ranges: Vec<Range<usize>> = Vec::new();
    let mut row_offset = 0usize;
    let mut any_pruned = false;

    for &rg_idx in row_groups {
        let row_group = metadata.row_group(rg_idx);
        let rg_rows = row_group.num_rows().max(0) as usize;
        result.rows_total += rg_rows;

        let mut ranges: Vec<Range<usize>> = std::iter::once(0..rg_rows).collect();
        let mut any_supported = false;

        for predicate in scan_predicates {
            let Some(col_chunk_idx) = physical_predicate_column_chunk_index(
                row_group,
                predicate,
                columns,
                case_sensitive,
            ) else {
                result.predicates_unsupported += 1;
                continue;
            };

            let Some(rg_col_index) = column_index.get(rg_idx).and_then(|v| v.get(col_chunk_idx))
            else {
                result.page_index_missing += 1;
                continue;
            };
            let Some(rg_offset_index) = offset_index.get(rg_idx).and_then(|v| v.get(col_chunk_idx))
            else {
                result.offset_index_missing += 1;
                continue;
            };

            let Some(page_ranges) =
                page_ranges_for_scan_predicate(rg_col_index, rg_offset_index, rg_rows, predicate)
            else {
                result.predicates_unsupported += 1;
                continue;
            };

            record_physical_page_predicate(
                &mut result,
                predicate,
                page_ranges.used_envelope_fallback,
            );
            any_supported = true;
            apply_page_ranges(&mut result, &mut ranges, &page_ranges.page_ranges);
            if ranges.is_empty() {
                any_pruned = true;
                break;
            }
        }

        if !ranges.is_empty() {
            for predicate in variant_predicates {
                if !variant_residual_value_all_null_for_row_group(row_group, predicate) {
                    result.predicates_unsupported += 1;
                    continue;
                }
                if predicate.leaf_column_index >= row_group.columns().len() {
                    result.predicates_unsupported += 1;
                    continue;
                }
                let col_chunk_idx = predicate.leaf_column_index;

                let Some(rg_col_index) =
                    column_index.get(rg_idx).and_then(|v| v.get(col_chunk_idx))
                else {
                    result.page_index_missing += 1;
                    continue;
                };
                let Some(rg_offset_index) =
                    offset_index.get(rg_idx).and_then(|v| v.get(col_chunk_idx))
                else {
                    result.offset_index_missing += 1;
                    continue;
                };

                let Some(page_ranges) = page_ranges_for_predicate(
                    rg_col_index,
                    rg_offset_index,
                    rg_rows,
                    &predicate.predicate,
                ) else {
                    result.predicates_unsupported += 1;
                    continue;
                };

                any_supported = true;
                apply_page_ranges(&mut result, &mut ranges, &page_ranges);
                if ranges.is_empty() {
                    any_pruned = true;
                    break;
                }
            }
        }

        if any_supported {
            any_pruned = true;
        }

        if !ranges.is_empty() {
            let merged = merge_ranges(ranges);
            for r in &merged {
                result.rows_selected += r.end.saturating_sub(r.start);
                result.ranges += 1;
                global_ranges.push((r.start + row_offset)..(r.end + row_offset));
            }
        }
        row_offset = row_offset.saturating_add(rg_rows);
    }

    if result.rows_total == 0 {
        return result;
    }

    if !any_pruned || result.rows_selected == result.rows_total {
        return result;
    }

    let selection = RowSelection::from_consecutive_ranges(global_ranges.into_iter(), row_offset);
    result.selection = Some(selection);
    result
}

fn physical_predicate_column_chunk_index(
    row_group: &parquet::file::metadata::RowGroupMetaData,
    predicate: &ScanPredicate,
    columns: &[String],
    case_sensitive: bool,
) -> Option<usize> {
    let col_idx = predicate.column().parse::<usize>().ok()?;
    let col_name = columns.get(col_idx)?;

    row_group.columns().iter().position(|column| {
        let path_str = column.column_path().string();
        if case_sensitive {
            path_str == *col_name
        } else {
            path_str.eq_ignore_ascii_case(col_name)
        }
    })
}

fn record_physical_page_predicate(
    result: &mut PageSelectionResult,
    predicate: &ScanPredicate,
    used_envelope_fallback: bool,
) {
    if used_envelope_fallback {
        result.physical_envelope_fallback_predicates += 1;
        return;
    }

    match predicate.domain().kind() {
        ScanPredicateDomainKind::Range => result.physical_range_predicates += 1,
        ScanPredicateDomainKind::DiscreteSet => result.physical_discrete_set_predicates += 1,
        ScanPredicateDomainKind::Membership => {}
    }
}

fn apply_page_ranges(
    result: &mut PageSelectionResult,
    ranges: &mut Vec<Range<usize>>,
    page_ranges: &PageRangeResult,
) {
    let pages_total = page_ranges.pages_total as u128;
    let pages_selected = page_ranges.pages_selected as u128;
    result.pages_total += pages_total;
    result.pages_selected += pages_selected;
    if pages_total >= pages_selected {
        result.pages_pruned += pages_total - pages_selected;
    }

    *ranges = intersect_ranges(ranges, &page_ranges.ranges);
}

fn page_ranges_for_scan_predicate(
    column_index: &ColumnIndexMetaData,
    offset_index: &OffsetIndexMetaData,
    total_rows: usize,
    predicate: &ScanPredicate,
) -> Option<PhysicalPageRangeResult> {
    if !parquet_page_index_supports_stats(column_index) {
        return None;
    }

    let page_locations = offset_index.page_locations();
    let num_pages = page_locations.len();
    if num_pages == 0 {
        return None;
    }

    let page_units = (0..num_pages).collect::<Vec<_>>();
    let pruner = ParquetPagePruner::new(column_index, page_units);
    let domain_kind = predicate.domain().kind();
    let predicate_batch;
    let used_envelope_fallback;

    let predicates = if pruner.accepts_domain(domain_kind) {
        used_envelope_fallback = false;
        std::slice::from_ref(predicate)
    } else {
        predicate_batch = predicate.fallback_predicates();
        if predicate_batch.is_empty()
            || !predicate_batch
                .iter()
                .all(|fallback| pruner.accepts_domain(fallback.domain().kind()))
        {
            return None;
        }
        used_envelope_fallback = true;
        predicate_batch.as_slice()
    };

    let prune_result = prune_units(&pruner, predicates).ok()?;
    Some(PhysicalPageRangeResult {
        page_ranges: page_ranges_from_kept_pages(
            offset_index,
            total_rows,
            num_pages,
            &prune_result.kept_units,
        ),
        used_envelope_fallback,
    })
}

fn page_ranges_from_kept_pages(
    offset_index: &OffsetIndexMetaData,
    total_rows: usize,
    num_pages: usize,
    kept_pages: &[UnitId],
) -> PageRangeResult {
    let page_locations = offset_index.page_locations();
    let mut ranges: Vec<Range<usize>> = Vec::new();
    let mut pages_selected = 0usize;

    for &page_idx in kept_pages {
        if page_idx >= num_pages {
            continue;
        }
        let start = page_locations[page_idx].first_row_index.max(0) as usize;
        let end = if page_idx + 1 < num_pages {
            page_locations[page_idx + 1].first_row_index.max(0) as usize
        } else {
            total_rows
        };
        if start < end {
            ranges.push(start..end);
            pages_selected += 1;
        }
    }

    PageRangeResult {
        ranges: merge_ranges(ranges),
        pages_total: num_pages,
        pages_selected,
    }
}

fn parquet_page_index_supports_stats(column_index: &ColumnIndexMetaData) -> bool {
    matches!(
        column_index,
        ColumnIndexMetaData::BOOLEAN(_)
            | ColumnIndexMetaData::INT32(_)
            | ColumnIndexMetaData::INT64(_)
            | ColumnIndexMetaData::FLOAT(_)
            | ColumnIndexMetaData::DOUBLE(_)
            | ColumnIndexMetaData::BYTE_ARRAY(_)
            | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(_)
    )
}

fn parquet_page_stats(
    column_index: &ColumnIndexMetaData,
    page_idx: usize,
) -> Option<ParquetPageColumnStats> {
    if !parquet_page_index_supports_stats(column_index) {
        return None;
    }
    if page_idx >= column_index.num_pages() as usize {
        return None;
    }
    if column_index.is_null_page(page_idx) {
        return Some(ParquetPageColumnStats::Null);
    }

    match column_index {
        ColumnIndexMetaData::BOOLEAN(index) => Some(ParquetPageColumnStats::MinMax {
            min: MinMaxPredicateValue::Boolean(index.min_values()[page_idx]),
            max: MinMaxPredicateValue::Boolean(index.max_values()[page_idx]),
        }),
        ColumnIndexMetaData::INT32(index) => Some(ParquetPageColumnStats::MinMax {
            min: MinMaxPredicateValue::Int32(index.min_values()[page_idx]),
            max: MinMaxPredicateValue::Int32(index.max_values()[page_idx]),
        }),
        ColumnIndexMetaData::INT64(index) => Some(ParquetPageColumnStats::MinMax {
            min: MinMaxPredicateValue::Int64(index.min_values()[page_idx]),
            max: MinMaxPredicateValue::Int64(index.max_values()[page_idx]),
        }),
        ColumnIndexMetaData::FLOAT(index) => {
            let min = index.min_values()[page_idx];
            let max = index.max_values()[page_idx];
            if min.is_nan() || max.is_nan() {
                return Some(ParquetPageColumnStats::Keep);
            }
            Some(ParquetPageColumnStats::MinMax {
                min: MinMaxPredicateValue::Float(min),
                max: MinMaxPredicateValue::Float(max),
            })
        }
        ColumnIndexMetaData::DOUBLE(index) => {
            let min = index.min_values()[page_idx];
            let max = index.max_values()[page_idx];
            if min.is_nan() || max.is_nan() {
                return Some(ParquetPageColumnStats::Keep);
            }
            Some(ParquetPageColumnStats::MinMax {
                min: MinMaxPredicateValue::Double(min),
                max: MinMaxPredicateValue::Double(max),
            })
        }
        ColumnIndexMetaData::BYTE_ARRAY(index) => Some(ParquetPageColumnStats::MinMax {
            min: MinMaxPredicateValue::ByteArray(index.min_value(page_idx)?.to_vec()),
            max: MinMaxPredicateValue::ByteArray(index.max_value(page_idx)?.to_vec()),
        }),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index) => Some(ParquetPageColumnStats::MinMax {
            min: MinMaxPredicateValue::FixedLenByteArray(index.min_value(page_idx)?.to_vec()),
            max: MinMaxPredicateValue::FixedLenByteArray(index.max_value(page_idx)?.to_vec()),
        }),
        ColumnIndexMetaData::NONE | ColumnIndexMetaData::INT96(_) => None,
    }
}

fn page_may_satisfy_min_max_predicate(
    min: &MinMaxPredicateValue,
    max: &MinMaxPredicateValue,
    predicate: &MinMaxPredicate,
) -> Option<bool> {
    match (min, max) {
        (MinMaxPredicateValue::Boolean(min), MinMaxPredicateValue::Boolean(max)) => {
            let value = predicate.value().as_bool()?;
            Some(page_satisfies_predicate_bool(*min, *max, value, predicate))
        }
        (MinMaxPredicateValue::Int32(min), MinMaxPredicateValue::Int32(max)) => {
            let value = predicate.value().as_i32()?;
            Some(page_satisfies_predicate_i32(*min, *max, value, predicate))
        }
        (MinMaxPredicateValue::Int64(min), MinMaxPredicateValue::Int64(max)) => {
            let value = predicate.value().as_i64()?;
            Some(page_satisfies_predicate_i64(*min, *max, value, predicate))
        }
        (MinMaxPredicateValue::Float(min), MinMaxPredicateValue::Float(max)) => {
            let value = predicate.value().as_f32()?;
            Some(page_satisfies_predicate_f32(*min, *max, value, predicate))
        }
        (MinMaxPredicateValue::Double(min), MinMaxPredicateValue::Double(max)) => {
            let value = predicate.value().as_f64()?;
            Some(page_satisfies_predicate_f64(*min, *max, value, predicate))
        }
        (MinMaxPredicateValue::ByteArray(min), MinMaxPredicateValue::ByteArray(max))
        | (
            MinMaxPredicateValue::FixedLenByteArray(min),
            MinMaxPredicateValue::FixedLenByteArray(max),
        ) => {
            let value = predicate.value().as_bytes()?;
            Some(page_satisfies_predicate_bytes(
                min.as_slice(),
                max.as_slice(),
                value,
                predicate,
            ))
        }
        _ => None,
    }
}

fn page_min_max_predicate_from_parts(
    column: String,
    op: MinMaxPredicateOp,
    value: MinMaxPredicateValue,
) -> MinMaxPredicate {
    match op {
        MinMaxPredicateOp::Le => MinMaxPredicate::Le { column, value },
        MinMaxPredicateOp::Ge => MinMaxPredicate::Ge { column, value },
        MinMaxPredicateOp::Lt => MinMaxPredicate::Lt { column, value },
        MinMaxPredicateOp::Gt => MinMaxPredicate::Gt { column, value },
        MinMaxPredicateOp::Eq => MinMaxPredicate::Eq { column, value },
    }
}

fn page_ranges_for_predicate(
    column_index: &ColumnIndexMetaData,
    offset_index: &OffsetIndexMetaData,
    total_rows: usize,
    predicate: &MinMaxPredicate,
) -> Option<PageRangeResult> {
    if matches!(column_index, ColumnIndexMetaData::NONE) {
        return None;
    }

    let page_locations = offset_index.page_locations();
    let num_pages = page_locations.len();
    if num_pages == 0 {
        return None;
    }

    let mut ranges: Vec<Range<usize>> = Vec::new();
    let mut pages_selected = 0usize;

    let mut push_page_range = |page_idx: usize| {
        let start = page_locations[page_idx].first_row_index.max(0) as usize;
        let end = if page_idx + 1 < num_pages {
            page_locations[page_idx + 1].first_row_index.max(0) as usize
        } else {
            total_rows
        };
        if start < end {
            ranges.push(start..end);
            pages_selected += 1;
        }
    };

    match column_index {
        ColumnIndexMetaData::BOOLEAN(index) => {
            let v = predicate.value().as_bool()?;
            for page_idx in 0..num_pages {
                if index.is_null_page(page_idx) {
                    continue;
                }
                let min = index.min_values()[page_idx];
                let max = index.max_values()[page_idx];
                if page_satisfies_predicate_bool(min, max, v, predicate) {
                    push_page_range(page_idx);
                }
            }
        }
        ColumnIndexMetaData::INT32(index) => {
            let v = predicate.value().as_i32()?;
            for page_idx in 0..num_pages {
                if index.is_null_page(page_idx) {
                    continue;
                }
                let min = index.min_values()[page_idx];
                let max = index.max_values()[page_idx];
                if page_satisfies_predicate_i32(min, max, v, predicate) {
                    push_page_range(page_idx);
                }
            }
        }
        ColumnIndexMetaData::INT64(index) => {
            let v = predicate.value().as_i64()?;
            for page_idx in 0..num_pages {
                if index.is_null_page(page_idx) {
                    continue;
                }
                let min = index.min_values()[page_idx];
                let max = index.max_values()[page_idx];
                if page_satisfies_predicate_i64(min, max, v, predicate) {
                    push_page_range(page_idx);
                }
            }
        }
        ColumnIndexMetaData::FLOAT(index) => {
            let v = predicate.value().as_f32()?;
            for page_idx in 0..num_pages {
                if index.is_null_page(page_idx) {
                    continue;
                }
                let min = index.min_values()[page_idx];
                let max = index.max_values()[page_idx];
                if min.is_nan() || max.is_nan() {
                    push_page_range(page_idx);
                    continue;
                }
                if page_satisfies_predicate_f32(min, max, v, predicate) {
                    push_page_range(page_idx);
                }
            }
        }
        ColumnIndexMetaData::DOUBLE(index) => {
            let v = predicate.value().as_f64()?;
            for page_idx in 0..num_pages {
                if index.is_null_page(page_idx) {
                    continue;
                }
                let min = index.min_values()[page_idx];
                let max = index.max_values()[page_idx];
                if min.is_nan() || max.is_nan() {
                    push_page_range(page_idx);
                    continue;
                }
                if page_satisfies_predicate_f64(min, max, v, predicate) {
                    push_page_range(page_idx);
                }
            }
        }
        ColumnIndexMetaData::BYTE_ARRAY(index) => {
            let v = predicate.value().as_bytes()?;
            for page_idx in 0..num_pages {
                if index.is_null_page(page_idx) {
                    continue;
                }
                let min = index.min_value(page_idx)?;
                let max = index.max_value(page_idx)?;
                if page_satisfies_predicate_bytes(min, max, v, predicate) {
                    push_page_range(page_idx);
                }
            }
        }
        _ => return None,
    }

    Some(PageRangeResult {
        ranges: merge_ranges(ranges),
        pages_total: num_pages,
        pages_selected,
    })
}

fn merge_ranges(ranges: Vec<Range<usize>>) -> Vec<Range<usize>> {
    if ranges.is_empty() {
        return ranges;
    }
    let mut merged = Vec::with_capacity(ranges.len());
    let mut current = ranges[0].clone();
    for r in ranges.into_iter().skip(1) {
        if r.start <= current.end {
            if r.end > current.end {
                current.end = r.end;
            }
        } else {
            merged.push(current);
            current = r;
        }
    }
    merged.push(current);
    merged
}

fn intersect_ranges(a: &[Range<usize>], b: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        let start = a[i].start.max(b[j].start);
        let end = a[i].end.min(b[j].end);
        if start < end {
            out.push(start..end);
        }
        if a[i].end < b[j].end {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

fn page_satisfies_predicate_bool(min: bool, max: bool, v: bool, pred: &MinMaxPredicate) -> bool {
    match pred {
        MinMaxPredicate::Le { .. } => min <= v,
        MinMaxPredicate::Ge { .. } => max >= v,
        MinMaxPredicate::Lt { .. } => min < v,
        MinMaxPredicate::Gt { .. } => max > v,
        MinMaxPredicate::Eq { .. } => min <= v && max >= v,
    }
}

fn page_satisfies_predicate_i32(min: i32, max: i32, v: i32, pred: &MinMaxPredicate) -> bool {
    match pred {
        MinMaxPredicate::Le { .. } => min <= v,
        MinMaxPredicate::Ge { .. } => max >= v,
        MinMaxPredicate::Lt { .. } => min < v,
        MinMaxPredicate::Gt { .. } => max > v,
        MinMaxPredicate::Eq { .. } => min <= v && max >= v,
    }
}

fn page_satisfies_predicate_i64(min: i64, max: i64, v: i64, pred: &MinMaxPredicate) -> bool {
    match pred {
        MinMaxPredicate::Le { .. } => min <= v,
        MinMaxPredicate::Ge { .. } => max >= v,
        MinMaxPredicate::Lt { .. } => min < v,
        MinMaxPredicate::Gt { .. } => max > v,
        MinMaxPredicate::Eq { .. } => min <= v && max >= v,
    }
}

fn page_satisfies_predicate_f32(min: f32, max: f32, v: f32, pred: &MinMaxPredicate) -> bool {
    match pred {
        MinMaxPredicate::Le { .. } => min <= v,
        MinMaxPredicate::Ge { .. } => max >= v,
        MinMaxPredicate::Lt { .. } => min < v,
        MinMaxPredicate::Gt { .. } => max > v,
        MinMaxPredicate::Eq { .. } => min <= v && max >= v,
    }
}

fn page_satisfies_predicate_f64(min: f64, max: f64, v: f64, pred: &MinMaxPredicate) -> bool {
    match pred {
        MinMaxPredicate::Le { .. } => min <= v,
        MinMaxPredicate::Ge { .. } => max >= v,
        MinMaxPredicate::Lt { .. } => min < v,
        MinMaxPredicate::Gt { .. } => max > v,
        MinMaxPredicate::Eq { .. } => min <= v && max >= v,
    }
}

fn page_satisfies_predicate_bytes(
    min: &[u8],
    max: &[u8],
    v: &[u8],
    pred: &MinMaxPredicate,
) -> bool {
    match pred {
        MinMaxPredicate::Le { .. } => min <= v,
        MinMaxPredicate::Ge { .. } => max >= v,
        MinMaxPredicate::Lt { .. } => min < v,
        MinMaxPredicate::Gt { .. } => max > v,
        MinMaxPredicate::Eq { .. } => min <= v && max >= v,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::serialized_reader::ReadOptionsBuilder;

    use crate::common::scan_predicate::{
        ScanLayer, ScanPredicate, ScanPredicateDomainKind, ScanPredicateSource, ScanPruner,
    };
    use crate::formats::parquet::{BoundVariantPathPruningPredicate, MinMaxPredicateValue};

    use super::*;

    fn page_index_metadata(field: Field, array: ArrayRef, rows_per_page: usize) -> ParquetMetaData {
        page_index_metadata_with_columns(vec![field], vec![array], rows_per_page)
    }

    fn page_index_metadata_with_columns(
        fields: Vec<Field>,
        arrays: Vec<ArrayRef>,
        rows_per_page: usize,
    ) -> ParquetMetaData {
        assert!(!arrays.is_empty());
        let row_count = arrays[0].len();
        assert!(arrays.iter().all(|array| array.len() == row_count));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).expect("batch");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(row_count))
            .set_write_batch_size(rows_per_page)
            .set_data_page_row_count_limit(rows_per_page)
            .set_data_page_size_limit(1)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();

        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer =
                ArrowWriter::try_new(cursor, schema, Some(props)).expect("parquet writer");
            writer.write(&batch).expect("write parquet batch");
            writer.close().expect("close parquet writer");
        }

        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(bytes::Bytes::from(buffer), options)
            .expect("metadata reader");
        reader.metadata().clone()
    }

    fn bound_variant_gt_i64(value: i64) -> BoundVariantPathPruningPredicate {
        BoundVariantPathPruningPredicate {
            leaf_column_index: 0,
            leaf_column_path: "payload.typed_value.a.typed_value".to_string(),
            residual_value_column_index: None,
            residual_value_column_path: None,
            requested_type: DataType::Int64,
            predicate: MinMaxPredicate::Gt {
                column: "__nr_var_payload_a".to_string(),
                value: MinMaxPredicateValue::Int64(value),
            },
        }
    }

    #[test]
    fn variant_page_selection_uses_bound_typed_leaf_page_index() {
        let metadata = page_index_metadata(
            Field::new("payload.typed_value.a.typed_value", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1, 2, 3, 10, 11, 12])) as ArrayRef,
            1,
        );
        let predicate = bound_variant_gt_i64(5);

        let selection =
            build_row_selection_for_row_groups(&metadata, &[0], &[], &[predicate], &[], true);

        assert_eq!(selection.rows_total, 6);
        assert_eq!(selection.rows_selected, 3);
        assert_eq!(selection.pages_total, 6);
        assert_eq!(selection.pages_selected, 3);
        assert_eq!(selection.pages_pruned, 3);
        assert_eq!(selection.predicates_unsupported, 0);
        assert!(selection.selection.is_some());
    }

    #[test]
    fn variant_page_selection_keeps_all_rows_when_residual_value_has_non_nulls() {
        let metadata = page_index_metadata_with_columns(
            vec![
                Field::new("payload.typed_value.a.typed_value", DataType::Int64, true),
                Field::new("payload.typed_value.a.value", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 10, 11, 12])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("residual"),
                    None,
                    None,
                    None,
                    None,
                    None,
                ])) as ArrayRef,
            ],
            1,
        );
        let mut predicate = bound_variant_gt_i64(5);
        predicate.residual_value_column_index = Some(1);
        predicate.residual_value_column_path = Some("payload.typed_value.a.value".to_string());

        let selection =
            build_row_selection_for_row_groups(&metadata, &[0], &[], &[predicate], &[], true);

        assert_eq!(selection.rows_total, 6);
        assert_eq!(selection.rows_selected, 6);
        assert_eq!(selection.pages_pruned, 0);
        assert!(selection.selection.is_none());
    }

    #[test]
    fn page_selection_supports_boolean_and_utf8_min_max() {
        let bool_metadata = page_index_metadata(
            Field::new("flag", DataType::Boolean, true),
            Arc::new(BooleanArray::from(vec![
                false, false, false, true, true, true,
            ])) as ArrayRef,
            1,
        );
        let bool_selection = build_row_selection_for_row_groups(
            &bool_metadata,
            &[0],
            &[MinMaxPredicate::Eq {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Boolean(true),
            }],
            &[],
            &["flag".to_string()],
            true,
        );

        assert_eq!(bool_selection.rows_total, 6);
        assert_eq!(bool_selection.rows_selected, 3);
        assert_eq!(bool_selection.pages_total, 6);
        assert_eq!(bool_selection.pages_selected, 3);
        assert_eq!(bool_selection.predicates_unsupported, 0);
        assert!(bool_selection.selection.is_some());

        let string_metadata = page_index_metadata(
            Field::new("name", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![
                "apple", "banana", "cat", "yak", "zebra", "zoo",
            ])) as ArrayRef,
            1,
        );
        let string_selection = build_row_selection_for_row_groups(
            &string_metadata,
            &[0],
            &[MinMaxPredicate::Ge {
                column: "0".to_string(),
                value: MinMaxPredicateValue::ByteArray(b"m".to_vec()),
            }],
            &[],
            &["name".to_string()],
            true,
        );

        assert_eq!(string_selection.rows_total, 6);
        assert_eq!(string_selection.rows_selected, 3);
        assert_eq!(string_selection.pages_total, 6);
        assert_eq!(string_selection.pages_selected, 3);
        assert_eq!(string_selection.predicates_unsupported, 0);
        assert!(string_selection.selection.is_some());
    }

    #[test]
    fn variant_page_selection_ignores_out_of_range_leaf_column_index() {
        let metadata = page_index_metadata(
            Field::new("payload.typed_value.a.typed_value", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1, 2, 3, 10, 11, 12])) as ArrayRef,
            1,
        );
        let mut predicate = bound_variant_gt_i64(5);
        predicate.leaf_column_index = 9;

        let selection =
            build_row_selection_for_row_groups(&metadata, &[0], &[], &[predicate], &[], true);

        assert_eq!(selection.rows_total, 6);
        assert_eq!(selection.rows_selected, 6);
        assert_eq!(selection.predicates_unsupported, 1);
        assert!(selection.selection.is_none());
    }

    fn sparse_i64_discrete_page_predicate() -> ScanPredicate {
        ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int64(1),
                MinMaxPredicateValue::Int64(101),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete page predicate")
    }

    #[test]
    fn parquet_page_pruner_declares_discrete_set_capability() {
        let metadata = page_index_metadata(
            Field::new("id", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1, 2, 50, 51, 100, 101])) as ArrayRef,
            2,
        );
        let column_index = metadata.column_index().expect("page column index");
        let offset_index = metadata.offset_index().expect("page offset index");
        let pruner = ParquetPagePruner::new(&column_index[0][0], vec![0, 1, 2]);

        assert_eq!(pruner.layer(), ScanLayer::Page);
        assert!(pruner.accepts_domain(ScanPredicateDomainKind::Range));
        assert!(pruner.accepts_domain(ScanPredicateDomainKind::DiscreteSet));
        assert!(!pruner.accepts_domain(ScanPredicateDomainKind::Membership));
        assert_eq!(pruner.units(), &[0, 1, 2]);
        assert_eq!(offset_index[0][0].page_locations().len(), 3);
    }

    #[test]
    fn page_selection_uses_discrete_set_without_envelope() {
        let metadata = page_index_metadata(
            Field::new("id", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1, 2, 50, 51, 100, 101])) as ArrayRef,
            2,
        );
        let predicate = sparse_i64_discrete_page_predicate();

        let selection = build_row_selection_for_scan_predicates(
            &metadata,
            &[0],
            &[predicate],
            &[],
            &["id".to_string()],
            true,
        );

        assert_eq!(selection.rows_total, 6);
        assert_eq!(selection.rows_selected, 4);
        assert_eq!(selection.pages_total, 3);
        assert_eq!(selection.pages_selected, 2);
        assert_eq!(selection.pages_pruned, 1);
        assert_eq!(selection.physical_range_predicates, 0);
        assert_eq!(selection.physical_discrete_set_predicates, 1);
        assert_eq!(selection.physical_envelope_fallback_predicates, 0);
        assert_eq!(selection.predicates_unsupported, 0);
        assert!(selection.selection.is_some());
    }

    #[test]
    fn page_selection_keeps_all_rows_when_discrete_set_column_is_missing() {
        let metadata = page_index_metadata(
            Field::new("id", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1, 2, 50, 51, 100, 101])) as ArrayRef,
            2,
        );
        let predicate = ScanPredicate::discrete_set(
            "9".to_string(),
            vec![
                MinMaxPredicateValue::Int64(1),
                MinMaxPredicateValue::Int64(101),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete page predicate");

        let selection = build_row_selection_for_scan_predicates(
            &metadata,
            &[0],
            &[predicate],
            &[],
            &["id".to_string()],
            true,
        );

        assert_eq!(selection.rows_total, 6);
        assert_eq!(selection.rows_selected, 6);
        assert_eq!(selection.pages_pruned, 0);
        assert_eq!(selection.predicates_unsupported, 1);
        assert!(selection.selection.is_none());
    }
}
