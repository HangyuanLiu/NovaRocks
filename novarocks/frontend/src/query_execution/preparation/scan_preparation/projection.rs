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

use crate::query_execution::preparation::scan::{
    ResolvedReadColumn, ResolvedReadReason, ResolvedScanColumn, ResolvedScanColumnKind,
};
use novarocks_sql::plan_read::ColumnId;
use novarocks_sql::plan_read::PlanScanNode;
use novarocks_sql::planning::query_execution::scan_preparation_facts;

fn resolve_physical_column_occurrences(
    node_id: i32,
    scan: &PlanScanNode,
) -> Result<Vec<ResolvedScanColumn>, String> {
    use std::collections::BTreeSet;

    let source_columns = scan
        .table
        .columns
        .iter()
        .map(|column| (column, ResolvedScanColumnKind::PhysicalTableColumn))
        .chain(
            scan.table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|column| (column, ResolvedScanColumnKind::IcebergMetadataColumn)),
        )
        .collect::<Vec<_>>();
    let planner_columns = scan
        .columns
        .iter()
        .filter(|column| !is_variant_synthetic_column(scan, column.column_id))
        .collect::<Vec<_>>();
    let mut planner_ids = BTreeSet::new();
    let mut provider_names = BTreeSet::new();
    for (provider_ordinal, (source, _)) in source_columns.iter().enumerate() {
        if !provider_names.insert(source.name.to_ascii_lowercase()) {
            return Err(format!(
                "scan binding node_id={node_id} repeats provider column '{}' at provider ordinal {provider_ordinal}",
                source.name
            ));
        }
    }
    let mut bound_provider_ordinals = BTreeSet::new();
    planner_columns
        .into_iter()
        .map(|planner| {
            if !planner_ids.insert(planner.column_id) {
                return Err(format!(
                    "scan binding node_id={node_id} repeats planner column id {}",
                    planner.column_id
                ));
            }
            let matches = source_columns
                .iter()
                .enumerate()
                .filter(|(_, (source, _))| source.name.eq_ignore_ascii_case(&planner.name))
                .collect::<Vec<_>>();
            let [(provider_ordinal, (source, kind))] = matches.as_slice() else {
                return Err(format!(
                    "scan binding node_id={node_id} cannot resolve planner physical column '{}' to exactly one provider ordinal in table '{}'",
                    planner.name, scan.table.name
                ));
            };
            if !bound_provider_ordinals.insert(*provider_ordinal) {
                return Err(format!(
                    "scan binding node_id={node_id} maps more than one planner occurrence to provider ordinal {provider_ordinal}"
                ));
            }
            if planner.data_type != source.data_type {
                return Err(format!(
                    "scan binding node_id={node_id} column '{}' at provider ordinal {provider_ordinal} has type mismatch: planner={:?}, provider={:?}",
                    planner.name, planner.data_type, source.data_type
                ));
            }
            if planner.nullable != source.nullable {
                return Err(format!(
                    "scan binding node_id={node_id} column '{}' at provider ordinal {provider_ordinal} has nullability mismatch: planner={}, provider={}",
                    planner.name, planner.nullable, source.nullable
                ));
            }
            Ok(ResolvedScanColumn {
                planner: planner.clone(),
                source: (*source).clone(),
                kind: *kind,
            })
        })
        .collect()
}

fn resolve_provider_source_by_name<'a>(
    node_id: i32,
    scan: &'a PlanScanNode,
    name: &str,
) -> Result<&'a novarocks_types::schema::ColumnDef, String> {
    let matches = scan
        .table
        .columns
        .iter()
        .chain(&scan.table.iceberg_row_lineage_metadata_columns)
        .filter(|source| source.name.eq_ignore_ascii_case(name))
        .collect::<Vec<_>>();
    let [source] = matches.as_slice() else {
        return Err(format!(
            "scan binding node_id={node_id} provider column '{name}' does not resolve to exactly one provider ordinal in table '{}'",
            scan.table.name
        ));
    };
    Ok(*source)
}

fn refresh_scan_projected_column_ids(
    node_id: i32,
    scan: &PlanScanNode,
    physical: &[ResolvedScanColumn],
) -> Result<Option<Vec<ColumnId>>, String> {
    let facts = scan_preparation_facts(scan);
    let Some(projected_names) = facts.refresh_projected_names() else {
        return Ok(None);
    };
    projected_names
        .iter()
        .map(|name| {
            let matches = physical
                .iter()
                .filter(|column| column.source.name.eq_ignore_ascii_case(name))
                .collect::<Vec<_>>();
            let [column] = matches.as_slice() else {
                return Err(format!(
                    "scan binding node_id={node_id} refresh projection column '{name}' does not resolve to exactly one provider ordinal in table '{}'",
                    scan.table.name
                ));
            };
            Ok(column.planner.column_id)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub(super) fn merge_required_column_ids_with_projected(
    node_id: i32,
    existing: Option<&[ColumnId]>,
    projected: &[ColumnId],
) -> Result<Vec<ColumnId>, String> {
    use std::collections::BTreeSet;

    let mut out = projected.to_vec();
    let mut seen = projected.iter().copied().collect::<BTreeSet<_>>();
    let mut existing_seen = BTreeSet::new();
    for column_id in existing.into_iter().flatten().copied() {
        if !existing_seen.insert(column_id) {
            return Err(format!(
                "scan binding node_id={node_id} repeats required planner column id {column_id}"
            ));
        }
        if seen.insert(column_id) {
            out.push(column_id);
        }
    }
    Ok(out)
}

fn effective_projection_ids(
    node_id: i32,
    scan: &PlanScanNode,
    physical: &[ResolvedScanColumn],
) -> Result<Option<Vec<ColumnId>>, String> {
    match refresh_scan_projected_column_ids(node_id, scan, physical)? {
        Some(projected) => Ok(Some(merge_required_column_ids_with_projected(
            node_id,
            scan.required_columns.as_deref(),
            &projected,
        )?)),
        None => Ok(None),
    }
}

pub(super) fn resolve_physical_columns(
    node_id: i32,
    scan: &PlanScanNode,
) -> Result<Vec<ResolvedScanColumn>, String> {
    let physical = resolve_physical_column_occurrences(node_id, scan)?;
    let Some(projected_ids) = effective_projection_ids(node_id, scan, &physical)? else {
        return Ok(physical);
    };
    let by_id = physical
        .into_iter()
        .map(|column| (column.planner.column_id, column))
        .collect::<std::collections::BTreeMap<_, _>>();
    projected_ids
        .into_iter()
        .map(|column_id| {
            by_id.get(&column_id).cloned().ok_or_else(|| {
                format!(
                    "scan binding node_id={node_id} projected planner column id {column_id} has no exact provider ordinal"
                )
            })
        })
        .collect()
}

/// The physical columns one typed connector scan actually reads.
///
/// The physical columns one typed connector scan reads, in scan output order.
///
/// This is the same set the wire declares as `required_columns`, resolved
/// through the same helper, because the backend filters its own decoded output
/// by exactly that list and then reads whatever survives. Deriving the
/// assignments from any wider list — every column the relation has, say — makes
/// the two sides describe different columns, and for an Iceberg relation the
/// wider list also contains the metadata pseudo-columns (`_file`, `_pos`) that
/// no data file holds and no connector column binding names.
pub(super) fn resolve_read_physical_columns(
    node_id: i32,
    scan: &PlanScanNode,
) -> Result<Vec<ResolvedScanColumn>, String> {
    // Resolved first so a projection that cannot be resolved at all is
    // reported as the projection defect it is, rather than as whichever
    // required name happened to reach the read resolver first.
    let physical = resolve_physical_columns(node_id, scan)?;
    // No equality-delete columns: those are a mutation-lane fact, and a typed
    // read never adds one of its own.
    let required = resolve_effective_required_reads(node_id, scan, &[])?;
    let mut required_ids = required
        .iter()
        .filter_map(|read| read.planner_column_id)
        .collect::<std::collections::BTreeSet<_>>();
    // A VARIANT path column is built above the connector out of a physical
    // source column. The derived column is required, the source it is derived
    // from may not be named anywhere else, and dropping it would leave the
    // materialization with no input.
    required_ids.extend(
        scan.variant_columns
            .iter()
            .map(|variant| variant.source_column_id),
    );
    Ok(physical
        .into_iter()
        .filter(|column| required_ids.contains(&column.planner.column_id))
        .collect())
}

pub(super) fn resolve_effective_required_reads(
    node_id: i32,
    scan: &PlanScanNode,
    equality_required: &[String],
) -> Result<Vec<ResolvedReadColumn>, String> {
    let physical = resolve_physical_column_occurrences(node_id, scan)?;
    let required_ids = match effective_projection_ids(node_id, scan, &physical)? {
        Some(ids) => ids,
        None => match scan.required_columns.as_deref() {
            Some(ids) => merge_required_column_ids_with_projected(node_id, Some(ids), &[])?,
            None => physical
                .iter()
                .map(|column| column.planner.column_id)
                .collect(),
        },
    };
    let physical_by_id = physical
        .iter()
        .map(|column| (column.planner.column_id, column))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut reads = required_ids
        .into_iter()
        .filter(|column_id| !is_variant_synthetic_column(scan, *column_id))
        .map(|column_id| {
            let column = physical_by_id
                .get(&column_id)
                .ok_or_else(|| {
                    format!(
                        "scan binding node_id={node_id} required planner column id {column_id} has no exact provider ordinal"
                    )
                })?;
            Ok(ResolvedReadColumn {
                planner_column_id: Some(column.planner.column_id),
                source: column.source.clone(),
                reason: ResolvedReadReason::PlannerRequiredOrOutput,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    for name in equality_required {
        if reads
            .iter()
            .any(|read| read.source.name.eq_ignore_ascii_case(name))
        {
            continue;
        }
        let source = resolve_provider_source_by_name(node_id, scan, name)?;
        let matches = physical
            .iter()
            .filter(|column| column.source.name.eq_ignore_ascii_case(&source.name))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [column] => reads.push(ResolvedReadColumn {
                planner_column_id: Some(column.planner.column_id),
                source: source.clone(),
                reason: ResolvedReadReason::PlannerRequiredOrOutput,
            }),
            [] => reads.push(ResolvedReadColumn {
                planner_column_id: None,
                source: source.clone(),
                reason: ResolvedReadReason::EqualityDeleteKey,
            }),
            _ => {
                return Err(format!(
                    "scan binding node_id={node_id} equality-delete column '{name}' maps to more than one planner occurrence"
                ));
            }
        }
    }
    Ok(reads)
}

fn is_variant_synthetic_column(scan: &PlanScanNode, column_id: ColumnId) -> bool {
    scan.variant_columns
        .iter()
        .any(|variant| variant.synthetic_column_id == column_id)
}
