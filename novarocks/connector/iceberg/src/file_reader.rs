// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.

//! Iceberg-owned use of connector-neutral physical file I/O.

use std::num::NonZeroUsize;

use bytes::Bytes;
use novarocks_fs::{
    FileBatch, FileFormat, FileIdentity, FileProjection, FileReadBudget, FileReadContext,
    FileReadRange, FileReadRequest, FsAccessHandle, MinMaxPredicateOp, MinMaxPredicateValue,
    PhysicalPruning, ScanPredicate, ScanPredicateDomain, ScanPredicateSource, open_file_reader,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::scan_model::{
    IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateOp,
    IcebergPhysicalPredicateValue,
};

/// Lower provider-owned Iceberg predicates into connector-neutral physical
/// file predicates.  Field IDs remain authoritative across Iceberg renames.
pub fn physical_predicates_to_file_predicates(
    predicates: &[IcebergPhysicalPredicate],
) -> Vec<ScanPredicate> {
    predicates
        .iter()
        .filter_map(|predicate| {
            let value = |value: &IcebergPhysicalPredicateValue| match value {
                IcebergPhysicalPredicateValue::Boolean(value) => {
                    MinMaxPredicateValue::Boolean(*value)
                }
                IcebergPhysicalPredicateValue::Int32(value) => MinMaxPredicateValue::Int32(*value),
                IcebergPhysicalPredicateValue::Int64(value) => MinMaxPredicateValue::Int64(*value),
                // Parquet exposes DATE statistics as INT32 day counts.
                IcebergPhysicalPredicateValue::Date32(value) => MinMaxPredicateValue::Int32(*value),
            };
            let domain = match &predicate.domain {
                IcebergPhysicalPredicateDomain::Range { op, value: literal } => {
                    ScanPredicateDomain::Range {
                        op: match op {
                            IcebergPhysicalPredicateOp::Eq => MinMaxPredicateOp::Eq,
                            IcebergPhysicalPredicateOp::Lt => MinMaxPredicateOp::Lt,
                            IcebergPhysicalPredicateOp::Le => MinMaxPredicateOp::Le,
                            IcebergPhysicalPredicateOp::Gt => MinMaxPredicateOp::Gt,
                            IcebergPhysicalPredicateOp::Ge => MinMaxPredicateOp::Ge,
                        },
                        value: value(literal),
                    }
                }
                IcebergPhysicalPredicateDomain::DiscreteSet { values } => {
                    let values = values.iter().map(value).collect::<Vec<_>>();
                    if values.is_empty() {
                        return None;
                    }
                    let min = values.first()?.clone();
                    let max = values.last()?.clone();
                    ScanPredicateDomain::DiscreteSet { values, min, max }
                }
            };
            Some(
                ScanPredicate::new(
                    predicate.column.clone(),
                    domain,
                    ScanPredicateSource::Static,
                )
                .with_physical_field_id(predicate.field_id),
            )
        })
        .collect()
}

/// Resolve the physical decoder from an Iceberg data file path.
pub fn iceberg_data_file_format(path: &str) -> Result<FileFormat, ConnectorError> {
    let path = path.split('?').next().unwrap_or(path);
    if path.to_ascii_lowercase().ends_with(".orc") {
        return Ok(FileFormat::Orc);
    }
    if path.to_ascii_lowercase().ends_with(".parquet")
        || path.to_ascii_lowercase().ends_with(".parq")
    {
        return Ok(FileFormat::Parquet);
    }
    Err(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        format!("Iceberg data file format is not declared or supported: {path}"),
    ))
}

pub fn read_parquet_batches(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    projection: FileProjection,
    context: FileReadContext,
) -> Result<Vec<FileBatch>, String> {
    context.check_active().map_err(|error| error.to_string())?;
    let provisional = access
        .bind_location(
            path,
            FileIdentity::new(path, file_size.unwrap_or_default(), None),
        )
        .map_err(|error| error.to_string())?;
    let resolved_size = match file_size {
        Some(size) if size > 0 => size,
        _ => {
            let file = provisional.clone();
            let cancellation = context.cancellation.clone();
            context
                .runtime
                .block_on_u64(Box::pin(async move { file.stat(&cancellation).await }))
                .map_err(|error| error.to_string())?
        }
    };
    context.check_active().map_err(|error| error.to_string())?;
    let file = access
        .bind_location(path, FileIdentity::new(path, resolved_size, None))
        .map_err(|error| error.to_string())?;
    let mut reader = open_file_reader(FileReadRequest {
        file,
        format: FileFormat::Parquet,
        range: FileReadRange::WholeFile,
        projection,
        budget: FileReadBudget {
            max_rows: NonZeroUsize::new(4096).expect("constant is nonzero"),
            max_bytes: NonZeroUsize::new(64 * 1024 * 1024).expect("constant is nonzero"),
        },
        predicates: Vec::new(),
        pruning: PhysicalPruning::default(),
        cache: None,
        context,
    })
    .map_err(|error| error.to_string())?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch().map_err(|error| error.to_string())? {
        batches.push(batch);
    }
    reader.close().map_err(|error| error.to_string())?;
    Ok(batches)
}

pub fn read_bytes(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    range: FileReadRange,
    context: &FileReadContext,
) -> Result<Bytes, String> {
    context.check_active().map_err(|error| error.to_string())?;
    let file = access
        .bind_location(
            path,
            FileIdentity::new(path, file_size.unwrap_or_default(), None),
        )
        .map_err(|error| error.to_string())?;
    let cancellation = context.cancellation.clone();
    context
        .runtime
        .block_on_bytes(Box::pin(
            async move { file.read(range, &cancellation).await },
        ))
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowers_static_predicates_by_iceberg_field_id() {
        let predicates = physical_predicates_to_file_predicates(&[
            IcebergPhysicalPredicate {
                field_id: 7,
                column: "renamed".to_string(),
                domain: IcebergPhysicalPredicateDomain::Range {
                    op: IcebergPhysicalPredicateOp::Ge,
                    value: IcebergPhysicalPredicateValue::Date32(20_000),
                },
            },
            IcebergPhysicalPredicate {
                field_id: 8,
                column: "empty".to_string(),
                domain: IcebergPhysicalPredicateDomain::DiscreteSet { values: Vec::new() },
            },
        ]);

        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].column(), "renamed");
        assert_eq!(predicates[0].physical_field_id(), Some(7));
        assert_eq!(predicates[0].source(), ScanPredicateSource::Static);
        assert_eq!(
            predicates[0].domain(),
            &ScanPredicateDomain::Range {
                op: MinMaxPredicateOp::Ge,
                value: MinMaxPredicateValue::Int32(20_000),
            }
        );
    }

    #[test]
    fn resolves_iceberg_physical_file_format_without_query_suffix() {
        assert_eq!(
            iceberg_data_file_format("s3://warehouse/part-0.parquet?version=1").expect("parquet"),
            FileFormat::Parquet
        );
        assert_eq!(
            iceberg_data_file_format("file:///warehouse/part-1.orc").expect("orc"),
            FileFormat::Orc
        );
        assert_eq!(
            iceberg_data_file_format("file:///warehouse/part-2.avro")
                .expect_err("unsupported")
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }
}
