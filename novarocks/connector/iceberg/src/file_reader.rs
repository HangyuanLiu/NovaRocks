// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.

//! Iceberg-owned use of connector-neutral physical file I/O.

use std::num::NonZeroUsize;

use bytes::Bytes;
use novarocks_fs::{
    FileBatch, FileFormat, FileIdentity, FileProjection, FileReadBudget, FileReadContext,
    FileReadRange, FileReadRequest, FsAccessHandle, PhysicalPruning, open_file_reader,
};

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
