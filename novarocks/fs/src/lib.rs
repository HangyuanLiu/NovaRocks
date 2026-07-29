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

//! Connector-neutral file access and columnar physical decoding.
//!
//! This crate deliberately has no connector identity and owns no table-format
//! correctness. Connector implementations bind authorized storage access and
//! ask this crate to read physical Parquet or ORC columns.

mod access;
mod error;
mod predicate;
mod read;
mod runtime;

pub use access::{
    BoundFile, FileIdentity, FsAccessHandle, FsAccessResolver, FsLocation, FsScheme,
    ObjectStoreConfig, ResolvedFsPath,
};
pub use error::{FileError, FileErrorKind, FileResult};
pub use predicate::{
    MinMaxPredicateOp, MinMaxPredicateValue, PhysicalPruning, ScanPredicate, ScanPredicateDomain,
    ScanPredicateSource,
};
pub use read::{
    FileBatch, FileBatchReader, FileFormat, FileMetricsSnapshot, FileProjection, FileReadBudget,
    FileReadContext, FileReadRange, FileReadRequest,
};
pub use runtime::{FileCancellation, FileIoRuntime, FileTask, FileTaskFuture, FileTaskSpawner};

// Design: ADR-0011 (docs/adr/ADR-0011-connector-neutral-file-foundation.md)
