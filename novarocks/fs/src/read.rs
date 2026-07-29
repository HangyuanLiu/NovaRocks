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

use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;

use crate::{
    BoundFile, DataCacheContext, FileCancellation, FileIoRuntime, FileResult, FileTaskSpawner,
    PhysicalPruning, ScanPredicate,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileFormat {
    Parquet,
    Orc,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileReadRange {
    WholeFile,
    Bounded { offset: u64, length: u64 },
}

impl FileReadRange {
    pub fn bounded(offset: u64, length: u64) -> FileResult<Self> {
        if length == 0 {
            return Err(crate::FileError::invalid(
                "bounded file read range length must be greater than zero",
            ));
        }
        offset
            .checked_add(length)
            .ok_or_else(|| crate::FileError::invalid("bounded file read range overflows"))?;
        Ok(Self::Bounded { offset, length })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileProjection {
    All,
    RootNames(Vec<String>),
    RootIndices(Vec<usize>),
    FieldIds(Vec<i32>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileReadBudget {
    pub max_rows: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
}

#[derive(Clone)]
pub struct FileReadContext {
    pub cancellation: FileCancellation,
    pub deadline: Option<Instant>,
    pub runtime: Arc<dyn FileIoRuntime>,
    pub task_spawner: Arc<dyn FileTaskSpawner>,
}

impl FileReadContext {
    pub fn check_active(&self) -> FileResult<()> {
        self.cancellation.check()?;
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(crate::FileError::deadline(
                "file operation deadline exceeded",
            ));
        }
        Ok(())
    }
}

impl Debug for FileReadContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileReadContext")
            .field("cancellation", &self.cancellation)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct FileReadRequest {
    pub file: BoundFile,
    pub format: FileFormat,
    pub range: FileReadRange,
    pub projection: FileProjection,
    pub budget: FileReadBudget,
    pub predicates: Vec<ScanPredicate>,
    pub pruning: PhysicalPruning,
    pub cache: Option<DataCacheContext>,
    pub context: FileReadContext,
}

#[derive(Debug)]
pub struct FileBatch {
    pub batch: RecordBatch,
    pub physical_row_positions: Option<UInt64Array>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FileMetricsSnapshot {
    pub bytes_read: u64,
    pub read_requests: u64,
    pub rows_decoded: u64,
    pub batches_delivered: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub io_time_ns: u64,
    pub decode_time_ns: u64,
    pub row_groups_read: u64,
    pub row_groups_pruned: u64,
    pub delayed_materialization_ranges: u64,
}

pub trait FileBatchReader: Send {
    fn next_batch(&mut self) -> FileResult<Option<FileBatch>>;

    fn close(&mut self) -> FileResult<()>;

    fn metrics_snapshot(&self) -> FileMetricsSnapshot;
}
