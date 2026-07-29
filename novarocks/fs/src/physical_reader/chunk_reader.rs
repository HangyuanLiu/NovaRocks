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

use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use orc_rust::reader::ChunkReader as OrcChunkReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader as ParquetChunkReader, Length};

use crate::{
    BoundFile, DataCacheContext, DataCacheManager, DataCachePageKey, FileError,
    FileMetricsSnapshot, FileReadContext, FileReadRange, FileResult,
};

const STREAM_CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Default)]
pub(crate) struct ReaderMetrics {
    bytes_read: AtomicU64,
    read_requests: AtomicU64,
    rows_decoded: AtomicU64,
    batches_delivered: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    io_time_ns: AtomicU64,
    decode_time_ns: AtomicU64,
}

impl ReaderMetrics {
    pub(crate) fn snapshot(&self) -> FileMetricsSnapshot {
        FileMetricsSnapshot {
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            read_requests: self.read_requests.load(Ordering::Relaxed),
            rows_decoded: self.rows_decoded.load(Ordering::Relaxed),
            batches_delivered: self.batches_delivered.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            io_time_ns: self.io_time_ns.load(Ordering::Relaxed),
            decode_time_ns: self.decode_time_ns.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_decode(&self, rows: usize, elapsed_ns: u128) {
        self.rows_decoded.fetch_add(rows as u64, Ordering::Relaxed);
        self.decode_time_ns
            .fetch_add(clamp_u128(elapsed_ns), Ordering::Relaxed);
    }

    pub(crate) fn record_delivery(&self) {
        self.batches_delivered.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub(crate) struct BoundChunkReader {
    file: BoundFile,
    context: FileReadContext,
    cache: Option<DataCacheContext>,
    metrics: Arc<ReaderMetrics>,
}

impl BoundChunkReader {
    pub(crate) fn new(
        file: BoundFile,
        context: FileReadContext,
        cache: Option<DataCacheContext>,
        metrics: Arc<ReaderMetrics>,
    ) -> Self {
        Self {
            file,
            context,
            cache,
            metrics,
        }
    }

    fn read_bytes(&self, start: u64, length: usize) -> FileResult<Bytes> {
        self.context.check_active()?;
        let length_u64 = u64::try_from(length)
            .map_err(|_| FileError::invalid("file read length overflows u64"))?;
        let end = start
            .checked_add(length_u64)
            .ok_or_else(|| FileError::invalid("file read range overflows"))?;
        if end > self.file.identity().file_size() {
            return Err(FileError::new(
                crate::FileErrorKind::Corrupt,
                format!(
                    "file read range [{start}, {end}) exceeds file length {}",
                    self.file.identity().file_size()
                ),
            ));
        }

        let cache_key = self.cache_key(start, length);
        if let Some(key) = cache_key.as_ref()
            && let Some(cache) = DataCacheManager::instance().page_cache()
            && let Some(bytes) = cache.lookup_bytes(key)
        {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        if cache_key.is_some() {
            self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
        }

        let file = self.file.clone();
        let cancellation = self.context.cancellation.clone();
        let range = FileReadRange::Bounded {
            offset: start,
            length: length_u64,
        };
        let began = Instant::now();
        let bytes = self.context.runtime.block_on_bytes(Box::pin(async move {
            file.read(range, &cancellation).await
        }))?;
        self.context.check_active()?;
        self.metrics.read_requests.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes_read
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.metrics
            .io_time_ns
            .fetch_add(clamp_u128(began.elapsed().as_nanos()), Ordering::Relaxed);

        if let Some(key) = cache_key
            && let Some(cache) = DataCacheManager::instance().page_cache()
        {
            let _ = cache.insert_bytes(key, bytes.clone(), bytes.len(), Some(100));
        }
        Ok(bytes)
    }

    fn cache_key(&self, start: u64, length: usize) -> Option<DataCachePageKey> {
        let cache = self.cache.as_ref()?;
        if !cache.datacache_requested() {
            return None;
        }
        let identity = self.file.identity();
        let key = format!(
            "{}\0{}\0{}\0{}\0{}",
            identity.path(),
            identity.file_size(),
            identity.modification_time().unwrap_or_default(),
            start,
            length
        );
        Some(DataCachePageKey::new(
            "physical-file-range",
            key.into_bytes(),
        ))
    }
}

impl Length for BoundChunkReader {
    fn len(&self) -> u64 {
        self.file.identity().file_size()
    }
}

impl ParquetChunkReader for BoundChunkReader {
    type T = BoundRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.file.identity().file_size() {
            return Err(ParquetError::EOF(format!(
                "read offset {start} exceeds file length {}",
                self.file.identity().file_size()
            )));
        }
        Ok(BoundRead::new(self.clone(), start))
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        self.read_bytes(start, length).map_err(to_parquet_error)
    }
}

impl OrcChunkReader for BoundChunkReader {
    type T = BoundRead;

    fn len(&self) -> u64 {
        self.file.identity().file_size()
    }

    fn get_read(&self, start: u64) -> io::Result<Self::T> {
        if start > self.file.identity().file_size() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "ORC read offset exceeds file length",
            ));
        }
        Ok(BoundRead::new(self.clone(), start))
    }

    fn get_bytes(&self, start: u64, length: u64) -> io::Result<Bytes> {
        let length = usize::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "ORC range too large"))?;
        self.read_bytes(start, length).map_err(to_io_error)
    }
}

pub(crate) struct BoundRead {
    reader: BoundChunkReader,
    position: u64,
    buffer: Bytes,
    buffer_start: u64,
}

impl BoundRead {
    fn new(reader: BoundChunkReader, position: u64) -> Self {
        Self {
            reader,
            position,
            buffer: Bytes::new(),
            buffer_start: position,
        }
    }
}

impl Read for BoundRead {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.position >= self.reader.file.identity().file_size() {
            return Ok(0);
        }
        let buffer_end = self.buffer_start.saturating_add(self.buffer.len() as u64);
        if self.position < self.buffer_start || self.position >= buffer_end {
            let remaining = self
                .reader
                .file
                .identity()
                .file_size()
                .saturating_sub(self.position);
            let fetch = remaining.min(STREAM_CHUNK_SIZE as u64) as usize;
            self.buffer = self
                .reader
                .read_bytes(self.position, fetch)
                .map_err(to_io_error)?;
            self.buffer_start = self.position;
        }
        let offset = (self.position - self.buffer_start) as usize;
        let available = &self.buffer[offset..];
        let count = available.len().min(output.len());
        output[..count].copy_from_slice(&available[..count]);
        self.position += count as u64;
        Ok(count)
    }
}

fn to_parquet_error(error: FileError) -> ParquetError {
    match error.kind() {
        crate::FileErrorKind::Corrupt | crate::FileErrorKind::NotFound => {
            ParquetError::EOF(error.to_string())
        }
        _ => ParquetError::General(error.to_string()),
    }
}

fn to_io_error(error: FileError) -> io::Error {
    let kind = match error.kind() {
        crate::FileErrorKind::NotFound => io::ErrorKind::NotFound,
        crate::FileErrorKind::Permission => io::ErrorKind::PermissionDenied,
        crate::FileErrorKind::Corrupt => io::ErrorKind::UnexpectedEof,
        // `Read::read_exact` retries `Interrupted` forever. Cancellation is
        // terminal for this reader and must propagate through format decoders.
        crate::FileErrorKind::Cancelled => io::ErrorKind::Other,
        crate::FileErrorKind::DeadlineExceeded => io::ErrorKind::TimedOut,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, error)
}

fn clamp_u128(value: u128) -> u64 {
    value.min(u64::MAX as u128) as u64
}
