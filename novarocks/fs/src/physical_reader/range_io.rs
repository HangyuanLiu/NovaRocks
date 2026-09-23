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

use std::ops::Range;

use bytes::Bytes;

use super::chunk_reader::BoundChunkReader;
use crate::{FileError, FileErrorKind, FileReaderOptions, FileResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CoalescedRange {
    pub(crate) range: Range<u64>,
    pub(crate) requests: Vec<usize>,
}

pub(crate) fn coalesce_ranges(
    ranges: &[Range<u64>],
    file_size: u64,
    options: FileReaderOptions,
) -> FileResult<Vec<CoalescedRange>> {
    let mut order = (0..ranges.len()).collect::<Vec<_>>();
    order.sort_by_key(|index| (ranges[*index].start, ranges[*index].end));
    let mut groups: Vec<CoalescedRange> = Vec::new();
    for index in order {
        let range = &ranges[index];
        if range.start >= range.end || range.end > file_size {
            return Err(FileError::new(
                FileErrorKind::Corrupt,
                format!(
                    "Parquet input range [{}, {}) is invalid for file length {file_size}",
                    range.start, range.end
                ),
            ));
        }
        if options.coalesce_reads
            && let Some(last) = groups.last_mut()
            && range.start <= last.range.end.saturating_add(options.coalesce_max_gap)
            && (range.end.max(last.range.end) - last.range.start <= options.coalesce_max_bytes
                || (range.start >= last.range.start && range.end <= last.range.end))
        {
            last.range.end = last.range.end.max(range.end);
            last.requests.push(index);
            continue;
        }
        groups.push(CoalescedRange {
            range: range.clone(),
            requests: vec![index],
        });
    }
    Ok(groups)
}

pub(crate) fn read_decoder_ranges(
    reader: &BoundChunkReader,
    ranges: &[Range<u64>],
    options: FileReaderOptions,
) -> FileResult<Vec<Bytes>> {
    let groups = coalesce_ranges(ranges, reader.file_size(), options)?;
    let mut output = vec![None; ranges.len()];
    for group in groups {
        let length = usize::try_from(group.range.end - group.range.start).map_err(|_| {
            FileError::new(
                FileErrorKind::ResourceExhausted,
                "Parquet input range is too large",
            )
        })?;
        let exact = group.requests.len() == 1 && ranges[group.requests[0]] == group.range;
        let bytes = if exact {
            reader.read_bytes(group.range.start, length)?
        } else {
            reader.read_backing_bytes(group.range.start, length)?
        };
        for index in group.requests {
            let range = &ranges[index];
            let start = usize::try_from(range.start - group.range.start).map_err(|_| {
                FileError::new(
                    FileErrorKind::ResourceExhausted,
                    "Parquet input slice is too large",
                )
            })?;
            let end = usize::try_from(range.end - group.range.start).map_err(|_| {
                FileError::new(
                    FileErrorKind::ResourceExhausted,
                    "Parquet input slice is too large",
                )
            })?;
            output[index] = Some(bytes.slice(start..end));
        }
    }
    output
        .into_iter()
        .map(|bytes| {
            bytes.ok_or_else(|| {
                FileError::new(
                    FileErrorKind::Internal,
                    "Parquet input range was not filled",
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FileCancellation, FileIdentity, FileIoRuntime, FileReadContext, FileTaskSpawner,
        FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::StorageAccessDomainId;
    use std::sync::Arc;

    use super::super::chunk_reader::ReaderMetrics;

    #[test]
    fn merges_nearby_ranges_within_limit_and_preserves_membership() {
        let ranges = vec![40..50, 0..10, 12..20, 100..110];
        let groups = coalesce_ranges(
            &ranges,
            110,
            FileReaderOptions {
                coalesce_max_bytes: 32,
                coalesce_max_gap: 4,
                ..Default::default()
            },
        )
        .expect("coalesce valid ranges");
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].range, 0..20);
        assert_eq!(groups[0].requests, vec![1, 2]);
        assert_eq!(groups[1].range, 40..50);
        assert_eq!(groups[2].range, 100..110);
    }

    #[test]
    fn disabled_coalescing_keeps_exact_ranges() {
        let ranges = vec![0..10, 10..20];
        let groups = coalesce_ranges(
            &ranges,
            20,
            FileReaderOptions {
                coalesce_reads: false,
                ..Default::default()
            },
        )
        .expect("valid ranges");
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn contained_request_reuses_a_large_existing_backing() {
        let ranges = vec![0..32, 8..16, 0..32];
        let groups = coalesce_ranges(
            &ranges,
            32,
            FileReaderOptions {
                coalesce_max_bytes: 16,
                coalesce_max_gap: 0,
                ..Default::default()
            },
        )
        .expect("contained input ranges");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].range, 0..32);
        assert_eq!(groups[0].requests, vec![0, 2, 1]);
    }

    #[test]
    fn duplicate_overlap_and_out_of_order_requests_keep_exact_membership() {
        let ranges = vec![14..20, 0..10, 5..15, 0..10];
        let groups = coalesce_ranges(&ranges, 20, FileReaderOptions::default())
            .expect("valid overlapping ranges");
        assert_eq!(
            groups,
            vec![CoalescedRange {
                range: 0..20,
                requests: vec![1, 3, 2, 0],
            }]
        );
    }

    #[test]
    fn merged_backing_returns_exact_bytes_in_original_request_order() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("ranges.bin");
        std::fs::write(&path, b"abcdefghijklmnopqrst").expect("write fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([17; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("local access");
        let file = access
            .bind(0, FileIdentity::new(path.to_string_lossy(), 20, None))
            .expect("bound fixture");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test runtime");
        let handle = runtime.handle().clone();
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: None,
            runtime: Arc::new(TokioFileIoRuntime::new(handle.clone())) as Arc<dyn FileIoRuntime>,
            task_spawner: Arc::new(TokioFileTaskSpawner::new(handle)) as Arc<dyn FileTaskSpawner>,
        };
        let reader = BoundChunkReader::new(
            file,
            context,
            None,
            false,
            Arc::new(ReaderMetrics::default()),
        );
        let ranges = vec![14..20, 0..10, 5..15, 0..10];
        let bytes = read_decoder_ranges(&reader, &ranges, FileReaderOptions::default())
            .expect("read merged input");
        assert_eq!(
            bytes.iter().map(Bytes::as_ref).collect::<Vec<_>>(),
            vec![
                &b"opqrst"[..],
                &b"abcdefghij"[..],
                &b"fghijklmno"[..],
                &b"abcdefghij"[..]
            ]
        );
    }

    #[test]
    fn rejects_empty_inverted_and_out_of_file_ranges() {
        for range in [0..0, 10..9, 9..11, u64::MAX - 1..u64::MAX] {
            assert_eq!(
                coalesce_ranges(&[range], 10, FileReaderOptions::default())
                    .expect_err("invalid range")
                    .kind(),
                FileErrorKind::Corrupt
            );
        }
    }
}
