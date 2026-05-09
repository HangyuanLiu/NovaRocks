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
//! Range read helper for native segment bytes.
//!
//! This wrapper centralizes OpenDAL range-read error mapping used by both
//! metadata and segment/page loading paths.
//!
//! Current limitations:
//! - Reads are synchronous from caller perspective (`Runtime::block_on`).

use opendal::{ErrorKind, Operator};

use crate::formats::starrocks::range_read::{ensure_exact_range_read_len, expected_range_len};

/// Read `[start, end)` bytes from object storage.
pub(crate) fn read_range_bytes(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    path: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, String> {
    if end <= start {
        return Err(format!(
            "invalid read range for native data loader: path={}, start={}, end={}",
            path, start, end
        ));
    }
    let expected_len = expected_range_len(path, start, end)?;
    const MAX_READ_ATTEMPTS: usize = 4;
    for attempt in 1..=MAX_READ_ATTEMPTS {
        match rt.block_on(op.read_with(path).range(start..end).into_future()) {
            Ok(v) => {
                let bytes = v.to_vec();
                match ensure_exact_range_read_len(path, start, end, bytes.len()) {
                    Ok(()) => return Ok(bytes),
                    Err(err) if attempt < MAX_READ_ATTEMPTS => {
                        let backoff_ms =
                            (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                        std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                        continue;
                    }
                    Err(err) => {
                        return Err(format!(
                            "read segment file range failed in native data loader: {err}, expected_bytes={expected_len}"
                        ));
                    }
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(format!("segment file not found: {}", path));
            }
            Err(e) if e.is_temporary() && attempt < MAX_READ_ATTEMPTS => {
                let backoff_ms = (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
            }
            Err(e) => {
                return Err(format!(
                    "read segment file range failed in native data loader: path={}, range={}..{}, error={}",
                    path, start, end, e
                ));
            }
        }
    }
    Err(format!(
        "read segment file range failed in native data loader after retries: path={}, range={}..{}",
        path, start, end
    ))
}

pub(crate) fn read_segment_bytes(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    path: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, String> {
    match read_range_bytes(rt, op, path, start, end) {
        Ok(bytes) => Ok(bytes),
        Err(range_err) if start == 0 && is_short_range_read_error(&range_err) => {
            read_all_segment_bytes(rt, op, path)
        }
        Err(range_err) => Err(range_err),
    }
}

fn is_short_range_read_error(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    lowered.contains("too little data") || lowered.contains("unexpected length")
}

fn read_all_segment_bytes(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    path: &str,
) -> Result<Vec<u8>, String> {
    const MAX_READ_ATTEMPTS: usize = 4;
    for attempt in 1..=MAX_READ_ATTEMPTS {
        match rt.block_on(op.read(path)) {
            Ok(v) => return Ok(v.to_vec()),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(format!("segment file not found: {}", path));
            }
            Err(e) if e.is_temporary() && attempt < MAX_READ_ATTEMPTS => {
                let backoff_ms = (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
            }
            Err(e) => {
                return Err(format!(
                    "read segment file failed in native data loader: path={}, error={}",
                    path, e
                ));
            }
        }
    }
    Err(format!(
        "read segment file failed in native data loader after retries: path={}",
        path
    ))
}

#[cfg(test)]
mod tests {
    use super::read_segment_bytes;
    use opendal::Operator;
    use tempfile::TempDir;

    fn local_operator(root: &str) -> Operator {
        let builder = opendal::services::Fs::default().root(root);
        Operator::new(builder)
            .expect("create local operator")
            .finish()
    }

    #[test]
    fn read_segment_bytes_falls_back_to_whole_file_for_offset_zero_short_range() {
        let temp_dir = TempDir::new().expect("create temp dir");
        std::fs::create_dir_all(temp_dir.path().join("data")).expect("create data dir");
        std::fs::write(temp_dir.path().join("data/standalone.dat"), [1_u8, 2, 3])
            .expect("write segment file");
        let op = local_operator(temp_dir.path().to_str().expect("temp path to str"));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");

        let bytes = read_segment_bytes(&rt, &op, "data/standalone.dat", 0, 5)
            .expect("offset-zero standalone segment should fall back to whole file");

        assert_eq!(bytes, vec![1, 2, 3]);
    }
}
