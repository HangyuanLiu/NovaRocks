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

pub(crate) fn ensure_exact_range_read_len(
    path: &str,
    start: u64,
    end: u64,
    actual_len: usize,
) -> Result<(), String> {
    let expected_len = expected_range_len(path, start, end)?;
    if actual_len == expected_len {
        return Ok(());
    }
    Err(format!(
        "range read returned unexpected length: path={path}, range={start}..{end}, expected={expected_len}, actual={actual_len}"
    ))
}

pub(crate) fn expected_range_len(path: &str, start: u64, end: u64) -> Result<usize, String> {
    let len = end.checked_sub(start).ok_or_else(|| {
        format!("invalid read range for segment file: path={path}, start={start}, end={end}")
    })?;
    usize::try_from(len).map_err(|_| {
        format!("read range length overflows usize: path={path}, start={start}, end={end}")
    })
}

#[cfg(test)]
mod tests {
    use super::ensure_exact_range_read_len;

    #[test]
    fn ensure_exact_range_read_len_rejects_short_successful_read() {
        let err = ensure_exact_range_read_len("data/seg.dat", 4, 12, 7)
            .expect_err("short read must be rejected");
        assert!(err.contains("path=data/seg.dat"), "err={err}");
        assert!(err.contains("range=4..12"), "err={err}");
        assert!(err.contains("expected=8"), "err={err}");
        assert!(err.contains("actual=7"), "err={err}");
    }
}
