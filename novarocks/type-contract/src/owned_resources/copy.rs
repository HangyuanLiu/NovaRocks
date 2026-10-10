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

//! Shared spelling-copy and allocation-exit operations on a caller-owned meter.
//! Admission and entry/ordinary/success scope tails remain with the caller.

use crate::{CompileCheckpoints, CompileControlError};
use std::collections::TryReserveError;

/// A captured allocation refusal is already the originating resource cause.
/// Observe the opaque exit only after success; a later cancellation must not
/// replace an actual resource failure. Request admission belongs to the caller.
pub fn reserve_exit<E: From<CompileControlError>>(
    result: Result<(), TryReserveError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    result.map_err(|_| E::from(CompileControlError::ResourceExhausted))?;
    work.flush().map_err(E::from)
}

/// Copy actual spelling without a clone followed by estimated work. Source
/// segments are at most 256 bytes with UTF-8 boundaries. Each actual append
/// copies one character (at most four bytes), followed by its completed step.
/// A step is a bounded character operation, not a promise of one byte/step.
/// The caller admits source, reserve, copy work and returned backing first.
pub fn copy_string<E: From<CompileControlError>>(
    input: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, E> {
    let mut output = String::new();
    work.flush().map_err(E::from)?;
    let result = output.try_reserve_exact(input.len());
    if result.is_ok() {
        work.step().map_err(E::from)?;
    }
    reserve_exit::<E>(result, work)?;
    let mut start = 0;
    while start < input.len() {
        let mut end = start.saturating_add(256).min(input.len());
        // UTF-8 requires at most three boundary adjustments.
        while !input.is_char_boundary(end) {
            end -= 1;
            work.step().map_err(E::from)?;
        }
        for character in input[start..end].chars() {
            output.push(character);
            work.step().map_err(E::from)?;
        }
        start = end;
    }
    Ok(output)
}

#[cfg(test)]
#[path = "copy/tests.rs"]
mod tests;
