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
//! ONE original SM3 stream, successful-empty rule and four-byte grouped lowercase text.
//! Evaluators own input/result projections; observation grants no formal memory.
use super::md5_shared::OwnedBytesArray;
use arrow_array::{ArrayRef, StringArray};
use sm3::{Digest, Sm3};
use std::{convert::Infallible, sync::Arc};
#[derive(Clone, Copy)]
pub enum Observation {
    Step,
    OpaqueBoundary,
}
/// Original SM3 empty input yields a successful empty string; nonempty yields 71 ASCII bytes.
pub fn output_width(text: &[u8]) -> usize {
    if text.is_empty() { 0 } else { 71 }
}
fn format_sm3_with_spaces(bytes: &[u8]) -> String {
    let mut out = String::new();
    for (idx, byte) in bytes.iter().enumerate() {
        if idx >= 4 && idx % 4 == 0 {
            out.push(' ');
        }
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

/// Original digest and renderer, with explicit observation and owned text.
pub fn render_digest<E>(
    text: &[u8],
    observe: &mut impl FnMut(Observation) -> Result<(), E>,
) -> Result<String, E> {
    if output_width(text) == 0 {
        return Ok(String::new());
    }
    observe(Observation::OpaqueBoundary)?;
    let mut digest = Sm3::new();
    observe(Observation::OpaqueBoundary)?;
    // Preserve the existing selected stream's original control timing. The
    // compression library sees exactly the same concatenated original bytes.
    for chunk in text.chunks(256) {
        observe(Observation::OpaqueBoundary)?;
        digest.update(chunk);
        observe(Observation::OpaqueBoundary)?;
        for _ in chunk {
            observe(Observation::Step)?;
        }
    }
    observe(Observation::OpaqueBoundary)?;
    let output = digest.finalize();
    observe(Observation::OpaqueBoundary)?;
    observe(Observation::OpaqueBoundary)?;
    let rendered = format_sm3_with_spaces(&output);
    observe(Observation::OpaqueBoundary)?;
    Ok(rendered)
}
/// Selected output projection preserves one step per original rendered byte.
pub fn visit_digest<E>(
    text: &[u8],
    observe: &mut impl FnMut(Observation) -> Result<(), E>,
    emit: &mut impl FnMut(u8),
) -> Result<(), E> {
    let rendered = render_digest(text, observe)?;
    for byte in rendered.as_bytes() {
        emit(*byte);
        observe(Observation::Step)?;
    }
    Ok(())
}

/// The raw v1 carrier projection keeps original source NULL, row indexing and Utf8 result.
pub fn evaluate_legacy(input: &OwnedBytesArray, rows: usize) -> ArrayRef {
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        if input.is_null(row) {
            out.push(None);
            continue;
        }
        let bytes = input.bytes(row);
        let text = render_digest(bytes, &mut |_| Ok::<(), Infallible>(()))
            .unwrap_or_else(|never| match never {});
        out.push(Some(text));
    }
    Arc::new(StringArray::from(out)) as ArrayRef
}
