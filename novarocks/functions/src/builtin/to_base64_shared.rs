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
//! ONE original Latin1 preference/fallback and STANDARD encoder author.
//! The caller projects its actual source fact before evaluation; math has no AST/name dispatch.
use super::md5_shared::{Observation, OwnedBytesArray};
use base64::Engine;
use novarocks_type_contract::ToBase64ByteSource;
use std::convert::Infallible;

/// Original compatibility API; the observed entrypoint owns the only loop.
pub fn latin1_string_to_bytes(s: &str) -> Option<Vec<u8>> {
    match latin1_string_to_bytes_observed(s, &mut |_| Ok::<(), Infallible>(())) {
        Ok(value) => value,
        Err(never) => match never {},
    }
}

/// Preserve each original character, early >255 exit, capacity and byte append.
/// A zero-capacity Vec does not invoke an opaque allocation boundary.
pub fn latin1_string_to_bytes_observed<E>(
    s: &str,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Option<Vec<u8>>, E> {
    if !s.is_empty() {
        observe(Observation::OpaqueBoundary)?;
    }
    let mut out = Vec::with_capacity(s.len());
    if !s.is_empty() {
        observe(Observation::OpaqueBoundary)?;
    }
    for ch in s.chars() {
        observe(Observation::Step)?;
        if (ch as u32) > 0xff {
            return Ok(None);
        }
        out.push(ch as u8);
    }
    Ok(Some(out))
}

pub fn encode_base64(input: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(input)
}

/// Preserve the original NULL/empty and >255 UTF8 branch, including raw Binary carriers.
pub fn encode_row(
    input: &OwnedBytesArray,
    row: usize,
    source: ToBase64ByteSource,
) -> Option<String> {
    match encode_row_observed(input, row, source, &mut |_| Ok::<(), Infallible>(())) {
        Ok(value) => value,
        Err(never) => match never {},
    }
}

/// ONE original row computation, with observation around actual library work.
pub fn encode_row_observed<E>(
    input: &OwnedBytesArray,
    row: usize,
    source: ToBase64ByteSource,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Option<String>, E> {
    if input.is_null(row) {
        return Ok(None);
    }
    let fallback;
    let bytes = if source.prefers_latin1() {
        if let Some(s) = input.utf8(row) {
            fallback = latin1_string_to_bytes_observed(s, observe)?;
            fallback.as_deref().unwrap_or_else(|| input.bytes(row))
        } else {
            input.bytes(row)
        }
    } else {
        input.bytes(row)
    };
    if bytes.is_empty() {
        return Ok(None);
    }
    observe(Observation::OpaqueBoundary)?;
    let encoded = encode_base64(bytes);
    observe(Observation::OpaqueBoundary)?;
    Ok(Some(encoded))
}
