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
//! ONE original arena-free AES row computation, with explicit source semantics.
use super::{
    aes_primitive::{self, AesMode},
    md5_shared::{Observation, OwnedBytesArray},
    to_base64_shared::latin1_string_to_bytes_observed,
};
use novarocks_type_contract::ToBase64ByteSource;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Encrypt,
    Decrypt,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataError {
    Arity,
    AadRequiresGcm,
}
pub enum Row {
    Value(Option<Vec<u8>>),
    Data(DataError),
}
fn decrypt_with_utf8_fallback<E>(
    mode: AesMode,
    src: &OwnedBytesArray,
    row: usize,
    key: &[u8],
    iv: Option<&[u8]>,
    aad: Option<&[u8]>,
    source: ToBase64ByteSource,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Option<Vec<u8>>, E> {
    let utf8_bytes = src.bytes(row);
    let latin1 = match src.utf8(row) {
        Some(text) => latin1_string_to_bytes_observed(text, observe)?,
        None => None,
    };
    let primary = if source.prefers_latin1() {
        latin1.as_deref().unwrap_or(utf8_bytes)
    } else {
        utf8_bytes
    };
    let mut result = aes_primitive::aes_decrypt_raw_observed(mode, primary, key, iv, aad, observe)?;
    if result.is_some() {
        return Ok(result);
    }
    let fallback = if source.prefers_latin1() {
        Some(utf8_bytes)
    } else {
        latin1.as_deref()
    };
    if let Some(fallback) = fallback
        && fallback != primary
    {
        result = aes_primitive::aes_decrypt_raw_observed(mode, fallback, key, iv, aad, observe)?;
    }
    Ok(result)
}
/// Addresses are supplied by the caller's exact evaluated-carrier projection.
/// Eager child evaluation and normalization remain outside the pure row body.
pub fn evaluate_row_observed<E>(
    operation: Operation,
    inputs: &[OwnedBytesArray],
    rows: &[usize],
    source: ToBase64ByteSource,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Row, E> {
    observe(Observation::Step)?;
    if !matches!(inputs.len(), 2 | 4 | 5) {
        return Ok(Row::Data(DataError::Arity));
    }
    let src = &inputs[0];
    let key = &inputs[1];
    let src_row = rows[0];
    let key_row = rows[1];
    if src.is_null(src_row) || key.is_null(key_row) {
        return Ok(Row::Value(None));
    }
    let src_bytes = src.bytes(src_row);
    let key_bytes = key.bytes(key_row);
    if operation == Operation::Decrypt && (src_bytes.is_empty() || key_bytes.is_empty()) {
        return Ok(Row::Value(None));
    }
    if inputs.len() == 2 {
        return Ok(Row::Value(match operation {
            Operation::Encrypt => aes_primitive::aes_encrypt_raw_observed(
                AesMode::Aes128Ecb,
                src_bytes,
                key_bytes,
                None,
                None,
                observe,
            )?,
            Operation::Decrypt => decrypt_with_utf8_fallback(
                AesMode::Aes128Ecb,
                src,
                src_row,
                key_bytes,
                None,
                None,
                source,
                observe,
            )?,
        }));
    }
    let mode_arr = &inputs[3];
    let mode_row = rows[3];
    if mode_arr.is_null(mode_row) {
        return Ok(Row::Value(None));
    }
    let mode = AesMode::parse_observed(mode_arr.bytes(mode_row), observe)?;
    let iv_arr = &inputs[2];
    let iv_row = rows[2];
    if !mode.is_ecb() && iv_arr.is_null(iv_row) {
        return Ok(Row::Value(None));
    }
    let iv_bytes = if iv_arr.is_null(iv_row) {
        None
    } else {
        Some(iv_arr.bytes(iv_row))
    };
    // then_some intentionally preserves the original eager hidden payload read.
    let aad_bytes = if inputs.len() == 5 {
        let arr = &inputs[4];
        let row = rows[4];
        (!arr.is_null(row)).then_some(arr.bytes(row))
    } else {
        None
    };
    if aad_bytes.is_some() && !mode.is_gcm() {
        return Ok(Row::Data(DataError::AadRequiresGcm));
    }
    Ok(Row::Value(match operation {
        Operation::Encrypt => aes_primitive::aes_encrypt_raw_observed(
            mode, src_bytes, key_bytes, iv_bytes, aad_bytes, observe,
        )?,
        Operation::Decrypt => decrypt_with_utf8_fallback(
            mode, src, src_row, key_bytes, iv_bytes, aad_bytes, source, observe,
        )?,
    }))
}

/// Original v1 no-control compatibility wrapper over the same row body.
pub fn evaluate_row(
    operation: Operation,
    inputs: &[OwnedBytesArray],
    rows: &[usize],
    source: ToBase64ByteSource,
) -> Row {
    match evaluate_row_observed(operation, inputs, rows, source, &mut |_| {
        Ok::<(), std::convert::Infallible>(())
    }) {
        Ok(value) => value,
        Err(never) => match never {},
    }
}

#[cfg(test)]
#[path = "aes_rows_tests.rs"]
mod tests;
