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

//! Allocation-free projection into an admitted, zero-initialized IPC body.
//! Padding belongs to the caller's initialized output; this emitter writes only
//! descriptor payloads. It neither reconstructs ArrayData nor owns a grant.

use super::geometry::{Geometry, add, aligned, bit_bytes, mul, native_offset};
use crate::{
    ipc_flat_batch_v2::{Layout, layout},
    physical_type_v2::TypeCodecError,
};
use arrow::array::ArrayData;
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::CompileCheckpoints;

fn invalid() -> TypeCodecError {
    TypeCodecError::InvalidShape("flat pool body differs from its checked geometry")
}

struct Destination<'a> {
    output: &'a mut [u8],
    cursor: usize,
    buffers: usize,
    payload: usize,
}

impl Destination<'_> {
    fn segment(&mut self, bytes: usize) -> Result<&mut [u8], TypeCodecError> {
        let start = self.cursor;
        let end = add(start, bytes)?;
        let padded_end = add(start, aligned(bytes)?)?;
        if padded_end > self.output.len() {
            return Err(invalid());
        }
        self.cursor = padded_end;
        self.buffers = add(self.buffers, 1)?;
        self.payload = add(self.payload, bytes)?;
        self.output.get_mut(start..end).ok_or_else(invalid)
    }
}

pub(crate) fn copy(
    source: &[u8],
    target: &mut [u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if source.len() != target.len() {
        return Err(invalid());
    }
    work.step()?; // The descriptor's borrowed ranges have been checked.
    for (source, target) in source.chunks(1024).zip(target.chunks_mut(1024)) {
        target.copy_from_slice(source);
        work.step()?;
    }
    Ok(())
}

pub(crate) fn bitmap(
    source: &[u8],
    offset: usize,
    rows: usize,
    target: &mut [u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    let end = add(offset, rows)?;
    if bit_bytes(end)? > source.len() || target.len() != bit_bytes(rows)? {
        return Err(invalid());
    }
    if offset.is_multiple_of(8) {
        // Arrow preserves the original final byte, including unused tail bits,
        // when its bit slice starts at a byte boundary.
        let start = offset / 8;
        return copy(
            source
                .get(start..add(start, target.len())?)
                .ok_or_else(invalid)?,
            target,
            work,
        );
    }
    work.step()?;
    let mut row = 0;
    for byte in target {
        let mut packed = 0_u8;
        let bits = (rows - row).min(8);
        for bit in 0..bits {
            let at = add(offset, add(row, bit)?)?;
            let value = *source.get(at / 8).ok_or_else(invalid)?;
            packed |= ((value >> (at % 8)) & 1) << bit;
        }
        *byte = packed;
        row = add(row, bits)?;
        work.step()?; // At most eight source bits and one output byte.
    }
    Ok(())
}

fn selected_bytes(source: &[u8], start: usize, bytes: usize) -> Result<&[u8], TypeCodecError> {
    source.get(start..add(start, bytes)?).ok_or_else(invalid)
}

pub(super) fn emit(
    pool: &ConstantPool,
    geometry: &Geometry,
    output: &mut [u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    emit_span(pool.data(), 0, geometry, output, work)
}

pub(crate) fn emit_span(
    data: &ArrayData,
    start: usize,
    geometry: &Geometry,
    output: &mut [u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if add(start, geometry.rows)? > data.len() {
        return Err(invalid());
    }
    let offset = add(data.offset(), start)?;
    let kind = layout(data.data_type())?;
    if output.len() != geometry.body_bytes {
        return Err(invalid());
    }
    let variadic = if matches!(kind, Layout::Views) {
        data.buffers().len().checked_sub(1).ok_or_else(invalid)?
    } else {
        0
    };
    if variadic != geometry.variadic {
        return Err(invalid());
    }
    work.step()?;
    let mut destination = Destination {
        output,
        cursor: 0,
        buffers: 0,
        payload: 0,
    };
    if !matches!(kind, Layout::Null) {
        let validity = destination.segment(bit_bytes(geometry.rows)?)?;
        emit_validity_span(data, start, geometry.rows, validity, work)?;
    }
    match kind {
        Layout::Null => (),
        Layout::Bits => {
            if geometry.values_bytes != bit_bytes(geometry.rows)? {
                return Err(invalid());
            }
            bitmap(
                data.buffers().first().ok_or_else(invalid)?.as_slice(),
                offset,
                geometry.rows,
                destination.segment(geometry.values_bytes)?,
                work,
            )?;
        }
        Layout::Fixed(_) | Layout::Views => {
            let width = match kind {
                Layout::Fixed(width) => width,
                Layout::Views => 16,
                _ => return Err(invalid()),
            };
            if geometry.values_start != mul(offset, width)?
                || geometry.values_bytes != mul(geometry.rows, width)?
            {
                return Err(invalid());
            }
            copy(
                selected_bytes(
                    data.buffers().first().ok_or_else(invalid)?.as_slice(),
                    geometry.values_start,
                    geometry.values_bytes,
                )?,
                destination.segment(geometry.values_bytes)?,
                work,
            )?;
            if matches!(kind, Layout::Views) {
                for buffer in data.buffers().iter().skip(1) {
                    copy(buffer.as_slice(), destination.segment(buffer.len())?, work)?;
                }
            }
        }
        Layout::Offsets(width) => {
            let values = data.buffers().get(1).ok_or_else(invalid)?.as_slice();
            if geometry.values_start != geometry.offset_base {
                return Err(invalid());
            }
            let offsets = destination.segment(mul(add(geometry.rows, 1)?, width)?)?;
            emit_offsets_span(
                data,
                start,
                geometry.rows,
                width,
                geometry.offset_base,
                geometry.values_bytes,
                offsets,
                work,
            )?;
            copy(
                selected_bytes(values, geometry.values_start, geometry.values_bytes)?,
                destination.segment(geometry.values_bytes)?,
                work,
            )?;
        }
    }
    if destination.cursor != geometry.body_bytes
        || destination.buffers != geometry.buffers
        || destination.payload != geometry.payload_bytes
    {
        return Err(invalid());
    }
    work.step()?;
    // The parent retains this same meter and performs its success/error tail.
    Ok(())
}

/// Projects the independently sliced validity owner, including hidden payload rows.
pub(crate) fn emit_validity_span(
    data: &ArrayData,
    start: usize,
    rows: usize,
    validity: &mut [u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if add(start, rows)? > data.len() || validity.len() != bit_bytes(rows)? {
        return Err(invalid());
    }
    if let Some(nulls) = data.nulls() {
        if add(start, rows)? > nulls.len() {
            return Err(invalid());
        }
        bitmap(
            nulls.inner().values(),
            add(nulls.offset(), start)?,
            rows,
            validity,
            work,
        )?;
    } else {
        work.step()?;
        for chunk in validity.chunks_mut(1024) {
            chunk.fill(0xff);
            work.step()?;
        }
    }
    Ok(())
}

/// Shared offset rebase for bytes and recursive List/Map child spans.
pub(crate) fn emit_offsets_span(
    data: &ArrayData,
    start: usize,
    rows: usize,
    width: usize,
    base: usize,
    extent: usize,
    offsets: &mut [u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if add(start, rows)? > data.len() || offsets.len() != mul(add(rows, 1)?, width)? {
        return Err(invalid());
    }
    let source = data.buffers().first().ok_or_else(invalid)?.as_slice();
    let offset = add(data.offset(), start)?;
    for (row, target) in offsets.chunks_exact_mut(width).enumerate() {
        let rebased = if rows == 0 {
            if base != 0 || extent != 0 {
                return Err(invalid());
            }
            0
        } else {
            native_offset(source, add(offset, row)?, width)?
                .checked_sub(base)
                .ok_or_else(invalid)?
        };
        if (row == 0 && rebased != 0) || (row == rows && rebased != extent) {
            return Err(invalid());
        }
        match width {
            4 => target
                .copy_from_slice(&i32::try_from(rebased).map_err(|_| invalid())?.to_le_bytes()),
            8 => target
                .copy_from_slice(&i64::try_from(rebased).map_err(|_| invalid())?.to_le_bytes()),
            _ => return Err(invalid()),
        }
        work.step()?;
    }
    Ok(())
}
