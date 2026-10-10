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

//! Direct recursive body projection into the final admitted stream backing.
use super::{
    geometry::{Geometry, Span, invalid, walk},
    header::descriptor,
};
use crate::{
    ipc_flat_pool_v2::{
        body::{emit_offsets_span, emit_span, emit_validity_span},
        geometry::{add, aligned},
    },
    physical_type_v2::TypeCodecError,
};
use arrow::array::ArrayData;
use novarocks_type_contract::CompileCheckpoints;

pub(super) fn emit(
    data: &ArrayData,
    geometry: &Geometry,
    output: &mut [u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if output.len() != geometry.body_bytes {
        return Err(invalid());
    }
    let mut cursor = 0;
    let mut buffers = 0;
    let mut payload = 0;
    walk(
        Span {
            data,
            start: 0,
            len: geometry.rows,
        },
        false,
        false,
        1,
        work,
        &mut |node, work| {
            let end = add(cursor, node.body_bytes)?;
            let target = output.get_mut(cursor..end).ok_or_else(invalid)?;
            if let Some(leaf) = &node.leaf {
                emit_span(node.span.data, node.span.start, leaf, target, work)?;
            } else {
                let validity_bytes = descriptor(node, 0)?;
                emit_validity_span(
                    node.span.data,
                    node.span.start,
                    node.span.len,
                    target.get_mut(..validity_bytes).ok_or_else(invalid)?,
                    work,
                )?;
                if let Some((width, base, extent)) = node.offset {
                    let start = aligned(validity_bytes)?;
                    let bytes = descriptor(node, 1)?;
                    emit_offsets_span(
                        node.span.data,
                        node.span.start,
                        node.span.len,
                        width,
                        base,
                        extent,
                        target
                            .get_mut(start..add(start, bytes)?)
                            .ok_or_else(invalid)?,
                        work,
                    )?;
                }
            }
            cursor = end;
            buffers = add(buffers, node.buffers)?;
            payload = add(payload, node.payload_bytes)?;
            work.step()?;
            Ok(())
        },
    )?;
    if cursor != geometry.body_bytes
        || buffers != geometry.buffers
        || payload != geometry.payload_bytes
    {
        return Err(invalid());
    }
    work.step()?;
    Ok(())
}
