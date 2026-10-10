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

//! Standard flat batch metadata, using official builders and controlled pushes.

use super::geometry::{Geometry, add, aligned, bit_bytes, mul};
use crate::{
    ipc_flat_batch_v2::{Layout, layout},
    physical_type_v2::TypeCodecError,
};
use arrow::array::ArrayData;
use arrow::ipc;
use flatbuffers::FlatBufferBuilder;
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::CompileCheckpoints;

fn invalid() -> TypeCodecError {
    TypeCodecError::InvalidShape("flat pool batch metadata differs from checked geometry")
}
pub(super) fn backing(geometry: &Geometry) -> Result<usize, TypeCodecError> {
    backing_counts(1, geometry.buffers, usize::from(geometry.views))
}

pub(crate) fn backing_counts(
    nodes: usize,
    buffers: usize,
    view_fields: usize,
) -> Result<usize, TypeCodecError> {
    // Two five-slot tables 64+54, node vector 16N+11,
    // descriptors 16C+11, root/alignment 11, optional i64 vector 8V+11.
    add(
        add(151, mul(16, add(nodes, buffers)?)?)?,
        if view_fields == 0 {
            0
        } else {
            add(11, mul(8, view_fields)?)?
        },
    )
}
pub(super) fn descriptor_bytes(
    pool: &ConstantPool,
    geometry: &Geometry,
    index: usize,
) -> Result<usize, TypeCodecError> {
    descriptor_bytes_span(pool.data(), geometry, index)
}

pub(crate) fn descriptor_bytes_span(
    data: &ArrayData,
    geometry: &Geometry,
    index: usize,
) -> Result<usize, TypeCodecError> {
    if index >= geometry.buffers {
        return Err(invalid());
    }
    if index == 0 {
        return bit_bytes(geometry.rows);
    }
    match layout(data.data_type())? {
        Layout::Bits | Layout::Fixed(_) => Ok(geometry.values_bytes),
        Layout::Offsets(width) => {
            if index == 1 {
                mul(add(geometry.rows, 1)?, width)
            } else {
                Ok(geometry.values_bytes)
            }
        }
        Layout::Views => {
            if index == 1 {
                Ok(geometry.values_bytes)
            } else {
                data.buffers()
                    .get(index - 1)
                    .map(|b| b.len())
                    .ok_or_else(invalid)
            }
        }
        Layout::Null => Err(invalid()),
    }
}
pub(super) fn emit(
    pool: &ConstantPool,
    geometry: &Geometry,
    builder: &mut FlatBufferBuilder<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    let views = matches!(layout(pool.data().data_type())?, Layout::Views);
    let rows = i64::try_from(geometry.rows).map_err(|_| invalid())?;
    let nulls = if matches!(pool.data().data_type(), arrow::datatypes::DataType::Null) {
        rows
    } else {
        i64::try_from(pool.data().null_count()).map_err(|_| invalid())?
    };
    work.flush()?;
    builder.start_vector::<ipc::Buffer>(geometry.buffers);
    work.step()?;
    let mut offset = geometry.body_bytes;
    for index in (0..geometry.buffers).rev() {
        let bytes = descriptor_bytes(pool, geometry, index)?;
        offset = offset.checked_sub(aligned(bytes)?).ok_or_else(invalid)?;
        let entry = ipc::Buffer::new(
            i64::try_from(offset).map_err(|_| invalid())?,
            i64::try_from(bytes).map_err(|_| invalid())?,
        );
        builder.push(entry);
        work.step()?;
    }
    if offset != 0 {
        return Err(invalid());
    }
    let buffers = builder.end_vector::<ipc::Buffer>(geometry.buffers);
    work.flush()?;
    let nodes = builder.create_vector(&[ipc::FieldNode::new(rows, nulls)]);
    work.flush()?;
    let variadic = if views {
        let count = i64::try_from(geometry.variadic).map_err(|_| invalid())?;
        let value = builder.create_vector(&[count]);
        work.flush()?;
        Some(value)
    } else {
        None
    };
    let batch = ipc::RecordBatch::create(
        builder,
        &ipc::RecordBatchArgs {
            length: rows,
            nodes: Some(nodes),
            buffers: Some(buffers),
            compression: None,
            variadicBufferCounts: variadic,
        },
    );
    work.flush()?;
    let message = ipc::Message::create(
        builder,
        &ipc::MessageArgs {
            version: ipc::MetadataVersion::V5,
            header_type: ipc::MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: i64::try_from(geometry.body_bytes).map_err(|_| invalid())?,
            custom_metadata: None,
        },
    );
    work.flush()?;
    ipc::finish_message_buffer(builder, message);
    work.flush()?;
    Ok(())
}
