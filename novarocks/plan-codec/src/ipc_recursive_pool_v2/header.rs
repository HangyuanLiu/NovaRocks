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

//! Standard preorder node/buffer tables, emitted in reverse without scratch vectors.
use super::geometry::{Geometry, Node, Span, invalid, walk};
use crate::{
    ipc_flat_pool_v2::{
        geometry::{add, aligned, bit_bytes, mul},
        header::descriptor_bytes_span,
    },
    physical_type_v2::TypeCodecError,
};
use arrow::{array::ArrayData, ipc};
use flatbuffers::FlatBufferBuilder;
use novarocks_type_contract::CompileCheckpoints;

pub(super) fn descriptor(node: &Node<'_>, index: usize) -> Result<usize, TypeCodecError> {
    if let Some(leaf) = &node.leaf {
        return descriptor_bytes_span(node.span.data, leaf, index);
    }
    if index >= node.buffers {
        return Err(invalid());
    }
    if index == 0 {
        return bit_bytes(node.span.len);
    }
    let (width, _, _) = node.offset.ok_or_else(invalid)?;
    mul(add(node.span.len, 1)?, width)
}

pub(super) fn emit(
    data: &ArrayData,
    geometry: &Geometry,
    builder: &mut FlatBufferBuilder<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    let root = Span {
        data,
        start: 0,
        len: geometry.rows,
    };
    work.flush()?;
    builder.start_vector::<ipc::Buffer>(geometry.buffers);
    work.step()?;
    let mut offset = geometry.body_bytes;
    let mut buffer_count = 0;
    walk(root, true, false, 1, work, &mut |node, work| {
        for index in (0..node.buffers).rev() {
            let bytes = descriptor(node, index)?;
            offset = offset.checked_sub(aligned(bytes)?).ok_or_else(invalid)?;
            builder.push(ipc::Buffer::new(
                i64::try_from(offset).map_err(|_| invalid())?,
                i64::try_from(bytes).map_err(|_| invalid())?,
            ));
            buffer_count = add(buffer_count, 1)?;
            work.step()?;
        }
        Ok(())
    })?;
    if offset != 0 || buffer_count != geometry.buffers {
        return Err(invalid());
    }
    let buffers = builder.end_vector::<ipc::Buffer>(geometry.buffers);
    work.flush()?;
    builder.start_vector::<ipc::FieldNode>(geometry.nodes);
    work.step()?;
    let mut node_count = 0;
    walk(root, true, true, 1, work, &mut |node, work| {
        builder.push(ipc::FieldNode::new(
            i64::try_from(node.span.len).map_err(|_| invalid())?,
            i64::try_from(node.null_count).map_err(|_| invalid())?,
        ));
        node_count = add(node_count, 1)?;
        work.step()?;
        Ok(())
    })?;
    if node_count != geometry.nodes {
        return Err(invalid());
    }
    let nodes = builder.end_vector::<ipc::FieldNode>(geometry.nodes);
    work.flush()?;
    let variadic = if geometry.view_fields == 0 {
        None
    } else {
        builder.start_vector::<i64>(geometry.view_fields);
        work.step()?;
        let mut view_count = 0;
        walk(root, true, false, 1, work, &mut |node, work| {
            if let Some(leaf) = &node.leaf
                && leaf.views
            {
                builder.push(i64::try_from(leaf.variadic).map_err(|_| invalid())?);
                view_count = add(view_count, 1)?;
                work.step()?;
            }
            Ok(())
        })?;
        if view_count != geometry.view_fields {
            return Err(invalid());
        }
        let vector = builder.end_vector::<i64>(geometry.view_fields);
        work.flush()?;
        Some(vector)
    };
    let batch = ipc::RecordBatch::create(
        builder,
        &ipc::RecordBatchArgs {
            length: i64::try_from(geometry.rows).map_err(|_| invalid())?,
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
