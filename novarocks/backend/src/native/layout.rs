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

//! Backend-owned output layout decoding for native wire plans.

use std::collections::HashMap;
use std::sync::Arc;

use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks_protocol::common;

use super::type_decode::decode_field_type;

pub(crate) fn chunk_schema_from_output_columns(
    columns: &[common::OutputColumn],
    path: FieldPath,
) -> Result<ChunkSchemaRef, ProtocolError> {
    let mut slots = Vec::with_capacity(columns.len());
    let mut seen = HashMap::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let column_path = path.clone().index(index);
        let slot_id = SlotId::new(column.column_id);
        if let Some(first_index) = seen.insert(slot_id, index) {
            return Err(error(
                column_path.field("column_id"),
                ProtocolErrorKind::InconsistentFields,
                format!(
                    "duplicate OutputColumn.column_id {} at index {} (first seen at index {})",
                    column.column_id, index, first_index
                ),
            ));
        }
        let type_desc = column.r#type.as_ref().ok_or_else(|| {
            error(
                column_path.clone().field("type"),
                ProtocolErrorKind::MissingField,
                format!(
                    "OutputColumn.type missing for column_id={} name='{}' at index {}",
                    column.column_id, column.name, index
                ),
            )
        })?;
        let field = decode_field_type(&column.name, column.nullable, type_desc).map_err(|detail| {
            error(
                column_path.field("type"),
                ProtocolErrorKind::InvalidValue,
                format!(
                    "OutputColumn.type decode failed for column_id={} name='{}' at index {}: {}",
                    column.column_id, column.name, index, detail
                ),
            )
        })?;
        slots.push(
            ChunkSchema::slot_schema_from_arrow_field(slot_id, &field)
                .map_err(|detail| error(path.clone(), ProtocolErrorKind::InvalidValue, detail))?,
        );
    }
    ChunkSchema::try_new(slots)
        .map(Arc::new)
        .map_err(|detail| error(path, ProtocolErrorKind::InvalidValue, detail))
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ProtocolFamily::Native, path, kind, detail)
}
