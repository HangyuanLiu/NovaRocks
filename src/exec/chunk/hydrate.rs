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

use std::sync::Arc;

use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use super::{Chunk, ChunkSchema};

pub(crate) fn hydrate_dictionary_columns(chunk: &Chunk) -> Result<Chunk, String> {
    let mut changed = false;
    let mut columns = Vec::with_capacity(chunk.columns().len());
    let mut fields = Vec::with_capacity(chunk.schema().fields().len());
    let mut slots = Vec::with_capacity(chunk.chunk_schema().slots().len());

    for (idx, ((column, field), slot)) in chunk
        .columns()
        .iter()
        .zip(chunk.schema().fields().iter())
        .zip(chunk.chunk_schema().slots().iter())
        .enumerate()
    {
        match column.data_type() {
            DataType::Dictionary(_, value_type) => {
                changed = true;
                let value_type = value_type.as_ref().clone();
                let flat_field = field.as_ref().clone().with_data_type(value_type.clone());
                let flat = cast(column.as_ref(), &value_type).map_err(|e| {
                    format!(
                        "hydrate dictionary chunk column {} to value type {:?} failed: {e}",
                        idx, value_type
                    )
                })?;
                columns.push(flat);
                fields.push(Arc::new(flat_field.clone()));
                slots.push(slot.with_field(flat_field)?);
            }
            _ => {
                columns.push(Arc::clone(column));
                fields.push(Arc::clone(field));
                slots.push(slot.clone());
            }
        }
    }

    if !changed {
        return Ok(chunk.clone());
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        chunk.schema().metadata().clone(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| format!("build hydrated chunk record batch failed: {e}"))?;
    let chunk_schema = Arc::new(ChunkSchema::try_new(slots)?);
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, DictionaryArray, LargeStringArray, LargeStringDictionaryBuilder,
        StringArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type};

    use super::hydrate_dictionary_columns;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkFieldSchema, ChunkSchema, ChunkSlotSchema};
    use crate::types::logical::{LogicalType, field_with_logical_type};

    fn dict_utf8_with_nulls_and_empty() -> ArrayRef {
        Arc::new(
            vec![Some("PAID"), None, Some(""), Some("NEW")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }

    fn dict_large_utf8_with_nulls_and_empty() -> ArrayRef {
        let mut builder = LargeStringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("PAID");
        builder.append_null();
        builder.append_value("");
        builder.append_value("NEW");
        Arc::new(builder.finish())
    }

    fn chunk_with_column(slot_id: SlotId, field: Field, column: ArrayRef) -> Chunk {
        chunk_with_slot(
            ChunkSlotSchema::new_with_field(slot_id, field, None, None),
            column,
        )
    }

    fn chunk_with_slot(slot: ChunkSlotSchema, column: ArrayRef) -> Chunk {
        let schema = Arc::new(ChunkSchema::try_new(vec![slot]).expect("chunk schema"));
        Chunk::try_new_with_columns(schema, vec![column]).expect("chunk")
    }

    fn json_field_schema() -> ChunkFieldSchema {
        ChunkFieldSchema::from_field(&field_with_logical_type(
            Field::new("logical_payload", DataType::Utf8, true),
            LogicalType::Json,
        ))
        .expect("logical field schema")
    }

    #[test]
    fn hydrate_dictionary_columns_flattens_utf8_dictionary_preserving_slot_contract() {
        let slot_id = SlotId::new(7);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), "dict".to_string());
        let chunk = chunk_with_column(
            slot_id,
            Field::new("status", DataType::Utf8, false).with_metadata(metadata.clone()),
            dict_utf8_with_nulls_and_empty(),
        );

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::Utf8);
        let values = hydrated.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(values.value(0), "PAID");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "");
        assert_eq!(values.value(3), "NEW");
        let field = hydrated
            .chunk_schema()
            .field_by_slot(slot_id)
            .expect("slot field");
        assert_eq!(field.name(), "status");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(field.metadata(), &metadata);
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(slot_id)
                .expect("slot schema")
                .slot_id(),
            slot_id
        );
    }

    #[test]
    fn hydrate_dictionary_columns_flattens_large_utf8_dictionary_with_nulls_and_empty_strings() {
        let slot_id = SlotId::new(11);
        let chunk = chunk_with_column(
            slot_id,
            Field::new("status_l", DataType::LargeUtf8, true),
            dict_large_utf8_with_nulls_and_empty(),
        );

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::LargeUtf8);
        let values = hydrated.columns()[0]
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("large string array");
        assert_eq!(values.value(0), "PAID");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "");
        assert_eq!(values.value(3), "NEW");
        assert_eq!(
            hydrated
                .chunk_schema()
                .field_by_slot(slot_id)
                .expect("slot field")
                .data_type(),
            &DataType::LargeUtf8
        );
    }

    #[test]
    fn hydrate_dictionary_columns_preserves_slot_unique_id_and_field_schema() {
        let slot_id = SlotId::new(13);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), "dict".to_string());
        let slot = ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("payload", DataType::Utf8, true).with_metadata(metadata.clone()),
            Some(json_field_schema()),
            Some(77),
        );
        let chunk = chunk_with_slot(slot, dict_utf8_with_nulls_and_empty());

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        let hydrated_slot = hydrated
            .chunk_schema()
            .slot(slot_id)
            .expect("hydrated slot");
        assert_eq!(hydrated_slot.unique_id(), Some(77));
        assert_eq!(hydrated_slot.field().metadata(), &metadata);
        assert_eq!(hydrated_slot.data_type(), &DataType::Utf8);
        assert!(hydrated_slot.field_schema().json_semantic());
    }

    #[test]
    fn hydrate_dictionary_columns_fast_path_preserves_plain_chunk_schema() {
        let slot_id = SlotId::new(17);
        let slot = ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("payload", DataType::Utf8, true),
            Some(json_field_schema()),
            Some(77),
        );
        let chunk = chunk_with_slot(
            slot,
            Arc::new(StringArray::from(vec![Some("PAID"), None, Some("")])),
        );

        let hydrated = hydrate_dictionary_columns(&chunk).expect("hydrate");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::Utf8);
        let hydrated_slot = hydrated
            .chunk_schema()
            .slot(slot_id)
            .expect("hydrated slot");
        assert_eq!(hydrated_slot.unique_id(), Some(77));
        assert_eq!(hydrated_slot.data_type(), &DataType::Utf8);
        assert!(hydrated_slot.field_schema().json_semantic());
    }
}
