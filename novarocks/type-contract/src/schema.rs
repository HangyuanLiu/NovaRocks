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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow_schema::{DataType, Field, Schema};

pub const MAX_ARROW_FIELD_NAME_BYTES: usize = 1024;
pub const MAX_ARROW_FIELD_METADATA_ENTRIES: usize = 256;
pub const MAX_ARROW_FIELD_METADATA_KEY_BYTES: usize = 1024;
pub const MAX_ARROW_FIELD_METADATA_VALUE_BYTES: usize = 16 * 1024;
pub const MAX_ARROW_FIELD_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES: usize = 1024;

/// Compare every Arrow physical field attribute recursively. Arrow's built-in
/// `Field::eq` deliberately ignores dictionary ids and dictionary ordering,
/// which is appropriate for logical schema compatibility but not for a frozen
/// internal-relation contract.
pub fn arrow_schemas_exact(left: &Schema, right: &Schema) -> bool {
    left.metadata() == right.metadata()
        && left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| arrow_fields_exact(left, right))
}

pub fn arrow_fields_exact(left: &Field, right: &Field) -> bool {
    #[allow(deprecated)]
    let dictionary_ids_equal = left.dict_id() == right.dict_id();
    left.name() == right.name()
        && left.is_nullable() == right.is_nullable()
        && left.metadata() == right.metadata()
        && dictionary_ids_equal
        && left.dict_is_ordered() == right.dict_is_ordered()
        && arrow_data_types_exact(left.data_type(), right.data_type())
}

pub fn arrow_data_types_exact(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::List(left), DataType::List(right))
        | (DataType::ListView(left), DataType::ListView(right))
        | (DataType::LargeList(left), DataType::LargeList(right))
        | (DataType::LargeListView(left), DataType::LargeListView(right)) => {
            arrow_fields_exact(left, right)
        }
        (
            DataType::FixedSizeList(left_field, left_size),
            DataType::FixedSizeList(right_field, right_size),
        ) => left_size == right_size && arrow_fields_exact(left_field, right_field),
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| arrow_fields_exact(left, right))
        }
        (DataType::Union(left_fields, left_mode), DataType::Union(right_fields, right_mode)) => {
            left_mode == right_mode
                && left_fields.len() == right_fields.len()
                && left_fields.iter().zip(right_fields.iter()).all(
                    |((left_id, left), (right_id, right))| {
                        left_id == right_id && arrow_fields_exact(left, right)
                    },
                )
        }
        (
            DataType::Dictionary(left_key, left_value),
            DataType::Dictionary(right_key, right_value),
        ) => {
            arrow_data_types_exact(left_key, right_key)
                && arrow_data_types_exact(left_value, right_value)
        }
        (
            DataType::Map(left_entries, left_ordered),
            DataType::Map(right_entries, right_ordered),
        ) => left_ordered == right_ordered && arrow_fields_exact(left_entries, right_entries),
        (
            DataType::RunEndEncoded(left_runs, left_values),
            DataType::RunEndEncoded(right_runs, right_values),
        ) => {
            arrow_fields_exact(left_runs, right_runs)
                && arrow_fields_exact(left_values, right_values)
        }
        _ => left == right,
    }
}
