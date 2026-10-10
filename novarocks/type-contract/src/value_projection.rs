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

//! Pure carrier projections used by function declaration owners.

use arrow_schema::{DataType, TimeUnit};

pub fn arrow_type_equals_ignoring_metadata(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    match (a, b) {
        (List(a), List(b))
        | (LargeList(a), LargeList(b))
        | (ListView(a), ListView(b))
        | (LargeListView(a), LargeListView(b)) => {
            arrow_type_equals_ignoring_metadata(a.data_type(), b.data_type())
        }
        (FixedSizeList(a, a_size), FixedSizeList(b, b_size)) => {
            a_size == b_size && arrow_type_equals_ignoring_metadata(a.data_type(), b.data_type())
        }
        (Struct(a), Struct(b)) => {
            a.len() == b.len()
                && a.iter().zip(b.iter()).all(|(af, bf)| {
                    arrow_type_equals_ignoring_metadata(af.data_type(), bf.data_type())
                })
        }
        (Map(a_field, a_sorted), Map(b_field, b_sorted)) => {
            a_sorted == b_sorted
                && arrow_type_equals_ignoring_metadata(a_field.data_type(), b_field.data_type())
        }
        (Dictionary(a_key, a_value), Dictionary(b_key, b_value)) => {
            arrow_type_equals_ignoring_metadata(a_key, b_key)
                && arrow_type_equals_ignoring_metadata(a_value, b_value)
        }
        (RunEndEncoded(a_run_ends, a_values), RunEndEncoded(b_run_ends, b_values)) => {
            arrow_type_equals_ignoring_metadata(a_run_ends.data_type(), b_run_ends.data_type())
                && arrow_type_equals_ignoring_metadata(a_values.data_type(), b_values.data_type())
        }
        (Union(a_fields, a_mode), Union(b_fields, b_mode)) => {
            a_mode == b_mode
                && a_fields.len() == b_fields.len()
                && a_fields.iter().all(|(a_tag, a_field)| {
                    b_fields.iter().any(|(b_tag, b_field)| {
                        a_tag == b_tag
                            && arrow_type_equals_ignoring_metadata(
                                a_field.data_type(),
                                b_field.data_type(),
                            )
                    })
                })
        }
        _ => a == b,
    }
}

pub fn variant_get_target_type(type_str: &str) -> Result<DataType, String> {
    match type_str.trim().to_ascii_lowercase().as_str() {
        "boolean" | "bool" => Ok(DataType::Boolean),
        "int" | "integer" | "int32" => Ok(DataType::Int32),
        "bigint" | "long" | "int64" => Ok(DataType::Int64),
        "float" | "float32" => Ok(DataType::Float32),
        "double" | "float64" => Ok(DataType::Float64),
        "string" | "varchar" => Ok(DataType::Utf8),
        "date" => Ok(DataType::Date32),
        "datetime" | "timestamp" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        other => Err(format!(
            "variant_get: unsupported type '{other}' \\
             (supported: boolean, int, bigint, float, double, string, date, datetime)"
        )),
    }
}
