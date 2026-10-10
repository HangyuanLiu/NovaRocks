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

//! The original Arrow safe Binary-to-Utf8 author shared by legacy and selected CAST.
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::DataType;

/// Exact original Arrow call; invalid UTF-8 remains a successful NULL.
/// Allocation, validation and copying inside Arrow are opaque work.
pub fn cast_array(array: &ArrayRef) -> Result<ArrayRef, String> {
    arrow_cast::cast(array.as_ref(), &DataType::Utf8).map_err(|e| e.to_string())
}

/// The original author on one already selected row, with no replacement decoder.
pub fn value_text(array: &ArrayRef, row: usize) -> Result<Option<String>, String> {
    let out = cast_array(&array.slice(row, 1))?;
    let text = out
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "binary text author returned a foreign carrier".to_string())?;
    Ok(if text.is_null(0) {
        None
    } else {
        Some(text.value(0).to_string())
    })
}
