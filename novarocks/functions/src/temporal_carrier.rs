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

//! Frozen v1 Arrow temporal casts shared by full-array and selected calls.
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;

/// Admit only these exact carrier conversions; timezone policy stays explicit.
pub fn supports(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (
            DataType::Date32,
            DataType::Utf8 | DataType::Timestamp(_, None)
        ) | (DataType::Timestamp(_, None), DataType::Date32)
    )
}

/// Preserve the original Arrow cast body and its complete errors. In particular,
/// Date32 timestamp multiplication still visits hidden NULL payloads and retains
/// its debug-overflow panic; Timestamp->Date32 uses Chrono calendar flooring.
/// A selected caller passes a one-row slice; the legacy shell passes all rows.
pub fn cast(array: &dyn Array, target: &DataType) -> Result<ArrayRef, String> {
    if !supports(array.data_type(), target) {
        return Err(format!(
            "unsupported exact temporal cast from {:?} to {target:?}",
            array.data_type()
        ));
    }
    arrow_cast::cast(array, target).map_err(|error| error.to_string())
}
