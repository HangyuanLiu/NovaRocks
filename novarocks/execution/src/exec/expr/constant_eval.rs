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

//! Legacy arena constant broadcasting. Format checks belong to the shared
//! neutral selected-copy author; this adapter retains the existing old path.

use arrow::array::{ArrayRef, UInt32Array};
use novarocks_functions::{ConstantValue, selected_copy::preflight_broadcast};

pub(super) fn broadcast(value: &ConstantValue, rows: usize) -> Result<ArrayRef, String> {
    let source = value.pool().array();
    if rows == 0 {
        return Ok(source.slice(value.ordinal() as usize, 0));
    }
    // The legacy arena has no cooperative evaluation control. Its existing
    // adapter stays explicit here while compiled evaluation uses its own meter.
    preflight_broadcast(source.as_ref(), value.ordinal(), rows, |_| Ok(()))
        .map_err(|error| error.to_string())?;
    let indices = UInt32Array::from(vec![value.ordinal(); rows]);
    let output = arrow::compute::take(source.as_ref(), &indices, None)
        .map_err(|error| format!("constant pool broadcast failed: {error}"))?;
    if !novarocks_type_contract::arrow_data_types_exact(output.data_type(), source.data_type()) {
        return Err("constant broadcast changed the exact Arrow carrier".to_owned());
    }
    Ok(output)
}
