// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Borrowed reads from an already admitted, canonical scalar backing.
//! This accessor neither grants a domain nor rescans selected text bytes.

use super::{ConstantError, ConstantValue};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, ValueLogicalType,
};

pub(super) fn utf8<'a>(
    value: &'a ConstantValue,
    phase: CompilePhase,
    control: &dyn PureCompileControl,
) -> Result<Option<&'a str>, ConstantError> {
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    let result = read(value, &mut work);
    if matches!(result, Err(ConstantError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn read<'a>(
    value: &'a ConstantValue,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<&'a str>, ConstantError> {
    let physical = value.value_type().logical_type == ValueLogicalType::Physical;
    work.step()?;
    if !physical {
        return Err(ConstantError::Invalid(
            "borrowed UTF8 read requires the Physical logical domain",
        ));
    }
    let utf8 = matches!(
        value.value_type().data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    );
    work.step()?;
    if !utf8 {
        return Err(ConstantError::Invalid(
            "borrowed UTF8 read requires an exact UTF8 carrier",
        ));
    }
    // The sole pool owner has admitted the complete Field/FVT, ordinal and
    // canonical concrete array. Reuse its O(1) borrowed accessor, including
    // slice offsets and view addressing; do not run UTF8 validation again.
    let result = value.try_utf8();
    work.step()?;
    result
}

#[cfg(test)]
#[path = "selected_scalar_read_tests.rs"]
mod tests;
