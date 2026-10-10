// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Installed unary MD5 delegates to the sole original digest computation.

use super::md5_shared::Operation;
use crate::{KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues};

pub(super) fn evaluate_string_md5<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    super::md5_selected::evaluate(Operation::Md5, input, control)
}

#[cfg(test)]
fn output_capacity(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    super::md5_selected::output_capacity(rows, bytes)
}

#[cfg(test)]
use crate::{
    FunctionArgumentType,
    kernel_control::{internal, invalid},
};
#[cfg(test)]
use arrow_array::{Array, ArrayRef, StringArray};
#[cfg(test)]
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
#[cfg(test)]
use arrow_schema::DataType;
#[cfg(test)]
use novarocks_type_contract::ValueLogicalType;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
#[path = "string_md5_tests.rs"]
mod tests;
