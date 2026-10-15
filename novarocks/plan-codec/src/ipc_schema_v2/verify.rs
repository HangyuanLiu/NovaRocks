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

//! Exact authored-source adapter to the shared standard IPC verifier.
use crate::physical_type_v2::TypeCodecError;
use arrow::datatypes::Field;
use arrow::ipc;
use novarocks_type_contract::CompileCheckpoints;

pub(super) fn verify_field(
    expected: &Field,
    actual: ipc::Field<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    use novarocks_functions::arrow_ipc_construction::{
        FieldVerificationFailure, FieldVerificationPurpose, verify_field,
    };
    verify_field(
        expected,
        actual,
        &mut || work.step(),
        FieldVerificationPurpose::ExactSource,
        &mut |_| Ok(()),
    )
    .map_err(|cause| match cause {
        FieldVerificationFailure::InvalidShape(message) => TypeCodecError::InvalidShape(message),
        FieldVerificationFailure::Observer(cause) => TypeCodecError::from(cause),
    })
}
