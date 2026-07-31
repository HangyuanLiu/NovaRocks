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

pub mod common;
pub(crate) mod native;
pub mod native_fragment_assembly_port;

pub use common::error::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};

/// Decode the lifecycle-owned query-options DTO into execution options.
///
/// This is deliberately separate from native fragment assembly: backend roles
/// validate their `InstanceParams` before passing the resulting execution value
/// to the core assembly kernel.
pub fn decode_native_query_options(
    src: &crate::proto::novarocks::QueryOptions,
) -> Result<crate::runtime::query_options::QueryOptions, ProtocolError> {
    native::query_options_contract::decode_query_options(src)
}
