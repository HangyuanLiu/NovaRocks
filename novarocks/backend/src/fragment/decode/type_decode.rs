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

//! Transitional imports for the native plan-wire type codec.
//!
//! The codec is owned by `novarocks-plan-codec`. This private module remains
//! only while Backend decode callers move to the owner-local imports.

pub(crate) use novarocks_plan_codec::native_type::{decode_field_type, decode_type};

#[cfg(test)]
pub(crate) use novarocks_plan_codec::encode_native_type as encode_type;
