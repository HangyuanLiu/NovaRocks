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

//! Native proto plan lowering scaffolding.

mod expr;
mod fragment;
mod layout;
mod node;
mod scan;
mod sink;

pub(crate) use crate::types::native_proto::{decode_field_type, decode_type};
pub(crate) use fragment::execute_fragment_native;
#[allow(unused_imports)]
pub(crate) use node::{LoweredNode, NodeLoweringContext, lower_proto_node};
