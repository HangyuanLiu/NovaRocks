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

mod arithmetic;
pub(crate) mod arrow_primitive;
#[cfg(feature = "compat")]
pub(crate) mod arrow_thrift;
pub(crate) mod coercion;
pub(crate) mod logical;
mod predicate;
pub(crate) mod primitive;

#[allow(unused_imports)]
pub(crate) use arithmetic::{
    arithmetic_result_type, arithmetic_result_type_with_op, canonical_agg_decimal_type,
    decimal_arithmetic_result_type,
};
pub(crate) use coercion::{comparison_common_type, wider_type};
#[allow(unused_imports)]
pub(crate) use primitive::PrimitiveType;
