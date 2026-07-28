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

//! Native coordinator-to-runtime wire encoders.

#[cfg(test)]
mod boundary_schema;
#[cfg(test)]
mod build;
mod bundle;
mod expr;
mod iceberg_delta_scan;
mod iceberg_literal_json;
pub(crate) mod instance;
pub(crate) mod plan;

#[cfg(test)]
pub(crate) use bundle::{NativeBundleTestDrift, corrupt_native_fragment_bundle_for_execution_test};
pub(crate) use bundle::{NativeFragmentBundle, encode_native_fragment_bundle};
pub(crate) use instance::encode_instance_params;
pub(crate) use plan::encode_data_partition;

#[cfg(test)]
mod tests;
