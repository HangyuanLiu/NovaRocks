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

/// Selects partition-boundary keys across current and historical sink wire shapes.
///
/// Current FEs send the repeated `key_nodes` shape; older FEs sent the singular
/// `key_node` shape. The current repeated field wins when both are present. This
/// rule can be removed once the minimum supported FE version never sends `key_node`.
pub(crate) fn select_partition_boundary_key<'a, T>(
    current: Option<&'a [T]>,
    legacy: Option<&'a T>,
) -> Option<&'a [T]> {
    current.or_else(|| legacy.map(std::slice::from_ref))
}
