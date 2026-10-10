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
//! Exact non-value provenance for REGEXP_COUNT's original error policy.
/// Native v1 emits this checked Utf8 Constant as a literal. Its NULL payload
/// is masked before the policy matters; every non-NULL payload is Utf8Literal.
/// Dynamic includes casts, calls, slots and the separate legacy pooled form.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RegexpCountPatternSource {
    NativeV1Utf8LiteralWhenPresent,
    Dynamic,
}
impl RegexpCountPatternSource {
    pub const fn invalid_pattern_is_error(self) -> bool {
        matches!(self, Self::NativeV1Utf8LiteralWhenPresent)
    }
}
