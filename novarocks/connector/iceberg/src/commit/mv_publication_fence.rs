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

//! Retired MV publication-fence ref name.
//!
//! Current code never decodes, mutates, or garbage-collects this historical
//! shape. Metadata projection still filters the reserved ref from user-facing
//! output, so the name remains local to the provider until that projection is
//! removed.

pub const MV_PUBLICATION_FENCE_REF: &str = "__novarocks_mv_publication_fence_v1";
