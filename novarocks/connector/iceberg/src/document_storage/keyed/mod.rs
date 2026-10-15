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

//! Canonical inline storage. Payload interpretation stays with each application.
// Design: ADR-0178 (docs/adr/ADR-0178-inline-keyed-documents-and-snapshot-records.md)

pub(crate) mod envelope;
pub(crate) mod expiry;
pub(crate) mod keys;
pub(crate) mod limits;
pub(crate) mod record;
pub(crate) mod resolution;
pub(crate) mod retention;
pub(crate) mod view;
pub(crate) mod write;

pub(crate) use view::{project_full_view, project_view, reference_closure};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod calculation_test_support;
