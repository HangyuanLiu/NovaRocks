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

//! Iceberg storage for application-owned opaque documents.
//!
//! The application owns every document body and version. This module owns the
//! Iceberg envelope, immutable sidecar names, exact metadata projection,
//! catalog pagination, and provider-level reachability used by cleanup.

pub(crate) mod codec;
mod create;
mod discovery;
pub(crate) mod envelope;
pub(crate) mod io;
pub(crate) mod observation;
mod reference;
mod retention;

pub use create::IcebergDocumentStorage;
pub(crate) use retention::retained_sidecars_for_roots;
