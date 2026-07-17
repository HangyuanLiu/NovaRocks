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

//! Single-source function signature registry.
//!
//! Before this module landed, analyzer and codegen each carried their own
//! private "given a function name and argument types, what is the return
//! type?" logic — analyzer in [`crate::sql::analyzer::functions`] and the now
//! retired legacy FE Thrift expression emitter. The two copies were drifting
//! (the emitter side, for example, recognised `parse_url -> Utf8` while the
//! analyzer did not), and adding a new SQL function meant patching both sides
//! at once.
//!
//! This module follows StarRocks' [`functions.py`] approach: every supported
//! scalar function (and operator) is described once, by a [`Signature`] of
//! parameter types and a return type. Resolving a call is then a lookup
//! against that table (`strict → polymorphic → cast`), and both analyzer
//! and codegen share the same answer.
//!
//! Step A of the migration deliberately covers only the high-frequency
//! function families (string / numeric / condition / a few array helpers).
//! Anything not yet registered here falls through to the legacy
//! hand-written `infer_*` paths so existing behaviour is preserved.
//!
//! [`functions.py`]: https://github.com/StarRocks/starrocks/blob/main/gensrc/script/functions.py

pub(crate) mod registry;
pub(crate) mod resolver;
pub(crate) mod signature;

pub(crate) use resolver::{
    ResolveError, ResolvedScalarFunction, resolve_scalar_function,
    resolve_scalar_function_signature,
};
pub(crate) use signature::{Signature, TypeSpec};
