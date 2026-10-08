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

//! TO_DATE registration shares the DATE extraction contract and CPU author.
use super::catalogue::BuiltinScalarResolver;
use crate::{FunctionBindingDeclaration, FunctionCatalogError, FunctionDefinition};
use novarocks_type_contract::FunctionEffectDeclaration;

pub(super) fn operation(name: &str) -> bool {
    name == "to_date"
}
pub(super) fn effects() -> FunctionEffectDeclaration {
    super::date_owner::effects()
}
pub(super) fn definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    super::date_owner::definition(name, declaration, resolver)
}
