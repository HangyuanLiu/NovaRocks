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

//! Sealed process function metadata. Compiled execution owns its pure kernels.
pub use novarocks_functions::FunctionCatalogError as ExecutionFunctionSetError;
use novarocks_functions::{EngineFunctionCatalog, EngineFunctionCatalogBuilder};
use std::{fmt, sync::Arc};

pub struct ExecutionFunctionSetBuilder {
    catalog: EngineFunctionCatalogBuilder,
}
impl Default for ExecutionFunctionSetBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl ExecutionFunctionSetBuilder {
    pub fn new() -> Self {
        Self {
            catalog: EngineFunctionCatalogBuilder::new(),
        }
    }
    pub fn catalog_builder_mut(&mut self) -> &mut EngineFunctionCatalogBuilder {
        &mut self.catalog
    }
    pub fn catalog_builder(&self) -> &EngineFunctionCatalogBuilder {
        &self.catalog
    }
    pub fn seal(self) -> Result<SealedExecutionFunctionSet, ExecutionFunctionSetError> {
        Ok(SealedExecutionFunctionSet {
            catalog: Arc::new(self.catalog.seal()?),
        })
    }
}

pub struct SealedExecutionFunctionSet {
    catalog: Arc<EngineFunctionCatalog>,
}
impl fmt::Debug for SealedExecutionFunctionSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealedExecutionFunctionSet")
            .field("catalog_digest", &self.catalog.digest())
            .finish()
    }
}
impl SealedExecutionFunctionSet {
    pub fn catalog(&self) -> &Arc<EngineFunctionCatalog> {
        &self.catalog
    }
}

#[cfg(test)]
pub(crate) fn test_builtin_execution_function_set() -> Arc<SealedExecutionFunctionSet> {
    let mut builder = ExecutionFunctionSetBuilder::new();
    novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
        .expect("builtin function metadata");
    Arc::new(builder.seal().expect("builtin function metadata seal"))
}
