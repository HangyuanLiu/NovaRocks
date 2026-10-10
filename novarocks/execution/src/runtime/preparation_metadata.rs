// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Borrowed preparation port at the original Project schema birth sites.
//! An implementation qualifies the complete operation before consuming its
//! ONE original body. This trusted port is not a public provenance issuer.
use crate::{exec::chunk::ChunkSchemaRef, runtime::fragment::ExecutionResult};
use novarocks_local_program::{LocalProgram, ProgramNodeId, StaticLayout};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectSchemaSite {
    Project(ProgramNodeId),
    FinalResult,
}
pub trait CompiledSchemaMetadataScope {
    fn materialize<B>(
        &mut self,
        program: &Arc<LocalProgram>,
        site: ProjectSchemaSite,
        layout: &StaticLayout,
        body: B,
    ) -> ExecutionResult<ChunkSchemaRef>
    where
        B: FnOnce() -> Result<ChunkSchemaRef, String>;
}
