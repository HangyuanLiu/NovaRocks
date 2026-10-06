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

//! Numerical policy for the original IPC writer resource authors.
//! A snapshot replaces this child contribution; it is not a new wallet.
use crate::physical_type_v2::TypeCodecError;
use novarocks_type_contract::CompileControlError;

#[derive(Clone, Copy)]
pub(crate) struct Policy(pub bool);
impl Policy {
    pub(crate) fn numeric<T>(self, result: Result<T, TypeCodecError>) -> Result<T, TypeCodecError> {
        if self.0 {
            result.map_err(|_| CompileControlError::ResourceExhausted.into())
        } else {
            result
        }
    }
    pub(crate) fn cap(
        self,
        actual: usize,
        maximum: usize,
        message: &'static str,
    ) -> Result<(), TypeCodecError> {
        if actual <= maximum {
            return Ok(());
        }
        if self.0 {
            Err(CompileControlError::ResourceExhausted.into())
        } else {
            Err(TypeCodecError::InvalidShape(message))
        }
    }
}
/// Only the three quantities produced by the existing Requests/count author.
/// No SchemaPreflight, source ownership, allocation grant or new limit exists.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SchemaWriterRequestFacts {
    pub request_bytes: usize,
    pub request_count: usize,
    pub work_upper_bound: usize,
}
pub(crate) type SchemaAdmit<'a> =
    dyn FnMut(&SchemaWriterRequestFacts) -> Result<(), CompileControlError> + 'a;

pub(super) struct Admission<'a, 'b> {
    pub parent: Option<&'a mut SchemaAdmit<'b>>,
    pub source: usize,
    pub reader: bool,
    pub max_work: usize,
    pub facts: SchemaWriterRequestFacts,
}
impl Admission<'_, '_> {
    pub fn policy(&self) -> Policy {
        Policy(self.parent.is_some())
    }
    pub fn update(&mut self, facts: SchemaWriterRequestFacts) -> Result<(), TypeCodecError> {
        self.facts.request_bytes = self.facts.request_bytes.max(facts.request_bytes);
        self.facts.request_count = self.facts.request_count.max(facts.request_count);
        self.facts.work_upper_bound = self.facts.work_upper_bound.max(facts.work_upper_bound);
        if let Some(parent) = self.parent.as_mut() {
            if self.facts.work_upper_bound > self.max_work {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            parent(&self.facts)?;
        }
        Ok(())
    }
}
