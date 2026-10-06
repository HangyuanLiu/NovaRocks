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

//! Delegated observation of the original receiver numerical authors. This
//! adapter borrows one caller meter; it owns no controller or work counter.
use super::FlatPoolResourceError;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};

pub(crate) trait ResourceWork {
    fn step(&mut self) -> Result<(), CompileControlError>;
    fn flush(&mut self) -> Result<(), CompileControlError>;
    fn requests(&mut self, bytes: usize, count: usize) -> Result<(), CompileControlError>;
    fn parent(&self) -> bool;
    fn library_work(&mut self, upper: usize) -> Result<(), CompileControlError>;
    fn numeric<T>(
        &self,
        result: Result<T, FlatPoolResourceError>,
    ) -> Result<T, FlatPoolResourceError> {
        if self.parent() {
            result.map_err(|_| CompileControlError::ResourceExhausted.into())
        } else {
            result
        }
    }
}
impl ResourceWork for CompileCheckpoints<'_> {
    fn step(&mut self) -> Result<(), CompileControlError> {
        CompileCheckpoints::step(self)
    }
    fn flush(&mut self) -> Result<(), CompileControlError> {
        CompileCheckpoints::flush(self)
    }
    fn requests(&mut self, _: usize, _: usize) -> Result<(), CompileControlError> {
        Ok(())
    }
    fn parent(&self) -> bool {
        false
    }
    fn library_work(&mut self, _: usize) -> Result<(), CompileControlError> {
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ResourceCountFacts {
    pub bytes: usize,
    pub count: usize,
    pub work: usize,
    pub observed_work: usize,
}
pub(crate) struct CapturedWork<'a, 'c, F> {
    pub work: &'a mut CompileCheckpoints<'c>,
    pub capture: F,
    pub facts: ResourceCountFacts,
}
impl<F: FnMut(ResourceCountFacts) -> Result<(), CompileControlError>> ResourceWork
    for CapturedWork<'_, '_, F>
{
    fn step(&mut self) -> Result<(), CompileControlError> {
        self.facts.observed_work = self
            .facts
            .observed_work
            .checked_add(1)
            .ok_or(CompileControlError::ResourceExhausted)?;
        (self.capture)(self.facts)?;
        self.work.step()
    }
    fn flush(&mut self) -> Result<(), CompileControlError> {
        self.work.flush()
    }
    fn requests(&mut self, bytes: usize, count: usize) -> Result<(), CompileControlError> {
        self.facts.bytes = self.facts.bytes.max(bytes);
        self.facts.count = self.facts.count.max(count);
        (self.capture)(self.facts)
    }
    fn library_work(&mut self, upper: usize) -> Result<(), CompileControlError> {
        self.facts.work = self.facts.work.max(upper);
        (self.capture)(self.facts)
    }
    fn parent(&self) -> bool {
        true
    }
}

/// O(1) header projection through the same numerical author. It neither
/// traverses children nor reports synthetic completed operations.
pub(crate) struct HeaderWork;
impl ResourceWork for HeaderWork {
    fn step(&mut self) -> Result<(), CompileControlError> {
        Ok(())
    }
    fn flush(&mut self) -> Result<(), CompileControlError> {
        Ok(())
    }
    fn requests(&mut self, _: usize, _: usize) -> Result<(), CompileControlError> {
        Ok(())
    }
    fn library_work(&mut self, _: usize) -> Result<(), CompileControlError> {
        Ok(())
    }
    fn parent(&self) -> bool {
        true
    }
}
