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
    type Error: From<CompileControlError>
        + From<FlatPoolResourceError>
        + From<crate::physical_type_v2::TypeCodecError>;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
    fn requests(&mut self, bytes: usize, count: usize) -> Result<(), Self::Error>;
    fn parent(&self) -> bool;
    fn library_work(&mut self, upper: usize) -> Result<(), Self::Error>;
    fn numeric<T>(&self, result: Result<T, FlatPoolResourceError>) -> Result<T, Self::Error> {
        if self.parent() {
            result.map_err(|_| CompileControlError::ResourceExhausted.into())
        } else {
            result.map_err(Into::into)
        }
    }
}
impl ResourceWork for CompileCheckpoints<'_> {
    type Error = FlatPoolResourceError;
    fn step(&mut self) -> Result<(), Self::Error> {
        CompileCheckpoints::step(self).map_err(Into::into)
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        CompileCheckpoints::flush(self).map_err(Into::into)
    }
    fn requests(&mut self, _: usize, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn parent(&self) -> bool {
        false
    }
    fn library_work(&mut self, _: usize) -> Result<(), Self::Error> {
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
impl<F, E> ResourceWork for CapturedWork<'_, '_, F>
where
    F: FnMut(ResourceCountFacts) -> Result<(), E>,
    E: From<CompileControlError>
        + From<FlatPoolResourceError>
        + From<crate::physical_type_v2::TypeCodecError>,
{
    type Error = E;
    fn step(&mut self) -> Result<(), Self::Error> {
        self.facts.observed_work = self
            .facts
            .observed_work
            .checked_add(1)
            .ok_or(CompileControlError::ResourceExhausted)?;
        (self.capture)(self.facts)?;
        self.work.step().map_err(Into::into)
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.work.flush().map_err(Into::into)
    }
    fn requests(&mut self, bytes: usize, count: usize) -> Result<(), Self::Error> {
        self.facts.bytes = self.facts.bytes.max(bytes);
        self.facts.count = self.facts.count.max(count);
        (self.capture)(self.facts)
    }
    fn library_work(&mut self, upper: usize) -> Result<(), Self::Error> {
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
    type Error = FlatPoolResourceError;
    fn step(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn requests(&mut self, _: usize, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn library_work(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn parent(&self) -> bool {
        true
    }
}
