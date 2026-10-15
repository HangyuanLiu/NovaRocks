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

//! One construction body for explicit pure authors and original host creators.
use super::BindingCodecError as Error;
use crate::host_projection_v2::ProjectionFailure;
use novarocks_type_contract::{
    AggregateStateFormatId, CompileCheckpoints, CompileControlError, FunctionId,
    FunctionIdentityError, FunctionOverloadId, TypedFact, TypedFactSlice, ValueSchemaCreationScope,
    ValueTypeStorageLifetime, owned_resources::layout::LayoutResourceError,
};
use std::alloc::Layout;

pub(crate) type Failure<H> = ProjectionFailure<Error, H>;
pub(crate) struct Construction<'a, H> {
    pub(crate) creator: Option<&'a mut dyn ValueSchemaCreationScope<H>>,
}
enum IdentityFailure<H> {
    Grammar(FunctionIdentityError),
    Resource(LayoutResourceError),
    Host(H),
    Control(CompileControlError),
}
impl<H> From<FunctionIdentityError> for IdentityFailure<H> {
    fn from(error: FunctionIdentityError) -> Self {
        Self::Grammar(error)
    }
}
impl<H> From<LayoutResourceError> for IdentityFailure<H> {
    fn from(error: LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}
impl<H> Construction<'_, H> {
    pub(crate) fn creator_reborrow(&mut self) -> Option<&mut dyn ValueSchemaCreationScope<H>> {
        match self.creator.as_mut() {
            Some(creator) => Some(&mut **creator),
            None => None,
        }
    }

    pub(crate) fn request(&mut self, layout: Layout) -> Result<(), Failure<H>> {
        if let Some(creator) = self.creator.as_deref_mut() {
            creator
                .request_layout(layout)
                .map_err(ProjectionFailure::Host)?;
        }
        Ok(())
    }
    pub(crate) fn storage(&self) -> ValueTypeStorageLifetime {
        self.creator
            .as_ref()
            .map(|value| value.storage_lifetime())
            .unwrap_or_else(ValueTypeStorageLifetime::pure)
    }
    pub(crate) fn reserve<T>(
        &mut self,
        count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<T>, Failure<H>> {
        let layout =
            Layout::array::<T>(count).map_err(|_| CompileControlError::ResourceExhausted)?;
        work.flush()?;
        if count != 0 {
            self.request(layout)?;
        }
        // The original Vec body and ordinary refusal/control ordering stay shared.
        super::materialize::reserve(count, work).map_err(ProjectionFailure::Codec)
    }
    pub(crate) fn boxed<T>(
        &mut self,
        values: Vec<T>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[T]>, Failure<H>> {
        work.flush()?;
        if values.capacity() > values.len() && !values.is_empty() {
            let layout = Layout::array::<T>(values.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            self.request(layout)?;
        }
        super::materialize::boxed(values, work).map_err(ProjectionFailure::Codec)
    }
    pub(crate) fn collection<T: TypedFact>(
        &mut self,
        items: Box<[T]>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<TypedFactSlice<T>, Failure<H>> {
        work.flush()?;
        let value = if self.creator.is_some() {
            let storage = self.storage();
            TypedFactSlice::try_from_boxed_observed(items, storage, |layout| self.request(layout))?
        } else {
            TypedFactSlice::from_pure_boxed(items)
        };
        work.step()?;
        work.flush()?;
        Ok(value)
    }
    fn identity<T>(
        &mut self,
        value: &str,
        work: &mut CompileCheckpoints<'_>,
        construct: impl FnOnce(
            &str,
            ValueTypeStorageLifetime,
            &mut dyn FnMut(Layout) -> Result<(), IdentityFailure<H>>,
            &mut dyn FnMut(usize) -> Result<(), IdentityFailure<H>>,
        ) -> Result<T, IdentityFailure<H>>,
    ) -> Result<T, Failure<H>> {
        work.flush()?;
        let storage = self.storage();
        let mut request = |layout| {
            self.request(layout).map_err(|cause| match cause {
                ProjectionFailure::Host(cause) => IdentityFailure::Host(cause),
                ProjectionFailure::Codec(Error::Control(cause)) => IdentityFailure::Control(cause),
                ProjectionFailure::Codec(_) => {
                    unreachable!("request port has no ordinary codec error")
                }
            })
        };
        let mut observe = |units| {
            for _ in 0..units {
                work.step().map_err(IdentityFailure::Control)?;
            }
            Ok(())
        };
        let result = construct(value, storage, &mut request, &mut observe);
        match result {
            Err(IdentityFailure::Host(cause)) => Err(ProjectionFailure::Host(cause)),
            Err(IdentityFailure::Control(cause)) => Err(cause.into()),
            Err(IdentityFailure::Resource(_)) => Err(CompileControlError::ResourceExhausted.into()),
            result => {
                work.flush()?;
                result.map_err(|cause| match cause {
                    IdentityFailure::Grammar(_) => ProjectionFailure::Codec(Error::InvalidShape(
                        "materialized identity is invalid",
                    )),
                    IdentityFailure::Resource(_) => unreachable!("handled resource refusal"),
                    IdentityFailure::Host(_) | IdentityFailure::Control(_) => {
                        unreachable!("handled original refusal")
                    }
                })
            }
        }
    }
    pub(crate) fn function(
        &mut self,
        value: &str,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionId, Failure<H>> {
        self.identity(value, work, |value, storage, request, work| {
            FunctionId::try_new_observed(value, storage, request, work)
        })
    }
    pub(crate) fn overload(
        &mut self,
        value: &str,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionOverloadId, Failure<H>> {
        self.identity(value, work, |value, storage, request, work| {
            FunctionOverloadId::try_new_observed(value, storage, request, work)
        })
    }
    pub(crate) fn state(
        &mut self,
        value: &str,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<AggregateStateFormatId, Failure<H>> {
        self.identity(value, work, |value, storage, request, work| {
            AggregateStateFormatId::try_new_observed(value, storage, request, work)
        })
    }
}

#[cfg(test)]
#[path = "creation_tests.rs"]
mod tests;
