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

use std::{alloc::Layout, cmp::Ordering, sync::Arc};

use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError,
    owned_resources::{copy, layout, vec as owned_vec},
};

use super::{AdmissionRefusal, Failure, LogicalTypeCodecError};

pub(crate) struct Context<'a, 'control, H> {
    pub(crate) request: &'a mut dyn FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    pub(crate) work: &'a mut CompileCheckpoints<'control>,
    pub(crate) observe_scope: Option<&'a mut dyn FnMut(Layout) -> Result<(), Failure<H>>>,
}
pub(crate) fn invalid<H>(message: &'static str) -> Failure<H> {
    LogicalTypeCodecError::InvalidShape(message).into()
}
pub(crate) fn arithmetic<H>() -> Failure<H> {
    CompileControlError::ResourceExhausted.into()
}
pub(crate) fn source_add<H>(total: &mut usize, bytes: usize) -> Result<(), Failure<H>> {
    *total = total.checked_add(bytes).ok_or_else(arithmetic)?;
    Ok(())
}
pub(crate) fn array<T, H>(count: usize) -> Result<Layout, Failure<H>> {
    Layout::array::<T>(count).map_err(|_| arithmetic())
}
pub(crate) fn arc<H>(payload: Layout) -> Result<Layout, Failure<H>> {
    layout::arc_layout(payload).map_err(layout_error)
}
pub(crate) fn layout_error<H>(error: layout::LayoutResourceError) -> Failure<H> {
    match error {
        layout::LayoutResourceError::SourceModel => {
            LogicalTypeCodecError::ResourceSource("logical type Arc source model drift").into()
        }
        _ => arithmetic(),
    }
}
pub(crate) fn admit<H>(
    request: &mut dyn FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    allocation: Layout,
) -> Result<(), Failure<H>> {
    if allocation.size() == 0 {
        return Ok(());
    }
    request(allocation).map_err(|error| match error {
        AdmissionRefusal::Control(error) => error.into(),
        AdmissionRefusal::Host(error) => Failure::Host(error),
    })
}
impl<H> Context<'_, '_, H> {
    pub(crate) fn step(&mut self) -> Result<(), Failure<H>> {
        self.work.step().map_err(Into::into)
    }
    pub(crate) fn allocate(&mut self, allocation: Layout) -> Result<(), Failure<H>> {
        if allocation.size() != 0
            && let Some(observe) = self.observe_scope.as_deref_mut()
        {
            observe(allocation)?;
        }
        admit(self.request, allocation)
    }
    pub(crate) fn vector<T>(&mut self, count: usize) -> Result<Vec<T>, Failure<H>> {
        let mut output = Vec::new();
        if count != 0 {
            self.allocate(array::<T, H>(count)?)?;
            self.work.flush()?;
            let result = output.try_reserve_exact(count);
            if result.is_ok() {
                self.step()?;
            }
            copy::reserve_exit::<Failure<H>>(result, self.work)?;
        }
        Ok(output)
    }
    pub(crate) fn boxed<T>(&mut self, source: Vec<T>) -> Result<Box<[T]>, Failure<H>> {
        self.boxed_with_source_facts(source).map(|(value, _)| value)
    }
    pub(crate) fn boxed_with_source_facts<T>(
        &mut self,
        source: Vec<T>,
    ) -> Result<(Box<[T]>, usize), Failure<H>> {
        let final_backing = array::<T, H>(source.len())?.size();
        let request = &mut self.request;
        let observer = &mut self.observe_scope;
        owned_vec::boxed_slice_in(
            source,
            &mut |facts| {
                if let Some(allocation) = facts.requested_backing {
                    if let Some(observe) = observer.as_deref_mut() {
                        observe(allocation)?;
                    }
                    admit(*request, allocation)?;
                }
                Ok::<_, Failure<H>>(())
            },
            self.work,
        )
        .map(|value| (value, final_backing))
    }
    pub(crate) fn string(&mut self, source: &str) -> Result<String, Failure<H>> {
        self.string_with_source_facts(source)
            .map(|(value, _)| value)
    }
    pub(crate) fn string_with_source_facts(
        &mut self,
        source: &str,
    ) -> Result<(String, usize), Failure<H>> {
        let backing = array::<u8, H>(source.len())?;
        self.allocate(backing)?;
        let value = copy::copy_string::<Failure<H>>(source, self.work)?;
        // The final String retains its actual public capacity. This is source
        // geometry, not a second construction request or capacity grant.
        let retained = array::<u8, H>(value.capacity())?.size();
        Ok((value, retained))
    }
    pub(crate) fn boxed_name(&mut self, source: &str) -> Result<Box<str>, Failure<H>> {
        self.boxed_name_with_source_facts(source)
            .map(|(value, _)| value)
    }
    pub(crate) fn boxed_name_with_source_facts(
        &mut self,
        source: &str,
    ) -> Result<(Box<str>, usize), Failure<H>> {
        let name = self.string(source)?;
        // String's actual public length/capacity define the same original
        // Vec::shrink_to_fit operation used by String::into_boxed_str.
        if name.capacity() > name.len() {
            self.allocate(array::<u8, H>(name.len())?)?;
        }
        self.work.flush()?;
        let retained = array::<u8, H>(name.len())?.size();
        let output = name.into_boxed_str();
        self.step()?;
        self.work.flush()?;
        Ok((output, retained))
    }
    pub(crate) fn timezone(&mut self, source: &str) -> Result<Arc<str>, Failure<H>> {
        self.timezone_with_source_facts(source)
            .map(|(value, _)| value)
    }
    pub(crate) fn timezone_with_source_facts(
        &mut self,
        source: &str,
    ) -> Result<(Arc<str>, usize), Failure<H>> {
        let spelling = self.string(source)?;
        let retained = arc(array::<u8, H>(spelling.len())?)?;
        self.allocate(retained)?;
        self.work.flush()?;
        let output = Arc::<str>::from(spelling.as_str());
        self.step()?;
        self.work.flush()?;
        Ok((output, retained.size()))
    }
}

/// In-place, interruptible sorting. No comparator hides a refusal or continues
/// with fabricated Ordering after one. Scratch size does not depend on IDs.
pub(crate) fn sort_by_id<T, H>(
    values: &mut [T],
    key: impl Fn(&T) -> u32 + Copy,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    sort_by_id_work_observed(values, key, &mut || context.step())
}

/// The same interruptible sort for a borrowed scratch index whose actual
/// allocation belongs to its enclosing constructor. Sorting requests nothing.
pub(crate) fn sort_by_id_work_observed<T, E>(
    values: &mut [T],
    key: impl Fn(&T) -> u32 + Copy,
    step: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    fn sift<T, E>(
        values: &mut [T],
        mut root: usize,
        end: usize,
        key: impl Fn(&T) -> u32 + Copy,
        step: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        while root < end / 2 {
            let mut child = root * 2 + 1;
            if child + 1 < end {
                step()?;
                if key(&values[child]) < key(&values[child + 1]) {
                    child += 1;
                }
            }
            step()?;
            if key(&values[root]) >= key(&values[child]) {
                break;
            }
            values.swap(root, child);
            step()?;
            root = child;
        }
        Ok(())
    }
    let end = values.len();
    for root in (0..end / 2).rev() {
        sift(values, root, end, key, step)?;
    }
    for end in (1..end).rev() {
        values.swap(0, end);
        step()?;
        sift(values, 0, end, key, step)?;
    }
    Ok(())
}

pub(crate) fn find<T>(
    values: &[T],
    id: u32,
    key: impl Fn(&T) -> u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<usize>, CompileControlError> {
    let (mut low, mut high) = (0, values.len());
    while low < high {
        let middle = low + (high - low) / 2;
        work.step()?;
        match key(&values[middle]).cmp(&id) {
            Ordering::Equal => return Ok(Some(middle)),
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
        }
    }
    Ok(None)
}

/// A compile-time choice for the existing unscoped adapter. This uninhabited
/// type cannot supply an owner, scope, request callback or successful grant.
pub(crate) enum NoFactory {}
impl<H> novarocks_type_contract::ValueSchemaCreationScope<H> for NoFactory {
    fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        match *self {}
    }
    fn request_layout(&mut self, _: Layout) -> Result<(), H> {
        match *self {}
    }
    fn observe_opaque_upper_bound(&mut self, _: usize) -> Result<(), H> {
        match *self {}
    }
}
impl<H> novarocks_type_contract::ValueSchemaCreationFactory<H> for NoFactory {
    type Scope<'a> = NoFactory;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, H> {
        match *self {}
    }
    fn begin_scope_observed<O>(
        &self,
        _: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, novarocks_type_contract::ValueSchemaScopeBeginFailure<O, H>> {
        match *self {}
    }
}

/// Redirect only actual construction requests and storage. The same mutable
/// checkpoint stream and the caller's already prepared whole-graph limits are
/// borrowed throughout nested entity lifetimes.
pub(crate) fn with_entity<H, F, T>(
    factory: Option<&F>,
    original_storage: &novarocks_type_contract::ValueTypeStorageLifetime,
    context: &mut Context<'_, '_, H>,
    body: impl FnOnce(
        novarocks_type_contract::ValueTypeStorageLifetime,
        &mut Context<'_, '_, H>,
    ) -> Result<T, Failure<H>>,
) -> Result<T, Failure<H>>
where
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
{
    with_entity_core(factory, original_storage, context, false, body).map(|(value, _)| value)
}

/// Carry only the actual scope holder geometry from its constructor. Entity
/// payload contributions are published separately by their sole authors.
pub(crate) fn with_entity_source_facts<H, F, T>(
    factory: Option<&F>,
    original_storage: &novarocks_type_contract::ValueTypeStorageLifetime,
    context: &mut Context<'_, '_, H>,
    body: impl FnOnce(
        novarocks_type_contract::ValueTypeStorageLifetime,
        &mut Context<'_, '_, H>,
    ) -> Result<T, Failure<H>>,
) -> Result<(T, usize), Failure<H>>
where
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
{
    with_entity_core(factory, original_storage, context, true, body)
}

pub(crate) fn with_entity_core<H, F, T>(
    factory: Option<&F>,
    original_storage: &novarocks_type_contract::ValueTypeStorageLifetime,
    context: &mut Context<'_, '_, H>,
    source_facts: bool,
    body: impl FnOnce(
        novarocks_type_contract::ValueTypeStorageLifetime,
        &mut Context<'_, '_, H>,
    ) -> Result<T, Failure<H>>,
) -> Result<(T, usize), Failure<H>>
where
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
{
    use novarocks_type_contract::ValueSchemaCreationScope;
    match factory {
        Some(factory) => {
            let mut scope_bytes = 0usize;
            let mut scope = if source_facts || context.observe_scope.is_some() {
                let mut observe = |allocation: Layout| {
                    if let Some(original) = context.observe_scope.as_deref_mut() {
                        original(allocation)?;
                    }
                    if source_facts {
                        source_add(&mut scope_bytes, allocation.size())?;
                    }
                    Ok::<_, Failure<H>>(())
                };
                factory
                    .begin_scope_observed(&mut observe)
                    .map_err(|error| match error {
                        novarocks_type_contract::ValueSchemaScopeBeginFailure::Observer(error) => {
                            error
                        }
                        novarocks_type_contract::ValueSchemaScopeBeginFailure::Creator(error) => {
                            Failure::Host(error)
                        }
                    })?
            } else {
                factory.begin_scope().map_err(Failure::Host)?
            };
            let storage = scope.storage_lifetime();
            let mut request = |layout| scope.request_layout(layout).map_err(AdmissionRefusal::Host);
            match &mut context.observe_scope {
                Some(original) => {
                    // Delegate through this local borrow instead of putting
                    // the invariant outer trait object in a shorter Context.
                    let mut observe = |layout| original(layout);
                    let mut entity = Context {
                        request: &mut request,
                        work: &mut *context.work,
                        observe_scope: Some(&mut observe),
                    };
                    body(storage, &mut entity).map(|value| (value, scope_bytes))
                }
                None => {
                    let mut entity = Context {
                        request: &mut request,
                        work: &mut *context.work,
                        observe_scope: None,
                    };
                    body(storage, &mut entity).map(|value| (value, scope_bytes))
                }
            }
        }
        None => body(original_storage.clone(), context).map(|value| (value, 0)),
    }
}
