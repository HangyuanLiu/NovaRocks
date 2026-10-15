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

//! Borrowed creation requests for one caller-owned type/schema construction.
//! Exact public Layout requests and locked-library opaque byte upper bounds
//! remain separate observations. This port never creates an account or grant;
//! control remains on the caller's independent CompileCheckpoints.

use std::alloc::Layout;

use crate::ValueTypeStorageLifetime;

pub struct ValueSchemaCreation<'a, H> {
    storage: ValueTypeStorageLifetime,
    request: &'a mut dyn FnMut(Layout) -> Result<(), H>,
    opaque: &'a mut dyn FnMut(usize) -> Result<(), H>,
}
impl<'a, H> ValueSchemaCreation<'a, H> {
    pub fn new(
        storage: ValueTypeStorageLifetime,
        request: &'a mut dyn FnMut(Layout) -> Result<(), H>,
        opaque: &'a mut dyn FnMut(usize) -> Result<(), H>,
    ) -> Self {
        Self {
            storage,
            request,
            opaque,
        }
    }
    /// Borrow both original request channels independently for one operation.
    /// This preserves their nominal errors and allocates no side port or guard.
    pub fn with_request_ports<R>(
        &mut self,
        body: impl FnOnce(
            &mut dyn FnMut(Layout) -> Result<(), H>,
            &mut dyn FnMut(usize) -> Result<(), H>,
        ) -> R,
    ) -> R {
        body(&mut *self.request, &mut *self.opaque)
    }

    /// Reborrow the same caller ports for one operation with a different
    /// nominal error envelope. No callback borrow can escape this stack scope.
    pub fn with_error_mapping<E, R>(
        &mut self,
        map: impl Fn(H) -> E,
        body: impl FnOnce(&mut ValueSchemaCreation<'_, E>) -> R,
    ) -> R {
        let request = &mut self.request;
        let opaque = &mut self.opaque;
        let mut request = |layout| request(layout).map_err(&map);
        let mut opaque = |bytes| opaque(bytes).map_err(&map);
        let mut mapped = ValueSchemaCreation::new(self.storage.clone(), &mut request, &mut opaque);
        body(&mut mapped)
    }
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
    pub fn request_layout(&mut self, layout: Layout) -> Result<(), H> {
        (self.request)(layout)
    }
    pub fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), H> {
        (self.opaque)(bytes)
    }
}

/// One actual construction lifetime supplied by an existing caller owner.
/// Control and complete-operation bounds remain on the independent caller.
pub trait ValueSchemaCreationScope<H> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime;
    fn request_layout(&mut self, layout: Layout) -> Result<(), H>;
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), H>;
}

/// A pure model observer and the actual construction owner retain their
/// original, distinct errors. Neither failure invokes a later callback.
#[derive(Debug)]
pub enum ValueSchemaScopeBeginFailure<O, H> {
    Observer(O),
    Creator(H),
}

// Design: ADR-0176 (docs/adr/ADR-0176-recursive-execution-types-and-independent-storage-ownership.md)
/// Opens independent entity or temporary-operation lifetimes. A pure caller
/// supplies no factory; it does not implement a successful no-op admission.
pub trait ValueSchemaCreationFactory<H> {
    type Scope<'a>: ValueSchemaCreationScope<H>
    where
        Self: 'a;

    fn begin_scope(&self) -> Result<Self::Scope<'_>, H>;

    /// Observe the actual scope author's private-holder geometry before its
    /// allocation. This callback is a pure model/check gate, never a grant.
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, ValueSchemaScopeBeginFailure<O, H>>;
}

/// Reborrow the original stack ports through the existing scope contract.
/// This creates no scope, authority, allocation or lifetime holder.
impl<H> ValueSchemaCreationScope<H> for ValueSchemaCreation<'_, H> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        ValueSchemaCreation::storage_lifetime(self)
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), H> {
        ValueSchemaCreation::request_layout(self, layout)
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), H> {
        ValueSchemaCreation::observe_opaque_upper_bound(self, bytes)
    }
}
