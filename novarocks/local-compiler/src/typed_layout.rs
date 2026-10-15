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

//! Pure compiler creation of exact execution layouts from explicit type authors.
//! Requests observe representation and original control; they grant no memory.
use arrow_schema::{Field, FieldRef, Schema};
use novarocks_local_program::{
    LayoutCompileError, LayoutCreationFailure, LayoutError, StaticLayout,
};
use novarocks_type_contract::owned_resources::layout::arc_layout;
use novarocks_type_contract::{
    CompileCheckpoints, FunctionValueType, TypedValueBinding, ValueSchemaCreation,
    ValueTypeStorageLifetime,
};
use novarocks_types::SlotId;
use std::{alloc::Layout, sync::Arc};

pub(crate) struct TypedLayoutBuilder {
    count: usize,
    fields: Vec<FieldRef>,
    bindings: Vec<(TypedValueBinding, Option<i32>)>,
    storage: ValueTypeStorageLifetime,
}
fn request<H>(
    layout: Layout,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut ValueSchemaCreation<'_, H>,
) -> Result<(), LayoutCreationFailure<H>> {
    work.flush()?;
    creation
        .request_layout(layout)
        .map_err(LayoutCreationFailure::Creator)
}
fn array<T>(count: usize) -> Result<Layout, LayoutCompileError> {
    Layout::array::<T>(count).map_err(|_| LayoutError::AllocationModel.into())
}
pub(crate) fn copy_name_with_creation<H>(
    name: &str,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut ValueSchemaCreation<'_, H>,
) -> Result<String, LayoutCreationFailure<H>> {
    request(array::<u8>(name.len())?, work, creation)?;
    let mut copied = String::new();
    copied
        .try_reserve_exact(name.len())
        .map_err(|_| LayoutError::AllocationModel)?;
    for ch in name.chars() {
        for _ in 0..ch.len_utf8() {
            work.step()?;
        }
        copied.push(ch);
    }
    Ok(copied)
}
pub(crate) fn with_local_name<T>(
    prefix: &str,
    node: impl std::fmt::Display,
    ordinal: impl std::fmt::Display,
    body: impl FnOnce(&str) -> T,
) -> Result<T, LayoutError> {
    use std::fmt::Write;
    struct Buffer {
        bytes: [u8; 96],
        len: usize,
    }
    impl Write for Buffer {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let end = self
                .len
                .checked_add(text.len())
                .filter(|end| *end <= self.bytes.len())
                .ok_or(std::fmt::Error)?;
            self.bytes[self.len..end].copy_from_slice(text.as_bytes());
            self.len = end;
            Ok(())
        }
    }
    let mut buffer = Buffer {
        bytes: [0; 96],
        len: 0,
    };
    write!(&mut buffer, "{prefix}_{node}_{ordinal}").map_err(|_| LayoutError::AllocationModel)?;
    let name = std::str::from_utf8(&buffer.bytes[..buffer.len]).map_err(|_| LayoutError::Encode)?;
    Ok(body(name))
}

pub(crate) fn local_name_with_creation<H>(
    prefix: &str,
    node: impl std::fmt::Display,
    ordinal: impl std::fmt::Display,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut ValueSchemaCreation<'_, H>,
) -> Result<String, LayoutCreationFailure<H>> {
    work.flush()?;
    with_local_name(prefix, node, ordinal, |name| {
        copy_name_with_creation(name, work, creation)
    })?
}

impl TypedLayoutBuilder {
    pub(crate) fn new_with_creation<H>(
        count: usize,
        work: &mut CompileCheckpoints<'_>,
        creation: &mut ValueSchemaCreation<'_, H>,
    ) -> Result<Self, LayoutCreationFailure<H>> {
        request(array::<FieldRef>(count)?, work, creation)?;
        request(
            array::<(TypedValueBinding, Option<i32>)>(count)?,
            work,
            creation,
        )?;
        let mut fields = Vec::new();
        let mut bindings = Vec::new();
        fields
            .try_reserve_exact(count)
            .map_err(|_| LayoutError::AllocationModel)?;
        bindings
            .try_reserve_exact(count)
            .map_err(|_| LayoutError::AllocationModel)?;
        Ok(Self {
            count,
            fields,
            bindings,
            storage: creation.storage_lifetime(),
        })
    }
    pub(crate) fn push_with_creation<H>(
        &mut self,
        ty: &FunctionValueType,
        name: String,
        work: &mut CompileCheckpoints<'_>,
        creation: &mut ValueSchemaCreation<'_, H>,
    ) -> Result<(), LayoutCreationFailure<H>> {
        self.push_binding_with_creation(ty.typed_binding(), name, work, creation)
    }
    pub(crate) fn push_binding_with_creation<H>(
        &mut self,
        ty: &TypedValueBinding,
        name: String,
        work: &mut CompileCheckpoints<'_>,
        creation: &mut ValueSchemaCreation<'_, H>,
    ) -> Result<(), LayoutCreationFailure<H>> {
        if self.fields.len() >= self.count {
            return Err(LayoutError::ArityMismatch.into());
        }
        for _ in name.as_bytes() {
            work.step()?;
        }
        let binding =
            ty.clone_observed(|| work.step().map_err(LayoutCreationFailure::<H>::from))?;
        // Normalized layout cloning shares child Field/Fields/timezone backing.
        // The binding above retains that backing's original creator lifetime.
        request(arc_layout(Layout::new::<Field>())?, work, creation)?;
        work.step()?;
        self.fields.push(Arc::new(Field::new(
            name,
            ty.layout().data_type().clone(),
            ty.nullable(),
        )));
        self.bindings.push((binding, None));
        Ok(())
    }
    pub(crate) fn finish_with_creation<H>(
        self,
        slots: &[SlotId],
        work: &mut CompileCheckpoints<'_>,
        creation: &mut ValueSchemaCreation<'_, H>,
    ) -> Result<StaticLayout, LayoutCreationFailure<H>> {
        if self.fields.len() != slots.len() {
            return Err(LayoutError::ArityMismatch.into());
        }
        request(
            arc_layout(array::<FieldRef>(self.fields.len())?)?,
            work,
            creation,
        )?;
        request(arc_layout(Layout::new::<Schema>())?, work, creation)?;
        let schema = Arc::new(Schema::new(self.fields));
        request(arc_layout(array::<SlotId>(slots.len())?)?, work, creation)?;
        for _ in slots {
            work.step()?;
        }
        let slots = Arc::from(slots);
        work.flush()?;
        let control = work.control();
        StaticLayout::try_new_typed_with_creator(
            schema,
            slots,
            self.bindings,
            self.storage,
            |layout| {
                creation
                    .request_layout(layout)
                    .map_err(LayoutCreationFailure::Creator)
            },
            control,
        )
    }
}
