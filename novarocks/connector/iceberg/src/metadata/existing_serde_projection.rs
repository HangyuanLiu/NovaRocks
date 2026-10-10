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

// Checked Nova output projection, preserving the existing Iceberg serializer.
// Original borrowed Iceberg Serialize is deliberately retained (D17).
use crate::iceberg::spec::{NestedField, Schema, TableMetadata, Type};
use crate::scan_model::{IcebergSchemaDef, IcebergSchemaFieldDef};
use novarocks_spi::connector::{ConnectorError, ConnectorPayloadRetentionGuard};
use serde::Serialize;
use std::{io, mem::size_of};
#[path = "raw_defaults.rs"]
mod raw_defaults;

pub(crate) enum Error<E = ConnectorError> {
    Control(E),
    Json(serde_json::Error),
    Overflow,
    ShapeChanged,
}
impl<E> std::fmt::Debug for Error<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Control(_) => "Control",
            Self::Json(_) => "Json",
            Self::Overflow => "Overflow",
            Self::ShapeChanged => "ShapeChanged",
        })
    }
}
impl<E> std::fmt::Display for Error<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
fn add<E>(a: u64, b: u64) -> Result<u64, Error<E>> {
    a.checked_add(b).ok_or(Error::Overflow)
}
fn mul<E>(a: u64, b: u64) -> Result<u64, Error<E>> {
    a.checked_mul(b).ok_or(Error::Overflow)
}

// This only forwards the ORIGINAL window checker, deadline/stop and holder.
// No default implementation, capacity acquisition, deadline or new work scope.
pub(crate) trait OriginalScope<E = ConnectorError> {
    fn check_active(&self) -> Result<(), E>;
    fn check_total(&self, whole_live_upper: u64) -> Result<(), E>;
    fn original_guard(&self) -> ConnectorPayloadRetentionGuard;
}

// Library errors are retained as objects; no arbitrary Display into a String.
// io::Error::from(kind) is allocation-free. Original control error stays here.
struct Writer<'a, E> {
    scope: &'a dyn OriginalScope<E>,
    original: Option<E>,
    length: u64,
    output: Option<&'a mut Vec<u8>>,
    ceiling: u64,
    changed: bool,
}
impl<E> io::Write for Writer<'_, E> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.original.is_some() {
            return Err(io::ErrorKind::Other.into());
        }
        if let Err(error) = self.scope.check_active() {
            self.original = Some(error);
            // write_all retries Interrupted. Keep the first cause and exit.
            return Err(io::ErrorKind::Other.into());
        }
        let Some(next) = self.length.checked_add(bytes.len() as u64) else {
            self.changed = true;
            return Err(io::ErrorKind::InvalidData.into());
        };
        if next > self.ceiling {
            self.changed = true;
            return Err(io::ErrorKind::InvalidData.into());
        }
        if let Some(output) = &mut self.output {
            output.extend_from_slice(bytes);
        }
        self.length = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn serialize<T: Serialize + ?Sized, E>(
    value: &T,
    scope: &dyn OriginalScope<E>,
    output: Option<&mut Vec<u8>>,
    ceiling: u64,
) -> Result<u64, Error<E>> {
    scope.check_active().map_err(Error::Control)?;
    let mut writer = Writer {
        scope,
        original: None,
        length: 0,
        output,
        ceiling,
        changed: false,
    };
    let result = serde_json::to_writer(&mut writer, value);
    if let Some(error) = writer.original {
        return Err(Error::Control(error));
    }
    if writer.changed {
        return Err(Error::ShapeChanged);
    }
    result.map_err(Error::Json)?;
    scope.check_active().map_err(Error::Control)?;
    Ok(writer.length)
}
pub(crate) fn count<T: Serialize + ?Sized, E>(
    value: &T,
    scope: &dyn OriginalScope<E>,
) -> Result<u64, Error<E>> {
    serialize(value, scope, None, u64::MAX)
}
pub(crate) fn exact_json<T: Serialize + ?Sized, E>(
    value: &T,
    length: u64,
    scope: &dyn OriginalScope<E>,
) -> Result<String, Error<E>> {
    let length = usize::try_from(length).map_err(|_| Error::Overflow)?;
    let mut bytes = Vec::with_capacity(length);
    if bytes.capacity() != length {
        return Err(Error::ShapeChanged);
    }
    let actual = serialize(value, scope, Some(&mut bytes), length as u64)?;
    if actual != length as u64 {
        return Err(Error::ShapeChanged);
    }
    // Serializer produces UTF-8; conversion transfers this SAME allocation.
    String::from_utf8(bytes).map_err(|_| Error::ShapeChanged)
}
fn exact_string<E>(text: &str) -> Result<String, Error<E>> {
    let mut output = String::with_capacity(text.len());
    if output.capacity() != text.len() {
        return Err(Error::ShapeChanged);
    }
    output.push_str(text);
    Ok(output)
}
fn children(ty: &Type) -> &[std::sync::Arc<NestedField>] {
    // Struct has a native slice. List/Map are handled separately below.
    match ty {
        Type::Struct(ty) => ty.fields(),
        _ => &[],
    }
}
fn child_count(ty: &Type) -> usize {
    match ty {
        Type::Struct(ty) => ty.fields().len(),
        Type::List(_) => 1,
        Type::Map(_) => 2,
        Type::Primitive(_) => 0,
    }
}
fn for_children<E>(
    ty: &Type,
    mut visit: impl FnMut(&NestedField) -> Result<(), Error<E>>,
) -> Result<(), Error<E>> {
    match ty {
        Type::Struct(_) => {
            for field in children(ty) {
                visit(field)?;
            }
        }
        Type::List(ty) => visit(&ty.element_field)?,
        Type::Map(ty) => {
            visit(&ty.key_field)?;
            visit(&ty.value_field)?;
        }
        Type::Primitive(_) => {}
    }
    Ok(())
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Shape {
    pub metadata_json: u64,
    pub schema_owned_upper: u64,
    pub maximum_field_temporary: u64,
}
impl Shape {
    pub fn owned_upper<E>(self) -> Result<u64, Error<E>> {
        add(
            add(self.metadata_json, self.schema_owned_upper)?,
            (size_of::<OwnedProjection>() - size_of::<IcebergSchemaDef>()) as u64,
        )
    }
    // Field JSON N, RawValue nesting scratch old+new <= 3N+16. Default
    // substring copies are already counted in the COMPLETE schema graph.
    fn field_temporary<E>(n: u64) -> Result<u64, Error<E>> {
        add(mul(n, 4)?, 16)
    }
}
fn inspect_field<E>(
    field: &NestedField,
    scope: &dyn OriginalScope<E>,
    graph: &mut u64,
    temporary: &mut u64,
) -> Result<(), Error<E>> {
    scope.check_active().map_err(Error::Control)?;
    // No Nova Literal.clone / Value graph. Public Serialize's internal child
    // clones are the narrow D17 library invocation, measured by its owner.
    let field_json = count(field, scope)?;
    *graph = add(*graph, field.name.len() as u64)?;
    // Both raw default slices are DISJOINT members of this exact field JSON.
    *graph = add(*graph, field_json)?;
    *graph = add(
        *graph,
        mul(
            child_count(&field.field_type) as u64,
            size_of::<IcebergSchemaFieldDef>() as u64,
        )?,
    )?;
    *temporary = (*temporary).max(Shape::field_temporary(field_json)?);
    for_children(&field.field_type, |child| {
        inspect_field(child, scope, graph, temporary)
    })
}

pub(crate) struct Plan<'a> {
    metadata: &'a TableMetadata,
    schema: &'a Schema,
    shape: Shape,
}
impl<'a> Plan<'a> {
    pub fn inspect<E>(
        metadata: &'a TableMetadata,
        snapshot_schema: &'a Schema,
        scope: &dyn OriginalScope<E>,
    ) -> Result<Self, Error<E>> {
        let metadata_json = count(metadata, scope)?;
        let fields = snapshot_schema.as_struct().fields();
        let mut graph = add(
            size_of::<IcebergSchemaDef>() as u64,
            mul(
                fields.len() as u64,
                size_of::<IcebergSchemaFieldDef>() as u64,
            )?,
        )?;
        let mut temporary = 0;
        for field in fields {
            inspect_field(field, scope, &mut graph, &mut temporary)?;
        }
        Ok(Self {
            metadata,
            schema: snapshot_schema,
            shape: Shape {
                metadata_json,
                schema_owned_upper: graph,
                maximum_field_temporary: temporary,
            },
        })
    }
    pub fn shape(&self) -> Shape {
        self.shape
    }
    // Caller supplies truthful coexisting backing and ALL other new own
    // allocation recipes, not a wire cap. No zero fallback exists here.
    // Invoke BEFORE the first branch's output/schema/new DTO allocation.
    pub fn authorize<E>(
        self,
        copies: usize,
        existing_upper: u64,
        other_new_upper: u64,
        scope: &'a dyn OriginalScope<E>,
    ) -> Result<Checked<'a, E>, Error<E>> {
        let owned = mul(copies as u64, self.shape.owned_upper()?)?;
        let total = add(
            add(add(existing_upper, other_new_upper)?, owned)?,
            self.shape.maximum_field_temporary,
        )?;
        scope.check_active().map_err(Error::Control)?;
        scope.check_total(total).map_err(Error::Control)?;
        Ok(Checked {
            plan: self,
            scope,
            remaining: copies,
        })
    }
}
pub(crate) struct OwnedProjection {
    metadata_json: String,
    schema: IcebergSchemaDef,
    _guard: ConnectorPayloadRetentionGuard,
}
impl OwnedProjection {
    pub fn metadata_json(&self) -> &str {
        &self.metadata_json
    }
    pub fn schema(&self) -> &IcebergSchemaDef {
        &self.schema
    }
    pub(crate) fn into_guarded_parts(
        self,
    ) -> (String, IcebergSchemaDef, ConnectorPayloadRetentionGuard) {
        (self.metadata_json, self.schema, self._guard)
    }
}
pub(crate) struct Checked<'a, E> {
    plan: Plan<'a>,
    scope: &'a dyn OriginalScope<E>,
    remaining: usize,
}
impl<E> Checked<'_, E> {
    pub fn build_one(&mut self) -> Result<OwnedProjection, Error<E>> {
        self.scope.check_active().map_err(Error::Control)?;
        if self.remaining == 0 {
            return Err(Error::ShapeChanged);
        }
        let guard = self.scope.original_guard(); // Before any own output growth.
        let metadata_json = exact_json(
            self.plan.metadata,
            self.plan.shape.metadata_json,
            self.scope,
        )?;
        let fields = self.plan.schema.as_struct().fields();
        let mut output = Vec::with_capacity(fields.len());
        if output.capacity() != fields.len() {
            return Err(Error::ShapeChanged);
        }
        for field in fields {
            output.push(build_field(field, self.scope)?);
        }
        self.scope.check_active().map_err(Error::Control)?;
        self.remaining -= 1;
        Ok(OwnedProjection {
            metadata_json,
            schema: IcebergSchemaDef { fields: output },
            _guard: guard,
        })
    }
}
fn build_field<E>(
    field: &NestedField,
    scope: &dyn OriginalScope<E>,
) -> Result<IcebergSchemaFieldDef, Error<E>> {
    scope.check_active().map_err(Error::Control)?;
    // Recount without any output graph; there is no per-field recipe Vec.
    let length = count(field, scope)?;
    let (initial_default_json, write_default_json) = {
        let text = exact_json(field, length, scope)?;
        let raw: &serde_json::value::RawValue = serde_json::from_str(&text).map_err(Error::Json)?;
        let defaults = raw_defaults::defaults(raw).map_err(Error::Json)?;
        let initial = defaults
            .initial
            .map(|value| exact_string(value.get()))
            .transpose()?;
        let write = defaults
            .write
            .map(|value| exact_string(value.get()))
            .transpose()?;
        (initial, write)
        // text and parser scratch exit BEFORE child recursion.
    };
    let mut output = Vec::with_capacity(child_count(&field.field_type));
    if output.capacity() != child_count(&field.field_type) {
        return Err(Error::ShapeChanged);
    }
    for_children(&field.field_type, |child| {
        output.push(build_field(child, scope)?);
        Ok(())
    })?;
    Ok(IcebergSchemaFieldDef {
        field_id: field.id,
        name: exact_string(&field.name)?,
        // These two are serde-skipped and no production COW default reader
        // consumes them. Canonical typed JSON defaults are retained verbatim.
        initial_default: None,
        write_default: None,
        initial_default_json,
        write_default_json,
        children: output,
    })
}
#[cfg(test)]
#[path = "projection_tests.rs"]
mod tests;

// Schema-only specialization of the SAME checked field codec. It does not
// serialize the whole metadata, create a Literal clone, or construct a Value.
pub(crate) struct SchemaOnlyPlan<'a> {
    schema: &'a Schema,
    graph_with_inline_root: u64,
    maximum_field_temporary: u64,
}
impl<'a> SchemaOnlyPlan<'a> {
    pub(crate) fn inspect<E>(
        schema: &'a Schema,
        scope: &dyn OriginalScope<E>,
    ) -> Result<Self, Error<E>> {
        let fields = schema.as_struct().fields();
        let mut graph = add(
            size_of::<IcebergSchemaDef>() as u64,
            mul(
                fields.len() as u64,
                size_of::<IcebergSchemaFieldDef>() as u64,
            )?,
        )?;
        let mut temporary = 0;
        for field in fields {
            inspect_field(field, scope, &mut graph, &mut temporary)?;
        }
        Ok(Self {
            schema,
            graph_with_inline_root: graph,
            maximum_field_temporary: temporary,
        })
    }
    pub(crate) fn retained_upper<E>(&self) -> Result<u64, Error<E>> {
        use crate::commit::write_stack::domain::IcebergCowSchemaBacking;
        // Arc allocation: two atomic usize counts, padded data, trailing pad.
        // SchemaDef inline lives IN the backing; never charged twice.
        let a = std::mem::align_of::<IcebergCowSchemaBacking>().max(std::mem::align_of::<usize>())
            as u64;
        let header = add(2 * size_of::<usize>() as u64, a - 1)? / a * a;
        let layout = add(header, size_of::<IcebergCowSchemaBacking>() as u64)?;
        let layout = add(layout, a - 1)? / a * a;
        add(
            layout,
            self.graph_with_inline_root - size_of::<IcebergSchemaDef>() as u64,
        )
    }
    pub(crate) fn authorize<E>(
        self,
        existing_and_other_new_upper: u64,
        scope: &'a dyn OriginalScope<E>,
    ) -> Result<CheckedSchemaOnly<'a, E>, Error<E>> {
        let total = add(
            add(existing_and_other_new_upper, self.retained_upper()?)?,
            self.maximum_field_temporary,
        )?;
        scope.check_active().map_err(Error::Control)?;
        scope.check_total(total).map_err(Error::Control)?;
        Ok(CheckedSchemaOnly { plan: self, scope })
    }
}
pub(crate) struct CheckedSchemaOnly<'a, E> {
    plan: SchemaOnlyPlan<'a>,
    scope: &'a dyn OriginalScope<E>,
}
impl<E> CheckedSchemaOnly<'_, E> {
    pub(crate) fn build(
        self,
    ) -> Result<std::sync::Arc<crate::commit::write_stack::domain::IcebergCowSchemaBacking>, Error<E>>
    {
        self.scope.check_active().map_err(Error::Control)?;
        let guard = self.scope.original_guard(); // Same holder BEFORE schema growth.
        let fields = self.plan.schema.as_struct().fields();
        let mut output = Vec::with_capacity(fields.len());
        if output.capacity() != fields.len() {
            return Err(Error::ShapeChanged);
        }
        for field in fields {
            output.push(build_field(field, self.scope)?);
        }
        self.scope.check_active().map_err(Error::Control)?;
        let backing = crate::commit::write_stack::domain::IcebergCowSchemaBacking::from_checked(
            IcebergSchemaDef { fields: output },
            guard,
        );
        if let Err(original) = self.scope.check_active() {
            drop(backing);
            return Err(Error::Control(original));
        }
        Ok(backing)
    }
}
#[cfg(test)]
#[path = "schema_only_tests.rs"]
mod schema_only_tests;
