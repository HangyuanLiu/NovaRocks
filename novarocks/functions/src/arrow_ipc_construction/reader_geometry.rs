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

//! Pinned Arrow58.4 fb_to_schema construction events, after the shared
//! borrowed field verifier. Temporary constructor backing and actual source
//! header backing use separate caller scopes. These are operation requests,
//! not exact retained-byte measurements or unique allocator facts.
use arrow_ipc as ipc;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use novarocks_type_contract::{
    ControlResourceError,
    owned_resources::{
        hashmap::{HashMapResourceError, original_fresh_unique_insert_tables_observed},
        layout::{LayoutResourceError, arc_layout},
        vec::{original_fresh_push_allocation_requests_observed, push_growth},
    },
};
use std::alloc::Layout;

#[derive(Debug)]
pub enum ReaderStructureFailure<H> {
    Creator(H),
    Layout(LayoutResourceError),
    Vector(ControlResourceError),
    Map(HashMapResourceError),
    SourceModel(&'static str),
}
impl<H> From<ControlResourceError> for ReaderStructureFailure<H> {
    fn from(e: ControlResourceError) -> Self {
        Self::Vector(e)
    }
}
impl<H> From<HashMapResourceError> for ReaderStructureFailure<H> {
    fn from(e: HashMapResourceError) -> Self {
        Self::Map(e)
    }
}
impl<H> From<LayoutResourceError> for ReaderStructureFailure<H> {
    fn from(e: LayoutResourceError) -> Self {
        Self::Layout(e)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReaderStructureClass {
    SourceHeader,
    Temporary,
}

/// Supplied by the actual caller, not retained by any decoded type. The model
/// chooses the constructor class; callers cannot sign payload coverage with it.
pub struct ReaderStructureObserver<'a, H> {
    pub request: &'a mut dyn FnMut(Layout, ReaderStructureClass) -> Result<(), H>,
    pub opaque: &'a mut dyn FnMut(usize, ReaderStructureClass) -> Result<(), H>,
    pub work: &'a mut dyn FnMut() -> Result<(), H>,
}
impl<H> ReaderStructureObserver<'_, H> {
    fn step(&mut self) -> Result<(), ReaderStructureFailure<H>> {
        (self.work)().map_err(ReaderStructureFailure::Creator)
    }
    fn layout(
        &mut self,
        layout: Layout,
        class: ReaderStructureClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        (self.request)(layout, class).map_err(ReaderStructureFailure::Creator)
    }
    fn array<T>(
        &mut self,
        count: usize,
        class: ReaderStructureClass,
    ) -> Result<(), ReaderStructureFailure<H>> {
        self.layout(
            Layout::array::<T>(count).map_err(|_| LayoutResourceError::Arithmetic)?,
            class,
        )
    }
    fn shared<T>(&mut self) -> Result<(), ReaderStructureFailure<H>> {
        self.layout(
            arc_layout(Layout::new::<T>())?,
            ReaderStructureClass::SourceHeader,
        )
    }
    fn shared_slice<T>(&mut self, count: usize) -> Result<(), ReaderStructureFailure<H>> {
        self.layout(
            arc_layout(Layout::array::<T>(count).map_err(|_| LayoutResourceError::Arithmetic)?)?,
            ReaderStructureClass::SourceHeader,
        )
    }
    fn fresh_pushes<T>(&mut self, count: usize) -> Result<(), ReaderStructureFailure<H>> {
        let request = &mut *self.request;
        let work = &mut *self.work;
        let mut loan = |layout, copies| {
            for _ in 0..copies {
                request(layout, ReaderStructureClass::Temporary)
                    .map_err(ReaderStructureFailure::Creator)?;
            }
            Ok(())
        };
        original_fresh_push_allocation_requests_observed::<T, ReaderStructureFailure<H>>(
            count,
            &mut || work().map_err(ReaderStructureFailure::Creator),
            &mut Some(&mut loan),
        )?;
        Ok(())
    }
    fn known_nontrusted_collect<T>(
        &mut self,
        count: usize,
    ) -> Result<(), ReaderStructureFailure<H>> {
        if count == 0 {
            return Ok(());
        }
        // Rust1.98 SpecFromIterNested: after first element, exact size_hint
        // yields count, maxed with the sole RawVec minimum. No pushes grow it.
        let empty = Vec::<T>::new();
        let minimum = push_growth(&empty)?.requested_capacity;
        self.array::<T>(count.max(minimum), ReaderStructureClass::Temporary)
    }
    fn metadata(
        &mut self,
        entries: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<ipc::KeyValue<'_>>>>,
    ) -> Result<(), ReaderStructureFailure<H>> {
        let Some(entries) = entries else {
            return Ok(());
        };
        // A real borrowed association scratch author avoids a quadratic source
        // scan. This temporary map creates no String and never escapes this
        // metadata callback; its exact constructor is admitted before use.
        let table = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
            &str,
            (usize, usize),
        >(entries.len())?;
        (self.opaque)(
            table.request_bytes_upper_bound,
            ReaderStructureClass::Temporary,
        )
        .map_err(ReaderStructureFailure::Creator)?;
        self.step()?;
        let mut associations =
            std::collections::HashMap::<&str, (usize, usize)>::with_capacity(entries.len());
        self.step()?;
        for index in 0..entries.len() {
            let entry = entries.get(index);
            let (Some(key), Some(_)) = (entry.key(), entry.value()) else {
                self.step()?;
                continue;
            };
            self.step()?;
            associations
                .entry(key)
                .and_modify(|positions| positions.1 = index)
                .or_insert((index, index));
            self.step()?;
        }
        for index in 0..entries.len() {
            let entry = entries.get(index);
            let (Some(key), Some(value)) = (entry.key(), entry.value()) else {
                self.step()?;
                continue;
            };
            self.step()?;
            let (first, last) =
                *associations
                    .get(key)
                    .ok_or(ReaderStructureFailure::SourceModel(
                        "original metadata association missing",
                    ))?;
            self.step()?;
            // std insert retains the first equal key and last replacement
            // value. Every to_string is real; overwritten strings are scratch.
            self.array::<u8>(
                key.len(),
                if index == first {
                    ReaderStructureClass::SourceHeader
                } else {
                    ReaderStructureClass::Temporary
                },
            )?;
            self.array::<u8>(
                value.len(),
                if index == last {
                    ReaderStructureClass::SourceHeader
                } else {
                    ReaderStructureClass::Temporary
                },
            )?;
        }
        let unique = associations.len();
        let opaque = &mut *self.opaque;
        let work = &mut *self.work;
        original_fresh_unique_insert_tables_observed::<String, String, ReaderStructureFailure<H>>(
            unique,
            || work().map_err(ReaderStructureFailure::Creator),
            |table, last| {
                opaque(
                    table.request_bytes_upper_bound,
                    if last {
                        ReaderStructureClass::SourceHeader
                    } else {
                        ReaderStructureClass::Temporary
                    },
                )
                .map_err(ReaderStructureFailure::Creator)
            },
        )
    }
    /// Call once after the sole schema's profile/field count has been checked.
    pub fn schema(&mut self, source: ipc::Schema<'_>) -> Result<(), ReaderStructureFailure<H>> {
        if !novarocks_type_contract::owned_resources::profile::LOCKED_TOOLCHAIN {
            return Err(ReaderStructureFailure::SourceModel(
                "IPC reader toolchain source model drift",
            ));
        }
        let fields = source.fields().ok_or(ReaderStructureFailure::SourceModel(
            "IPC reader schema fields missing",
        ))?;
        self.fresh_pushes::<Field>(fields.len())?;
        // Vec<Field>::IntoIter.map(Arc::new) is TrustedLen; Arc FromIterator
        // allocates the final slice directly, without a temporary FieldRef Vec.
        self.shared_slice::<FieldRef>(fields.len())?;
        self.metadata(source.custom_metadata())?;
        self.shared::<Schema>()
    }
    /// Size-only projection from the writer's real Arrow schema. It shares
    /// constructor heads/type shapes with the borrowed IPC reader author and
    /// allocates no replacement schema, field, string or logical type.
    pub fn schema_from_arrow(&mut self, schema: &Schema) -> Result<(), ReaderStructureFailure<H>> {
        if !novarocks_type_contract::owned_resources::profile::LOCKED_TOOLCHAIN {
            return Err(ReaderStructureFailure::SourceModel(
                "IPC reader toolchain source model drift",
            ));
        }
        for field in schema.fields() {
            self.field_from_arrow(field)?;
        }
        self.fresh_pushes::<Field>(schema.fields().len())?;
        self.shared_slice::<FieldRef>(schema.fields().len())?;
        self.metadata_from_arrow(schema.metadata())?;
        self.shared::<Schema>()
    }
    fn metadata_from_arrow(
        &mut self,
        entries: &std::collections::HashMap<String, String>,
    ) -> Result<(), ReaderStructureFailure<H>> {
        // StreamWriter serializes this actual unique-key map. Receiver's
        // association scratch and final String map use their original authors.
        let table = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
            &str,
            (usize, usize),
        >(entries.len())?;
        (self.opaque)(
            table.request_bytes_upper_bound,
            ReaderStructureClass::Temporary,
        )
        .map_err(ReaderStructureFailure::Creator)?;
        for (key, value) in entries {
            self.step()?;
            self.array::<u8>(key.len(), ReaderStructureClass::SourceHeader)?;
            self.array::<u8>(value.len(), ReaderStructureClass::SourceHeader)?;
        }
        let opaque = &mut *self.opaque;
        let work = &mut *self.work;
        original_fresh_unique_insert_tables_observed::<String, String, ReaderStructureFailure<H>>(
            entries.len(),
            || work().map_err(ReaderStructureFailure::Creator),
            |table, last| {
                opaque(
                    table.request_bytes_upper_bound,
                    if last {
                        ReaderStructureClass::SourceHeader
                    } else {
                        ReaderStructureClass::Temporary
                    },
                )
                .map_err(ReaderStructureFailure::Creator)
            },
        )
    }
    fn field_from_arrow(&mut self, field: &Field) -> Result<(), ReaderStructureFailure<H>> {
        self.field_head(field.name().len())?;
        self.metadata_from_arrow(field.metadata())?;
        let dictionary = matches!(field.data_type(), DataType::Dictionary(..));
        let ty = if let DataType::Dictionary(_, value) = field.data_type() {
            value.as_ref()
        } else {
            field.data_type()
        };
        let shape = match ty {
            DataType::Struct(fields) => ReaderFieldShape::Struct(fields.len()),
            DataType::Timestamp(_, zone) => {
                ReaderFieldShape::Timestamp(zone.as_ref().map(|zone| zone.len()))
            }
            DataType::Union(fields, _) => ReaderFieldShape::Union(fields.len()),
            _ => ReaderFieldShape::Plain,
        };
        self.field_type(dictionary, shape)?;
        match ty {
            DataType::Struct(fields) => {
                for field in fields {
                    self.field_from_arrow(field)?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    self.field_from_arrow(field)?;
                }
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => self.field_from_arrow(field)?,
            DataType::RunEndEncoded(ends, values) => {
                self.field_from_arrow(ends)?;
                self.field_from_arrow(values)?;
            }
            _ => {}
        }
        Ok(())
    }
    fn field_head(&mut self, name_bytes: usize) -> Result<(), ReaderStructureFailure<H>> {
        self.array::<u8>(name_bytes, ReaderStructureClass::SourceHeader)?;
        self.shared::<Field>()
    }
    fn field_type(
        &mut self,
        dictionary: bool,
        shape: ReaderFieldShape,
    ) -> Result<(), ReaderStructureFailure<H>> {
        if dictionary {
            self.layout(
                Layout::new::<DataType>(),
                ReaderStructureClass::SourceHeader,
            )?;
            self.layout(
                Layout::new::<DataType>(),
                ReaderStructureClass::SourceHeader,
            )?;
        }
        match shape {
            ReaderFieldShape::Struct(children) => {
                self.known_nontrusted_collect::<FieldRef>(children)?;
                self.shared_slice::<FieldRef>(children)?;
            }
            ReaderFieldShape::Timestamp(Some(bytes)) => self.shared_slice::<u8>(bytes)?,
            ReaderFieldShape::Union(children) => {
                self.fresh_pushes::<Field>(children)?;
                self.fresh_pushes::<(i8, FieldRef)>(children)?;
                self.shared_slice::<(i8, FieldRef)>(children)?;
            }
            _ => {}
        }
        Ok(())
    }
    /// Constructor facts only. The source must first pass the shared full
    /// verifier; this callback does not interpret metadata as value semantics.
    pub fn field(&mut self, source: ipc::Field<'_>) -> Result<(), ReaderStructureFailure<H>> {
        self.field_head(source.name().unwrap_or_default().len())?;
        self.metadata(source.custom_metadata())?;
        let children = source.children().map_or(0, |children| children.len());
        let shape = match source.type_type() {
            ipc::Type::Struct_ => ReaderFieldShape::Struct(children),
            ipc::Type::Timestamp => {
                let timestamp =
                    source
                        .type_as_timestamp()
                        .ok_or(ReaderStructureFailure::SourceModel(
                            "unverified timestamp constructor",
                        ))?;
                ReaderFieldShape::Timestamp(timestamp.timezone().map(str::len))
            }
            ipc::Type::Union => {
                let union = source
                    .type_as_union()
                    .ok_or(ReaderStructureFailure::SourceModel(
                        "unverified union constructor",
                    ))?;
                if union.typeIds().is_none() {
                    return Err(ReaderStructureFailure::SourceModel(
                        "IPC union implicit IDs are outside the verified source author",
                    ));
                }
                ReaderFieldShape::Union(children)
            }
            _ => ReaderFieldShape::Plain,
        };
        self.field_type(source.dictionary().is_some(), shape)
    }
}

enum ReaderFieldShape {
    Plain,
    Struct(usize),
    Timestamp(Option<usize>),
    Union(usize),
}

#[cfg(test)]
mod network_projection_tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use std::{cell::Cell, io::Cursor, sync::Arc};
    #[test]
    fn writer_schema_projection_covers_the_actual_reader_schema_round_trip() {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true)
                    .with_metadata([("field-owner".to_owned(), "original".to_owned())].into()),
            ],
            [("schema-owner".to_owned(), "original".to_owned())].into(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();
        let mut bytes = Vec::new();
        {
            let mut writer =
                arrow_ipc::writer::StreamWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let mut reader =
            arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes.as_slice()), None).unwrap();
        assert_eq!(
            reader.next().unwrap().unwrap().schema().as_ref(),
            schema.as_ref()
        );
        let arrow_source = Cell::new(0usize);
        let arrow_temporary = Cell::new(0usize);
        let mut request = |layout: Layout, class| {
            let target = if class == ReaderStructureClass::SourceHeader {
                &arrow_source
            } else {
                &arrow_temporary
            };
            target.set(target.get() + layout.size());
            Ok::<_, std::convert::Infallible>(())
        };
        let mut opaque = |bytes, class| {
            let target = if class == ReaderStructureClass::SourceHeader {
                &arrow_source
            } else {
                &arrow_temporary
            };
            target.set(target.get() + bytes);
            Ok::<_, std::convert::Infallible>(())
        };
        let mut work = || Ok::<_, std::convert::Infallible>(());
        ReaderStructureObserver {
            request: &mut request,
            opaque: &mut opaque,
            work: &mut work,
        }
        .schema_from_arrow(schema.as_ref())
        .unwrap();
        let ipc_source = Cell::new(0usize);
        let ipc_temporary = Cell::new(0usize);
        let mut request = |layout: Layout, class| {
            let target = if class == ReaderStructureClass::SourceHeader {
                &ipc_source
            } else {
                &ipc_temporary
            };
            target.set(target.get() + layout.size());
            Ok::<_, std::convert::Infallible>(())
        };
        let mut opaque = |bytes, class| {
            let target = if class == ReaderStructureClass::SourceHeader {
                &ipc_source
            } else {
                &ipc_temporary
            };
            target.set(target.get() + bytes);
            Ok::<_, std::convert::Infallible>(())
        };
        let mut work = || Ok::<_, std::convert::Infallible>(());
        let mut observer = ReaderStructureObserver {
            request: &mut request,
            opaque: &mut opaque,
            work: &mut work,
        };
        let metadata_len = i32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let message = arrow_ipc::root_as_message(&bytes[8..8 + metadata_len]).unwrap();
        let actual = message.header_as_schema().unwrap();
        for field in actual.fields().unwrap().iter() {
            observer.field(field).unwrap();
        }
        observer.schema(actual).unwrap();
        assert_eq!(arrow_source.get(), ipc_source.get());
        assert_eq!(arrow_temporary.get(), ipc_temporary.get());
    }
}
