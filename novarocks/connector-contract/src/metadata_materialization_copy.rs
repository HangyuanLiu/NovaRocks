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

//! Optional positive receipts emitted by the existing owned schema visitor.
use crate::owned_copy::OwnedCopy;
use novarocks_type_contract::owned_resources::metadata_materialization::{
    SchemaMetadataMaterializations, SharedMaterializedField, SharedMaterializedSchema,
    MetadataFieldLoan,
};
use std::{alloc::Layout, collections::TryReserveError};

pub(crate) struct MetadataMaterializationCopy<'a, O> {
    inner: &'a mut O,
    count: usize,
    fields: Option<Vec<MetadataFieldLoan>>,
    schema: Option<SharedMaterializedSchema>,
}
impl<'a, O: OwnedCopy> MetadataMaterializationCopy<'a, O> {
    pub(crate) fn new(inner: &'a mut O) -> Self {
        Self {
            inner,
            count: 0,
            fields: None,
            schema: None,
        }
    }
    pub(crate) fn finish(mut self) -> Result<SchemaMetadataMaterializations, O::Error> {
        let schema = self.schema.take().ok_or_else(|| self.inner.arithmetic())?;
        let fields = self.finish_fields()?;
        Ok(SchemaMetadataMaterializations::from_materialized_owners(
            schema, fields,
        ))
    }
    pub(crate) fn finish_namespace(mut self) -> Result<novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace, O::Error>{
        let fields = self.finish_fields()?;
        Ok(novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace::from_original_loans(fields))
    }
    fn finish_fields(&mut self) -> Result<std::sync::Arc<[MetadataFieldLoan]>, O::Error> {
        let fields = self.fields.take().ok_or_else(|| self.inner.arithmetic())?;
        if fields.len() != self.count {
            return Err(self.inner.arithmetic());
        }
        self.inner.flush()?;
        let fields = fields.into_boxed_slice();
        self.inner.step()?;
        self.inner.flush()?;
        let fields = fields.into();
        self.inner.step()?;
        self.inner.flush()?;
        Ok(fields)
    }
}
impl<O: OwnedCopy> OwnedCopy for MetadataMaterializationCopy<'_, O> {
    type Error = O::Error;
    fn materializes(&self) -> bool {
        self.inner.materializes()
    }
    fn source_invoice(&self) -> Option<usize> {
        self.inner.source_invoice()
    }
    fn request(&mut self, layout: Layout, copies: usize) -> Result<(), Self::Error> {
        self.inner.request(layout, copies)
    }
    fn work(&mut self, units: usize) -> Result<(), Self::Error> {
        self.inner.work(units)
    }
    fn source_floor(&mut self, bytes: usize) -> Result<(), Self::Error> {
        self.inner.source_floor(bytes)
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        self.inner.step()
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }
    fn arithmetic(&self) -> Self::Error {
        self.inner.arithmetic()
    }
    fn reserve_exit(&mut self, result: Result<(), TryReserveError>) -> Result<(), Self::Error> {
        self.inner.reserve_exit(result)
    }
    fn spelling(&mut self, value: &str) -> Result<String, Self::Error> {
        self.inner.spelling(value)
    }
    fn prepare_field_materialization(&mut self) -> Result<(), Self::Error> {
        if !self.materializes() {
            self.count = self.inner.add(self.count, 1)?;
        }
        Ok(())
    }
    fn begin_copy(&mut self) -> Result<(), Self::Error> {
        // Both Vec storage and final Arc slice are admitted by the SAME
        // preparation context before the record vector is actually allocated.
        self.inner.array::<MetadataFieldLoan>(self.count, 2)?;
        self.inner.arc_slice::<MetadataFieldLoan>(self.count)?;
        self.inner.work(
            self.inner
                .mul(self.count, 2 * size_of::<MetadataFieldLoan>())?,
        )?;
        self.inner.begin_copy()?;
        self.inner.flush()?;
        let mut fields = Vec::new();
        let reservation = fields.try_reserve_exact(self.count);
        self.inner.reserve_exit(reservation)?;
        self.fields = Some(fields);
        Ok(())
    }
    fn materialized_field(&mut self, field: &SharedMaterializedField) -> Result<(), Self::Error> {
        let fields = self
            .fields
            .as_mut()
            .ok_or_else(|| self.inner.arithmetic())?;
        if fields.len() >= self.count {
            return Err(self.inner.arithmetic());
        }
        fields.push(field.loan());
        self.inner.step()
    }
    fn materialized_schema(
        &mut self,
        schema: &SharedMaterializedSchema,
    ) -> Result<(), Self::Error> {
        self.schema = Some(schema.clone());
        self.inner.step()
    }
}
