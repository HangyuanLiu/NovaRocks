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

//! Prospective copy terms for the closed COW schema/overlay conversion.
//!
//! These terms are allocation requests, not allocator/RSS measurements. They
//! cover named current copy sites plus the actual overlay closure allocation.
//! Generic query binding maps and optimizer graphs remain planning Work.

use arrow::datatypes::{DataType, Schema};
use novarocks_spi::connector::ConnectorPinnedFileSet;
use novarocks_types::schema::ColumnDef;
use std::mem::{align_of, size_of};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FootprintError {
    Overflow,
    NotSingleFileCow,
    IncompleteIdentity,
}
type Result<T> = std::result::Result<T, FootprintError>;
fn size<T>() -> Result<u64> {
    u64::try_from(size_of::<T>()).map_err(|_| FootprintError::Overflow)
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(FootprintError::Overflow)
}
fn mul(a: u64, b: usize) -> Result<u64> {
    a.checked_mul(u64::try_from(b).map_err(|_| FootprintError::Overflow)?)
        .ok_or(FootprintError::Overflow)
}
fn bytes(value: &str) -> Result<u64> {
    u64::try_from(value.len()).map_err(|_| FootprintError::Overflow)
}

// DataType derives Clone. FieldRef, Fields, UnionFields and timezone Arc<str>
// remain shared. Dictionary's two Box<DataType> children are the owned case.
// Do not use DataType::size(), which recursively counts shared pointees.
fn data_type_clone_requests(value: &DataType) -> Result<u64> {
    match value {
        DataType::Dictionary(key, value) => add(
            mul(size::<DataType>()?, 2)?,
            add(
                data_type_clone_requests(key)?,
                data_type_clone_requests(value)?,
            )?,
        ),
        _ => Ok(0),
    }
}

pub(crate) fn pinned_file_set_clone_requests(value: &ConnectorPinnedFileSet) -> Result<u64> {
    if value.files().len() != 1 {
        return Err(FootprintError::NotSingleFileCow);
    }
    // Arc<str> pointees and relation names remain under the original source.
    mul(size::<Arc<str>>()?, value.files().len())
}

// All-target admission does not allocate a UUID/identity to measure it.
// The closed caller freezes exactly this prefix + 32 ASCII UUID hex digits.
#[derive(Clone, Copy)]
struct IdentityLengths {
    catalog: u64,
    namespace: u64,
    table: u64,
}
impl IdentityLengths {
    fn closed_cow(namespace: &str) -> Result<Self> {
        if namespace.is_empty() {
            return Err(FootprintError::IncompleteIdentity);
        }
        Ok(Self {
            catalog: bytes("default_catalog")?,
            namespace: bytes(namespace)?,
            table: add(bytes("__nr_cow_")?, 32)?,
        })
    }
    fn total(self) -> Result<u64> {
        add(add(self.catalog, self.namespace)?, self.table)
    }
}

// Exact recipe for pinned_file_set_resolved_analyzer_table -> from_planner:
// two columns Vecs; two name/type copies; TableDef.name; one SQL scan identity;
// one CatalogTable identity. Empty hidden/UKFK Vecs allocate nothing. Schema is
// consumed only to borrow fields and never cloned into this SQL materialization.
fn synthetic_table_requests(identity: IdentityLengths, schema: &Schema) -> Result<u64> {
    let mut total = mul(size::<ColumnDef>()?, schema.fields().len())?;
    for field in schema.fields() {
        total = add(
            total,
            add(
                bytes(field.name())?,
                data_type_clone_requests(field.data_type())?,
            )?,
        )?;
    }
    total = mul(total, 2)?;
    add(total, add(mul(identity.total()?, 2)?, identity.table)?)
}

// Each numbered event is a concrete current production clone site. This is
// intentionally not a tunable multiplier or a canonical wire-byte allowance.
pub(crate) struct KnownCompilerCopies {
    pub(crate) read_binding_headers: u64,
    pub(crate) read_binding_identity_and_key: u64,
    pub(crate) overlay_index_and_lookup_strings: u64,
    pub(crate) overlay_capture_headers: u64,
    pub(crate) provider_freeze_headers: u64,
    pub(crate) factory_retry_headers: u64,
    pub(crate) original_overlay_strings: u64,
    pub(crate) compiler_overlay_strings: u64,
    pub(crate) resolver_overlay_strings: u64,
    pub(crate) initial_sql_table_heap: u64,
    pub(crate) analyzer_sql_table_heap: u64,
}
impl KnownCompilerCopies {
    pub(crate) fn before_closed_identity_growth(
        original_namespace: &str,
        schema: &Schema,
        pinned: &ConnectorPinnedFileSet,
    ) -> Result<Self> {
        Self::for_lengths(
            IdentityLengths::closed_cow(original_namespace)?,
            schema,
            pinned,
        )
    }
    fn for_lengths(
        identity: IdentityLengths,
        schema: &Schema,
        pinned: &ConnectorPinnedFileSet,
    ) -> Result<Self> {
        let headers = pinned_file_set_clone_requests(pinned)?;
        // namespace/table fields + QueryTableBindingKey's three strings;
        // compiler clone shares materialize Arc, not the captured read graph.
        let pair = add(identity.namespace, identity.table)?;
        let names = identity.total()?;
        let overlay = add(pair, names)?;
        let sql = synthetic_table_requests(identity, schema)?;
        Ok(Self {
            read_binding_headers: headers,
            read_binding_identity_and_key: mul(names, 2)?,
            overlay_index_and_lookup_strings: mul(pair, 2)?,
            overlay_capture_headers: headers,
            provider_freeze_headers: headers,
            // Pre-admission installs the key before analysis, so factory normally
            // is memoized out. Reserve one first materialization, never unbounded
            // calls: resolve_or_insert_with_id is the original at-most-once owner.
            factory_retry_headers: headers,
            original_overlay_strings: add(overlay, names)?,
            compiler_overlay_strings: overlay,
            resolver_overlay_strings: overlay,
            initial_sql_table_heap: sql,
            analyzer_sql_table_heap: sql,
        })
    }
    pub(crate) fn simultaneous_request_terms(&self) -> Result<u64> {
        let mut total = 0;
        for value in [
            self.read_binding_headers,
            self.read_binding_identity_and_key,
            self.overlay_index_and_lookup_strings,
            self.overlay_capture_headers,
            self.provider_freeze_headers,
            self.factory_retry_headers,
            self.original_overlay_strings,
            self.compiler_overlay_strings,
            self.resolver_overlay_strings,
            self.initial_sql_table_heap,
            self.analyzer_sql_table_heap,
        ] {
            total = add(total, value)?;
        }
        Ok(total)
    }
}

// Private owner constructors can use this before Arc::new of their named
// holder or before Arc::new(factory). T is the actual sized closure value;
// move captures create no additional heap before this check.
pub(crate) fn prospective_arc_request<T>() -> Result<u64> {
    // ArcInner<T>: two usize counters, alignment padding, then actual T.
    // Conservative padding is derived from align_of<T>, never a byte cap.
    let head = mul(size::<usize>()?, 2)?;
    let field_padding = u64::try_from(align_of::<T>() - 1).map_err(|_| FootprintError::Overflow)?;
    let tail_padding = u64::try_from(align_of::<T>().max(align_of::<usize>()) - 1)
        .map_err(|_| FootprintError::Overflow)?;
    add(add(add(head, field_padding)?, size::<T>()?)?, tail_padding)
}
