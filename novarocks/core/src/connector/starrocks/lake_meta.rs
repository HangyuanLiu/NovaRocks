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

//! Protocol-neutral lake metadata request and materialization facts.
//!
//! The compat decoder constructs these values from StarRocks wire requests;
//! the core lake metadata kernel consumes them without depending on that
//! decoder or generated protocol types.

use std::collections::BTreeMap;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::runtime::query_context::QueryId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LakeMetaColumnKind {
    Dictionary,
    Value(DataType),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakeMetaColumnRequest {
    pub column_id: String,
    pub kind: LakeMetaColumnKind,
}

impl LakeMetaColumnRequest {
    pub fn storage_key(&self) -> String {
        format!("{}:{:?}", self.column_id, self.kind)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakeMetaTabletRequest {
    pub tablet_id: i64,
    pub version: i64,
    pub row_count_hint: Option<i64>,
}

impl LakeMetaTabletRequest {
    pub const fn tablet_id(&self) -> i64 {
        self.tablet_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakeMetaStorageRequest {
    id: u64,
    pub query_id: QueryId,
    pub catalog: String,
    pub db_name: String,
    pub table_name: String,
    pub db_id: i64,
    pub table_id: i64,
    pub schema_id: i64,
    pub tablets: Vec<LakeMetaTabletRequest>,
    pub columns: Vec<LakeMetaColumnRequest>,
}

impl LakeMetaStorageRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_id: QueryId,
        catalog: String,
        db_name: String,
        table_name: String,
        db_id: i64,
        table_id: i64,
        schema_id: i64,
        tablets: Vec<LakeMetaTabletRequest>,
        columns: Vec<LakeMetaColumnRequest>,
    ) -> Self {
        let stable_key = format!(
            "{query_id}:{catalog}:{db_name}:{table_name}:{db_id}:{table_id}:{schema_id}:{tablets:?}:{columns:?}"
        );
        Self {
            id: stable_dependency_id("lake-meta-storage", &stable_key),
            query_id,
            catalog,
            db_name,
            table_name,
            db_id,
            table_id,
            schema_id,
            tablets,
            columns,
        }
    }

    pub const fn table_id(&self) -> i64 {
        self.table_id
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub const fn db_id(&self) -> i64 {
        self.db_id
    }

    pub const fn schema_id(&self) -> i64 {
        self.schema_id
    }

    pub fn tablets(&self) -> &[LakeMetaTabletRequest] {
        &self.tablets
    }

    pub const fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Clone, Debug)]
pub struct LakeMetaStorageFacts {
    pub total_rows: i64,
    pub column_arrays: BTreeMap<String, Vec<ArrayRef>>,
}

impl LakeMetaStorageFacts {
    pub fn new(total_rows: i64, column_arrays: BTreeMap<String, Vec<ArrayRef>>) -> Self {
        Self {
            total_rows,
            column_arrays,
        }
    }

    pub const fn total_rows(&self) -> i64 {
        self.total_rows
    }
}

fn stable_dependency_id(kind: &str, key: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in kind.bytes().chain([0]).chain(key.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
