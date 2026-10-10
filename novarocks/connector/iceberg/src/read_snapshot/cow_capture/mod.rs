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

//! Prospective request-layout receipts for the actual COW capture pipeline.

pub(crate) mod geometry;
pub(crate) mod metrics;
pub(crate) mod partition;
pub(crate) mod stats;

use geometry::{Failure, add, hash_max, mul, slots};
use novarocks_spi::connector::{
    ConnectorCowBeginCause, ConnectorError, ConnectorErrorKind, ConnectorOriginalResultScope,
};
use std::{
    io::Write,
    mem::size_of,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug)]
pub(crate) enum ReadFailure {
    Semantic(String),
    Original(ConnectorCowBeginCause),
}
impl From<String> for ReadFailure {
    fn from(error: String) -> Self {
        Self::Semantic(error)
    }
}
impl std::fmt::Display for ReadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Semantic(message) => f.write_str(message),
            Self::Original(_) => f.write_str("original COW capture check failed"),
        }
    }
}
impl ReadFailure {
    pub(crate) fn into_cause(self) -> ConnectorCowBeginCause {
        match self {
            Self::Original(cause) => cause,
            Self::Semantic(message) => {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, message).into()
            }
        }
    }
}
pub(crate) fn recipe<T>(
    result: Result<T, Failure<ConnectorCowBeginCause>>,
) -> Result<T, ReadFailure> {
    result.map_err(|error| match error {
        Failure::Original(cause) => ReadFailure::Original(cause),
        Failure::Overflow => {
            ReadFailure::Semantic("COW capture request-layout upper overflow".into())
        }
        Failure::ReceiptExceeded => {
            ReadFailure::Semantic("COW capture exceeded its prospective receipt".into())
        }
    })
}

/// Cumulative constructor requests are a conservative coexistence upper. This
/// never subtracts a branch while any derived file, bucket or output may hold it.
/// The caller supplies the actual already-retained provider/request upper.
pub(crate) struct CaptureBudget<'a> {
    pub(crate) scope: &'a ConnectorOriginalResultScope,
    upper: AtomicU64,
}
impl<'a> CaptureBudget<'a> {
    pub(crate) fn new(
        scope: &'a ConnectorOriginalResultScope,
        base: u64,
    ) -> Result<Self, ReadFailure> {
        scope
            .check_before_growth(base)
            .map_err(|e| ReadFailure::Original(e.into()))?;
        Ok(Self {
            scope,
            upper: AtomicU64::new(base),
        })
    }
    pub(crate) fn upper(&self) -> u64 {
        self.upper.load(Ordering::Relaxed)
    }
    pub(crate) fn active(&self) -> Result<(), ReadFailure> {
        self.scope
            .check_active()
            .map_err(|e| ReadFailure::Original(e.into()))
    }
    pub(crate) fn charge(&self, additional: u64) -> Result<(), ReadFailure> {
        let upper = recipe(add::<ConnectorCowBeginCause>(
            self.upper.load(Ordering::Relaxed),
            additional,
        ))?;
        self.scope
            .check_before_growth(upper)
            .map_err(|e| ReadFailure::Original(e.into()))?;
        self.upper.store(upper, Ordering::Relaxed);
        Ok(())
    }
    pub(crate) fn vector<T>(&self, values: &Vec<T>) -> Result<(), ReadFailure> {
        if values.len() == values.capacity() {
            let slots = values
                .capacity()
                .checked_mul(2)
                .ok_or_else(|| ReadFailure::Semantic("COW vector upper overflow".into()))?
                .max(8);
            self.charge(recipe(geometry::slots::<ConnectorCowBeginCause, T>(slots))?)?;
        }
        self.active()
    }
    pub(crate) fn hash_insert<T>(&self, len: usize, capacity: usize) -> Result<(), ReadFailure> {
        if len == capacity {
            let next = capacity
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .ok_or_else(|| ReadFailure::Semantic("COW hash upper overflow".into()))?;
            self.charge(recipe(hash_max::<ConnectorCowBeginCause, T>(next))?)?;
        }
        self.active()
    }
    pub(crate) fn json<T: serde::Serialize + ?Sized>(&self, value: &T) -> Result<u64, ReadFailure> {
        self.active()?;
        let mut counter = CountWriter {
            budget: self,
            bytes: 0,
            failure: None,
        };
        let result = serde_json::to_writer(&mut counter, value);
        if let Some(failure) = counter.failure {
            return Err(failure);
        }
        result.map_err(|e| ReadFailure::Semantic(e.to_string()))?;
        Ok(counter.bytes)
    }
    /// Every JSON node/key/byte has at least one source byte. Charge one
    /// maximum BTree node, four Value slots (geometric old/new vectors), one
    /// parsed SDK field/index pair, and escaped/string buffers per source byte.
    /// This is pinned layout arithmetic, not an empirically fitted coefficient.
    pub(crate) fn json_working(&self, bytes: u64) -> Result<(), ReadFailure> {
        use crate::iceberg::spec::NestedField;
        let node = recipe(geometry::btree_node::<
            ConnectorCowBeginCause,
            String,
            serde_json::Value,
        >())?;
        let per_byte = recipe(add::<ConnectorCowBeginCause>(
            node,
            (4 * size_of::<serde_json::Value>()
                + 4 * size_of::<NestedField>()
                + 4 * size_of::<(String, std::sync::Arc<NestedField>)>()
                + 16) as u64,
        ))?;
        self.charge(recipe(mul::<ConnectorCowBeginCause>(bytes, per_byte))?)
    }
    /// Iceberg schema decode additionally builds qualified-name maps and
    /// cloned accessor chains. Those can grow faster than the JSON byte count.
    pub(crate) fn schema_indexes(
        &self,
        schema: &crate::iceberg::spec::Schema,
    ) -> Result<(), ReadFailure> {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Type};
        fn field(
            budget: &CaptureBudget<'_>,
            node: &NestedField,
            prefix: u64,
            depth: u64,
        ) -> Result<u64, ReadFailure> {
            budget.active()?;
            let path = recipe(add::<ConnectorCowBeginCause>(
                prefix,
                node.name.len() as u64,
            ))?;
            // Full, short, lowercase and reverse-name maps plus old/new joins.
            let mut total = recipe(mul::<ConnectorCowBeginCause>(path, 8))?;
            let accessor_node = (size_of::<usize>()
                + size_of::<PrimitiveType>()
                + size_of::<Option<Box<usize>>>()
                + 3 * std::mem::align_of::<usize>()) as u64;
            // Each wrap may clone its complete already-built accessor chain.
            let chains = recipe(mul::<ConnectorCowBeginCause>(depth, depth))?;
            total = recipe(add::<ConnectorCowBeginCause>(
                total,
                recipe(mul::<ConnectorCowBeginCause>(
                    chains,
                    recipe(mul::<ConnectorCowBeginCause>(accessor_node, 2))?,
                ))?,
            ))?;
            let child_prefix = recipe(add::<ConnectorCowBeginCause>(path, 1))?;
            let child_depth = recipe(add::<ConnectorCowBeginCause>(depth, 1))?;
            match node.field_type.as_ref() {
                Type::Struct(ty) => {
                    for child in ty.fields() {
                        total = recipe(add::<ConnectorCowBeginCause>(
                            total,
                            field(budget, child, child_prefix, child_depth)?,
                        ))?;
                    }
                }
                Type::List(ty) => {
                    total = recipe(add::<ConnectorCowBeginCause>(
                        total,
                        field(budget, &ty.element_field, child_prefix, child_depth)?,
                    ))?;
                }
                Type::Map(ty) => {
                    total = recipe(add::<ConnectorCowBeginCause>(
                        total,
                        field(budget, &ty.key_field, child_prefix, child_depth)?,
                    ))?;
                    total = recipe(add::<ConnectorCowBeginCause>(
                        total,
                        field(budget, &ty.value_field, child_prefix, child_depth)?,
                    ))?;
                }
                Type::Primitive(_) => (),
            }
            Ok(total)
        }
        let mut total = 0;
        for node in schema.as_struct().fields() {
            total = recipe(add::<ConnectorCowBeginCause>(
                total,
                field(self, node, 0, 1)?,
            ))?;
        }
        self.charge(total)
    }
    pub(crate) fn delete_structures(
        &self,
        manifests: usize,
        members: usize,
    ) -> Result<(), ReadFailure> {
        use crate::delete_semantics::*;
        let mut upper = recipe(hash_max::<ConnectorCowBeginCause, std::sync::Arc<str>>(
            manifests,
        ))?;
        let prefix = members
            .checked_add(1)
            .ok_or_else(|| ReadFailure::Semantic("COW descriptor prefix upper overflow".into()))?;
        for part in [
            hash_max::<ConnectorCowBeginCause, std::sync::Arc<str>>(members),
            hash_max::<ConnectorCowBeginCause, EqualityFieldGroup>(members),
            hash_max::<ConnectorCowBeginCause, (BucketKind, Vec<std::sync::Arc<DeleteFact>>)>(
                members,
            ),
            hash_max::<ConnectorCowBeginCause, (TypedPartition, Vec<std::sync::Arc<FrozenBucket>>)>(
                members,
            ),
            hash_max::<ConnectorCowBeginCause, (TypedPartition, std::sync::Arc<FrozenBucket>)>(
                members,
            ),
            hash_max::<ConnectorCowBeginCause, (std::sync::Arc<str>, std::sync::Arc<FrozenBucket>)>(
                members,
            ),
            hash_max::<ConnectorCowBeginCause, (std::sync::Arc<str>, std::sync::Arc<DeleteFact>)>(
                members,
            ),
            slots::<ConnectorCowBeginCause, DeleteFact>(members),
            slots::<ConnectorCowBeginCause, FrozenBucket>(members),
            slots::<ConnectorCowBeginCause, usize>(prefix),
            slots::<ConnectorCowBeginCause, std::sync::Arc<DeleteFact>>(members),
            slots::<ConnectorCowBeginCause, std::sync::Arc<FrozenBucket>>(members),
            geometry::stable_sort_scratch::<ConnectorCowBeginCause, std::sync::Arc<DeleteFact>>(
                members,
            ),
        ] {
            upper = recipe(add::<ConnectorCowBeginCause>(upper, recipe(part)?))?;
        }
        // Insert-only tables/Vecs may coexist with their previous backing,
        // and each bucket's minimum capacity can be four even for one member.
        self.charge(recipe(mul::<ConnectorCowBeginCause>(upper, 8))?)
    }
}
struct CountWriter<'a, 'b> {
    budget: &'a CaptureBudget<'b>,
    bytes: u64,
    failure: Option<ReadFailure>,
}
impl Write for CountWriter<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Err(failure) = self.budget.active() {
            self.failure = Some(failure);
            return Err(std::io::Error::other("original COW capture stopped"));
        }
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("COW JSON length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn active(budget: Option<&CaptureBudget<'_>>) -> Result<(), ReadFailure> {
    if let Some(budget) = budget {
        budget.active()?;
    }
    Ok(())
}
