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

//! Instance-local typed key equality and storage. Join rows, aggregate state,
//! set markers and distributed hash identities remain with their own authors.

mod canonical_store;
mod compact;
mod export;
mod fixed_store;
mod hash_layout;
#[cfg(test)]
mod hash_layout_public_tests;
mod local_hash;
mod owned_batch;
mod pairs;
mod prepared;
mod read;
mod sizing;
mod statistics;
mod store;
mod table;
#[cfg(test)]
mod tests;

pub(crate) use export::ExportedKeys;
pub(crate) use local_hash::{LocalHashBatch, LocalHashPolicy, hash_batch};
pub(crate) use prepared::{
    IndexPurpose, KeyId, KeyInput, KeySpec, KeyStrategy, NullPolicy, PreparedKeyBatch,
};
pub(crate) use read::{KeyReadColumn, KeyReadPage, KeyReadRow, KeyReadValues, OwnedKeyReadBatch};
pub(crate) use table::{InternBatch, KeyIndex, KeyMatch, NewKey, PresenceBatch, ProbeBatch};

use crate::{
    exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use novarocks_value_arrays::{ValueError, ValueFailure, ValueViolation};
use std::alloc::Layout;

fn invariant(violation: ValueViolation) -> ExecutionFailure {
    ExecutionFailure::from_domain(ValueError::Invariant(violation))
}
fn original<E: Into<ExecutionFailure>>(failure: ValueFailure<E>) -> ExecutionFailure {
    match failure {
        ValueFailure::Value(error) => ExecutionFailure::from_domain(error),
        ValueFailure::Caller(error) => error.into(),
    }
}
fn request<T>(owner: &dyn InputConstructionOwner, count: usize) -> ExecutionResult<()> {
    if count != 0 {
        owner.request(Layout::array::<T>(count).map_err(|_| {
            ConstructionFailure::from(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?)?;
    }
    Ok(())
}
fn vector<T>(owner: &dyn InputConstructionOwner, count: usize) -> ExecutionResult<Vec<T>> {
    request::<T>(owner, count)?;
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|source| {
        ExecutionFailure::from_domain(ValueError::Allocation {
            requested: count.saturating_mul(std::mem::size_of::<T>()),
            source,
        })
    })?;
    Ok(values)
}
fn reserve_push<T>(
    values: &mut Vec<T>,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    let growth =
        novarocks_type_contract::owned_resources::vec::push_growth(values).map_err(|cause| {
            match cause {
                novarocks_type_contract::ControlResourceError::Control(cause) => {
                    ConstructionFailure::Control(cause)
                }
                novarocks_type_contract::ControlResourceError::SourceModel(cause) => {
                    ConstructionFailure::InvalidSource(cause.to_owned())
                }
            }
        })?;
    if let Some(layout) = growth.requested_backing {
        owner.request(layout)?;
        values.try_reserve(1).map_err(|source| {
            ExecutionFailure::from_domain(ValueError::Allocation {
                requested: layout.size(),
                source,
            })
        })?;
    }
    checkpoint()
}
fn shared<T>(owner: &dyn InputConstructionOwner, value: T) -> ExecutionResult<std::sync::Arc<T>> {
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
            .map_err(ConstructionFailure::from)?,
    )?;
    Ok(std::sync::Arc::new(value))
}
fn checkpoint_at(row: usize, checkpoint: &dyn Fn() -> ExecutionResult<()>) -> ExecutionResult<()> {
    if row.is_multiple_of(256) {
        checkpoint()?;
    }
    Ok(())
}
