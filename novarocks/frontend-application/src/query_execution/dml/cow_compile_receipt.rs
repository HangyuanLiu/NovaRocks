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

//! Prospective geometry for the closed result-derived COW syntax conversion.
//! This is a numeric proof over the original source and sealed route. It
//! grants no capacity and does not cover general optimizer exploration.

pub(super) mod borrowed_value_footprint;
pub(crate) mod compiler_handoff;
pub(super) mod deep_closed_query;
pub(super) mod new_input_footprint;
pub(super) mod recipe;
pub(super) mod route_skeleton;
pub(super) mod skeleton_shape;
pub(super) mod type_footprint;

use super::cow_closed_ast::{BuildError, Result};
use novarocks_spi::connector::{
    ConnectorWriteInputShape, write_stack::ConnectorWriteRewriteSource,
};
use novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity;
use std::mem::size_of;

/// This counts the new target container and its actual owned input clone.
/// Query's root is already charged by the closed syntax recipe. The identity
/// and one pinned-file Vec move into FrozenRead; shared Arc pointees are not
/// cloned. Generic SQL materializer graphs remain under their planning owner.
pub(super) fn target_metadata_upper<T>(
    input: &ConnectorWriteInputShape,
    namespace: &str,
    rewrite: Option<&ConnectorWriteRewriteSource>,
) -> Result<u64> {
    let root = size_of::<T>()
        .checked_sub(size_of::<novarocks_parser::ast::Query>())
        .ok_or(BuildError::ResourceExhausted)? as u64;
    let mut upper = root
        .checked_add(
            new_input_footprint::new_input_clone_heap_upper(input).map_err(BuildError::Control)?,
        )
        .ok_or(BuildError::ResourceExhausted)?;
    if let Some(source) = rewrite {
        if source.pinned_source().files().len() != 1 {
            return Err(BuildError::InvalidSource(
                "COW rewrite branch must replace exactly one data file",
            ));
        }
        let identity_bytes = "default_catalog"
            .len()
            .checked_add(namespace.len())
            .and_then(|n| n.checked_add("__nr_cow_".len()))
            .and_then(|n| n.checked_add(uuid::fmt::Simple::LENGTH))
            .ok_or(BuildError::ResourceExhausted)? as u64;
        upper = upper
            .checked_add(identity_bytes)
            .and_then(|n| n.checked_add(size_of::<std::sync::Arc<str>>() as u64))
            .ok_or(BuildError::ResourceExhausted)?;
    }
    Ok(upper)
}

/// Mint only the query-local name after its prospective bytes were checked.
/// All Strings request their exact copied length; formatting allocates no
/// intermediate String. This identity does not authorize provider access.
pub(super) fn closed_identity(namespace: &str) -> Result<FrozenConnectorScanIdentity> {
    fn copy(value: &str) -> Result<String> {
        let mut output = String::new();
        output
            .try_reserve_exact(value.len())
            .map_err(BuildError::Allocation)?;
        if output.capacity() != value.len() {
            return Err(BuildError::ResourceExhausted);
        }
        output.push_str(value);
        Ok(output)
    }
    let uuid = uuid::Uuid::new_v4();
    let mut encoded = [0; uuid::fmt::Simple::LENGTH];
    let suffix = uuid.simple().encode_lower(&mut encoded);
    let mut name = String::new();
    let length = "__nr_cow_"
        .len()
        .checked_add(suffix.len())
        .ok_or(BuildError::ResourceExhausted)?;
    name.try_reserve_exact(length)
        .map_err(BuildError::Allocation)?;
    if name.capacity() != length {
        return Err(BuildError::ResourceExhausted);
    }
    name.push_str("__nr_cow_");
    name.push_str(suffix);
    Ok(FrozenConnectorScanIdentity::new(
        copy("default_catalog")?,
        copy(namespace)?,
        name,
    ))
}
