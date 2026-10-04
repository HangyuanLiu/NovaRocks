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

//! Authored logical aggregate channels, independent of runtime merge state.

use super::SqlFunctionBinding;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Origin {
    LogicalUpdate,
    Uncertified,
}

/// SQL source authors mint the logical origin at an actual update definition.
/// Optimizer bridges and clones preserve it; materialization never infers it
/// from a compatible signature, output column identity, or merge-state type.
/// The owned channels remain canonical until final request capture. They have
/// no runtime capabilities and do not retain a compile control or grant.
#[derive(Clone, Debug)]
pub(crate) struct AggregateArgumentSource<A, O> {
    origin: Origin,
    arguments: Vec<A>,
    order_by: Vec<O>,
    binding: SqlFunctionBinding,
}

impl<A, O> AggregateArgumentSource<A, O> {
    /// For genuine SQL update authors after coercion/rebinding. Merge authors
    /// transfer this existing source instead of calling this with state slots.
    pub(crate) fn logical_update(
        arguments: Vec<A>,
        order_by: Vec<O>,
        binding: SqlFunctionBinding,
    ) -> Self {
        Self {
            origin: Origin::LogicalUpdate,
            arguments,
            order_by,
            binding,
        }
    }

    /// Explicit legacy/manual facts without a logical-source handoff. Existing
    /// structural consumers may inspect them; fresh specialization must refuse.
    pub(crate) fn uncertified(
        arguments: Vec<A>,
        order_by: Vec<O>,
        binding: SqlFunctionBinding,
    ) -> Self {
        Self {
            origin: Origin::Uncertified,
            arguments,
            order_by,
            binding,
        }
    }

    pub(crate) fn arguments(&self) -> &[A] {
        &self.arguments
    }
    pub(crate) fn order_by(&self) -> &[O] {
        &self.order_by
    }
    pub(crate) fn binding(&self) -> &SqlFunctionBinding {
        &self.binding
    }

    pub(crate) fn logical_parts(&self) -> Option<(&[A], &[O], &SqlFunctionBinding)> {
        match self.origin {
            Origin::LogicalUpdate => Some((&self.arguments, &self.order_by, &self.binding)),
            Origin::Uncertified => None,
        }
    }

    /// Existing exact IR projection owns observation, allocation admission and
    /// ordinary/control tails. This move preserves source origin and the same
    /// binding handle; neither callback can mint a missing update origin.
    pub(crate) fn try_map_parts<B, P, E>(
        &self,
        arguments: impl FnOnce(&[A]) -> Result<Vec<B>, E>,
        order_by: impl FnOnce(&[O]) -> Result<Vec<P>, E>,
    ) -> Result<AggregateArgumentSource<B, P>, E> {
        Ok(AggregateArgumentSource {
            origin: self.origin,
            arguments: arguments(&self.arguments)?,
            order_by: order_by(&self.order_by)?,
            binding: self.binding.clone(),
        })
    }

    /// Only an already-authorized logical rewrite may change these canonical
    /// channels. Its original caller owns legality and control observation.
    /// Rewriting uncertified facts never upgrades their origin.
    pub(crate) fn rewrite_channels<R>(
        &mut self,
        rewrite: impl FnOnce(&mut Vec<A>, &mut Vec<O>) -> R,
    ) -> R {
        rewrite(&mut self.arguments, &mut self.order_by)
    }
}
