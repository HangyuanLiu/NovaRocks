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

//! Sole source-order borrowed definition visitor, shared with package encoding.
//! It owns no decoder, binding resolver, runtime capability or capacity grant.
use crate as p;
use novarocks_type_contract::CompileCheckpoints;

#[derive(Clone, Copy, Debug)]
pub enum FragmentDefinitionSource<'source> {
    Value {
        id: p::ValueId,
        value: &'source p::ValueDef,
    },
    Expression {
        id: p::ExprId,
        expression: &'source p::ExprNode,
    },
    Request {
        definition: p::PhysicalCallDefinition,
        request: &'source p::PhysicalCallRequest,
    },
}

/// Observe all stored fragment definitions on the caller's original meter.
/// Captures include dead and TypeOnly definitions, in original source order.
/// Before capture, the caller admits its actual lookup/copy/encoding work and
/// backing requests. Original source admission and completed observations are
/// the callback's responsibility; this port adds no successful callback step.
/// No entry/footer, resource grant, package validation, lookup or owner prepare
/// occurs here. The first callback/control error stops the original traversal.
/// Constants/cuts and relational binding lifecycles remain their own authors;
/// request rows preserve relational definition keys without rebuilding them.
pub fn visit_fragment_definitions_observed<'source, E>(
    fragment: &'source p::Fragment,
    work: &mut CompileCheckpoints<'_>,
    capture: impl FnMut(FragmentDefinitionSource<'source>, &mut CompileCheckpoints<'_>) -> Result<(), E>,
) -> Result<(), E> {
    visit_fragment_definitions(fragment, work, capture)
}

/// Single source-order author. The package callbacks keep their original
/// header admission, CountPass/FillPass operations and completed checkpoints.
/// The public observation wrapper delegates without adding another step.
fn visit_fragment_definitions<'source, E>(
    fragment: &'source p::Fragment,
    work: &mut CompileCheckpoints<'_>,
    mut capture: impl FnMut(
        FragmentDefinitionSource<'source>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>,
) -> Result<(), E> {
    for (id, value) in fragment.values() {
        capture(FragmentDefinitionSource::Value { id: *id, value }, work)?;
    }
    for (id, expression) in fragment.expressions().iter() {
        capture(
            FragmentDefinitionSource::Expression {
                id: *id,
                expression,
            },
            work,
        )?;
    }
    for (definition, request) in fragment.call_requests().entries() {
        capture(
            FragmentDefinitionSource::Request {
                definition: *definition,
                request,
            },
            work,
        )?;
    }
    Ok(())
}
