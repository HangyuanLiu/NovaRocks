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

//! Shared final attachment sequence for actual compiled expression roots.
//!
//! Caller-owned lowering supplies actual graph positions, occurrences, frozen
//! prepared calls and complete types. This body neither selects functions nor
//! invents root roles, source declarations, resource policy or creation grants.

use crate::{
    LocalProgramGraph, ProgramCallSite, ProgramChannelSite, ProgramChannelTypeError,
    ProgramControlFlow, ProgramExpressionArena, ProgramExpressionTypeError, ProgramResolvedCalls,
    ProgramResolvedCallsError, ProgramRootBindingError, ProgramRootControlBindings,
    ProgramRootUseBinding, ProgramTypedChannels, ProgramTypedExpressions,
};
use novarocks_functions::{FunctionArgumentType, PureCallSpecialization};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, FunctionValueType};
use std::{collections::BTreeMap, fmt};

/// The original failing constructor and cause remain available at the caller.
#[derive(Debug)]
pub enum RootAttachmentError {
    Control(CompileControlError),
    Roots(ProgramRootBindingError),
    Calls(ProgramResolvedCallsError),
    Expressions(ProgramExpressionTypeError),
    Channels(ProgramChannelTypeError),
}
impl fmt::Display for RootAttachmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Roots(e) => e.fmt(f),
            Self::Calls(e) => e.fmt(f),
            Self::Expressions(e) => e.fmt(f),
            Self::Channels(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for RootAttachmentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Control(e) => e,
            Self::Roots(e) => e,
            Self::Calls(e) => e,
            Self::Expressions(e) => e,
            Self::Channels(e) => e,
        })
    }
}
impl From<CompileControlError> for RootAttachmentError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}

/// The sole sequence formerly local to LocalCompiler. Each constructor runs
/// under the same original control; its first refusal returns without a tail.
/// Allocation requests and finite storage belong to the actual input authors.
#[expect(
    clippy::too_many_arguments,
    reason = "actual attachment inputs remain explicit"
)]
pub fn attach_typed_root_channels_in(
    graph: LocalProgramGraph,
    flows: BTreeMap<ProgramExpressionArena, ProgramControlFlow>,
    roots: Vec<ProgramRootUseBinding>,
    tokens: Vec<(ProgramCallSite, PureCallSpecialization)>,
    types: BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
    channels: Vec<(ProgramChannelSite, FunctionValueType)>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProgramTypedChannels, RootAttachmentError> {
    attach_core::<std::convert::Infallible, crate::creation::NoFactory>(
        graph,
        flows,
        roots,
        tokens,
        Types::Pure(types),
        channels,
        None,
        None,
        work,
    )
    .map_err(|e| match e {
        crate::ProgramCreationFailure::Control(e) => RootAttachmentError::Control(e),
        crate::ProgramCreationFailure::Owner(e) => e,
        crate::ProgramCreationFailure::Creator(never) => match never {},
        crate::ProgramCreationFailure::Resource(_) | crate::ProgramCreationFailure::Capacity(_) => {
            RootAttachmentError::Control(CompileControlError::ResourceExhausted)
        }
    })
}
/// Each immutable attachment family gets its own actual scope. Type slices
/// already carry their separate per-arena creation before entering this body.
#[expect(
    clippy::too_many_arguments,
    reason = "actual attachment sources and factory remain explicit"
)]
pub fn attach_typed_root_channels_with_factory_in<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    graph: LocalProgramGraph,
    flows: BTreeMap<ProgramExpressionArena, ProgramControlFlow>,
    roots: Vec<ProgramRootUseBinding>,
    tokens: Vec<(ProgramCallSite, PureCallSpecialization)>,
    types: BTreeMap<ProgramExpressionArena, novarocks_type_contract::FunctionArgumentTypes>,
    channels: Vec<(ProgramChannelSite, FunctionValueType)>,
    temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProgramTypedChannels, crate::ProgramCreationFailure<RootAttachmentError, H>> {
    attach_core(
        graph,
        flows,
        roots,
        tokens,
        Types::Created(types),
        channels,
        Some(temporary),
        Some(factory),
        work,
    )
}
/// Pure caller already owns immutable type slices; preserve their original
/// backing without re-materializing the declarations into temporary Vecs.
#[expect(
    clippy::too_many_arguments,
    reason = "actual attachment inputs remain explicit"
)]
pub fn attach_typed_root_channels_created_in(
    graph: LocalProgramGraph,
    flows: BTreeMap<ProgramExpressionArena, ProgramControlFlow>,
    roots: Vec<ProgramRootUseBinding>,
    tokens: Vec<(ProgramCallSite, PureCallSpecialization)>,
    types: BTreeMap<ProgramExpressionArena, novarocks_type_contract::FunctionArgumentTypes>,
    channels: Vec<(ProgramChannelSite, FunctionValueType)>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProgramTypedChannels, RootAttachmentError> {
    attach_core::<std::convert::Infallible, crate::creation::NoFactory>(
        graph,
        flows,
        roots,
        tokens,
        Types::Created(types),
        channels,
        None,
        None,
        work,
    )
    .map_err(|e| match e {
        crate::ProgramCreationFailure::Control(e) => RootAttachmentError::Control(e),
        crate::ProgramCreationFailure::Owner(e) => e,
        crate::ProgramCreationFailure::Creator(never) => match never {},
        crate::ProgramCreationFailure::Resource(_) | crate::ProgramCreationFailure::Capacity(_) => {
            RootAttachmentError::Control(CompileControlError::ResourceExhausted)
        }
    })
}
enum Types {
    Pure(BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>),
    Created(BTreeMap<ProgramExpressionArena, novarocks_type_contract::FunctionArgumentTypes>),
}
#[expect(
    clippy::too_many_arguments,
    reason = "sole attachment body preserves original sources"
)]
fn attach_core<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    graph: LocalProgramGraph,
    flows: BTreeMap<ProgramExpressionArena, ProgramControlFlow>,
    roots: Vec<ProgramRootUseBinding>,
    tokens: Vec<(ProgramCallSite, PureCallSpecialization)>,
    types: Types,
    channels: Vec<(ProgramChannelSite, FunctionValueType)>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProgramTypedChannels, crate::ProgramCreationFailure<RootAttachmentError, H>> {
    use crate::ProgramCreationFailure as Failure;
    work.flush()?;
    let snapshot = match factory {
        Some(f) => {
            let mut retained = f.begin_scope().map_err(Failure::Creator)?;
            ProgramRootControlBindings::try_new_with_creator_in(
                graph,
                flows,
                roots,
                temporary
                    .as_deref_mut()
                    .expect("strong entry supplies temporary scope"),
                &mut retained,
                work.control(),
            )
            .map_err(|e| e.map_owner(RootAttachmentError::Roots))?
        }
        None => ProgramRootControlBindings::try_new(graph, flows, roots, work.control()).map_err(
            |e| match e {
                ProgramRootBindingError::Control(c)
                | ProgramRootBindingError::Roots(crate::ProgramExpressionRootError::Control(c)) => {
                    Failure::Control(c)
                }
                e => Failure::Owner(RootAttachmentError::Roots(e)),
            },
        )?,
    };
    work.flush()?;
    let calls = match factory {
        Some(f) => {
            let mut retained = f.begin_scope().map_err(Failure::Creator)?;
            ProgramResolvedCalls::try_new_with_creator_in(
                snapshot,
                tokens,
                temporary.as_deref_mut().expect("strong temporary scope"),
                &mut retained,
                work.control(),
            )
            .map_err(|e| e.map_owner(RootAttachmentError::Calls))?
        }
        None => ProgramResolvedCalls::try_new(snapshot, tokens, work.control()).map_err(
            |e| match e {
                ProgramResolvedCallsError::Control(c) => Failure::Control(c),
                e => Failure::Owner(RootAttachmentError::Calls(e)),
            },
        )?,
    };
    work.flush()?;
    let expressions = match (factory, types) {
        (Some(f), Types::Created(types)) => {
            let mut retained = f.begin_scope().map_err(Failure::Creator)?;
            ProgramTypedExpressions::try_new_with_creator_in(
                calls,
                types,
                temporary.expect("strong temporary scope"),
                &mut retained,
                work.control(),
            )
            .map_err(|e| e.map_owner(RootAttachmentError::Expressions))?
        }
        (None, Types::Pure(types)) => {
            ProgramTypedExpressions::try_new(calls, types, work.control()).map_err(|e| match e {
                ProgramExpressionTypeError::Control(c) => Failure::Control(c),
                e => Failure::Owner(RootAttachmentError::Expressions(e)),
            })?
        }
        (None, Types::Created(types)) => {
            ProgramTypedExpressions::try_new_guarded(calls, types, work.control()).map_err(|e| {
                match e {
                    ProgramExpressionTypeError::Control(c) => Failure::Control(c),
                    e => Failure::Owner(RootAttachmentError::Expressions(e)),
                }
            })?
        }
        _ => unreachable!("public adapters pair their actual/pure type authors"),
    };
    work.flush()?;
    match factory {
        Some(f) => {
            let mut retained = f.begin_scope().map_err(Failure::Creator)?;
            ProgramTypedChannels::try_new_with_creator_in(
                expressions,
                channels,
                &mut retained,
                work.control(),
            )
            .map_err(|e| e.map_owner(RootAttachmentError::Channels))
        }
        None => {
            ProgramTypedChannels::try_new(expressions, channels, work.control()).map_err(
                |e| match e {
                    ProgramChannelTypeError::Control(c) => Failure::Control(c),
                    e => Failure::Owner(RootAttachmentError::Channels(e)),
                },
            )
        }
    }
}
