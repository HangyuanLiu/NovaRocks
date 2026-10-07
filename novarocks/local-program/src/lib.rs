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

//! Pure, task-independent facts used while compiling a local fragment program.
//!
//! This crate deliberately has no execution, transport, worker, connector SPI,
//! or async runtime dependency. A program's static representation moves here
//! as its node and expression types are separated from task-owned bindings.

mod compiled;
mod compiled_origins;
mod contract;
mod control_flow;
mod expression_roots;
mod expressions;
mod layout;
mod lexical_bindings;
mod primitives;
mod program;
mod provenance;
mod provider_links;
mod requirements;
mod resolved_calls;
mod runtime_filter;
mod sink;
mod typed_channels;
mod typed_expressions;
mod values;

pub use compiled::{
    CompiledExchangeInput, CompiledScanInput, LocalProgram, LocalProgramCompileError,
};
pub use compiled_origins::CompiledOriginsError;
pub use contract::{
    CompileProfile, FragmentProgramOptions, FragmentSinkAssignmentKind,
    FragmentSinkAssignmentRequirement, KernelAbiVersion, LayoutIdentity, RuntimeFilterContract,
    RuntimeFilterId, ScanAssignmentKind, ScanSourceContract,
};
pub use control_flow::*;
pub use expression_roots::*;
pub use expressions::{
    ExpressionsCompileError, ImmutableExpressions, MAX_STATIC_EXPRESSION_DEPTH,
    MAX_STATIC_EXPRESSION_DYNAMIC_BYTES, MAX_STATIC_EXPRESSIONS, ProgramExprId, StaticExprKind,
    StaticExprNode, StaticExpressionError, StaticFieldSchema, StaticFunctionKind, StaticLiteral,
};
pub use layout::{LayoutCompileError, LayoutError, StaticLayout};
pub use lexical_bindings::*;
pub use novarocks_connector_contract::{
    ScanColumnId, StaticConnectorScan, StaticConnectorScanError, StaticScanAssignment,
    StaticScanDynamicFilter,
};
// The partition vocabulary of `StaticStreamBranch`, re-exported so a compiler
// can author a stream sink without a second execution-contract dependency.
pub use novarocks_execution_contract::DataStreamPartitionType;
pub use primitives::{ProgramComparisonSite, ProgramPrimitiveError};
pub use program::{
    AggregateTopNFilter, AnalyticOutputColumn, AssertRowsMode, ChangeEventOutputExpr,
    ChangeEventSpec, FilterConsumerAtExpr, FilterProducerAtExpr, JoinDistributionMode, JoinType,
    LocalProgramError, LocalProgramGraph, MAX_PROGRAM_EXPANDED_OCCURRENCES, MAX_PROGRAM_NODE_DEPTH,
    MAX_PROGRAM_NODES, NestedLoopJoinType, ProgramCompileError, ProgramNode, ProgramNodeKind,
    ProgramScanSource, ProjectExpressionSlot, RowAssertion, SetOpKind, SortExpression,
    SortTopNType, StaticAggregateCall, StaticAggregateOrder, StaticAggregateTypeSignature,
    StaticWindowFunction, StaticWriterProjection, StreamingPreaggregationMode,
    TableFunctionOutputSlot, UnpivotConstant, UnpivotMapping, UnpivotPassthrough, WindowBoundary,
    WindowFrame, WindowFunctionKind, WindowType, WriterFinalAggregateCall,
    WriterFinalAggregatePlan, WriterGroupedUnpivotMapping, WriterGroupedUnpivotPlan,
    WriterPartialAggregateCall,
};
pub use provenance::*;
pub use provider_links::ProviderLinkError;
pub use requirements::{
    BindingRequirement, BindingRequirements, BindingRequirementsCompileError,
    BindingRequirementsError, ProgramNodeId, ScanSourceKind,
};
pub use resolved_calls::*;
pub use runtime_filter::{
    FilterConsumerActivation, FilterLateApplyGranularity, FilterNullOrder, FilterNullSemantics,
    FilterOrderKey, FilterProducerKind, FilterReduction, FilterSortDirection, StaticFilterConsumer,
    StaticFilterContract, StaticFilterError, StaticFilterProducer,
};
pub use sink::{
    MAX_STATIC_SINK_BRANCHES, MAX_STATIC_SINK_COLUMNS, MAX_STATIC_SINK_EXPRESSIONS,
    SinkCompileError, StaticSinkError, StaticSinkProgram, StaticStreamBranch,
};
pub use typed_channels::*;
pub use typed_expressions::*;
pub use values::{
    MAX_STATIC_VALUES_BACKING_BYTES, StaticValues, StaticValuesBacking, StaticValuesCell,
    StaticValuesError, ValuesCompileError,
};
