#![allow(dead_code)]
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

mod contract_lowering;
mod lowered_draft;
pub use lowered_draft::{
    AggregateRuntimeDemand, CheckedSqlResultDeclaration, ResultDeclarationError,
    SqlAuthoredPhysicalPlan, SqlCallDependencyLoan, SqlCallDependencyProvenance,
    SqlCallDependencySite, SqlCanonicalDependencyLoan, SqlExpressionCallKind,
    SqlSourceJournalError,
};
pub(crate) use lowered_draft::{CheckedAggregateLogicalSourceEntry, LoweredSqlPhysicalDraft};
mod expression_occurrences;
mod package_semantics;
mod physical_aggregate_occurrences;
mod physical_aggregate_requests;
mod physical_call_arguments;
mod physical_expression_effects;
mod physical_fragment_effects;
mod physical_relational_effects;
mod physical_scalar_occurrences;
mod physical_scalar_requests;
mod physical_table_occurrences;
mod physical_table_requests;
mod physical_temporal_sources;
mod physical_window_occurrences;
mod physical_window_requests;
mod physical_writer_occurrences;
mod physical_writer_requests;
pub use package_semantics::{
    FragmentPackageSemantics, PackageSemanticsError, author_fragment_package_semantics,
};

pub(crate) use contract_lowering::{
    ContractLoweringError, FinalChangeStreamWriteLowering, FinalWriteLowering,
    lower_final_change_stream_write_plan, lower_final_physical_plan,
    lower_final_physical_plan_with_provider_reads, lower_final_physical_write_plan,
};

#[cfg(test)]
mod physical_temporal_journal_tests;

#[cfg(feature = "test-support")]
pub mod state_source_fixture;

#[cfg(feature = "test-support")]
pub mod state_author_fixture;
#[cfg(feature = "test-support")]
pub mod writer_state_fixture;
