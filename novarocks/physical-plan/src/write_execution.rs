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

//! Orthogonal write-result, distribution and placement facts.

use std::num::NonZeroU32;

use crate::{
    ChangeStreamWriterCut, EdgeId, FragmentId, ValueId, WriteTargetOrdinal, WriterResultCut,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryRequirement {
    DemandDriven,
    MustDrain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputRole {
    Relational,
    ProviderRoute { target: WriteTargetOrdinal },
    LogicalSummary,
    WriterResults,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StagePlacementPolicy {
    Singleton,
    ScanWork,
    Spread { max_instances: Option<NonZeroU32> },
    FollowSources { sources: Box<[FragmentId]> },
}

pub use novarocks_result_contract::{FinishCountMode, FinishInputs};

/// Which fragment-owned output a source edge projects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentOutputPort {
    Root,
    LogicalSummary,
}

/// Fixed, ordered semantic fields of the Task-local logical-change relation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalChangeCountField {
    Inserted,
    Replaced,
    Deleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalSummaryColumns {
    pub inserted: ValueId,
    pub replaced: ValueId,
    pub deleted: ValueId,
}

impl LogicalSummaryColumns {
    pub const fn values(self) -> [ValueId; 3] {
        [self.inserted, self.replaced, self.deleted]
    }
    pub const fn fields(self) -> [(LogicalChangeCountField, ValueId); 3] {
        [
            (LogicalChangeCountField::Inserted, self.inserted),
            (LogicalChangeCountField::Replaced, self.replaced),
            (LogicalChangeCountField::Deleted, self.deleted),
        ]
    }
}

/// Symmetric source/destination identities. Every field is non-null Int64.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalSummaryCut {
    pub source: LogicalSummaryColumns,
    pub destination: LogicalSummaryColumns,
}

/// Mutually exclusive payload proofs. The payload is the sole cut-role author;
/// summary counters cannot be accepted as a provider writer relation.
#[derive(Clone, Debug, PartialEq)]
pub enum ExchangePayloadContract {
    Relational,
    ProviderRoute(ChangeStreamWriterCut),
    WriterResults(WriterResultCut),
    LogicalSummary(LogicalSummaryCut),
}

impl ExchangePayloadContract {
    pub fn provider_route(&self) -> Option<&ChangeStreamWriterCut> {
        match self {
            Self::ProviderRoute(proof) => Some(proof),
            _ => None,
        }
    }
    pub fn writer_results(&self) -> Option<&WriterResultCut> {
        match self {
            Self::WriterResults(proof) => Some(proof),
            _ => None,
        }
    }
    pub fn role(&self) -> OutputRole {
        match self {
            Self::Relational => OutputRole::Relational,
            Self::ProviderRoute(proof) => OutputRole::ProviderRoute {
                target: proof.write_target_ordinal,
            },
            Self::WriterResults(_) => OutputRole::WriterResults,
            Self::LogicalSummary(_) => OutputRole::LogicalSummary,
        }
    }
}

impl crate::InboundFragmentCut {
    pub fn change_stream_writer(&self) -> Option<&ChangeStreamWriterCut> {
        self.payload.provider_route()
    }
    pub fn writer_result(&self) -> Option<&WriterResultCut> {
        self.payload.writer_results()
    }
}
impl crate::OutboundFragmentCut {
    pub fn change_stream_writer(&self) -> Option<&ChangeStreamWriterCut> {
        self.payload.provider_route()
    }
    pub fn writer_result(&self) -> Option<&WriterResultCut> {
        self.payload.writer_results()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalSummaryOutput {
    pub edge: EdgeId,
    pub columns: LogicalSummaryColumns,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssertionApplicability {
    AllRows,
    WhenTrue(ValueId),
}

/// Correctness colocation is independent from layout and stage placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteColocation {
    None,
    Singleton,
    ByKey(Box<[ValueId]>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteLayoutPreference {
    None,
    ClusterBy(Box<[ValueId]>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRouteRequirements {
    pub colocation: WriteColocation,
    pub layout: WriteLayoutPreference,
}
impl WriteRouteRequirements {
    pub fn colocation_keys(&self) -> &[ValueId] {
        match &self.colocation {
            WriteColocation::ByKey(keys) => keys,
            _ => &[],
        }
    }
    pub fn layout_keys(&self) -> &[ValueId] {
        match &self.layout {
            WriteLayoutPreference::ClusterBy(keys) => keys,
            _ => &[],
        }
    }
}
