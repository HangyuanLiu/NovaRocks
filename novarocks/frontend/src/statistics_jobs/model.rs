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
// software distributed under the Apache License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Frontend input adapters for the statistics application model.
//!
//! The statistics job state machine is owned by
//! `novarocks_statistics_application`. This module preserves only the
//! frontend parsed-name shape and re-exports that product model; it owns no
//! job state or identity minting.

pub use novarocks_statistics_application::{
    StatisticsColumns, StatisticsConvergence, StatisticsFailure, StatisticsJob,
    StatisticsJobConclusion, StatisticsJobCreate, StatisticsJobId, StatisticsJobPhase,
    StatisticsJobState, StatisticsLogicalExecutionId, StatisticsPublicationFact,
    StatisticsPublicationId, StatisticsQueryAttemptId, StatisticsTarget,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl From<super::application::StatisticsTableTarget> for StatisticsJobTarget {
    fn from(value: super::application::StatisticsTableTarget) -> Self {
        Self {
            catalog: value.catalog,
            namespace: value.namespace,
            table: value.table,
        }
    }
}
