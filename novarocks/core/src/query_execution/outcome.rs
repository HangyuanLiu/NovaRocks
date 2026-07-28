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

//! Intent-bound completion capability.

use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent,
};
use crate::query_execution::write::{WriteAbortInput, WriteCommitInput};
use crate::runtime::profile::RuntimeProfileTree;
use crate::runtime::query_result::QueryResult;

/// Role-neutral execution data assembled by core engine flows before intent
/// validation seals the public distributed-query outcome.
#[derive(Debug)]
pub(crate) struct QueryExecutionResult {
    pub(crate) query_result: QueryResult,
    pub(crate) write_commit: Option<WriteCommitInput>,
    pub(crate) write_abort: Option<WriteAbortInput>,
    pub(crate) fragment_profiles: Vec<RuntimeProfileTree>,
}

pub enum DistributedQueryOutcome {
    Result(ResultExecutionOutcome),
    Write(WriteExecutionOutcome),
    Profile(ProfileExecutionOutcome),
}

pub struct ResultExecutionOutcome {
    result: QueryResult,
}

impl ResultExecutionOutcome {
    pub(crate) fn into_query_result(self) -> QueryResult {
        self.result
    }
}

pub struct WriteExecutionOutcome {
    result: QueryResult,
    commit: Option<WriteCommitInput>,
    abort: Option<WriteAbortInput>,
}

impl WriteExecutionOutcome {
    pub(crate) fn into_parts(
        self,
    ) -> (
        QueryResult,
        Option<WriteCommitInput>,
        Option<WriteAbortInput>,
    ) {
        (self.result, self.commit, self.abort)
    }
}

pub struct FragmentProfileSet {
    profiles: Vec<RuntimeProfileTree>,
}

impl FragmentProfileSet {
    pub(crate) fn new(profiles: Vec<RuntimeProfileTree>) -> Self {
        Self { profiles }
    }

    pub(crate) fn into_profiles(self) -> Vec<RuntimeProfileTree> {
        self.profiles
    }
}

pub struct ProfileExecutionOutcome {
    result: QueryResult,
    profiles: FragmentProfileSet,
}

impl ProfileExecutionOutcome {
    pub(crate) fn into_parts(self) -> (QueryResult, FragmentProfileSet) {
        (self.result, self.profiles)
    }
}

impl DistributedQueryOutcome {
    pub fn intent(&self) -> DistributedQueryIntent {
        match self {
            Self::Result(_) => DistributedQueryIntent::Result,
            Self::Write(_) => DistributedQueryIntent::Write,
            Self::Profile(_) => DistributedQueryIntent::Profile,
        }
    }

    pub(crate) fn into_write(self) -> Result<WriteExecutionOutcome, DistributedQueryError> {
        match self {
            Self::Write(outcome) => Ok(outcome),
            other => Err(outcome_variant_mismatch(
                DistributedQueryIntent::Write,
                other.intent(),
            )),
        }
    }

    pub(crate) fn into_result(self) -> Result<ResultExecutionOutcome, DistributedQueryError> {
        match self {
            Self::Result(outcome) => Ok(outcome),
            other => Err(outcome_variant_mismatch(
                DistributedQueryIntent::Result,
                other.intent(),
            )),
        }
    }

    pub(crate) fn into_profile(self) -> Result<ProfileExecutionOutcome, DistributedQueryError> {
        match self {
            Self::Profile(outcome) => Ok(outcome),
            other => Err(outcome_variant_mismatch(
                DistributedQueryIntent::Profile,
                other.intent(),
            )),
        }
    }
}

pub struct QueryOutcomeFactory {
    intent: DistributedQueryIntent,
}

impl QueryOutcomeFactory {
    pub(super) fn new(intent: DistributedQueryIntent) -> Self {
        Self { intent }
    }

    pub fn intent(&self) -> DistributedQueryIntent {
        self.intent
    }

    pub fn write(
        self,
        result: QueryResult,
        commit: Option<WriteCommitInput>,
        abort: Option<WriteAbortInput>,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.require_intent(DistributedQueryIntent::Write)?;
        if commit.is_some() && abort.is_some() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "Write outcome cannot contain both commit and abort payloads",
            ));
        }
        Ok(DistributedQueryOutcome::Write(WriteExecutionOutcome {
            result,
            commit,
            abort,
        }))
    }

    pub(crate) fn from_execution_result(
        self,
        result: QueryExecutionResult,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let QueryExecutionResult {
            query_result,
            write_commit,
            write_abort,
            fragment_profiles,
        } = result;
        match self.intent {
            DistributedQueryIntent::Write => {
                if !fragment_profiles.is_empty() {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "Write outcome cannot contain fragment profiles",
                    ));
                }
                self.write(query_result, write_commit, write_abort)
            }
            DistributedQueryIntent::Profile => {
                if write_commit.is_some() || write_abort.is_some() {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "Profile outcome cannot contain write commit or abort payloads",
                    ));
                }
                self.profile(query_result, FragmentProfileSet::new(fragment_profiles))
            }
            DistributedQueryIntent::Result => {
                if write_commit.is_some() || write_abort.is_some() || !fragment_profiles.is_empty()
                {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "Result outcome cannot contain write or profile payloads",
                    ));
                }
                self.result(query_result)
            }
        }
    }

    pub fn result(
        self,
        result: QueryResult,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.require_intent(DistributedQueryIntent::Result)?;
        Ok(DistributedQueryOutcome::Result(ResultExecutionOutcome {
            result,
        }))
    }

    pub fn profile(
        self,
        result: QueryResult,
        profiles: FragmentProfileSet,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.require_intent(DistributedQueryIntent::Profile)?;
        Ok(DistributedQueryOutcome::Profile(ProfileExecutionOutcome {
            result,
            profiles,
        }))
    }

    fn require_intent(
        &self,
        received: DistributedQueryIntent,
    ) -> Result<(), DistributedQueryError> {
        if self.intent == received {
            return Ok(());
        }
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            format!(
                "distributed query outcome intent mismatch: expected {:?}, received {received:?}",
                self.intent
            ),
        ))
    }
}

fn outcome_variant_mismatch(
    expected: DistributedQueryIntent,
    received: DistributedQueryIntent,
) -> DistributedQueryError {
    DistributedQueryError::new(
        DistributedQueryErrorKind::ContractViolation,
        format!(
            "distributed query outcome variant mismatch: expected {expected:?}, received {received:?}"
        ),
    )
}
