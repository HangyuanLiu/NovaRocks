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

use crate::query_execution::contract::{DistributedQueryErrorKind, DistributedQueryIntent};
use crate::query_execution::outcome::QueryOutcomeFactory;
use crate::query_execution::statistics::{StatisticsExecutionMode, StatisticsExecutionPolicy};
use novarocks_query_application::cancellation::{QueryCancellationReason, QueryCancellationSource};

#[test]
fn cancellation_view_observes_injected_flag() {
    let cancelled = QueryCancellationSource::new();
    let view = cancelled.view();

    assert!(!view.is_cancelled());
    let _ = cancelled.request(QueryCancellationReason::ClientDisconnected);
    assert!(view.is_cancelled());
}

#[test]
fn outcome_factory_rejects_intent_mismatch() {
    let result = QueryOutcomeFactory::new(DistributedQueryIntent::Result).from_execution_result(
        crate::query_execution::outcome::QueryExecutionResult {
            query_result: novarocks_query_application::api::QueryResult::empty(),
            write_session: None,
            fragment_profiles: vec![
                crate::query_execution::profile::FragmentProfileTree::unattributed(
                    novarocks_execution::runtime::profile::Profiler::new("fragment-1")
                        .to_native_tree(),
                ),
            ],
        },
    );

    let Err(error) = result else {
        panic!("Result intent must reject a profile payload");
    };
    assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
    assert_eq!(
        error.message(),
        "Result outcome cannot contain write or profile payloads"
    );
}

#[test]
fn durable_statistics_attempt_ignores_statement_cancellation_and_is_bounded() {
    let policy = StatisticsExecutionPolicy::try_new(
        StatisticsExecutionMode::BackgroundCollectionAttempt,
        std::time::Duration::from_secs(30 * 60),
    )
    .expect("maximum durable policy");
    assert!(!policy.mode().statement_cancellation_terminates_execution());
    assert_eq!(
        policy.attempt_timeout(),
        std::time::Duration::from_secs(30 * 60)
    );
    assert!(
        StatisticsExecutionPolicy::try_new(
            StatisticsExecutionMode::BackgroundCollectionAttempt,
            std::time::Duration::from_secs(30 * 60 + 1),
        )
        .is_ok(),
        "the shared LakePublicationRuntimePolicy, not this transport-neutral Core policy, owns the configured maximum"
    );
    assert!(
        StatisticsExecutionPolicy::try_new(
            StatisticsExecutionMode::BackgroundCollectionAttempt,
            std::time::Duration::ZERO,
        )
        .is_err()
    );
    assert!(StatisticsExecutionMode::SynchronousWait.statement_cancellation_terminates_execution());
}

#[test]
fn profile_outcome_preserves_fragment_profiles() {
    let profile = crate::query_execution::profile::FragmentProfileTree::for_fragment(
        7,
        novarocks_execution::runtime::profile::Profiler::new("fragment-7").to_native_tree(),
    );
    let outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Profile)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: novarocks_query_application::api::build_string_query_result(
                "status",
                vec!["profiled".to_string()],
            )
            .expect("profile result"),
            write_session: None,
            fragment_profiles: vec![profile.clone()],
        })
        .expect("Profile intent accepts fragment profiles");

    let (result, profiles) = outcome
        .into_profile()
        .expect("profile outcome variant")
        .into_parts();
    assert_eq!(result.row_count(), 1);
    assert_eq!(profiles.into_profiles(), vec![profile]);
}

#[test]
fn result_outcome_preserves_query_result() {
    let outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Result)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: novarocks_query_application::api::build_string_query_result(
                "value",
                vec!["kept".to_string()],
            )
            .expect("result payload"),
            write_session: None,
            fragment_profiles: Vec::new(),
        })
        .expect("Result intent accepts a plain query result");

    assert_eq!(
        outcome
            .into_result()
            .expect("result outcome variant")
            .into_query_result()
            .row_count(),
        1
    );
}
