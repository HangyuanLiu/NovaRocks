// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Literal cursor oracles, independent of subscription, matching and storage.
//! Neither a legacy shell nor the compiled source is exercised here.

use super::live_cursor::{LiveCursorDisposition, LiveCursorViolation, apply_poll_outcome};
use crate::runtime_filter::evaluator::{
    RuntimeFilterArtifactQuery, RuntimeFilterArtifactQueryError, RuntimeFilterScalarRef,
};
use crate::runtime_filter::{
    LivePollOutcome, LiveTerminal, LogicalVersion, RuntimeFilterBindingId, RuntimeFilterSnapshot,
    UnavailableReason,
};
use arrow::datatypes::DataType;
use novarocks_spi::connector::ConnectorScalarValue;
use std::sync::Arc;

struct UnusedQuery;

impl RuntimeFilterArtifactQuery for UnusedQuery {
    fn data_type(&self) -> &DataType {
        panic!("cursor transition must not inspect the artifact query")
    }

    fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("cursor transition must not query NULL")
    }

    fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("cursor transition must not query artifact contents")
    }

    fn non_null_value_may_match(
        &self,
        _: RuntimeFilterScalarRef<'_>,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("cursor transition must not match a scalar")
    }

    fn non_null_range_may_match(
        &self,
        _: &ConnectorScalarValue,
        _: &ConnectorScalarValue,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("cursor transition must not match a range")
    }
}

fn snapshot(version: u64, marker: u8) -> Arc<RuntimeFilterSnapshot> {
    Arc::new(RuntimeFilterSnapshot::new(
        RuntimeFilterBindingId::new(31),
        LogicalVersion::new(version),
        [marker; 32],
        Arc::new(UnusedQuery),
    ))
}

fn terminals() -> [LiveTerminal; 4] {
    [
        LiveTerminal::Completed,
        LiveTerminal::CompletedWithoutArtifact,
        LiveTerminal::Unavailable(UnavailableReason::ProducerFailed),
        LiveTerminal::Cancelled,
    ]
}

#[test]
fn ordered_live_cursor_first_and_strictly_newer_updates_keep_the_actual_snapshot() {
    let first = snapshot(1, 11);
    let newer = snapshot(3, 33);
    let mut observed = None;
    let mut latest = None;
    let mut terminal = None;
    assert_eq!(
        apply_poll_outcome(
            &mut observed,
            &mut latest,
            &mut terminal,
            LivePollOutcome::Updated {
                snapshot: first.clone(),
                terminal: None,
            },
        ),
        Ok(LiveCursorDisposition::Keep)
    );
    assert_eq!(observed, Some(LogicalVersion::new(1)));
    assert!(Arc::ptr_eq(latest.as_ref().unwrap(), &first));
    assert_eq!(terminal, None);
    assert_eq!(
        apply_poll_outcome(
            &mut observed,
            &mut latest,
            &mut terminal,
            LivePollOutcome::Updated {
                snapshot: newer.clone(),
                terminal: None,
            },
        ),
        Ok(LiveCursorDisposition::Keep)
    );
    assert_eq!(observed, Some(LogicalVersion::new(3)));
    assert!(Arc::ptr_eq(latest.as_ref().unwrap(), &newer));
    assert_eq!(latest.as_ref().unwrap().contract_digest(), [33; 32]);
    assert_eq!(terminal, None);
}

#[test]
fn ordered_live_cursor_equal_and_stale_updated_terminal_preserves_the_last_artifact() {
    for incoming_version in [2, 3] {
        for ending in terminals() {
            let original = snapshot(3, 33);
            let mut observed = Some(LogicalVersion::new(3));
            let mut latest = Some(original.clone());
            let mut terminal = None;
            assert_eq!(
                apply_poll_outcome(
                    &mut observed,
                    &mut latest,
                    &mut terminal,
                    LivePollOutcome::Updated {
                        snapshot: snapshot(incoming_version, 99),
                        terminal: Some(ending),
                    },
                ),
                Ok(LiveCursorDisposition::Keep)
            );
            assert_eq!(observed, Some(LogicalVersion::new(3)));
            assert!(Arc::ptr_eq(latest.as_ref().unwrap(), &original));
            assert_eq!(latest.as_ref().unwrap().contract_digest(), [33; 32]);
            assert_eq!(terminal, Some(ending));
        }
    }
}

#[test]
fn ordered_live_cursor_idle_newer_version_refuses_before_any_terminal_transition() {
    for ending in terminals().map(Some).into_iter().chain([None]) {
        for previous in [None, Some(3)] {
            let original = previous.map(|version| snapshot(version, 33));
            let mut observed = previous.map(LogicalVersion::new);
            let mut latest = original.clone();
            let mut terminal = Some(LiveTerminal::Cancelled);
            assert_eq!(
                apply_poll_outcome(
                    &mut observed,
                    &mut latest,
                    &mut terminal,
                    LivePollOutcome::Idle {
                        latest_version: Some(LogicalVersion::new(4)),
                        terminal: ending,
                    },
                ),
                Err(LiveCursorViolation::NewerVersionWithoutArtifact)
            );
            assert_eq!(observed, previous.map(LogicalVersion::new));
            match (&latest, &original) {
                (Some(actual), Some(expected)) => assert!(Arc::ptr_eq(actual, expected)),
                (None, None) => {}
                _ => panic!("a refused idle outcome changed the artifact"),
            }
            assert_eq!(terminal, Some(LiveTerminal::Cancelled));
        }
    }
}

#[test]
fn ordered_live_cursor_idle_regression_refuses_before_any_terminal_transition() {
    for ending in terminals().map(Some).into_iter().chain([None]) {
        let original = snapshot(3, 33);
        let mut observed = Some(LogicalVersion::new(3));
        let mut latest = Some(original.clone());
        let mut terminal = Some(LiveTerminal::Completed);
        assert_eq!(
            apply_poll_outcome(
                &mut observed,
                &mut latest,
                &mut terminal,
                LivePollOutcome::Idle {
                    latest_version: Some(LogicalVersion::new(2)),
                    terminal: ending,
                },
            ),
            Err(LiveCursorViolation::Regressed)
        );
        assert_eq!(observed, Some(LogicalVersion::new(3)));
        assert!(Arc::ptr_eq(latest.as_ref().unwrap(), &original));
        assert_eq!(terminal, Some(LiveTerminal::Completed));
    }
}

#[test]
fn ordered_live_cursor_terminal_without_artifact_requests_pass_through() {
    for ending in terminals() {
        let mut observed = None;
        let mut latest = None;
        let mut terminal = None;
        assert_eq!(
            apply_poll_outcome(
                &mut observed,
                &mut latest,
                &mut terminal,
                LivePollOutcome::Idle {
                    latest_version: None,
                    terminal: Some(ending),
                },
            ),
            Ok(LiveCursorDisposition::PassThrough)
        );
        assert_eq!(observed, None);
        assert!(latest.is_none());
        // The shell replaces the binding, rather than inventing a snapshot
        // or publishing a terminal into a bound state with no artifact.
        assert_eq!(terminal, None);
    }
}

#[test]
fn ordered_live_cursor_idle_terminal_keeps_the_existing_snapshot() {
    for latest_version in [None, Some(LogicalVersion::new(3))] {
        for ending in terminals() {
            let original = snapshot(3, 33);
            let mut observed = Some(LogicalVersion::new(3));
            let mut latest = Some(original.clone());
            let mut terminal = None;
            assert_eq!(
                apply_poll_outcome(
                    &mut observed,
                    &mut latest,
                    &mut terminal,
                    LivePollOutcome::Idle {
                        latest_version,
                        terminal: Some(ending),
                    },
                ),
                Ok(LiveCursorDisposition::Keep)
            );
            assert_eq!(observed, Some(LogicalVersion::new(3)));
            assert!(Arc::ptr_eq(latest.as_ref().unwrap(), &original));
            assert_eq!(terminal, Some(ending));
        }
    }
}

#[test]
fn ordered_live_cursor_outcome_without_terminal_does_not_clear_an_existing_terminal() {
    for incoming_version in [2, 3, 4] {
        let original = snapshot(3, 33);
        let incoming = snapshot(incoming_version, 99);
        let mut observed = Some(LogicalVersion::new(3));
        let mut latest = Some(original.clone());
        let mut terminal = Some(LiveTerminal::Cancelled);
        assert_eq!(
            apply_poll_outcome(
                &mut observed,
                &mut latest,
                &mut terminal,
                LivePollOutcome::Updated {
                    snapshot: incoming.clone(),
                    terminal: None,
                },
            ),
            Ok(LiveCursorDisposition::Keep)
        );
        let (expected_version, expected_snapshot) = if incoming_version == 4 {
            (LogicalVersion::new(4), &incoming)
        } else {
            (LogicalVersion::new(3), &original)
        };
        assert_eq!(observed, Some(expected_version));
        assert!(Arc::ptr_eq(latest.as_ref().unwrap(), expected_snapshot));
        assert_eq!(terminal, Some(LiveTerminal::Cancelled));
        assert_eq!(
            apply_poll_outcome(
                &mut observed,
                &mut latest,
                &mut terminal,
                LivePollOutcome::Idle {
                    latest_version: Some(expected_version),
                    terminal: None,
                },
            ),
            Ok(LiveCursorDisposition::Keep)
        );
        assert_eq!(observed, Some(expected_version));
        assert!(Arc::ptr_eq(latest.as_ref().unwrap(), expected_snapshot));
        assert_eq!(terminal, Some(LiveTerminal::Cancelled));
    }
}
