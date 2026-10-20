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

//! Stable client-facing rendering shared by native task terminal consumers.

use novarocks_execution::task_execution::TerminationDetail;

pub(crate) fn render_termination_detail(detail: Option<&TerminationDetail>) -> String {
    match detail {
        Some(TerminationDetail::Canceled(reason)) => format!("CANCELED(reason={reason})"),
        Some(TerminationDetail::Aborted(cause)) => format!("ABORTED(cause={cause})"),
        Some(TerminationDetail::Failed(failure)) => format!(
            "FAILED(category={}, phase={}, detail={})",
            failure.category(),
            failure.phase(),
            failure.detail()
        ),
        None => "NONE".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::render_termination_detail;
    use novarocks_execution::task_execution::{
        AbortCause, CancelReason, SafeDetail, TaskFailure, TerminationDetail,
    };
    use novarocks_execution_contract::TaskFailurePhase;

    #[test]
    fn failed_termination_renders_stable_category_phase_and_detail() {
        for (phase, text) in [
            (TaskFailurePhase::Execution, "EXECUTION"),
            (TaskFailurePhase::Preparation, "PREPARATION"),
        ] {
            let failure = TerminationDetail::Failed(TaskFailure::new_in_phase(
                novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Data,
                    novarocks_execution_contract::failure::FailureReason::new(
                        std::num::NonZeroU16::new(u16::MAX).unwrap(),
                        std::num::NonZeroU16::new(1).unwrap(),
                    ),
                ),
                SafeDetail::new("map key must not be NULL").expect("bounded safe detail"),
                phase,
            ));
            let rendered = render_termination_detail(Some(&failure));
            assert_eq!(
                rendered,
                format!(
                    "FAILED(category=EXECUTION, phase={text}, detail=map key must not be NULL)"
                )
            );
            assert!(!rendered.contains("TaskFailure {"));
            assert!(!rendered.contains("SafeDetail("));
        }
    }

    #[test]
    fn other_termination_states_keep_stable_client_text() {
        assert_eq!(
            render_termination_detail(Some(&TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded
            ))),
            "CANCELED(reason=UPSTREAM_NO_LONGER_NEEDED)"
        );
        assert_eq!(
            render_termination_detail(Some(&TerminationDetail::Aborted(
                AbortCause::PeerTaskFailed
            ))),
            "ABORTED(cause=PEER_TASK_FAILED)"
        );
        assert_eq!(render_termination_detail(None), "NONE");
    }
}
