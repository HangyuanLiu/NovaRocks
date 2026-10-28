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

// Included in task_execution::tests; exercises the real Task owner, bridge and driver.
mod split_delivery_local_terminal_tests {
    use super::*;
    use crate::query_execution::split_assignment::{
        AssignmentTarget, ScanNodeKey, SplitAssignmentDriver, SplitAssignmentStop,
        TaskUpdateTransport,
    };
    use crate::task_execution::split_transport::SplitDeliveryBridge;
    use novarocks_spi::connector::ConnectorReadWireEncoder;

    struct TerminalOnlyEncoder;

    impl ConnectorReadWireEncoder for TerminalOnlyEncoder {
        fn owner(&self) -> &str {
            "split-delivery-test"
        }

        fn encode_relation_payload(
            &self,
            _relation: &novarocks_spi::connector::read_stack::ConnectorReadRelation,
        ) -> Result<
            novarocks_spi::connector::ConnectorReadRelationPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("split delivery tests never encode a relation")
        }

        fn encode_column_payload(
            &self,
            _column: &novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
        ) -> Result<
            novarocks_spi::connector::ConnectorEncodedPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("split delivery tests never encode a column")
        }

        fn encode_transaction_payload(
            &self,
            _transaction: &novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
        ) -> Result<
            novarocks_spi::connector::ConnectorEncodedPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("split delivery tests never encode a transaction")
        }

        fn encode_split_payload(
            &self,
            _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
        ) -> Result<
            novarocks_spi::connector::ConnectorReadSplitPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("split delivery tests never enumerate a split")
        }
    }

    fn late_terminal_delivery(
        harness: &Harness,
        task: TaskId,
    ) -> (Arc<SplitDeliveryBridge>, SplitAssignmentDriver, ScanNodeKey) {
        let graph = harness.execution.graph();
        let original = graph.task(task).unwrap();
        let scan = ScanNodeKey::new(LEAF_FRAGMENT, SCAN_NODE);
        let target = AssignmentTarget {
            backend_idx: 0,
            fragment_instance_id: original.fragment_instance_id(),
        };
        let bridge = SplitDeliveryBridge::for_graph(graph);
        let mut driver = SplitAssignmentDriver::new(
            graph.execution_id(),
            Arc::clone(&bridge) as Arc<dyn TaskUpdateTransport>,
            BTreeMap::from([(scan, vec![target])]),
            1,
            BTreeMap::from([(
                scan,
                Arc::new(TerminalOnlyEncoder) as Arc<dyn ConnectorReadWireEncoder>,
            )]),
            crate::task_protocol_config::TaskUpdateRetryPolicy::default(),
            SplitAssignmentStop::default(),
        );
        driver.start_delivery(scan, BTreeMap::new(), true).unwrap();
        assert!(driver.drive_delivery().unwrap());
        (bridge, driver, scan)
    }

    fn admit_original_pending(
        harness: &mut Harness,
        bridge: &SplitDeliveryBridge,
        task: TaskId,
    ) -> Result<UpdateAdmission, TaskExecutionError> {
        let mut pending = bridge.take_pending();
        assert_eq!(pending.len(), 1);
        let pending = pending.pop().unwrap();
        assert_eq!(pending.task(), task);
        let delivery = pending.delivery();
        let result = harness
            .execution
            .enqueue_task_update(task, pending.into_update());
        bridge.admit(delivery, result.as_ref().copied()).unwrap();
        result
    }

    #[test]
    fn original_normal_demand_withdrawal_settles_late_split_without_a_failure() {
        for terminal in [None, Some(TaskState::Finished), Some(TaskState::Canceled)] {
            let mut harness = Harness::new(&[0], &[0], 512);
            harness.settle_until_quiet(Duration::from_secs(10));
            let leaf = harness.stage_tasks(1)[0];
            let identity = harness.identity(leaf);
            harness.publish(leaf, TaskState::Running, None, false);
            match terminal {
                None => harness
                    .execution
                    .stand_down_task_normally(identity)
                    .unwrap(),
                Some(TaskState::Finished) => {
                    harness.publish(leaf, TaskState::Flushing, None, false);
                    harness.publish(leaf, TaskState::Finished, None, true);
                }
                Some(TaskState::Canceled) => {
                    let cause = TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded);
                    harness.publish(leaf, TaskState::Canceling, Some(cause.clone()), false);
                    harness.publish(leaf, TaskState::Canceled, Some(cause), false);
                }
                _ => unreachable!(),
            }
            let original_status = harness.execution.task(leaf).unwrap().status().cloned();
            let (bridge, mut driver, scan) = late_terminal_delivery(&harness, leaf);
            assert_eq!(
                admit_original_pending(&mut harness, &bridge, leaf).unwrap(),
                UpdateAdmission::DiscardedNoLongerNeeded
            );
            assert!(driver.drive_delivery().unwrap());
            assert!(!driver.delivery_in_progress());
            assert!(driver.is_terminal_for(scan));
            assert!(bridge.take_pending().is_empty());
            assert_eq!(
                harness.execution.task(leaf).unwrap().status(),
                original_status.as_ref()
            );
            assert!(harness.execution.failure_cause().is_none());
        }
    }

    #[test]
    fn original_failure_is_not_normalized_by_earlier_or_later_stand_down() {
        let original = TerminationDetail::Failed(novarocks_execution_contract::TaskFailure::new(
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Resource,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(17).unwrap(),
                ),
            ),
            novarocks_execution_contract::SafeDetail::truncating("original resource failure"),
        ));
        for stand_down_first in [true, false] {
            let mut harness = Harness::new(&[0], &[0], 512);
            harness.settle_until_quiet(Duration::from_secs(10));
            let leaf = harness.stage_tasks(1)[0];
            let identity = harness.identity(leaf);
            harness.publish(leaf, TaskState::Running, None, false);
            if stand_down_first {
                harness
                    .execution
                    .stand_down_task_normally(identity)
                    .unwrap();
            }
            harness.publish(leaf, TaskState::Failing, Some(original.clone()), false);
            harness.publish(leaf, TaskState::Failed, Some(original.clone()), false);
            if !stand_down_first {
                harness
                    .execution
                    .stand_down_task_normally(identity)
                    .unwrap();
            }
            let (bridge, mut driver, scan) = late_terminal_delivery(&harness, leaf);
            // The accepted failure already closed admission on the original
            // QueryTaskExecution owner. It must never reach the normal-demand
            // discard branch or be mistaken for a successful destination.
            assert!(matches!(
                admit_original_pending(&mut harness, &bridge, leaf),
                Err(TaskExecutionError::Schedule(_))
            ));
            assert!(matches!(
                driver.drive_delivery(),
                Err(SplitAssignmentDriverError::Transport { .. })
            ));
            assert!(!driver.is_terminal_for(scan));
            assert_eq!(harness.execution.failure_cause(), Some(&original));
            assert_eq!(
                harness
                    .execution
                    .task(leaf)
                    .unwrap()
                    .status()
                    .unwrap()
                    .termination(),
                Some(&original)
            );
        }
    }

    #[test]
    fn missing_terminal_status_does_not_supply_normal_completion() {
        let mut harness = Harness::new(&[0], &[0], 512);
        harness.settle_until_quiet(Duration::from_secs(10));
        let leaf = harness.stage_tasks(1)[0];
        harness
            .execution
            .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
            .unwrap();
        let original = harness
            .released()
            .into_iter()
            .find(|intent| intent.kind() == OperationKind::UpdateTask)
            .unwrap();
        harness
            .update_ack(&original, OperationOutcome::TerminalRejected)
            .unwrap();
        assert!(!harness.execution.task(leaf).unwrap().is_terminal());
        let (bridge, mut driver, scan) = late_terminal_delivery(&harness, leaf);
        assert_eq!(
            admit_original_pending(&mut harness, &bridge, leaf).unwrap(),
            UpdateAdmission::DiscardedTerminal
        );
        assert!(matches!(
            driver.drive_delivery(),
            Err(SplitAssignmentDriverError::Transport { .. })
        ));
        assert!(!driver.is_terminal_for(scan));
        assert!(!harness.execution.task(leaf).unwrap().is_terminal());
        assert!(harness.execution.failure_cause().is_none());
    }
}
