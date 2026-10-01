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

//! Quota content membership production. One producer partition covers the complete task demand.
use super::quota_preclaim::QuotaContentFilterObserver;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter as rf;
use std::sync::{Arc, Mutex};

pub(crate) struct NativeQuotaContentFilterObserver {
    arena: Arc<ExprArena>,
    session: rf::RuntimeFilterSessionRef,
    streams: Mutex<Vec<Stream>>,
}
struct Stream {
    expr: ExprId,
    contract: rf::RuntimeFilterProducerContract,
    producer: Option<rf::RuntimeFilterProducerHandle>,
    sequence: u64,
    terminal: bool,
}
impl NativeQuotaContentFilterObserver {
    pub(crate) fn try_new(
        bindings: Vec<(ExprId, rf::RuntimeFilterProducerContract)>,
        arena: Arc<ExprArena>,
        session: rf::RuntimeFilterSessionRef,
    ) -> Result<Arc<Self>, String> {
        let mut streams = Vec::with_capacity(bindings.len());
        for (expr, contract) in bindings {
            if !matches!(arena.node(expr), Some(ExprNode::SlotId(_))) {
                return Err(
                    "quota content filter requires an exact materialized demand field".into(),
                );
            }
            if contract.kind() != rf::RuntimeFilterProducerKind::Membership {
                return Err("quota content filter requires membership production".into());
            }
            if !arena
                .data_type(expr)
                .is_some_and(novarocks_type_contract::quota_content_runtime_filter_type_supported)
            {
                return Err(
                    "content field has no conservative membership-filter representation".into(),
                );
            }
            let rf::RuntimeFilterExecutionContract::Membership(schema) = contract.contract() else {
                return Err("quota content filter requires a membership schema".into());
            };
            if schema.null_semantics() != rf::RuntimeFilterNullSemantics::NullSafeEqual
                || arena.data_type(expr) != Some(schema.data_type())
            {
                return Err("quota content membership schema must match its exact field and null-safe content semantics".into());
            }
            streams.push(Stream {
                expr,
                contract,
                producer: None,
                sequence: 0,
                terminal: false,
            });
        }
        Ok(Arc::new(Self {
            arena,
            session,
            streams: Mutex::new(streams),
        }))
    }
    fn fail_all(&self, reason: rf::RuntimeFilterProducerFailure) {
        for stream in self
            .streams
            .lock()
            .expect("quota filter state lock")
            .iter_mut()
        {
            stream.fail(reason);
        }
    }
}
impl Stream {
    fn fail(&mut self, reason: rf::RuntimeFilterProducerFailure) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        if let Some(producer) = &self.producer {
            let _ = producer.fail(reason);
        }
    }
    fn closed_error(&mut self, error: rf::RuntimeFilterContractViolation) -> Result<(), String> {
        if error.kind() == rf::RuntimeFilterContractViolationKind::SessionClosed {
            self.terminal = true;
            Ok(())
        } else {
            Err(format!("quota content filter protocol failed: {error}"))
        }
    }
}
impl QuotaContentFilterObserver for NativeQuotaContentFilterObserver {
    fn bind_runtime(&self, _state: &RuntimeState) -> Result<(), String> {
        for stream in self
            .streams
            .lock()
            .expect("quota filter state lock")
            .iter_mut()
        {
            if stream.terminal || stream.producer.is_some() {
                continue;
            }
            match self
                .session
                .open_producer(rf::RuntimeFilterProducerOpenRequest::new(
                    stream.contract.clone(),
                    1,
                )) {
                Ok(rf::RuntimeFilterBindOutcome::Bound(producer)) => {
                    stream.producer = Some(producer)
                }
                Ok(rf::RuntimeFilterBindOutcome::Unavailable(_)) => stream.terminal = true,
                Err(error) => stream.closed_error(error)?,
            }
        }
        Ok(())
    }
    fn activate(&self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }
    fn observe(&self, chunk: &Chunk) -> Result<(), String> {
        for stream in self
            .streams
            .lock()
            .expect("quota filter state lock")
            .iter_mut()
        {
            if stream.terminal {
                continue;
            }
            let array = self.arena.eval(stream.expr, chunk)?;
            let producer = stream
                .producer
                .as_ref()
                .ok_or_else(|| "quota content producer is not bound".to_string())?;
            let contributions = rf::contribution::encode_membership_contributions(
                &stream.contract,
                &array,
                producer.max_contribution_bytes(),
            )
            .map_err(|error| format!("quota content membership encoding failed: {error}"))?;
            let rf::contribution::MembershipContributionEncodingOutcome::Contributions(
                contributions,
            ) = contributions
            else {
                stream.fail(rf::RuntimeFilterProducerFailure::UpstreamUnavailable);
                continue;
            };
            for contribution in contributions {
                match stream.producer.as_ref().expect("bound producer").submit(
                    rf::PartitionId::new(0),
                    rf::ProducerSequence::new(stream.sequence),
                    contribution,
                ) {
                    Ok(rf::RuntimeFilterSubmitOutcome::TerminalNoop) => {
                        stream.terminal = true;
                        break;
                    }
                    Ok(_) => {
                        stream.sequence = stream
                            .sequence
                            .checked_add(1)
                            .ok_or_else(|| "quota content producer sequence overflow".to_string())?
                    }
                    Err(error) => {
                        stream.closed_error(error)?;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
    fn complete(&self) -> Result<(), String> {
        for stream in self
            .streams
            .lock()
            .expect("quota filter state lock")
            .iter_mut()
        {
            if stream.terminal {
                continue;
            }
            let producer = stream.producer.as_ref().ok_or_else(|| {
                "quota content producer is not bound before demand EOS".to_string()
            })?;
            if let Err(error) = producer.close_partition(
                rf::PartitionId::new(0),
                rf::ProducerSequence::new(stream.sequence),
            ) {
                stream.closed_error(error)?;
            }
            stream.terminal = true;
        }
        Ok(())
    }
    fn fail(&self) {
        self.fail_all(rf::RuntimeFilterProducerFailure::ExecutionFailed);
    }
}
impl Drop for NativeQuotaContentFilterObserver {
    fn drop(&mut self) {
        self.fail_all(rf::RuntimeFilterProducerFailure::ExecutionFailed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct Producer {
        contributions: AtomicUsize,
        closed: AtomicUsize,
        failed: AtomicUsize,
    }
    impl rf::RuntimeFilterProducer for Producer {
        fn max_contribution_bytes(&self) -> usize {
            1024
        }
        fn submit(
            &self,
            partition: rf::PartitionId,
            sequence: rf::ProducerSequence,
            _: rf::RuntimeFilterContribution,
        ) -> Result<rf::RuntimeFilterSubmitOutcome, rf::RuntimeFilterContractViolation> {
            assert_eq!(partition.get(), 0);
            assert_eq!(
                sequence.get(),
                self.contributions.fetch_add(1, Ordering::AcqRel) as u64
            );
            Ok(rf::RuntimeFilterSubmitOutcome::Applied)
        }
        fn close_partition(
            &self,
            partition: rf::PartitionId,
            sequence: rf::ProducerSequence,
        ) -> Result<rf::RuntimeFilterSubmitOutcome, rf::RuntimeFilterContractViolation> {
            assert_eq!(partition.get(), 0);
            assert_eq!(
                sequence.get(),
                self.contributions.load(Ordering::Acquire) as u64
            );
            self.closed.fetch_add(1, Ordering::AcqRel);
            Ok(rf::RuntimeFilterSubmitOutcome::Completed)
        }
        fn fail(
            &self,
            _: rf::RuntimeFilterProducerFailure,
        ) -> Result<rf::RuntimeFilterSubmitOutcome, rf::RuntimeFilterContractViolation> {
            self.failed.fetch_add(1, Ordering::AcqRel);
            Ok(rf::RuntimeFilterSubmitOutcome::TerminalNoop)
        }
    }
    struct Session {
        producer: Arc<Producer>,
        opens: AtomicUsize,
    }
    impl rf::RuntimeFilterSession for Session {
        fn open_producer(
            &self,
            request: rf::RuntimeFilterProducerOpenRequest,
        ) -> Result<
            rf::RuntimeFilterBindOutcome<rf::RuntimeFilterProducerHandle>,
            rf::RuntimeFilterContractViolation,
        > {
            assert_eq!(request.local_partition_count(), 1);
            self.opens.fetch_add(1, Ordering::AcqRel);
            Ok(rf::RuntimeFilterBindOutcome::Bound(self.producer.clone()))
        }
        fn subscribe(
            &self,
            _: rf::RuntimeFilterSubscriptionRequest,
        ) -> Result<
            rf::RuntimeFilterBindOutcome<rf::RuntimeFilterSubscriptionHandle>,
            rf::RuntimeFilterContractViolation,
        > {
            unreachable!()
        }
        fn open_final_domain_completion(
            &self,
            _: rf::RuntimeFilterFinalDomainOpenRequest,
        ) -> Result<
            rf::RuntimeFilterBindOutcome<rf::RuntimeFilterFinalDomainCompletionHandle>,
            rf::RuntimeFilterContractViolation,
        > {
            unreachable!()
        }
    }
    fn contract(
        ty: &DataType,
        nulls: rf::RuntimeFilterNullSemantics,
    ) -> rf::RuntimeFilterProducerContract {
        rf::RuntimeFilterProducerContract::membership(
            rf::RuntimeFilterBindingId::new(1),
            rf::RuntimeFilterChannelId::new(2),
            rf::RuntimeFilterExecutionContract::Membership(
                rf::RuntimeFilterMembershipSchema::new(ty, nulls).unwrap(),
            ),
        )
        .unwrap()
    }
    fn fixture() -> (Arc<NativeQuotaContentFilterObserver>, Arc<Session>, Chunk) {
        let mut arena = ExprArena::default();
        let id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let session = Arc::new(Session {
            producer: Arc::new(Producer::default()),
            opens: AtomicUsize::new(0),
        });
        let observer = NativeQuotaContentFilterObserver::try_new(
            vec![(
                id,
                contract(
                    &DataType::Int32,
                    rf::RuntimeFilterNullSemantics::NullSafeEqual,
                ),
            )],
            Arc::new(arena),
            session.clone(),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(2)]))],
        )
        .unwrap();
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[SlotId::new(1)]).unwrap();
        (
            observer,
            session,
            Chunk::new_with_chunk_schema(batch, chunk_schema),
        )
    }
    #[test]
    fn quota_content_producer_opens_once_and_closes_only_at_complete_demand_eos() {
        let (observer, session, chunk) = fixture();
        let state = RuntimeState::default();
        observer.bind_runtime(&state).unwrap();
        observer.bind_runtime(&state).unwrap();
        observer.observe(&chunk).unwrap();
        assert_eq!(session.opens.load(Ordering::Acquire), 1);
        assert_eq!(session.producer.closed.load(Ordering::Acquire), 0);
        observer.complete().unwrap();
        observer.complete().unwrap();
        assert_eq!(session.producer.closed.load(Ordering::Acquire), 1);
        assert!(session.producer.contributions.load(Ordering::Acquire) > 0);
        drop(observer);
        assert_eq!(session.producer.failed.load(Ordering::Acquire), 0);
    }
    #[test]
    fn quota_content_drop_and_failure_never_publish_a_complete_empty_domain() {
        let (observer, session, _) = fixture();
        observer.bind_runtime(&RuntimeState::default()).unwrap();
        drop(observer);
        assert_eq!(session.producer.failed.load(Ordering::Acquire), 1);
        assert_eq!(session.producer.closed.load(Ordering::Acquire), 0);
        let (observer, session, _) = fixture();
        observer.bind_runtime(&RuntimeState::default()).unwrap();
        observer.fail();
        observer.fail();
        observer.complete().unwrap();
        assert_eq!(session.producer.failed.load(Ordering::Acquire), 1);
        assert_eq!(session.producer.closed.load(Ordering::Acquire), 0);
    }
    #[test]
    fn quota_content_rejects_wrong_field_contract_null_semantics_and_float() {
        let (_, session, _) = fixture();
        let mut arena = ExprArena::default();
        let id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        assert!(
            NativeQuotaContentFilterObserver::try_new(
                vec![(
                    id,
                    contract(
                        &DataType::Int64,
                        rf::RuntimeFilterNullSemantics::NullSafeEqual
                    )
                )],
                Arc::new(arena.clone()),
                session.clone()
            )
            .is_err()
        );
        assert!(
            NativeQuotaContentFilterObserver::try_new(
                vec![(
                    id,
                    contract(
                        &DataType::Int32,
                        rf::RuntimeFilterNullSemantics::NeverMatches
                    )
                )],
                Arc::new(arena),
                session.clone()
            )
            .is_err()
        );
        let mut arena = ExprArena::default();
        let id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Float64);
        assert!(
            NativeQuotaContentFilterObserver::try_new(
                vec![(
                    id,
                    contract(
                        &DataType::Float64,
                        rf::RuntimeFilterNullSemantics::NullSafeEqual
                    )
                )],
                Arc::new(arena),
                session
            )
            .is_err()
        );
    }
}
