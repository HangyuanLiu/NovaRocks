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

//! Serial Internal domain state moved between the coordinator and finite CPU jobs.

use crate::query_execution::{
    row_mutation::CowMatchRootConsumer,
    statistics::StatisticsRootResultDecoder,
    write_result::{DecodedPreparedWriteSet, RootWriteResultDecoder},
};
use novarocks_execution_contract::root_result::RootResultEnd;
use novarocks_query_application::api::{RetainedRootReply, RootReplyView};
use novarocks_spi::connector::{ConnectorRowMutationSelection, StatisticsArtifactDraft};

pub(super) enum InternalDomainState {
    Statistics(StatisticsRootResultDecoder),
    Write(RootWriteResultDecoder),
    Cow(CowMatchRootConsumer),
}

pub(super) enum InternalDomainCompletion {
    Statistics(StatisticsRootResultDecoder),
    Write(DecodedPreparedWriteSet),
    Cow(ConnectorRowMutationSelection),
}

pub(super) enum InternalDomainSlot {
    Collecting {
        state: InternalDomainState,
        receipt: Option<u64>,
    },
    Eof {
        completion: InternalDomainCompletion,
        end: RootResultEnd,
    },
    StatisticsFinished(Vec<StatisticsArtifactDraft>),
}
impl InternalDomainSlot {
    pub(super) fn receipt(&self) -> Option<u64> {
        match self {
            Self::Collecting { receipt, .. } => *receipt,
            _ => None,
        }
    }
    pub(super) fn end(&self) -> Option<RootResultEnd> {
        match self {
            Self::Eof { end, .. } => Some(*end),
            _ => None,
        }
    }
    pub(super) fn statistics_finished(&self) -> bool {
        matches!(self, Self::StatisticsFinished(_))
    }
    pub(super) fn apply_body(self, reply: RetainedRootReply) -> Result<Self, String> {
        let Self::Collecting { mut state, .. } = self else {
            return Err("internal domain received data after EOF".into());
        };
        let RootReplyView::Data { sequence, body, .. } = reply.outcome() else {
            return Err("internal domain data owner has no data".into());
        };
        let result = match &mut state {
            InternalDomainState::Statistics(decoder) => decoder.apply_relay_body(body),
            InternalDomainState::Write(decoder) => decoder.apply_relay_body(body),
            InternalDomainState::Cow(consumer) => consumer.push_body(body),
        };
        // Receipt contains no body alias. Actual retained reply destruction
        // precedes CPU completion, local consumption and the remote ACK.
        drop(reply);
        result?;
        Ok(Self::Collecting {
            state,
            receipt: Some(sequence.get() - 1),
        })
    }
    pub(super) fn finish_eof(self, end: RootResultEnd) -> Result<Self, String> {
        let Self::Collecting { state, .. } = self else {
            return Err("internal domain received duplicate EOF".into());
        };
        let completion = match state {
            InternalDomainState::Statistics(mut decoder) => {
                decoder.check_relay_end(end.output_rows)?;
                decoder.observe_root_eof()?;
                InternalDomainCompletion::Statistics(decoder)
            }
            InternalDomainState::Write(mut decoder) => {
                decoder.check_relay_end(end.output_rows)?;
                decoder.observe_root_eof()?;
                InternalDomainCompletion::Write(decoder.finish()?)
            }
            InternalDomainState::Cow(consumer) => {
                consumer.check_end(end.output_rows)?;
                InternalDomainCompletion::Cow(consumer.finish()?)
            }
        };
        Ok(Self::Eof { completion, end })
    }
    pub(super) fn finish_statistics(self) -> Result<Self, String> {
        let Self::Eof {
            completion: InternalDomainCompletion::Statistics(mut decoder),
            ..
        } = self
        else {
            return Err("statistics finalization requires its checked EOF".into());
        };
        decoder.observe_execution_success()?;
        Ok(Self::StatisticsFinished(decoder.finish()?))
    }
    pub(super) fn into_write(self) -> Result<DecodedPreparedWriteSet, String> {
        match self {
            Self::Eof {
                completion: InternalDomainCompletion::Write(prepared),
                ..
            } => Ok(prepared),
            _ => Err("write lost its CPU-validated prepared set".into()),
        }
    }
    pub(super) fn into_cow(self) -> Result<ConnectorRowMutationSelection, String> {
        match self {
            Self::Eof {
                completion: InternalDomainCompletion::Cow(selection),
                ..
            } => Ok(selection),
            _ => Err("COW lost its CPU-validated selection".into()),
        }
    }
    pub(super) fn into_statistics(self) -> Result<Vec<StatisticsArtifactDraft>, String> {
        match self {
            Self::StatisticsFinished(artifacts) => Ok(artifacts),
            _ => Err("statistics lost its CPU-validated artifact membership".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::data_runtime::FrontendDataRuntime;
    use crate::query_execution::internal_result_cpu::{
        InternalResultCpuOwner, InternalResultValue, admitted_internal_fixture,
    };
    use crate::task_execution::status_intake::CondvarWake;
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use std::{
        num::NonZeroU64,
        sync::Arc,
        time::{Duration, Instant},
    };

    async fn claim(
        job: &mut crate::query_execution::internal_result_cpu::InternalResultCpuJob<
            Result<InternalDomainSlot, String>,
        >,
    ) -> Result<InternalResultValue<InternalDomainSlot>, String> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(result) = job.try_take() {
                    break result.and_then(|value| value.try_transform(|slot, _| slot));
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("domain CPU watchdog")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn statistics_eof_and_all_success_finalization_are_distinct_cpu_receipts() {
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let cancellation = QueryCancellationSource::new();
        let end = RootResultEnd {
            sequence: NonZeroU64::new(1).unwrap(),
            output_rows: 0,
        };
        let input =
            InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                Ok(InternalDomainSlot::Collecting {
                    state: InternalDomainState::Statistics(
                        StatisticsRootResultDecoder::for_test_with_capacity([], &binding),
                    ),
                    receipt: None,
                })
            })
            .unwrap()
            .track_actual_exit(&binding)
            .unwrap();
        let mut eof = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(CondvarWake::default()),
            move |slot, _| slot.finish_eof(end),
        );
        let checked_eof = claim(&mut eof).await.unwrap();
        binding.wait_result_activities_exited();
        assert_eq!(checked_eof.value().end(), Some(end));
        assert!(!checked_eof.value().statistics_finished());
        assert!(checked_eof.value().receipt().is_none());
        let mut finalization = owner.runtime().submit(
            checked_eof.track_actual_exit(&binding).unwrap(),
            cancellation.view(),
            &runtime,
            Arc::new(CondvarWake::default()),
            |slot, _| slot.finish_statistics(),
        );
        let finished = claim(&mut finalization).await.unwrap();
        binding.wait_result_activities_exited();
        assert!(finished.value().statistics_finished());
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        let artifacts = finished.hand_off(|slot, _| slot.into_statistics()).unwrap();
        assert!(artifacts.is_empty());
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bad_domain_body_never_produces_an_ack_receipt() {
        use novarocks_execution_contract::{
            identity::TaskIdentity,
            root_result::{RootReadOutcome, RootResultData, RootResultReply},
        };
        use novarocks_result_contract::{InternalResultDomain, RootOutputKind, RootProfileId};
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let cancellation = QueryCancellationSource::new();
        let kind = RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1);
        let reply = RetainedRootReply::try_new(
            RootResultReply {
                root_task: TaskIdentity::new(
                    QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
                    StageId::new(1).unwrap(),
                    TaskId::new(1).unwrap(),
                    BackendProcessId::new_v7(),
                ),
                profile: RootProfileId::V1,
                kind,
                accepted_consumed: 0,
                outcome: RootReadOutcome::Data(
                    RootResultData::try_new(
                        kind,
                        NonZeroU64::new(1).unwrap(),
                        bytes::Bytes::from_static(
                            b"invalid record prefix with enough bytes to fail validation",
                        ),
                        None,
                    )
                    .unwrap(),
                ),
            },
            binding.window_alias(),
            1024,
        )
        .unwrap();
        let input =
            InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                Ok(InternalDomainSlot::Collecting {
                    state: InternalDomainState::Statistics(
                        StatisticsRootResultDecoder::for_test_with_capacity([], &binding),
                    ),
                    receipt: None,
                })
            })
            .unwrap()
            .transform(|slot, _| (slot, reply))
            .track_actual_exit(&binding)
            .unwrap();
        let mut job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(CondvarWake::default()),
            |(slot, reply), _| slot.apply_body(reply),
        );
        assert!(claim(&mut job).await.is_err());
        binding.wait_result_activities_exited();
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
