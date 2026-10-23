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
use super::*;
use novarocks_execution_contract::{FragmentNodeId, TaskIdentity};
use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId};

fn stream(node: i32) -> ExchangeStreamIdentity {
    let attempt = QueryExecutionId::new(QueryId::new(37, 41), AttemptId::new(1).unwrap()).unwrap();
    let backend = BackendProcessId::new_v7();
    let task = |id| {
        TaskIdentity::new(
            attempt,
            StageId::new(1).unwrap(),
            TaskId::new(id).unwrap(),
            backend,
        )
    };
    ExchangeStreamIdentity::try_new(
        ExchangeSource::new(task(1), UniqueId::new(7, 11), 0),
        task(2),
        UniqueId::new(13, 17),
        FragmentNodeId::new(node),
    )
    .unwrap()
}
fn contract(
    pool: ExchangeReceivePool,
    stream: ExchangeStreamIdentity,
    delivery: ExchangeDeliveryRequirement,
    ttl: Duration,
) -> ExchangeReceiverNetworkContract {
    ExchangeReceiverNetworkContract::bind(
        pool,
        ExchangeReceiverNetworkSpec {
            streams: Box::from([stream]),
            delivery,
            kind: ExchangeFrameKind::Data,
            per_contributor_bytes: 8 * 1024 * 1024,
            contributors: 1,
            workspace_bytes: 0,
            queue_positions: 1,
            holder_positions: 1,
            minimum_frame_bytes: 1,
            max_wire_bytes: 1024 * 1024,
            max_chunks: 4,
            permit_ttl: ttl,
        },
        MemTracker::new_root("receiver fixture input"),
    )
    .unwrap()
}
fn request(
    stream: ExchangeStreamIdentity,
    sequence: i64,
    kind: ExchangeFrameKind,
    wire: u64,
    decoded: u64,
    scratch: u64,
) -> ExchangePermitRequest {
    ExchangePermitRequest {
        stream,
        sequence: ExchangeFrameSequence::try_new(sequence).unwrap(),
        kind,
        geometry: ExchangeFrameGeometry::try_new(wire, decoded, scratch).unwrap(),
    }
}
async fn granted(
    contract: &ExchangeReceiverNetworkContract,
    request: ExchangePermitRequest,
) -> ExchangePermitGrant {
    match contract
        .acquire(
            request,
            CancellationToken::new(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap()
    {
        ExchangePermitAdmission::Granted(grant) => grant,
        ExchangePermitAdmission::Closed(_) => panic!("unexpected close"),
    }
}
fn registered(
    contract: ExchangeReceiverNetworkContract,
) -> (ExecutionExchangeRegistry, ExchangeKey) {
    let identity = contract.0.state.lock().unwrap().streams[0].identity;
    let key = ExchangeKey {
        finst_id_hi: identity.destination_fragment_instance_id().high(),
        finst_id_lo: identity.destination_fragment_instance_id().low(),
        node_id: identity.destination_node().get(),
    };
    let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),MemTracker::new_root("receiver fixture construction"),Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let layout = crate::exec::chunk::typed_fixture::layout(
        Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "v",
            DataType::Int64,
            false,
        )])),
        &[SlotId::new(1)],
        &[novarocks_type_contract::FunctionValueType::new(
            DataType::Int64,
            false,
        )],
    );
    let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
    let registry = ExecutionExchangeRegistry::default();
    registry
        .try_register_expected_chunk_schema_with_binding(
            key,
            1,
            schema,
            ExchangeColumnBinding::Positional,
            factory,
        )
        .unwrap();
    registry.register_network_receiver(key, contract).unwrap();
    (registry, key)
}
fn pool(bytes: usize, streams: usize) -> ExchangeReceivePool {
    ExchangeReceivePool::bind(
        bytes,
        streams,
        MemTracker::new_root("receiver fixture BE pool"),
        &tokio::runtime::Handle::current(),
    )
    .unwrap()
}
#[tokio::test]
async fn end_ack_is_not_consumed_and_only_real_source_exhaustion_authors_the_close() {
    let source = stream(19);
    let contract = contract(
        pool(4096, 1),
        source,
        ExchangeDeliveryRequirement::MustDrain,
        Duration::from_secs(1),
    );
    let (registry, key) = registered(contract.clone());
    let end = request(source, 0, ExchangeFrameKind::End, 0, 0, 0);
    let grant = granted(&contract, end).await;
    let claim = contract.claim(grant).unwrap();
    assert!(contract.claim(grant).is_err(), "a token is single claim");
    registry.finish_claimed_frame(claim, vec![]).unwrap();
    assert!(
        contract.close_snapshot().is_none(),
        "normal ACK and accepted End cannot author Consumed"
    );
    let receiver = registry.get_receiver_handle(key, 1).unwrap();
    assert!(matches!(
        receiver.try_pop_next_sourced().unwrap(),
        Some(ExchangeSourcedPopResult::Finished(_))
    ));
    let snapshot = receiver.network_close_snapshot().unwrap();
    assert_eq!(snapshot.actual_jobs, 0);
    assert_eq!(
        snapshot.streams.as_ref(),
        &[ExchangeStreamClose {
            stream: source,
            disposition: ExchangeCloseDisposition::Consumed {
                final_sequence: end.sequence
            }
        }]
    );
    assert!(
        matches!(contract.acquire(end,CancellationToken::new(),Instant::now()+Duration::from_secs(1)).await.unwrap(),ExchangePermitAdmission::Closed(close) if close==snapshot.streams[0])
    );
}
#[tokio::test]
async fn codec_result_does_not_retire_the_actual_job_or_original_ingress_scope() {
    let source = stream(19);
    let contract = contract(
        pool(4096, 1),
        source,
        ExchangeDeliveryRequirement::MustDrain,
        Duration::from_secs(1),
    );
    let (registry, key) = registered(contract.clone());
    let grant = granted(
        &contract,
        request(source, 0, ExchangeFrameKind::End, 0, 0, 0),
    )
    .await;
    let mut claim = contract.claim(grant).unwrap();
    struct OriginalIngress(Arc<AtomicUsize>);
    impl ExchangeTransportHold for OriginalIngress {}
    impl Drop for OriginalIngress {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let exits = Arc::new(AtomicUsize::new(0));
    claim
        .retain_transport_lifetime(Arc::new(OriginalIngress(exits.clone())))
        .unwrap();
    let mut job = ClaimedExchangeJob::new(claim, Vec::new());
    job.finish(&registry).unwrap();
    assert_eq!(contract.0.actual_jobs.load(Ordering::Acquire), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    let receiver = registry.get_receiver_handle(key, 1).unwrap();
    assert!(receiver.try_pop_next_sourced().unwrap().is_none());
    assert!(contract.close_snapshot().is_none());
    drop(job);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert!(matches!(
        receiver.try_pop_next_sourced().unwrap(),
        Some(ExchangeSourcedPopResult::Finished(_))
    ));
    assert_eq!(contract.close_snapshot().unwrap().actual_jobs, 0);
}
#[tokio::test]
async fn precise_stream_token_gap_and_end_geometry_are_rejected_without_advancing() {
    let source = stream(19);
    let contract = contract(
        pool(4096, 1),
        source,
        ExchangeDeliveryRequirement::MustDrain,
        Duration::from_secs(1),
    );
    let end = request(source, 0, ExchangeFrameKind::End, 0, 0, 0);
    for invalid in [
        request(source, 1, ExchangeFrameKind::End, 0, 0, 0),
        request(stream(23), 0, ExchangeFrameKind::End, 0, 0, 0),
        request(source, 0, ExchangeFrameKind::End, 1, 0, 0),
    ] {
        assert!(
            contract
                .acquire(
                    invalid,
                    CancellationToken::new(),
                    Instant::now() + Duration::from_secs(1)
                )
                .await
                .is_err()
        );
    }
    let grant = granted(&contract, end).await;
    assert!(
        contract
            .acquire(
                end,
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    let mut forged = grant;
    forged.request.sequence = ExchangeFrameSequence::try_new(1).unwrap();
    assert!(contract.claim(forged).is_err());
    assert!(contract.lookup_grant(source, grant.token).is_ok());
    drop(contract.claim(grant).unwrap());
    assert_eq!(contract.close_snapshot().unwrap().actual_jobs, 0);
    assert!(matches!(
        contract.0.state.lock().unwrap().streams[0].next,
        ExchangeFrameSequence::FIRST
    ));
}
#[tokio::test]
async fn unused_expiry_returns_staging_and_queue_but_claimed_cancellation_waits_for_actual_exit() {
    let shared = pool(1024, 2);
    let a = stream(19);
    let b = stream(23);
    let first = contract(
        shared.clone(),
        a,
        ExchangeDeliveryRequirement::MustDrain,
        Duration::from_millis(20),
    );
    let second = contract(
        shared,
        b,
        ExchangeDeliveryRequirement::DemandDriven,
        Duration::from_secs(1),
    );
    let first_grant = granted(&first, request(a, 0, ExchangeFrameKind::Data, 768, 8, 0)).await;
    let second_grant = granted(&second, request(b, 0, ExchangeFrameKind::Data, 768, 8, 0)).await;
    assert!(
        first.claim(first_grant).is_err(),
        "expiry revoked the unused ticket before allowing another stream"
    );
    let mut claim = second.claim(second_grant).unwrap();
    struct OriginalIngress(Arc<AtomicUsize>);
    impl ExchangeTransportHold for OriginalIngress {}
    impl Drop for OriginalIngress {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let exits = Arc::new(AtomicUsize::new(0));
    claim
        .retain_transport_lifetime(Arc::new(OriginalIngress(exits.clone())))
        .unwrap();
    second.withdraw().unwrap();
    assert_eq!(second.close_snapshot().unwrap().actual_jobs, 1);
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    drop(claim);
    assert_eq!(second.close_snapshot().unwrap().actual_jobs, 0);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn strict_withdrawal_fails_and_demand_driven_preserves_its_exact_close_fact() {
    for delivery in [
        ExchangeDeliveryRequirement::MustDrain,
        ExchangeDeliveryRequirement::DemandDriven,
    ] {
        let source = stream(19);
        let contract = contract(pool(4096, 1), source, delivery, Duration::from_secs(1));
        assert_eq!(
            contract.withdraw().is_err(),
            delivery == ExchangeDeliveryRequirement::MustDrain
        );
        let expected = ExchangeStreamClose {
            stream: source,
            disposition: ExchangeCloseDisposition::Withdrawn,
        };
        assert_eq!(
            contract.close_snapshot().unwrap().streams.as_ref(),
            &[expected]
        );
        let result = contract
            .acquire(
                request(source, 0, ExchangeFrameKind::End, 0, 0, 0),
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(matches!(result,ExchangePermitAdmission::Closed(close) if close==expected));
    }
}
