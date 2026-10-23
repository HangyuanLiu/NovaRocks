//! Application-hosted ingress for one exchange receiver.

use crate::exec::chunk::ChunkSchemaRef;
pub use crate::runtime::exchange::{
    ClaimedExchangeFrame, ExchangePermitAdmission, ExchangeReceiveFuture, ExchangeReceivePool,
    ExchangeReceiverCloseSnapshot, ExchangeReceiverNetworkContract, ExchangeReceiverNetworkSpec,
    ExchangeTransportHold,
};
use crate::runtime::exchange::{
    ExchangeColumnBinding, ExchangeKey, ExchangeReceiverHandle, ExchangeSenderIdentity,
};
use crate::runtime::execution_runtime::ExecutionRuntime;
use crate::runtime::mem_tracker::MemTracker;
use novarocks_execution_contract::{
    ExchangePermitGrant, ExchangePermitRequest, ExchangePermitToken, ExchangeStreamIdentity,
};
use novarocks_types::UniqueId;
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExchangeReceiverKey {
    pub fragment_instance_id: UniqueId,
    pub node_id: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeReceiverFrame {
    pub source_fragment_instance_id: UniqueId,
    pub sender_ordinal: u32,
    pub sender_count: u32,
    pub sender_id: i32,
    pub backend_number: i32,
    pub sequence: i64,
    pub eos: bool,
    pub payload: Vec<u8>,
}

#[derive(Clone)]
pub struct ExchangeReceiverRegistration {
    pub key: ExchangeReceiverKey,
    pub expected_senders: usize,
    /// None is the explicit local-only profile and grants no native ingress.
    pub network: Option<ExchangeReceiverNetworkContract>,
    pub expected_chunk_schema: ChunkSchemaRef,
    /// How decoded wire columns bind to `expected_chunk_schema`. Legacy
    /// fragments bind by slot id; compiled fragments bind by position.
    pub column_binding: ExchangeColumnBinding,
    pub creation_factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
}

/// Application-hosted receiver registry and ingress boundary.
///
/// Execution uses this port instead of a process-global exchange map. The
/// role installs one shared implementation for native ingress and operators.
pub trait ExchangeReceiverPort: Send + Sync + 'static {
    fn acquire(
        &self,
        request: ExchangePermitRequest,
        stop: CancellationToken,
        deadline: Instant,
    ) -> ExchangeReceiveFuture<'_, ExchangePermitAdmission>;
    fn lookup_grant(
        &self,
        stream: ExchangeStreamIdentity,
        token: ExchangePermitToken,
    ) -> Result<ExchangePermitGrant, crate::runtime::exchange::ExchangeFailure>;
    fn claim(
        &self,
        grant: ExchangePermitGrant,
    ) -> Result<ClaimedExchangeFrame, crate::runtime::exchange::ExchangeFailure>;
    fn accept(
        &self,
        claim: ClaimedExchangeFrame,
        payload: Vec<u8>,
    ) -> ExchangeReceiveFuture<'_, ()>;
    fn close_snapshot(
        &self,
        key: ExchangeReceiverKey,
    ) -> Result<Option<ExchangeReceiverCloseSnapshot>, crate::runtime::exchange::ExchangeFailure>;
    fn withdraw(
        &self,
        key: ExchangeReceiverKey,
    ) -> Result<Option<ExchangeReceiverCloseSnapshot>, crate::runtime::exchange::ExchangeFailure>;

    fn register(
        &self,
        registration: ExchangeReceiverRegistration,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure>;
    fn push(
        &self,
        key: ExchangeReceiverKey,
        frame: ExchangeReceiverFrame,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure>;
    fn cancel(&self, key: ExchangeReceiverKey);
    fn remove(&self, key: ExchangeReceiverKey);
    fn cancel_fragment(&self, fragment_instance_id: UniqueId);
    fn receiver_handle(
        &self,
        key: ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, crate::runtime::exchange::ExchangeFailure>;
    fn ensure_mem_tracker(
        &self,
        key: ExchangeReceiverKey,
        root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, crate::runtime::exchange::ExchangeFailure>;
    fn push_local(
        &self,
        key: ExchangeReceiverKey,
        sender_id: i32,
        backend_number: i32,
        chunks: Vec<crate::exec::chunk::Chunk>,
        eos: bool,
    );
    fn snapshot(
        &self,
        key: ExchangeReceiverKey,
    ) -> Option<crate::runtime::exchange::ExchangeReceiverSnapshot>;
}

/// Runtime-owned bridge from process ingress frames to one execution runtime's
/// exchange registry.
///
/// The adapter has no role, listener, or Worker lifecycle state. A host gives
/// the same instance to native ingress and fragment operators so they share
/// the one execution runtime registry for the process.
#[derive(Clone, Debug)]
pub struct ExecutionRuntimeExchangeReceiverPort {
    runtime: Arc<ExecutionRuntime>,
    #[cfg(novarocks_dev_controls)]
    test_observer: Option<Arc<dyn crate::runtime::exchange::TestEventObserver>>,
}

impl ExecutionRuntimeExchangeReceiverPort {
    pub fn new(runtime: Arc<ExecutionRuntime>) -> Self {
        Self {
            runtime,
            #[cfg(novarocks_dev_controls)]
            test_observer: None,
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_test_observer(
        mut self,
        observer: Arc<dyn crate::runtime::exchange::TestEventObserver>,
    ) -> Self {
        self.test_observer = Some(observer);
        self
    }

    fn key(key: ExchangeReceiverKey) -> ExchangeKey {
        ExchangeKey {
            finst_id_hi: key.fragment_instance_id.high(),
            finst_id_lo: key.fragment_instance_id.low(),
            node_id: key.node_id,
        }
    }
}

impl ExchangeReceiverPort for ExecutionRuntimeExchangeReceiverPort {
    fn acquire(
        &self,
        request: ExchangePermitRequest,
        stop: CancellationToken,
        deadline: Instant,
    ) -> ExchangeReceiveFuture<'_, ExchangePermitAdmission> {
        Box::pin(async move {
            let key = ExchangeReceiverKey {
                fragment_instance_id: request.stream.destination_fragment_instance_id(),
                node_id: request.stream.destination_node().get(),
            };
            let contract = self
                .runtime
                .exchange_registry()
                .network_receiver(Self::key(key))?;
            contract.acquire(request, stop, deadline).await
        })
    }
    fn lookup_grant(
        &self,
        stream: ExchangeStreamIdentity,
        token: ExchangePermitToken,
    ) -> Result<ExchangePermitGrant, crate::runtime::exchange::ExchangeFailure> {
        let key = ExchangeKey {
            finst_id_hi: stream.destination_fragment_instance_id().high(),
            finst_id_lo: stream.destination_fragment_instance_id().low(),
            node_id: stream.destination_node().get(),
        };
        self.runtime
            .exchange_registry()
            .network_receiver(key)?
            .lookup_grant(stream, token)
    }
    fn claim(
        &self,
        grant: ExchangePermitGrant,
    ) -> Result<ClaimedExchangeFrame, crate::runtime::exchange::ExchangeFailure> {
        self.runtime.exchange_registry().claim_exchange_frame(grant)
    }
    fn accept(
        &self,
        claim: ClaimedExchangeFrame,
        payload: Vec<u8>,
    ) -> ExchangeReceiveFuture<'_, ()> {
        let registry = self.runtime.exchange_registry();
        let executor = self.runtime.exchange_codec_executor();
        let mut job = crate::runtime::exchange::ClaimedExchangeJob::new(claim, payload);
        Box::pin(async move {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            // The closure owns claim, raw backing and both windows through real
            // exit. Dropping this future only drops its result observation.
            executor.submit(move |_context| {
                let result = job.finish(&registry);
                let _ = sender.send(result);
                drop(job);
            });
            receiver.await.map_err(|error| {
                crate::runtime::exchange::ExchangeFailure::metadata_channel(error)
            })?
        })
    }
    fn close_snapshot(
        &self,
        key: ExchangeReceiverKey,
    ) -> Result<Option<ExchangeReceiverCloseSnapshot>, crate::runtime::exchange::ExchangeFailure>
    {
        let registry = self.runtime.exchange_registry();
        let contract = registry.network_receiver(Self::key(key))?;
        Ok(contract.close_snapshot())
    }
    fn withdraw(
        &self,
        key: ExchangeReceiverKey,
    ) -> Result<Option<ExchangeReceiverCloseSnapshot>, crate::runtime::exchange::ExchangeFailure>
    {
        let registry = self.runtime.exchange_registry();
        let contract = registry.network_receiver(Self::key(key))?;
        contract.withdraw().map(Some)
    }

    fn register(
        &self,
        registration: ExchangeReceiverRegistration,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        let key = Self::key(registration.key);
        self.runtime
            .exchange_registry()
            .try_register_expected_chunk_schema_with_binding(
                key,
                registration.expected_senders,
                registration.expected_chunk_schema,
                registration.column_binding,
                registration.creation_factory,
            )?;
        if let Some(network) = registration.network {
            self.runtime
                .exchange_registry()
                .register_network_receiver(key, network)?;
        }
        #[cfg(novarocks_dev_controls)]
        if let Some(observer) = self.test_observer.clone() {
            self.runtime
                .exchange_registry()
                .install_test_observer(key, observer)?;
        }
        Ok(())
    }

    fn push(
        &self,
        key: ExchangeReceiverKey,
        frame: ExchangeReceiverFrame,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        let _ = (key, frame);
        Err(crate::runtime::exchange::ExchangeFailure::unpermitted_native_frame())
    }

    fn cancel(&self, key: ExchangeReceiverKey) {
        self.runtime
            .exchange_registry()
            .cancel_exchange_key(Self::key(key));
    }

    fn remove(&self, key: ExchangeReceiverKey) {
        self.runtime
            .exchange_registry()
            .remove_exchange_key(Self::key(key));
    }

    fn cancel_fragment(&self, fragment_instance_id: UniqueId) {
        self.runtime
            .exchange_registry()
            .cancel_fragment(fragment_instance_id.high(), fragment_instance_id.low());
    }

    fn receiver_handle(
        &self,
        key: ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, crate::runtime::exchange::ExchangeFailure> {
        self.runtime
            .exchange_registry()
            .get_receiver_handle(Self::key(key), expected_senders)
    }

    fn ensure_mem_tracker(
        &self,
        key: ExchangeReceiverKey,
        root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, crate::runtime::exchange::ExchangeFailure> {
        self.runtime
            .exchange_registry()
            .ensure_receiver_mem_tracker(Self::key(key), root)
    }

    fn push_local(
        &self,
        key: ExchangeReceiverKey,
        sender_id: i32,
        backend_number: i32,
        chunks: Vec<crate::exec::chunk::Chunk>,
        eos: bool,
    ) {
        self.runtime.exchange_registry().push_chunks(
            Self::key(key),
            ExchangeSenderIdentity::local(sender_id, backend_number),
            chunks,
            eos,
        );
    }

    fn snapshot(
        &self,
        key: ExchangeReceiverKey,
    ) -> Option<crate::runtime::exchange::ExchangeReceiverSnapshot> {
        self.runtime
            .exchange_registry()
            .snapshot_receiver_state(Self::key(key))
    }
}

#[derive(Debug, Default)]
pub struct UnavailableExchangeReceiverPort;

impl ExchangeReceiverPort for UnavailableExchangeReceiverPort {
    fn acquire(
        &self,
        _request: ExchangePermitRequest,
        _stop: CancellationToken,
        _deadline: Instant,
    ) -> ExchangeReceiveFuture<'_, ExchangePermitAdmission> {
        Box::pin(async { Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable()) })
    }
    fn lookup_grant(
        &self,
        _stream: ExchangeStreamIdentity,
        _token: ExchangePermitToken,
    ) -> Result<ExchangePermitGrant, crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }
    fn claim(
        &self,
        _grant: ExchangePermitGrant,
    ) -> Result<ClaimedExchangeFrame, crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }
    fn accept(
        &self,
        _claim: ClaimedExchangeFrame,
        _payload: Vec<u8>,
    ) -> ExchangeReceiveFuture<'_, ()> {
        Box::pin(async { Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable()) })
    }
    fn close_snapshot(
        &self,
        _key: ExchangeReceiverKey,
    ) -> Result<Option<ExchangeReceiverCloseSnapshot>, crate::runtime::exchange::ExchangeFailure>
    {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }
    fn withdraw(
        &self,
        _key: ExchangeReceiverKey,
    ) -> Result<Option<ExchangeReceiverCloseSnapshot>, crate::runtime::exchange::ExchangeFailure>
    {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn register(
        &self,
        _registration: ExchangeReceiverRegistration,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn push(
        &self,
        _key: ExchangeReceiverKey,
        _frame: ExchangeReceiverFrame,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn cancel(&self, _key: ExchangeReceiverKey) {}

    fn remove(&self, _key: ExchangeReceiverKey) {}

    fn cancel_fragment(&self, _fragment_instance_id: UniqueId) {}

    fn receiver_handle(
        &self,
        _key: ExchangeReceiverKey,
        _expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn ensure_mem_tracker(
        &self,
        _key: ExchangeReceiverKey,
        _root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn push_local(
        &self,
        _key: ExchangeReceiverKey,
        _sender_id: i32,
        _backend_number: i32,
        _chunks: Vec<crate::exec::chunk::Chunk>,
        _eos: bool,
    ) {
    }

    fn snapshot(
        &self,
        _key: ExchangeReceiverKey,
    ) -> Option<crate::runtime::exchange::ExchangeReceiverSnapshot> {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        ExchangeReceiverFrame, ExchangeReceiverKey, ExchangeReceiverPort,
        ExecutionRuntimeExchangeReceiverPort,
    };
    use crate::runtime::execution_runtime::test_execution_runtime;
    use novarocks_types::UniqueId;

    #[test]
    fn local_receiver_registration_does_not_authorize_native_frames_without_permits() {
        let port = ExecutionRuntimeExchangeReceiverPort::new(Arc::clone(&test_execution_runtime()));
        let key = ExchangeReceiverKey {
            fragment_instance_id: UniqueId::new(50, 60),
            node_id: 70,
        };
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("exact native EOS fixture");
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(), std::sync::Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        let layout = crate::exec::chunk::typed_fixture::layout(
            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            &[],
            &[],
        );
        let expected = factory.create_compiled_chunk_schema(&layout).unwrap();
        port.register(crate::runtime::fragment::io::ExchangeReceiverRegistration {
            key,
            expected_senders: 2,
            network: None,
            expected_chunk_schema: expected,
            column_binding: crate::runtime::exchange::ExchangeColumnBinding::Positional,
            creation_factory: factory.clone(),
        })
        .expect("actual native receiver installation");
        let frame = |source_fragment_instance_id, sender_ordinal| ExchangeReceiverFrame {
            source_fragment_instance_id,
            sender_ordinal,
            sender_count: 2,
            // These legacy fields intentionally collide. Native completion
            // identity must not use either of them.
            sender_id: 7,
            backend_number: 11,
            sequence: 0,
            eos: true,
            payload: Vec::new(),
        };

        assert!(port.push(key, frame(UniqueId::new(1, 1), 0)).is_err());
        assert!(port.push(key, frame(UniqueId::new(2, 2), 1)).is_err());

        let snapshot = port.snapshot(key).expect("receiver snapshot");
        assert_eq!(snapshot.finished_senders, 0);
        port.remove(key);
        assert!(port.snapshot(key).is_none());
        drop(factory);
        assert_eq!(
            tracker.current(),
            0,
            "removed receiver releases its finite creation"
        );
    }
}
