//! Application-hosted ingress for one exchange receiver.

use crate::exec::chunk::ChunkSchemaRef;
use crate::runtime::exchange::{ExchangeKey, ExchangeReceiverHandle, ExchangeSenderIdentity};
use crate::runtime::execution_runtime::ExecutionRuntime;
use crate::runtime::mem_tracker::MemTracker;
use novarocks_types::UniqueId;
use std::sync::Arc;
use std::time::Instant;

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
    pub expected_chunk_schema: ChunkSchemaRef,
}

/// Application-hosted receiver registry and ingress boundary.
///
/// Execution uses this port instead of a process-global exchange map. The
/// role installs one shared implementation for native ingress and operators.
pub trait ExchangeReceiverPort: Send + Sync + 'static {
    fn register(&self, registration: ExchangeReceiverRegistration) -> Result<(), String>;
    fn push(&self, key: ExchangeReceiverKey, frame: ExchangeReceiverFrame) -> Result<(), String>;
    fn cancel(&self, key: ExchangeReceiverKey);
    fn remove(&self, key: ExchangeReceiverKey);
    fn cancel_fragment(&self, fragment_instance_id: UniqueId);
    fn receiver_handle(
        &self,
        key: ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, String>;
    fn ensure_mem_tracker(
        &self,
        key: ExchangeReceiverKey,
        root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, String>;
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
}

impl ExecutionRuntimeExchangeReceiverPort {
    pub fn new(runtime: Arc<ExecutionRuntime>) -> Self {
        Self { runtime }
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
    fn register(&self, registration: ExchangeReceiverRegistration) -> Result<(), String> {
        self.runtime
            .exchange_registry()
            .try_register_expected_chunk_schema(
                Self::key(registration.key),
                registration.expected_senders,
                registration.expected_chunk_schema,
            )
    }

    fn push(&self, key: ExchangeReceiverKey, frame: ExchangeReceiverFrame) -> Result<(), String> {
        let exchange_key = Self::key(key);
        let decode_start = Instant::now();
        let registry = self.runtime.exchange_registry();
        let sender =
            ExchangeSenderIdentity::native(frame.source_fragment_instance_id, frame.sender_ordinal);
        let chunks = registry.decode_chunks_for_sender(exchange_key, sender, &frame.payload)?;
        registry.push_chunks_with_stats(
            exchange_key,
            sender,
            chunks,
            frame.eos,
            frame.payload.len(),
            decode_start.elapsed().as_nanos(),
        );
        Ok(())
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
    ) -> Result<ExchangeReceiverHandle, String> {
        self.runtime
            .exchange_registry()
            .get_receiver_handle(Self::key(key), expected_senders)
    }

    fn ensure_mem_tracker(
        &self,
        key: ExchangeReceiverKey,
        root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, String> {
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
    fn register(&self, _registration: ExchangeReceiverRegistration) -> Result<(), String> {
        Err("exchange receiver port is unavailable".to_string())
    }

    fn push(&self, _key: ExchangeReceiverKey, _frame: ExchangeReceiverFrame) -> Result<(), String> {
        Err("exchange receiver port is unavailable".to_string())
    }

    fn cancel(&self, _key: ExchangeReceiverKey) {}

    fn remove(&self, _key: ExchangeReceiverKey) {}

    fn cancel_fragment(&self, _fragment_instance_id: UniqueId) {}

    fn receiver_handle(
        &self,
        _key: ExchangeReceiverKey,
        _expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, String> {
        Err("exchange receiver port is unavailable".to_string())
    }

    fn ensure_mem_tracker(
        &self,
        _key: ExchangeReceiverKey,
        _root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, String> {
        Err("exchange receiver port is unavailable".to_string())
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
    fn runtime_port_uses_exact_native_source_identity_for_eos() {
        let port = ExecutionRuntimeExchangeReceiverPort::new(Arc::clone(&test_execution_runtime()));
        let key = ExchangeReceiverKey {
            fragment_instance_id: UniqueId::new(50, 60),
            node_id: 70,
        };
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

        port.push(key, frame(UniqueId::new(1, 1), 0))
            .expect("first native eos");
        port.push(key, frame(UniqueId::new(2, 2), 1))
            .expect("second native eos");

        let snapshot = port.snapshot(key).expect("receiver snapshot");
        assert_eq!(snapshot.finished_senders, 2);
    }
}
