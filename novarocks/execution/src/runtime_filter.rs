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

use std::any::Any;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::DataType;

macro_rules! id {
    ($name:ident, $raw:ty) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name($raw);
        impl $name {
            pub const fn new(raw: $raw) -> Self {
                Self(raw)
            }
            pub const fn get(self) -> $raw {
                self.0
            }
        }
    };
}

id!(RuntimeFilterBindingId, u32);
id!(RuntimeFilterChannelId, u32);
id!(PartitionId, u32);
id!(ProducerSequence, u64);
id!(LogicalVersion, u64);

impl LogicalVersion {
    pub const FIRST: Self = Self(1);
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterExecutionContract {
    Membership {
        canonical_schema: Arc<[u8]>,
        schema_digest: [u8; 32],
    },
    Ordered {
        keys: Arc<[RuntimeOrderKey]>,
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeOrderSortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeOrderNullOrder {
    First,
    Last,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeOrderKey {
    data_type: DataType,
    direction: RuntimeOrderSortDirection,
    null_order: RuntimeOrderNullOrder,
}

impl RuntimeOrderKey {
    pub const fn new(
        data_type: DataType,
        direction: RuntimeOrderSortDirection,
        null_order: RuntimeOrderNullOrder,
    ) -> Self {
        Self {
            data_type,
            direction,
            null_order,
        }
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn direction(&self) -> RuntimeOrderSortDirection {
        self.direction
    }

    pub const fn null_order(&self) -> RuntimeOrderNullOrder {
        self.null_order
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterProducerKind {
    Membership,
    OrderedBound,
    TopKSummary,
    FinalDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterContributionKind {
    Membership,
    OrderedBound,
    TopKSummary,
    FinalDomain,
}

/// Immutable canonical contribution bytes. The execution surface owns the
/// producer vocabulary; a role-local adapter owns codec validation and reducer
/// delivery for the installed contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterContribution {
    kind: RuntimeFilterContributionKind,
    contract_digest: [u8; 32],
    canonical_bytes: Arc<[u8]>,
}

/// A frozen, fragment-local membership domain submitted through a final-domain
/// completion fence.
///
/// The execution surface owns the lifecycle of this payload, while a
/// role-local session adapter validates and converts the opaque value into its
/// reducer representation. `canonical_bytes` gives callers a stable size
/// bound without exposing a transport envelope or a service-issued authority.
#[derive(Clone)]
pub struct RuntimeFilterFinalDomain {
    canonical_bytes: Arc<[u8]>,
    value: Arc<dyn Any + Send + Sync>,
}

impl RuntimeFilterFinalDomain {
    pub fn new<T>(canonical_bytes: impl Into<Arc<[u8]>>, value: T) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            canonical_bytes: canonical_bytes.into(),
            value: Arc::new(value),
        }
    }

    pub const fn canonical_bytes(&self) -> &Arc<[u8]> {
        &self.canonical_bytes
    }

    /// This is for a role-local session adapter only. Execution callers pass
    /// the opaque payload to a typed completion capability; they never mint
    /// completion authority or address a delivery route.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.value.downcast_ref()
    }
}

impl fmt::Debug for RuntimeFilterFinalDomain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeFilterFinalDomain")
            .field("canonical_bytes_len", &self.canonical_bytes.len())
            .finish_non_exhaustive()
    }
}

impl RuntimeFilterContribution {
    pub fn new(
        kind: RuntimeFilterContributionKind,
        contract_digest: [u8; 32],
        canonical_bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            kind,
            contract_digest,
            canonical_bytes: canonical_bytes.into(),
        }
    }

    pub const fn kind(&self) -> RuntimeFilterContributionKind {
        self.kind
    }

    pub const fn canonical_bytes(&self) -> &Arc<[u8]> {
        &self.canonical_bytes
    }

    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerActivation {
    BlockingSnapshot,
    NonBlockingLive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterContractViolationKind {
    MissingSession,
    UnauthorizedBinding,
    ContractMismatch,
    RoleMismatch,
    InvalidPartitionCount,
    SessionClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterContractViolation {
    kind: RuntimeFilterContractViolationKind,
    detail: Arc<str>,
}

impl RuntimeFilterContractViolation {
    pub fn new(kind: RuntimeFilterContractViolationKind, detail: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }
    pub const fn kind(&self) -> RuntimeFilterContractViolationKind {
        self.kind
    }
    pub fn detail(&self) -> &str {
        &self.detail
    }
}
impl fmt::Display for RuntimeFilterContractViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "runtime-filter contract violation {:?}: {}",
            self.kind, self.detail
        )
    }
}
impl Error for RuntimeFilterContractViolation {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterProducerContract {
    binding_id: RuntimeFilterBindingId,
    channel_id: RuntimeFilterChannelId,
    kind: RuntimeFilterProducerKind,
    contract: RuntimeFilterExecutionContract,
}
impl RuntimeFilterProducerContract {
    pub const fn new(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        kind: RuntimeFilterProducerKind,
        contract: RuntimeFilterExecutionContract,
    ) -> Self {
        Self {
            binding_id,
            channel_id,
            kind,
            contract,
        }
    }
    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }
    pub const fn channel_id(&self) -> RuntimeFilterChannelId {
        self.channel_id
    }
    pub const fn kind(&self) -> RuntimeFilterProducerKind {
        self.kind
    }
    pub const fn contract(&self) -> &RuntimeFilterExecutionContract {
        &self.contract
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterConsumerContract {
    binding_id: RuntimeFilterBindingId,
    channel_id: RuntimeFilterChannelId,
    activation: ConsumerActivation,
    contract: RuntimeFilterExecutionContract,
}
impl RuntimeFilterConsumerContract {
    pub const fn new(
        binding_id: RuntimeFilterBindingId,
        channel_id: RuntimeFilterChannelId,
        activation: ConsumerActivation,
        contract: RuntimeFilterExecutionContract,
    ) -> Self {
        Self {
            binding_id,
            channel_id,
            activation,
            contract,
        }
    }
    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }
    pub const fn channel_id(&self) -> RuntimeFilterChannelId {
        self.channel_id
    }
    pub const fn activation(&self) -> ConsumerActivation {
        self.activation
    }
    pub const fn contract(&self) -> &RuntimeFilterExecutionContract {
        &self.contract
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterProducerOpenRequest {
    contract: RuntimeFilterProducerContract,
    local_partition_count: u32,
}

/// Opens the aggregate-only completion fence associated with a FinalDomain
/// producer binding. It is deliberately separate from the regular producer
/// handle: the adapter retains issuance authority until every claimed local
/// partition has frozen and closed exactly once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterFinalDomainOpenRequest {
    contract: RuntimeFilterProducerContract,
    local_partition_count: u32,
}

impl RuntimeFilterFinalDomainOpenRequest {
    pub const fn new(contract: RuntimeFilterProducerContract, local_partition_count: u32) -> Self {
        Self {
            contract,
            local_partition_count,
        }
    }

    pub const fn contract(&self) -> &RuntimeFilterProducerContract {
        &self.contract
    }

    pub const fn local_partition_count(&self) -> u32 {
        self.local_partition_count
    }
}
impl RuntimeFilterProducerOpenRequest {
    pub const fn new(contract: RuntimeFilterProducerContract, local_partition_count: u32) -> Self {
        Self {
            contract,
            local_partition_count,
        }
    }
    pub const fn contract(&self) -> &RuntimeFilterProducerContract {
        &self.contract
    }
    pub const fn local_partition_count(&self) -> u32 {
        self.local_partition_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterSubscriptionRequest {
    contract: RuntimeFilterConsumerContract,
}
impl RuntimeFilterSubscriptionRequest {
    pub const fn new(contract: RuntimeFilterConsumerContract) -> Self {
        Self { contract }
    }
    pub const fn contract(&self) -> &RuntimeFilterConsumerContract {
        &self.contract
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterBindOutcome<T> {
    Bound(T),
    Unavailable(UnavailableReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnavailableReason {
    ResourceLimit,
    IncompleteCoverage,
    ProducerFailed,
    MaterializationFailed,
    RouteUnavailable,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactUnsupportedReason {
    RangeDeferred,
    NoAcceptedRepresentation,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveTerminal {
    Completed,
    CompletedWithoutArtifact,
    Unavailable(UnavailableReason),
    Cancelled,
}

pub trait RuntimeFilterPredicate: Send + Sync {
    /// Allows a role-local execution adapter to expose an optional, typed
    /// evaluator extension without exposing delivery artifacts or Service
    /// state through the session/snapshot surface.
    fn as_any(&self) -> &dyn Any;

    fn evaluate(&self, input: &ArrayRef) -> Result<BooleanArray, RuntimeFilterContractViolation>;
}

#[derive(Clone)]
pub struct RuntimeFilterSnapshot {
    binding_id: RuntimeFilterBindingId,
    logical_version: LogicalVersion,
    contract_digest: [u8; 32],
    predicate: Arc<dyn RuntimeFilterPredicate>,
}
impl RuntimeFilterSnapshot {
    pub fn new(
        binding_id: RuntimeFilterBindingId,
        logical_version: LogicalVersion,
        contract_digest: [u8; 32],
        predicate: Arc<dyn RuntimeFilterPredicate>,
    ) -> Self {
        Self {
            binding_id,
            logical_version,
            contract_digest,
            predicate,
        }
    }
    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }
    pub const fn logical_version(&self) -> LogicalVersion {
        self.logical_version
    }
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
    pub const fn predicate(&self) -> &Arc<dyn RuntimeFilterPredicate> {
        &self.predicate
    }
}

#[derive(Clone)]
pub enum SnapshotAcquireOutcome {
    Published(Arc<RuntimeFilterSnapshot>),
    Unsupported(ArtifactUnsupportedReason),
    Unavailable(UnavailableReason),
    Cancelled,
    TimedOut,
}
pub enum LivePollOutcome {
    Updated {
        snapshot: Arc<RuntimeFilterSnapshot>,
        terminal: Option<LiveTerminal>,
    },
    Idle {
        latest_version: Option<LogicalVersion>,
        terminal: Option<LiveTerminal>,
    },
}

pub trait BlockingSnapshotSubscription: Send + Sync {
    fn acquire(&self, timeout: Duration) -> SnapshotAcquireOutcome;
    fn snapshot(&self) -> Option<Arc<RuntimeFilterSnapshot>>;
}
pub trait NonBlockingLiveSubscription: Send + Sync {
    fn snapshot(&self) -> Option<Arc<RuntimeFilterSnapshot>>;
    fn poll_after(&self, observed: Option<LogicalVersion>) -> LivePollOutcome;
}
pub enum RuntimeFilterSubscriptionHandle {
    Blocking(Arc<dyn BlockingSnapshotSubscription>),
    Live(Arc<dyn NonBlockingLiveSubscription>),
}

pub trait RuntimeFilterProducer: Send + Sync {
    /// The maximum canonical contribution payload accepted by this exact
    /// fragment-local producer route. It is an execution capability, not a
    /// deployment descriptor, so callers can bound local encoding without
    /// observing the installed service state.
    fn max_contribution_bytes(&self) -> usize;

    fn submit(
        &self,
        partition: PartitionId,
        sequence: ProducerSequence,
        contribution: RuntimeFilterContribution,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation>;
    fn close_partition(
        &self,
        partition: PartitionId,
        terminal: ProducerSequence,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation>;
    fn fail(
        &self,
        reason: RuntimeFilterProducerFailure,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation>;
}
pub type RuntimeFilterProducerHandle = Arc<dyn RuntimeFilterProducer>;

/// One-shot ownership of a FinalDomain partition. A caller must freeze a
/// domain then close it; dropping an unclosed handle is terminal failure for
/// the whole completion fence.
pub trait RuntimeFilterFinalDomainPartition: Send {
    fn seal(
        &mut self,
        domain: RuntimeFilterFinalDomain,
    ) -> Result<(), RuntimeFilterContractViolation>;
    fn close(&mut self) -> Result<(), RuntimeFilterContractViolation>;
}
pub type RuntimeFilterFinalDomainPartitionHandle = Box<dyn RuntimeFilterFinalDomainPartition>;

/// Fragment-local FinalDomain fence capability. It exposes only local
/// partition ownership, the frozen-domain budget, and typed terminal failure;
/// registry membership and service issuance proofs stay adapter-private.
pub trait RuntimeFilterFinalDomainCompletion: Send + Sync {
    fn membership_key_type(&self) -> DataType;
    fn max_domain_canonical_bytes(&self) -> usize;
    fn claim_partition(
        &self,
        partition: PartitionId,
    ) -> Result<RuntimeFilterFinalDomainPartitionHandle, RuntimeFilterContractViolation>;
    fn fail(
        &self,
        reason: RuntimeFilterProducerFailure,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation>;
}
pub type RuntimeFilterFinalDomainCompletionHandle = Arc<dyn RuntimeFilterFinalDomainCompletion>;

/// A producer operation is admitted exactly once by the installed route. The
/// result preserves terminal and replay semantics without exposing the
/// role-local reducer's implementation types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterSubmitOutcome {
    Applied,
    Duplicate,
    Stale,
    SequenceAdvancedEqual,
    StreamAcceptedNoGlobalChange,
    Published,
    PendingGap,
    PendingFinalSnapshot,
    CoverageStillPossible,
    TerminalNoop,
    Completed,
    CompletedWithoutArtifact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterProducerFailure {
    Cancelled,
    ExecutionFailed,
    UpstreamUnavailable,
}

pub trait RuntimeFilterSession: Send + Sync {
    fn open_producer(
        &self,
        request: RuntimeFilterProducerOpenRequest,
    ) -> Result<RuntimeFilterBindOutcome<RuntimeFilterProducerHandle>, RuntimeFilterContractViolation>;
    fn subscribe(
        &self,
        request: RuntimeFilterSubscriptionRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<RuntimeFilterSubscriptionHandle>,
        RuntimeFilterContractViolation,
    >;
    fn open_final_domain_completion(
        &self,
        request: RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<RuntimeFilterFinalDomainCompletionHandle>,
        RuntimeFilterContractViolation,
    >;
}
pub type RuntimeFilterSessionRef = Arc<dyn RuntimeFilterSession>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterRowEffect {
    binding_id: RuntimeFilterBindingId,
    logical_version: LogicalVersion,
    input_rows: u64,
    output_rows: u64,
}
impl RuntimeFilterRowEffect {
    pub const fn new(
        binding_id: RuntimeFilterBindingId,
        logical_version: LogicalVersion,
        input_rows: u64,
        output_rows: u64,
    ) -> Self {
        Self {
            binding_id,
            logical_version,
            input_rows,
            output_rows,
        }
    }
    pub const fn binding_id(self) -> RuntimeFilterBindingId {
        self.binding_id
    }
    pub const fn logical_version(self) -> LogicalVersion {
        self.logical_version
    }
    pub const fn input_rows(self) -> u64 {
        self.input_rows
    }
    pub const fn output_rows(self) -> u64 {
        self.output_rows
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeFilterLiveCursor {
    observed: Option<LogicalVersion>,
}
impl RuntimeFilterLiveCursor {
    pub const fn observed(self) -> Option<LogicalVersion> {
        self.observed
    }
    pub fn observe(
        &mut self,
        snapshot: &RuntimeFilterSnapshot,
    ) -> Result<(), RuntimeFilterContractViolation> {
        if self
            .observed
            .is_some_and(|version| snapshot.logical_version() <= version)
        {
            return Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "runtime-filter live snapshot version regressed or repeated",
            ));
        }
        self.observed = Some(snapshot.logical_version());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    use super::*;
    use arrow::array::Int64Array;

    struct Predicate;
    impl RuntimeFilterPredicate for Predicate {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn evaluate(
            &self,
            input: &ArrayRef,
        ) -> Result<BooleanArray, RuntimeFilterContractViolation> {
            Ok(BooleanArray::from(vec![true; input.len()]))
        }
    }
    struct Producer;
    impl RuntimeFilterProducer for Producer {
        fn max_contribution_bytes(&self) -> usize {
            1024
        }

        fn submit(
            &self,
            _: PartitionId,
            _: ProducerSequence,
            _: RuntimeFilterContribution,
        ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
            Ok(RuntimeFilterSubmitOutcome::Applied)
        }
        fn close_partition(
            &self,
            _: PartitionId,
            _: ProducerSequence,
        ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
            Ok(RuntimeFilterSubmitOutcome::Applied)
        }
        fn fail(
            &self,
            _: RuntimeFilterProducerFailure,
        ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
            Ok(RuntimeFilterSubmitOutcome::Applied)
        }
    }

    struct FinalDomainFenceState {
        partition_count: u32,
        claimed: Mutex<BTreeSet<u32>>,
        closed: Mutex<BTreeSet<u32>>,
    }

    struct FinalDomainFence(Arc<FinalDomainFenceState>);

    impl FinalDomainFence {
        fn new(partition_count: u32) -> Self {
            Self(Arc::new(FinalDomainFenceState {
                partition_count,
                claimed: Mutex::new(BTreeSet::new()),
                closed: Mutex::new(BTreeSet::new()),
            }))
        }

        fn completed(&self) -> bool {
            self.0.closed.lock().expect("closed partitions").len()
                == self.0.partition_count as usize
        }
    }

    struct FinalDomainPartition {
        fence: Arc<FinalDomainFenceState>,
        partition: u32,
        sealed: bool,
    }

    impl RuntimeFilterFinalDomainPartition for FinalDomainPartition {
        fn seal(
            &mut self,
            _domain: RuntimeFilterFinalDomain,
        ) -> Result<(), RuntimeFilterContractViolation> {
            if self.sealed {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::ContractMismatch,
                    "fake final-domain partition was sealed twice",
                ));
            }
            self.sealed = true;
            Ok(())
        }

        fn close(&mut self) -> Result<(), RuntimeFilterContractViolation> {
            if !self.sealed {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::ContractMismatch,
                    "fake final-domain partition closed before seal",
                ));
            }
            self.fence
                .closed
                .lock()
                .expect("closed partitions")
                .insert(self.partition);
            Ok(())
        }
    }

    impl RuntimeFilterFinalDomainCompletion for FinalDomainFence {
        fn membership_key_type(&self) -> DataType {
            DataType::Int64
        }

        fn max_domain_canonical_bytes(&self) -> usize {
            1024
        }

        fn claim_partition(
            self: &Self,
            partition: PartitionId,
        ) -> Result<RuntimeFilterFinalDomainPartitionHandle, RuntimeFilterContractViolation>
        {
            if partition.get() >= self.0.partition_count {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::ContractMismatch,
                    "fake final-domain partition is outside declared DOP",
                ));
            }
            let mut claimed = self.0.claimed.lock().expect("claimed partitions");
            if !claimed.insert(partition.get()) {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::ContractMismatch,
                    "fake final-domain partition was claimed twice",
                ));
            }
            Ok(Box::new(FinalDomainPartition {
                fence: Arc::clone(&self.0),
                partition: partition.get(),
                sealed: false,
            }))
        }

        fn fail(
            &self,
            _reason: RuntimeFilterProducerFailure,
        ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
            Ok(RuntimeFilterSubmitOutcome::TerminalNoop)
        }
    }
    struct FakeSession;
    impl RuntimeFilterSession for FakeSession {
        fn open_producer(
            &self,
            request: RuntimeFilterProducerOpenRequest,
        ) -> Result<
            RuntimeFilterBindOutcome<RuntimeFilterProducerHandle>,
            RuntimeFilterContractViolation,
        > {
            if request.local_partition_count() == 0 {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::InvalidPartitionCount,
                    "partition count must be non-zero",
                ));
            }
            Ok(RuntimeFilterBindOutcome::Bound(Arc::new(Producer)))
        }
        fn subscribe(
            &self,
            request: RuntimeFilterSubscriptionRequest,
        ) -> Result<
            RuntimeFilterBindOutcome<RuntimeFilterSubscriptionHandle>,
            RuntimeFilterContractViolation,
        > {
            if request.contract().binding_id().get() == 99 {
                Ok(RuntimeFilterBindOutcome::Unavailable(
                    UnavailableReason::IncompleteCoverage,
                ))
            } else {
                Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "fake session has no subscription",
                ))
            }
        }

        fn open_final_domain_completion(
            &self,
            request: RuntimeFilterFinalDomainOpenRequest,
        ) -> Result<
            RuntimeFilterBindOutcome<RuntimeFilterFinalDomainCompletionHandle>,
            RuntimeFilterContractViolation,
        > {
            if request.contract().kind() != RuntimeFilterProducerKind::FinalDomain {
                return Err(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::RoleMismatch,
                    "fake session only exposes a final-domain completion fence",
                ));
            }
            Err(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "fake session has no final-domain completion fence",
            ))
        }
    }
    fn membership() -> RuntimeFilterExecutionContract {
        RuntimeFilterExecutionContract::Membership {
            canonical_schema: Arc::from([1_u8]),
            schema_digest: [2; 32],
        }
    }
    #[test]
    fn fake_session_distinguishes_exact_admission_from_unavailability() {
        let session: RuntimeFilterSessionRef = Arc::new(FakeSession);
        let producer = RuntimeFilterProducerContract::new(
            RuntimeFilterBindingId::new(1),
            RuntimeFilterChannelId::new(2),
            RuntimeFilterProducerKind::Membership,
            membership(),
        );
        assert!(matches!(
            session.open_producer(RuntimeFilterProducerOpenRequest::new(producer, 1)),
            Ok(RuntimeFilterBindOutcome::Bound(handle))
                if handle.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    RuntimeFilterContribution::new(
                        RuntimeFilterContributionKind::Membership,
                        [2; 32],
                        Arc::<[u8]>::from([1_u8, 2]),
                    ),
                ).is_ok()
        ));
        let consumer = RuntimeFilterConsumerContract::new(
            RuntimeFilterBindingId::new(99),
            RuntimeFilterChannelId::new(2),
            ConsumerActivation::BlockingSnapshot,
            membership(),
        );
        assert!(matches!(
            session.subscribe(RuntimeFilterSubscriptionRequest::new(consumer)),
            Ok(RuntimeFilterBindOutcome::Unavailable(
                UnavailableReason::IncompleteCoverage
            ))
        ));
    }
    #[test]
    fn live_cursor_rejects_version_regression_and_effect_is_versioned() {
        let predicate = Arc::new(Predicate);
        let snapshot = RuntimeFilterSnapshot::new(
            RuntimeFilterBindingId::new(3),
            LogicalVersion::new(2),
            [9; 32],
            predicate,
        );
        let mut cursor = RuntimeFilterLiveCursor::default();
        assert!(cursor.observe(&snapshot).is_ok());
        assert!(cursor.observe(&snapshot).is_err());
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        assert_eq!(snapshot.predicate().evaluate(&values).unwrap().len(), 2);
        assert_eq!(
            RuntimeFilterRowEffect::new(
                RuntimeFilterBindingId::new(3),
                LogicalVersion::new(2),
                2,
                1
            )
            .output_rows(),
            1
        );
    }

    #[test]
    fn final_domain_fence_requires_every_partition_to_seal_and_close() {
        let completion = Arc::new(FinalDomainFence::new(2));
        let mut first = completion
            .claim_partition(PartitionId::new(0))
            .expect("first partition is claimable");
        assert!(first.close().is_err());
        first
            .seal(RuntimeFilterFinalDomain::new([1_u8], 7_i64))
            .expect("first partition seals");
        first.close().expect("first partition closes");
        assert!(!completion.completed());

        let mut second = completion
            .claim_partition(PartitionId::new(1))
            .expect("second partition is claimable");
        second
            .seal(RuntimeFilterFinalDomain::new([2_u8], 8_i64))
            .expect("second partition seals");
        second.close().expect("second partition closes");
        assert!(completion.completed());
        assert!(completion.claim_partition(PartitionId::new(1)).is_err());
    }
}
