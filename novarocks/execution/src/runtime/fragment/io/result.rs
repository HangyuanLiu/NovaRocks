use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::runtime::observable::Observable;
use novarocks_execution_contract::TaskIdentity;
use novarocks_types::{FieldRenderSchema, PrimitiveType, SlotId, UniqueId};

use super::FragmentIoError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResultAbort {
    PrepareRollback,
    NeverStarted,
    Failed(String),
    Cancelled(String),
}

/// Byte credit reserved by the result owner before the pipeline transfers a
/// chunk into the result stream.
///
/// The result owner decides when the reservation is released. A queue keeps
/// the credit beside the retained payload and drops it only after consumer
/// progress, acknowledgement, cancellation, or terminal cleanup releases
/// those bytes.
pub struct ResultWriteCredit {
    bytes: usize,
    release: Option<Box<dyn Fn(usize) + Send + Sync + 'static>>,
}

impl ResultWriteCredit {
    pub fn new(bytes: usize, release: impl Fn(usize) + Send + Sync + 'static) -> Self {
        Self {
            bytes,
            release: Some(Box::new(release)),
        }
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Releases an unused suffix of this reservation before the retained
    /// payload takes ownership of the remaining bytes.
    pub fn shrink_to(&mut self, retained_bytes: usize) -> Result<(), String> {
        let released = self.bytes.checked_sub(retained_bytes).ok_or_else(|| {
            format!(
                "result credit cannot grow from {} to {retained_bytes} bytes",
                self.bytes
            )
        })?;
        if released > 0 {
            // Commit the smaller ownership before invoking host code so an
            // unwinding release callback cannot make Drop return the original
            // reservation a second time.
            self.bytes = retained_bytes;
            if let Some(release) = self.release.as_ref() {
                release(released);
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for ResultWriteCredit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResultWriteCredit")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for ResultWriteCredit {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release(self.bytes);
        }
    }
}

#[derive(Debug)]
pub enum ResultWriteAdmission {
    Granted(ResultWriteCredit),
    Blocked,
}

/// An opened host result stream. Encoding and presentation remain with the
/// host; the execution kernel only writes Arrow chunks and terminal state.
pub trait FragmentResultSession: Send + Sync + 'static {
    /// Returns the upper-bound reservation required before the host encodes
    /// this chunk.
    ///
    /// The driver calls this before `try_acquire`, so a host must use the same
    /// configured packet bound, encode once in `write_with_credit`, reject an
    /// oversized encoding, and release the unused suffix before retaining it.
    fn reservation_bytes(&self, chunk: &Chunk) -> Result<usize, FragmentIoError>;

    /// Attempts to reserve result-owned bytes without blocking the calling
    /// driver.
    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, FragmentIoError>;

    /// Stable readiness observable for a blocked credit acquisition.
    fn writable_observable(&self) -> Option<Arc<Observable>>;

    /// Transfers both the chunk and its exact byte reservation to the result
    /// owner.
    fn write_with_credit(
        &self,
        chunk: Chunk,
        credit: ResultWriteCredit,
    ) -> Result<(), FragmentIoError>;

    fn finish(&self) -> Result<(), FragmentIoError>;
    fn abort(&self, reason: ResultAbort);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultPresentation {
    MysqlText,
    HttpJson,
    Statistic,
}

#[derive(Clone, Debug)]
pub struct ResultProjection {
    slot_id: SlotId,
    primitive: PrimitiveType,
    field_schema: FieldRenderSchema,
}

impl ResultProjection {
    pub fn new(slot_id: SlotId, primitive: PrimitiveType, field_schema: FieldRenderSchema) -> Self {
        Self {
            slot_id,
            primitive,
            field_schema,
        }
    }

    pub const fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub const fn primitive(&self) -> PrimitiveType {
        self.primitive
    }

    pub fn field_schema(&self) -> &FieldRenderSchema {
        &self.field_schema
    }
}

#[derive(Clone, Debug)]
pub struct ResultWriteSpec {
    task_identity: Option<TaskIdentity>,
    fragment_instance_id: UniqueId,
    presentation: ResultPresentation,
    projections: Option<Vec<ResultProjection>>,
    typed: bool,
}

impl ResultWriteSpec {
    pub fn new(
        fragment_instance_id: UniqueId,
        presentation: ResultPresentation,
        projections: Option<Vec<ResultProjection>>,
        typed: bool,
    ) -> Self {
        Self {
            task_identity: None,
            fragment_instance_id,
            presentation,
            projections,
            typed,
        }
    }

    /// Binds a native result stream to its complete Task identity.
    ///
    /// Standalone execution tests may omit this, but a native Worker result
    /// writer must reject an unbound spec rather than fall back to the kernel
    /// fragment id as an admission identity.
    pub const fn with_task_identity(mut self, identity: TaskIdentity) -> Self {
        self.task_identity = Some(identity);
        self
    }

    pub const fn task_identity(&self) -> Option<TaskIdentity> {
        self.task_identity
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn presentation(&self) -> ResultPresentation {
        self.presentation
    }

    pub fn projections(&self) -> Option<&[ResultProjection]> {
        self.projections.as_deref()
    }

    pub const fn is_typed(&self) -> bool {
        self.typed
    }
}

/// Host-owned result-session factory. Presentation encoding remains outside
/// execution; the kernel only owns the lifecycle and Arrow chunk writes.
pub trait FragmentResultWriter: Send + Sync + 'static {
    fn open(
        &self,
        spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError>;
}

/// Exact root facts are mandatory before opening a bounded producer. The
/// contract stays separate from carrier/hydration capabilities and contains
/// no runtime writer, Arrow array or transport handle.
#[derive(Clone, Debug)]
pub struct RootResultWriteSpec {
    pub task: TaskIdentity,
    pub contract: Arc<novarocks_result_contract::RootOutputContract>,
}

/// Reservation for the one final upstream pull. The host acquires the input
/// position and complete original/hydrate overlap before that pull can grow
/// an Arrow backing. CountOnly also takes a position but needs no hydration.
/// This reuses the host's retained pool; it is not a second allocator wallet.
/// A host-minted input position issuer. A new issuer is distinct even for the
/// same Task identity, so a reconstructed or foreign session cannot accept an
/// old input grant. Runtime sessions keep their issuer private.
#[derive(Clone)]
pub struct RootInputAuthority {
    inner: Arc<RootInputAuthorityInner>,
}
struct RootInputAuthorityInner {
    task: TaskIdentity,
    required_bytes: usize,
    state: std::sync::Mutex<RootInputPosition>,
    observable: Arc<Observable>,
}
#[derive(Default)]
struct RootInputPosition {
    generation: u64,
    occupied: bool,
    closed: bool,
}
impl RootInputAuthority {
    pub fn new(spec: &RootResultWriteSpec) -> Self {
        let geometry =
            novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1;
        let required_bytes = geometry.root_original_input_backing_capacity_bytes as usize
            + if spec.contract.kind() == novarocks_result_contract::RootOutputKind::CountOnly {
                0
            } else {
                geometry.root_additional_hydrate_backing_capacity_bytes as usize
            };
        Self {
            inner: Arc::new(RootInputAuthorityInner {
                task: spec.task,
                required_bytes,
                state: std::sync::Mutex::new(RootInputPosition::default()),
                observable: Arc::new(Observable::new()),
            }),
        }
    }
    pub fn required_bytes(&self) -> usize {
        self.inner.required_bytes
    }
    /// Readiness hint only: `try_acquire` still performs exact admission.
    /// Hosts avoid reserving a second overlap while another input is live.
    pub fn is_available(&self) -> bool {
        let state = self.inner.state.lock().unwrap();
        !state.closed && !state.occupied
    }
    pub fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.inner.observable)
    }
    /// Credit comes from this host's existing retained pool. Only this issuer
    /// can mint the corresponding position; the session validates `owns()`
    /// before taking input, and retains the whole permit until physical exit.
    pub fn try_acquire(
        &self,
        credit: ResultWriteCredit,
    ) -> Result<RootInputAdmission, FragmentIoError> {
        use super::{FragmentIoErrorKind, FragmentIoOperation};
        if credit.bytes() != self.inner.required_bytes {
            return Err(FragmentIoError::new(
                FragmentIoOperation::ResultWrite,
                FragmentIoErrorKind::InvalidResponse,
                "root input credit does not cover its exact frozen overlap",
            ));
        }
        let generation = {
            let mut state = self.inner.state.lock().unwrap();
            if state.closed {
                return Err(FragmentIoError::new(
                    FragmentIoOperation::ResultWrite,
                    FragmentIoErrorKind::Cancelled,
                    "root input issuer is closed",
                ));
            }
            if state.occupied {
                None
            } else {
                let generation = state.generation.checked_add(1).ok_or_else(|| {
                    FragmentIoError::new(
                        FragmentIoOperation::ResultWrite,
                        FragmentIoErrorKind::Internal,
                        "root input generation overflow",
                    )
                })?;
                state.generation = generation;
                state.occupied = true;
                Some(generation)
            }
        };
        match generation {
            Some(generation) => Ok(RootInputAdmission::Granted(RootInputPermit {
                credit: Some(credit),
                issuer: Arc::clone(&self.inner),
                generation,
            })),
            None => Ok(RootInputAdmission::Blocked),
        }
    }
    pub fn owns(&self, permit: &RootInputPermit) -> bool {
        if !Arc::ptr_eq(&self.inner, &permit.issuer) {
            return false;
        }
        let state = self.inner.state.lock().unwrap();
        !state.closed && state.occupied && state.generation == permit.generation
    }
    pub fn close(&self) {
        self.inner.state.lock().unwrap().closed = true;
        self.inner.observable.notify_observers();
    }
}
/// Neither the issuer nor its underlying credit can be extracted by a driver.
/// The host validates and retains the unique position through its actual input,
/// hydration, counting and encoding exit, including after cancellation.
pub struct RootInputPermit {
    credit: Option<ResultWriteCredit>,
    issuer: Arc<RootInputAuthorityInner>,
    generation: u64,
}
impl RootInputPermit {
    pub fn retained_bytes(&self) -> usize {
        self.credit
            .as_ref()
            .expect("input retains its credit")
            .bytes()
    }
    pub fn task(&self) -> TaskIdentity {
        self.issuer.task
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
impl Drop for RootInputPermit {
    fn drop(&mut self) {
        drop(self.credit.take());
        {
            let mut state = self.issuer.state.lock().unwrap();
            assert!(
                state.occupied && state.generation == self.generation,
                "unique root input position is still held"
            );
            state.occupied = false;
        }
        self.issuer.observable.notify_observers();
    }
}

pub enum RootInputAdmission {
    Granted(RootInputPermit),
    Blocked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootProducerState {
    Accepting,
    /// No more input is allowed; encoding/publication may still be running.
    Finishing,
    /// End is published, the encoder actually exited, and its exact context
    /// holds every retained result owner. Consumer progress is independent.
    ContextHeld,
    Failed(String),
}

/// Host-owned bounded root producer. Driver methods never synchronously
/// hydrate, count, render or wait for consumer acknowledgement. A dedicated
/// finite producer advances the one admitted input/cursor under its quantum.
pub trait RootResultSession: Send + Sync + 'static {
    fn spec(&self) -> &RootResultWriteSpec;
    fn try_acquire_input(&self) -> Result<RootInputAdmission, FragmentIoError>;
    fn writable_observable(&self) -> Arc<Observable>;
    fn submit_input(&self, chunk: Chunk, permit: RootInputPermit) -> Result<(), FragmentIoError>;
    /// This requests finish. Only ContextHeld permits a successful root
    /// Task terminal; callers continue observing the producer until then.
    fn finish_input(&self) -> Result<(), FragmentIoError>;
    fn producer_state(&self) -> RootProducerState;
    /// Failure/cancellation is a logical decision. The driver still waits
    /// for every admitted input/cursor/job to actually exit under its grant.
    fn producer_exited(&self) -> bool;
    fn abort(&self, reason: ResultAbort);
}

pub trait RootResultWriter: Send + Sync + 'static {
    fn open_root(
        &self,
        spec: RootResultWriteSpec,
    ) -> Result<Arc<dyn RootResultSession>, FragmentIoError>;
}

#[cfg(test)]
pub(crate) fn discard_result_session() -> Arc<dyn FragmentResultSession> {
    Arc::new(DiscardResultSession)
}

#[cfg(test)]
pub(crate) fn discard_result_writer() -> Arc<dyn FragmentResultWriter> {
    Arc::new(DiscardResultWriter)
}

#[cfg(test)]
struct DiscardResultSession;

#[cfg(test)]
struct DiscardResultWriter;

#[cfg(test)]
impl FragmentResultWriter for DiscardResultWriter {
    fn open(
        &self,
        _spec: ResultWriteSpec,
    ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
        Ok(discard_result_session())
    }
}

#[cfg(test)]
impl FragmentResultSession for DiscardResultSession {
    fn reservation_bytes(&self, chunk: &Chunk) -> Result<usize, FragmentIoError> {
        Ok(chunk.logical_bytes())
    }

    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, FragmentIoError> {
        Ok(ResultWriteAdmission::Granted(ResultWriteCredit::new(
            bytes,
            |_| {},
        )))
    }

    fn writable_observable(&self) -> Option<Arc<Observable>> {
        None
    }

    fn write_with_credit(
        &self,
        chunk: Chunk,
        credit: ResultWriteCredit,
    ) -> Result<(), FragmentIoError> {
        debug_assert_eq!(credit.bytes(), chunk.logical_bytes());
        Ok(())
    }

    fn finish(&self) -> Result<(), FragmentIoError> {
        Ok(())
    }

    fn abort(&self, _reason: ResultAbort) {}
}

#[cfg(test)]
mod root_input_tests {
    use super::*;
    #[test]
    fn root_input_is_issuer_bound_single_position_and_retained_until_actual_drop() {
        use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let task = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let spec = RootResultWriteSpec {
            task,
            contract: Arc::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::CountOnly,
            )),
        };
        let authority = RootInputAuthority::new(&spec);
        let reconstructed = RootInputAuthority::new(&spec);
        let credit = || ResultWriteCredit::new(authority.required_bytes(), |_| {});
        let RootInputAdmission::Granted(permit) = authority.try_acquire(credit()).unwrap() else {
            panic!("first input")
        };
        assert!(authority.owns(&permit));
        assert!(
            !reconstructed.owns(&permit),
            "exact Task identity does not forge an issuer"
        );
        assert!(matches!(
            authority.try_acquire(credit()).unwrap(),
            RootInputAdmission::Blocked
        ));
        let first_generation = permit.generation();
        drop(permit);
        let RootInputAdmission::Granted(next) = authority.try_acquire(credit()).unwrap() else {
            panic!("next input")
        };
        assert!(next.generation() > first_generation);
        authority.close();
        assert!(!authority.owns(&next));
        assert!(authority.try_acquire(credit()).is_err());
        drop(next);
        assert!(
            reconstructed
                .try_acquire(ResultWriteCredit::new(1, |_| {}))
                .is_err()
        );
    }
}
