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

//! Closed continuation of the original read-snapshot algorithm.
//! The CPU worker polls only this concrete F. Its two I/O suspension sites
//! publish a named raw read; only the original async driver polls FileIO.
use super::*;
use crate::iceberg::{io::FileIO, table::Table};
use crate::read_snapshot::cow_capture::ReadFailure;
use novarocks_spi::connector::{
    ConnectorOriginalResultScope, ConnectorPayloadRetentionGuard, ConnectorRequestContext,
    ProviderMetadataCpuInput, ProviderMetadataCpuPort,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[derive(Clone)]
pub(crate) enum SnapshotReadKind {
    Statistics,
    Rewrite,
    ChangeWindow {
        schema: Arc<crate::iceberg::spec::Schema>,
    },
}
pub(crate) struct ScopedReadSnapshot {
    pub(crate) files: Vec<crate::read_model::IcebergReadFile>,
    pub(crate) all_deletes: Vec<Arc<crate::delete_semantics::DeleteFact>>,
    pub(crate) retention_guard: ConnectorPayloadRetentionGuard,
}
const SNAPSHOT_RAW_DETAIL: &str = "original snapshot metadata I/O failed";
#[derive(Debug)]
struct SnapshotRawFailure {
    original: crate::iceberg::Error,
    kind: ConnectorErrorKind,
}
impl std::fmt::Display for SnapshotRawFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(SNAPSHOT_RAW_DETAIL)
    }
}
impl std::error::Error for SnapshotRawFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}
impl novarocks_failure_contract::FailureProjection for SnapshotRawFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::FailureProjection;
        self.kind.failure_fact()
    }
}
pub(crate) fn raw_read_failure(error: crate::iceberg::Error) -> ConnectorError {
    // Preserve the original SDK error as a typed domain cause. Its Display is
    // not evaluated on this producer/receive boundary or in the public error.
    let kind = match error.kind() {
        crate::iceberg::ErrorKind::DataInvalid => ConnectorErrorKind::CorruptData,
        crate::iceberg::ErrorKind::FeatureUnsupported => ConnectorErrorKind::Unsupported,
        crate::iceberg::ErrorKind::NamespaceNotFound | crate::iceberg::ErrorKind::TableNotFound => {
            ConnectorErrorKind::NotFound
        }
        crate::iceberg::ErrorKind::PreconditionFailed => ConnectorErrorKind::InvalidRequest,
        _ => ConnectorErrorKind::Unavailable,
    };
    ConnectorError::from_domain_static(
        kind,
        SnapshotRawFailure {
            original: error,
            kind,
        },
        SNAPSHOT_RAW_DETAIL,
    )
}
enum IoState {
    Idle,
    Requested {
        path: String,
    },
    Reading {
        input: Option<crate::iceberg::io::InputFile>,
        result: Option<Result<Bytes, ConnectorError>>,
    },
    Returned(Result<Bytes, ConnectorError>),
}
pub(crate) struct SnapshotIoGate {
    state: Mutex<Option<IoState>>,
}
struct IoLoan<'a> {
    gate: &'a SnapshotIoGate,
    state: Option<IoState>,
}
impl<'a> IoLoan<'a> {
    fn take(gate: &'a SnapshotIoGate) -> Self {
        let state = gate.state.lock().unwrap_or_else(|p| p.into_inner()).take();
        assert!(state.is_some(), "one original snapshot I/O loan");
        Self { gate, state }
    }
}
impl Drop for IoLoan<'_> {
    fn drop(&mut self) {
        let mut target = self.gate.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(target.is_none(), "same original snapshot I/O receiver");
        *target = self.state.take();
    }
}
impl SnapshotIoGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Some(IoState::Idle)),
        })
    }
    pub(crate) async fn read(&self, path: &str) -> Result<Bytes, ReadFailure> {
        std::future::poll_fn(|_| {
            let mut loan = IoLoan::take(self);
            match loan.state.as_mut().unwrap() {
                IoState::Idle => {
                    loan.state = Some(IoState::Requested {
                        path: path.to_owned(),
                    });
                    Poll::Pending
                }
                IoState::Requested { path: original, .. } => {
                    assert_eq!(original, path, "same original snapshot raw read");
                    Poll::Pending
                }
                IoState::Reading { .. } => {
                    panic!("snapshot read collided with an active original read")
                }
                IoState::Returned(_) => {
                    let Some(IoState::Returned(result)) = loan.state.replace(IoState::Idle) else {
                        unreachable!()
                    };
                    Poll::Ready(result.map_err(|error| ReadFailure::Original(error.into())))
                }
            }
        })
        .await
    }
    async fn fulfill(
        &self,
        io: &FileIO,
        context: &ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        // The same requested path remains in this loan across the foreign F.
        // Neither poll, wake nor SDK Drop runs under the mutex.
        let mut loan = IoLoan::take(self);
        let IoState::Requested { path } = loan.state.as_ref().unwrap() else {
            return Err(invalid(
                "snapshot CPU suspended without an original I/O request",
            ));
        };
        let input = match io.new_input(path) {
            Ok(input) => input,
            Err(error) => {
                loan.state = Some(IoState::Returned(Err(raw_read_failure(error))));
                return Ok(());
            }
        };
        loan.state = Some(IoState::Reading {
            input: Some(input),
            result: None,
        });
        let IoState::Reading { input, result } = loan.state.as_mut().unwrap() else {
            unreachable!()
        };
        // Metadata objects are immutable. Observe the actual original object's
        // byte length, then grow this same Root before its whole-object read.
        // A failed size/read stays in this receiver before any handle Drop.
        let observed = input.as_ref().unwrap().metadata().await;
        match observed {
            Ok(metadata) => {
                let total = metadata.size;
                if let Some(total) = std::num::NonZeroU64::new(total) {
                    if context
                        .io_admission()
                        .and_then(std::sync::Weak::upgrade)
                        .is_none()
                    {
                        return Err(invalid("snapshot raw read lost its original MIO source"));
                    }
                    context.reserve_owned_metadata(total).await?;
                }
                *result = Some(
                    input
                        .as_ref()
                        .unwrap()
                        .read()
                        .await
                        .map_err(raw_read_failure)
                        .and_then(|bytes| {
                            if bytes.len() as u64 != metadata.size {
                                Err(ConnectorError::new(
                                    ConnectorErrorKind::CorruptData,
                                    "immutable snapshot metadata size changed during read",
                                ))
                            } else {
                                Ok(bytes)
                            }
                        }),
                );
            }
            Err(error) => *result = Some(Err(raw_read_failure(error))),
        }
        // Result is already stable even if the foreign input's destructor
        // unwinds. No lock is held and the original raw input is never repolled.
        drop(input.take());
        let received = result.take().expect("original snapshot raw read result");
        loan.state = Some(IoState::Returned(received));
        Ok(())
    }
}
type Reader = Pin<Box<dyn Future<Output = Result<ScopedReadSnapshot, ReadFailure>> + Send>>;
pub(super) struct SnapshotBody {
    io: FileIO,
    gate: Arc<SnapshotIoGate>,
    context: ConnectorRequestContext,
    reader: Option<Reader>,
    result: Option<Result<ScopedReadSnapshot, ConnectorError>>,
    // Independent of the borrowed reader F and disposed after its inputs.
    _original: ConnectorOriginalResultScope,
}
impl SnapshotBody {
    pub(super) fn run(&mut self) {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        // No SDK future is reachable from the closed scoped reader's poll:
        // both original raw read sites await SnapshotIoGate::read instead.
        if let Poll::Ready(value) = self
            .reader
            .as_mut()
            .expect("original reader F")
            .as_mut()
            .poll(&mut cx)
        {
            self.result = Some(value.map_err(super::scoped_read::checked_read));
            // Original result first, actual F Drop second; a Drop panic leaves
            // result + gate in the same restored body and poisons the CPU port.
            drop(self.reader.take());
        }
    }
}
impl IcebergMetadataCpuOriginal {
    pub(crate) fn read_snapshot_source(
        provider: IcebergMetadata,
        table: Table,
        snapshot: i64,
        context: ConnectorRequestContext,
        original: ConnectorOriginalResultScope,
        kind: SnapshotReadKind,
    ) -> Arc<Self> {
        Self::read_snapshot_source_for_generation(
            provider.descriptor().clone(),
            provider.incarnation(),
            table,
            snapshot,
            context,
            original,
            kind,
        )
    }
    pub(crate) fn read_snapshot_source_for_generation(
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        incarnation: novarocks_spi::connector::ProviderBindingEpoch,
        table: Table,
        snapshot: i64,
        context: ConnectorRequestContext,
        original: ConnectorOriginalResultScope,
        kind: SnapshotReadKind,
    ) -> Arc<Self> {
        let gate = SnapshotIoGate::new();
        let io = table.file_io().clone();
        let reader = Box::pin(crate::read_snapshot::make_scoped_read_snapshot(
            table,
            snapshot,
            context.clone(),
            original.clone(),
            kind,
            gate.clone(),
        ));
        Self::new(
            descriptor,
            incarnation,
            CpuBody::ReadSnapshot(SnapshotBody {
                io,
                gate,
                context,
                reader: Some(reader),
                result: None,
                _original: original,
            }),
        )
    }
    pub(crate) fn take_read_snapshot_in(
        &self,
        target: &mut Option<Result<ScopedReadSnapshot, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        if target.is_some() {
            return Err(invalid("snapshot receiver is occupied"));
        }
        let mut loan = BodyLoan::take(self);
        let CpuBody::ReadSnapshot(body) = &mut loan.body.as_mut().unwrap().body else {
            return Err(invalid("foreign snapshot body"));
        };
        if body.result.is_none() {
            return Err(invalid("snapshot original result is not ready"));
        }
        *target = body.result.take();
        Ok(())
    }
}
pub(crate) async fn drive_read_snapshot(
    source: &Arc<IcebergMetadataCpuOriginal>,
    cpu: &Arc<dyn ProviderMetadataCpuPort>,
) -> Result<(), ConnectorError> {
    loop {
        let mut input = Some(ProviderMetadataCpuInput(source.clone()));
        cpu.submit_metadata_in(&mut input)?;
        std::future::poll_fn(|cx| cpu.poll_finished(cx)).await?;
        let mut loan = BodyLoan::take(source);
        let original = loan.body.as_mut().unwrap();
        let CpuBody::ReadSnapshot(body) = &mut original.body else {
            return Err(invalid("foreign snapshot CPU body"));
        };
        if body.result.is_some() {
            return Ok(());
        }
        body.gate.fulfill(&body.io, &body.context).await?;
        // Only this successful original I/O delivery permits the next pure
        // continuation. The previous physical CPU/forwarder has actually joined.
        original.started = false;
    }
}
pub(super) fn reader_backing_bytes() -> Result<usize, ConnectorError> {
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    fn bytes<A, B, C, D, E, F, G>(_: fn(A, B, C, D, E, F) -> G) -> usize {
        std::mem::size_of::<G>()
    }
    let gate = arc_layout(std::alloc::Layout::new::<SnapshotIoGate>())
        .map_err(|_| internal("snapshot gate layout overflow"))?
        .size();
    let error =
        ConnectorError::static_domain_backing_bytes::<SnapshotRawFailure>(SNAPSHOT_RAW_DETAIL)
            .map_err(|_| internal("snapshot error layout overflow"))?;
    bytes(crate::read_snapshot::make_scoped_read_snapshot)
        .checked_add(gate)
        .and_then(|n| n.checked_add(error))
        .and_then(|n| n.checked_add(if cfg!(target_os = "macos") { 64 } else { 0 }))
        .ok_or_else(|| internal("snapshot reader layout overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CountWake(AtomicUsize);
    impl std::task::Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn original_raw_read_survives_borrowed_waiter_without_self_wake() {
        let gate = SnapshotIoGate::new();
        let wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        let path = "memory:///original-manifest.avro";
        let mut borrowed = Box::pin(gate.read(path));
        assert!(borrowed.as_mut().poll(&mut cx).is_pending());
        assert!(borrowed.as_mut().poll(&mut cx).is_pending());
        assert_eq!(wake.0.load(Ordering::SeqCst), 0);
        drop(borrowed);
        let loan = IoLoan::take(&gate);
        assert!(
            matches!(loan.state.as_ref(), Some(IoState::Requested { path: original }) if original == path)
        );
    }
    #[test]
    fn raw_snapshot_error_retains_sdk_source_and_original_public_fact() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
        use std::error::Error;
        let error = raw_read_failure(crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::DataInvalid,
            "private SDK object context",
        ));
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_eq!(error.failure_fact().reason().code(), 9);
        assert_eq!(error.message(), SNAPSHOT_RAW_DETAIL);
        let shared = error
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        let raw = shared.downcast_ref::<SnapshotRawFailure>().unwrap();
        let sdk = raw
            .source()
            .unwrap()
            .downcast_ref::<crate::iceberg::Error>()
            .unwrap();
        assert_eq!(sdk.kind(), crate::iceberg::ErrorKind::DataInvalid);
        assert!(!error.message().contains("private SDK"));
        assert!(
            reader_backing_bytes().unwrap()
                >= ConnectorError::static_domain_backing_bytes::<SnapshotRawFailure>(
                    SNAPSHOT_RAW_DETAIL
                )
                .unwrap()
        );
    }
}
