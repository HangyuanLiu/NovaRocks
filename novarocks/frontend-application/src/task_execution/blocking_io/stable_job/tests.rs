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

use super::*;
use novarocks_query_application::cpu::{
    QueryBlockingExecutorConfig, QueryBlockingExecutorOwner, QueryCpuExecutorConfig,
    QueryCpuExecutorOwner,
};
use std::{
    num::NonZeroUsize,
    sync::{atomic::AtomicUsize, mpsc},
    time::{Duration, Instant},
};

fn original_owners() -> (
    QueryCpuExecutorOwner,
    QueryBlockingExecutorOwner,
    ConnectorBlockingIoSupervisor,
) {
    let cpu = QueryCpuExecutorOwner::try_new(QueryCpuExecutorConfig::with_idle_keepalive(
        Duration::from_secs(1),
    ))
    .unwrap();
    let blocking = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let supervisor = ConnectorBlockingIoSupervisor::new(Handle::current());
    supervisor
        .install_original_executors(cpu.executor(), blocking.executor())
        .unwrap();
    (cpu, blocking, supervisor)
}
struct Probe(Arc<AtomicUsize>);
impl Drop for Probe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
struct ApplyInput {
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
    _probe: Probe,
}
fn gated_apply(input: &mut ApplyInput, _: &ConnectorSourceCloseResult<'_>) -> Option<usize> {
    input.entered.send(()).unwrap();
    input.release.recv_timeout(Duration::from_secs(5)).unwrap();
    Some(17)
}
#[tokio::test]
async fn dropped_borrowed_wait_keeps_the_same_real_input_and_receipt() {
    let (mut cpu, mut blocking, supervisor) = original_owners();
    let drops = Arc::new(AtomicUsize::new(0));
    let (entered, seen) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let mut job = supervisor.prepare_cpu_owned(
        Vec::new(),
        ApplyInput {
            entered,
            release: released,
            _probe: Probe(Arc::clone(&drops)),
        },
        gated_apply,
    );
    let pointer = Arc::as_ptr(job.header());
    let mut output = None;
    let mut wait = Box::pin(job.finish_in(&mut output));
    use std::future::Future;
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    seen.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(wait);
    assert_eq!(Arc::as_ptr(job.header()), pointer);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert!(output.is_none());
    release.send(()).unwrap();
    job.finish_in(&mut output).await;
    assert_eq!(output.take().unwrap().unwrap(), 17);
    assert!(job.actual_exited);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    drop(job);
    cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    blocking
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
struct PanicInput {
    drops: Probe,
    cause: Option<Probe>,
}
fn panicking_apply(input: &mut PanicInput, _: &ConnectorSourceCloseResult<'_>) -> Option<()> {
    std::panic::panic_any(input.cause.take().unwrap())
}
#[tokio::test]
async fn apply_unwind_restores_the_same_input_box_and_original_opaque_cause() {
    let (mut cpu, mut blocking, supervisor) = original_owners();
    let input_drops = Arc::new(AtomicUsize::new(0));
    let cause_drops = Arc::new(AtomicUsize::new(0));
    let mut job = supervisor.prepare_cpu_owned(
        Vec::new(),
        PanicInput {
            drops: Probe(Arc::clone(&input_drops)),
            cause: Some(Probe(Arc::clone(&cause_drops))),
        },
        panicking_apply,
    );
    let call_pointer = {
        let guard = job.header().call.lock().unwrap();
        &**guard.as_ref().unwrap() as *const dyn OriginalCall<()> as *const ()
    };
    let mut output = None;
    job.finish_in(&mut output).await;
    assert!(output.as_ref().unwrap().is_err());
    assert!(job.actual_exited);
    assert_eq!(input_drops.load(Ordering::Acquire), 0);
    assert_eq!(cause_drops.load(Ordering::Acquire), 0);
    let header = job.header.take().unwrap();
    let restored = {
        let guard = header.call.lock().unwrap();
        &**guard.as_ref().unwrap() as *const dyn OriginalCall<()> as *const ()
    };
    assert_eq!(call_pointer, restored);
    assert!(header.poisoned.load(Ordering::Acquire));
    // Explicit fixture disposition after observing the actual CPU exit. There
    // is no Work/Source completion proof inferred from this test-only Drop.
    drop(header.call.lock().unwrap().take());
    drop(header);
    drop(job);
    drop(output);
    assert_eq!(input_drops.load(Ordering::Acquire), 1);
    assert_eq!(cause_drops.load(Ordering::Acquire), 1);
    cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    blocking
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}

#[derive(Debug)]
struct CloseCause(Arc<AtomicUsize>);
impl std::fmt::Display for CloseCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original typed close cause")
    }
}
impl std::error::Error for CloseCause {}
impl novarocks_execution_contract::failure::FailureProjection for CloseCause {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::FailureProjection;
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unavailable,
            "fixture close fact",
        )
        .failure_fact()
    }
}
impl Drop for CloseCause {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
struct TypedCloseSource {
    cause: Option<CloseCause>,
    dropped: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}
impl novarocks_spi::connector::read_stack::ConnectorReadSplitSource for TypedCloseSource {
    fn next_batch(
        &mut self,
        _: usize,
        _: &novarocks_spi::connector::read_stack::ConnectorReadDynamicFilterSnapshot,
    ) -> Result<
        novarocks_spi::connector::read_stack::ConnectorSplitBatch<
            novarocks_spi::connector::read_stack::ConnectorReadSplit,
        >,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(novarocks_spi::connector::read_stack::ConnectorSplitBatch::finished())
    }
    fn is_finished(&self) -> bool {
        false
    }
    fn close(&mut self) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Err(novarocks_spi::connector::ConnectorError::from_domain(
            novarocks_spi::connector::ConnectorErrorKind::Unavailable,
            self.cause.take().expect("one original close cause"),
        ))
    }
}
impl Drop for TypedCloseSource {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::AcqRel);
    }
}
fn close_original_source(
    input: &mut crate::query_execution::split_assignment::SupervisedSplitSource,
    result: &ConnectorSourceCloseResult<'_>,
) -> Option<()> {
    if let Err(error) = input.retire_in(result) {
        result.record_error(error);
    }
    Some(())
}
#[tokio::test]
async fn real_source_box_exits_on_original_worker_and_typed_close_header_survives() {
    let (mut cpu, mut blocking, supervisor) = original_owners();
    let cause_drops = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let source = crate::query_execution::split_assignment::SupervisedSplitSource::fixture(
        Box::new(TypedCloseSource {
            cause: Some(CloseCause(Arc::clone(&cause_drops))),
            dropped: Arc::clone(&dropped),
            calls: Arc::clone(&calls),
        }),
    );
    let mut job = supervisor.prepare_cpu_retirement(Vec::new(), source, close_original_source);
    let mut output = None;
    job.finish_in(&mut output).await;
    assert!(output.take().unwrap().is_ok());
    assert!(job.actual_exited);
    assert_eq!(dropped.load(Ordering::Acquire), 1);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    let primary = job
        .source_close_failure()
        .expect("same stable typed close error");
    assert_eq!(
        primary.kind(),
        novarocks_spi::connector::ConnectorErrorKind::Unavailable
    );
    assert_eq!(cause_drops.load(Ordering::Acquire), 0);
    drop(job);
    assert_eq!(cause_drops.load(Ordering::Acquire), 0);
    drop(primary);
    assert_eq!(cause_drops.load(Ordering::Acquire), 1);
    cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    blocking
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
