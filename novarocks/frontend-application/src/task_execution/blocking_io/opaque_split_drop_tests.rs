// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual provider split cleanup through the original admitted blocking lane.
use super::*;
use std::sync::mpsc;
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use novarocks_spi::connector::read_stack::adapter::{ProviderReadRuntime, ReadRuntimeAdapter};
use novarocks_spi::connector::read_stack::{ColumnHandle, ConnectorSplit};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorProviderId,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CanaryColumn;
impl ColumnHandle for CanaryColumn {}

// A concrete provider Split; the only role-visible value will be ConnectorReadSplit.
struct CanarySplit {
    entered: mpsc::Sender<ThreadId>,
    watchdog_entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    exited: mpsc::Sender<bool>,
    drop_count: Arc<AtomicUsize>,
}
impl fmt::Debug for CanarySplit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpaqueProviderSplitDropCanary")
    }
}
impl ConnectorSplit for CanarySplit {
    fn retained_size_in_bytes(&self) -> u64 {
        // A provider fact about this fixture, not an allocator/whole-graph bound.
        std::mem::size_of::<Self>() as u64
    }
}
impl Drop for CanarySplit {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.entered.send(std::thread::current().id());
        let _ = self.watchdog_entered.send(());
        let released = self
            .release
            .get_mut()
            .map(|receiver| receiver.recv_timeout(Duration::from_secs(2)).is_ok())
            .unwrap_or(false);
        let _ = self.exited.send(released);
    }
}

struct CanaryProvider {
    descriptor: ConnectorInstanceDescriptor,
    catalog: CatalogHandle,
}
impl ProviderReadRuntime for CanaryProvider {
    type Table = ();
    type Column = CanaryColumn;
    type Transaction = ();
    type Split = CanarySplit;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog
    }
    fn transaction(&self) -> Self::Transaction {}
}

#[test]
fn unclaimed_original_opaque_split_drop_is_retired_off_the_waiter_thread() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .expect("component runtime");
    let supervisor = ConnectorBlockingIoSupervisor::new(runtime.handle().clone());
    let (control, root, window) = super::tests::admitted();
    let scope = root.owner.scope();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (watchdog_tx, watchdog_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (exited_tx, exited_rx) = mpsc::channel();
    let drop_count = Arc::new(AtomicUsize::new(0));

    // Owned independent host watchdog exists before the payload is minted.
    // Even an observation failure releases the destructor before any assertion.
    let watchdog = std::thread::spawn(move || {
        let observed_enter = watchdog_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        if observed_enter {
            std::thread::sleep(Duration::from_millis(250));
        }
        let release_delivered = release_tx.send(()).is_ok();
        (observed_enter, release_delivered)
    });
    let instance = ConnectorInstanceId::parse("drop.canary").expect("fixture instance");
    let adapter = ReadRuntimeAdapter::new(Arc::new(CanaryProvider {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("drop-canary").expect("fixture provider"),
            instance_id: instance.clone(),
        },
        catalog: CatalogHandle::new(instance, CatalogVersion::from_bytes([7; 32])),
    }));
    let split = adapter.wrap_split(CanarySplit {
        entered: entered_tx,
        watchdog_entered: watchdog_tx,
        release: Mutex::new(release_rx),
        exited: exited_tx,
        drop_count: Arc::clone(&drop_count),
    });
    // Drop the provider-side adapter; it does not own or clone this split's payload.
    drop(adapter);
    let job = supervisor
        .spawn_admitted(&scope, &window.retain_alias(), move || split)
        .expect("original admitted opaque split job");

    let observation_deadline = Instant::now() + Duration::from_secs(2);
    let mut original_join_observed = false;
    loop {
        // Test-only inspection of the original JobOutcome. No result is taken,
        // and no SPI payload/handle internals are inspected or reconstructed.
        let published_ok = matches!(
            job.outcome
                .value
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref(),
            Some(Ok(_))
        );
        if published_ok
            && job.outcome.pins.len() == 1
            && job.outcome.pins[0].joined.load(Ordering::Acquire)
        {
            original_join_observed = true;
            break;
        }
        if Instant::now() >= observation_deadline {
            break;
        }
        std::thread::yield_now();
    }
    root.owner.complete();
    root.business.release();
    drop(window);
    super::tests::settle_control(&control);
    let before_waiter_drop = control.snapshot();
    let (waiter_thread_tx, waiter_thread_rx) = mpsc::channel();
    let waiter = runtime.spawn(async move {
        let _ = waiter_thread_tx.send(std::thread::current().id());
        drop(job); // Only the waiter/result owner is dropped, after original call join.
    });
    let waiter_thread = waiter_thread_rx.recv_timeout(Duration::from_secs(2));
    let destructor_thread = entered_rx.recv_timeout(Duration::from_secs(2));
    let while_drop_held = control.snapshot();

    // All cleanup precedes assertions: watchdog owns release, original waiter
    // handle is actually awaited, then the original workload is drained.
    let watchdog_result = watchdog.join();
    let destructor_exited = exited_rx.recv_timeout(Duration::from_secs(2));
    let waiter_result = runtime.block_on(waiter);
    let cleanup_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        super::tests::settle_control(&control);
        if control.snapshot().scopes.is_empty() || Instant::now() >= cleanup_deadline {
            break;
        }
        std::thread::yield_now();
    }
    let after = control.snapshot();
    control.close_admission();
    let workload_exit = control.shutdown();
    let retained_failure = supervisor.take_original_failure();
    if let Some(error) = retained_failure {
        supervisor.retire_original_failure(error);
    }
    drop(supervisor);
    drop(runtime);

    assert!(
        original_join_observed,
        "original call join was not observed"
    );
    assert_eq!(before_waiter_drop.root_responsibilities, 1);
    assert_eq!(
        before_waiter_drop.result_windows.held_positions,
        [0, 0, 1, 0]
    );
    assert_eq!(while_drop_held.root_responsibilities, 1);
    assert_eq!(while_drop_held.result_windows.held_positions, [0, 0, 1, 0]);
    assert!(
        matches!(watchdog_result, Ok((true, true))),
        "watchdog did not settle its release"
    );
    assert!(
        matches!(destructor_exited, Ok(true)),
        "opaque provider destructor did not exit normally"
    );
    assert!(
        waiter_result.is_ok(),
        "original waiter task did not join normally"
    );
    assert!(
        after.scopes.is_empty(),
        "original responsibility did not converge"
    );
    assert!(workload_exit.is_ok(), "same original workload did not exit");
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    let waiter_thread = waiter_thread.expect("actual waiter thread source");
    let destructor_thread = destructor_thread.expect("actual provider destructor thread source");
    assert_ne!(
        destructor_thread, waiter_thread,
        "opaque provider split destructor ran on the async waiter thread"
    );
}
