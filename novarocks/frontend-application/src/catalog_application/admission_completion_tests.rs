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
use novarocks_spi::connector::{CatalogVersion, ConnectorInstanceId, ConnectorProviderId};
use std::sync::mpsc;

fn fixture() -> (
    Arc<AdmissionCompletion>,
    CatalogRuntimeObservation,
    InstalledOwner,
) {
    let projection = CatalogRuntimeObservation {
        attachment_id: uuid::Uuid::from_bytes([7; 16]),
        instance_id: ConnectorInstanceId::try_from_canonical("m07_hms_readonly_cl").unwrap(),
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        generation: 3,
    };
    let selected = InstalledOwner {
        handle: CatalogHandle::new(
            projection.instance_id.clone(),
            CatalogVersion::from_bytes([4; 32]),
        ),
        incarnation: ProviderBindingEpoch::from_bytes([8; 16]),
    };
    let observation = AdmissionCompletion::new();
    let captured = selected.clone();
    observation
        .bind_selector(Arc::new(move |candidate| {
            if candidate.instance_id == *captured.handle.catalog_name() {
                Ok(Some(captured.clone()))
            } else {
                Ok(None)
            }
        }))
        .unwrap();
    (observation, projection, selected)
}
fn snapshot(owner: &AdmissionCompletion) -> serde_json::Value {
    serde_json::from_slice(&owner.snapshot_json().unwrap()).unwrap()
}

#[test]
fn non_target_catalogs_cannot_occupy_the_one_original_target_slot() {
    let (owner, projection, selected) = fixture();
    let mut rest = projection.clone();
    rest.instance_id = ConnectorInstanceId::try_from_canonical("startup_rest").unwrap();
    assert!(owner.queue(&rest).is_none());
    owner.retired(&rest.instance_id, rest.generation);
    assert!(snapshot(&owner)["row"].is_null());
    let sweep = owner.queue(&projection).unwrap();
    run_original_sweep(Some(&sweep), || {
        sweep.bound(&selected.handle, selected.incarnation);
        sweep.quarantined(novarocks_spi::connector::ConnectorErrorKind::Unsupported);
    });
    let value = snapshot(&owner);
    assert_eq!(value["invalid"], false);
    assert_eq!(value["row"]["outcome"], "QuarantinedUnsupported");
    assert_eq!(value["row"]["returned"], 5);
}

#[test]
fn repeated_target_never_overwrites_the_original_generation_history() {
    let (owner, projection, _) = fixture();
    let _original = owner.queue(&projection).unwrap();
    let before = snapshot(&owner)["row"].clone();
    assert!(owner.queue(&projection).is_none());
    let after = snapshot(&owner);
    assert_eq!(after["row"], before);
    assert_eq!(after["invalid"], true);
}

#[test]
fn planning_generation_mismatch_and_original_retirement_are_sticky() {
    for mode in 0..3 {
        let (owner, projection, selected) = fixture();
        let sweep = owner.queue(&projection).unwrap();
        run_original_sweep(Some(&sweep), || match mode {
            0 => sweep.bound(
                &CatalogHandle::new(
                    projection.instance_id.clone(),
                    CatalogVersion::from_bytes([5; 32]),
                ),
                selected.incarnation,
            ),
            1 => sweep.bound(&selected.handle, ProviderBindingEpoch::from_bytes([9; 16])),
            _ => owner.retired(&projection.instance_id, projection.generation),
        });
        assert_eq!(snapshot(&owner)["invalid"], true);
        assert_eq!(snapshot(&owner)["row"]["returned"], 0);
    }
}

#[test]
fn actual_worker_promise_preserves_queued_started_and_returned_boundaries() {
    // This is an actual host thread/channel component, not provider or Native acceptance.
    let (owner, projection, selected) = fixture();
    let sweep = owner.queue(&projection).unwrap();
    let (queue_tx, queue_rx) = mpsc::channel();
    let (start_tx, start_rx) = mpsc::channel();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (end_tx, end_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || -> Result<(), &'static str> {
        let sweep: Sweep = queue_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .map_err(|_| "queue test failed")?;
        start_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .map_err(|_| "start test failed")?;
        let mut work_result = Ok(());
        run_original_sweep(Some(&sweep), || {
            sweep.bound(&selected.handle, selected.incarnation);
            if entered_tx.send(()).is_err()
                || end_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .is_err()
            {
                sweep.failed();
                work_result = Err("end test failed");
                return;
            }
            sweep.quarantined(novarocks_spi::connector::ConnectorErrorKind::Unsupported);
        });
        work_result
    });
    let send = queue_tx.send(sweep);
    let queued = snapshot(&owner);
    let start = start_tx.send(());
    let entered = entered_rx.recv_timeout(std::time::Duration::from_secs(2));
    let running = snapshot(&owner);
    let end = end_tx.send(());
    // Join the original worker before any assertion can panic.
    let joined = worker.join();
    let returned = snapshot(&owner);
    assert!(send.is_ok() && start.is_ok() && entered.is_ok() && end.is_ok());
    assert!(matches!(joined, Ok(Ok(()))));
    assert_eq!(queued["row"]["queued"], 1);
    assert_eq!(queued["row"]["started"], 0);
    assert_eq!(running["row"]["started"], 2);
    assert_eq!(running["row"]["bound"], 3);
    assert_eq!(running["row"]["returned"], 0);
    assert_eq!(returned["row"]["quarantined"], 4);
    assert_eq!(returned["row"]["returned"], 5);
    assert_eq!(returned["invalid"], false);
}

#[test]
fn missing_bound_unwind_and_checked_overflow_cannot_mint_completion() {
    for mode in 0..3 {
        let (owner, projection, _) = fixture();
        let sweep = owner.queue(&projection).unwrap();
        if mode == 2 {
            owner.inner.lock().unwrap().sequence = u64::MAX;
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_original_sweep(Some(&sweep), || {
                if mode == 1 {
                    panic!("test original worker panic");
                }
            });
        }));
        let value = snapshot(&owner);
        assert_eq!(value["row"]["returned"], 0);
        if mode != 1 {
            assert_eq!(value["invalid"], true);
        } else {
            assert!(result.is_err());
        }
    }
}

#[test]
fn diagnostic_failure_never_changes_original_work_result_or_error_identity() {
    let (owner, projection, _) = fixture();
    let sweep = owner.queue(&projection).unwrap();
    let original = Arc::new(());
    let mut result: Result<(), Arc<()>> = Ok(());
    run_original_sweep(Some(&sweep), || {
        sweep.failed();
        result = Err(Arc::clone(&original));
    });
    assert!(Arc::ptr_eq(&original, result.as_ref().unwrap_err()));
    assert_eq!(snapshot(&owner)["invalid"], true);
    assert_eq!(snapshot(&owner)["row"]["returned"], 0);
}
