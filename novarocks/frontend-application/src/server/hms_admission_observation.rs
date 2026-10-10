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

//! Listener-local feature diagnostics bound to the original opened FE host.
//! The one late-bound scalar reader grants no provider or execution capability.
use std::sync::{Arc, Mutex};

use super::HmsListingObservationHandler;
use crate::catalog_application::{
    CatalogRuntimeProjection,
    admission_completion::{AdmissionCompletion, OwnerSelector},
};

/// Server supplies the original captured HMS owner selector and raw handler.
/// Absence keeps the original management listener free of this diagnostic route.
#[derive(Clone)]
pub struct HmsListingObservationSetup {
    handler: HmsListingObservationHandler,
    selector: OwnerSelector,
}
impl HmsListingObservationSetup {
    pub fn new(handler: HmsListingObservationHandler, selector: OwnerSelector) -> Self {
        Self { handler, selector }
    }
}

pub(super) struct HmsAdmissionObservation {
    setup: HmsListingObservationSetup,
    installed: Mutex<Option<Arc<AdmissionCompletion>>>,
}
impl HmsAdmissionObservation {
    pub(super) fn new(setup: &HmsListingObservationSetup) -> Arc<Self> {
        Arc::new(Self {
            setup: setup.clone(),
            installed: Mutex::new(None),
        })
    }
    pub(super) fn install(
        &self,
        projection: &Arc<CatalogRuntimeProjection>,
    ) -> Result<(), &'static str> {
        let mut installed = self
            .installed
            .lock()
            .map_err(|_| "HMS admission installation lock failed")?;
        if let Some(original) = installed.as_ref() {
            original.fail();
            return Err("HMS admission original projection is already installed");
        }
        projection.bind_hms_admission_selector(Arc::clone(&self.setup.selector))?;
        *installed = Some(projection.hms_admission_completion());
        Ok(())
    }
    pub(super) fn require_installed(
        &self,
        projection: &Arc<CatalogRuntimeProjection>,
    ) -> Result<(), &'static str> {
        let installed = self
            .installed
            .lock()
            .map_err(|_| "HMS admission installation lock failed")?;
        let original = installed
            .as_ref()
            .ok_or("HMS admission original projection is not installed")?;
        if !Arc::ptr_eq(original, &projection.hms_admission_completion()) {
            original.fail();
            return Err("HMS admission serving projection differs from the original opened host");
        }
        Ok(())
    }
    pub(super) fn handler(self: &Arc<Self>) -> HmsListingObservationHandler {
        let owner = Arc::clone(self);
        Arc::new(move |body| owner.observe(body))
    }
    fn observe(&self, body: &[u8]) -> Result<Vec<u8>, &'static str> {
        if body.len() > 1024 {
            return Err("HMS observation request exceeds its limit");
        }
        #[derive(serde::Deserialize)]
        #[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
        enum Request {
            AdmissionCompletion {},
            Snapshot {},
            Reset {
                domain: uuid::Uuid,
                phase: u64,
                sequence: u64,
            },
        }
        let request: Request =
            serde_json::from_slice(body).map_err(|_| "HMS observation request is invalid")?;
        match request {
            Request::AdmissionCompletion {} => {
                let installed = self
                    .installed
                    .lock()
                    .map_err(|_| "HMS admission installation lock failed")?;
                installed
                    .as_ref()
                    .ok_or("HMS admission original projection is not installed")?
                    .snapshot_json()
            }
            Request::Snapshot {} => (self.setup.handler)(body),
            Request::Reset {
                domain,
                phase,
                sequence,
            } => {
                let _ = (domain, phase, sequence);
                // The original probe receives exact original bytes and retains
                // its actual idle/domain/phase/sequence reset authority.
                (self.setup.handler)(body)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_application::admission_completion::InstalledOwner;
    use novarocks_catalog_application::{CatalogRuntimeObservation, CatalogRuntimePublisherSink};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, ConnectorProviderId,
        ProviderBindingEpoch,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fixture() -> (
        Arc<HmsAdmissionObservation>,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<Vec<u8>>>>,
    ) {
        let selections = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let received = Arc::clone(&calls);
        let selected = Arc::clone(&selections);
        let setup = HmsListingObservationSetup::new(
            Arc::new(move |body| {
                received.lock().unwrap().push(body.to_vec());
                Ok(b"original-handler".to_vec())
            }),
            Arc::new(move |candidate| {
                selected.fetch_add(1, Ordering::SeqCst);
                if candidate.instance_id.as_str() != "m07_hms_readonly_cl" {
                    return Ok(None);
                }
                Ok(Some(InstalledOwner {
                    handle: CatalogHandle::new(
                        candidate.instance_id.clone(),
                        CatalogVersion::from_bytes([7; 32]),
                    ),
                    incarnation: ProviderBindingEpoch::from_bytes([8; 16]),
                }))
            }),
        );
        (HmsAdmissionObservation::new(&setup), selections, calls)
    }
    fn projection(name: &str) -> CatalogRuntimeObservation {
        CatalogRuntimeObservation {
            attachment_id: uuid::Uuid::from_bytes([9; 16]),
            instance_id: ConnectorInstanceId::try_from_canonical(name).unwrap(),
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            generation: 2,
        }
    }
    #[test]
    fn original_host_installs_once_before_target_notification_and_serving() {
        let (binding, selections, calls) = fixture();
        let host = CatalogRuntimeProjection::new();
        assert!(binding.require_installed(&host).is_err());
        assert!(
            binding
                .observe(br#"{"operation":"admission_completion"}"#)
                .is_err()
        );
        binding.install(&host).unwrap();
        binding.require_installed(&host).unwrap();
        let rest = projection("startup_rest");
        host.publish_catalog_runtime(rest.clone()).unwrap();
        host.catalog_runtime_admitted(&rest.instance_id);
        let idle: serde_json::Value = serde_json::from_slice(
            &binding
                .observe(br#"{"operation":"admission_completion"}"#)
                .unwrap(),
        )
        .unwrap();
        assert!(idle["row"].is_null());
        let target = projection("m07_hms_readonly_cl");
        host.publish_catalog_runtime(target.clone()).unwrap();
        host.catalog_runtime_admitted(&target.instance_id);
        let queued: serde_json::Value = serde_json::from_slice(
            &binding
                .observe(br#"{"operation":"admission_completion"}"#)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(queued["row"]["queued"], 1);
        assert_eq!(queued["row"]["projection_generation"], target.generation);
        assert_eq!(
            queued["row"]["attachment_id"],
            target.attachment_id.to_string()
        );
        assert_eq!(queued["row"]["started"], 0);
        assert_eq!(selections.load(Ordering::SeqCst), 2);
        assert!(calls.lock().unwrap().is_empty());
    }
    #[test]
    fn second_install_and_different_serving_host_never_replace_original_scalar_source() {
        for replacement in [false, true] {
            let (binding, _, _) = fixture();
            let host = CatalogRuntimeProjection::new();
            binding.install(&host).unwrap();
            let original = host.hms_admission_completion();
            let result = if replacement {
                binding.require_installed(&CatalogRuntimeProjection::new())
            } else {
                binding.install(&host)
            };
            assert!(result.is_err());
            assert!(Arc::ptr_eq(
                binding.installed.lock().unwrap().as_ref().unwrap(),
                &original
            ));
            let value: serde_json::Value =
                serde_json::from_slice(&original.snapshot_json().unwrap()).unwrap();
            assert_eq!(value["invalid"], true);
        }
    }
    #[test]
    fn strict_request_refusal_never_enters_original_handler_or_resets() {
        let (binding, _, calls) = fixture();
        let bad: [&[u8]; 5] = [
            br#"{"operation":"admission_completion","operation":"snapshot"}"#,
            br#"{"operation":"admission_completion","other":0}"#,
            br#"{"operation":"reset","domain":"not-a-uuid","phase":1,"sequence":9}"#,
            br#"{"operation":"snapshot""#,
            br#"{"operation":"unknown"}"#,
        ];
        for raw in bad {
            assert!(binding.observe(raw).is_err());
        }
        assert!(binding.observe(&[b' '; 1025]).is_err());
        assert!(calls.lock().unwrap().is_empty());
    }
    #[test]
    fn snapshot_reset_exact_original_bytes_and_error_identity_are_forwarded() {
        let (binding, _, calls) = fixture();
        let snapshot = br#"{ "operation" : "snapshot" }"#;
        let reset=br#"{ "operation":"reset", "domain":"01a12434-b125-7b41-b46d-0329dc6602dc", "phase":1,"sequence":9 }"#;
        assert_eq!(binding.observe(snapshot).unwrap(), b"original-handler");
        assert_eq!(binding.observe(reset).unwrap(), b"original-handler");
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            [snapshot.to_vec(), reset.to_vec()]
        );
        let original: &'static str = "original-finite-probe-failure";
        let setup = HmsListingObservationSetup::new(
            Arc::new(move |_| Err(original)),
            Arc::new(|_| Ok(None)),
        );
        let failing = HmsAdmissionObservation::new(&setup);
        let error = failing.observe(snapshot).unwrap_err();
        assert!(std::ptr::eq(error.as_ptr(), original.as_ptr()));
    }
}
