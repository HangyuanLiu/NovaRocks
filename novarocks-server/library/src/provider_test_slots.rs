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

//! Role-local inert provider observation slots and post-identity activation.
use novarocks_connector_iceberg::test_observation::IcebergObservationSlot;
use novarocks_failpoint::scope::SessionScope;
use std::sync::{Arc, Mutex};
#[derive(Clone, Default)]
pub struct ProviderTestSlots {
    pub iceberg: IcebergObservationSlot,
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub hms: novarocks_connector_iceberg::hms_listing_probe::HmsListingObservationSlot,
    storage: Arc<Mutex<Vec<Arc<novarocks_fs::StorageAuthorityRegistry>>>>,
}
impl ProviderTestSlots {
    pub fn register_storage(
        &self,
        registry: Arc<novarocks_fs::StorageAuthorityRegistry>,
    ) -> Result<(), String> {
        self.storage
            .lock()
            .map_err(|_| "provider storage registry lock poisoned")?
            .push(registry);
        Ok(())
    }
    pub fn bind_frontend(
        &self,
        scope: &SessionScope,
    ) -> Result<novarocks_frontend_application::test_session::FrontendProviderTestBindings, String>
    {
        let result = self.try_bind_frontend(scope);
        if result.is_err() {
            self.iceberg.close();
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            self.hms.close();
        }
        result
    }
    fn try_bind_frontend(
        &self,
        scope: &SessionScope,
    ) -> Result<novarocks_frontend_application::test_session::FrontendProviderTestBindings, String>
    {
        self.iceberg
            .activate(scope.grants_for(IcebergObservationSlot::DOMAIN))
            .map_err(str::to_owned)?;
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        let probe = self
            .hms
            .activate(scope.grants_for(
                novarocks_connector_iceberg::hms_listing_probe::HmsListingProbe::DOMAIN,
            ))
            .map_err(str::to_owned)?;
        #[cfg(not(feature = "mem-1-m07-hms-listing-observe"))]
        if !scope.grants_for("hms-listing-observation").is_empty() {
            return Err("HMS listing observation requires its explicit build capability".into());
        }
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        let close_probe = probe.clone();
        let input_writer = self.iceberg.clone();
        let writer = self.iceberg.clone();
        let close = self.iceberg.clone();
        Ok(
            novarocks_frontend_application::test_session::FrontendProviderTestBindings {
                pin_writer_input: Some(Arc::new(move |input| {
                    input_writer.pin_statement_input(input)
                })),
                nominate_writer: Some(Arc::new(move |input, nomination| {
                    writer.nominate_writer_statement(input, nomination)
                })),
                close: Some(Arc::new(move || {
                    close.close();
                    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
                    if let Some(probe) = &close_probe {
                        probe.close();
                    }
                    Ok(())
                })),
                #[cfg(feature = "mem-1-m07-hms-listing-observe")]
                hms_listing_observation: probe.as_ref().map(|probe| {
                    let handler = probe.clone();
                    let selected = probe.clone();
                    novarocks_frontend_application::HmsListingObservationSetup::new(
                        Arc::new(move |body| handler.handle_json(body)),
                        Arc::new(move |observation| {
                            selected
                                .captured_admission_owner(&observation.instance_id)
                                .map(|owner| {
                                    owner.map(|(handle, incarnation)| {
                                        novarocks_frontend_application::HmsAdmissionInstalledOwner {
                                            handle,
                                            incarnation,
                                        }
                                    })
                                })
                        }),
                        probe.cancellation(),
                    )
                }),
            },
        )
    }
    pub fn bind_backend(
        &self,
        scope: &SessionScope,
    ) -> Result<Option<novarocks_native_adapter::backend_test_session::ProviderCloseHook>, String>
    {
        self.iceberg
            .activate(scope.grants_for(IcebergObservationSlot::DOMAIN))
            .map_err(str::to_owned)?;
        let storage = novarocks_fs::StorageAuthorityCloseControls::from_grants(
            scope.grants_for("storage-authority"),
        )
        .map_err(|error| {
            self.iceberg.close();
            error
        })?;
        let install = (|| -> Result<(), String> {
            let registries = self
                .storage
                .lock()
                .map_err(|_| "provider storage registry lock poisoned")?;
            for registry in registries.iter() {
                registry.attach_test_close_controls(storage.clone())?;
            }
            Ok(())
        })();
        if let Err(primary) = install {
            self.iceberg.close();
            return Err(match storage.close_and_join() {
                Ok(()) => primary,
                Err(cleanup) => format!("{primary}; storage control rollback: {cleanup}"),
            });
        }
        let close = self.iceberg.clone();
        Ok(Some(Arc::new(move || {
            close.close();
            storage.close_and_join()
        })))
    }
}
