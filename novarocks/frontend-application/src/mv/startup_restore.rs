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

//! Frontend-owned MV startup restore.
//!
//! Installing this makes the frontend the owner of *when* MV state is restored at
//! startup, which is what "Frontend orchestrates through the installed
//! provider/runtime boundary" asks for. The lake-reading work itself stays in the
//! engine, because a production SQL procedure calls the same targeted rebuild; the
//! decision that moved here is the orchestration, not the code.
//!
//! Every input is a port the frontend already holds. Nothing here reaches into
//! aggregate engine state, which is precisely why this implementation can exist.

use std::sync::Arc;

use crate::catalog_application::CatalogRuntimeProjection;
use crate::mv::domain::readiness::MvReadinessPort;
use crate::mv::domain::startup_restore::MvStartupRestore;
use novarocks_catalog_application::CatalogApplicationPort;
use novarocks_spi::connector::ConnectorControlRegistry;

/// The frontend's implementation of the ordered startup restore steps.
pub(crate) struct FrontendMvStartupRestore {
    connector_control: Arc<dyn ConnectorControlRegistry>,
    catalog_runtime_projection: Arc<CatalogRuntimeProjection>,
    catalog_application: Arc<dyn CatalogApplicationPort>,
    readiness: Arc<MvReadinessPort>,
}

impl FrontendMvStartupRestore {
    pub(crate) fn new(
        connector_control: Arc<dyn ConnectorControlRegistry>,
        catalog_runtime_projection: Arc<CatalogRuntimeProjection>,
        catalog_application: Arc<dyn CatalogApplicationPort>,
        readiness: Arc<MvReadinessPort>,
    ) -> Self {
        Self {
            connector_control,
            catalog_runtime_projection,
            catalog_application,
            readiness,
        }
    }
}

/// Rediscovers one catalog's lake-native MVs the moment it is admitted.
///
/// Restoring at startup alone cannot work: a catalog created by SQL is
/// admitted long after the process opened, and an MV inside it would then stay
/// invisible until the next restart -- which would find the same empty set.
/// Reacting to admission makes the lake the single source the inventory is
/// rebuilt from, whenever its catalog appears.
impl crate::catalog_application::CatalogAdmissionObserver for FrontendMvStartupRestore {
    fn catalog_admitted(&self, instance_id: &novarocks_spi::connector::ConnectorInstanceId) {
        // Both restore steps belong here, in their one order: an MV is
        // rediscovered from the lake and only then registered with the
        // provider-local catalog state that makes it a resolvable table.
        if let Err(error) = crate::mv::domain::lake_rebuild::rebuild_imv_cache_from_catalogs(
            &self.rebuild_context(),
            std::slice::from_ref(instance_id),
        ) {
            // Admission already happened; an observer cannot unadmit it. The
            // affected targets quarantine themselves inside the sweep, so what
            // is left to report here is the sweep failing as a whole.
            tracing::warn!(
                catalog = instance_id.as_str(),
                %error,
                "rebuilding the MV inventory of a newly admitted catalog failed"
            );
            return;
        }
        if let Err(error) = self.restore_targets() {
            tracing::warn!(
                catalog = instance_id.as_str(),
                %error,
                "registering the MV targets of a newly admitted catalog failed"
            );
        }
    }
}

impl FrontendMvStartupRestore {
    fn rebuild_context(&self) -> crate::mv::domain::lake_rebuild::LakeRebuildContext<'_> {
        crate::mv::domain::lake_rebuild::LakeRebuildContext {
            catalog_runtime_projection: Some(&self.catalog_runtime_projection),
            catalog_application: Some(self.catalog_application.as_ref()),
            connector_control: self.connector_control.as_ref(),
            readiness: self.readiness.as_ref(),
        }
    }
}

impl MvStartupRestore for FrontendMvStartupRestore {
    fn rebuild_cache_from_lake(&self) -> Result<(), String> {
        // Always enter the bounded discovery sweep. The admitted catalog
        // projection and provider observations naturally determine whether any
        // lake package is eligible for rebuild.
        crate::mv::domain::lake_rebuild::rebuild_imv_cache_from_lake(&self.rebuild_context())
    }

    fn restore_targets(&self) -> Result<(), String> {
        crate::mv::domain::iceberg_refresh::restore_iceberg_mv_targets(
            &crate::mv::domain::iceberg_refresh::MvTargetRestoreContext {
                connector_control: self.connector_control.as_ref(),
                readiness: self.readiness.as_ref(),
            },
        )
    }
}
