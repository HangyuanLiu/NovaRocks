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

//! Feature-only scalar observations of the original MV admission worker.
//! This one selected owner grants no catalog, SDK, request, or reset authority.
use std::sync::{Arc, Mutex};

use novarocks_catalog_application::CatalogRuntimeObservation;
use novarocks_spi::connector::{CatalogHandle, ProviderBindingEpoch};
use serde::Serialize;

#[derive(Clone, PartialEq, Eq)]
pub struct InstalledOwner {
    pub handle: CatalogHandle,
    pub incarnation: ProviderBindingEpoch,
}
// Server binds this once before SQL admission. It reads the existing HMS
// probe's captured owner only, never metadata/SDK or a later journal DTO.
pub type OwnerSelector = Arc<
    dyn Fn(&CatalogRuntimeObservation) -> Result<Option<InstalledOwner>, &'static str>
        + Send
        + Sync,
>;

#[derive(Clone, Copy, Serialize, PartialEq, Eq, Debug)]
pub enum Outcome {
    Pending,
    QuarantinedUnsupported,
    ReturnedComplete,
    Failed,
}

#[derive(Clone, Serialize)]
pub struct Row {
    catalog_name: String,
    attachment_id: uuid::Uuid,
    projection_generation: u64,
    catalog_version: String,
    incarnation: uuid::Uuid,
    queued: u64,
    started: u64,
    bound: u64,
    quarantined: u64,
    returned: u64,
    outcome: Outcome,
}
struct Inner {
    selector: Option<OwnerSelector>,
    identity: Option<InstalledOwner>,
    projection: Option<CatalogRuntimeObservation>,
    row: Option<Row>,
    sequence: u64,
    invalid: bool,
}
pub struct AdmissionCompletion {
    inner: Mutex<Inner>,
}
#[derive(Clone)]
pub struct Sweep {
    owner: Arc<AdmissionCompletion>,
    projection: CatalogRuntimeObservation,
}

impl AdmissionCompletion {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                selector: None,
                identity: None,
                projection: None,
                row: None,
                sequence: 0,
                invalid: false,
            }),
        })
    }
    pub(crate) fn bind_selector(&self, selector: OwnerSelector) -> Result<(), &'static str> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| "admission observation lock failed")?;
        if inner.selector.is_some() || inner.row.is_some() || inner.invalid {
            inner.invalid = true;
            return Err("admission selector already bound or failed");
        }
        inner.selector = Some(selector);
        Ok(())
    }
    pub(crate) fn queue(self: &Arc<Self>, projection: &CatalogRuntimeObservation) -> Option<Sweep> {
        // Never invoke a selector while holding the observation lock.
        let selector = self.inner.lock().ok()?.selector.clone()?;
        let selected = match selector(projection) {
            Ok(None) => return None, // Non-target startup REST catalog: unchanged.
            Ok(Some(value)) => value,
            Err(_) => {
                self.fail();
                return None;
            }
        };
        let mut inner = self.inner.lock().ok()?;
        if inner.invalid
            || inner.row.is_some()
            || projection.generation == 0
            || projection.instance_id.as_str().len() > 64
            || selected.handle.catalog_name() != &projection.instance_id
        {
            inner.invalid = true;
            return None;
        }
        let Some(sequence) = inner.sequence.checked_add(1) else {
            inner.invalid = true;
            return None;
        };
        inner.sequence = sequence;
        inner.row = Some(Row {
            catalog_name: projection.instance_id.as_str().to_owned(),
            attachment_id: projection.attachment_id,
            projection_generation: projection.generation,
            catalog_version: selected
                .handle
                .version()
                .as_bytes()
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect(),
            incarnation: uuid::Uuid::from_bytes(selected.incarnation.to_bytes()),
            queued: sequence,
            started: 0,
            bound: 0,
            quarantined: 0,
            returned: 0,
            outcome: Outcome::Pending,
        });
        inner.identity = Some(selected);
        inner.projection = Some(projection.clone());
        Some(Sweep {
            owner: Arc::clone(self),
            projection: projection.clone(),
        })
    }
    pub(crate) fn retired(
        &self,
        instance: &novarocks_spi::connector::ConnectorInstanceId,
        generation: u64,
    ) {
        if let Ok(mut inner) = self.inner.lock() {
            if inner
                .projection
                .as_ref()
                .is_some_and(|p| p.instance_id == *instance && p.generation == generation)
            {
                inner.invalid = true;
            }
        }
    }
    pub(crate) fn fail(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.invalid = true;
        }
    }
    pub fn snapshot_json(&self) -> Result<Vec<u8>, &'static str> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| "admission observation lock failed")?;
        #[derive(Serialize)]
        struct Reply<'a> {
            schema_version: u32,
            process_id: u32,
            invalid: bool,
            sequence: u64,
            row: &'a Option<Row>,
        }
        let raw = serde_json::to_vec(&Reply {
            schema_version: 1,
            process_id: std::process::id(),
            invalid: inner.invalid,
            sequence: inner.sequence,
            row: &inner.row,
        })
        .map_err(|_| "admission observation encoding failed")?;
        if raw.len() > 2048 {
            return Err("admission observation exceeds fixed bound");
        }
        Ok(raw)
    }
}
impl Sweep {
    fn update(&self, apply: impl FnOnce(&mut Row, u64) -> bool) {
        let Ok(mut inner) = self.owner.inner.lock() else {
            return;
        };
        if inner.invalid || inner.projection.as_ref() != Some(&self.projection) {
            inner.invalid = true;
            return;
        }
        let Some(sequence) = inner.sequence.checked_add(1) else {
            inner.invalid = true;
            return;
        };
        let Some(row) = inner.row.as_mut() else {
            inner.invalid = true;
            return;
        };
        if !apply(row, sequence) {
            inner.invalid = true;
            return;
        }
        inner.sequence = sequence;
    }
    pub(crate) fn started(&self) {
        self.update(|row, seq| {
            if row.started != 0 || row.returned != 0 {
                return false;
            }
            row.started = seq;
            true
        });
    }
    pub(crate) fn bound(&self, handle: &CatalogHandle, incarnation: ProviderBindingEpoch) {
        let matches = self.owner.inner.lock().ok().is_some_and(|inner| {
            inner
                .identity
                .as_ref()
                .is_some_and(|v| v.handle == *handle && v.incarnation == incarnation)
        });
        if !matches {
            self.owner.fail();
            return;
        }
        self.update(|row, seq| {
            if row.started == 0 || row.bound != 0 || row.returned != 0 {
                return false;
            }
            row.bound = seq;
            true
        });
    }
    pub(crate) fn quarantined(&self, kind: novarocks_spi::connector::ConnectorErrorKind) {
        self.update(|row, seq| {
            if row.bound == 0 || row.quarantined != 0 || row.returned != 0 {
                return false;
            }
            row.quarantined = seq;
            row.outcome = if kind == novarocks_spi::connector::ConnectorErrorKind::Unsupported {
                Outcome::QuarantinedUnsupported
            } else {
                Outcome::Failed
            };
            true
        });
    }
    pub(crate) fn failed(&self) {
        self.owner.fail();
    }
    pub(crate) fn returned(&self) {
        self.update(|row, seq| {
            if row.started == 0 || row.bound == 0 || row.returned != 0 {
                return false;
            }
            row.returned = seq;
            if row.outcome == Outcome::Pending {
                row.outcome = Outcome::ReturnedComplete;
            }
            true
        });
    }
    // Drop deliberately publishes nothing: unwind/drop cannot mint Returned.
}

/// Instrument the existing worker call, without waiting or changing its result.
/// A panic cannot publish Returned; a dropped queued ticket publishes nothing.
pub(crate) fn run_original_sweep(sweep: Option<&Sweep>, work: impl FnOnce()) {
    if let Some(sweep) = sweep {
        sweep.started();
    }
    work();
    if let Some(sweep) = sweep {
        sweep.returned();
    }
}

#[cfg(test)]
#[path = "admission_completion_tests.rs"]
mod tests;
