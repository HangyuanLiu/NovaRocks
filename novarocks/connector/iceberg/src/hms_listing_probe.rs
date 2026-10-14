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

//! Bounded feature-only observations of one original catalog generation.
//! The weak reference grants no provider capability or listing position.

use std::io::Write;
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use novarocks_connector_contract::CatalogHandle;
use novarocks_spi::connector::ProviderBindingEpoch;
use serde::ser::{SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};

use crate::catalog::hms_listing_observer::{InvocationRecord, Snapshot};
use crate::catalog::listing_admission::ListingAdmission;

pub const REQUEST_LIMIT: usize = 1024;
pub const RESPONSE_LIMIT: usize = 1024 * 1024;

/// Startup composition installs this empty local slot before the FE mints identity.
/// Only its post-bind activation creates a probe; it grants no provider authority.
#[derive(Clone, Default)]
pub struct HmsListingObservationSlot(Arc<Mutex<SlotState>>);
#[derive(Default)]
struct SlotState {
    activated: bool,
    probe: Option<Arc<HmsListingProbe>>,
}
impl HmsListingObservationSlot {
    pub fn close(&self) {
        if let Ok(mut state) = self.0.lock() {
            if let Some(probe) = state.probe.take() {
                probe.close();
            }
        }
    }

    pub fn activate(
        &self,
        grants: Vec<novarocks_failpoint::scope::GrantHandle>,
    ) -> Result<Option<Arc<HmsListingProbe>>, &'static str> {
        let mut installed = self
            .0
            .lock()
            .map_err(|_| "HMS observation slot lock failed")?;
        if installed.activated {
            return Err("HMS observation slot was already activated");
        }
        let probe = HmsListingProbe::from_grants(grants)?;
        installed.activated = true;
        installed.probe = probe.clone();
        Ok(probe)
    }
    pub(crate) fn active_probe(&self) -> Option<Arc<HmsListingProbe>> {
        self.0
            .lock()
            .ok()?
            .probe
            .as_ref()
            .filter(|probe| probe.is_active())
            .cloned()
    }
}

/// Server-composed test observation, bound once to the named HMS generation.
/// Even after its generation exits, this probe never silently selects another.
pub struct HmsListingProbe {
    catalog_name: String,
    cancellation: novarocks_failpoint::scope::ControlCancellation,
    claim: Mutex<novarocks_failpoint::scope::ActionClaim>,
    target: Mutex<Option<Target>>,
}
struct Target {
    handle: CatalogHandle,
    incarnation: ProviderBindingEpoch,
    listing: Weak<ListingAdmission>,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "lowercase", deny_unknown_fields)]
enum Request {
    Snapshot {},
    Reset {
        domain: uuid::Uuid,
        phase: u64,
        sequence: u64,
    },
}

impl HmsListingProbe {
    pub const DOMAIN: &'static str = "hms-listing-observation";
    pub fn from_grants(
        grants: Vec<novarocks_failpoint::scope::GrantHandle>,
    ) -> Result<Option<Arc<Self>>, &'static str> {
        if grants.is_empty() {
            return Ok(None);
        }
        if grants.iter().any(|grant| grant.domain() != Self::DOMAIN) {
            return Err("HMS observation received a foreign domain grant");
        }
        if grants.len() != 1 {
            return Err("HMS observation requires exactly one grant");
        }
        let grant = grants.into_iter().next().expect("one grant");
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Parameters {
            action: String,
            catalog_name: String,
        }
        let parameters: Parameters = serde_json::from_value(grant.parameters().clone())
            .map_err(|_| "HMS observation grant is invalid")?;
        if parameters.action != "observe" {
            return Err("HMS observation action is unsupported");
        }
        let catalog_name = parameters.catalog_name;
        if catalog_name.is_empty()
            || catalog_name.len() > 64
            || !catalog_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err("HMS observation catalog name is invalid");
        }
        let cancellation = grant.cancellation();
        if !matches!(
            cancellation.process(),
            novarocks_failpoint::scope::ProcessBinding::Frontend(_)
        ) {
            return Err("HMS observation requires the actual FE process");
        }
        let claim = grant
            .claim_once(cancellation.process())
            .map_err(|_| "HMS observation claim refused")?;
        Ok(Some(Arc::new(Self {
            catalog_name: catalog_name.to_ascii_lowercase(),
            cancellation,
            claim: Mutex::new(claim),
            target: Mutex::new(None),
        })))
    }
    pub fn cancellation(&self) -> novarocks_failpoint::scope::ControlCancellation {
        self.cancellation.clone()
    }
    pub fn is_active(&self) -> bool {
        self.cancellation.is_active()
    }
    pub fn close(&self) {
        if let Ok(mut target) = self.target.lock() {
            if let Some(target) = target.take() {
                if let Some(listing) = target.listing.upgrade() {
                    listing.close_hms_observation();
                }
            }
        }
    }
    pub async fn close_when_cancelled(&self) {
        self.cancellation.cancelled().await;
        self.close();
    }

    pub(crate) fn capture(
        &self,
        handle: &CatalogHandle,
        incarnation: ProviderBindingEpoch,
        implementation: &str,
        listing: &Arc<ListingAdmission>,
    ) -> Result<(), &'static str> {
        if !self.is_active() {
            return Err("HMS observation session is closed");
        }
        if !handle
            .catalog_name()
            .as_str()
            .eq_ignore_ascii_case(&self.catalog_name)
        {
            return Ok(());
        }
        if implementation != "hive" {
            return Err("HMS observation target is not a Hive catalog");
        }
        let mut target = self
            .target
            .lock()
            .map_err(|_| "HMS observation target lock failed")?;
        let weak = Arc::downgrade(listing);
        if let Some(original) = target.as_ref() {
            if original.handle == *handle
                && original.incarnation == incarnation
                && original.listing.ptr_eq(&weak)
            {
                return Ok(());
            }
            return Err("HMS observation generation replacement refused");
        }
        listing.install_hms_observation(self.cancellation.clone())?;
        self.claim
            .lock()
            .map_err(|_| "HMS observation claim lock failed")?
            .record_hit()
            .map_err(|_| "HMS observation capture could not be recorded")?;
        *target = Some(Target {
            handle: handle.clone(),
            incarnation,
            listing: weak,
        });
        Ok(())
    }

    /// Read the original materialized target; no SDK call or second selection.
    pub fn captured_admission_owner(
        &self,
        candidate: &novarocks_connector_contract::ConnectorInstanceId,
    ) -> Result<Option<(CatalogHandle, ProviderBindingEpoch)>, &'static str> {
        if !self.is_active() {
            self.close();
            return Err("HMS observation session is closed");
        }
        if candidate.as_str() != self.catalog_name {
            return Ok(None);
        }
        let target = self
            .target
            .lock()
            .map_err(|_| "HMS observation target lock failed")?;
        let target = target
            .as_ref()
            .ok_or("HMS admission owner was not captured before publication")?;
        if target.handle.catalog_name() != candidate || target.listing.upgrade().is_none() {
            return Err("HMS captured admission generation differs or has exited");
        }
        Ok(Some((target.handle.clone(), target.incarnation)))
    }

    /// Strict, finite request/response boundary for the existing management owner.
    pub fn handle_json(&self, body: &[u8]) -> Result<Vec<u8>, &'static str> {
        if !self.is_active() {
            self.close();
            return Err("HMS observation session is closed");
        }
        if body.len() > REQUEST_LIMIT {
            return Err("HMS observation request exceeds its limit");
        }
        let request: Request =
            serde_json::from_slice(body).map_err(|_| "HMS observation request is invalid")?;
        let target = self
            .target
            .lock()
            .map_err(|_| "HMS observation target lock failed")?;
        let target = target
            .as_ref()
            .ok_or("HMS observation generation is not installed")?;
        let listing = target
            .listing
            .upgrade()
            .ok_or("HMS observation generation has exited")?;
        match request {
            Request::Snapshot {} => {
                let snapshot = listing.hms_snapshot()?;
                let response = SnapshotResponse {
                    schema_version: 1,
                    process_id: std::process::id(),
                    catalog_name: &self.catalog_name,
                    catalog_version: hex32(*target.handle.version().as_bytes()),
                    incarnation: uuid::Uuid::from_bytes(target.incarnation.to_bytes()),
                    domain: snapshot.domain,
                    phase: snapshot.phase,
                    sequence: snapshot.sequence,
                    invocations_in_flight: snapshot.invocations_in_flight,
                    admitted_wrappers_live: snapshot.admitted_wrappers_live,
                    peak_admitted_wrappers_live: snapshot.peak_admitted_wrappers_live,
                    available_positions_sample: snapshot.available_positions_sample,
                    sdk_objects_live: snapshot.sdk_objects_live,
                    peak_sdk_objects_live: snapshot.peak_sdk_objects_live,
                    used: snapshot.used,
                    records: Records {
                        snapshot: &snapshot,
                        now: Instant::now(),
                    },
                };
                encode(&response)
            }
            Request::Reset {
                domain,
                phase,
                sequence,
            } => {
                let phase = listing.reset_hms_observation_idle(domain, phase, sequence)?;
                // The fixed response always fits; a serialization limit cannot
                // turn a committed reset into an ambiguous outcome.
                encode(&ResetResponse {
                    schema_version: 1,
                    process_id: std::process::id(),
                    domain,
                    phase,
                })
            }
        }
    }
}

#[derive(Serialize)]
struct ResetResponse {
    schema_version: u32,
    process_id: u32,
    domain: uuid::Uuid,
    phase: u64,
}
#[derive(Serialize)]
struct SnapshotResponse<'a> {
    schema_version: u32,
    process_id: u32,
    catalog_name: &'a str,
    catalog_version: String,
    incarnation: uuid::Uuid,
    domain: uuid::Uuid,
    phase: u64,
    sequence: u64,
    invocations_in_flight: u64,
    admitted_wrappers_live: u64,
    peak_admitted_wrappers_live: u64,
    available_positions_sample: Option<usize>,
    sdk_objects_live: u64,
    peak_sdk_objects_live: u64,
    used: usize,
    records: Records<'a>,
}
struct Records<'a> {
    snapshot: &'a Snapshot,
    now: Instant,
}
impl Serialize for Records<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.snapshot.used))?;
        for slot in self.snapshot.records.iter().take(self.snapshot.used) {
            let record = slot
                .as_ref()
                .ok_or_else(|| serde::ser::Error::custom("missing original HMS record"))?;
            sequence.serialize_element(
                &RecordResponse::new(record, self.now).map_err(serde::ser::Error::custom)?,
            )?;
        }
        sequence.end()
    }
}
#[derive(Serialize)]
struct RecordResponse {
    ordinal: u64,
    operation: &'static str,
    target_sha256: Option<String>,
    original_deadline_remaining_nanos: u64,
    original_deadline_elapsed: bool,
    started: u64,
    acquired: u64,
    sdk_created: u64,
    sdk_first_poll: u64,
    sdk_ready: u64,
    sdk_dropped: u64,
    wrapper_dropped: u64,
    permit_returned: u64,
    settled: u64,
    selection: &'static str,
    stop_at_selection: bool,
    deadline_at_selection: bool,
}
impl RecordResponse {
    fn new(r: &InvocationRecord, now: Instant) -> Result<Self, &'static str> {
        use crate::catalog::hms_listing_observer::{ExitSelection as E, HmsListingOperation as O};
        let remaining = r.original_deadline.checked_duration_since(now);
        let nanos = u64::try_from(remaining.map_or(0, |d| d.as_nanos()))
            .map_err(|_| "HMS observation deadline exceeds its representation")?;
        Ok(Self {
            ordinal: r.ordinal,
            operation: match r.operation {
                O::Namespaces => "Namespaces",
                O::Tables => "Tables",
                O::Views => "Views",
            },
            target_sha256: r.target_sha256.map(hex32),
            original_deadline_remaining_nanos: nanos,
            original_deadline_elapsed: now >= r.original_deadline,
            started: r.started,
            acquired: r.acquired,
            sdk_created: r.sdk_created,
            sdk_first_poll: r.sdk_first_poll,
            sdk_ready: r.sdk_ready,
            sdk_dropped: r.sdk_dropped,
            wrapper_dropped: r.wrapper_dropped,
            permit_returned: r.permit_returned,
            settled: r.settled,
            selection: match r.selection {
                E::OwnerDropped => "OwnerDropped",
                E::InitialCheck => "InitialCheck",
                E::StopWaiting => "StopWaiting",
                E::DeadlineWaiting => "DeadlineWaiting",
                E::AdmissionClosed => "AdmissionClosed",
                E::StopAdmitted => "StopAdmitted",
                E::DeadlineAdmitted => "DeadlineAdmitted",
                E::ReadyOk => "ReadyOk",
                E::ReadyErr => "ReadyErr",
            },
            stop_at_selection: r.stop_at_selection,
            deadline_at_selection: r.deadline_at_selection,
        })
    }
}
fn hex32(bytes: [u8; 32]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(64);
    for byte in bytes {
        write!(&mut result, "{byte:02x}").expect("finite hex string");
    }
    result
}
struct BoundedWriter(Vec<u8>);
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > RESPONSE_LIMIT - self.0.len() {
            return Err(std::io::Error::other(
                "HMS observation response exceeds its limit",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>, &'static str> {
    // Reserve the finite buffer once, so allocator growth cannot overshoot it.
    let mut writer = BoundedWriter(Vec::with_capacity(RESPONSE_LIMIT));
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| "HMS observation response could not be encoded")?;
    Ok(writer.0)
}

#[cfg(all(test, unix))]
mod authorization_tests {
    use super::*;
    use novarocks_failpoint::liveness::{PreparedRoleSession, RunnerSession};
    use novarocks_failpoint::scope::ProcessBinding;
    use novarocks_failpoint::session::{
        DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole,
    };
    use novarocks_spi::connector::{CatalogVersion, ConnectorInstanceId};
    use std::os::unix::fs::DirBuilderExt;
    #[test]
    fn inert_or_empty_slot_creates_no_hms_observer() {
        let slot = HmsListingObservationSlot::default();
        assert!(slot.active_probe().is_none());
        assert!(slot.activate(Vec::new()).unwrap().is_none());
        assert!(slot.activate(Vec::new()).is_err());
        assert!(ListingAdmission::default().hms_snapshot().is_err());
    }
    #[test]
    fn exact_authorized_capture_closes_with_its_original_session() {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let root = std::path::PathBuf::from(format!(
            "/tmp/g2hms-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let mut declaration = SessionDeclaration {
            schema_version: DECLARATION_VERSION,
            session_id: "hms-session".into(),
            launch_id: "hms-launch".into(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: root.join("live.sock"),
            handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
            domain_grants: vec![DomainGrant {
                domain: HmsListingProbe::DOMAIN.into(),
                grant_id: "observe-hms".into(),
                parameters: serde_json::json!({"action":"observe","catalog_name":"target_hms"}),
            }],
        };
        let mut foreign = declaration.domain_grants[0].clone();
        foreign.domain = "foreign-domain".into();
        foreign.grant_id = "foreign-grant".into();
        declaration.domain_grants.push(foreign);
        let path = root.join("declaration.json");
        declaration.write_private(&path).unwrap();
        let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let mut role = prepared
            .bind(ProcessBinding::Frontend(
                novarocks_types::FrontendProcessId::new_v7(),
            ))
            .unwrap();
        assert!(HmsListingProbe::from_grants(role.scope().grants_for("foreign-domain")).is_err());
        let slot = HmsListingObservationSlot::default();
        let probe = slot
            .activate(role.scope().grants_for(HmsListingProbe::DOMAIN))
            .unwrap()
            .unwrap();
        let listing = Arc::new(ListingAdmission::default());
        assert!(listing.hms_snapshot().is_err());
        let handle = CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("target_hms").unwrap(),
            CatalogVersion::from_bytes([7; 32]),
        );
        probe
            .capture(
                &handle,
                ProviderBindingEpoch::from_bytes([8; 16]),
                "hive",
                &listing,
            )
            .unwrap();
        assert!(listing.hms_snapshot().is_ok());
        assert!(probe.handle_json(br#"{"operation":"snapshot"}"#).is_ok());
        role.close_and_join().unwrap();
        // Direct old-capability lookup must reject before HTTP or helper cleanup.
        assert!(
            probe
                .captured_admission_owner(handle.catalog_name())
                .is_err()
        );
        assert!(listing.hms_snapshot().is_err());
        assert!(slot.active_probe().is_none());
        assert!(probe.handle_json(br#"{"operation":"snapshot"}"#).is_err());
        assert!(listing.hms_snapshot().is_err());
        runner.end_and_join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
