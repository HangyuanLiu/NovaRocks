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

use crate::catalog::hms_listing_observer::{HmsListingOperation, sdk_call};
use crate::catalog::listing_admission::ListingAdmission;
use crate::hms_listing_probe::{HmsListingProbe, REQUEST_LIMIT, RESPONSE_LIMIT};
use novarocks_connector_contract::{CatalogHandle, CatalogVersion, ConnectorInstanceId};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRequestContext, ConnectorStopOwner,
    ProviderBindingEpoch,
};
use serde_json::{Value, json};
use std::time::Duration;
use std::{sync::Arc, time::Instant};

fn handle(version: u8) -> CatalogHandle {
    CatalogHandle::new(
        ConnectorInstanceId::parse("hms_test").unwrap(),
        CatalogVersion::from_bytes([version; 32]),
    )
}
fn snapshot(probe: &HmsListingProbe) -> Value {
    serde_json::from_slice(&probe.handle_json(br#"{"operation":"snapshot"}"#).unwrap()).unwrap()
}
fn reset(probe: &HmsListingProbe, s: &Value) -> Result<Vec<u8>, &'static str> {
    probe.handle_json(&serde_json::to_vec(&json!({"operation":"reset", "domain":s["domain"], "phase":s["phase"], "sequence":s["sequence"]})).unwrap())
}
fn attach(probe: &HmsListingProbe, gate: &Arc<ListingAdmission>, epoch: ProviderBindingEpoch) {
    probe.capture(&handle(7), epoch, "hive", gate).unwrap();
}

#[test]
fn observation_does_not_retain_or_replace_the_original_generation() {
    let probe = HmsListingProbe::new("HMS_TEST").unwrap();
    let gate = Arc::new(ListingAdmission::default());
    let weak = Arc::downgrade(&gate);
    let epoch = ProviderBindingEpoch::new();
    attach(&probe, &gate, epoch);
    attach(&probe, &gate, epoch);
    assert_eq!(Arc::strong_count(&gate), 1);
    let s = snapshot(&probe);
    assert_eq!(s["catalog_name"], "hms_test");
    assert_eq!(s["catalog_version"], "07".repeat(32));
    assert_eq!(
        s["incarnation"],
        uuid::Uuid::from_bytes(epoch.to_bytes()).to_string()
    );
    assert_eq!(s["process_id"], std::process::id());
    assert!(probe.capture(&handle(8), epoch, "hive", &gate).is_err());
    assert!(
        probe
            .capture(&handle(7), ProviderBindingEpoch::new(), "hive", &gate)
            .is_err()
    );
    let replacement = Arc::new(ListingAdmission::default());
    assert!(
        probe
            .capture(&handle(7), epoch, "hive", &replacement)
            .is_err()
    );
    drop(gate);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        probe
            .handle_json(br#"{"operation":"snapshot"}"#)
            .unwrap_err(),
        "HMS observation generation has exited"
    );
    assert!(
        probe
            .capture(&handle(7), epoch, "hive", &replacement)
            .is_err()
    );
}

#[tokio::test]
async fn actual_pending_call_prevents_reset_until_sdk_and_position_exit() {
    let probe = HmsListingProbe::new("hms_test").unwrap();
    let gate = Arc::new(ListingAdmission::default());
    attach(&probe, &gate, ProviderBindingEpoch::new());
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(5),
        stop.view(),
        1024,
        4096,
    )
    .unwrap();
    let call = gate.run_hms(
        &context,
        HmsListingOperation::Tables,
        Some([3; 32]),
        |invocation| async move {
            sdk_call(
                Some(&invocation),
                std::future::pending::<Result<(), ConnectorError>>(),
            )
            .await
        },
    );
    tokio::pin!(call);
    assert!(futures::poll!(&mut call).is_pending());
    let active = snapshot(&probe);
    assert_eq!(active["admitted_wrappers_live"], 1);
    assert_eq!(active["sdk_objects_live"], 1);
    assert_eq!(active["available_positions_sample"], 7);
    assert!(reset(&probe, &active).is_err());
    stop.request_stop();
    assert_eq!(
        call.await.unwrap_err().kind(),
        ConnectorErrorKind::Cancelled
    );
    let retired = snapshot(&probe);
    assert_eq!(retired["admitted_wrappers_live"], 0);
    assert_eq!(retired["sdk_objects_live"], 0);
    assert_eq!(retired["invocations_in_flight"], 0);
    assert_eq!(retired["available_positions_sample"], 8);
    let record = &retired["records"][0];
    assert_eq!(record["selection"], "StopAdmitted");
    assert_eq!(record["target_sha256"], "03".repeat(32));
    let events = [
        "started",
        "acquired",
        "sdk_created",
        "sdk_first_poll",
        "sdk_dropped",
        "wrapper_dropped",
        "permit_returned",
        "settled",
    ];
    for pair in events.windows(2) {
        assert!(record[pair[0]].as_u64().unwrap() < record[pair[1]].as_u64().unwrap());
    }
    assert!(reset(&probe, &active).is_err());
    reset(&probe, &retired).unwrap();
    assert!(reset(&probe, &retired).is_err());
    let next = snapshot(&probe);
    assert_eq!(next["domain"], retired["domain"]);
    assert_eq!(next["phase"], retired["phase"].as_u64().unwrap() + 1);
    assert_eq!(next["sequence"], retired["sequence"]);
    assert_eq!(next["used"], 0);
}

#[tokio::test]
async fn full_original_journal_fits_finite_export_and_overflow_stays_invalid() {
    let probe = HmsListingProbe::new("hms_test").unwrap();
    let gate = Arc::new(ListingAdmission::default());
    attach(&probe, &gate, ProviderBindingEpoch::new());
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        stop.view(),
        1024,
        4096,
    )
    .unwrap();
    for _ in 0..1024 {
        gate.run_hms(
            &context,
            HmsListingOperation::Tables,
            Some([255; 32]),
            |invocation| async move {
                sdk_call(Some(&invocation), async { Ok::<_, ConnectorError>(()) }).await
            },
        )
        .await
        .unwrap();
    }
    let body = probe.handle_json(br#"{"operation":"snapshot"}"#).unwrap();
    assert!(body.len() < RESPONSE_LIMIT);
    assert!(body.capacity() <= RESPONSE_LIMIT);
    let s: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(s["records"].as_array().unwrap().len(), 1024);
    gate.run_hms(&context, HmsListingOperation::Namespaces, None, |_| async {
        Ok::<_, ConnectorError>(())
    })
    .await
    .unwrap();
    assert!(probe.handle_json(br#"{"operation":"snapshot"}"#).is_err());
    assert!(reset(&probe, &s).is_err());
}

#[test]
fn malformed_requests_cannot_reset_and_foreign_catalog_cannot_attach() {
    let probe = HmsListingProbe::new("hms_test").unwrap();
    let gate = Arc::new(ListingAdmission::default());
    let epoch = ProviderBindingEpoch::new();
    let foreign = CatalogHandle::new(
        ConnectorInstanceId::parse("other").unwrap(),
        handle(7).version(),
    );
    probe.capture(&foreign, epoch, "hive", &gate).unwrap();
    assert!(probe.handle_json(br#"{"operation":"snapshot"}"#).is_err());
    assert!(probe.capture(&handle(7), epoch, "rest", &gate).is_err());
    attach(&probe, &gate, epoch);
    let before = snapshot(&probe);
    for bad in [
        br#"{"operation":"snapshot","extra":1}"#.as_slice(),
        br#"{"operation":"snapshot","operation":"reset"}"#,
        br#"{"operation":"reset","domain":false,"phase":true,"sequence":0}"#,
        br#"{"operation":"snapshot"}{}"#,
    ] {
        assert!(
            probe.handle_json(bad).is_err(),
            "accepted malformed request: {}",
            String::from_utf8_lossy(bad)
        );
    }
    assert!(probe.handle_json(&vec![b' '; REQUEST_LIMIT + 1]).is_err());
    assert_eq!(snapshot(&probe), before);
    for name in ["", "bad/name", "has space"] {
        assert!(HmsListingProbe::new(name).is_err());
    }
    assert!(HmsListingProbe::new(&"a".repeat(65)).is_err());
}
