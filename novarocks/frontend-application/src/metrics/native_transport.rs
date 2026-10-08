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

//! Frontend Native lane metrics. The Frontend's outgoing transport admission
//! publishes its transitions here; admission never reads these gauges.

use std::sync::Arc;

use novarocks_native_adapter::native_lane::{
    NativeLane, NativeTransportObserver, PositionKind, StreamDirection,
};
use novarocks_native_adapter::native_transport_admission::TransportClass;
use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};

static FRONTEND_NATIVE_TRANSPORT_POSITIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_native_transport_positions",
            "Process Native connection admission positions by class and kind: live \
             connections (until their IO is dropped) and dials still connecting, used and limit.",
        ),
        &["class", "kind", "dimension"],
    )
    .expect("construct Frontend Native transport position gauge")
});

static FRONTEND_NATIVE_TRANSPORT_REFUSED: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_frontend_native_transport_refused_connections_total",
            "Native connections refused for lack of a connection or handshake position.",
        ),
        &["class"],
    )
    .expect("construct Frontend Native transport refusal counter")
});

static FRONTEND_NATIVE_LANE_CONNECTIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_native_lane_connections",
            "Established Native connections by lane, until their IO is dropped.",
        ),
        &["lane"],
    )
    .expect("construct Frontend Native lane connection gauge")
});

static FRONTEND_NATIVE_LANE_STREAMS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_native_lane_streams",
            "Outgoing lane and incoming Membership stream positions in use by lane, held until the response body \
             ends or is dropped.",
        ),
        &["lane"],
    )
    .expect("construct Frontend Native lane stream gauge")
});

pub(crate) fn register_collectors(registry: &Registry) -> Result<(), String> {
    for lane in NativeLane::ALL {
        let _ = FRONTEND_NATIVE_LANE_CONNECTIONS.get_metric_with_label_values(&[lane.label()]);
        let _ = FRONTEND_NATIVE_LANE_STREAMS.get_metric_with_label_values(&[lane.label()]);
    }
    for class in [
        TransportClass::Data,
        TransportClass::Control,
        TransportClass::Membership,
    ] {
        let _ = FRONTEND_NATIVE_TRANSPORT_REFUSED.get_metric_with_label_values(&[class.label()]);
    }
    for collector in [
        Box::new(FRONTEND_NATIVE_TRANSPORT_POSITIONS.clone())
            as Box<dyn prometheus::core::Collector>,
        Box::new(FRONTEND_NATIVE_TRANSPORT_REFUSED.clone()),
        Box::new(FRONTEND_NATIVE_LANE_CONNECTIONS.clone()),
        Box::new(FRONTEND_NATIVE_LANE_STREAMS.clone()),
    ] {
        registry
            .register(collector)
            .map_err(|error| format!("register frontend native lane collector failed: {error}"))?;
    }
    Ok(())
}

#[derive(Debug, Default)]
struct FrontendNativeTransportMetrics;

impl NativeTransportObserver for FrontendNativeTransportMetrics {
    fn positions(&self, class: TransportClass, kind: PositionKind, used: usize, limit: usize) {
        for (dimension, value) in [("used", used), ("limit", limit)] {
            FRONTEND_NATIVE_TRANSPORT_POSITIONS
                .with_label_values(&[class.label(), kind.label(), dimension])
                .set(i64::try_from(value).unwrap_or(i64::MAX));
        }
    }

    fn refused(&self, class: TransportClass) {
        FRONTEND_NATIVE_TRANSPORT_REFUSED
            .with_label_values(&[class.label()])
            .inc();
    }

    fn lane_connections(&self, lane: NativeLane, delta: i64) {
        FRONTEND_NATIVE_LANE_CONNECTIONS
            .with_label_values(&[lane.label()])
            .add(delta);
    }

    fn lane_streams(&self, lane: NativeLane, direction: StreamDirection, delta: i64) {
        if direction == StreamDirection::Outgoing || lane == NativeLane::Membership {
            FRONTEND_NATIVE_LANE_STREAMS
                .with_label_values(&[lane.label()])
                .add(delta);
        }
    }
}

/// The observer a Frontend installs on its process-scoped transport admission.
pub(crate) fn frontend_native_transport_observer() -> Arc<dyn NativeTransportObserver> {
    Arc::new(FrontendNativeTransportMetrics)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lane_transitions_publish_without_feeding_back() {
        let registry = Registry::new();
        register_collectors(&registry).unwrap();
        let observer = frontend_native_transport_observer();
        let before = FRONTEND_NATIVE_LANE_STREAMS
            .with_label_values(&["observation"])
            .get();
        observer.lane_streams(NativeLane::Observation, StreamDirection::Outgoing, 1);
        observer.lane_streams(NativeLane::Observation, StreamDirection::Incoming, 1);
        assert_eq!(
            FRONTEND_NATIVE_LANE_STREAMS
                .with_label_values(&["observation"])
                .get(),
            before + 1
        );
        observer.lane_streams(NativeLane::Observation, StreamDirection::Outgoing, -1);
        let membership_before = FRONTEND_NATIVE_LANE_STREAMS
            .with_label_values(&["membership"])
            .get();
        observer.lane_streams(NativeLane::Membership, StreamDirection::Incoming, 1);
        assert_eq!(
            FRONTEND_NATIVE_LANE_STREAMS
                .with_label_values(&["membership"])
                .get(),
            membership_before + 1
        );
        observer.lane_streams(NativeLane::Membership, StreamDirection::Incoming, -1);
        observer.positions(TransportClass::Control, PositionKind::Handshake, 3, 32);
        let names: Vec<_> = registry
            .gather()
            .iter()
            .map(|family| family.get_name().to_owned())
            .collect();
        for name in [
            "novarocks_frontend_native_transport_positions",
            "novarocks_frontend_native_transport_refused_connections_total",
            "novarocks_frontend_native_lane_connections",
            "novarocks_frontend_native_lane_streams",
        ] {
            assert!(names.iter().any(|family| family == name), "{name}");
        }
    }
}
