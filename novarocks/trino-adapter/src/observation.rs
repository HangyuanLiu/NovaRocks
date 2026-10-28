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

//! Listener-local protocol facts. Observing these counters grants no query authority.
use crate::settlement::Disposition;
use prometheus::{IntCounterVec, IntGauge, Opts, Registry};
#[derive(Clone)]
pub struct TrinoProtocolObservation {
    active: IntGauge,
    settled: IntCounterVec,
    write_failures: IntCounterVec,
}
#[derive(Clone, Copy)]
pub(crate) enum ResponseWriteFailure {
    Timeout,
    Interrupted,
}
impl TrinoProtocolObservation {
    pub(crate) fn new() -> Result<Self, prometheus::Error> {
        Ok(Self {
            active: IntGauge::new(
                "novarocks_trino_queries_active",
                "Executing Trino entries, including sealed output awaiting acknowledgement.",
            )?,
            settled: IntCounterVec::new(
                Opts::new(
                    "novarocks_trino_query_settlements_total",
                    "Trino protocol settlements by first terminal disposition.",
                ),
                &["outcome"],
            )?,
            write_failures: IntCounterVec::new(
                Opts::new(
                    "novarocks_trino_response_write_failures_total",
                    "Trino response writes interrupted or closed by their write deadline.",
                ),
                &["reason"],
            )?,
        })
    }
    pub fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        registry.register(Box::new(self.active.clone()))?;
        registry.register(Box::new(self.settled.clone()))?;
        registry.register(Box::new(self.write_failures.clone()))?;
        Ok(())
    }
    pub(crate) fn write_failed(&self, reason: ResponseWriteFailure) {
        self.write_failures
            .with_label_values(&[match reason {
                ResponseWriteFailure::Timeout => "timeout",
                ResponseWriteFailure::Interrupted => "interrupted",
            }])
            .inc();
    }
    pub(crate) fn start(&self) {
        self.active.inc();
    }
    pub(crate) fn settle(&self, executing: bool, disposition: Disposition) {
        if executing {
            self.active.dec();
        }
        self.settled
            .with_label_values(&[match disposition {
                Disposition::Complete => "complete",
                Disposition::Fail | Disposition::Cancelled => "failed",
                Disposition::ClientCancel => "client_cancel",
                Disposition::Abandon => "abandoned",
            }])
            .inc();
    }
}
