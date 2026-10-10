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

//! Frontend process memory readings for `/metrics`.
//!
//! The Frontend has no memory ledger. Its third-party internals (transport,
//! runtime, protocol state) are bounded structurally and verified by
//! measurement, which needs the same allocator and resident readings the
//! Backend already exports. The Server supplies the sampler; this module only
//! publishes what it returns on each scrape.

use std::sync::Arc;

use novarocks_memory::observe::PhysicalMemoryReading;
use prometheus::{IntGaugeVec, Opts, Registry};

/// What the Frontend `/metrics` endpoint reports about its own process.
#[derive(Clone)]
pub struct FrontendProcessMemoryObservation {
    /// The allocator that serves this process, as a stable label.
    pub allocator: &'static str,
    /// Sampled on every scrape.
    pub sample: Arc<dyn Fn() -> PhysicalMemoryReading + Send + Sync>,
}

pub(super) struct ProcessMemoryGauges {
    observation: FrontendProcessMemoryObservation,
    physical: IntGaugeVec,
    allocator: IntGaugeVec,
}

impl ProcessMemoryGauges {
    pub(super) fn register(
        registry: &Registry,
        observation: FrontendProcessMemoryObservation,
    ) -> Result<Self, String> {
        let gauge_vec = |name: &str, help: &str, label: &str| -> Result<IntGaugeVec, String> {
            let gauge = IntGaugeVec::new(Opts::new(name, help), &[label])
                .map_err(|error| format!("construct {name}: {error}"))?;
            registry
                .register(Box::new(gauge.clone()))
                .map_err(|error| format!("register {name}: {error}"))?;
            Ok(gauge)
        };
        let allocator_info = gauge_vec(
            "novarocks_frontend_process_allocator_info",
            "The allocator serving this Frontend process; the value is always 1.",
            "allocator",
        )?;
        let physical = gauge_vec(
            "novarocks_frontend_process_physical_memory_bytes",
            "Physical memory of this Frontend process as the operating system reports it, one \
             series per source. Sources overlap and are not additive; a source that cannot be \
             read is absent.",
            "source",
        )?;
        let allocator = gauge_vec(
            "novarocks_frontend_process_allocator_memory_bytes",
            "The process allocator's own statistics, one series per statistic. Statistics \
             overlap and are not additive; they are absent when the allocator publishes none.",
            "statistic",
        )?;
        allocator_info
            .with_label_values(&[observation.allocator])
            .set(1);
        Ok(Self {
            observation,
            physical,
            allocator,
        })
    }

    /// Publish one fresh reading. A source the reading lacks is removed, not
    /// reported as zero.
    pub(super) fn publish(&self) {
        let reading = (self.observation.sample)();
        let set_or_remove = |gauge: &IntGaugeVec, label: &str, value: Option<u64>| match value {
            Some(bytes) => gauge
                .with_label_values(&[label])
                .set(i64::try_from(bytes).unwrap_or(i64::MAX)),
            None => {
                let _ = gauge.remove_label_values(&[label]);
            }
        };
        set_or_remove(
            &self.physical,
            "cgroup_anonymous",
            reading.cgroup_anonymous_bytes,
        );
        set_or_remove(
            &self.physical,
            "process_resident",
            reading.process_resident_bytes,
        );
        let internals = reading.allocator_internals;
        set_or_remove(
            &self.allocator,
            "allocated",
            internals.map(|reading| reading.allocated_bytes),
        );
        set_or_remove(
            &self.allocator,
            "active",
            internals.map(|reading| reading.active_bytes),
        );
        set_or_remove(
            &self.allocator,
            "resident",
            internals.map(|reading| reading.resident_bytes),
        );
    }
}
