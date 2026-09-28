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

//! Physical memory readings from mechanisms other than the counting wrapper
//! (MEM-1 M08a).
//!
//! A sample holds what the operating system and the selected allocator report
//! about this process at one instant. Like the lines of a
//! [`super::CoverageReport`], each field comes from its own mechanism over its
//! own scope, so the fields are never added to each other or to the wrapper's
//! count. A field the process cannot measure — no cgroup, an allocator build
//! without statistics, a failed read — is `None`: unknown, never zero.
//!
//! This module only defines the shape. Reading cgroup files, the resident set
//! or allocator statistics belongs to the process that knows its platform and
//! allocator; this crate stays free of both.

use super::coverage::{MeasuredSource, SourceReading};

/// The selected allocator's own statistics about the memory it manages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocatorInternalsReading {
    /// Bytes currently allocated by the application, as the allocator counts
    /// them (for jemalloc, `stats.allocated`).
    pub allocated_bytes: u64,
    /// Bytes in pages the allocator has handed out, including size-class
    /// rounding (for jemalloc, `stats.active`).
    pub active_bytes: u64,
    /// Bytes of physically resident memory the allocator maps, including its
    /// metadata and freed pages it has not returned yet (for jemalloc,
    /// `stats.resident`).
    pub resident_bytes: u64,
}

/// One sample of this process's physical memory, from sources other than the
/// counting wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PhysicalMemoryReading {
    /// Anonymous memory charged to this process's cgroup
    /// (`active_anon + inactive_anon`). `None` when no cgroup limit bounds the
    /// process or the cgroup could not be read.
    pub cgroup_anonymous_bytes: Option<u64>,
    /// This process's resident set size, read when no cgroup limit bounds the
    /// process. `None` when a cgroup reading is used instead or the platform
    /// offers no reading.
    pub process_resident_bytes: Option<u64>,
    /// The allocator's statistics. `None` when the build's allocator publishes
    /// none, or they could not be read.
    pub allocator_internals: Option<AllocatorInternalsReading>,
}

impl PhysicalMemoryReading {
    /// Returns the allocator-internals line of a coverage report: the
    /// allocator's resident bytes, or an explicit unknown.
    ///
    /// Resident bytes are the figure that answers
    /// [`super::BlindSpot::AllocatorRetentionAndFragmentation`]: they include
    /// the rounding, retention and fragmentation the wrapper cannot see.
    pub const fn allocator_internals_line(&self) -> SourceReading {
        match self.allocator_internals {
            Some(reading) => {
                SourceReading::measured(MeasuredSource::AllocatorInternals, reading.resident_bytes)
            }
            None => SourceReading::unknown(MeasuredSource::AllocatorInternals),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::BlindSpot;

    #[test]
    fn an_allocator_without_statistics_is_an_unknown_line_not_zero() {
        let reading = PhysicalMemoryReading::default();
        assert_eq!(
            reading.allocator_internals_line(),
            SourceReading::unknown(MeasuredSource::AllocatorInternals)
        );
        assert_eq!(reading.allocator_internals_line().measured_bytes(), None);
    }

    #[test]
    fn allocator_statistics_answer_the_retention_blind_spot_with_resident_bytes() {
        let reading = PhysicalMemoryReading {
            allocator_internals: Some(AllocatorInternalsReading {
                allocated_bytes: 100,
                active_bytes: 120,
                resident_bytes: 150,
            }),
            ..PhysicalMemoryReading::default()
        };
        let line = reading.allocator_internals_line();
        assert_eq!(line.measured_bytes(), Some(150));
        assert_eq!(
            BlindSpot::AllocatorRetentionAndFragmentation.measurable_source(),
            Some(line.source())
        );
    }
}
