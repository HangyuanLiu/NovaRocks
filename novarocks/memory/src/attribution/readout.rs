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

//! Independent observation samples. No sampling operation belongs in an allocator hook.
use super::{
    BandSnapshot,
    band::{ATTRIBUTION_THRESHOLD_BYTES, ATTRIBUTION_TOKEN_BYTES},
};
use crate::lane::faults::FaultSnapshot;
use crate::lane::record::{CLASS_MASK, CLASS_SHIFT, PRODUCTION_MASK};
use crate::lane::store::SEGMENT_RECORDS;
use crate::lane::{
    LaneRecord, RecordSnapshot, RecordStore, SLOT_QUANTUM_BYTES, UNATTRIBUTED_SHARDS,
};
use std::time::{SystemTime, UNIX_EPOCH};

pub const CLASS_LABELS: [&str; 4] = ["query", "residual", "service", "unattributed"];
pub const PRODUCTION_LABELS: [&str; 3] = ["producing", "sealed", "stopped"];
/// Per-class signed observations. Negative intermediate samples are permitted
/// by pinned batching, and must not be hidden as zero live physical memory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClassFacts {
    pub tagged_bytes: i128,
    pub r1_small_bytes: i128,
}
/// One bounded-prefix sample, not a coherent heap snapshot or settlement proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttributionSnapshot {
    pub process: BandSnapshot,
    /// Indices follow CLASS_LABELS; unattributed tagged bytes remain explicit.
    pub classified: [ClassFacts; 3],
    pub unattributed_tagged_bytes: i128,
    pub ledger_blind_spot_bytes: i128,
    pub tagged_reconciliation_bytes: i128,
    pub records: [[u64; 3]; 4],
    pub record_capacity: u32,
    pub record_high_water: u32,
    pub draining_records: usize,
    /// Requested record-segment backing represented by the sampled occupied
    /// prefix; excludes inline records/control storage and is not resident size.
    pub record_segment_requested_bytes: u64,
    /// The reporting authority's observation-control storage estimate. None
    /// means no authority supplied it. S1 never adds this diagnostic to C.
    pub observation_metadata_bytes: Option<u64>,
    pub faults: FaultSnapshot,
    pub attribution_threshold_bytes: usize,
    pub token_bytes: usize,
    pub batch_threshold_bytes: u64,
    /// Q times independently sampled pins, excluding in-flight allocations.
    /// This is neither an instantaneous nor a concurrent mathematical bound.
    pub slot_balance_estimate_excluding_in_flight_bytes: u64,
    pub sampled_at_unix_millis: Option<u64>,
    pub sequence_sum: u64,
}
impl Default for AttributionSnapshot {
    fn default() -> Self {
        Self {
            process: BandSnapshot::default(),
            classified: [ClassFacts::default(); 3],
            unattributed_tagged_bytes: 0,
            ledger_blind_spot_bytes: 0,
            tagged_reconciliation_bytes: 0,
            records: [[0; 3]; 4],
            record_capacity: 0,
            record_high_water: 0,
            draining_records: 0,
            record_segment_requested_bytes: 0,
            observation_metadata_bytes: None,
            faults: FaultSnapshot::default(),
            attribution_threshold_bytes: ATTRIBUTION_THRESHOLD_BYTES,
            token_bytes: ATTRIBUTION_TOKEN_BYTES,
            batch_threshold_bytes: SLOT_QUANTUM_BYTES,
            slot_balance_estimate_excluding_in_flight_bytes: 0,
            sampled_at_unix_millis: None,
            sequence_sum: 0,
        }
    }
}
impl AttributionSnapshot {
    /// Scans at most the store's sampled high-water prefix. Every record is
    /// revalidated through snapshot_ref; raw borrowed fields are never sampled
    /// across reclamation/reuse. Class/bytes remain independent atomic samples.
    pub fn sample(store: &RecordStore, process: BandSnapshot) -> Self {
        let mut result = Self {
            process,
            record_capacity: store.capacity(),
            record_high_water: store.high_water(),
            draining_records: store.draining_len(),
            faults: store.faults.snapshot(),
            sampled_at_unix_millis: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|d| u64::try_from(d.as_millis()).ok()),
            ..Self::default()
        };
        for (reference, _) in store.records() {
            if reference.index >= result.record_high_water {
                break;
            }
            if let Some(snapshot) = store.snapshot_ref(reference) {
                result.add_record(snapshot);
            }
        }
        let tagged_sum = result
            .classified
            .iter()
            .map(|f| f.tagged_bytes)
            .sum::<i128>()
            + result.unattributed_tagged_bytes;
        let small_sum = result
            .classified
            .iter()
            .map(|f| f.r1_small_bytes)
            .sum::<i128>();
        result.tagged_reconciliation_bytes = i128::from(process.tagged.live_bytes) - tagged_sum;
        result.ledger_blind_spot_bytes = i128::from(process.small.live_bytes) - small_sum;
        result.slot_balance_estimate_excluding_in_flight_bytes = result
            .faults
            .pinned_slots
            .saturating_mul(SLOT_QUANTUM_BYTES);
        let segments = if result.record_high_water <= UNATTRIBUTED_SHARDS {
            0
        } else {
            (result.record_high_water as usize).div_ceil(SEGMENT_RECORDS)
        };
        result.record_segment_requested_bytes =
            (segments * SEGMENT_RECORDS * std::mem::size_of::<LaneRecord>()) as u64;
        result
    }
    fn add_record(&mut self, record: RecordSnapshot) {
        let class = ((record.flags & CLASS_MASK) >> CLASS_SHIFT) as usize;
        let production = (record.flags & PRODUCTION_MASK).min(2) as usize;
        self.records[class][production] += 1;
        self.sequence_sum = self.sequence_sum.wrapping_add(record.sequence);
        if class == 3 {
            self.unattributed_tagged_bytes += i128::from(record.tagged_bytes);
        } else {
            self.classified[class].tagged_bytes += i128::from(record.tagged_bytes);
            self.classified[class].r1_small_bytes += i128::from(record.r1_small_bytes);
        }
    }
    #[must_use]
    pub fn with_observation_metadata_bytes(mut self, bytes: u64) -> Self {
        self.observation_metadata_bytes = Some(bytes);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lane::{ResponsibilityClass, StoreHandle};
    use crate::sync::Ordering;
    #[test]
    fn classified_facts_blind_spot_and_reconciliation_are_sampled_separately() {
        let store = StoreHandle::owned(20);
        let query = store.acquire(1, ResponsibilityClass::Query).unwrap();
        let residual = store.acquire(2, ResponsibilityClass::Residual).unwrap();
        let service = store.acquire(3, ResponsibilityClass::Service).unwrap();
        // Fixture fields remain protected by live owners; these are readout
        // samples, not lifetime publications or simulated allocation counts.
        for (owner, tagged, small, state) in [
            (&query, 520, 20, 0),
            (&residual, 1032, 30, 1),
            (&service, 2048, 40, 2),
        ] {
            owner.record().tagged.store(tagged, Ordering::Release);
            owner.record().r1_small.store(small, Ordering::Release);
            owner.record().flags.fetch_or(state, Ordering::Release);
            owner.record().sequence.store(7, Ordering::Release);
        }
        store
            .store()
            .resolve(store.store().unattributed_ref(0))
            .unwrap()
            .tagged
            .store(100, Ordering::Release);
        let mut counted = BandSnapshot::default();
        counted.small.live_bytes = 200;
        counted.tagged.live_bytes = 3700;
        let sample = AttributionSnapshot::sample(store.store(), counted)
            .with_observation_metadata_bytes(1234);
        assert_eq!(
            sample.classified[0],
            ClassFacts {
                tagged_bytes: 520,
                r1_small_bytes: 20
            }
        );
        assert_eq!(sample.classified[1].tagged_bytes, 1032);
        assert_eq!(sample.classified[2].r1_small_bytes, 40);
        assert_eq!(sample.unattributed_tagged_bytes, 100);
        assert_eq!(sample.ledger_blind_spot_bytes, 110);
        assert_eq!(sample.tagged_reconciliation_bytes, 0);
        assert_eq!(sample.records[0], [1, 0, 0]);
        assert_eq!(sample.records[1], [0, 1, 0]);
        assert_eq!(sample.records[2], [0, 0, 1]);
        assert_eq!(sample.records[3], [16, 0, 0]);
        assert_eq!(sample.record_capacity, 20);
        assert_eq!(sample.record_high_water, 19);
        assert_eq!(sample.sequence_sum, 21);
        assert_eq!(sample.observation_metadata_bytes, Some(1234));
        assert!(sample.sampled_at_unix_millis.is_some());
        assert_eq!(
            sample.record_segment_requested_bytes,
            (SEGMENT_RECORDS * std::mem::size_of::<LaneRecord>()) as u64
        );
        // Restore diagnostic fixture fields before owner reclamation.
        for owner in [&query, &residual, &service] {
            owner.record().tagged.store(0, Ordering::Release);
            owner.record().r1_small.store(0, Ordering::Release);
        }
    }
    #[test]
    fn signed_samples_faults_and_pin_estimates_do_not_claim_settlement() {
        let store = StoreHandle::owned(17);
        let query = store.acquire(1, ResponsibilityClass::Query).unwrap();
        query.record().tagged.store(-520, Ordering::Release);
        query.record().r1_small.store(10, Ordering::Release);
        store.store().faults.binding_failed();
        store.store().faults.scope_refused();
        store.store().faults.orphan();
        store.store().faults.pin_added();
        let sample = AttributionSnapshot::sample(store.store(), BandSnapshot::default());
        assert_eq!(sample.classified[0].tagged_bytes, -520);
        assert_eq!(sample.tagged_reconciliation_bytes, 520);
        assert_eq!(sample.ledger_blind_spot_bytes, -10);
        assert_eq!(sample.faults.binding_failures, 1);
        assert_eq!(sample.faults.scope_refusals, 1);
        assert_eq!(sample.faults.orphan_events, 1);
        assert_eq!(
            sample.slot_balance_estimate_excluding_in_flight_bytes,
            SLOT_QUANTUM_BYTES
        );
        assert_eq!(sample.observation_metadata_bytes, None);
        query.record().tagged.store(0, Ordering::Release);
        query.record().r1_small.store(0, Ordering::Release);
        store.store().faults.pin_removed();
    }
    #[test]
    fn reclaimed_generation_is_not_observed_as_the_previous_record() {
        let store = StoreHandle::owned(17);
        let first = store.acquire(1, ResponsibilityClass::Query).unwrap();
        let old = first.reference();
        drop(first);
        assert_eq!(store.store().draining_len(), 1);
        assert_eq!(store.store().reclaim(1), 1);
        let second = store.acquire(2, ResponsibilityClass::Service).unwrap();
        assert_ne!(old, second.reference());
        assert!(store.store().snapshot_ref(old).is_none());
        let sample = AttributionSnapshot::sample(store.store(), BandSnapshot::default());
        assert_eq!(sample.records[0], [0; 3]);
        assert_eq!(sample.records[2], [1, 0, 0]);
        assert_eq!(sample.draining_records, 0);
    }
}
