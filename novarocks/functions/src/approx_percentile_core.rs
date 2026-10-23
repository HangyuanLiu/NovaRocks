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

use crate::approx_percentile_failure::{
    ApproxPercentileDataRecipe as DataRecipe, ApproxPercentileFailureSink,
    ApproxPercentileObservation as WorkObservation, RequiredApproxPercentileFailure,
};
use allocator_api2::alloc::{Allocator, Global};
use allocator_api2::vec::Vec as AllocVec;
use serde::{Deserialize, Serialize};

const PERCENTILE_STATE_MAGIC: u8 = 0xA2;
const PERCENTILE_STATE_VERSION: u8 = 4;
const QUANTILE_KIND_NONE: u8 = 0;
const QUANTILE_KIND_SCALAR: u8 = 1;
const QUANTILE_KIND_ARRAY: u8 = 2;
const HEADER_LEN: usize = 11;
const QUANTILE_TOLERANCE: f64 = 1e-12;

pub const MIN_COMPRESSION: f64 = 2048.0;
pub const MAX_COMPRESSION: f64 = 10000.0;
pub const DEFAULT_COMPRESSION_FACTOR: usize = 10000;
pub const MAX_QUANTILE_COUNT: usize = 4096;
const MAX_DECODED_PROCESSED: usize = 40_000;
const MAX_DECODED_UNPROCESSED: usize = 160_000;
const MAX_DECODED_CUMULATIVE: usize = MAX_DECODED_PROCESSED + 1;

#[derive(Clone, Debug, PartialEq)]
pub enum QuantileSpec<A: Allocator + Clone = Global> {
    Scalar(f64),
    Array(AllocVec<f64, A>),
}

#[derive(Clone, Debug)]
pub struct PercentileState<A: Allocator + Clone = Global> {
    allocator: A,
    pub digest: TDigest<A>,
    pub quantiles: Option<QuantileSpec<A>>,
    pub compression: usize,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Centroid {
    mean: f32,
    weight: f32,
}

// The actual original element author pins the selected sort extent recipe.
const _: () = assert!(std::mem::size_of::<Centroid>() == std::mem::size_of::<[f32; 2]>());

impl Centroid {
    fn new(mean: f32, weight: f32) -> Self {
        Self { mean, weight }
    }

    fn add(&mut self, other: &Centroid) {
        if self.weight != 0.0 {
            self.weight += other.weight;
            self.mean += other.weight * (other.mean - self.mean) / self.weight;
        } else {
            self.weight = other.weight;
            self.mean = other.mean;
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SerializableTDigest {
    compression: f32,
    min: f32,
    max: f32,
    max_processed: usize,
    max_unprocessed: usize,
    processed_weight: f32,
    unprocessed_weight: f32,
    processed: Vec<Centroid>,
    unprocessed: Vec<Centroid>,
    cumulative: Vec<f32>,
}

#[derive(Clone, Debug)]
pub struct TDigest<A: Allocator + Clone = Global> {
    allocator: A,
    compression: f32,
    min: f32,
    max: f32,
    max_processed: usize,
    max_unprocessed: usize,
    processed_weight: f32,
    unprocessed_weight: f32,
    processed: AllocVec<Centroid, A>,
    unprocessed: AllocVec<Centroid, A>,
    cumulative: AllocVec<f32, A>,
}

impl<A: Allocator + Clone> TDigest<A> {
    fn new_in(compression: f32, allocator: A) -> Self {
        Self {
            allocator: allocator.clone(),
            compression,
            min: f32::MAX,
            max: f32::MIN,
            max_processed: (2.0 * compression.ceil()) as usize,
            max_unprocessed: (8.0 * compression.ceil()) as usize,
            processed_weight: 0.0,
            unprocessed_weight: 0.0,
            processed: AllocVec::new_in(allocator.clone()),
            unprocessed: AllocVec::new_in(allocator.clone()),
            cumulative: AllocVec::new_in(allocator),
        }
    }

    fn is_empty(&self) -> bool {
        self.processed.is_empty() && self.unprocessed.is_empty()
    }

    fn total_weight(&self) -> f32 {
        self.processed_weight + self.unprocessed_weight
    }

    fn try_clone(&self) -> Result<Self, crate::KernelFailure> {
        self.try_clone_with_sink(&mut RequiredApproxPercentileFailure)
    }

    fn try_clone_with_sink<S: ApproxPercentileFailureSink>(
        &self,
        sink: &mut S,
    ) -> Result<Self, S::Error> {
        Ok(Self {
            allocator: self.allocator.clone(),
            compression: self.compression,
            min: self.min,
            max: self.max,
            max_processed: self.max_processed,
            max_unprocessed: self.max_unprocessed,
            processed_weight: self.processed_weight,
            unprocessed_weight: self.unprocessed_weight,
            processed: try_copy_slice_with_sink(
                &self.processed,
                self.allocator.clone(),
                "TDigest processed centroids",
                sink,
            )?,
            unprocessed: try_copy_slice_with_sink(
                &self.unprocessed,
                self.allocator.clone(),
                "TDigest unprocessed centroids",
                sink,
            )?,
            cumulative: try_copy_slice_with_sink(
                &self.cumulative,
                self.allocator.clone(),
                "TDigest cumulative weights",
                sink,
            )?,
        })
    }

    pub fn count(&self) -> f32 {
        self.total_weight()
    }

    fn add(&mut self, value: f32, weight: f32) -> Result<(), crate::KernelFailure> {
        self.add_with_sink(value, weight, &mut RequiredApproxPercentileFailure)
    }
    fn add_with_sink<S: ApproxPercentileFailureSink>(
        &mut self,
        value: f32,
        weight: f32,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        if value.is_nan() || weight <= 0.0 {
            return Ok(());
        }
        self.unprocessed.try_reserve(1).map_err(|source| {
            sink.allocation(
                DataRecipe::Static("ResourceExhausted: reserve TDigest centroid"),
                source,
            )
        })?;
        self.unprocessed.push(Centroid::new(value, weight));
        self.unprocessed_weight += weight;
        self.process_if_necessary_with_sink(sink)
    }

    fn merge(&mut self, other: &TDigest<A>) -> Result<(), crate::KernelFailure> {
        self.merge_with_sink(other, &mut RequiredApproxPercentileFailure)
    }
    fn merge_with_sink<S: ApproxPercentileFailureSink>(
        &mut self,
        other: &TDigest<A>,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        if other.is_empty() {
            return Ok(());
        }
        if !other.processed.is_empty() {
            self.processed_weight += other.processed_weight;
            self.processed = merge_sorted_centroids_with_sink(
                &self.processed,
                &other.processed,
                self.allocator.clone(),
                sink,
            )?;
            if let Some(first) = self.processed.first() {
                self.min = self.min.min(first.mean);
            }
            if let Some(last) = self.processed.last() {
                self.max = self.max.max(last.mean);
            }
        }
        if !other.unprocessed.is_empty() {
            self.unprocessed
                .try_reserve(other.unprocessed.len())
                .map_err(|source| {
                    sink.allocation(
                        DataRecipe::Static("ResourceExhausted: reserve merged TDigest centroids"),
                        source,
                    )
                })?;
            // The original complete reserve precedes the same ordered byte copies.
            // Split only the copies into bounded observed work quanta.
            for chunk in other.unprocessed.chunks(256) {
                self.unprocessed.extend_from_slice(chunk);
                for _ in chunk {
                    sink.observe(WorkObservation::Step)?;
                }
            }
            self.unprocessed_weight += other.unprocessed_weight;
        }
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.process_if_necessary_with_sink(sink)?;
        self.update_cumulative_with_sink(sink)
    }

    fn quantile(&mut self, q: f32) -> Result<Option<f32>, crate::KernelFailure> {
        self.quantile_with_sink(q, &mut RequiredApproxPercentileFailure)
    }

    fn quantile_with_sink<S: ApproxPercentileFailureSink>(
        &mut self,
        q: f32,
        sink: &mut S,
    ) -> Result<Option<f32>, S::Error> {
        if !(0.0..=1.0).contains(&q) {
            return Ok(None);
        }
        if self.have_unprocessed() || self.is_dirty() {
            self.process_with_sink(sink)?;
        }
        self.quantile_processed_with_sink(q, sink)
    }

    fn serialize_binary(&self) -> Result<Vec<u8>, crate::KernelFailure> {
        self.serialize_binary_observed(&mut |_| Ok::<_, crate::KernelFailure>(()))
    }
    fn encoded_digest_size(&self) -> Result<usize, crate::KernelFailure> {
        use crate::approx_percentile_failure::{storage_count, wire_count};
        wire_count(self.processed.len())?;
        wire_count(self.unprocessed.len())?;
        wire_count(self.cumulative.len())?;
        storage_count::<u8>(
            48 + 8 * (self.processed.len() as u128 + self.unprocessed.len() as u128)
                + 4 * self.cumulative.len() as u128,
        )
    }
    fn serialize_binary_observed<E: From<crate::KernelFailure>>(
        &self,
        observe: &mut dyn FnMut(PercentileEncodeObservation) -> Result<(), E>,
    ) -> Result<Vec<u8>, E> {
        let bytes = self.encoded_digest_size()?;
        let processed = crate::approx_percentile_failure::wire_count(self.processed.len())?;
        let unprocessed = crate::approx_percentile_failure::wire_count(self.unprocessed.len())?;
        let cumulative = crate::approx_percentile_failure::wire_count(self.cumulative.len())?;
        observe(PercentileEncodeObservation::OpaqueBegin)?;
        let mut out = Vec::new();
        out.try_reserve_exact(bytes).map_err(|source| {
            crate::KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
                "reserve encoded TDigest state",
                source,
            ))
        })?;
        observe(PercentileEncodeObservation::OpaqueEnd)?;
        out.extend_from_slice(&self.compression.to_le_bytes());
        out.extend_from_slice(&self.min.to_le_bytes());
        out.extend_from_slice(&self.max.to_le_bytes());
        out.extend_from_slice(&(self.max_processed as u64).to_le_bytes());
        out.extend_from_slice(&(self.max_unprocessed as u64).to_le_bytes());
        out.extend_from_slice(&self.processed_weight.to_le_bytes());
        out.extend_from_slice(&self.unprocessed_weight.to_le_bytes());
        out.extend_from_slice(&processed.to_le_bytes());
        for centroid in &self.processed {
            observe(PercentileEncodeObservation::Step)?;
            out.extend_from_slice(&centroid.mean.to_le_bytes());
            out.extend_from_slice(&centroid.weight.to_le_bytes());
        }
        out.extend_from_slice(&unprocessed.to_le_bytes());
        for centroid in &self.unprocessed {
            observe(PercentileEncodeObservation::Step)?;
            out.extend_from_slice(&centroid.mean.to_le_bytes());
            out.extend_from_slice(&centroid.weight.to_le_bytes());
        }
        out.extend_from_slice(&cumulative.to_le_bytes());
        for value in &self.cumulative {
            observe(PercentileEncodeObservation::Step)?;
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    fn deserialize_binary_in(payload: &[u8], allocator: A) -> Result<Self, crate::KernelFailure> {
        Self::deserialize_binary_with_sink(payload, allocator, &mut RequiredApproxPercentileFailure)
    }

    fn deserialize_binary_with_sink<S: ApproxPercentileFailureSink>(
        payload: &[u8],
        allocator: A,
        sink: &mut S,
    ) -> Result<Self, S::Error> {
        let mut offset = 0usize;
        let compression = read_f32_with_sink(payload, &mut offset, "tdigest compression", sink)?;
        let min = read_f32_with_sink(payload, &mut offset, "tdigest min", sink)?;
        let max = read_f32_with_sink(payload, &mut offset, "tdigest max", sink)?;
        let max_processed =
            read_u64_with_sink(payload, &mut offset, "tdigest max_processed", sink)? as usize;
        let max_unprocessed =
            read_u64_with_sink(payload, &mut offset, "tdigest max_unprocessed", sink)? as usize;
        let processed_weight =
            read_f32_with_sink(payload, &mut offset, "tdigest processed_weight", sink)?;
        let unprocessed_weight =
            read_f32_with_sink(payload, &mut offset, "tdigest unprocessed_weight", sink)?;

        if !compression.is_finite() || compression <= 0.0 || compression > MAX_COMPRESSION as f32 {
            return Err(sink.encoding(DataRecipe::Arguments(format_args!(
                "tdigest compression out of bounds: {compression}"
            ))));
        }
        if max_processed > MAX_DECODED_PROCESSED {
            return Err(sink.encoding(DataRecipe::Arguments(format_args!(
                "tdigest max_processed {max_processed} exceeds {MAX_DECODED_PROCESSED}"
            ))));
        }
        if max_unprocessed > MAX_DECODED_UNPROCESSED {
            return Err(sink.encoding(DataRecipe::Arguments(format_args!(
                "tdigest max_unprocessed {max_unprocessed} exceeds {MAX_DECODED_UNPROCESSED}"
            ))));
        }

        let processed_len =
            read_u32_with_sink(payload, &mut offset, "tdigest processed len", sink)? as usize;
        if processed_len > MAX_DECODED_PROCESSED {
            return Err(sink.encoding(DataRecipe::Arguments(format_args!(
                "tdigest processed length {processed_len} exceeds {MAX_DECODED_PROCESSED}"
            ))));
        }
        if processed_len > payload.len().saturating_sub(offset) / std::mem::size_of::<Centroid>() {
            return Err(sink.encoding(DataRecipe::Static("tdigest processed payload truncated")));
        }
        let mut processed = AllocVec::new_in(allocator.clone());
        processed
            .try_reserve_exact(processed_len)
            .map_err(|source| {
                sink.allocation(
                    DataRecipe::Static(
                        "ResourceExhausted: reserve decoded TDigest processed centroids",
                    ),
                    source,
                )
            })?;
        for _ in 0..processed_len {
            sink.observe(WorkObservation::Step)?;
            processed.push(Centroid::new(
                read_f32_with_sink(payload, &mut offset, "tdigest processed mean", sink)?,
                read_f32_with_sink(payload, &mut offset, "tdigest processed weight", sink)?,
            ));
        }

        let unprocessed_len =
            read_u32_with_sink(payload, &mut offset, "tdigest unprocessed len", sink)? as usize;
        if unprocessed_len > MAX_DECODED_UNPROCESSED {
            return Err(sink.encoding(DataRecipe::Arguments(format_args!(
                "tdigest unprocessed length {unprocessed_len} exceeds {MAX_DECODED_UNPROCESSED}"
            ))));
        }
        if unprocessed_len > payload.len().saturating_sub(offset) / std::mem::size_of::<Centroid>()
        {
            return Err(sink.encoding(DataRecipe::Static("tdigest unprocessed payload truncated")));
        }
        let mut unprocessed = AllocVec::new_in(allocator.clone());
        unprocessed
            .try_reserve_exact(unprocessed_len)
            .map_err(|source| {
                sink.allocation(
                    DataRecipe::Static(
                        "ResourceExhausted: reserve decoded TDigest unprocessed centroids",
                    ),
                    source,
                )
            })?;
        for _ in 0..unprocessed_len {
            sink.observe(WorkObservation::Step)?;
            unprocessed.push(Centroid::new(
                read_f32_with_sink(payload, &mut offset, "tdigest unprocessed mean", sink)?,
                read_f32_with_sink(payload, &mut offset, "tdigest unprocessed weight", sink)?,
            ));
        }

        let cumulative_len =
            read_u32_with_sink(payload, &mut offset, "tdigest cumulative len", sink)? as usize;
        if cumulative_len > MAX_DECODED_CUMULATIVE {
            return Err(sink.encoding(DataRecipe::Arguments(format_args!(
                "tdigest cumulative length {cumulative_len} exceeds {MAX_DECODED_CUMULATIVE}"
            ))));
        }
        if cumulative_len > payload.len().saturating_sub(offset) / std::mem::size_of::<f32>() {
            return Err(sink.encoding(DataRecipe::Static("tdigest cumulative payload truncated")));
        }
        let mut cumulative = AllocVec::new_in(allocator.clone());
        cumulative
            .try_reserve_exact(cumulative_len)
            .map_err(|source| {
                sink.allocation(
                    DataRecipe::Static(
                        "ResourceExhausted: reserve decoded TDigest cumulative weights",
                    ),
                    source,
                )
            })?;
        for _ in 0..cumulative_len {
            sink.observe(WorkObservation::Step)?;
            cumulative.push(read_f32_with_sink(
                payload,
                &mut offset,
                "tdigest cumulative value",
                sink,
            )?);
        }
        if offset != payload.len() {
            return Err(sink.encoding(DataRecipe::Arguments(format_args!(
                "tdigest payload has trailing bytes: consumed={} total={}",
                offset,
                payload.len()
            ))));
        }
        let mut digest = Self {
            allocator,
            compression,
            min,
            max,
            max_processed,
            max_unprocessed,
            processed_weight,
            unprocessed_weight,
            processed,
            unprocessed,
            cumulative,
        };
        // Serialized cumulative centers and running totals are derived caches.
        // Rebuild them from the retained centroids once on state admission,
        // including clean states produced before mass normalization.
        if !digest.processed.is_empty() {
            digest.update_cumulative_with_sink(sink)?;
        }
        Ok(digest)
    }

    fn have_unprocessed(&self) -> bool {
        !self.unprocessed.is_empty()
    }

    fn is_dirty(&self) -> bool {
        self.processed.len() > self.max_processed || self.unprocessed.len() > self.max_unprocessed
    }

    fn process_if_necessary(&mut self) -> Result<(), crate::KernelFailure> {
        self.process_if_necessary_with_sink(&mut RequiredApproxPercentileFailure)
    }
    fn process_if_necessary_with_sink<S: ApproxPercentileFailureSink>(
        &mut self,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        if self.is_dirty() {
            self.process_with_sink(sink)?;
        }
        Ok(())
    }

    fn process(&mut self) -> Result<(), crate::KernelFailure> {
        self.process_with_sink(&mut RequiredApproxPercentileFailure)
    }

    fn process_with_sink<S: ApproxPercentileFailureSink>(
        &mut self,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        if self.unprocessed.is_empty() && self.processed.is_empty() {
            return Ok(());
        }

        sink.observe(WorkObservation::SortBegin(self.unprocessed.len()))?;
        self.unprocessed.sort_by(|left, right| {
            left.mean
                .partial_cmp(&right.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sink.observe(WorkObservation::SortEnd)?;
        let processed = std::mem::replace(
            &mut self.processed,
            AllocVec::new_in(self.allocator.clone()),
        );
        if !processed.is_empty() {
            let merged_capacity = self
                .unprocessed
                .len()
                .checked_add(processed.len())
                .ok_or_else(|| {
                    sink.data(DataRecipe::Static(
                        "TDigest merged centroid length overflow",
                    ))
                })?;
            let mut merged = AllocVec::new_in(self.allocator.clone());
            merged
                .try_reserve_exact(merged_capacity)
                .map_err(|source| {
                    sink.allocation(
                        DataRecipe::Static("ResourceExhausted: reserve sorted TDigest merge"),
                        source,
                    )
                })?;
            let mut left_idx = 0usize;
            let mut right_idx = 0usize;
            while left_idx < self.unprocessed.len() && right_idx < processed.len() {
                sink.observe(WorkObservation::Step)?;
                if self.unprocessed[left_idx].mean <= processed[right_idx].mean {
                    merged.push(self.unprocessed[left_idx]);
                    left_idx += 1;
                } else {
                    merged.push(processed[right_idx]);
                    right_idx += 1;
                }
            }
            for value in &self.unprocessed[left_idx..] {
                sink.observe(WorkObservation::Step)?;
                merged.push(*value);
            }
            for value in &processed[right_idx..] {
                sink.observe(WorkObservation::Step)?;
                merged.push(*value);
            }
            self.unprocessed = merged;
        }

        // Rebuild the represented mass after collecting both centroid lists.
        // Adding rounded per-partial totals makes the quantile index depend on
        // the input partitioning and disagree with the cumulative centers.
        self.processed_weight = self
            .unprocessed
            .iter()
            .try_fold(-0.0_f64, |sum, centroid| {
                sink.observe(WorkObservation::Step)?;
                Ok::<_, S::Error>(sum + f64::from(centroid.weight))
            })? as f32;
        self.unprocessed_weight = 0.0;

        let Some(first) = self.unprocessed.first().copied() else {
            return Ok(());
        };
        let mut processed = AllocVec::new_in(self.allocator.clone());
        processed
            .try_reserve_exact(self.max_processed.max(1))
            .map_err(|source| {
                sink.allocation(
                    DataRecipe::Static("ResourceExhausted: reserve processed TDigest centroids"),
                    source,
                )
            })?;
        self.processed = processed;
        self.processed.push(first);
        let mut w_so_far = f64::from(first.weight);
        let mut w_limit = f64::from(self.processed_weight) * f64::from(self.integrated_q(1.0));

        for centroid in self.unprocessed.iter().skip(1).copied() {
            sink.observe(WorkObservation::Step)?;
            let projected = w_so_far + f64::from(centroid.weight);
            if projected <= w_limit {
                w_so_far = projected;
                self.processed
                    .last_mut()
                    .expect("processed has first centroid")
                    .add(&centroid);
            } else {
                let k1 =
                    self.integrated_location((w_so_far / f64::from(self.processed_weight)) as f32);
                w_limit = f64::from(self.processed_weight) * f64::from(self.integrated_q(k1 + 1.0));
                w_so_far += f64::from(centroid.weight);
                self.processed.try_reserve(1).map_err(|source| {
                    sink.allocation(
                        DataRecipe::Static("ResourceExhausted: grow processed TDigest centroids"),
                        source,
                    )
                })?;
                self.processed.push(centroid);
            }
        }

        self.unprocessed.clear();
        self.min = self
            .min
            .min(self.processed.first().map(|c| c.mean).unwrap_or(self.min));
        self.max = self
            .max
            .max(self.processed.last().map(|c| c.mean).unwrap_or(self.max));
        self.update_cumulative_with_sink(sink)
    }

    fn quantile_processed(&self, q: f32) -> Option<f32> {
        self.quantile_processed_with_sink(q, &mut RequiredApproxPercentileFailure)
            .expect("no fallible observation in original quantile search")
    }

    fn quantile_processed_with_sink<S: ApproxPercentileFailureSink>(
        &self,
        q: f32,
        sink: &mut S,
    ) -> Result<Option<f32>, S::Error> {
        if self.processed.is_empty() {
            return Ok(None);
        }
        if self.processed.len() == 1 {
            return Ok(Some(self.processed[0].mean));
        }

        let n = self.processed.len();
        let index = q * self.processed_weight;

        if index <= self.weight(0) / 2.0 {
            return Ok(Some(
                (f64::from(self.min)
                    + 2.0 * f64::from(index) / f64::from(self.weight(0))
                        * f64::from(self.mean(0) - self.min)) as f32,
            ));
        }

        let mut first = None;
        for (i, value) in self.cumulative.iter().enumerate() {
            sink.observe(WorkObservation::Step)?;
            if *value >= index {
                first = Some(i);
                break;
            }
        }
        if let Some(i) = first
            && i > 0
            && i < self.cumulative.len() - 1
        {
            let z1 = index - self.cumulative[i - 1];
            let z2 = self.cumulative[i] - index;
            return Ok(Some(Self::weighted_average(
                self.mean(i - 1),
                z2,
                self.mean(i),
                z1,
            )));
        }

        let z1 = f64::from(index - self.processed_weight) - f64::from(self.weight(n - 1)) / 2.0;
        let z2 = f64::from(self.weight(n - 1)) / 2.0 - z1;
        Ok(Some(Self::weighted_average(
            self.mean(n - 1),
            z1 as f32,
            self.max,
            z2 as f32,
        )))
    }

    fn update_cumulative(&mut self) -> Result<(), crate::KernelFailure> {
        self.update_cumulative_with_sink(&mut RequiredApproxPercentileFailure)
    }

    fn update_cumulative_with_sink<S: ApproxPercentileFailureSink>(
        &mut self,
        sink: &mut S,
    ) -> Result<(), S::Error> {
        self.cumulative.clear();
        self.cumulative
            .try_reserve(self.processed.len() + 1)
            .map_err(|source| {
                sink.allocation(
                    DataRecipe::Static("ResourceExhausted: reserve TDigest cumulative weights"),
                    source,
                )
            })?;
        // The reference digest accumulates in double precision, then stores
        // each CDF center as a float. Repeated float accumulation loses small
        // weights after a large prefix and changes quantile interpolation.
        let mut previous = 0.0_f64;
        for centroid in &self.processed {
            sink.observe(WorkObservation::Step)?;
            let weight = f64::from(centroid.weight);
            self.cumulative.push((previous + weight / 2.0) as f32);
            previous += weight;
        }
        let represented_mass = previous as f32;
        self.cumulative.push(represented_mass);
        self.processed_weight = represented_mass;
        Ok(())
    }

    fn mean(&self, idx: usize) -> f32 {
        self.processed[idx].mean
    }

    fn weight(&self, idx: usize) -> f32 {
        self.processed[idx].weight
    }

    fn integrated_location(&self, q: f32) -> f32 {
        debug_assert!(q.is_finite() && (0.0..=1.0).contains(&q));
        (f64::from(self.compression)
            * ((2.0 * f64::from(q) - 1.0).asin() + std::f64::consts::FRAC_PI_2)
            / std::f64::consts::PI) as f32
    }

    fn integrated_q(&self, k: f32) -> f32 {
        ((((f64::from(k.min(self.compression)) * std::f64::consts::PI
            / f64::from(self.compression))
            - std::f64::consts::FRAC_PI_2)
            .sin()
            + 1.0)
            / 2.0) as f32
    }

    fn weighted_average(x1: f32, w1: f32, x2: f32, w2: f32) -> f32 {
        if x1 <= x2 {
            Self::weighted_average_sorted(x1, w1, x2, w2)
        } else {
            Self::weighted_average_sorted(x2, w2, x1, w1)
        }
    }

    fn weighted_average_sorted(x1: f32, w1: f32, x2: f32, w2: f32) -> f32 {
        let x = (x1 * w1 + x2 * w2) / (w1 + w2);
        x.max(x1).min(x2)
    }

    fn retained_bytes(&self) -> usize {
        self.processed
            .capacity()
            .saturating_mul(std::mem::size_of::<Centroid>())
            .saturating_add(
                self.unprocessed
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Centroid>()),
            )
            .saturating_add(
                self.cumulative
                    .capacity()
                    .saturating_mul(std::mem::size_of::<f32>()),
            )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PercentileStateMeta {
    quantiles: Option<SerializableQuantileSpec>,
    compression: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum SerializableQuantileSpec {
    Scalar(f64),
    Array(Vec<f64>),
}

impl Default for PercentileState<Global> {
    fn default() -> Self {
        Self::new_in(DEFAULT_COMPRESSION_FACTOR, Global)
    }
}

impl<A: Allocator + Clone> PercentileState<A> {
    pub fn new_in(compression: usize, allocator: A) -> Self {
        Self {
            allocator: allocator.clone(),
            digest: TDigest::new_in(compression as f32, allocator),
            quantiles: None,
            compression,
        }
    }

    pub fn allocator(&self) -> A {
        self.allocator.clone()
    }

    /// Heap bytes owned by containers in this state. The state body itself is
    /// charged by its aggregate arena (or by its ordinary owner on scalar paths).
    pub fn retained_bytes(&self) -> usize {
        let quantile_bytes = match &self.quantiles {
            Some(QuantileSpec::Array(values)) => {
                values.capacity().saturating_mul(std::mem::size_of::<f64>())
            }
            _ => 0,
        };
        std::mem::size_of::<Self>()
            .saturating_add(self.digest.retained_bytes())
            .saturating_add(quantile_bytes)
    }
}

pub fn normalize_compression(compression: Option<f64>) -> Result<usize, crate::KernelFailure> {
    normalize_compression_with_sink(compression, &mut RequiredApproxPercentileFailure)
}
pub fn normalize_compression_with_sink<S: ApproxPercentileFailureSink>(
    compression: Option<f64>,
    sink: &mut S,
) -> Result<usize, S::Error> {
    let Some(value) = compression else {
        return Ok(DEFAULT_COMPRESSION_FACTOR);
    };
    if !value.is_finite() {
        return Err(sink.data(DataRecipe::Static("percentile compression must be finite")));
    }
    if value <= 0.0 {
        return Err(sink.data(DataRecipe::Arguments(format_args!(
            "compression parameter must be positive in percentile_approx_weighted, but got: {}",
            value
        ))));
    }
    if !(MIN_COMPRESSION..=MAX_COMPRESSION).contains(&value) {
        return Ok(DEFAULT_COMPRESSION_FACTOR);
    }
    Ok(value.round() as usize)
}

pub fn add_value<A: Allocator + Clone>(
    state: &mut PercentileState<A>,
    value: f64,
) -> Result<(), crate::KernelFailure> {
    add_value_with_sink(state, value, &mut RequiredApproxPercentileFailure)
}
pub fn add_value_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    state: &mut PercentileState<A>,
    value: f64,
    sink: &mut S,
) -> Result<(), S::Error> {
    state.digest.add_with_sink(value as f32, 1.0, sink)?;
    validate_state_with_sink(state, sink)
}

pub fn add_weighted_value<A: Allocator + Clone>(
    state: &mut PercentileState<A>,
    value: f64,
    weight: i64,
) -> Result<(), crate::KernelFailure> {
    add_weighted_value_with_sink(state, value, weight, &mut RequiredApproxPercentileFailure)
}
pub fn add_weighted_value_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    state: &mut PercentileState<A>,
    value: f64,
    weight: i64,
    sink: &mut S,
) -> Result<(), S::Error> {
    if weight < 0 {
        return Err(sink.data(DataRecipe::Arguments(format_args!(
            "percentile weight must be non-negative, got {}",
            weight
        ))));
    }
    if weight == 0 {
        return Ok(());
    }
    state
        .digest
        .add_with_sink(value as f32, weight as f32, sink)?;
    validate_state_with_sink(state, sink)
}

pub fn set_quantile<A: Allocator + Clone>(
    state: &mut PercentileState<A>,
    quantile: f64,
) -> Result<(), crate::KernelFailure> {
    set_quantile_with_sink(state, quantile, &mut RequiredApproxPercentileFailure)
}
pub fn set_quantile_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    state: &mut PercentileState<A>,
    quantile: f64,
    sink: &mut S,
) -> Result<(), S::Error> {
    validate_quantile_with_sink(quantile, sink)?;
    match &state.quantiles {
        Some(QuantileSpec::Scalar(existing)) => {
            if (existing - quantile).abs() > QUANTILE_TOLERANCE {
                return Err(sink.data(DataRecipe::Arguments(format_args!(
                    "percentile quantile mismatch while merging states: existing={} incoming={}",
                    existing, quantile
                ))));
            }
        }
        Some(QuantileSpec::Array(existing)) => {
            if existing.len() != 1 || (existing[0] - quantile).abs() > QUANTILE_TOLERANCE {
                return Err(sink.data(DataRecipe::Static(
                    "percentile quantile mismatch while merging states: scalar/array mismatch",
                )));
            }
        }
        None => state.quantiles = Some(QuantileSpec::Scalar(quantile)),
    }
    Ok(())
}

pub fn set_quantiles<A: Allocator + Clone>(
    state: &mut PercentileState<A>,
    quantiles: &[f64],
) -> Result<(), crate::KernelFailure> {
    set_quantiles_with_sink(state, quantiles, &mut RequiredApproxPercentileFailure)
}
pub fn set_quantiles_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    state: &mut PercentileState<A>,
    quantiles: &[f64],
    sink: &mut S,
) -> Result<(), S::Error> {
    if quantiles.is_empty() {
        return Err(sink.data(DataRecipe::Static("percentile array cannot be empty")));
    }
    if quantiles.len() > MAX_QUANTILE_COUNT {
        return Err(sink.data(DataRecipe::Arguments(format_args!(
            "percentile quantile count {} exceeds {}",
            quantiles.len(),
            MAX_QUANTILE_COUNT
        ))));
    }
    for &q in quantiles {
        sink.observe(WorkObservation::Step)?;
        validate_quantile_with_sink(q, sink)?;
    }
    match &state.quantiles {
        Some(QuantileSpec::Scalar(existing)) => {
            if quantiles.len() != 1 || (existing - quantiles[0]).abs() > QUANTILE_TOLERANCE {
                return Err(sink.data(DataRecipe::Static(
                    "percentile quantile mismatch while merging states: scalar/array mismatch",
                )));
            }
        }
        Some(QuantileSpec::Array(existing)) => {
            let mut same = existing.len() == quantiles.len();
            if same {
                for (l, r) in existing.iter().zip(quantiles) {
                    sink.observe(WorkObservation::Step)?;
                    if !((*l - *r).abs() <= QUANTILE_TOLERANCE) {
                        same = false;
                        break;
                    }
                }
            }
            if !same {
                return Err(sink.data(DataRecipe::Static(
                    "percentile quantile array mismatch while merging states",
                )));
            }
        }
        None => {
            state.quantiles = Some(QuantileSpec::Array(try_copy_slice_with_sink(
                quantiles,
                state.allocator.clone(),
                "percentile quantile array",
                sink,
            )?))
        }
    }
    Ok(())
}

pub fn set_compression<A: Allocator + Clone>(
    state: &mut PercentileState<A>,
    compression: f64,
) -> Result<(), crate::KernelFailure> {
    set_compression_with_sink(state, compression, &mut RequiredApproxPercentileFailure)
}
pub fn set_compression_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    state: &mut PercentileState<A>,
    compression: f64,
    sink: &mut S,
) -> Result<(), S::Error> {
    let normalized = normalize_compression_with_sink(Some(compression), sink)?;
    state.compression = normalized;
    if state.digest.is_empty() {
        state.digest = TDigest::new_in(normalized as f32, state.allocator.clone());
    }
    Ok(())
}

pub fn merge_state<A: Allocator + Clone>(
    target: &mut PercentileState<A>,
    incoming: &PercentileState<A>,
) -> Result<(), crate::KernelFailure> {
    merge_state_with_sink(target, incoming, &mut RequiredApproxPercentileFailure)
}
pub fn merge_state_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    target: &mut PercentileState<A>,
    incoming: &PercentileState<A>,
    sink: &mut S,
) -> Result<(), S::Error> {
    if let Some(q) = &incoming.quantiles {
        match q {
            QuantileSpec::Scalar(q) => set_quantile_with_sink(target, *q, sink)?,
            QuantileSpec::Array(qs) => set_quantiles_with_sink(target, qs, sink)?,
        }
    }
    if target.digest.is_empty() {
        target.compression = incoming.compression;
        target.digest = incoming.digest.try_clone_with_sink(sink)?;
        return validate_state_with_sink(target, sink);
    }
    target.compression = target.compression.max(incoming.compression);
    target.digest.merge_with_sink(&incoming.digest, sink)?;
    validate_state_with_sink(target, sink)
}

pub fn merge_serialized_state_into(
    target: &mut PercentileState,
    payload: &[u8],
) -> Result<(), crate::KernelFailure> {
    let decoded = decode_state(payload)?;
    merge_state(target, &decoded)
}

/// Aggregate execution accepts only the current bounded binary format. Legacy
/// JSON v3 decoding remains available to scalar compatibility readers, but it
/// has no allocation bound and therefore cannot enter a hard-limited aggregate
/// state.
pub fn merge_bounded_serialized_state_into(
    target: &mut PercentileState<impl Allocator + Clone>,
    payload: &[u8],
) -> Result<(), crate::KernelFailure> {
    merge_bounded_serialized_state_into_with_sink(
        target,
        payload,
        &mut RequiredApproxPercentileFailure,
    )
}
pub fn merge_bounded_serialized_state_into_with_sink<
    A: Allocator + Clone,
    S: ApproxPercentileFailureSink,
>(
    target: &mut PercentileState<A>,
    payload: &[u8],
    sink: &mut S,
) -> Result<(), S::Error> {
    if payload.get(1).copied() != Some(PERCENTILE_STATE_VERSION) {
        return Err(sink.data(DataRecipe::Arguments(format_args!(
            "bounded percentile aggregate requires state version {PERCENTILE_STATE_VERSION}"
        ))));
    }
    let decoded = decode_state_v4_with_sink(payload, target.allocator(), sink)?;
    merge_state_with_sink(target, &decoded, sink)?;
    validate_state_with_sink(target, sink)
}

pub fn encode_empty_state() -> Result<Vec<u8>, crate::KernelFailure> {
    encode_state(&PercentileState::default())
}

/// The original singleton calculation with an explicit allocation policy.
/// Required and selected callers keep the original allocation refusal.
pub fn single_value_state_in<A: Allocator + Clone>(
    value: f64,
    allocator: A,
) -> Result<PercentileState<A>, crate::KernelFailure> {
    let mut state = PercentileState::new_in(DEFAULT_COMPRESSION_FACTOR, allocator);
    add_value(&mut state, value)?;
    Ok(state)
}
pub fn encode_single_value(value: f64) -> Result<Vec<u8>, crate::KernelFailure> {
    let state = single_value_state_in(value, Global)?;
    encode_state(&state)
}
/// Both actual exact-reserve byte buffers coexist until encoding returns.
/// This is an opaque request extent, not an allocation or a memory grant.
pub fn scalar_hash_codec_extent<A: Allocator + Clone>(state: &PercentileState<A>) -> Option<usize> {
    if state.quantiles.is_some() || state.compression != DEFAULT_COMPRESSION_FACTOR {
        return None;
    }
    if state.digest.is_empty() {
        return Some(HEADER_LEN);
    }
    if !state.digest.processed.is_empty()
        || state.digest.unprocessed.len() != 1
        || !state.digest.cumulative.is_empty()
    {
        return None;
    }
    let digest = state.digest.encoded_digest_size().ok()?;
    digest.checked_add(HEADER_LEN)?.checked_add(digest)
}
/// Exact byte geometry of this whole state, authored before codec allocation.
pub fn encoded_size<A: Allocator + Clone>(
    state: &PercentileState<A>,
) -> Result<usize, crate::KernelFailure> {
    use crate::approx_percentile_failure::{storage_count, wire_count};
    wire_count(state.compression)?;
    let count = match &state.quantiles {
        Some(QuantileSpec::Scalar(_)) => 1,
        Some(QuantileSpec::Array(values)) => values.len(),
        None => 0,
    };
    wire_count(count)?;
    let digest = if state.digest.is_empty() {
        0
    } else {
        state.digest.encoded_digest_size()?
    };
    storage_count::<u8>(HEADER_LEN as u128 + 8 * count as u128 + digest as u128)
}

/// Work observation at the original serializer, never memory permission.
#[derive(Clone, Copy, Debug)]
pub enum PercentileEncodeObservation {
    Step,
    OpaqueBegin,
    OpaqueEnd,
}
pub fn encode_state<A: Allocator + Clone>(
    state: &PercentileState<A>,
) -> Result<Vec<u8>, crate::KernelFailure> {
    encode_state_observed(state, &mut |_| Ok::<_, crate::KernelFailure>(()))
}
pub fn encode_state_observed<A: Allocator + Clone, E: From<crate::KernelFailure>>(
    state: &PercentileState<A>,
    observe: &mut dyn FnMut(PercentileEncodeObservation) -> Result<(), E>,
) -> Result<Vec<u8>, E> {
    let bytes = encoded_size(state)?;
    let compression = crate::approx_percentile_failure::wire_count(state.compression)?;
    let (quantile_kind, quantiles) = match &state.quantiles {
        Some(QuantileSpec::Scalar(q)) => (QUANTILE_KIND_SCALAR, std::slice::from_ref(q)),
        Some(QuantileSpec::Array(values)) => (QUANTILE_KIND_ARRAY, values.as_slice()),
        None => (QUANTILE_KIND_NONE, &[][..]),
    };
    let digest_payload = if state.digest.is_empty() {
        Vec::new()
    } else {
        state.digest.serialize_binary_observed(observe)?
    };
    observe(PercentileEncodeObservation::OpaqueBegin)?;
    let mut out = Vec::new();
    out.try_reserve_exact(bytes).map_err(|source| {
        crate::KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
            "reserve encoded percentile state",
            source,
        ))
    })?;
    observe(PercentileEncodeObservation::OpaqueEnd)?;
    out.push(PERCENTILE_STATE_MAGIC);
    out.push(PERCENTILE_STATE_VERSION);
    out.push(quantile_kind);
    out.extend_from_slice(&compression.to_le_bytes());
    out.extend_from_slice(
        &crate::approx_percentile_failure::wire_count(quantiles.len())?.to_le_bytes(),
    );
    for quantile in quantiles {
        observe(PercentileEncodeObservation::Step)?;
        out.extend_from_slice(&quantile.to_le_bytes());
    }
    if !digest_payload.is_empty() {
        observe(PercentileEncodeObservation::OpaqueBegin)?;
    }
    out.extend_from_slice(&digest_payload);
    if !digest_payload.is_empty() {
        observe(PercentileEncodeObservation::OpaqueEnd)?;
    }
    Ok(out)
}

pub fn decode_state(payload: &[u8]) -> Result<PercentileState, crate::KernelFailure> {
    decode_state_with_sink(payload, Global, &mut RequiredApproxPercentileFailure)
}

pub fn decode_state_with_sink<A: PercentileDecodeStorage, S: ApproxPercentileFailureSink>(
    payload: &[u8],
    allocator: A,
    sink: &mut S,
) -> Result<PercentileState<A>, S::Error> {
    if payload.is_empty() {
        return Ok(PercentileState::new_in(
            DEFAULT_COMPRESSION_FACTOR,
            allocator,
        ));
    }
    if payload.len() < HEADER_LEN {
        return Err(sink.encoding(DataRecipe::Arguments(format_args!(
            "percentile state payload too short: expected>={} actual={}",
            HEADER_LEN,
            payload.len()
        ))));
    }
    if payload[0] != PERCENTILE_STATE_MAGIC {
        return Err(sink.encoding(DataRecipe::Arguments(format_args!(
            "unsupported percentile state payload magic: expected=0x{:02x} actual=0x{:02x}",
            PERCENTILE_STATE_MAGIC, payload[0]
        ))));
    }
    match payload[1] {
        PERCENTILE_STATE_VERSION => decode_state_v4_with_sink(payload, allocator, sink),
        3 => decode_state_v3_with_sink(payload, allocator, sink),
        other => Err(sink.encoding(DataRecipe::Arguments(format_args!(
            "unsupported percentile state payload version: expected={} actual={}",
            PERCENTILE_STATE_VERSION, other
        )))),
    }
}

/// The clone frontier has one original allocation body per existing policy.
/// Quantile processing/interpolation below is shared unchanged. The original
/// public/v1 wrappers retain their derived, infallible clone semantics.
pub trait TDigestClonePolicy<A: Allocator + Clone> {
    fn clone_digest(&mut self, digest: &TDigest<A>) -> Result<TDigest<A>, crate::KernelFailure>;
}
pub struct OriginalDerivedClone;
impl<A: Allocator + Clone> TDigestClonePolicy<A> for OriginalDerivedClone {
    fn clone_digest(&mut self, digest: &TDigest<A>) -> Result<TDigest<A>, crate::KernelFailure> {
        Ok(digest.clone())
    }
}
pub struct FallibleClone;
impl<A: Allocator + Clone> TDigestClonePolicy<A> for FallibleClone {
    fn clone_digest(&mut self, digest: &TDigest<A>) -> Result<TDigest<A>, crate::KernelFailure> {
        digest.try_clone()
    }
}
pub fn quantile_value<A: Allocator + Clone>(
    state: &PercentileState<A>,
    quantile: f64,
) -> Result<Option<f64>, crate::KernelFailure> {
    quantile_value_with_policy(state, quantile, &mut OriginalDerivedClone)
}
pub fn quantile_value_with_policy<A: Allocator + Clone, P: TDigestClonePolicy<A>>(
    state: &PercentileState<A>,
    quantile: f64,
    policy: &mut P,
) -> Result<Option<f64>, crate::KernelFailure> {
    quantile_value_with_sink(
        state,
        quantile,
        &mut OriginalCloneLoan(policy),
        &mut RequiredApproxPercentileFailure,
    )
}
pub trait ObservedTDigestClone<A: Allocator + Clone, S: ApproxPercentileFailureSink> {
    fn clone_digest(&mut self, digest: &TDigest<A>, sink: &mut S) -> Result<TDigest<A>, S::Error>;
}
pub(crate) struct OriginalCloneLoan<'a, P>(pub(crate) &'a mut P);
impl<A: Allocator + Clone, S: ApproxPercentileFailureSink, P: TDigestClonePolicy<A>>
    ObservedTDigestClone<A, S> for OriginalCloneLoan<'_, P>
{
    fn clone_digest(&mut self, digest: &TDigest<A>, sink: &mut S) -> Result<TDigest<A>, S::Error> {
        self.0
            .clone_digest(digest)
            .map_err(|source| sink.kernel(source))
    }
}
pub struct TrackedTDigestClone;
impl<A: Allocator + Clone, S: ApproxPercentileFailureSink> ObservedTDigestClone<A, S>
    for TrackedTDigestClone
{
    fn clone_digest(&mut self, digest: &TDigest<A>, sink: &mut S) -> Result<TDigest<A>, S::Error> {
        digest.try_clone_with_sink(sink)
    }
}
pub fn quantile_value_with_sink<
    A: Allocator + Clone,
    S: ApproxPercentileFailureSink,
    P: ObservedTDigestClone<A, S>,
>(
    state: &PercentileState<A>,
    quantile: f64,
    policy: &mut P,
    sink: &mut S,
) -> Result<Option<f64>, S::Error> {
    let mut digest = policy.clone_digest(&state.digest, sink)?;
    Ok(digest
        .quantile_with_sink(quantile as f32, sink)?
        .map(|value| value as f64))
}

pub fn quantile_from_state<A: Allocator + Clone>(
    state: &PercentileState<A>,
    quantile: Option<f64>,
) -> Result<Option<f64>, crate::KernelFailure> {
    quantile_from_state_with_policy(state, quantile, &mut OriginalDerivedClone)
}
pub fn quantile_from_state_with_policy<A: Allocator + Clone, P: TDigestClonePolicy<A>>(
    state: &PercentileState<A>,
    quantile: Option<f64>,
    policy: &mut P,
) -> Result<Option<f64>, crate::KernelFailure> {
    quantile_from_state_with_sink(
        state,
        quantile,
        &mut OriginalCloneLoan(policy),
        &mut RequiredApproxPercentileFailure,
    )
}

pub fn quantile_from_state_with_sink<
    A: Allocator + Clone,
    S: ApproxPercentileFailureSink,
    P: ObservedTDigestClone<A, S>,
>(
    state: &PercentileState<A>,
    quantile: Option<f64>,
    policy: &mut P,
    sink: &mut S,
) -> Result<Option<f64>, S::Error> {
    let q = match quantile {
        Some(q) => q,
        None => match &state.quantiles {
            Some(QuantileSpec::Scalar(q)) => *q,
            Some(QuantileSpec::Array(values)) if values.len() == 1 => values[0],
            _ => return Ok(None),
        },
    };
    quantile_value_with_sink(state, q, policy, sink)
}

pub fn quantiles_from_state<A: Allocator + Clone>(
    state: &PercentileState<A>,
) -> Result<Option<Vec<f64>>, crate::KernelFailure> {
    quantiles_from_state_with_policy(state, &mut OriginalDerivedClone)
}
pub fn quantiles_from_state_with_policy<A: Allocator + Clone, P: TDigestClonePolicy<A>>(
    state: &PercentileState<A>,
    policy: &mut P,
) -> Result<Option<Vec<f64>>, crate::KernelFailure> {
    quantiles_from_state_with_sink(
        state,
        &mut OriginalCloneLoan(policy),
        &mut RequiredApproxPercentileFailure,
    )
}
pub fn quantiles_from_state_with_sink<
    A: Allocator + Clone,
    S: ApproxPercentileFailureSink,
    P: ObservedTDigestClone<A, S>,
>(
    state: &PercentileState<A>,
    policy: &mut P,
    sink: &mut S,
) -> Result<Option<Vec<f64>>, S::Error> {
    let quantiles: &[f64] = match &state.quantiles {
        Some(QuantileSpec::Scalar(q)) => std::slice::from_ref(q),
        Some(QuantileSpec::Array(values)) => values,
        None => return Ok(None),
    };
    let mut output = Vec::new();
    output
        .try_reserve_exact(quantiles.len())
        .map_err(|source| {
            sink.allocation(
                DataRecipe::Static("reserve percentile quantile results"),
                source,
            )
        })?;
    if state.digest.is_empty() {
        output.resize(quantiles.len(), f64::NAN);
        return Ok(Some(output));
    }
    let mut digest = policy.clone_digest(&state.digest, sink)?;
    for q in quantiles {
        sink.observe(WorkObservation::Step)?;
        output.push(
            digest
                .quantile_with_sink(*q as f32, sink)?
                .unwrap_or(f32::NAN) as f64,
        );
    }
    Ok(Some(output))
}

pub fn validate_state<A: Allocator + Clone>(
    state: &PercentileState<A>,
) -> Result<(), crate::KernelFailure> {
    validate_state_with_sink(state, &mut RequiredApproxPercentileFailure)
}

pub fn validate_state_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    state: &PercentileState<A>,
    sink: &mut S,
) -> Result<(), S::Error> {
    let quantile_count = match &state.quantiles {
        Some(QuantileSpec::Array(values)) => values.len(),
        Some(QuantileSpec::Scalar(_)) => 1,
        None => 0,
    };
    if quantile_count > MAX_QUANTILE_COUNT {
        return Err(sink.data(DataRecipe::Arguments(format_args!(
            "percentile quantile count {quantile_count} exceeds {MAX_QUANTILE_COUNT}"
        ))));
    }
    Ok(())
}

fn validate_quantile(quantile: f64) -> Result<(), crate::KernelFailure> {
    validate_quantile_with_sink(quantile, &mut RequiredApproxPercentileFailure)
}
fn validate_quantile_with_sink<S: ApproxPercentileFailureSink>(
    quantile: f64,
    sink: &mut S,
) -> Result<(), S::Error> {
    if !quantile.is_finite() {
        return Err(sink.data(DataRecipe::Static("percentile quantile must be finite")));
    }
    if !(0.0..=1.0).contains(&quantile) {
        return Err(sink.data(DataRecipe::Arguments(format_args!(
            "percentile quantile must be between 0 and 1, got {}",
            quantile
        ))));
    }
    Ok(())
}

fn merge_sorted_centroids<A: Allocator + Clone>(
    left: &[Centroid],
    right: &[Centroid],
    allocator: A,
) -> Result<AllocVec<Centroid, A>, crate::KernelFailure> {
    merge_sorted_centroids_with_sink(left, right, allocator, &mut RequiredApproxPercentileFailure)
}
fn merge_sorted_centroids_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    left: &[Centroid],
    right: &[Centroid],
    allocator: A,
    sink: &mut S,
) -> Result<AllocVec<Centroid, A>, S::Error> {
    if left.is_empty() {
        return try_copy_slice_with_sink(right, allocator, "TDigest right centroids", sink);
    }
    if right.is_empty() {
        return try_copy_slice_with_sink(left, allocator, "TDigest left centroids", sink);
    }
    let capacity = left
        .len()
        .checked_add(right.len())
        .ok_or_else(|| sink.data(DataRecipe::Static("TDigest merge length overflow")))?;
    let mut merged = AllocVec::new_in(allocator);
    merged.try_reserve_exact(capacity).map_err(|source| {
        sink.allocation(
            DataRecipe::Static("ResourceExhausted: reserve TDigest merge"),
            source,
        )
    })?;
    let (mut li, mut ri) = (0usize, 0usize);
    while li < left.len() && ri < right.len() {
        sink.observe(WorkObservation::Step)?;
        if left[li].mean <= right[ri].mean {
            merged.push(left[li]);
            li += 1;
        } else {
            merged.push(right[ri]);
            ri += 1;
        }
    }
    for value in &left[li..] {
        sink.observe(WorkObservation::Step)?;
        merged.push(*value);
    }
    for value in &right[ri..] {
        sink.observe(WorkObservation::Step)?;
        merged.push(*value);
    }
    Ok(merged)
}

fn decode_state_v4(payload: &[u8]) -> Result<PercentileState, crate::KernelFailure> {
    decode_state_v4_in(payload, Global)
}

fn decode_state_v4_in<A: Allocator + Clone>(
    payload: &[u8],
    allocator: A,
) -> Result<PercentileState<A>, crate::KernelFailure> {
    decode_state_v4_with_sink(payload, allocator, &mut RequiredApproxPercentileFailure)
}

fn decode_state_v4_with_sink<A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    payload: &[u8],
    allocator: A,
    sink: &mut S,
) -> Result<PercentileState<A>, S::Error> {
    if payload.len() < HEADER_LEN {
        return Err(sink.encoding(DataRecipe::Static("percentile state payload too short")));
    }
    let quantile_kind = payload[2];
    let compression = u32::from_le_bytes(payload[3..7].try_into().map_err(|_| {
        sink.encoding(DataRecipe::Static(
            "percentile state compression decode failed",
        ))
    })?) as usize;
    let quantile_count = u32::from_le_bytes(payload[7..11].try_into().map_err(|_| {
        sink.encoding(DataRecipe::Static(
            "percentile state quantile count decode failed",
        ))
    })?) as usize;
    if quantile_count > MAX_QUANTILE_COUNT {
        return Err(sink.encoding(DataRecipe::Arguments(format_args!(
            "percentile state quantile count {quantile_count} exceeds {MAX_QUANTILE_COUNT}"
        ))));
    }
    if compression == 0 || compression > MAX_COMPRESSION as usize {
        return Err(sink.encoding(DataRecipe::Arguments(format_args!(
            "percentile state compression {compression} exceeds bounded range"
        ))));
    }
    let quantile_bytes = quantile_count
        .checked_mul(std::mem::size_of::<f64>())
        .ok_or_else(|| {
            sink.encoding(DataRecipe::Static(
                "percentile state quantile bytes overflow",
            ))
        })?;
    let quantile_end = HEADER_LEN.checked_add(quantile_bytes).ok_or_else(|| {
        sink.encoding(DataRecipe::Static("percentile state quantile end overflow"))
    })?;
    if payload.len() < quantile_end {
        return Err(sink.encoding(DataRecipe::Static(
            "percentile state quantile payload truncated",
        )));
    }

    let mut quantiles = AllocVec::new_in(allocator.clone());
    quantiles
        .try_reserve_exact(quantile_count)
        .map_err(|source| {
            sink.allocation(
                DataRecipe::Static("ResourceExhausted: reserve decoded percentile quantiles"),
                source,
            )
        })?;
    let mut offset = HEADER_LEN;
    for _ in 0..quantile_count {
        sink.observe(WorkObservation::Step)?;
        quantiles.push(read_f64_with_sink(
            payload,
            &mut offset,
            "percentile state quantile",
            sink,
        )?);
    }

    let quantiles = match (quantile_kind, quantiles.len()) {
        (QUANTILE_KIND_NONE, 0) => None,
        (QUANTILE_KIND_SCALAR, 1) => Some(QuantileSpec::Scalar(quantiles[0])),
        (QUANTILE_KIND_ARRAY, _) => Some(QuantileSpec::Array(quantiles)),
        _ => {
            return Err(sink.encoding(DataRecipe::Arguments(format_args!(
                "invalid percentile state quantile metadata: kind={} count={}",
                quantile_kind, quantile_count
            ))));
        }
    };

    let digest = if payload.len() == quantile_end {
        TDigest::new_in(compression as f32, allocator.clone())
    } else {
        TDigest::deserialize_binary_with_sink(&payload[quantile_end..], allocator.clone(), sink)?
    };
    let state = PercentileState {
        allocator,
        digest,
        quantiles,
        compression,
    };
    validate_state_with_sink(&state, sink)?;
    Ok(state)
}

fn try_copy_slice_in<T: Copy, A: Allocator + Clone>(
    values: &[T],
    allocator: A,
    label: &str,
) -> Result<AllocVec<T, A>, crate::KernelFailure> {
    try_copy_slice_with_sink(
        values,
        allocator,
        label,
        &mut RequiredApproxPercentileFailure,
    )
}
fn try_copy_slice_with_sink<T: Copy, A: Allocator + Clone, S: ApproxPercentileFailureSink>(
    values: &[T],
    allocator: A,
    label: &str,
    sink: &mut S,
) -> Result<AllocVec<T, A>, S::Error> {
    let mut copied = AllocVec::new_in(allocator);
    copied.try_reserve_exact(values.len()).map_err(|source| {
        sink.allocation(
            DataRecipe::Arguments(format_args!("ResourceExhausted: reserve {label}")),
            source,
        )
    })?;
    for value in values {
        sink.observe(WorkObservation::Step)?;
        copied.push(*value);
    }
    Ok(copied)
}

/// Copies only already-decoded vectors. Global preserves original collect;
/// execution storage may use the real fallible allocator at this same frontier.
pub trait PercentileDecodeStorage: Allocator + Clone {
    fn collect_decoded<T: Copy, S: ApproxPercentileFailureSink>(
        &self,
        values: Vec<T>,
        sink: &mut S,
    ) -> Result<AllocVec<T, Self>, S::Error>
    where
        Self: Sized;
}
impl PercentileDecodeStorage for Global {
    fn collect_decoded<T: Copy, S: ApproxPercentileFailureSink>(
        &self,
        values: Vec<T>,
        _sink: &mut S,
    ) -> Result<AllocVec<T, Self>, S::Error> {
        Ok(values.into_iter().collect())
    }
}

fn decode_state_v3(payload: &[u8]) -> Result<PercentileState, crate::KernelFailure> {
    decode_state_v3_with_sink(payload, Global, &mut RequiredApproxPercentileFailure)
}

fn decode_state_v3_with_sink<A: PercentileDecodeStorage, S: ApproxPercentileFailureSink>(
    payload: &[u8],
    allocator: A,
    sink: &mut S,
) -> Result<PercentileState<A>, S::Error> {
    let meta_len = u32::from_le_bytes(payload[2..6].try_into().map_err(|_| {
        sink.encoding(DataRecipe::Static(
            "percentile state meta length decode failed",
        ))
    })?) as usize;
    if payload.len() < 6 + meta_len {
        return Err(sink.encoding(DataRecipe::Static(
            "percentile state meta payload truncated",
        )));
    }
    sink.observe(WorkObservation::JsonBegin(meta_len))?;
    let meta: PercentileStateMeta =
        serde_json::from_slice(&payload[6..6 + meta_len]).map_err(|source| sink.json(source))?;
    sink.observe(WorkObservation::JsonEnd)?;
    let mut digest = if payload.len() == 6 + meta_len {
        TDigest::new_in(meta.compression as f32, allocator.clone())
    } else {
        sink.observe(WorkObservation::JsonBegin(payload.len() - 6 - meta_len))?;
        let decoded: SerializableTDigest =
            serde_json::from_slice(&payload[6 + meta_len..]).map_err(|source| sink.json(source))?;
        sink.observe(WorkObservation::JsonEnd)?;
        TDigest {
            allocator: allocator.clone(),
            compression: decoded.compression,
            min: decoded.min,
            max: decoded.max,
            max_processed: decoded.max_processed,
            max_unprocessed: decoded.max_unprocessed,
            processed_weight: decoded.processed_weight,
            unprocessed_weight: decoded.unprocessed_weight,
            processed: allocator.collect_decoded(decoded.processed, sink)?,
            unprocessed: allocator.collect_decoded(decoded.unprocessed, sink)?,
            cumulative: allocator.collect_decoded(decoded.cumulative, sink)?,
        }
    };
    if !digest.processed.is_empty() {
        digest.update_cumulative_with_sink(sink)?;
    }
    Ok(PercentileState {
        allocator: allocator.clone(),
        digest,
        quantiles: match meta.quantiles {
            None => None,
            Some(quantiles) => Some(match quantiles {
                SerializableQuantileSpec::Scalar(value) => QuantileSpec::Scalar(value),
                SerializableQuantileSpec::Array(values) => {
                    QuantileSpec::Array(allocator.collect_decoded(values, sink)?)
                }
            }),
        },
        compression: meta.compression,
    })
}

fn read_u32(payload: &[u8], offset: &mut usize, label: &str) -> Result<u32, crate::KernelFailure> {
    read_u32_with_sink(payload, offset, label, &mut RequiredApproxPercentileFailure)
}

fn read_u32_with_sink<S: ApproxPercentileFailureSink>(
    payload: &[u8],
    offset: &mut usize,
    label: &str,
    sink: &mut S,
) -> Result<u32, S::Error> {
    let end = offset
        .checked_add(std::mem::size_of::<u32>())
        .ok_or_else(|| {
            sink.encoding(DataRecipe::Arguments(format_args!(
                "{label} offset overflow"
            )))
        })?;
    let bytes: [u8; 4] = payload
        .get(*offset..end)
        .ok_or_else(|| sink.encoding(DataRecipe::Arguments(format_args!("{label} truncated"))))?
        .try_into()
        .map_err(|_| sink.encoding(DataRecipe::Arguments(format_args!("{label} decode failed"))))?;
    *offset = end;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(payload: &[u8], offset: &mut usize, label: &str) -> Result<u64, crate::KernelFailure> {
    read_u64_with_sink(payload, offset, label, &mut RequiredApproxPercentileFailure)
}

fn read_u64_with_sink<S: ApproxPercentileFailureSink>(
    payload: &[u8],
    offset: &mut usize,
    label: &str,
    sink: &mut S,
) -> Result<u64, S::Error> {
    let end = offset
        .checked_add(std::mem::size_of::<u64>())
        .ok_or_else(|| {
            sink.encoding(DataRecipe::Arguments(format_args!(
                "{label} offset overflow"
            )))
        })?;
    let bytes: [u8; 8] = payload
        .get(*offset..end)
        .ok_or_else(|| sink.encoding(DataRecipe::Arguments(format_args!("{label} truncated"))))?
        .try_into()
        .map_err(|_| sink.encoding(DataRecipe::Arguments(format_args!("{label} decode failed"))))?;
    *offset = end;
    Ok(u64::from_le_bytes(bytes))
}

fn read_f32(payload: &[u8], offset: &mut usize, label: &str) -> Result<f32, crate::KernelFailure> {
    read_f32_with_sink(payload, offset, label, &mut RequiredApproxPercentileFailure)
}

fn read_f32_with_sink<S: ApproxPercentileFailureSink>(
    payload: &[u8],
    offset: &mut usize,
    label: &str,
    sink: &mut S,
) -> Result<f32, S::Error> {
    let bits = read_u32_with_sink(payload, offset, label, sink)?;
    Ok(f32::from_le_bytes(bits.to_le_bytes()))
}

fn read_f64(payload: &[u8], offset: &mut usize, label: &str) -> Result<f64, crate::KernelFailure> {
    read_f64_with_sink(payload, offset, label, &mut RequiredApproxPercentileFailure)
}

fn read_f64_with_sink<S: ApproxPercentileFailureSink>(
    payload: &[u8],
    offset: &mut usize,
    label: &str,
    sink: &mut S,
) -> Result<f64, S::Error> {
    let bits = read_u64_with_sink(payload, offset, label, sink)?;
    Ok(f64::from_le_bytes(bits.to_le_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cumulative_centers_retain_unit_weights_after_large_prefix() {
        let mut digest = TDigest::new_in(10000.0, Global);
        for (mean, weight) in [(0.0, 16777216.0), (1.0, 1.0), (2.0, 1.0), (3.0, 1.0)] {
            digest.processed.push(Centroid::new(mean, weight));
        }
        digest.update_cumulative().unwrap();
        // Integer sums and halves are exact in f64; each center rounds only
        // once at the frozen f32 CDF boundary (nearest, ties to even).
        assert_eq!(
            digest.cumulative.as_slice(),
            &[8388608.0, 16777216.0, 16777218.0, 16777218.0, 16777220.0]
        );
        // The interpolation denominator and the stored CDF end are one mass.
        assert_eq!(digest.processed_weight, 16777220.0);
        assert_eq!(digest.count(), 16777220.0);
    }

    #[test]
    fn scale_functions_match_fixed_double_intermediate_reference() {
        let digest = TDigest::new_in(10000.0, Global);
        assert_eq!(digest.integrated_location(0.01_f32).to_bits(), 0x441f6be1);
        assert_eq!(digest.integrated_location(0.25_f32).to_bits(), 0x45505555);
        assert_eq!(digest.integrated_location(0.5_f32).to_bits(), 0x459c4000);
        assert_eq!(digest.integrated_location(0.9_f32).to_bits(), 0x45f87d60);
        assert_eq!(digest.integrated_q(1.0_f32).to_bits(), 0x32d3f2b9);
        assert_eq!(digest.integrated_q(2048.0_f32).to_bits(), 0x3dccbc9c);
        assert_eq!(digest.integrated_q(5000.0_f32).to_bits(), 0x3f000000);
        assert_eq!(digest.integrated_q(9999.0_f32).to_bits(), 0x3f800000);
    }

    #[test]
    fn fixed_large_weighted_series_matches_independent_cdf_reference() {
        let mut state = PercentileState::default();
        let mut exact_mass = 0_u64;
        let mut exact_rows = 0_u64;
        for value in (1_i64..=50000).step_by(3).chain([1, 2, 3, 4]) {
            add_weighted_value(&mut state, value as f64, value).unwrap();
            exact_mass += value as u64;
            exact_rows += 1;
        }
        assert_eq!(exact_rows, 16671);
        assert_eq!(exact_mass, 416675010);
        // Round the exact mass once: 416675010 -> 416675008, index208337504.
        // Centers208305776/208341136 at means35353/35356 independently give
        // the f32 weighted interpolation35355.6875.
        assert_eq!(state.digest.quantile(0.5).unwrap(), Some(35_355.688));
        assert_eq!(state.digest.total_weight(), 416675008.0);
        assert_eq!(state.digest.cumulative.last(), Some(&416675008.0));
        assert_eq!(quantile_value(&state, 0.5).unwrap(), Some(35355.6875));
    }

    #[test]
    fn weighted_fixed_reference_survives_merge_orders_zero_and_state_roundtrip() {
        // CDF centers for (2,w1),(3,w2),(4,w3) are .5,2,4.5.
        // q=.5 has index3: interpolation 3+(3-2)/(4.5-2)=3.4.
        // q=.25 has index1.5: 2+(1.5-.5)/(2-.5)=8/3.
        for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2]] {
            let inputs = [(2.0, 1), (3.0, 2), (4.0, 3)];
            let mut merged = PercentileState::default();
            for index in order {
                let mut part = PercentileState::default();
                let (value, weight) = inputs[index];
                add_weighted_value(&mut part, value, weight).unwrap();
                // Force a real processed digest before the serialized merge.
                assert_eq!(quantile_value(&part, 0.5).unwrap(), Some(value));
                assert_eq!(part.digest.quantile(0.5).unwrap(), Some(value as f32));
                merge_bounded_serialized_state_into(&mut merged, &encode_state(&part).unwrap())
                    .unwrap();
            }
            add_weighted_value(&mut merged, 1000000.0, 0).unwrap();
            let decoded = decode_state(&encode_state(&merged).unwrap()).unwrap();
            for state in [&merged, &decoded] {
                assert_eq!(quantile_value(state, 0.0).unwrap(), Some(2.0));
                assert_eq!(
                    quantile_value(state, 0.25).unwrap(),
                    Some(f64::from(8.0_f32 / 3.0))
                );
                assert_eq!(
                    quantile_value(state, 0.5).unwrap(),
                    Some(f64::from(3.4_f32))
                );
                assert_eq!(quantile_value(state, 1.0).unwrap(), Some(4.0));
                assert_eq!(state.digest.total_weight(), 6.0);
            }
        }
        let mut empty = PercentileState::default();
        add_weighted_value(&mut empty, 10.0, 0).unwrap();
        assert_eq!(quantile_value(&empty, 0.5).unwrap(), None);
        assert!(add_weighted_value(&mut empty, 1.0, -1).is_err());
        assert_eq!(quantile_value(&empty, 0.5).unwrap(), None);
    }

    fn historical_clean_state_with_stale_mass() -> PercentileState {
        let mut state = PercentileState::default();
        set_quantile(&mut state, 1.0).unwrap();
        for (mean, weight) in [(0.0, 16777216.0), (1.0, 1.0), (2.0, 1.0), (3.0, 1.0)] {
            state.digest.processed.push(Centroid::new(mean, weight));
        }
        state.digest.min = 0.0;
        state.digest.max = 3.0;
        state.digest.update_cumulative().unwrap();
        // A historical running f32 sum loses all three unit weights. Its CDF
        // can still contain the once-rounded represented mass 16777220.
        state.digest.processed_weight = 16777216.0;
        assert_eq!(quantile_value(&state, 1.0).unwrap(), Some(1.0));
        state
    }

    #[test]
    fn clean_v4_state_rebuilds_mass_on_decode_and_first_bounded_merge() {
        let source = historical_clean_state_with_stale_mass();
        let payload = encode_state(&source).unwrap();
        let decoded = decode_state(&payload).unwrap();
        assert!(decoded.digest.unprocessed.is_empty());
        assert!(!decoded.digest.is_dirty());
        assert_eq!(decoded.digest.processed_weight, 16777220.0);
        assert_eq!(decoded.digest.cumulative.last(), Some(&16777220.0));
        assert_eq!(quantile_from_state(&decoded, None).unwrap(), Some(3.0));
        let mut target = PercentileState::default();
        merge_bounded_serialized_state_into(&mut target, &payload).unwrap();
        assert_eq!(quantile_from_state(&target, None).unwrap(), Some(3.0));
        assert_eq!(target.digest.processed_weight, 16777220.0);
    }

    #[test]
    fn clean_legacy_v3_scalar_state_rebuilds_mass_without_entering_bounded_merge() {
        let source = historical_clean_state_with_stale_mass();
        let metadata = serde_json::to_vec(&PercentileStateMeta {
            compression: source.compression,
            quantiles: Some(SerializableQuantileSpec::Scalar(1.0)),
        })
        .unwrap();
        let digest = &source.digest;
        let legacy = SerializableTDigest {
            compression: digest.compression,
            min: digest.min,
            max: digest.max,
            max_processed: digest.max_processed,
            max_unprocessed: digest.max_unprocessed,
            processed_weight: digest.processed_weight,
            unprocessed_weight: digest.unprocessed_weight,
            processed: digest.processed.to_vec(),
            unprocessed: digest.unprocessed.to_vec(),
            cumulative: digest.cumulative.to_vec(),
        };
        let mut payload = vec![PERCENTILE_STATE_MAGIC, 3];
        payload.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        payload.extend_from_slice(&metadata);
        payload.extend_from_slice(&serde_json::to_vec(&legacy).unwrap());
        let decoded = decode_state(&payload).unwrap();
        assert_eq!(decoded.digest.processed_weight, 16777220.0);
        assert_eq!(quantile_from_state(&decoded, None).unwrap(), Some(3.0));
        let mut target = PercentileState::default();
        assert!(merge_bounded_serialized_state_into(&mut target, &payload).is_err());
        assert_eq!(quantile_from_state(&target, Some(1.0)).unwrap(), None);
    }

    #[test]
    fn compression_prefix_stays_in_the_scale_domain_after_large_weight() {
        let mut state = PercentileState::default();
        add_weighted_value(&mut state, 0.0, 16777216).unwrap();
        for value in 1..=1000 {
            add_weighted_value(&mut state, value as f64, 3).unwrap();
        }
        // Repeated f32 prefix additions round each small weight up to four,
        // eventually exceeding the once-rounded represented total. Every
        // integrated_location call must instead receive a finite q in [0,1].
        // Its debug assertion checks this invariant during actual processing.
        let value = state.digest.quantile(0.9999).unwrap().unwrap();
        assert!(value.is_finite() && (0.0..=1000.0).contains(&value));
        assert!(state.digest.processed.iter().all(|centroid| {
            centroid.mean.is_finite() && centroid.weight.is_finite() && centroid.weight > 0.0
        }));
        assert!(
            state
                .digest
                .cumulative
                .iter()
                .all(|center| center.is_finite())
        );
        assert_eq!(
            state.digest.cumulative.last().copied(),
            Some(state.digest.processed_weight)
        );
        let represented = state
            .digest
            .processed
            .iter()
            .map(|centroid| f64::from(centroid.weight))
            .sum::<f64>() as f32;
        assert_eq!(state.digest.processed_weight, represented);
    }

    #[test]
    fn tdigest_round_trip_preserves_weight() {
        let mut state = PercentileState::default();
        add_value(&mut state, 1.0).unwrap();
        add_value(&mut state, 2.0).unwrap();
        add_value(&mut state, 3.0).unwrap();
        let encoded = encode_state(&state).unwrap();
        let decoded = decode_state(&encoded).expect("decode");
        assert_eq!(decoded.digest.total_weight() as i64, 3);
    }

    #[test]
    fn merge_serialized_state_into_empty_target_preserves_compression() {
        let mut source = PercentileState::default();
        set_quantiles(&mut source, &[0.5, 0.9]).expect("set quantiles");
        set_compression(&mut source, 5000.0).expect("set compression");
        for value in 1..=50_000 {
            add_value(&mut source, value as f64).unwrap();
        }

        let payload = encode_state(&source).unwrap();
        let expected = quantiles_from_state(&decode_state(&payload).expect("decode source"))
            .expect("source quantiles");

        let mut merged = PercentileState::default();
        merge_serialized_state_into(&mut merged, &payload).expect("merge payload");
        let actual = quantiles_from_state(&merged).expect("merged quantiles");

        assert_eq!(merged.compression, 5000);
        assert_eq!(actual, expected);
    }

    #[test]
    fn retained_bytes_tracks_quantiles_digest_growth_and_merge() {
        let mut source = PercentileState::default();
        let empty = source.retained_bytes();
        set_quantiles(&mut source, &Vec::with_capacity(8)).expect_err("empty quantiles");
        assert_eq!(source.retained_bytes(), empty);

        let mut quantiles = Vec::with_capacity(8);
        quantiles.extend([0.1, 0.5, 0.9]);
        set_quantiles(&mut source, &quantiles).expect("set quantiles");
        let with_quantiles = source.retained_bytes();
        let retained_quantile_bytes = match &source.quantiles {
            Some(QuantileSpec::Array(values)) => values.capacity() * std::mem::size_of::<f64>(),
            _ => panic!("array quantiles must remain an array"),
        };
        assert_eq!(with_quantiles, empty + retained_quantile_bytes);

        for value in 0..1024 {
            add_value(&mut source, value as f64).unwrap();
        }
        let with_digest = source.retained_bytes();
        assert!(with_digest > with_quantiles);

        let mut merged = PercentileState::default();
        merge_state(&mut merged, &source).expect("merge state");
        let expected_quantiles = match &merged.quantiles {
            Some(QuantileSpec::Array(values)) => values.capacity() * std::mem::size_of::<f64>(),
            _ => 0,
        };
        assert_eq!(
            merged.retained_bytes(),
            std::mem::size_of::<PercentileState>()
                + merged.digest.retained_bytes()
                + expected_quantiles
        );
        assert!(merged.retained_bytes() > empty);
    }

    #[test]
    fn failed_weighted_update_does_not_change_retained_bytes() {
        let mut state = PercentileState::default();
        let before = state.retained_bytes();
        assert!(add_weighted_value(&mut state, 1.0, -1).is_err());
        assert_eq!(state.retained_bytes(), before);
    }
    #[derive(Clone, Debug, Default)]
    struct DecodeProbe(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    // SAFETY: the probe delegates each exact block/layout unchanged to Global.
    unsafe impl Allocator for DecodeProbe {
        fn allocate(
            &self,
            layout: std::alloc::Layout,
        ) -> Result<std::ptr::NonNull<[u8]>, allocator_api2::alloc::AllocError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Global.allocate(layout)
        }
        unsafe fn deallocate(&self, p: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
            unsafe { Global.deallocate(p, layout) };
        }
    }
    #[test]
    fn percentile_codec_checks_each_declared_component_before_allocator_entry() {
        use novarocks_failure_contract::FailureProjection;
        let digest = TDigest::new_in(10_000.0, Global);
        let empty = digest.serialize_binary().unwrap();
        assert_eq!(empty.len(), 48);
        for (offset, count) in [
            (36, MAX_DECODED_PROCESSED),
            (40, MAX_DECODED_UNPROCESSED),
            (44, MAX_DECODED_CUMULATIVE),
        ] {
            let mut payload = empty[..offset + 4].to_vec();
            payload[offset..offset + 4].copy_from_slice(&(count as u32).to_le_bytes());
            let allocator = DecodeProbe::default();
            let cause = TDigest::deserialize_binary_in(&payload, allocator.clone()).unwrap_err();
            assert_eq!(
                cause.failure_fact(),
                crate::DataFailureReason::InvalidEncoding.failure_fact()
            );
            assert_eq!(allocator.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }
    #[test]
    fn percentile_codec_format_boundaries_keep_exact_extent_and_original_sources() {
        use crate::approx_percentile_failure::{storage_count, wire_count};
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(wire_count(u32::MAX as usize).unwrap(), u32::MAX);
        let required = u32::MAX as usize + 1;
        let cause = wire_count(required).unwrap_err();
        assert_eq!(cause.failure_fact().class(), FailureClass::Representation);
        let facts = cause.failure_fact().representation().unwrap();
        assert_eq!(facts.required, Some(required as u64));
        assert_eq!(facts.extent_limit, Some(u32::MAX as u64));
        let mut source: &(dyn std::error::Error + 'static) = &cause;
        while !source.is::<std::num::TryFromIntError>() {
            source = source
                .source()
                .expect("actual wire count conversion source");
        }
        let cause = storage_count::<f64>(usize::MAX as u128).unwrap_err();
        assert_eq!(
            cause.failure_fact().representation().unwrap().required,
            None
        );
        let mut source: &(dyn std::error::Error + 'static) = &cause;
        while !source.is::<std::alloc::LayoutError>() {
            source = source.source().expect("actual storage layout source");
        }
    }
    #[test]
    fn percentile_codec_actual_exact_reserves_and_singleton_envelope_match_wire() {
        let state = single_value_state_in(3.0, Global).unwrap();
        let digest = state.digest.serialize_binary().unwrap();
        let encoded = encode_state(&state).unwrap();
        assert_eq!(digest.len(), 56);
        assert_eq!(digest.capacity(), 56);
        assert_eq!(encoded.len(), 67);
        assert_eq!(encoded.capacity(), 67);
        assert_eq!(encoded_size(&state).unwrap(), encoded.len());
        assert_eq!(
            scalar_hash_codec_extent(&state).unwrap(),
            digest.capacity() + encoded.capacity()
        );
        assert_eq!(&encoded[11..], digest);
    }
}
