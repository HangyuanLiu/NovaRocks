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

//! ONE original scalar HLL state computation over explicit evaluated addresses.
use crate::aggregate_scalar::{
    AggScalarValue, ScalarReadFailure, ScalarReadFailureSink, ScalarReadObservation,
    scalar_from_array_with_failure,
};
use crate::datasketches_hll::{HllHandle, HllTargetType};
use crate::datasketches_hll_failure::{
    HllDataRecipe, HllFailureSink, HllInvariantRecipe, HllObservation,
};
use crate::sketch_hash::{SketchHashFailure, SketchHashFailureSink, prehash_array_value_with_failure};
use arrow_array::{ArrayRef, builder::BinaryBuilder};
use std::{fmt, sync::Arc};
type Error<F> = <F as HllFailureSink>::Error;
/// The recipe borrows the original owned scalar while its backing is alive.
pub enum TuningFailure<'a> {
    LogInteger(i64),
    LogRange(u8),
    LogFloat(f64),
    LogType(&'a AggScalarValue),
    TargetType(&'a AggScalarValue),
}
impl fmt::Display for TuningFailure<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogInteger(v) => {
                write!(f, "ds_hll_count_distinct_state log_k out of range: {}", v)
            }
            Self::LogRange(v) => write!(
                f,
                "ds_hll_count_distinct_state log_k must be in [4, 21], got {}",
                v
            ),
            Self::LogFloat(v) => write!(
                f,
                "ds_hll_count_distinct_state log_k must be in [4, 21], got {}",
                v
            ),
            Self::LogType(v) => write!(
                f,
                "ds_hll_count_distinct_state log_k expects numeric input, got {:?}",
                v
            ),
            Self::TargetType(v) => write!(
                f,
                "ds_hll_count_distinct_state target type expects string input, got {:?}",
                v
            ),
        }
    }
}
pub trait ScalarStateSink:
    Sized
    + HllFailureSink
    + SketchHashFailureSink<Error = Error<Self>>
    + ScalarReadFailureSink<Error = Error<Self>>
{
    fn tuning(&mut self, recipe: TuningFailure<'_>) -> Error<Self>;
}
/// Only an actual host creates a reservation; the original legacy adapter has
/// no new authority and keeps its original unreserved allocations.
pub trait ScalarSketchPort<F: ScalarStateSink> {
    type Reservation;
    fn reserve(&self, bytes: usize, sink: &mut F) -> Result<Self::Reservation, Error<F>>;
    fn reconcile(
        &mut self,
        bytes: usize,
        reservation: &mut Self::Reservation,
        sink: &mut F,
    ) -> Result<(), Error<F>>;
}
/// Field order destroys original payload and handle before releasing authority.
pub struct ProducedState<R, P> {
    bytes: Vec<u8>,
    handle: HllHandle,
    reservation: R,
    port: P,
}
impl<R, P> ProducedState<R, P> {
    pub fn payload(&self) -> &[u8] {
        &self.bytes
    }
}
fn log_k<F: ScalarStateSink>(array: &ArrayRef, row: usize, sink: &mut F) -> Result<u8, Error<F>> {
    match scalar_from_array_with_failure(array, row, sink)? {
        Some(AggScalarValue::Int64(v)) => {
            let lg = u8::try_from(v).map_err(|_| sink.tuning(TuningFailure::LogInteger(v)))?;
            if !(4..=21).contains(&lg) {
                return Err(sink.tuning(TuningFailure::LogRange(lg)));
            }
            Ok(lg)
        }
        Some(AggScalarValue::Float64(v)) => {
            let lg = v as u8;
            if !(4..=21).contains(&lg) {
                return Err(sink.tuning(TuningFailure::LogFloat(v)));
            }
            Ok(lg)
        }
        Some(other) => Err(sink.tuning(TuningFailure::LogType(&other))),
        None => Ok(17),
    }
}
fn target<F: ScalarStateSink>(
    array: &ArrayRef,
    row: usize,
    sink: &mut F,
) -> Result<HllTargetType, Error<F>> {
    match scalar_from_array_with_failure(array, row, sink)? {
        Some(AggScalarValue::Utf8(v)) => {
            // Observe the original ASCII operation without replacing its author.
            for _ in v.bytes() {
                HllFailureSink::observe(sink, HllObservation::Step)?;
            }
            HllFailureSink::observe(sink, HllObservation::OpaqueBoundary)?;
            sink.reserve_scalar_copy(v.len(), 1, 1)?;
            let upper = v.to_ascii_uppercase();
            HllFailureSink::observe(sink, HllObservation::OpaqueBoundary)?;
            Ok(match upper.as_str() {
                "HLL_4" => HllTargetType::Hll4,
                "HLL_8" => HllTargetType::Hll8,
                _ => HllTargetType::Hll6,
            })
        }
        Some(other) => Err(sink.tuning(TuningFailure::TargetType(&other))),
        None => Ok(HllTargetType::Hll6),
    }
}
pub fn produce_row<F: ScalarStateSink, P: ScalarSketchPort<F>>(
    values: (&ArrayRef, usize),
    log: Option<(&ArrayRef, usize)>,
    target_input: Option<(&ArrayRef, usize)>,
    mut port: P,
    sink: &mut F,
) -> Result<Option<ProducedState<P::Reservation, P>>, Error<F>> {
    let Some(hash) = prehash_array_value_with_failure(values.0, values.1, sink)? else {
        return Ok(None);
    };
    let lg = match log {
        Some((a, r)) => log_k(a, r, sink)?,
        None => 17,
    };
    let target = match target_input {
        Some((a, r)) => target(a, r, sink)?,
        None => HllTargetType::Hll6,
    };
    let preflight = HllHandle::new_allocation_preflight_with_failure(lg, target, sink)?;
    let mut reservation = port.reserve(preflight.bounds().operation_peak_bytes, sink)?;
    let (mut handle, outcome) =
        HllHandle::new_under_reservation_with_failure(&preflight, &reservation, sink)?;
    port.reconcile(
        outcome
            .current_bytes
            .saturating_sub(std::mem::size_of::<HllHandle>()),
        &mut reservation,
        sink,
    )?;
    HllFailureSink::observe(sink, HllObservation::OpaqueBoundary)?;
    drop(reservation);
    let preflight = handle.update_hash_allocation_preflight_with_failure(sink)?;
    let mut reservation = port.reserve(preflight.bounds().additional_headroom_bytes(), sink)?;
    let outcome =
        handle.update_hash_under_reservation_with_failure(hash, &preflight, &reservation, sink)?;
    port.reconcile(
        outcome
            .current_bytes
            .saturating_sub(std::mem::size_of::<HllHandle>()),
        &mut reservation,
        sink,
    )?;
    HllFailureSink::observe(sink, HllObservation::OpaqueBoundary)?;
    drop(reservation);
    let preflight = handle.serialization_allocation_preflight_observed(&mut || {
        HllFailureSink::observe(sink, HllObservation::Step)
    })?;
    let reservation = port.reserve(preflight.bounds().additional_headroom_bytes, sink)?;
    let bytes = handle.serialize_under_reservation_with_failure(&preflight, &reservation, sink)?;
    Ok(Some(ProducedState {
        bytes,
        handle,
        reservation,
        port,
    }))
}
struct LegacySink;
impl HllFailureSink for LegacySink {
    type Error = String;
    fn data(&mut self, r: HllDataRecipe<'_>) -> String {
        r.to_string()
    }
    fn invariant(&mut self, r: HllInvariantRecipe) -> String {
        r.to_string()
    }
    fn observe(&mut self, _: HllObservation) -> Result<(), String> {
        Ok(())
    }
}
impl SketchHashFailureSink for LegacySink {
    type Error = String;
    fn hash_data(&mut self, r: SketchHashFailure<'_>) -> String {
        r.message("ds_hll_count_distinct_state").to_string()
    }
    fn observe_hash(&mut self, _: HllObservation) -> Result<(), String> {
        Ok(())
    }
}
impl ScalarReadFailureSink for LegacySink {
    type Error = String;
    fn read_data(&mut self, r: ScalarReadFailure<'_>) -> String {
        r.to_string()
    }
    fn observe(&mut self, _: ScalarReadObservation) -> Result<(), String> {
        Ok(())
    }
    fn reserve_scalar_copy(&mut self, _: usize, _: usize, _: usize) -> Result<(), String> {
        Ok(())
    }
}
impl ScalarStateSink for LegacySink {
    fn tuning(&mut self, r: TuningFailure<'_>) -> String {
        r.to_string()
    }
}
struct LegacyPort;
impl ScalarSketchPort<LegacySink> for LegacyPort {
    type Reservation = ();
    fn reserve(&self, _: usize, _: &mut LegacySink) -> Result<(), String> {
        Ok(())
    }
    fn reconcile(&mut self, _: usize, _: &mut (), _: &mut LegacySink) -> Result<(), String> {
        Ok(())
    }
}
pub fn evaluate(
    values: &ArrayRef,
    log_ks: Option<ArrayRef>,
    target_types: Option<ArrayRef>,
) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for row in 0..values.len() {
        let produced = produce_row(
            (values, row),
            log_ks.as_ref().map(|a| (a, row)),
            target_types.as_ref().map(|a| (a, row)),
            LegacyPort,
            &mut LegacySink,
        )?;
        match produced {
            None => builder.append_null(),
            Some(produced) => builder.append_value(produced.payload()),
        }
    }
    Ok(Arc::new(builder.finish()))
}
