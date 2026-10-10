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

//! Original percentile error formatting, with no new mathematical dispatch.
use crate::aggregate_scalar::{AggScalarValue, ScalarStateError, TrackedAggScalarValue};
use crate::exact_percentile_core::{
    EXACT_PERCENTILE_MAGIC, EXACT_PERCENTILE_VERSION, ExactPercentileAllocator,
};
use arrow_schema::DataType;
use std::fmt;

/// Borrowed failure facts exist only while the unique original computation
/// is constructing its error. A receiver must consume them synchronously.
pub enum PercentileDataRecipe<'a, A: ExactPercentileAllocator> {
    RateOutOfRange,
    RateMismatch { existing: f64, incoming: f64 },
    InvalidScalar(&'a TrackedAggScalarValue<A>),
    InterpolationInput(&'a AggScalarValue),
    OutputType(&'a DataType),
    ShortPayload,
    Magic(u8),
    Version(u8),
    ScratchOverflow,
    Json(&'a serde_json::Error),
}
impl<A: ExactPercentileAllocator> fmt::Display for PercentileDataRecipe<'_, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RateOutOfRange => f.write_str("Percentile rate must be between 0 and 1"),
            Self::RateMismatch { existing, incoming } => write!(
                f,
                "percentile rate mismatch while merging states: existing={} incoming={}",
                existing, incoming
            ),
            Self::InvalidScalar(value) => write!(
                f,
                "unsupported percentile_disc/cont input scalar {:?}",
                value
            ),
            Self::InterpolationInput(value) => write!(
                f,
                "percentile_cont: unsupported percentile_cont interpolation input {:?}",
                value
            ),
            Self::OutputType(ty) => write!(f, "unsupported percentile_cont output type {:?}", ty),
            Self::ShortPayload => f.write_str("exact percentile payload too short"),
            Self::Magic(actual) => write!(
                f,
                "unsupported exact percentile payload magic: expected=0x{:02x} actual=0x{:02x}",
                EXACT_PERCENTILE_MAGIC, actual
            ),
            Self::Version(actual) => write!(
                f,
                "unsupported exact percentile payload version: expected={} actual={}",
                EXACT_PERCENTILE_VERSION, actual
            ),
            Self::ScratchOverflow => f.write_str("exact percentile parser scratch bound overflow"),
            Self::Json(error) => fmt::Display::fmt(error, f),
        }
    }
}
/// Typed receiver at the original error-construction frontier. It neither
/// evaluates an argument nor walks a state/type/AST to decide a failure.
pub trait PercentileFailureSink<A: ExactPercentileAllocator> {
    type Error;
    fn data(&mut self, recipe: PercentileDataRecipe<'_, A>) -> Self::Error;
    fn scalar(&mut self, error: ScalarStateError) -> Self::Error;
    fn reader(&mut self, error: String) -> Self::Error;
    fn json(&mut self, error: &serde_json::Error, allocator: &A) -> Self::Error {
        let _ = allocator;
        self.data(PercentileDataRecipe::Json(error))
    }
}
pub struct LegacyPercentileFailure;
impl<A: ExactPercentileAllocator> PercentileFailureSink<A> for LegacyPercentileFailure {
    type Error = String;
    fn data(&mut self, recipe: PercentileDataRecipe<'_, A>) -> String {
        recipe.to_string()
    }
    fn scalar(&mut self, error: ScalarStateError) -> String {
        error.to_string()
    }
    fn reader(&mut self, error: String) -> String {
        error
    }
}
