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

//! Original DS HLL input diagnostic facts, synchronously borrowed.
use crate::aggregate_scalar::ScalarStateError;
use crate::datasketches_hll_failure::{HllFailureSink, HllObservation};
use crate::percentile_input::{PercentileInputDiagnostic, PercentilePayloadFailure};
use crate::sketch_hash::{SketchHashFailure, SketchHashFailureSink};
use arrow_schema::DataType;
use std::fmt;
#[derive(Clone, Copy, Debug)]
pub enum DsHllInputContext {
    CountDistinct,
    Merge,
}
impl DsHllInputContext {
    pub const fn label(self) -> &'static str {
        match self {
            Self::CountDistinct => "ds_hll_count_distinct",
            Self::Merge => "ds_hll_merge",
        }
    }
}
pub enum DsHllInputDetail<'a> {
    Downcast(&'static str),
    LogK(&'a DataType),
    Target(&'a DataType),
    EmptyStruct,
    Output(&'a DataType),
    Uninitialized,
    Hash(SketchHashFailure<'a>),
    Payload(PercentilePayloadFailure<'a>),
}
pub struct DsHllInputRecipe<'a> {
    pub context: DsHllInputContext,
    pub detail: DsHllInputDetail<'a>,
}
impl fmt::Display for DsHllInputRecipe<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let context = self.context.label();
        match &self.detail {
            DsHllInputDetail::Downcast(kind) => write!(f, "{context}: failed to downcast {kind}"),
            DsHllInputDetail::LogK(ty) => write!(
                f,
                "{context}: ds_hll log_k expects integer input, got {:?}",
                ty
            ),
            DsHllInputDetail::Target(ty) => write!(
                f,
                "{context}: ds_hll target type expects string input, got {:?}",
                ty
            ),
            DsHllInputDetail::EmptyStruct => write!(f, "{context}: ds_hll input struct is empty"),
            DsHllInputDetail::Output(ty) => write!(
                f,
                "ds_hll output type must be Binary or Int64, got {:?}",
                ty
            ),
            DsHllInputDetail::Uninitialized => f.write_str("ds_hll handle is not initialized"),
            DsHllInputDetail::Hash(error) => fmt::Display::fmt(&error.message(context), f),
            DsHllInputDetail::Payload(error) => fmt::Display::fmt(
                &error.message(PercentileInputDiagnostic::LegacyLabel(context)),
                f,
            ),
        }
    }
}
pub trait DsHllFailureSink: HllFailureSink {
    fn kernel(&mut self, cause: crate::KernelFailure) -> Self::Error;
    type Temporary;
    fn reserve_temporary(&mut self, bytes: usize) -> Result<Self::Temporary, Self::Error>;
    fn input(&mut self, recipe: DsHllInputRecipe<'_>) -> Self::Error;
    fn scalar(&mut self, error: ScalarStateError) -> Self::Error;
}
pub struct LegacyDsHllFailure;
impl HllFailureSink for LegacyDsHllFailure {
    type Error = String;
    fn data(&mut self, recipe: crate::datasketches_hll_failure::HllDataRecipe<'_>) -> String {
        recipe.to_string()
    }
    fn invariant(&mut self, recipe: crate::datasketches_hll_failure::HllInvariantRecipe) -> String {
        recipe.to_string()
    }
    fn observe(&mut self, _: HllObservation) -> Result<(), String> {
        Ok(())
    }
}
impl DsHllFailureSink for LegacyDsHllFailure {
    fn kernel(&mut self, cause: crate::KernelFailure) -> String {
        cause.to_string()
    }
    type Temporary = ();
    fn reserve_temporary(&mut self, _: usize) -> Result<(), String> {
        Ok(())
    }
    fn input(&mut self, recipe: DsHllInputRecipe<'_>) -> String {
        recipe.to_string()
    }
    fn scalar(&mut self, error: ScalarStateError) -> String {
        error.to_string()
    }
}
pub struct HashSink<'a, F: DsHllFailureSink> {
    pub sink: &'a mut F,
    pub context: DsHllInputContext,
}
impl<F: DsHllFailureSink> SketchHashFailureSink for HashSink<'_, F> {
    type Error = F::Error;
    fn hash_data(&mut self, error: SketchHashFailure<'_>) -> Self::Error {
        self.sink.input(DsHllInputRecipe {
            context: self.context,
            detail: DsHllInputDetail::Hash(error),
        })
    }
    fn observe_hash(&mut self, event: HllObservation) -> Result<(), Self::Error> {
        self.sink.observe(event)
    }
}
