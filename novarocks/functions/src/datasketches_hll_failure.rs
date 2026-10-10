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

//! Borrowed original DS HLL failure facts at their actual construction frontier.
use std::fmt;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HllDiagnosticContext {
    Direct,
    InitialPreflight,
    MergePreflight,
}
impl fmt::Display for HllDiagnosticContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Direct => "ds_hll",
            Self::InitialPreflight => "ds_hll preflight",
            Self::MergePreflight => "ds_hll merge preflight",
        })
    }
}
pub enum HllDataDetail<'a> {
    Truncated(usize),
    Length { required: usize, actual: usize },
    Version(u8),
    Family(u8),
    LogK(u8),
    Target(u8),
    ListHeader { preamble: u8, lg_arr: u8 },
    ListCount,
    SetPreamble(u8),
    SetLgArr { max: u8, actual: u8 },
    SetCount { count: usize, capacity: usize },
    HllPreamble(u8),
    RegisterCount,
    Hll4LgArr(u8),
    AuxOverflow,
    PayloadOverflow,
    Hll6Aux,
    Hll8Aux,
    Mode(u8),
    Deserialize(&'a datasketches::error::Error),
    Create(&'a datasketches::error::Error),
    NewLogK(u8),
}
pub struct HllDataRecipe<'a> {
    pub context: HllDiagnosticContext,
    pub detail: HllDataDetail<'a>,
}
impl fmt::Display for HllDataRecipe<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use HllDataDetail::*;
        let context = self.context;
        match &self.detail {
            Truncated(offset) => write!(f, "{context}: HLL header is truncated at byte {offset}"),
            Length { required, actual } => write!(
                f,
                "{context}: HLL payload requires {required} bytes, got {actual}"
            ),
            Version(actual) => write!(
                f,
                "{context}: expected HLL serial version {}, got {actual}",
                crate::datasketches_hll::HLL_SERIAL_VERSION
            ),
            Family(actual) => write!(
                f,
                "{context}: expected HLL family {}, got {actual}",
                crate::datasketches_hll::HLL_FAMILY_ID
            ),
            LogK(actual) => write!(f, "{context}: HLL lg_k must be in [4, 21], got {actual}"),
            Target(actual) => write!(f, "{context}: invalid HLL target type {actual}"),
            ListHeader { preamble, lg_arr } => write!(
                f,
                "{context}: invalid LIST header preamble={preamble} lg_arr={lg_arr}"
            ),
            ListCount => write!(f, "{context}: invalid LIST coupon count or empty flag"),
            SetPreamble(actual) => write!(f, "{context}: invalid SET preamble {actual}"),
            SetLgArr { max, actual } => write!(
                f,
                "{context}: SET lg_arr must be in [5, {max}], got {actual}"
            ),
            SetCount { count, capacity } => write!(
                f,
                "{context}: SET coupon count {count} exceeds capacity {capacity}"
            ),
            HllPreamble(actual) => write!(f, "{context}: invalid HLL preamble {actual}"),
            RegisterCount => write!(f, "{context}: HLL register or auxiliary count exceeds k"),
            Hll4LgArr(actual) => write!(f, "{context}: invalid HLL4 lg_arr {actual}"),
            AuxOverflow => write!(f, "{context}: HLL4 auxiliary payload length overflows"),
            PayloadOverflow => write!(f, "{context}: HLL4 payload length overflows"),
            Hll6Aux => write!(f, "{context}: HLL6 auxiliary count must be zero"),
            Hll8Aux => write!(f, "{context}: HLL8 auxiliary count must be zero"),
            Mode(actual) => write!(f, "{context}: invalid HLL mode {actual}"),
            Deserialize(error) => {
                write!(f, "{context}: failed to deserialize HLL payload: {error}")
            }
            Create(error) => write!(f, "ds_hll: failed to create HLL union: {error}"),
            NewLogK(actual) => write!(f, "ds_hll log_k must be in [4, 21], got {actual}"),
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum HllInvariantRecipe {
    NewPreflight,
    PayloadPreflight,
    UpdatePreflight,
    MergePreflight,
    SerializePreflight,
    Generation,
}
impl HllInvariantRecipe {
    pub const fn message(self) -> &'static str {
        match self {
            Self::NewPreflight => "ds_hll: new allocation preflight does not match operation",
            Self::PayloadPreflight => {
                "ds_hll: payload allocation preflight does not match operation"
            }
            Self::UpdatePreflight => "ds_hll: update allocation preflight is stale or mismatched",
            Self::MergePreflight => "ds_hll: merge allocation preflight is stale or mismatched",
            Self::SerializePreflight => {
                "ds_hll: serialize allocation preflight is stale or mismatched"
            }
            Self::Generation => "ds_hll: handle generation overflow",
        }
    }
}
impl fmt::Display for HllInvariantRecipe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

#[derive(Clone, Copy, Debug)]
pub enum HllObservation {
    Step,
    OpaqueBoundary,
}
pub trait HllFailureSink {
    type Error;
    fn data(&mut self, recipe: HllDataRecipe<'_>) -> Self::Error;
    fn invariant(&mut self, recipe: HllInvariantRecipe) -> Self::Error;
    fn observe(&mut self, event: HllObservation) -> Result<(), Self::Error>;
}
pub struct LegacyHllFailure;
impl HllFailureSink for LegacyHllFailure {
    type Error = String;
    fn data(&mut self, recipe: HllDataRecipe<'_>) -> String {
        recipe.to_string()
    }
    fn invariant(&mut self, recipe: HllInvariantRecipe) -> String {
        recipe.to_string()
    }
    fn observe(&mut self, _: HllObservation) -> Result<(), String> {
        Ok(())
    }
}
