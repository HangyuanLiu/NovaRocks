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

//! Borrowed original TDigest failure formatting and observed host frontiers.
use std::fmt;
#[derive(Clone, Copy, Debug)]
pub enum ApproxPercentileObservation {
    Step,
    SortBegin(usize),
    SortEnd,
    JsonBegin(usize),
    JsonEnd,
}
pub enum ApproxPercentileDataRecipe<'a> {
    Static(&'static str),
    Arguments(fmt::Arguments<'a>),
    Json(&'a serde_json::Error),
    Existing(&'a str),
}
impl fmt::Display for ApproxPercentileDataRecipe<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(s) => f.write_str(s),
            Self::Existing(s) => f.write_str(s),
            Self::Arguments(a) => f.write_fmt(*a),
            Self::Json(e) => fmt::Display::fmt(e, f),
        }
    }
}
pub trait ApproxPercentileFailureSink {
    type Error;
    fn data(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> Self::Error;
    fn allocation(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> Self::Error;
    fn observe(&mut self, observation: ApproxPercentileObservation) -> Result<(), Self::Error>;
}
pub struct LegacyApproxPercentileFailure;
impl ApproxPercentileFailureSink for LegacyApproxPercentileFailure {
    type Error = String;
    fn data(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> String {
        recipe.to_string()
    }
    fn allocation(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> String {
        recipe.to_string()
    }
    fn observe(&mut self, _: ApproxPercentileObservation) -> Result<(), String> {
        Ok(())
    }
}
