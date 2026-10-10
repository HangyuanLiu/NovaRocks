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
//! ONE original byte-suffix predicate and renderer with explicit output ports.
//! Carrier admission, row addresses, SQL NULL masking and output projection
//! belong to each caller. String allocation remains opaque, not a host grant.
use std::convert::Infallible;
#[derive(Clone, Copy, Debug)]
pub struct RowPlan<'a> {
    text: &'a str,
    suffix: &'a str,
    keep: bool,
}
impl RowPlan<'_> {
    pub fn text_len(self) -> usize {
        self.text.len()
    }
    pub fn appends_suffix(self) -> bool {
        !self.keep
    }
}
/// The two observation sites are the existing selected wrapper's predicate
/// steps. A rejected suffix never asks the caller for its text payload.
pub fn plan_observed<'a, E>(
    ch: &'a str,
    text: impl FnOnce() -> &'a str,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<Option<RowPlan<'a>>, E> {
    if ch.len() != 1 {
        observe()?;
        return Ok(None);
    }
    observe()?;
    let s = text();
    let trailing = ch.as_bytes()[0];
    let s_bytes = s.as_bytes();
    let keep = s_bytes.is_empty() || s_bytes[s_bytes.len() - 1] == trailing;
    observe()?;
    Ok(Some(RowPlan {
        text: s,
        suffix: ch,
        keep,
    }))
}
pub fn plan<'a>(s: &'a str, ch: &'a str) -> Option<RowPlan<'a>> {
    match plan_observed(ch, || s, || Ok::<(), Infallible>(())) {
        Ok(plan) => plan,
        Err(impossible) => match impossible {},
    }
}
/// A concrete output carrier projection. These ports do not decide whether a
/// suffix is appended. Their capacity argument is an original renderer extent,
/// not a host allocation grant.
pub trait OutputWriter: Sized {
    type Output;
    type Error;
    fn unchanged(self, text: &str) -> Result<Self::Output, Self::Error>;
    fn begin_append(self, capacity: usize) -> Result<Self, Self::Error>;
    fn push_str(&mut self, text: &str) -> Result<(), Self::Error>;
    fn finish(self) -> Result<Self::Output, Self::Error>;
}
/// ONE original renderer control flow. The v1 port keeps owned String and its
/// allocation order; the selected port writes into its existing compact bytes.
pub fn render_with<W: OutputWriter>(plan: RowPlan<'_>, writer: W) -> Result<W::Output, W::Error> {
    let s = plan.text;
    let ch = plan.suffix;
    if plan.keep {
        writer.unchanged(s)
    } else {
        let mut v = writer.begin_append(s.len() + 1)?;
        v.push_str(s)?;
        v.push_str(ch)?;
        v.finish()
    }
}
struct OriginalStringWriter(String);
impl OutputWriter for OriginalStringWriter {
    type Output = String;
    type Error = Infallible;
    fn unchanged(self, text: &str) -> Result<String, Infallible> {
        Ok(text.to_string())
    }
    fn begin_append(self, capacity: usize) -> Result<Self, Infallible> {
        Ok(Self(String::with_capacity(capacity)))
    }
    fn push_str(&mut self, text: &str) -> Result<(), Infallible> {
        self.0.push_str(text);
        Ok(())
    }
    fn finish(self) -> Result<String, Infallible> {
        Ok(self.0)
    }
}
/// The legacy adapter has the original infallible owned String policy.
pub fn render_original(plan: RowPlan<'_>) -> String {
    match render_with(plan, OriginalStringWriter(String::new())) {
        Ok(value) => value,
        Err(impossible) => match impossible {},
    }
}
#[cfg(test)]
#[path = "append_trailing_core_tests.rs"]
mod tests;
