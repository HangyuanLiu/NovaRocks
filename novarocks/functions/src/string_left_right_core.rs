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
//! ONE original Unicode selection program with explicit collection policies.
//! Original collection keeps the two RIGHT Strings; selected collection stores
//! the same selected characters as a borrowed source span, without a temporary.
use std::convert::Infallible;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeftRightOp {
    Left,
    Right,
}
/// A collection representation must retain the exact yielded character sequence.
/// Reverse collection may retain it as a directional view of the source. The
/// restore operation reverses that sequence into the final forward output.
pub trait LeftRightProjection<'a> {
    type Error;
    type Output;
    type Reversed;
    fn empty(&mut self) -> Result<Self::Output, Self::Error>;
    fn forward(
        &mut self,
        source: &'a str,
        characters: impl Iterator<Item = char>,
    ) -> Result<Self::Output, Self::Error>;
    fn reverse(
        &mut self,
        source: &'a str,
        characters: impl Iterator<Item = char>,
    ) -> Result<Self::Reversed, Self::Error>;
    fn restore(&mut self, reversed: Self::Reversed) -> Result<Self::Output, Self::Error>;
}
pub fn project<'a, P: LeftRightProjection<'a>>(
    s: &'a str,
    n: i64,
    op: LeftRightOp,
    mut projection: P,
) -> Result<P::Output, P::Error> {
    if n <= 0 {
        return projection.empty();
    }
    let n = n as usize;
    if op == LeftRightOp::Left {
        projection.forward(s, s.chars().take(n))
    } else {
        let reversed = projection.reverse(s, s.chars().rev().take(n))?;
        projection.restore(reversed)
    }
}
struct OriginalProjection;
impl<'a> LeftRightProjection<'a> for OriginalProjection {
    type Error = Infallible;
    type Output = String;
    type Reversed = String;
    fn empty(&mut self) -> Result<String, Infallible> {
        Ok(String::new())
    }
    fn forward(
        &mut self,
        _source: &'a str,
        characters: impl Iterator<Item = char>,
    ) -> Result<String, Infallible> {
        Ok(characters.collect::<String>())
    }
    fn reverse(
        &mut self,
        _source: &'a str,
        characters: impl Iterator<Item = char>,
    ) -> Result<String, Infallible> {
        Ok(characters.collect::<String>())
    }
    fn restore(&mut self, reversed: String) -> Result<String, Infallible> {
        Ok(reversed.chars().rev().collect())
    }
}
pub fn original(s: &str, n: i64, op: LeftRightOp) -> String {
    match project(s, n, op, OriginalProjection) {
        Ok(value) => value,
        Err(impossible) => match impossible {},
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_projection_keeps_unicode_scalar_order_and_nonpositive_count() {
        for (s, n, left, right) in [
            ("aé中🙂", 2, "aé", "中🙂"),
            ("e\u{301}x", 2, "e\u{301}", "\u{301}x"),
            ("\0é", i64::MAX, "\0é", "\0é"),
            ("🙂", i64::MIN, "", ""),
            ("", i64::MAX, "", ""),
        ] {
            assert_eq!(original(s, n, LeftRightOp::Left), left);
            assert_eq!(original(s, n, LeftRightOp::Right), right);
        }
    }
}
