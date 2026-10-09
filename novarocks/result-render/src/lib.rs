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

//! Pure bounded MySQL text encoding boundary shared by BE roots and FE-local
//! producers. It owns neither packet framing nor a result-retention wallet.
//! Hosts pre-admit input/hydration/scratch and hold their capability through
//! actual cursor exit. This interface alone advertises no runtime support.

mod encoder;
pub use encoder::ArrowMysqlTextEncoder;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenderErrorKind {
    SchemaMismatch,
    UnsupportedCarrier,
    UnsupportedPresentation,
    RowTooLarge,
    ElementLimit,
    DepthLimit,
    ArithmeticOverflow,
    Cancelled,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenderError {
    pub kind: RenderErrorKind,
    pub output_ordinal: Option<u32>,
}
impl std::fmt::Display for RenderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bounded MySQL text encoding failed: {:?}", self.kind)?;
        if let Some(ordinal) = self.output_ordinal {
            write!(f, " at output occurrence {ordinal}")?;
        }
        Ok(())
    }
}
impl std::error::Error for RenderError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenderTurnStatus {
    Yielded,
    NeedsOutput,
    InputComplete,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenderTurn {
    pub emitted_bytes: usize,
    /// Count/hydrate/escape scans must obey this CPU-byte quantum even when no
    /// logical payload has been emitted yet.
    pub examined_bytes: usize,
    pub visited_cells: usize,
    pub completed_rows: u64,
    pub status: RenderTurnStatus,
}

/// Emits the flat ClientRows body, including each exact u32LE rowTotal and at
/// least one payload byte in the same segment. Output remains unpublished until
/// the host commits its immutable segment. A 1..4-byte tail returns NeedsOutput
/// at a new row, while short continuation slices are legal. No padding is added.
///
/// Each call advances at most RootProfileV1's 64KiB byte/1024-cell quantum,
/// including row-length counting and nested traversal. Rows <=64KiB use bounded
/// single-pass staging; larger rows use an exact count then an immutable cursor,
/// never whole-row materialization. Cancellation prevents further publication.
pub trait BoundedMysqlTextEncoder: Send {
    fn step(&mut self, output: &mut [u8]) -> Result<RenderTurn, RenderError>;
    fn cancel(&mut self);
    fn scratch_capacity_bytes(&self) -> usize;
}
