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

//! JSON value-form IN / NOT IN membership: RHS and probe lifecycle.
//!
//! Per exact Task and node, one [`MembershipShared`] owns the RHS. A single
//! local build driver, gathered to one by the builder, moves whole RHS chunks
//! into a fallible index and transfers their leases to the exact Task
//! tracker. The RHS is published `Complete` only by that driver's normal
//! finish, after every frozen sender and the local build reached EOS; failure
//! and cancellation publish their own terminal phase and wake waiting probes.
//!
//! Every local probe driver shares the complete RHS. A probe holds at most one
//! input chunk with its row, candidate and pair cursor and the accumulated
//! UNKNOWN; it accepts no new input until that chunk's output is emitted, and
//! downstream backpressure keeps the cursor where it is. Each driver turn
//! refills a bounded work allowance; spending it raises an explicit yield
//! request, so the driver returns Ready and reschedules instead of reading an
//! absent output as an end of stream. Pair semantics are the governed JSON
//! IN-list owner's; SQL NULL and an empty RHS follow the three-valued table.
//!
//! All dynamic state is task-allocated and admitted before allocation; the
//! output re-exposes the probe columns through a scoped buffer owner that
//! keeps the probe chunk's charge until the last derived buffer drops. There
//! is no asynchronous parser, no process-global registry, and no new task
//! terminal or service acknowledgement.

mod build;
mod output;
mod probe;
mod shared;
#[cfg(test)]
mod tests;

pub(crate) use build::MembershipBuildSinkFactory;
pub(crate) use probe::MembershipProbeFactory;
pub(crate) use shared::MembershipShared;
