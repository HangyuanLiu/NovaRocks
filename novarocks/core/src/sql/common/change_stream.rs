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

//! SQL-owned row-mutation vocabulary.
//!
//! SQL emits only the logical effect selected by a statement. Provider
//! strategy, identity encoding and the opaque sink route are sealed by the
//! connector admission contract and are never reconstructed from SQL values.

pub(crate) const ROW_MUTATION_EFFECT_COLUMN: &str = "__row_mutation_effect";

/// Aggregate MV deltas retain their signed multiplicity column. This is not a
/// row-mutation route discriminator.
pub(crate) const CHANGE_OP_COLUMN: &str = "__change_op";
pub(crate) const CHANGE_OP_INSERT: i8 = 1;
pub(crate) const CHANGE_OP_DELETE: i8 = -1;

pub(crate) use novarocks_spi::connector::ConnectorRowMutationEffect;
