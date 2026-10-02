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
//! Private resumable JSON IN-list pair policy. Membership never calls ExprArena
//! once per candidate. Parser implementation follows this frozen interface.
//!
//! P28 supplies JsonPairCursor with these concrete methods:
//! new(&JsonPairTask) -> Self; start(JsonPairInput) -> Result<(), JsonPairError>;
//! poll(JsonPairInput, JsonPairContext, &mut JsonPairWork) -> Result<JsonPairPoll, JsonPairError>;
//! clear(&mut self). The cursor owns only tracked flat state and input offsets;
//! each poll borrows immutable input anew and checks its owner-minted identity.
mod interface;
pub(crate) use interface::*;
