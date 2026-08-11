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

//! Golden parity between this crate's write-control preparation paths and the
//! legacy Core implementation they replace.
//!
//! The provider crate must not depend on Core, so the Core outcomes are frozen
//! here as golden constants rather than computed side by side. Every constant
//! records the Core function and source commit it was captured from.
//!
//! TEMPORARY: this module exists only while both implementations coexist. It
//! must be deleted in the same PR that removes the Core implementation
//! (SPI-5J); it is not a permanent conformance suite.
