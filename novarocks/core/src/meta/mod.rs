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

pub mod avro;
pub mod error;
pub mod id;
pub mod keys;
pub mod payload;
pub mod provider;
pub mod record;
pub mod repository;
pub mod sqlite;

pub use error::{MetaError, MetaErrorKind};
pub use id::IdScope;
pub use provider::{
    MetaCommitOutcome, MetaReadTxn, MetaStoreCapabilities, MetaStoreProvider, MetaWriteTxn,
};
pub use record::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaPayload, MetaPayloadEncoding, MetaRecord,
    MetaRecordKind, MetaRecordPut, MetaRevision,
};
pub use sqlite::SqliteMetaStoreProvider;
