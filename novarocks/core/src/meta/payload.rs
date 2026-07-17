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

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::meta::MetaPayload;
use crate::meta::avro;
use crate::meta::repository::RepositoryResult;

pub fn encode_record_payload<T>(kind: &str, value: &T) -> RepositoryResult<MetaPayload>
where
    T: Serialize,
{
    avro::encode_payload(kind, value)
}

pub fn decode_payload_for_kind<T>(kind: &str, payload: &MetaPayload) -> RepositoryResult<T>
where
    T: DeserializeOwned,
{
    avro::decode_payload(kind, payload)
}
