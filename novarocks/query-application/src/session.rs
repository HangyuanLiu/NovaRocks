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

use std::sync::Arc;

use crate::client_connection::ClientConnectionToken;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySessionOpenRequest {
    connection: ClientConnectionToken,
    principal: Arc<str>,
}

impl QuerySessionOpenRequest {
    pub fn new(connection: ClientConnectionToken, principal: impl Into<Arc<str>>) -> Self {
        Self {
            connection,
            principal: principal.into(),
        }
    }

    pub const fn connection_id(&self) -> u32 {
        self.connection.connection_id()
    }
    pub const fn connection_token(&self) -> ClientConnectionToken {
        self.connection
    }
    pub fn principal(&self) -> &str {
        &self.principal
    }
}
