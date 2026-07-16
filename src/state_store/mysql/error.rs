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

use super::super::{StateStoreError, StateStoreErrorKind};
use mysql_async::{DriverError, Error, IoError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MysqlNativeError {
    public: StateStoreError,
}

impl MysqlNativeError {
    pub(crate) fn provider_unavailable() -> Self {
        Self {
            public: StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "MySQL provider operation failed",
            ),
        }
    }

    pub(crate) fn deadline() -> Self {
        Self {
            public: StateStoreError::new(
                StateStoreErrorKind::DeadlineExceeded,
                "MySQL provider operation exceeded its deadline",
            ),
        }
    }

    pub(crate) fn invalid_configuration() -> Self {
        Self {
            public: StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL provider configuration was rejected",
            ),
        }
    }

    pub(crate) fn into_public(self) -> StateStoreError {
        self.public
    }
}

impl From<Error> for MysqlNativeError {
    fn from(error: Error) -> Self {
        match error {
            Error::Server(server) if matches!(server.code, 1044 | 1045 | 1049) => {
                Self::invalid_configuration()
            }
            Error::Io(IoError::Tls(_)) | Error::Url(_) => Self::invalid_configuration(),
            Error::Driver(
                DriverError::UnknownAuthPlugin { .. }
                | DriverError::MysqlOldPasswordDisabled
                | DriverError::NoKeyFound
                | DriverError::NoClientSslFlagFromServer
                | DriverError::CleartextPluginDisabled
                | DriverError::InvalidParsecSalt,
            ) => Self::invalid_configuration(),
            Error::Driver(_) | Error::Io(IoError::Io(_)) | Error::Other(_) | Error::Server(_) => {
                Self::provider_unavailable()
            }
        }
    }
}
