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

//! MySQL protocol adaptation for Query Application contracts.

mod connection_registry;

use novarocks_query_application::session_error::QueryServiceErrorKind;
use opensrv_mysql::ErrorKind;

pub use connection_registry::MysqlClientConnectionRegistry;

pub fn error_kind_for_query_service_error(kind: QueryServiceErrorKind) -> ErrorKind {
    match kind {
        QueryServiceErrorKind::Parse => ErrorKind::ER_PARSE_ERROR,
        QueryServiceErrorKind::BadDatabase => ErrorKind::ER_BAD_DB_ERROR,
        QueryServiceErrorKind::Unsupported => ErrorKind::ER_NOT_SUPPORTED_YET,
        QueryServiceErrorKind::PermissionDenied => ErrorKind::ER_SPECIFIC_ACCESS_DENIED_ERROR,
        QueryServiceErrorKind::NoSuchSession => ErrorKind::ER_NO_SUCH_THREAD,
        QueryServiceErrorKind::Interrupted => ErrorKind::ER_QUERY_INTERRUPTED,
        QueryServiceErrorKind::Timeout => ErrorKind::ER_UNKNOWN_ERROR,
        QueryServiceErrorKind::InvalidValue => ErrorKind::ER_WRONG_VALUE,
        QueryServiceErrorKind::Unavailable | QueryServiceErrorKind::Internal => {
            ErrorKind::ER_UNKNOWN_ERROR
        }
        QueryServiceErrorKind::FrontendDraining => ErrorKind::ER_SERVER_SHUTDOWN,
    }
}
