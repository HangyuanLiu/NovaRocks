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

//! Provider-owned `ConnectorWriteControl::prepare_row_mutation`.
//!
//! Plans a logical row mutation under one exact write-control generation. The
//! provider owns the physical strategy, the signed match layout, and the
//! base version; the application only persists what it is handed.

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorRowMutationPreparationOutcome, ConnectorRowMutationPreparationRequest,
};

pub(crate) fn prepare_row_mutation(
    _request: ConnectorRowMutationPreparationRequest,
    _owner: &ConnectorExecutionBindingKey,
) -> Result<ConnectorRowMutationPreparationOutcome, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "connector write control does not implement row-mutation preparation",
    ))
}
