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

//! Provider-owned `ConnectorWriteControl::activate_row_mutation`.
//!
//! Materializes the provider-owned route graph after a durable operation has
//! retained the exact write lease. This is deliberately not a call to
//! `prepare_write`: route preparation is derived from the sealed row-mutation
//! contract and every physical choice stays inside the Iceberg provider.

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorRowMutationActivationRequest, ConnectorRowMutationExecutionPlan,
};

pub(crate) fn activate_row_mutation(
    _request: ConnectorRowMutationActivationRequest,
    _owner: &ConnectorExecutionBindingKey,
) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "connector write control does not implement row-mutation activation",
    ))
}
