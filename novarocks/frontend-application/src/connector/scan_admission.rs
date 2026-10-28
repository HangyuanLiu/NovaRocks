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

//! Provider scan-admission leaves over an exact planning lease.

use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorChangeWindow,
    ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey,
    ConnectorReadPurpose, ConnectorRequestContext, ConnectorScan, ConnectorScanSelection,
    ConnectorTableHandle,
};

pub(crate) fn validate_owner(
    table: &ConnectorTableHandle,
    lease: &ConnectorControlPlanningLease,
) -> Result<(), ConnectorError> {
    if table.owner() != &lease.binding().descriptor().instance_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector change-window table handle owner does not match its exact planning lease",
        ));
    }
    Ok(())
}

pub(crate) fn change_window_request(
    schema: &arrow::datatypes::Schema,
    context: ConnectorRequestContext,
    window: ConnectorChangeWindow,
) -> ConnectorBeginScanRequest {
    ConnectorBeginScanRequest {
        projection: (0..schema.fields().len()).collect(),
        static_predicates: Vec::new(),
        selection: ConnectorScanSelection::ChangeWindow(window),
        purpose: ConnectorReadPurpose::Query,
        limit: None,
        batch: ConnectorBatchBudget {
            max_rows: std::num::NonZeroUsize::new(4096).expect("batch rows are nonzero"),
            max_bytes: std::num::NonZeroUsize::new(
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            )
            .expect("batch bytes are nonzero"),
        },
        context,
    }
}

pub(crate) fn validate_scan(
    scan: ConnectorScan,
    schema: &arrow::datatypes::Schema,
    lease: &ConnectorControlPlanningLease,
    window: ConnectorChangeWindow,
) -> Result<ConnectorScan, ConnectorError> {
    let binding = lease.binding();
    scan.validate(
        &ConnectorProviderBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        },
        ConnectorScanSelection::ChangeWindow(window),
    )?;
    if scan.output_schema().fields() != schema.fields() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector change-window scan schema does not match its exact table metadata",
        ));
    }
    if !scan.predicate_dispositions().is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector change-window scan returned dispositions without static predicates",
        ));
    }
    Ok(scan)
}
