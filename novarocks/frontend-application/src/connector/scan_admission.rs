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

pub fn admit_connector_change_window(
    table: &novarocks_spi::connector::ConnectorTableHandle,
    schema: &arrow::datatypes::Schema,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    window: novarocks_spi::connector::ConnectorChangeWindow,
) -> Result<novarocks_spi::connector::ConnectorScan, String> {
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorReadPurpose,
        ConnectorScanSelection,
    };

    let binding = planning_lease.binding();
    if table.owner() != &binding.descriptor().instance_id {
        return Err(
            "connector change-window table handle owner does not match its exact planning lease"
                .to_string(),
        );
    }
    let scan = binding
        .planning()
        .begin_scan(
            table,
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
                context: context.clone(),
            },
        )
        .map_err(|error| error.to_string())?;
    scan.validate(
        &novarocks_spi::connector::ConnectorProviderBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        },
        ConnectorScanSelection::ChangeWindow(window),
    )
    .map_err(|error| error.to_string())?;
    if scan.output_schema().fields() != schema.fields() {
        return Err(
            "connector change-window scan schema does not match its exact table metadata"
                .to_string(),
        );
    }
    if !scan.predicate_dispositions().is_empty() {
        return Err(
            "connector change-window scan returned dispositions without static predicates"
                .to_string(),
        );
    }
    Ok(scan)
}

/// The same table, generation and validation as the legacy leaf, with physical
/// observation delegated only to the Provider's explicit original async author.
pub(crate) async fn admit_connector_change_window_async_in(
    table: &ConnectorTableHandle,
    schema: &arrow::datatypes::Schema,
    planning_lease: &ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    window: ConnectorChangeWindow,
    cpu: &std::sync::Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    original: &novarocks_spi::connector::ConnectorOriginalResultScope,
    owner: &mut Option<novarocks_spi::connector::ConnectorProcessLocalPayload>,
) -> Result<ConnectorScan, ConnectorError> {
    validate_owner(table, planning_lease)?;
    let scan = planning_lease
        .binding()
        .planning()
        .begin_scan_async_in(
            table,
            change_window_request(schema, context, window),
            cpu,
            original,
            owner,
        )
        .await?;
    validate_scan(scan, schema, planning_lease, window)
}

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
