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

//! Fragment-owned staged-report collection for provider-neutral writers.
//!
//! This is intentionally independent from `sink_commit`: it owns opaque SPI
//! reports only, and does not know about Iceberg files or commit DTOs.

use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{
    ConnectorStagedReport, ConnectorStagedReportFrame, WriteCommitEvidenceLedger,
};

#[derive(Clone, Debug)]
pub struct ConnectorStagedReportCollector {
    inner: Arc<Mutex<ConnectorStagedReportCollectorState>>,
}

#[derive(Debug)]
struct ConnectorStagedReportCollectorState {
    frames: Option<Vec<ConnectorStagedReportFrame>>,
    ledger: WriteCommitEvidenceLedger,
}

impl Default for ConnectorStagedReportCollector {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ConnectorStagedReportCollectorState {
                frames: None,
                ledger: WriteCommitEvidenceLedger::default(),
            })),
        }
    }
}

impl ConnectorStagedReportCollector {
    /// Bind this decoder-created collector to the fragment commit lease before
    /// an operator can publish its connector frames.
    pub fn bind_write_commit_evidence_ledger(
        &self,
        ledger: WriteCommitEvidenceLedger,
    ) -> Result<(), String> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|error| format!("lock connector staged report collector: {error}"))?;
        if guard.frames.is_some() {
            return Err(
                "connector staged report collector cannot rebind after publication".to_string(),
            );
        }
        guard.ledger = ledger;
        Ok(())
    }

    pub fn record(&self, report: ConnectorStagedReport) -> Result<(), String> {
        report
            .validate()
            .map_err(|error| format!("validate connector staged report: {error}"))?;
        let mut guard = self
            .inner
            .lock()
            .map_err(|error| format!("lock connector staged report collector: {error}"))?;
        if guard.frames.is_some() {
            return Err(
                "connector staged report collector received more than one logical report"
                    .to_string(),
            );
        }
        let frames = report.frames();
        let bytes = frames.iter().try_fold(0usize, |total, frame| {
            frame
                .terminal_evidence_encoded_len()
                .map_err(|error| {
                    format!("measure connector staged report terminal evidence: {error}")
                })
                .and_then(|bytes| {
                    total.checked_add(bytes).ok_or_else(|| {
                        "connector staged report byte accounting overflowed".to_string()
                    })
                })
        })?;
        guard
            .ledger
            .reserve(bytes, frames.len())
            .map_err(|error| format!("reserve connector staged report evidence: {error}"))?;
        guard.frames = Some(frames);
        Ok(())
    }

    pub fn take(&self) -> Vec<ConnectorStagedReportFrame> {
        self.inner
            .lock()
            .map(|mut guard| guard.frames.take().unwrap_or_default())
            .unwrap_or_default()
    }

    pub fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorStagedReportSummary, ConnectorWriteCohortId, ConnectorWriteExecutionId,
        ConnectorWriteOperationId, ConnectorWriterIdentity, ConnectorWriterTerminalState,
    };

    use super::*;

    fn writer() -> ConnectorWriterIdentity {
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        ConnectorWriterIdentity::new(
            operation_id,
            ConnectorWriteCohortId::primary(operation_id),
            ConnectorWriteExecutionId::new([2; 16], 3),
            [4; 16],
            5,
            6,
            7,
            ConnectorExecutionBindingKey {
                instance_id: ConnectorInstanceId::parse("test-connector").expect("instance"),
                incarnation: ConnectorInstanceIncarnation::from_bytes([8; 16]),
            },
        )
    }

    #[test]
    fn collector_accepts_one_logical_report() {
        let collector = ConnectorStagedReportCollector::default();
        let report = ConnectorStagedReport::try_new(
            writer(),
            1,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from_static(b"opaque"),
        )
        .expect("report");
        collector.record(report.clone()).expect("record once");
        assert_eq!(collector.take().len(), 1);
        collector.record(report).expect("record after take");
    }

    #[test]
    fn collector_reserves_before_publishing_frames() {
        let collector = ConnectorStagedReportCollector::default();
        collector
            .bind_write_commit_evidence_ledger(
                novarocks_spi::connector::WriteCommitEvidenceLedger::new(
                    novarocks_spi::connector::WriteCommitEvidenceLimits::try_new(3, 1)
                        .expect("limits"),
                ),
            )
            .expect("bind ledger");
        let report = ConnectorStagedReport::try_new(
            writer(),
            1,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from_static(b"four"),
        )
        .expect("report");
        assert!(collector.record(report).is_err());
        assert!(collector.take().is_empty());
    }
}
