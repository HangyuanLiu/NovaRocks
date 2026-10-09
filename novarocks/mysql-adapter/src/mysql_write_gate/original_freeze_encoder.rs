// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Fixed private wire-v2 projection of the ONE original resident freeze.

use super::*;
use crate::mysql_write_gate::original_freeze::{
    ClientBodyScalars, CurrentBodySource, OriginalFreezeScalars,
};
use novarocks_query_application::api::{ResidentSegmentScalars, RootDataScalars};

impl ReplyWriter<'_> {
    fn root_data_scalars(&mut self, data: RootDataScalars) -> Result<(), ControlClass> {
        if data.kind != novarocks_result_contract::RootOutputKind::ClientRows {
            return Err(ControlClass::Fields);
        }
        let task = data.root_task;
        let execution = task.query_execution_id();
        let query = execution.query_id();
        self.put(&query.high().to_le_bytes())?;
        self.put(&query.low().to_le_bytes())?;
        self.u64(execution.attempt_id().get())?;
        self.put(&task.stage_id().get().to_le_bytes())?;
        self.put(&task.task_id().get().to_le_bytes())?;
        self.put(&task.backend_process_id().to_bytes())?;
        self.put(&data.profile.get().to_le_bytes())?;
        self.put(&[1])?; // Exact ClientRows kind; no inferred/unknown purpose encoding.
        self.u64(data.accepted_consumed)?;
        self.u64(data.native_sequence.get())?;
        self.u64(data.body_bytes)?;
        self.flag(data.end_after_data.is_some())?;
        if let Some(end) = data.end_after_data {
            self.u64(end.sequence.get())?;
            self.u64(end.output_rows)?;
        }
        Ok(())
    }
    fn resident_scalars(
        &mut self,
        value: Option<ResidentSegmentScalars>,
    ) -> Result<(), ControlClass> {
        self.flag(value.is_some())?;
        if let Some(value) = value {
            self.root_data_scalars(value.data)?;
            self.u64(value.window_sequence.get())?;
            self.u64(value.completed_rows_by_item)?;
            self.flag(value.has_validated_client_rows)?;
        }
        Ok(())
    }
    fn client_body_scalars(
        &mut self,
        value: Option<ClientBodyScalars>,
    ) -> Result<(), ControlClass> {
        self.flag(value.is_some())?;
        if let Some(value) = value {
            self.u64(value.body_bytes)?;
            self.put(&value.before_remaining.to_le_bytes())?;
            self.u64(value.before_completed_rows)?;
            self.put(&value.after_remaining.to_le_bytes())?;
            self.u64(value.after_completed_rows)?;
        }
        Ok(())
    }
    pub(super) fn original_freeze_scalars(
        &mut self,
        value: Option<OriginalFreezeScalars>,
    ) -> Result<(), ControlClass> {
        self.flag(value.is_some())?;
        if let Some(value) = value {
            self.flag(value.had_resident_window)?;
            for slot in value.slots {
                self.resident_scalars(slot)?;
            }
            self.flag(value.fallback_delivery.is_some())?;
            if let Some(data) = value.fallback_delivery {
                self.root_data_scalars(data)?;
            }
            self.flag(value.fallback_delivery_rows.is_some())?;
            if let Some(rows) = value.fallback_delivery_rows {
                self.u64(rows)?;
            }
            self.put(&[match value.current_source {
                CurrentBodySource::None => 0,
                CurrentBodySource::FrozenDelivering => 1,
                CurrentBodySource::FrozenReady => 2,
                CurrentBodySource::OriginalDeliveryFallback => 3,
            }])?;
            self.receipt(Some(value.framing))?;
            self.u64(value.buffered_row_bytes)?;
            self.client_body_scalars(value.current)?;
            self.client_body_scalars(value.next)?;
            self.flag(value.tail_complete)?;
            self.put(&[value.tail_parts])?;
            for bytes in value.tail_part_bytes {
                self.u64(bytes)?;
            }
            self.u64(value.tail_selected_bytes)?;
        }
        Ok(())
    }
}
