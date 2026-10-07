// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Explicit allocation limits for the protocol owner. The caller supplies deadlines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolLimits {
    pub row_bytes: usize,
    pub metadata_bytes: usize,
    pub command_bytes: usize,
    pub auth_bytes: usize,
    pub diagnostic_bytes: usize,
    pub long_data_bytes: usize,
    pub prepared_statements: usize,
    /// Aggregate declared parameter positions across all prepared statements.
    pub prepared_parameters: usize,
    pub long_data_entries: usize,
    /// Complete input backing and protocol bookkeeping coverage, including peaks.
    pub connection_input_bytes: usize,
    pub columns: usize,
    pub coalescing_bytes: usize,
}
impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            row_bytes: 64 * 1024 * 1024,
            metadata_bytes: 512 * 1024,
            command_bytes: 1024 * 1024,
            auth_bytes: 64 * 1024,
            diagnostic_bytes: 16 * 1024,
            long_data_bytes: 1024 * 1024,
            prepared_statements: 64,
            prepared_parameters: 4096,
            long_data_entries: 4096,
            connection_input_bytes: 2 * 1024 * 1024,
            columns: 4096,
            coalescing_bytes: 64 * 1024,
        }
    }
}
impl ProtocolLimits {
    pub fn validate(self) -> std::io::Result<Self> {
        if self.row_bytes == 0
            || self.row_bytes > 1024 * 1024 * 1024
            || self.metadata_bytes == 0
            || self.metadata_bytes > 512 * 1024
            || self.command_bytes == 0
            || self.command_bytes > 1024 * 1024
            || self.auth_bytes == 0
            || self.auth_bytes > 64 * 1024
            || self.diagnostic_bytes == 0
            || self.diagnostic_bytes > 16 * 1024
            || self.long_data_bytes == 0
            || self.long_data_bytes > 1024 * 1024
            || self.prepared_statements == 0
            || self.prepared_statements > 64
            || self.prepared_parameters == 0
            || self.prepared_parameters > 4096
            || self.long_data_entries == 0
            || self.long_data_entries > 4096
            || self.connection_input_bytes
                <= self
                    .command_bytes
                    .max(self.auth_bytes.saturating_mul(2).saturating_add(1024))
            || self.connection_input_bytes > 2 * 1024 * 1024
            || self.columns == 0
            || self.columns > 4096
            || self.coalescing_bytes == 0
            || self.coalescing_bytes > 64 * 1024
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid MySQL protocol limits",
            ));
        }
        Ok(self)
    }
}
