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

use arrow::datatypes::DataType;

use crate::common::types::UniqueId;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimeFilterTransmission {
    pub(crate) is_partial: bool,
    pub(crate) query_id: UniqueId,
    pub(crate) filter_id: i32,
    pub(crate) data: Vec<u8>,
    pub(crate) build_be_number: i32,
    pub(crate) column_type: Option<DataType>,
}

impl RuntimeFilterTransmission {
    pub(crate) fn try_new(
        is_partial: bool,
        query_id: UniqueId,
        filter_id: i32,
        data: Vec<u8>,
        build_be_number: i32,
        column_type: Option<DataType>,
    ) -> Result<Self, String> {
        if query_id.hi == 0 && query_id.lo == 0 {
            return Err("runtime filter query_id must not be all-zero".to_string());
        }
        Ok(Self {
            is_partial,
            query_id,
            filter_id,
            data,
            build_be_number,
            column_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeFilterTransmission;
    use crate::common::types::UniqueId;

    #[test]
    fn rejects_all_zero_query_id() {
        let error = RuntimeFilterTransmission::try_new(
            false,
            UniqueId { hi: 0, lo: 0 },
            7,
            Vec::new(),
            0,
            None,
        )
        .expect_err("zero query id must fail in the domain");
        assert!(error.contains("query_id"), "{error}");
    }
}
