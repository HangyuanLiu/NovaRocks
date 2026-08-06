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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergFileFormat {
    Parquet,
    Puffin,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergFileContent {
    Data,
    PositionDeletes,
    EqualityDeletes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergDeleteFileSpec {
    pub path: String,
    pub file_format: IcebergFileFormat,
    pub file_content: IcebergFileContent,
    pub length: Option<u64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
}

impl IcebergDeleteFileSpec {
    pub fn parquet_position_delete(path: String, length: Option<u64>) -> Self {
        Self {
            path,
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::PositionDeletes,
            length,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    pub fn puffin_position_delete(
        path: String,
        length: Option<u64>,
        content_offset: i64,
        content_size_in_bytes: i64,
    ) -> Self {
        Self {
            path,
            file_format: IcebergFileFormat::Puffin,
            file_content: IcebergFileContent::PositionDeletes,
            length,
            content_offset: Some(content_offset),
            content_size_in_bytes: Some(content_size_in_bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquet_position_delete_sets_required_content() {
        let spec = IcebergDeleteFileSpec::parquet_position_delete(
            "/tmp/delete.parquet".to_string(),
            Some(99),
        );

        assert_eq!(spec.file_format, IcebergFileFormat::Parquet);
        assert_eq!(spec.file_content, IcebergFileContent::PositionDeletes);
        assert_eq!(spec.length, Some(99));
        assert_eq!(spec.content_offset, None);
        assert_eq!(spec.content_size_in_bytes, None);
    }

    #[test]
    fn puffin_position_delete_carries_byte_range() {
        let spec = IcebergDeleteFileSpec::puffin_position_delete(
            "/tmp/delete.puffin".to_string(),
            Some(512),
            12,
            34,
        );

        assert_eq!(spec.file_format, IcebergFileFormat::Puffin);
        assert_eq!(spec.file_content, IcebergFileContent::PositionDeletes);
        assert_eq!(spec.length, Some(512));
        assert_eq!(spec.content_offset, Some(12));
        assert_eq!(spec.content_size_in_bytes, Some(34));
    }
}
