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

//! Deterministic Iceberg literal JSON serialization for native protobuf fields.

pub(crate) fn serialize_iceberg_literal_json(
    literal: &novarocks_connector_iceberg::iceberg::spec::Literal,
) -> Result<String, String> {
    match literal {
        novarocks_connector_iceberg::iceberg::spec::Literal::Primitive(prim) => match prim {
            novarocks_connector_iceberg::iceberg::spec::PrimitiveLiteral::Boolean(b) => {
                Ok(b.to_string())
            }
            novarocks_connector_iceberg::iceberg::spec::PrimitiveLiteral::Int(v) => {
                Ok(v.to_string())
            }
            novarocks_connector_iceberg::iceberg::spec::PrimitiveLiteral::Long(v) => {
                Ok(v.to_string())
            }
            novarocks_connector_iceberg::iceberg::spec::PrimitiveLiteral::Float(v) => {
                Ok(v.0.to_string())
            }
            novarocks_connector_iceberg::iceberg::spec::PrimitiveLiteral::Double(v) => {
                Ok(v.0.to_string())
            }
            novarocks_connector_iceberg::iceberg::spec::PrimitiveLiteral::Int128(v) => {
                Ok(v.to_string())
            }
            novarocks_connector_iceberg::iceberg::spec::PrimitiveLiteral::String(s) => {
                serde_json::to_string(s).map_err(|e| format!("serialize String default: {e}"))
            }
            novarocks_connector_iceberg::iceberg::spec::PrimitiveLiteral::Binary(b) => {
                let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
                serde_json::to_string(&hex).map_err(|e| format!("serialize Binary default: {e}"))
            }
            other => Err(format!(
                "unsupported primitive literal for native plan emission: {other:?}"
            )),
        },
        other => Err(format!(
            "unsupported literal kind for native plan emission: {other:?}"
        )),
    }
}
