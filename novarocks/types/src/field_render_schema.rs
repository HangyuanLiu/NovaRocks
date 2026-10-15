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

use crate::PrimitiveType;

/// Render metadata carried with a result-field projection.
///
/// This is a value description of an Arrow field, not a protocol encoder.  It
/// belongs in the neutral type layer so fragment execution can request a
/// result session without depending on a Core/Frontend rendering owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldRenderSchema {
    primitive: Option<PrimitiveType>,
    json_value: bool,
    children: Vec<FieldRenderSchema>,
}

impl FieldRenderSchema {
    pub fn scalar(primitive: Option<PrimitiveType>) -> Self {
        Self {
            primitive,
            json_value: primitive.is_some_and(PrimitiveType::is_json),
            children: Vec::new(),
        }
    }

    pub fn complex(children: Vec<FieldRenderSchema>) -> Self {
        Self {
            primitive: None,
            json_value: false,
            children,
        }
    }

    pub fn primitive(&self) -> Option<PrimitiveType> {
        self.primitive
    }

    pub const fn is_json_value(&self) -> bool {
        self.json_value
    }

    pub fn struct_child(&self, idx: usize) -> Option<&FieldRenderSchema> {
        self.children.get(idx)
    }

    pub fn list_item(&self) -> Option<&FieldRenderSchema> {
        self.children.first()
    }

    pub fn map_key(&self) -> Option<&FieldRenderSchema> {
        self.children.first()
    }

    pub fn map_value(&self) -> Option<&FieldRenderSchema> {
        self.children.get(1)
    }

    pub fn renders_opaque_binary(&self) -> bool {
        self.primitive.is_some_and(PrimitiveType::is_opaque_binary)
    }
}
