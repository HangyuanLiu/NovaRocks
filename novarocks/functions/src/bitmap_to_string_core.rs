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

//! Original owned renderer. The legacy path preserves its original allocation
//! order; the selected path admits the same opaque operation before execution.
use crate::bitmap_value::{BitmapDecodePort, LegacyBitmapPort};

pub fn render(bytes: &[u8]) -> Result<String, String> {
    render_with_port(bytes, &mut LegacyBitmapPort)
}
pub fn render_with_port<P: BitmapDecodePort>(
    bytes: &[u8],
    port: &mut P,
) -> Result<String, P::Error> {
    let values = crate::bitmap_value::decode_bitmap_with_port(bytes, port)?;
    port.before_render(values.len())?;
    let out = values
        .into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    port.boundary()?;
    Ok(out)
}
