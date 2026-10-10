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
//! The original v1 PARSE_URL computation. The key supplier is evaluated only for QUERY.
//! The parser, uppercase conversion, query iterator and owned text are opaque library work.
use url::Url;
/// Outer None means the original two-argument call. Some(None) means its NULL query key.
/// This supplier preserves the original conditional key access without depending on an arena.
pub fn parse_value<'a>(
    url_str: &str,
    part_str: &str,
    key_value: &mut dyn FnMut() -> Option<Option<&'a str>>,
) -> Option<String> {
    let part = part_str.to_uppercase();
    let url = Url::parse(url_str).ok();
    match (url, part.as_str()) {
        (Some(u), "HOST") => u.host_str().map(|s| s.to_string()),
        (Some(u), "PATH") => Some(u.path().to_string()),
        (Some(u), "PROTOCOL") => Some(u.scheme().to_string()),
        (Some(u), "REF") => u.fragment().map(|s| s.to_string()),
        (Some(u), "QUERY") => {
            if let Some(key) = key_value() {
                if let Some(key) = key {
                    u.query_pairs()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.to_string())
                } else {
                    None
                }
            } else {
                u.query().map(|s| s.to_string())
            }
        }
        _ => None,
    }
}
