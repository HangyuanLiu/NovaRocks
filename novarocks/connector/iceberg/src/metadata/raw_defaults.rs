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

// Borrowed default extraction uses the public serde_json RawValue API.
// Only canonical JSON just emitted from the SAME metadata owner may enter.
use serde::Deserializer;
use serde::de::{Error, MapAccess, Visitor};
use serde_json::value::RawValue;
use std::fmt;

pub(super) fn member<'a>(
    json: &'a RawValue,
    wanted: &str,
) -> Result<Option<&'a RawValue>, serde_json::Error> {
    struct Find<'w>(&'w str);
    impl<'de> Visitor<'de> for Find<'_> {
        type Value = Option<&'de RawValue>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("canonical metadata object")
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut found = None;
            // Canonical TableMetadata/schema keys are fixed ASCII, not escaped.
            while let Some(key) = map.next_key::<&'de str>()? {
                let value = map.next_value::<&'de RawValue>()?;
                if key == self.0 {
                    if found.is_some() {
                        return Err(M::Error::custom("duplicate canonical member"));
                    }
                    found = Some(value);
                }
            }
            Ok(found)
        }
    }
    let mut de = serde_json::Deserializer::from_str(json.get());
    let result = de.deserialize_map(Find(wanted))?;
    de.end()?;
    Ok(result)
}

pub(super) struct Defaults<'a> {
    pub initial: Option<&'a RawValue>,
    pub write: Option<&'a RawValue>,
}
pub(super) fn defaults(field: &RawValue) -> Result<Defaults<'_>, serde_json::Error> {
    // Manual presence is essential: Option<&RawValue> Deserialize would map
    // a PRESENT JSON null to None. Original Some("null") must remain present.
    Ok(Defaults {
        initial: member(field, "initial-default")?,
        write: member(field, "write-default")?,
    })
}
