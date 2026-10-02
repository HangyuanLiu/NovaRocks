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

//! Allocation-free schema shape preflight before SDK serde builds indexes.

use novarocks_types::logical_type::LogicalTypeLimits;

const MAX_JSON_DEPTH: usize = 256;
const MAX_JSON_VALUES: usize = 131_072;

#[derive(Clone, Copy)]
enum Role {
    Root,
    Type(usize),
    Field(usize),
    Fields(usize),
    Name,
    Opaque,
}

struct Scan<'a> {
    bytes: &'a [u8],
    offset: usize,
    nodes: usize,
    names: usize,
    values: usize,
    limits: LogicalTypeLimits,
}

pub(super) fn preflight(json: &str) -> Result<(), String> {
    let mut scan = Scan {
        bytes: json.as_bytes(),
        offset: 0,
        nodes: 0,
        names: 0,
        values: 0,
        limits: LogicalTypeLimits::default(),
    };
    scan.value(Role::Root, 0)?;
    scan.space();
    if scan.offset != scan.bytes.len() {
        return Err("writer schema preflight: trailing JSON".into());
    }
    Ok(())
}

impl<'a> Scan<'a> {
    fn space(&mut self) {
        while self
            .bytes
            .get(self.offset)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.offset += 1;
        }
    }
    fn expect(&mut self, byte: u8) -> Result<(), String> {
        self.space();
        if self.bytes.get(self.offset) != Some(&byte) {
            return Err("writer schema preflight: invalid JSON shape".into());
        }
        self.offset += 1;
        Ok(())
    }
    fn node(&mut self, depth: usize, name_bytes: usize) -> Result<(), String> {
        self.nodes += 1;
        self.names = self
            .names
            .checked_add(name_bytes)
            .ok_or("writer schema preflight: name overflow")?;
        if depth > self.limits.max_depth
            || self.nodes > self.limits.max_nodes
            || self.names > self.limits.max_text_bytes
        {
            return Err(
                "writer schema preflight: semantic budget exceeded before SDK decode".into(),
            );
        }
        Ok(())
    }
    fn hex(&mut self) -> Result<u32, String> {
        let mut value = 0;
        for _ in 0..4 {
            let byte = *self
                .bytes
                .get(self.offset)
                .ok_or("writer schema preflight: truncated escape")?;
            self.offset += 1;
            value = value * 16
                + match byte {
                    b'0'..=b'9' => u32::from(byte - b'0'),
                    b'a'..=b'f' => u32::from(byte - b'a' + 10),
                    b'A'..=b'F' => u32::from(byte - b'A' + 10),
                    _ => return Err("writer schema preflight: invalid escape".into()),
                };
        }
        Ok(value)
    }
    // Return a borrowed raw key plus its decoded UTF-8 byte count. Escaped
    // schema grammar keys are noncanonical; opaque defaults may contain them.
    fn string(&mut self) -> Result<(&'a [u8], bool, usize), String> {
        self.expect(b'"')?;
        let start = self.offset;
        let mut escaped = false;
        let mut decoded = 0;
        loop {
            let byte = *self
                .bytes
                .get(self.offset)
                .ok_or("writer schema preflight: unterminated string")?;
            if byte == b'"' {
                let end = self.offset;
                self.offset += 1;
                return Ok((&self.bytes[start..end], escaped, decoded));
            }
            self.offset += 1;
            match byte {
                0..=31 => return Err("writer schema preflight: invalid string byte".into()),
                b'\\' => {
                    escaped = true;
                    let escape = *self
                        .bytes
                        .get(self.offset)
                        .ok_or("writer schema preflight: truncated escape")?;
                    self.offset += 1;
                    decoded += match escape {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => 1,
                        b'u' => {
                            let mut scalar = self.hex()?;
                            if (0xd800..=0xdbff).contains(&scalar) {
                                if self.bytes.get(self.offset..self.offset + 2) != Some(b"\\u") {
                                    return Err(
                                        "writer schema preflight: missing low surrogate".into()
                                    );
                                }
                                self.offset += 2;
                                let low = self.hex()?;
                                if !(0xdc00..=0xdfff).contains(&low) {
                                    return Err(
                                        "writer schema preflight: invalid low surrogate".into()
                                    );
                                }
                                scalar = 0x10000 + ((scalar - 0xd800) << 10) + low - 0xdc00;
                            }
                            char::from_u32(scalar)
                                .ok_or("writer schema preflight: invalid Unicode scalar")?
                                .len_utf8()
                        }
                        _ => return Err("writer schema preflight: invalid escape".into()),
                    };
                }
                _ => decoded += 1,
            }
        }
    }
    fn value(&mut self, role: Role, json_depth: usize) -> Result<(), String> {
        self.values += 1;
        if json_depth > MAX_JSON_DEPTH || self.values > MAX_JSON_VALUES {
            return Err("writer schema preflight: independent JSON budget exceeded".into());
        }
        if let Role::Field(depth) = role {
            self.node(depth, 0)?;
        }
        self.space();
        let byte = *self
            .bytes
            .get(self.offset)
            .ok_or("writer schema preflight: missing value")?;
        if matches!(role, Role::Root | Role::Field(_)) && byte != b'{'
            || matches!(role, Role::Fields(_)) && byte != b'['
        {
            return Err("writer schema preflight: invalid semantic shape".into());
        }
        match byte {
            b'{' => {
                self.offset += 1;
                self.space();
                if self.bytes.get(self.offset) == Some(&b'}') {
                    self.offset += 1;
                    return Ok(());
                }
                loop {
                    let (key, escaped, _) = self.string()?;
                    if escaped && !matches!(role, Role::Opaque) {
                        return Err("writer schema preflight: noncanonical grammar key".into());
                    }
                    // Keys borrow the input, so no field vector or SDK index
                    // exists while semantic child budgets are being checked.
                    let next = match (role, key) {
                        (Role::Root, b"fields") => Role::Fields(1),
                        (Role::Type(depth), b"fields") => Role::Fields(depth + 1),
                        (Role::Field(depth), b"type") => Role::Type(depth),
                        (Role::Field(_), b"name") => Role::Name,
                        (Role::Type(depth), b"element") => {
                            self.node(depth + 1, 7)?;
                            Role::Type(depth + 1)
                        }
                        (Role::Type(depth), b"key") => {
                            self.node(depth + 1, 3)?;
                            Role::Type(depth + 1)
                        }
                        (Role::Type(depth), b"value") => {
                            self.node(depth + 1, 5)?;
                            Role::Type(depth + 1)
                        }
                        _ => Role::Opaque,
                    };
                    self.expect(b':')?;
                    self.value(next, json_depth + 1)?;
                    self.space();
                    match self.bytes.get(self.offset) {
                        Some(b',') => self.offset += 1,
                        Some(b'}') => {
                            self.offset += 1;
                            break;
                        }
                        _ => return Err("writer schema preflight: invalid object separator".into()),
                    }
                }
            }
            b'[' => {
                self.offset += 1;
                self.space();
                if self.bytes.get(self.offset) == Some(&b']') {
                    self.offset += 1;
                    return Ok(());
                }
                loop {
                    let next = match role {
                        Role::Fields(depth) => Role::Field(depth),
                        _ => Role::Opaque,
                    };
                    self.value(next, json_depth + 1)?;
                    self.space();
                    match self.bytes.get(self.offset) {
                        Some(b',') => self.offset += 1,
                        Some(b']') => {
                            self.offset += 1;
                            break;
                        }
                        _ => return Err("writer schema preflight: invalid array separator".into()),
                    }
                }
            }
            b'"' => {
                let (_, _, length) = self.string()?;
                if matches!(role, Role::Name) {
                    self.names = self
                        .names
                        .checked_add(length)
                        .ok_or("writer schema preflight: name overflow")?;
                    if self.names > self.limits.max_text_bytes {
                        return Err("writer schema preflight: semantic name budget exceeded before SDK decode".into());
                    }
                }
            }
            _ => {
                if matches!(role, Role::Name) {
                    return Err("writer schema preflight: invalid name".into());
                }
                let start = self.offset;
                while self
                    .bytes
                    .get(self.offset)
                    .is_some_and(|b| !b.is_ascii_whitespace() && !b",]}".contains(b))
                {
                    self.offset += 1;
                }
                if start == self.offset {
                    return Err("writer schema preflight: invalid primitive".into());
                }
                // This scalar-only parse has no recursive allocation. SDK serde
                // remains the authority for primitive type names and values.
                serde_json::from_slice::<serde::de::IgnoredAny>(&self.bytes[start..self.offset])
                    .map_err(|_| "writer schema preflight: invalid primitive".to_string())?;
            }
        }
        Ok(())
    }
}
