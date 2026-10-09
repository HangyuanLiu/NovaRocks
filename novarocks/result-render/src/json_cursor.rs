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

//! Incremental JSON syntax and UTF-8 validation without a value allocation.
use crate::{RenderError, RenderErrorKind};
fn invalid() -> RenderError {
    RenderError {
        kind: RenderErrorKind::UnsupportedPresentation,
        output_ordinal: None,
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Utf8 {
    remaining: u8,
    first_min: u8,
    first_max: u8,
}
impl Utf8 {
    pub fn feed(&mut self, b: u8) -> Result<(), RenderError> {
        if self.remaining != 0 {
            if b < self.first_min || b > self.first_max {
                return Err(invalid());
            }
            self.remaining -= 1;
            self.first_min = 0x80;
            self.first_max = 0xbf;
            return Ok(());
        }
        (self.remaining, self.first_min, self.first_max) = match b {
            0..=0x7f => (0, 0, 0),
            0xc2..=0xdf => (1, 0x80, 0xbf),
            0xe0 => (2, 0xa0, 0xbf),
            0xe1..=0xec | 0xee..=0xef => (2, 0x80, 0xbf),
            0xed => (2, 0x80, 0x9f),
            0xf0 => (3, 0x90, 0xbf),
            0xf1..=0xf3 => (3, 0x80, 0xbf),
            0xf4 => (3, 0x80, 0x8f),
            _ => return Err(invalid()),
        };
        Ok(())
    }
    pub fn finish(self) -> Result<(), RenderError> {
        if self.remaining == 0 {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
enum Expect {
    #[default]
    Value,
    ArrayFirst,
    ObjectFirst,
    Key,
    Colon,
    AfterArray,
    AfterObject,
    Done,
}
#[derive(Clone, Copy, Debug)]
enum Lex {
    None,
    String { key: bool, escape: bool, hex: u8 },
    Literal { word: &'static [u8], at: usize },
    Number(u8),
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct JsonCursor {
    stack: [Expect; 65],
    objects: [bool; 65],
    depth: usize,
    base_depth: usize,
    lex: Lex,
    utf8: Utf8,
}
impl JsonCursor {
    pub fn new(base_depth: usize) -> Self {
        Self {
            stack: [Expect::Value; 65],
            objects: [false; 65],
            depth: 0,
            base_depth,
            lex: Lex::None,
            utf8: Utf8::default(),
        }
    }
    fn scalar_complete(&mut self) -> Result<(), RenderError> {
        if self.depth == 0 {
            self.stack[0] = Expect::Done;
        } else {
            self.stack[self.depth] = if self.objects[self.depth] {
                Expect::AfterObject
            } else {
                Expect::AfterArray
            };
        }
        Ok(())
    }
    fn close(&mut self, object: bool) -> Result<(), RenderError> {
        if self.depth == 0 {
            return Err(invalid());
        }
        let valid = matches!(
            (object, self.stack[self.depth]),
            (true, Expect::ObjectFirst | Expect::AfterObject)
                | (false, Expect::ArrayFirst | Expect::AfterArray)
        );
        if !valid {
            return Err(invalid());
        }
        self.depth -= 1;
        self.scalar_complete()
    }
    pub fn feed(&mut self, b: u8) -> Result<bool, RenderError> {
        loop {
            match self.lex {
                Lex::String { key, escape, hex } => {
                    if hex != 0 {
                        if !b.is_ascii_hexdigit() {
                            return Err(invalid());
                        }
                        self.lex = Lex::String {
                            key,
                            escape: false,
                            hex: hex - 1,
                        };
                        return Ok(false);
                    }
                    if escape {
                        if b == b'u' {
                            self.lex = Lex::String {
                                key,
                                escape: false,
                                hex: 4,
                            };
                        } else if matches!(
                            b,
                            b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't'
                        ) {
                            self.lex = Lex::String {
                                key,
                                escape: false,
                                hex: 0,
                            };
                        } else {
                            return Err(invalid());
                        }
                        return Ok(false);
                    }
                    if b == b'"' {
                        self.utf8.finish()?;
                        self.lex = Lex::None;
                        if key {
                            self.stack[self.depth] = Expect::Colon;
                        } else {
                            self.scalar_complete()?;
                        }
                    } else if b == b'\\' {
                        self.utf8.finish()?;
                        self.lex = Lex::String {
                            key,
                            escape: true,
                            hex: 0,
                        };
                    } else {
                        if b < 0x20 {
                            return Err(invalid());
                        }
                        self.utf8.feed(b)?;
                    }
                    return Ok(false);
                }
                Lex::Literal { word, at } => {
                    if word.get(at) != Some(&b) {
                        return Err(invalid());
                    }
                    if at + 1 == word.len() {
                        self.lex = Lex::None;
                        self.scalar_complete()?;
                    } else {
                        self.lex = Lex::Literal { word, at: at + 1 };
                    }
                    return Ok(false);
                }
                Lex::Number(state) => {
                    let next = match (state, b) {
                        (0, b'0') => Some(1),
                        (0, b'1'..=b'9') => Some(2),
                        (1 | 2, b'.') => Some(3),
                        (2, b'0'..=b'9') => Some(2),
                        (1 | 2 | 4, b'e' | b'E') => Some(5),
                        (3, b'0'..=b'9') => Some(4),
                        (4, b'0'..=b'9') => Some(4),
                        (5, b'+' | b'-') => Some(6),
                        (5 | 6, b'0'..=b'9') => Some(7),
                        (7, b'0'..=b'9') => Some(7),
                        _ => None,
                    };
                    if let Some(next) = next {
                        self.lex = Lex::Number(next);
                        return Ok(false);
                    }
                    if !matches!(state, 1 | 2 | 4 | 7) {
                        return Err(invalid());
                    }
                    self.lex = Lex::None;
                    self.scalar_complete()?;
                    continue;
                }
                Lex::None => {}
            }
            if matches!(b, b' ' | b'\n' | b'\r' | b'\t') {
                return Ok(false);
            }
            let expect = self.stack[self.depth];
            match expect {
                Expect::Done => return Err(invalid()),
                Expect::AfterArray => {
                    if b == b',' {
                        self.stack[self.depth] = Expect::Value;
                    } else if b == b']' {
                        self.close(false)?;
                    } else {
                        return Err(invalid());
                    }
                    return Ok(false);
                }
                Expect::AfterObject => {
                    if b == b',' {
                        self.stack[self.depth] = Expect::Key;
                    } else if b == b'}' {
                        self.close(true)?;
                    } else {
                        return Err(invalid());
                    }
                    return Ok(false);
                }
                Expect::Colon => {
                    if b != b':' {
                        return Err(invalid());
                    } // Save object identity beside its child expectation.
                    self.stack[self.depth] = Expect::Value;
                    return Ok(false);
                }
                Expect::ObjectFirst | Expect::Key => {
                    if b == b'}' && matches!(expect, Expect::ObjectFirst) {
                        self.close(true)?;
                        return Ok(false);
                    }
                    if b != b'"' {
                        return Err(invalid());
                    }
                    self.lex = Lex::String {
                        key: true,
                        escape: false,
                        hex: 0,
                    };
                    self.utf8 = Utf8::default();
                    return Ok(false);
                }
                Expect::ArrayFirst if b == b']' => {
                    self.close(false)?;
                    return Ok(false);
                }
                Expect::Value | Expect::ArrayFirst => {}
            }
            match b {
                b'[' | b'{' => {
                    if self.base_depth + self.depth + 1 > 64 {
                        return Err(RenderError {
                            kind: RenderErrorKind::DepthLimit,
                            output_ordinal: None,
                        });
                    }
                    self.depth += 1;
                    self.objects[self.depth] = b == b'{';
                    self.stack[self.depth] = if b == b'[' {
                        Expect::ArrayFirst
                    } else {
                        Expect::ObjectFirst
                    };
                }
                b'"' => {
                    self.lex = Lex::String {
                        key: false,
                        escape: false,
                        hex: 0,
                    };
                    self.utf8 = Utf8::default();
                }
                b't' => {
                    self.lex = Lex::Literal {
                        word: b"true",
                        at: 1,
                    }
                }
                b'f' => {
                    self.lex = Lex::Literal {
                        word: b"false",
                        at: 1,
                    }
                }
                b'n' => {
                    self.lex = Lex::Literal {
                        word: b"null",
                        at: 1,
                    }
                }
                b'-' => self.lex = Lex::Number(0),
                b'0' => self.lex = Lex::Number(1),
                b'1'..=b'9' => self.lex = Lex::Number(2),
                _ => return Err(invalid()),
            }
            return Ok(true);
        }
    }
    pub fn finish(&mut self) -> Result<(), RenderError> {
        if let Lex::Number(s) = self.lex {
            if !matches!(s, 1 | 2 | 4 | 7) {
                return Err(invalid());
            }
            self.lex = Lex::None;
            self.scalar_complete()?;
        }
        if self.depth == 0 && matches!(self.stack[0], Expect::Done) && matches!(self.lex, Lex::None)
        {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}
