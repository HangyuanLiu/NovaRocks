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
//! A byte-at-a-time JSON parser. No input references survive a poll.
use super::number::{NumberCursor, NumberStep};
use super::tree::{Kind, NONE, Span, Tree};
use super::{JsonPairError, JsonPairTask};
use crate::exec::expr::agg::AggregateVec;

#[derive(Clone, Copy)]
enum Expect {
    ArrayFirst,
    ArrayValue,
    ArrayAfter,
    ObjectFirst,
    ObjectKey,
    ObjectColon,
    ObjectValue,
    ObjectAfter,
}
#[derive(Clone, Copy)]
struct Frame {
    node: usize,
    last: usize,
    expect: Expect,
    key: Span,
}
#[derive(Clone, Copy)]
enum Escape {
    Normal,
    Slash,
    Hex {
        value: u16,
        count: u8,
        high: Option<u16>,
    },
    LowSlash(u16),
    LowU(u16),
}
enum Token {
    None,
    String {
        start: usize,
        key: bool,
        escape: Escape,
    },
    Literal {
        expected: &'static [u8],
        at: usize,
        kind: Kind,
    },
    Number(NumberCursor),
}
pub(super) enum ParseStep {
    Consumed,
    Progress,
    Valid,
    Invalid,
}
pub(super) struct Parser {
    pub tree: Tree,
    frames: AggregateVec<Frame>,
    token: Token,
    root: bool,
}
impl Parser {
    pub(super) fn new(task: &JsonPairTask) -> Self {
        Self {
            tree: Tree::new(task),
            frames: AggregateVec::new_in(task.allocator().clone()),
            token: Token::None,
            root: false,
        }
    }
    fn add(&mut self, task: &JsonPairTask, kind: Kind, text: Span) -> Result<usize, JsonPairError> {
        let key = self.frames.last().map_or(Span::default(), |f| f.key);
        let node = self.tree.node(task, kind, text, key)?;
        if let Some(parent) = self.frames.last_mut() {
            if parent.last == NONE {
                self.tree.nodes[parent.node].first = node;
            } else {
                self.tree.nodes[parent.last].next = node;
            }
            parent.last = node;
            parent.expect = match parent.expect {
                Expect::ArrayFirst | Expect::ArrayValue => Expect::ArrayAfter,
                Expect::ObjectValue => Expect::ObjectAfter,
                _ => unreachable!("only value positions attach nodes"),
            };
        } else {
            self.root = true;
        }
        Ok(node)
    }
    pub(super) fn step(
        &mut self,
        task: &JsonPairTask,
        byte: Option<u8>,
    ) -> Result<ParseStep, JsonPairError> {
        let token = std::mem::replace(&mut self.token, Token::None);
        match token {
            Token::Literal { expected, at, kind } => {
                if byte != Some(expected[at]) {
                    return Ok(ParseStep::Invalid);
                }
                if at + 1 == expected.len() {
                    self.add(task, kind, Span::default())?;
                } else {
                    self.token = Token::Literal {
                        expected,
                        at: at + 1,
                        kind,
                    };
                }
                return Ok(ParseStep::Consumed);
            }
            Token::Number(mut number) => {
                let result = number.step(byte);
                let step = match result {
                    NumberStep::Consumed => ParseStep::Consumed,
                    NumberStep::Progress => ParseStep::Progress,
                    NumberStep::Invalid => return Ok(ParseStep::Invalid),
                    NumberStep::Done(value) => {
                        self.add(task, Kind::Number(value), Span::default())?;
                        return Ok(ParseStep::Progress);
                    }
                };
                self.token = Token::Number(number);
                return Ok(step);
            }
            Token::String { start, key, escape } => {
                let Some(b) = byte else {
                    return Ok(ParseStep::Invalid);
                };
                let next = match escape {
                    Escape::Normal => match b {
                        b'"' => {
                            let span = Span {
                                start,
                                len: self.tree.bytes.len() - start,
                            };
                            if key {
                                let parent = self.frames.last_mut().expect("object key frame");
                                parent.key = span;
                                parent.expect = Expect::ObjectColon;
                            } else {
                                self.add(task, Kind::String, span)?;
                            }
                            return Ok(ParseStep::Consumed);
                        }
                        b'\\' => Escape::Slash,
                        0..=0x1f => return Ok(ParseStep::Invalid),
                        _ => {
                            self.tree.append(task, &[b])?;
                            Escape::Normal
                        }
                    },
                    Escape::Slash => match b {
                        b'"' | b'\\' | b'/' => {
                            self.tree.append(task, &[b])?;
                            Escape::Normal
                        }
                        b'b' => {
                            self.tree.append(task, &[8])?;
                            Escape::Normal
                        }
                        b'f' => {
                            self.tree.append(task, &[12])?;
                            Escape::Normal
                        }
                        b'n' => {
                            self.tree.append(task, b"\n")?;
                            Escape::Normal
                        }
                        b'r' => {
                            self.tree.append(task, b"\r")?;
                            Escape::Normal
                        }
                        b't' => {
                            self.tree.append(task, b"\t")?;
                            Escape::Normal
                        }
                        b'u' => Escape::Hex {
                            value: 0,
                            count: 0,
                            high: None,
                        },
                        _ => return Ok(ParseStep::Invalid),
                    },
                    Escape::Hex { value, count, high } => {
                        let digit = match b {
                            b'0'..=b'9' => b - b'0',
                            b'a'..=b'f' => b - b'a' + 10,
                            b'A'..=b'F' => b - b'A' + 10,
                            _ => return Ok(ParseStep::Invalid),
                        };
                        let value = (value << 4) | u16::from(digit);
                        if count < 3 {
                            Escape::Hex {
                                value,
                                count: count + 1,
                                high,
                            }
                        } else if let Some(high) = high {
                            if !(0xdc00..=0xdfff).contains(&value) {
                                return Ok(ParseStep::Invalid);
                            }
                            let scalar = 0x10000
                                + ((u32::from(high) - 0xd800) << 10)
                                + (u32::from(value) - 0xdc00);
                            let mut encoded = [0u8; 4];
                            self.tree.append(
                                task,
                                char::from_u32(scalar)
                                    .expect("paired surrogate")
                                    .encode_utf8(&mut encoded)
                                    .as_bytes(),
                            )?;
                            Escape::Normal
                        } else if (0xd800..=0xdbff).contains(&value) {
                            Escape::LowSlash(value)
                        } else if (0xdc00..=0xdfff).contains(&value) {
                            return Ok(ParseStep::Invalid);
                        } else {
                            let mut encoded = [0u8; 4];
                            self.tree.append(
                                task,
                                char::from_u32(u32::from(value))
                                    .expect("non surrogate")
                                    .encode_utf8(&mut encoded)
                                    .as_bytes(),
                            )?;
                            Escape::Normal
                        }
                    }
                    Escape::LowSlash(high) => {
                        if b != b'\\' {
                            return Ok(ParseStep::Invalid);
                        }
                        Escape::LowU(high)
                    }
                    Escape::LowU(high) => {
                        if b != b'u' {
                            return Ok(ParseStep::Invalid);
                        }
                        Escape::Hex {
                            value: 0,
                            count: 0,
                            high: Some(high),
                        }
                    }
                };
                self.token = Token::String {
                    start,
                    key,
                    escape: next,
                };
                return Ok(ParseStep::Consumed);
            }
            Token::None => {}
        }
        if matches!(byte, Some(b' ' | b'\n' | b'\r' | b'\t')) {
            return Ok(ParseStep::Consumed);
        }
        let expect = self.frames.last().map(|f| f.expect);
        match expect {
            Some(Expect::ArrayAfter) => match byte {
                Some(b',') => {
                    self.frames.last_mut().unwrap().expect = Expect::ArrayValue;
                    return Ok(ParseStep::Consumed);
                }
                Some(b']') => {
                    self.frames.pop();
                    return Ok(ParseStep::Consumed);
                }
                _ => return Ok(ParseStep::Invalid),
            },
            Some(Expect::ObjectAfter) => match byte {
                Some(b',') => {
                    self.frames.last_mut().unwrap().expect = Expect::ObjectKey;
                    return Ok(ParseStep::Consumed);
                }
                Some(b'}') => {
                    self.frames.pop();
                    return Ok(ParseStep::Consumed);
                }
                _ => return Ok(ParseStep::Invalid),
            },
            Some(Expect::ObjectColon) => {
                if byte != Some(b':') {
                    return Ok(ParseStep::Invalid);
                }
                self.frames.last_mut().unwrap().expect = Expect::ObjectValue;
                return Ok(ParseStep::Consumed);
            }
            Some(Expect::ObjectFirst | Expect::ObjectKey) => {
                if matches!(expect, Some(Expect::ObjectFirst)) && byte == Some(b'}') {
                    self.frames.pop();
                    return Ok(ParseStep::Consumed);
                }
                if byte != Some(b'"') {
                    return Ok(ParseStep::Invalid);
                }
                self.token = Token::String {
                    start: self.tree.bytes.len(),
                    key: true,
                    escape: Escape::Normal,
                };
                return Ok(ParseStep::Consumed);
            }
            Some(Expect::ArrayFirst) if byte == Some(b']') => {
                self.frames.pop();
                return Ok(ParseStep::Consumed);
            }
            None if self.root => {
                return Ok(if byte.is_none() {
                    ParseStep::Valid
                } else {
                    ParseStep::Invalid
                });
            }
            _ => {}
        }
        let Some(b) = byte else {
            return Ok(ParseStep::Invalid);
        };
        match b {
            b'{' | b'[' => {
                // serde_json's default remaining_depth=128 rejects entering container 128.
                if self.frames.len() == 127 {
                    return Ok(ParseStep::Invalid);
                }
                self.frames
                    .try_reserve(1)
                    .map_err(|_| task.allocation_error())?;
                let node = self.add(
                    task,
                    if b == b'{' { Kind::Object } else { Kind::Array },
                    Span::default(),
                )?;
                self.frames.push(Frame {
                    node,
                    last: NONE,
                    expect: if b == b'{' {
                        Expect::ObjectFirst
                    } else {
                        Expect::ArrayFirst
                    },
                    key: Span::default(),
                });
                Ok(ParseStep::Consumed)
            }
            b'"' => {
                self.token = Token::String {
                    start: self.tree.bytes.len(),
                    key: false,
                    escape: Escape::Normal,
                };
                Ok(ParseStep::Consumed)
            }
            b'n' => {
                self.token = Token::Literal {
                    expected: b"null",
                    at: 1,
                    kind: Kind::Null,
                };
                Ok(ParseStep::Consumed)
            }
            b't' => {
                self.token = Token::Literal {
                    expected: b"true",
                    at: 1,
                    kind: Kind::Bool(true),
                };
                Ok(ParseStep::Consumed)
            }
            b'f' => {
                self.token = Token::Literal {
                    expected: b"false",
                    at: 1,
                    kind: Kind::Bool(false),
                };
                Ok(ParseStep::Consumed)
            }
            b'-' | b'0'..=b'9' => {
                self.token = Token::Number(NumberCursor::new());
                Ok(ParseStep::Progress)
            }
            _ => Ok(ParseStep::Invalid),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{assert_refusal, limited_task, occupy_remaining, release_occupation};
    use super::*;
    #[test]
    fn json_in_pair_parser_frame_first_and_growth_refusal() {
        for grow in [false, true] {
            let (tracker, task) = limited_task();
            let mut parser = Parser::new(&task);
            if grow {
                assert!(matches!(
                    parser.step(&task, Some(b'[')).unwrap(),
                    ParseStep::Consumed
                ));
                while parser.frames.len() < parser.frames.capacity() {
                    assert!(matches!(
                        parser.step(&task, Some(b'[')).unwrap(),
                        ParseStep::Consumed
                    ));
                }
            }
            let capacity = parser.frames.capacity();
            let len = parser.frames.len();
            let nodes = parser.tree.nodes.len();
            let (retained, occupied) = occupy_remaining(&tracker);
            assert_refusal(parser.step(&task, Some(b'[')));
            assert_eq!(parser.frames.capacity(), capacity);
            assert_eq!(parser.frames.len(), len);
            assert_eq!(parser.tree.nodes.len(), nodes);
            release_occupation(&tracker, retained, occupied);
            drop(parser);
            assert_eq!(tracker.current(), 0);
        }
        // A container turn can allocate both a frame and a node. Admit its
        // frame in advance, then independently refuse that turn's node.
        let (tracker, task) = limited_task();
        let mut parser = Parser::new(&task);
        parser.frames.try_reserve(1).unwrap();
        let capacity = parser.frames.capacity();
        let (retained, occupied) = occupy_remaining(&tracker);
        assert_refusal(parser.step(&task, Some(b'[')));
        assert_eq!(parser.frames.capacity(), capacity);
        assert!(parser.frames.is_empty());
        assert!(parser.tree.nodes.is_empty());
        release_occupation(&tracker, retained, occupied);
        drop(parser);
        assert_eq!(tracker.current(), 0);
    }
}
