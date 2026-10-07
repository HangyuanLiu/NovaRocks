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
//! Object lookup and byte equality are resumable, including duplicate last-key-wins.
use super::tree::{Kind, NONE, Span, Tree};
use super::{JsonPairError, JsonPairTask};
use crate::exec::expr::agg::AggregateVec;

#[derive(Clone, Copy)]
enum Frame {
    Node(usize, usize),
    Bytes {
        lhs: Span,
        rhs: Span,
        at: usize,
    },
    Array(usize, usize),
    Object {
        lhs_first: usize,
        rhs_first: usize,
        candidate: usize,
        scan: usize,
        hit: usize,
        at: usize,
        phase: u8,
    },
}
pub(super) enum CompareStep {
    Progress,
    Equal,
    Different,
}
pub(super) struct CompareCursor {
    frames: AggregateVec<Frame>,
}
impl CompareCursor {
    pub(super) fn new(task: &JsonPairTask) -> Self {
        Self {
            frames: AggregateVec::new_in(task.allocator().clone()),
        }
    }
    pub(super) fn start(&mut self, task: &JsonPairTask) -> Result<(), JsonPairError> {
        self.push(task, Frame::Node(0, 0))
    }
    fn push(&mut self, task: &JsonPairTask, frame: Frame) -> Result<(), JsonPairError> {
        self.frames
            .try_reserve(1)
            .map_err(|_| task.allocation_error())?;
        self.frames.push(frame);
        Ok(())
    }
    pub(super) fn step(
        &mut self,
        task: &JsonPairTask,
        lhs: &Tree,
        rhs: &Tree,
    ) -> Result<CompareStep, JsonPairError> {
        let Some(frame) = self.frames.pop() else {
            return Ok(CompareStep::Equal);
        };
        match frame {
            Frame::Node(l, r) => {
                let a = lhs.nodes[l];
                let b = rhs.nodes[r];
                if a.kind != b.kind {
                    return Ok(CompareStep::Different);
                }
                match a.kind {
                    Kind::String => self.push(
                        task,
                        Frame::Bytes {
                            lhs: a.text,
                            rhs: b.text,
                            at: 0,
                        },
                    )?,
                    Kind::Array => self.push(task, Frame::Array(a.first, b.first))?,
                    Kind::Object => self.push(
                        task,
                        Frame::Object {
                            lhs_first: a.first,
                            rhs_first: b.first,
                            candidate: a.first,
                            scan: if a.first == NONE {
                                NONE
                            } else {
                                lhs.nodes[a.first].next
                            },
                            hit: NONE,
                            at: 0,
                            phase: 0,
                        },
                    )?,
                    _ => {}
                }
            }
            Frame::Bytes { lhs: a, rhs: b, at } => {
                if a.len != b.len {
                    return Ok(CompareStep::Different);
                }
                if at < a.len {
                    if lhs.bytes[a.start + at] != rhs.bytes[b.start + at] {
                        return Ok(CompareStep::Different);
                    }
                    self.push(
                        task,
                        Frame::Bytes {
                            lhs: a,
                            rhs: b,
                            at: at + 1,
                        },
                    )?;
                }
            }
            Frame::Array(a, b) => {
                if (a == NONE) != (b == NONE) {
                    return Ok(CompareStep::Different);
                }
                if a != NONE {
                    self.push(task, Frame::Array(lhs.nodes[a].next, rhs.nodes[b].next))?;
                    self.push(task, Frame::Node(a, b))?;
                }
            }
            Frame::Object {
                lhs_first,
                rhs_first,
                mut candidate,
                mut scan,
                mut hit,
                mut at,
                mut phase,
            } => {
                if candidate == NONE {
                    if phase == 2 {
                        return Ok(CompareStep::Progress);
                    }
                    phase = 2;
                    candidate = rhs_first;
                    scan = lhs_first;
                    at = 0;
                }
                if candidate == NONE {
                    return Ok(CompareStep::Progress);
                }
                if scan == NONE {
                    if phase == 0 {
                        phase = 1;
                        scan = rhs_first;
                        hit = NONE;
                        at = 0;
                    } else if phase == 1 {
                        if hit == NONE {
                            return Ok(CompareStep::Different);
                        }
                        let value = Frame::Node(candidate, hit);
                        candidate = lhs.nodes[candidate].next;
                        phase = 0;
                        scan = if candidate == NONE {
                            NONE
                        } else {
                            lhs.nodes[candidate].next
                        };
                        hit = NONE;
                        at = 0;
                        self.push(
                            task,
                            Frame::Object {
                                lhs_first,
                                rhs_first,
                                candidate,
                                scan,
                                hit,
                                at,
                                phase,
                            },
                        )?;
                        self.push(task, value)?;
                        return Ok(CompareStep::Progress);
                    } else {
                        return Ok(CompareStep::Different);
                    }
                } else {
                    let (a, b, ab, bb) = if phase == 0 {
                        (
                            lhs.nodes[candidate].key,
                            lhs.nodes[scan].key,
                            &lhs.bytes,
                            &lhs.bytes,
                        )
                    } else if phase == 1 {
                        (
                            lhs.nodes[candidate].key,
                            rhs.nodes[scan].key,
                            &lhs.bytes,
                            &rhs.bytes,
                        )
                    } else {
                        (
                            rhs.nodes[candidate].key,
                            lhs.nodes[scan].key,
                            &rhs.bytes,
                            &lhs.bytes,
                        )
                    };
                    if a.len == b.len && at < a.len && ab[a.start + at] == bb[b.start + at] {
                        at += 1;
                    } else {
                        let equal = a.len == b.len && at == a.len;
                        at = 0;
                        if phase == 0 {
                            if equal {
                                candidate = lhs.nodes[candidate].next;
                                scan = if candidate == NONE {
                                    NONE
                                } else {
                                    lhs.nodes[candidate].next
                                };
                            } else {
                                scan = lhs.nodes[scan].next;
                            }
                        } else if phase == 1 {
                            if equal {
                                hit = scan;
                            }
                            scan = rhs.nodes[scan].next;
                        } else if equal {
                            candidate = rhs.nodes[candidate].next;
                            scan = lhs_first;
                        } else {
                            scan = lhs.nodes[scan].next;
                        }
                    }
                }
                self.push(
                    task,
                    Frame::Object {
                        lhs_first,
                        rhs_first,
                        candidate,
                        scan,
                        hit,
                        at,
                        phase,
                    },
                )?;
            }
        }
        Ok(CompareStep::Progress)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{assert_refusal, limited_task, occupy_remaining, release_occupation};
    use super::*;
    #[test]
    fn json_in_pair_comparison_frame_first_and_growth_refusal() {
        for grow in [false, true] {
            let (tracker, task) = limited_task();
            let mut cursor = CompareCursor::new(&task);
            if grow {
                cursor.start(&task).unwrap();
                while cursor.frames.len() < cursor.frames.capacity() {
                    cursor.push(&task, Frame::Node(0, 0)).unwrap();
                }
            }
            let capacity = cursor.frames.capacity();
            let len = cursor.frames.len();
            let (retained, occupied) = occupy_remaining(&tracker);
            assert_refusal(cursor.push(&task, Frame::Node(0, 0)));
            assert_eq!(cursor.frames.capacity(), capacity);
            assert_eq!(cursor.frames.len(), len);
            release_occupation(&tracker, retained, occupied);
            drop(cursor);
            assert_eq!(tracker.current(), 0);
        }
    }
}
