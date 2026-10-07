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
use super::number::JsonNumber;
use super::{JsonPairError, JsonPairTask};
use crate::exec::expr::agg::AggregateVec;

pub(super) const NONE: usize = usize::MAX;
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Span {
    pub start: usize,
    pub len: usize,
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Kind {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String,
    Array,
    Object,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct Node {
    pub kind: Kind,
    pub text: Span,
    pub key: Span,
    pub first: usize,
    pub next: usize,
}
pub(super) struct Tree {
    pub nodes: AggregateVec<Node>,
    pub bytes: AggregateVec<u8>,
}
impl Tree {
    pub(super) fn new(task: &JsonPairTask) -> Self {
        Self {
            nodes: AggregateVec::new_in(task.allocator().clone()),
            bytes: AggregateVec::new_in(task.allocator().clone()),
        }
    }
    pub(super) fn node(
        &mut self,
        task: &JsonPairTask,
        kind: Kind,
        text: Span,
        key: Span,
    ) -> Result<usize, JsonPairError> {
        self.nodes
            .try_reserve(1)
            .map_err(|_| task.allocation_error())?;
        let id = self.nodes.len();
        self.nodes.push(Node {
            kind,
            text,
            key,
            first: NONE,
            next: NONE,
        });
        Ok(id)
    }
    pub(super) fn append(
        &mut self,
        task: &JsonPairTask,
        bytes: &[u8],
    ) -> Result<(), JsonPairError> {
        self.bytes
            .try_reserve(bytes.len())
            .map_err(|_| task.allocation_error())?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{assert_refusal, limited_task, occupy_remaining, release_occupation};
    use super::*;
    #[test]
    fn json_in_pair_tree_node_and_decoded_pool_first_and_growth_refusal() {
        for bytes in [false, true] {
            for grow in [false, true] {
                let (tracker, task) = limited_task();
                let mut tree = Tree::new(&task);
                if grow {
                    if bytes {
                        tree.append(&task, b"x").unwrap();
                        while tree.bytes.len() < tree.bytes.capacity() {
                            tree.append(&task, b"x").unwrap();
                        }
                    } else {
                        tree.node(&task, Kind::Null, Span::default(), Span::default())
                            .unwrap();
                        while tree.nodes.len() < tree.nodes.capacity() {
                            tree.node(&task, Kind::Null, Span::default(), Span::default())
                                .unwrap();
                        }
                    }
                }
                let capacity = if bytes {
                    tree.bytes.capacity()
                } else {
                    tree.nodes.capacity()
                };
                let length = if bytes {
                    tree.bytes.len()
                } else {
                    tree.nodes.len()
                };
                let (retained, occupied) = occupy_remaining(&tracker);
                if bytes {
                    assert_refusal(tree.append(&task, b"y"));
                    assert_eq!(tree.bytes.capacity(), capacity);
                    assert_eq!(tree.bytes.len(), length);
                } else {
                    assert_refusal(tree.node(&task, Kind::Null, Span::default(), Span::default()));
                    assert_eq!(tree.nodes.capacity(), capacity);
                    assert_eq!(tree.nodes.len(), length);
                }
                release_occupation(&tracker, retained, occupied);
                drop(tree);
                assert_eq!(tracker.current(), 0);
            }
        }
    }
}
