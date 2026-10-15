// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Borrowed display-name propagation with the original immediate-sweep priority.
//! Index/event backings are exact caller-admitted vectors. Names remain borrowed
//! from their frozen writers/results until the actual DTO creator copies them.

use super::{TypeEncodingResources, TypeEncodingResult, sort_ord_observed};
use crate::physical_encode::find_sorted_key;

pub(crate) struct NameIndex<'s, K> {
    entries: Vec<(K, Option<&'s str>)>,
}
impl<'s, K: Copy + Ord> NameIndex<'s, K> {
    pub(crate) fn try_new<H>(
        count: usize,
        keys: impl Iterator<Item = K>,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        let mut entries = resources.vector(count)?;
        for key in keys {
            resources.work.step()?;
            if entries.len() == count {
                return Err("output-name source key count grew after preparation".into());
            }
            entries.push((key, None::<&str>));
        }
        if entries.len() != count {
            return Err("output-name source key count changed during preparation".into());
        }
        // All name positions are still None, so sorting compares only the
        // primitive source key, never unobserved strings.
        sort_ord_observed(&mut entries, &mut resources.work)?;
        for pair in entries.windows(2) {
            resources.work.step()?;
            if pair[0].0 == pair[1].0 {
                return Err("output-name source key repeats".into());
            }
        }
        Ok(Self { entries })
    }
    pub(crate) fn position<H>(
        &self,
        key: &K,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<usize, H> {
        find_sorted_key(&self.entries, key, &mut resources.work)?
            .ok_or_else(|| "output-name link is outside its frozen source index".into())
    }
    pub(crate) fn seed<H>(
        &mut self,
        key: &K,
        name: &'s str,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<(), H> {
        let index = self.position(key, resources)?;
        if self.entries[index].1.is_none() {
            self.entries[index].1 = Some(name);
        }
        Ok(())
    }
    pub(crate) fn get<H>(
        &self,
        key: &K,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Option<&'s str>, H> {
        Ok(find_sorted_key(&self.entries, key, &mut resources.work)?
            .and_then(|index| self.entries[index].1))
    }
}

// Sorted by destination for outgoing adjacency. Edge/ordinal positions are the
// genuine source traversal positions, independent of source key numbering.
pub(crate) type NameLink = (usize, usize, usize, usize);
#[derive(Clone, Copy)]
struct Event<'s> {
    pass: usize,
    edge: usize,
    ordinal: usize,
    source: usize,
    name: &'s str,
}
impl Event<'_> {
    fn priority(&self) -> (usize, usize, usize, usize) {
        (self.pass, self.edge, self.ordinal, self.source)
    }
}

/// A destination published at (pass p, edge j) reaches edge i in pass
/// p + [i <= j]. This strictly increasing event rank exactly reproduces the
/// legacy immediate-in-sweep first-publication law. Settled keys never change;
/// each adjacency link is dispatched once, so the queue needs at most E entries.
pub(crate) fn propagate<H, K: Copy + Ord>(
    index: &mut NameIndex<'_, K>,
    mut links: Vec<NameLink>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<(), H> {
    for (destination, _, _, source) in &links {
        resources.work.step()?;
        if *destination >= index.entries.len() || *source >= index.entries.len() {
            return Err("output-name adjacency is outside its frozen index".into());
        }
    }
    sort_ord_observed(&mut links, &mut resources.work)?;
    let mut queue = resources.vector(links.len())?;
    for (position, (_, name)) in index.entries.iter().enumerate() {
        resources.work.step()?;
        if let Some(name) = *name {
            dispatch(position, name, None, &links, &mut queue, resources)?;
        }
    }
    while let Some(event) = pop(&mut queue, resources)? {
        resources.work.step()?;
        if index.entries[event.source].1.is_some() {
            continue;
        }
        index.entries[event.source].1 = Some(event.name);
        dispatch(
            event.source,
            event.name,
            Some((event.pass, event.edge)),
            &links,
            &mut queue,
            resources,
        )?;
    }
    Ok(())
}
fn dispatch<'s, H>(
    destination: usize,
    name: &'s str,
    time: Option<(usize, usize)>,
    links: &[NameLink],
    queue: &mut Vec<Event<'s>>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<(), H> {
    let (mut low, mut high) = (0, links.len());
    while low < high {
        resources.work.step()?;
        let middle = low + (high - low) / 2;
        if links[middle].0 < destination {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    while low < links.len() && links[low].0 == destination {
        resources.work.step()?;
        let (_, edge, ordinal, source) = links[low];
        let pass = match time {
            None => 0,
            Some((pass, previous)) => pass
                .checked_add(usize::from(edge <= previous))
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?,
        };
        push(
            queue,
            Event {
                pass,
                edge,
                ordinal,
                source,
                name,
            },
            resources,
        )?;
        low += 1;
    }
    Ok(())
}
fn push<'s, H>(
    queue: &mut Vec<Event<'s>>,
    event: Event<'s>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<(), H> {
    resources.work.step()?;
    if queue.len() == queue.capacity() {
        return Err("output-name event count exceeded admitted adjacency backing".into());
    }
    queue.push(event);
    let mut current = queue.len() - 1;
    while current != 0 {
        let parent = (current - 1) / 2;
        resources.work.step()?;
        if queue[parent].priority() <= queue[current].priority() {
            break;
        }
        resources.work.step()?;
        queue.swap(parent, current);
        current = parent;
    }
    Ok(())
}
fn pop<'s, H>(
    queue: &mut Vec<Event<'s>>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<Option<Event<'s>>, H> {
    if queue.is_empty() {
        return Ok(None);
    }
    resources.work.step()?;
    let last = queue.len() - 1;
    queue.swap(0, last);
    let event = queue.pop().expect("nonempty event queue");
    let mut current = 0usize;
    while let Some(left) = current
        .checked_mul(2)
        .and_then(|index| index.checked_add(1))
        .filter(|index| *index < queue.len())
    {
        let right = left + 1;
        let child = if right < queue.len() {
            resources.work.step()?;
            if queue[right].priority() < queue[left].priority() {
                right
            } else {
                left
            }
        } else {
            left
        };
        resources.work.step()?;
        if queue[current].priority() <= queue[child].priority() {
            break;
        }
        resources.work.step()?;
        queue.swap(current, child);
        current = child;
    }
    Ok(Some(event))
}
