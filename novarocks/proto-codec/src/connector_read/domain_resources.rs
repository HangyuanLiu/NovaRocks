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

//! Resource inventory for the original Domain value/range author. Facts are
//! conservative requests and opaque work, not retained capacity or a host grant.
//! No column identity, value grammar, semantic cap or control scope is added.

use super::predicate::{decode_domain, encode_domain};
use crate::{FieldPath, FieldPathSegment, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::{ConnectorValue, Domain, Range};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::{alloc::Layout, fmt};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DomainResourceFacts {
    pub range_count: usize,
    pub scalar_bytes: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum DomainCodecError {
    Control(CompileControlError),
    Protocol(ProtocolError),
    SourceModel(&'static str),
}
impl fmt::Display for DomainCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Protocol(e) => e.fmt(f),
            Self::SourceModel(m) => f.write_str(m),
        }
    }
}
impl std::error::Error for DomainCodecError {}
impl From<CompileControlError> for DomainCodecError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<ProtocolError> for DomainCodecError {
    fn from(e: ProtocolError) -> Self {
        match e.kind() {
            ProtocolErrorKind::CompileControl(c) => Self::Control(c),
            _ => Self::Protocol(e),
        }
    }
}
fn add(a: usize, b: usize) -> Result<usize, DomainCodecError> {
    a.checked_add(b).ok_or(DomainCodecError::Control(
        CompileControlError::ResourceExhausted,
    ))
}
fn mul(a: usize, b: usize) -> Result<usize, DomainCodecError> {
    a.checked_mul(b).ok_or(DomainCodecError::Control(
        CompileControlError::ResourceExhausted,
    ))
}
fn layout<T>(n: usize) -> Result<Layout, DomainCodecError> {
    Layout::array::<T>(n)
        .map_err(|_| DomainCodecError::Control(CompileControlError::ResourceExhausted))
}
impl DomainResourceFacts {
    fn request_layout(&mut self, layout: Layout, times: usize) -> Result<(), DomainCodecError> {
        if layout.size() == 0 || times == 0 {
            return Ok(());
        }
        self.allocation_requests_upper_bound = add(self.allocation_requests_upper_bound, times)?;
        let bytes = mul(layout.size(), times)?;
        self.allocation_request_bytes_upper_bound =
            add(self.allocation_request_bytes_upper_bound, bytes)?;
        self.work(add(mul(bytes, 8)?, mul(times, 128)?)?)
    }
    fn request<T>(&mut self, n: usize, times: usize) -> Result<(), DomainCodecError> {
        self.request_layout(layout::<T>(n)?, times)
    }
    fn work(&mut self, n: usize) -> Result<(), DomainCodecError> {
        self.cumulative_work_upper_bound = add(self.cumulative_work_upper_bound, n)?;
        Ok(())
    }
    fn scalar(&mut self, n: usize) -> Result<(), DomainCodecError> {
        self.scalar_bytes = add(self.scalar_bytes, n)?;
        self.work(n)
    }
}
fn encode_scalar(
    value: &ConnectorValue,
    f: &mut DomainResourceFacts,
) -> Result<(), DomainCodecError> {
    let bytes = match value {
        ConnectorValue::Decimal { .. } | ConnectorValue::Uuid(_) => 16,
        ConnectorValue::Varchar(v) => v.len(),
        ConnectorValue::Varbinary(v) | ConnectorValue::Fixed(v) => v.len(),
        _ => 0,
    };
    f.scalar(bytes)?;
    f.request::<u8>(bytes, 1)
}
/// The original encoder owns one Range Vec and each actual scalar byte buffer.
/// Its optional scalar headers are inline, so they are not separate requests.
pub fn domain_encode_resource_facts(
    domain: &Domain,
) -> Result<DomainResourceFacts, DomainCodecError> {
    encode_facts_core(domain, &mut |_| Ok(()), &mut || Ok(()))
}
/// Count through the same numerical author, admitting the known initial Vec
/// and each real captured scalar prefix before its completed observation.
pub fn domain_encode_resource_facts_admitted(
    domain: &Domain,
    admit: &mut impl FnMut(&DomainResourceFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DomainResourceFacts, DomainCodecError> {
    encode_facts_core(domain, admit, &mut || work.step())
}
fn encode_facts_core(
    domain: &Domain,
    admit: &mut impl FnMut(&DomainResourceFacts) -> Result<(), CompileControlError>,
    observe: &mut impl FnMut() -> Result<(), CompileControlError>,
) -> Result<DomainResourceFacts, DomainCodecError> {
    let ranges = domain.values().ranges();
    let mut f = DomainResourceFacts {
        range_count: ranges.len(),
        cumulative_work_upper_bound: 64,
        ..Default::default()
    };
    f.request::<dto::Range>(ranges.len(), 1)?;
    f.work(mul(ranges.len(), 64)?)?;
    admit(&f)?;
    observe()?;
    for range in ranges {
        for bound in [range.low(), range.high()] {
            if let Some(value) = bound.value() {
                encode_scalar(value, &mut f)?;
            }
            admit(&f)?;
            observe()?;
        }
    }
    Ok(f)
}

fn decode_scalar(raw: &dto::Value, f: &mut DomainResourceFacts) -> Result<bool, DomainCodecError> {
    use dto::value::Value;
    let (bytes, arc, nan) = match raw.value.as_ref() {
        Some(Value::Varchar(v)) => (v.len(), true, false),
        Some(Value::Varbinary(v) | Value::Fixed(v)) => (v.len(), true, false),
        Some(Value::Decimal(v)) => (v.unscaled.len(), false, false),
        Some(Value::Uuid(v)) => (v.len(), false, false),
        Some(Value::Real(v)) => (0, false, v.is_nan()),
        Some(Value::DoubleValue(v)) => (0, false, v.is_nan()),
        _ => (0, false, false),
    };
    f.scalar(bytes)?;
    if arc {
        use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
        let layout = arc_layout(layout::<u8>(bytes)?).map_err(|e| match e {
            LayoutResourceError::SourceModel => {
                DomainCodecError::SourceModel("Domain Arc source profile is unsupported")
            }
            _ => DomainCodecError::Control(CompileControlError::ResourceExhausted),
        })?;
        f.request_layout(layout, 1)?;
    }
    Ok(nan)
}
/// The original value-type decoder receives one fresh static root. Its longest
/// failing branch evaluates clone().field(): temporary clone, field clone and
/// growth, in addition to that root. One original short diagnostic is possible.
/// No type metadata or parameter grammar is interpreted by this inventory.
pub fn value_type_decode_resource_facts() -> Result<DomainResourceFacts, DomainCodecError> {
    let mut facts = DomainResourceFacts {
        cumulative_work_upper_bound: 64,
        ..Default::default()
    };
    facts.request::<FieldPathSegment>(16, 4)?;
    facts.request::<u8>(256, 1)?;
    Ok(facts)
}
/// The original scalar decoder uses static-root paths only. Decimal evaluates
/// exact_bytes(clone().field()) before a possible precision/scale error with a
/// second clone().field(): root + two sets of three requests. Other branches
/// use no more paths. The byte-arm Arc and possible final type-mismatch String
/// coexist, so both requests are included, using the sole Arc layout author.
pub fn value_decode_resource_facts(
    raw: &dto::Value,
) -> Result<DomainResourceFacts, DomainCodecError> {
    let mut facts = DomainResourceFacts {
        cumulative_work_upper_bound: 64,
        ..Default::default()
    };
    facts.request::<FieldPathSegment>(16, 7)?;
    facts.request::<u8>(256, 1)?;
    decode_scalar(raw, &mut facts)?;
    Ok(facts)
}

/// This author uses the current original decoder/normalizer call inventory:
///
/// * A fresh static root has no MapKey Strings. Root/prologue/final ordinary
///   paths require at most twelve clone/grow Vec requests; each range requires
///   at most twenty-four (range path, low/high paths and scalar error paths).
///   There are at most eight segments, and a push after clone requests at most
///   sixteen. These count clone and growth as distinct real requests.
/// * decode_value_set owns its input Range Vec; normalize owns a merged Vec.
///   Rust 1.92 stable sort requests at most max(N,48) Range slots, once. This
///   remains conservative when the original insertion/stack path requests none.
/// * The original only dynamically retained scalar buffers are three Arc arms.
///   Decimal/Uuid are inline; their wire lengths still contribute real work.
/// * Ordinary diagnostics are bounded original strings. NaN comparator errors
///   can occur repeatedly before normalize returns, so they are cumulatively
///   prefunded rather than treating only the published error as an allocation.
///
/// Facts do not validate or alter types, ranges, ordinary errors or their order.
pub fn domain_decode_resource_facts(
    raw: &dto::Domain,
) -> Result<DomainResourceFacts, DomainCodecError> {
    decode_facts_core(raw, &mut |_| Ok(()), &mut || Ok(()))
}
/// Admit original path/range/sort requests from O(1) lengths before traversal;
/// each actual optional bound capture then admits its scalar/Arc/work prefix.
pub fn domain_decode_resource_facts_admitted(
    raw: &dto::Domain,
    admit: &mut impl FnMut(&DomainResourceFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DomainResourceFacts, DomainCodecError> {
    decode_facts_core(raw, admit, &mut || work.step())
}
fn decode_facts_core(
    raw: &dto::Domain,
    admit: &mut impl FnMut(&DomainResourceFacts) -> Result<(), CompileControlError>,
    observe: &mut impl FnMut() -> Result<(), CompileControlError>,
) -> Result<DomainResourceFacts, DomainCodecError> {
    if !novarocks_type_contract::owned_resources::profile::LOCKED_TOOLCHAIN {
        return Err(DomainCodecError::SourceModel(
            "Domain sort source profile is unsupported",
        ));
    }
    let ranges = raw.values.as_ref().map_or(&[][..], |v| v.ranges.as_slice());
    let n = ranges.len();
    let mut f = DomainResourceFacts {
        range_count: n,
        cumulative_work_upper_bound: 128,
        ..Default::default()
    };
    f.request::<FieldPathSegment>(16, add(12, mul(n, 24)?)?)?;
    f.request::<Range>(n, 2)?;
    if n > 1 {
        f.request::<Range>(n.max(48), 1)?;
    }
    // The same original comparison upper bound is incremented by each newly
    // captured scalar byte, so it is not delayed until after that observation.
    let comparisons = mul(mul(n, add(n, 1)?)?, 8)?;
    f.work(mul(comparisons, 64)?)?;
    f.request::<u8>(256, 4)?;
    admit(&f)?;
    observe()?;
    let mut nan = false;
    for range in ranges {
        for bound in [&range.low, &range.high] {
            if let Some(value) = bound.as_ref().and_then(|b| b.value.as_ref()) {
                let bytes_before = f.scalar_bytes;
                let new_nan = decode_scalar(value, &mut f)?;
                f.work(mul(comparisons, mul(f.scalar_bytes - bytes_before, 2)?)?)?;
                if new_nan && !nan {
                    f.request::<u8>(256, comparisons)?;
                }
                nan |= new_nan;
            }
            admit(&f)?;
            observe()?;
        }
    }
    Ok(f)
}

fn completed<T>(
    result: Result<T, DomainCodecError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<T, DomainCodecError> {
    if matches!(&result, Err(DomainCodecError::Control(_))) {
        return result;
    }
    work.step()?;
    work.flush()?;
    result
}
/// Caller-work port. Admit known facts before any observation or original
/// allocation; the underlying value/range author remains the only grammar.
pub fn encode_domain_observed(
    domain: &Domain,
    admit: &mut impl FnMut(&DomainResourceFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(dto::Domain, DomainResourceFacts), DomainCodecError> {
    let facts = domain_encode_resource_facts_admitted(domain, admit, work)?;
    work.flush()?;
    let output = encode_domain(domain);
    let output = completed(Ok(output), work)?;
    Ok((output, facts))
}
/// The new port authors its own static-root path after parent admission. It
/// does not clone an arbitrary caller MapKey path or create a control scope.
pub fn decode_domain_observed(
    raw: &dto::Domain,
    root: &'static str,
    admit: &mut impl FnMut(&DomainResourceFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Domain, DomainResourceFacts), DomainCodecError> {
    let facts = domain_decode_resource_facts_admitted(raw, admit, work)?;
    work.flush()?;
    let path = FieldPath::root(root);
    work.step()?;
    work.flush()?;
    let output = decode_domain(raw, path).map_err(DomainCodecError::from);
    let output = completed(output, work)?;
    Ok((output, facts))
}

#[cfg(test)]
mod tests;
