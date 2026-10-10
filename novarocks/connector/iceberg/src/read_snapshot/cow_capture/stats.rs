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

// Private read_snapshot child. Borrowed current DataFile only; no capture/clone.
use super::geometry::*;
use crate::iceberg::spec::{DataFile, Datum, PrimitiveLiteral};
use crate::scan_model::IcebergColumnStats;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug)]
pub(crate) struct StatsUpper {
    pub constructor_upper: u64,
    pub retained_upper: u64,
}
fn datum_bytes(d: &Datum) -> u64 {
    match d.literal() {
        PrimitiveLiteral::Boolean(_) => 1,
        PrimitiveLiteral::Int(_) | PrimitiveLiteral::Float(_) => 4,
        PrimitiveLiteral::Long(_) | PrimitiveLiteral::Double(_) => 8,
        PrimitiveLiteral::String(v) => v.len() as u64,
        PrimitiveLiteral::Binary(v) => v.len() as u64,
        // Decimal's minimum Vec can retain <=16 bytes before truncate.
        // Invalid sentinels return no buffer; 16 conservatively includes them.
        _ => 16,
    }
}
fn unique_ids<E>(
    df: &DataFile,
    mut f: impl FnMut(i32) -> Result<(), Failure<E>>,
) -> Result<(), Failure<E>> {
    let n = df.null_value_counts();
    let v = df.value_counts();
    let c = df.column_sizes();
    let l = df.lower_bounds();
    let u = df.upper_bounds();
    for &id in n.keys() {
        f(id)?
    }
    for &id in v.keys() {
        if !n.contains_key(&id) {
            f(id)?
        }
    }
    for &id in c.keys() {
        if !n.contains_key(&id) && !v.contains_key(&id) {
            f(id)?
        }
    }
    for &id in l.keys() {
        if !n.contains_key(&id) && !v.contains_key(&id) && !c.contains_key(&id) {
            f(id)?
        }
    }
    for &id in u.keys() {
        if !n.contains_key(&id)
            && !v.contains_key(&id)
            && !c.contains_key(&id)
            && !l.contains_key(&id)
        {
            f(id)?
        }
    }
    Ok(())
}
pub(crate) fn prospective<E>(
    df: &DataFile,
    names: &HashMap<i32, String>,
    active: &mut impl FnMut() -> Result<(), E>,
) -> Result<StatsUpper, Failure<E>> {
    active().map_err(Failure::Original)?;
    let mut hint = 0usize;
    for n in [
        df.null_value_counts().len(),
        df.value_counts().len(),
        df.column_sizes().len(),
        df.lower_bounds().len(),
        df.upper_bounds().len(),
    ] {
        hint = hint.checked_add(n).ok_or(Failure::Overflow)?;
    }
    let mut recognized = 0usize;
    let mut nested = 0u64;
    let mut maximum = 0u64;
    unique_ids(df, |id| {
        active().map_err(Failure::Original)?;
        if let Some(name) = names.get(&id) {
            recognized = recognized.checked_add(1).ok_or(Failure::Overflow)?;
            nested = add(nested, name.len() as u64)?;
            for bound in [df.lower_bounds().get(&id), df.upper_bounds().get(&id)]
                .into_iter()
                .flatten()
            {
                let n = datum_bytes(bound);
                nested = add(nested, n)?;
                maximum = maximum.max(n);
            }
        }
        Ok(())
    })?;
    // Actual original inferred HashSet<i32>, not HashSet<&i32>.
    let scratch = hash_max::<E, i32>(hint)?;
    let map = hash_max::<E, (String, IcebergColumnStats)>(recognized)?;
    let retained = add(map, nested)?;
    // Rehash keeps old+new table allocations concurrently. all_ids remains
    // live while map is built. ByteBuf and Vec output coexist until map closure
    // exits, so retain all final buffer uppers plus ONE largest temporary.
    let upper = add(add(mul(2, scratch)?, mul(2, map)?)?, add(nested, maximum)?)?;
    active().map_err(Failure::Original)?;
    Ok(StatsUpper {
        constructor_upper: upper,
        retained_upper: retained,
    })
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct StatsRetained {
    pub own_upper: u64,
}
pub(crate) fn retained<E>(
    value: &Option<HashMap<String, IcebergColumnStats>>,
) -> Result<StatsRetained, Failure<E>> {
    let Some(map) = value else {
        return Ok(StatsRetained { own_upper: 0 });
    };
    let mut n = hash_actual::<E, (String, IcebergColumnStats)>(map.capacity())?;
    for (key, stats) in map {
        n = add(n, key.capacity() as u64)?;
        for b in [&stats.lower_bound, &stats.upper_bound]
            .into_iter()
            .flatten()
        {
            n = add(n, b.capacity() as u64)?
        }
    }
    Ok(StatsRetained { own_upper: n })
}
// Execute the ORIGINAL column_stats kernel only after the same owner accepts
// complete E + other retained branches + this constructor. E is not supplied
// by this helper and an absent upstream receipt must be refused by `check`.
