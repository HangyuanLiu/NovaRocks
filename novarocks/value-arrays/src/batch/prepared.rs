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

use crate::{
    ValueError, ValueFailure, ValueViolation,
    arrow_raw::BoundView,
    decode::{DecodedBytes, RowMapping},
    reachability::SelectionSet,
};
use arrow_array::{Array, Int32Array};
use std::convert::Infallible;

/// Borrows a decoded owner and its admitted logical selection. No conversion
/// cache borrow or temporary descriptor allocation escapes preparation.
pub struct PreparedBytes<'a> {
    decoded: &'a DecodedBytes,
    rows: &'a SelectionSet,
    view: BoundView<'a>,
    count: usize,
}
impl<'a> PreparedBytes<'a> {
    pub fn new(decoded: &'a DecodedBytes, rows: &'a SelectionSet) -> Result<Self, ValueError> {
        Self::new_observed(decoded, rows, &mut |_| Ok::<_, Infallible>(()))
            .map_err(ValueFailure::into_error)
    }
    pub fn new_observed<E, W>(
        decoded: &'a DecodedBytes,
        rows: &'a SelectionSet,
        work: &mut W,
    ) -> Result<Self, ValueFailure<E>>
    where
        W: FnMut(usize) -> Result<(), E>,
    {
        if rows.len() != decoded.len() {
            return Err(ValueError::Invariant(ValueViolation::Bounds).into());
        }
        let view = BoundView::bind(decoded.bottom().as_ref())?;
        work(0).map_err(ValueFailure::Caller)?;
        let mut control = super::RowWork::new(work);
        let mut count = 0;
        let mut cursor = rows.cursor();
        while let Some(row) = cursor.next_observed(&mut |n| control.observe_metadata(n))? {
            control.row()?;
            if !decoded.readable_rows().contains(row) {
                return Err(ValueError::Invariant(ValueViolation::UndefinedRow).into());
            }
            if let Some(bottom) = decoded.mapping().mapped_row(row)? {
                if !decoded.bottom_coverage().contains(bottom) {
                    return Err(ValueError::Invariant(ValueViolation::UndefinedRow).into());
                }
                // Check only selected values; placeholders outside coverage are
                // never dereferenced, even when their descriptor is present.
                view.value(bottom)?;
            }
            count += 1;
        }
        control.flush()?;
        Ok(Self {
            decoded,
            rows,
            view,
            count,
        })
    }
    /// Number of selected logical occurrences in ascending logical-row order.
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn is_utf8(&self) -> bool {
        self.decoded.is_utf8()
    }
    pub fn selection(&self) -> &SelectionSet {
        self.rows
    }
    pub(crate) fn mapping(&self) -> &RowMapping {
        self.decoded.mapping()
    }
    pub(crate) fn view(&self) -> BoundView<'a> {
        self.view
    }
    pub(crate) fn check_row(&self, row: usize) -> Result<(), ValueError> {
        if self.rows.contains(row) {
            Ok(())
        } else {
            Err(ValueError::Invariant(ValueViolation::UndefinedRow))
        }
    }
}
pub(crate) trait Mapping {
    fn index(&self, row: usize) -> Option<usize>;
}
pub(crate) struct Identity;
impl Mapping for Identity {
    fn index(&self, row: usize) -> Option<usize> {
        Some(row)
    }
}
pub(crate) struct Constant(pub(crate) usize);
impl Mapping for Constant {
    fn index(&self, _: usize) -> Option<usize> {
        Some(self.0)
    }
}
pub(crate) struct Dictionary<'a>(pub(crate) &'a Int32Array);
impl Mapping for Dictionary<'_> {
    fn index(&self, row: usize) -> Option<usize> {
        // All selected mapping values were checked during preparation; NULL
        // keys never expose their physical integer.
        (!self.0.is_null(row)).then(|| self.0.value(row) as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ByteLayout, access::ByteSource, decode::ValueConversionContext, reachability::ReachableRows,
    };
    use arrow_array::{ArrayRef, StringArray};
    use std::sync::Arc;
    #[test]
    fn prepared_selection_keeps_coverage_and_owner_after_cache_clear() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None, Some("unread")]));
        let parsed = ByteSource::parse(&source, ByteLayout::Utf8, None).unwrap();
        let mut context = ValueConversionContext::default();
        let decoded = context
            .decode(&parsed, &ReachableRows::indices(3, &[0, 1]))
            .unwrap();
        context.clear();
        let mut rows = SelectionSet::try_new(3).unwrap();
        rows.insert(0).unwrap();
        rows.insert(1).unwrap();
        let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared.view().value(0).unwrap(), Some(b"x".as_slice()));
        assert_eq!(prepared.view().value(1).unwrap(), None);
        rows.insert(2).unwrap();
        assert!(matches!(
            PreparedBytes::new(&decoded, &rows),
            Err(ValueError::Invariant(ValueViolation::UndefinedRow))
        ));
    }
    #[test]
    fn prepared_reads_do_not_rebind_the_carrier_per_row() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![
            Some(
                "long external bytes to inspect"
            );
            4096
        ]));
        let parsed = ByteSource::parse(&source, ByteLayout::Utf8, None).unwrap();
        let mut context = ValueConversionContext::default();
        let decoded = context
            .decode(&parsed, &ReachableRows::all(source.len()))
            .unwrap();
        let rows = ReachableRows::all(source.len())
            .materialize(&mut |_| Ok::<_, Infallible>(()))
            .unwrap();
        let before = crate::arrow_raw::concrete_bindings();
        let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
        let bound = crate::arrow_raw::concrete_bindings();
        assert_eq!(bound - before, 1);
        for row in prepared.selection().iter() {
            assert!(prepared.view().value(row).unwrap().is_some());
        }
        assert_eq!(crate::arrow_raw::concrete_bindings(), bound);
    }

    #[test]
    fn preparation_reports_original_control_failure_even_for_null_rows() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 4096]));
        let parsed = ByteSource::parse(&source, ByteLayout::Utf8, None).unwrap();
        let decoded = ValueConversionContext::default()
            .decode(&parsed, &ReachableRows::all(source.len()))
            .unwrap();
        let rows = ReachableRows::all(source.len())
            .materialize(&mut |_| Ok::<_, Infallible>(()))
            .unwrap();
        let failure = PreparedBytes::new_observed(&decoded, &rows, &mut |units| {
            if units > 0 { Err("stop") } else { Ok(()) }
        });
        assert!(matches!(failure, Err(ValueFailure::Caller("stop"))));
    }
    #[test]
    fn empty_and_sparse_large_selection_observe_bitmap_work_before_rows() {
        let source: ArrayRef = Arc::new(arrow_array::StringViewArray::new_null(200_000));
        let parsed = ByteSource::parse(&source, ByteLayout::Utf8View, None).unwrap();
        let mut selected = SelectionSet::try_new(200_000).unwrap();
        let decoded = ValueConversionContext::default()
            .decode(&parsed, &ReachableRows::all(200_000))
            .unwrap();
        for sparse in [false, true] {
            if sparse {
                selected.insert(199_999).unwrap();
            }
            let result = PreparedBytes::new_observed(&decoded, &selected, &mut |units| {
                if units > 0 {
                    Err("bitmap stop")
                } else {
                    Ok(())
                }
            });
            assert!(matches!(result, Err(ValueFailure::Caller("bitmap stop"))));
        }
    }
}
