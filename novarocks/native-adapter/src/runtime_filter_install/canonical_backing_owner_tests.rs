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

use super::*;
use crate::native_type_construction::{NativeFactoryScope, NativeTypeScope};
use novarocks_plan_codec::execution_type_table::decode_execution_type_table_with_factory_in;
use novarocks_type_contract::{
    ValueLayout, ValueSchemaCreationFactory, ValueSchemaCreationScope,
    ValueSchemaScopeBeginFailure, ValueTypeStorageLifetime,
};
use std::{alloc::Layout, cell::RefCell, convert::Infallible};

#[derive(Clone)]
struct ScopeQuote {
    header: Layout,
    requests: Vec<Layout>,
    opaque: Vec<usize>,
}
impl ScopeQuote {
    fn accepted_quote(&self) -> usize {
        self.requests
            .iter()
            .map(Layout::size)
            .chain(self.opaque.iter().copied())
            .try_fold(self.header.size(), usize::checked_add)
            .expect("actual fixture requests fit address space")
    }
}

// Observe the actual factory body; no request or source invoice is invented.
// These fixture traces own no construction scope or returned storage marker.
struct RecordedFactory<'source, 'control> {
    actual: NativeTypeFactory<'source, 'control>,
    quotes: RefCell<Vec<ScopeQuote>>,
}
struct RecordedScope<'borrow> {
    actual: NativeFactoryScope<'borrow>,
    quotes: &'borrow RefCell<Vec<ScopeQuote>>,
    id: usize,
}
impl ValueSchemaCreationScope<NativeTypeConstructionError> for RecordedScope<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.actual.storage_lifetime()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), NativeTypeConstructionError> {
        self.actual.request_layout(layout)?;
        self.quotes.borrow_mut()[self.id].requests.push(layout);
        Ok(())
    }
    fn observe_opaque_upper_bound(
        &mut self,
        bytes: usize,
    ) -> Result<(), NativeTypeConstructionError> {
        self.actual.observe_opaque_upper_bound(bytes)?;
        self.quotes.borrow_mut()[self.id].opaque.push(bytes);
        Ok(())
    }
}
impl ValueSchemaCreationFactory<NativeTypeConstructionError> for RecordedFactory<'_, '_> {
    type Scope<'a>
        = RecordedScope<'a>
    where
        Self: 'a;

    fn begin_scope(&self) -> Result<Self::Scope<'_>, NativeTypeConstructionError> {
        let mut observe = |_: Layout| Ok::<_, Infallible>(());
        self.begin_scope_observed(&mut observe)
            .map_err(|cause| match cause {
                ValueSchemaScopeBeginFailure::Creator(cause) => cause,
                ValueSchemaScopeBeginFailure::Observer(never) => match never {},
            })
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, ValueSchemaScopeBeginFailure<O, NativeTypeConstructionError>> {
        let mut header = None;
        let actual = self.actual.begin_scope_observed(&mut |layout| {
            observe(layout)?;
            header = Some(layout);
            Ok::<_, O>(())
        })?;
        let mut quotes = self.quotes.borrow_mut();
        let id = quotes.len();
        quotes.push(ScopeQuote {
            header: header.expect("actual factory observes its own header before allocation"),
            requests: Vec::new(),
            opaque: Vec::new(),
        });
        Ok(RecordedScope {
            actual,
            quotes: &self.quotes,
            id,
        })
    }
}

#[test]
fn runtime_filter_canonical_late_owner_keeps_timezone_without_layout_cache_or_main() {
    use arrow::datatypes::{DataType, TimeUnit};
    use novarocks_execution::runtime_filter::{
        RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics,
    };
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    let mut independently_quoted_late = None;
    for unrelated_main_bytes in [0, 4096] {
        let declared = FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        );
        let wire = domain(&declared);
        let tracker = MemTracker::new_root("RF canonical independent physical backing");
        tracker.install_limit_once(1024 * 1024).unwrap();
        let stop = ConnectorStopOwner::new();
        let creation = creator(tracker.clone(), &stop);
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let main_layout = Layout::array::<u8>(unrelated_main_bytes).unwrap();
        creation.request_layout(main_layout).unwrap();
        let unrelated_main = vec![23u8; unrelated_main_bytes];
        let factory = RecordedFactory {
            actual: NativeTypeFactory::new(&creation),
            quotes: RefCell::new(Vec::new()),
        };
        let table = wire
            .logical_domain
            .as_ref()
            .unwrap()
            .execution_types
            .as_ref()
            .unwrap();
        let types = decode_execution_type_table_with_factory_in(
            table,
            ExecutionTypeTableLimits::for_validated_root_count(table.value_bindings.len()).unwrap(),
            &factory,
            &mut resources.work,
        )
        .unwrap();
        let source = types.get_observed(0, &mut resources.work).unwrap().unwrap();
        let late_layout = source.typed_binding().layout().clone();
        let source = source
            .clone_observed(|| {
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)
            })
            .unwrap();
        let own = creation.begin_scope().unwrap();
        let mut canonical_requests = Vec::new();
        let contract = RuntimeFilterMembershipSchema::new_value_typed_with_storage_observed::<
            NativeFragmentDecodeError,
        >(
            source,
            RuntimeFilterNullSemantics::NeverMatches,
            own.storage_lifetime(),
            |layout| {
                own.request_layout(layout)?;
                canonical_requests.push(layout);
                Ok(())
            },
            |units| {
                for _ in 0..units {
                    resources
                        .work
                        .step()
                        .map_err(NativeTypeConstructionError::Control)?;
                }
                Ok(())
            },
        )
        .unwrap();
        let canonical = contract.canonical_schema().clone();
        let digest = canonical.digest();
        assert_eq!(canonical.data_type(), declared.data_type());

        // The unique original physical Timestamp body requests its spelling
        // String and timezone Arc only. The logical node additionally requests
        // its own node holder; the cached layout additionally requests its own
        // storage holder. Classify actual geometries, never callback ordinals.
        let timezone_requests = vec![
            Layout::array::<u8>("UTC".len()).unwrap(),
            arc_layout(Layout::array::<u8>("UTC".len()).unwrap()).unwrap(),
        ];
        let cache_holder = ValueLayout::storage_allocation_layout().unwrap();
        assert_ne!(
            cache_holder,
            novarocks_type_contract::ValueTypeRef::storage_allocation_layout().unwrap(),
            "the actual fixture distinguishes its logical node and layout holder geometries"
        );
        let quotes = factory.quotes.borrow().clone();
        let physical = quotes
            .iter()
            .filter(|quote| quote.requests == timezone_requests && quote.opaque.is_empty())
            .collect::<Vec<_>>();
        assert_eq!(
            physical.len(),
            1,
            "actual physical timezone has one separate author"
        );
        let caches = quotes
            .iter()
            .filter(|quote| quote.requests.contains(&cache_holder))
            .collect::<Vec<_>>();
        assert_eq!(caches.len(), 1, "one actual cached Timestamp layout holder");
        let timezone_quote = physical[0].accepted_quote();
        let cache_quote = caches[0].accepted_quote();
        let canonical_quote = canonical_requests
            .iter()
            .map(Layout::size)
            .try_fold(
                NativeTypeScope::reservation_allocation_layout()
                    .unwrap()
                    .size(),
                usize::checked_add,
            )
            .unwrap();
        let expected_late = timezone_quote.checked_add(canonical_quote).unwrap();
        let expected_with_cache = expected_late.checked_add(cache_quote).unwrap();
        assert!(cache_quote >= cache_holder.size());
        assert!(timezone_quote > 0);

        drop(own);
        drop(contract);
        drop(types);
        drop(factory);
        resources.finish().unwrap();
        drop(creation);
        drop(stop);
        drop(wire);
        drop(declared);
        drop(unrelated_main);
        assert_eq!(
            tracker.current(),
            i64::try_from(expected_with_cache).unwrap(),
            "only the independently quoted canonical, timezone and late cache remain"
        );
        drop(late_layout);
        assert_eq!(
            tracker.current(),
            i64::try_from(expected_late).unwrap(),
            "canonical retains timezone backing without logical/layout holder or main fees"
        );
        if let Some(original) = independently_quoted_late {
            assert_eq!(
                expected_late, original,
                "unrelated actual main Vec cannot alter late responsibility"
            );
        } else {
            independently_quoted_late = Some(expected_late);
        }
        assert_eq!(
            canonical.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(canonical.digest(), digest);
        drop(canonical);
        assert_eq!(tracker.current(), 0);
    }
}
