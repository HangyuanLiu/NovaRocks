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

use super::*;
use novarocks_type_contract::{
    CompilePhase, FunctionArgumentType, FunctionArgumentTypes, PureCompileControl,
    ValueSchemaCreation, ValueTypeStorageHold,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct Control(AtomicUsize);
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Hold(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Hold {}
impl Drop for Hold {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn identity_exact_requests_preserve_first_host_and_have_no_control_tail() {
    for refusal in [0, 1] {
        let control = Control(AtomicUsize::new(0));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let original = Arc::new("original identity host cause");
        let mut trace = Vec::new();
        let mut at_refusal = 0;
        let mut request = |layout: Layout| {
            trace.push(layout);
            if trace.len() - 1 == refusal {
                at_refusal = control.0.load(Ordering::SeqCst);
                Err(original.clone())
            } else {
                Ok(())
            }
        };
        let mut opaque = |_: usize| -> Result<(), Arc<&str>> {
            panic!("identities use exact constructor layouts")
        };
        let mut creator =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        let mut construction = Construction {
            creator: Some(&mut creator),
        };
        let error = construction.function("identity", &mut work).unwrap_err();
        let ProjectionFailure::Host(error) = error else {
            panic!("original host error was replaced")
        };
        assert!(Arc::ptr_eq(&original, &error));
        drop(creator);
        assert_eq!(trace.len(), refusal + 1);
        assert_eq!(trace[0], Layout::array::<u8>(8).unwrap());
        if refusal == 1 {
            assert_eq!(trace[1], FunctionId::storage_allocation_layout().unwrap());
        }
        assert_eq!(
            control.0.load(Ordering::SeqCst),
            at_refusal,
            "no callback after original host refusal"
        );
    }
}
#[test]
fn materialized_identity_clone_keeps_only_its_original_storage_until_last_reference() {
    let control = Control(AtomicUsize::new(0));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let released = Arc::new(AtomicUsize::new(0));
    let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(released.clone())));
    let mut layouts = Vec::new();
    let mut request = |layout| {
        layouts.push(layout);
        Ok::<_, std::convert::Infallible>(())
    };
    let mut opaque = |_| panic!("no opaque identity construction");
    let mut creator = ValueSchemaCreation::new(storage, &mut request, &mut opaque);
    let mut construction = Construction {
        creator: Some(&mut creator),
    };
    let identity = construction.function("identity", &mut work).unwrap();
    let independent = identity.clone_observed(|| work.step()).unwrap();
    drop(creator);
    drop(identity);
    assert_eq!(released.load(Ordering::SeqCst), 0);
    drop(independent);
    assert_eq!(released.load(Ordering::SeqCst), 1);
    assert_eq!(
        layouts,
        [
            Layout::array::<u8>(8).unwrap(),
            FunctionId::storage_allocation_layout().unwrap()
        ]
    );
    work.finish().unwrap();
}

#[test]
fn argument_collection_private_holder_preserves_original_host_and_independent_lifetime() {
    use arrow::datatypes::DataType;
    use novarocks_type_contract::FunctionValueType;
    let control = Control(AtomicUsize::new(0));
    for refuse in [true, false] {
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let released = Arc::new(AtomicUsize::new(0));
        let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(released.clone())));
        let original = Arc::new("original argument holder cause");
        let mut trace = Vec::new();
        let failed_at = AtomicUsize::new(0);
        let mut request = |layout| {
            trace.push(layout);
            if refuse && trace.len() == 2 {
                failed_at.store(control.0.load(Ordering::SeqCst), Ordering::SeqCst);
                Err(original.clone())
            } else {
                Ok(())
            }
        };
        let mut opaque = |_| panic!("no opaque fact-slice holder");
        let mut creator = ValueSchemaCreation::new(storage, &mut request, &mut opaque);
        let mut construction = Construction {
            creator: Some(&mut creator),
        };
        // The real payload creator admits this Box before constructing it.
        construction
            .request(Layout::array::<FunctionArgumentType>(1).unwrap())
            .unwrap();
        let items = Box::new([FunctionArgumentType::Value(FunctionValueType::new(
            DataType::Int64,
            false,
        ))]);
        let result = construction.collection(items, &mut work);
        if refuse {
            let ProjectionFailure::Host(error) = result.unwrap_err() else {
                panic!("original host replaced")
            };
            assert!(Arc::ptr_eq(&original, &error));
            assert_eq!(
                control.0.load(Ordering::SeqCst),
                failed_at.load(Ordering::SeqCst),
                "no callback after holder refusal"
            );
            drop(creator);
            assert_eq!(released.load(Ordering::SeqCst), 1);
        } else {
            let arguments = result.unwrap();
            let independent = arguments.clone_observed(|| work.step()).unwrap();
            drop(creator);
            drop(arguments);
            assert_eq!(released.load(Ordering::SeqCst), 0);
            drop(independent);
            assert_eq!(released.load(Ordering::SeqCst), 1);
        }
        assert_eq!(
            trace,
            [
                Layout::array::<FunctionArgumentType>(1).unwrap(),
                FunctionArgumentTypes::storage_allocation_layout().unwrap()
            ]
        );
    }
}
