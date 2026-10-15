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

//! The actual writer constructor and Native tracker, without a copied grammar.
use crate::native_type_construction::{
    NativeTypeConstruction, NativeTypeConstructionError, NativeTypeFactory,
};
use arrow::datatypes::{DataType, Field};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorWriteBinding, ConnectorWriteFieldRef,
    ConnectorWriteFieldToken, ConnectorWriteInputRef, ConnectorWriteRecipeDraft,
    PureProviderCompileError, WriterOwnedCreationFailure,
};
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refuse: Mutex<Option<usize>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push((phase, units));
        if *self.refuse.lock().unwrap() == Some(at) {
            return Err(CompileControlError::Cancelled);
        }
        Ok(())
    }
}
fn envelope() -> (ConnectorWriteBinding, ConnectorEncodedPayload) {
    let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32]));
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    (
        ConnectorWriteBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        ),
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                provider,
                catalog,
                ConnectorCodecCategory::WriteHandle,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            // Empty private handles are legal here; provider seal/install is not
            // part of this shared Field-copy constructor test.
            Default::default(),
        ),
    )
}
fn fields(unused: usize) -> (Field, FunctionValueType, Field, FunctionValueType) {
    let source_child = Arc::new(Field::new("child", DataType::Int64, true).with_metadata(
        std::collections::HashMap::from([("source_id".into(), "77".into())]),
    ));
    let used = Field::new("used", DataType::Struct(vec![source_child].into()), false);
    // A complete independent execution declaration, not source metadata.
    let used_type = FunctionValueType::new(
        DataType::Struct(vec![Arc::new(Field::new("child", DataType::Int64, true))].into()),
        false,
    );
    let unused = Field::new("unused", DataType::Int64, false).with_metadata(
        std::collections::HashMap::from([("source_comment".into(), "x".repeat(unused))]),
    );
    (
        used,
        used_type,
        unused,
        FunctionValueType::new(DataType::Int64, false),
    )
}
fn late_field(unused: usize) -> i64 {
    let control = Control::default();
    let tracker = MemTracker::new_root("writer finite Field");
    let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
    creation
        .request_layout(std::alloc::Layout::array::<u8>(8192).unwrap())
        .unwrap();
    let (binding, payload) = envelope();
    let (used, used_type, other, other_type) = fields(unused);
    let refs = [
        ConnectorWriteFieldRef::new_typed(
            ConnectorWriteFieldToken::from_bytes([1; 32]),
            &used,
            &used_type,
        ),
        ConnectorWriteFieldRef::new_typed(
            ConnectorWriteFieldToken::from_bytes([2; 32]),
            &other,
            &other_type,
        ),
    ];
    let input: ConnectorWriteInputRef<'_> = ConnectorWriteInputRef::Data { fields: &refs };
    let factory = NativeTypeFactory::new(&creation);
    let mut work =
        CompileCheckpoints::try_new(&creation, CompilePhase::ProviderValidation).unwrap();
    let recipe = ConnectorWriteRecipeDraft::try_new_from_borrowed_input_with_factory_in(
        &binding,
        &payload,
        &input,
        32 * 1024 * 1024,
        &factory,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let late = recipe.input().fields_iter().next().unwrap().clone();
    let before = tracker.current();
    let allocated = tracker.allocated();
    let cloned = late.clone();
    assert!(std::ptr::eq(late.field(), cloned.field()));
    assert_eq!(tracker.current(), before);
    assert_eq!(tracker.allocated(), allocated);
    assert_eq!(late.execution_type(), Some(&used_type));
    assert_eq!(late.token(), refs[0].token());
    drop(recipe);
    drop(factory);
    drop(creation);
    let retained = tracker.current();
    assert!(retained > 0);
    assert!(retained < before);
    let DataType::Struct(children) = late.field().data_type() else {
        panic!("expected original Struct")
    };
    assert_eq!(children[0].metadata()["source_id"], "77");
    assert_eq!(children[0].is_nullable(), true);
    drop(late);
    assert_eq!(tracker.current(), retained);
    drop(cloned);
    assert_eq!(tracker.current(), 0);
    retained
}
#[test]
fn writer_field_clone_retains_only_its_own_reachable_fields() {
    assert_eq!(late_field(128), late_field(64 * 1024));
}
#[test]
fn writer_first_actual_scope_refusal_rolls_back_without_copy_tail() {
    let control = Control::default();
    let tracker = MemTracker::new_root("writer first actual refusal");
    let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
    let prior = tracker.current();
    tracker.install_limit_once(prior).unwrap();
    let (binding, payload) = envelope();
    let (used, used_type, _, _) = fields(0);
    let refs = [ConnectorWriteFieldRef::new_typed(
        ConnectorWriteFieldToken::from_bytes([1; 32]),
        &used,
        &used_type,
    )];
    let input: ConnectorWriteInputRef<'_> = ConnectorWriteInputRef::Data { fields: &refs };
    let factory = NativeTypeFactory::new(&creation);
    let mut work =
        CompileCheckpoints::try_new(&creation, CompilePhase::ProviderValidation).unwrap();
    let error = ConnectorWriteRecipeDraft::try_new_from_borrowed_input_with_factory_in(
        &binding,
        &payload,
        &input,
        32 * 1024 * 1024,
        &factory,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        WriterOwnedCreationFailure::Creator(NativeTypeConstructionError::Capacity(_))
    ));
    assert_eq!(factory.construction_request_bytes_upper_bound(), 0);
    assert_eq!(tracker.current(), prior);
    drop(work);
    drop(factory);
    drop(creation);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn writer_original_control_first_refusal_has_no_later_scope_or_copy_work() {
    fn run(refuse: Option<usize>) -> (Vec<(CompilePhase, u32)>, bool) {
        let control = Control::default();
        let tracker = MemTracker::new_root("writer original control");
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
        let (binding, payload) = envelope();
        let (used, used_type, _, _) = fields(0);
        let refs = [ConnectorWriteFieldRef::new_typed(
            ConnectorWriteFieldToken::from_bytes([1; 32]),
            &used,
            &used_type,
        )];
        let input: ConnectorWriteInputRef<'_> = ConnectorWriteInputRef::Data { fields: &refs };
        let factory = NativeTypeFactory::new(&creation);
        let mut work =
            CompileCheckpoints::try_new(&creation, CompilePhase::ProviderValidation).unwrap();
        control.trace.lock().unwrap().clear();
        *control.refuse.lock().unwrap() = refuse;
        let result = ConnectorWriteRecipeDraft::try_new_from_borrowed_input_with_factory_in(
            &binding,
            &payload,
            &input,
            32 * 1024 * 1024,
            &factory,
            &mut |_| Ok(()),
            &mut work,
        );
        let stopped = match result {
            Ok(recipe) => {
                drop(recipe);
                false
            }
            Err(error) => {
                assert!(matches!(
                    error,
                    WriterOwnedCreationFailure::Contract(PureProviderCompileError::Control(
                        CompileControlError::Cancelled
                    )) | WriterOwnedCreationFailure::Creator(NativeTypeConstructionError::Control(
                        CompileControlError::Cancelled
                    ))
                ));
                true
            }
        };
        let trace = control.trace.lock().unwrap().clone();
        drop(work);
        drop(factory);
        drop(creation);
        assert_eq!(tracker.current(), 0);
        (trace, stopped)
    }
    let (baseline, stopped) = run(None);
    assert!(!stopped);
    assert!(baseline.len() > 8);
    for at in 0..baseline.len() {
        let (actual, stopped) = run(Some(at));
        assert!(stopped);
        assert_eq!(actual.as_slice(), &baseline[..=at]);
    }
}
