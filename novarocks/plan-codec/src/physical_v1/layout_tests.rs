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
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, sync::Mutex};
#[derive(Debug, PartialEq)]
enum Failure {
    Layout(WireLayoutError),
    Control(CompileControlError),
    Host(usize),
}
impl From<WireLayoutError> for Failure {
    fn from(error: WireLayoutError) -> Self {
        match error {
            WireLayoutError::Control(cause) => Self::Control(cause),
            other => Self::Layout(other),
        }
    }
}
impl From<CompileControlError> for Failure {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
#[derive(Default)]
struct Trace {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len();
        calls.push((phase, units));
        if let Some((stop, error)) = self.stop
            && index == stop
        {
            Err(error)
        } else {
            Ok(())
        }
    }
}
fn construct(
    fragment: &novarocks_physical_plan::Fragment,
    request: &mut impl FnMut(Layout) -> Result<(), Failure>,
    control: &Trace,
) -> Result<WireLayout, Failure> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = WireLayout::try_new_observed(fragment, request, &mut work);
    if matches!(&result, Err(Failure::Host(_) | Failure::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
#[test]
fn observed_layout_preserves_duplicate_occurrences_and_every_request_refusal() {
    let (fragment, values, project, limit, value) = repeated_projection_fragment();
    let mut baseline = Vec::new();
    let layout = construct(
        &fragment,
        &mut |request| {
            baseline.push((request.size(), request.align()));
            Ok(())
        },
        &Trace::default(),
    )
    .unwrap();
    assert_eq!(layout.output_slot(values, 0).unwrap().get(), 1);
    assert_eq!(layout.output_slot(project, 0).unwrap().get(), 2);
    assert_eq!(layout.output_slot(project, 1).unwrap().get(), 3);
    assert_eq!(
        layout
            .input_value_slots(limit, value)
            .unwrap()
            .iter()
            .map(|slot| slot.get())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        layout
            .project_output(&fragment, project, &[value, value])
            .unwrap()
            .iter()
            .map(|slot| slot.get())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    for stop in 0..baseline.len() {
        let mut seen = Vec::new();
        let result = construct(
            &fragment,
            &mut |request| {
                let index = seen.len();
                seen.push((request.size(), request.align()));
                if index == stop {
                    Err(Failure::Host(index))
                } else {
                    Ok(())
                }
            },
            &Trace::default(),
        );
        assert_eq!(result, Err(Failure::Host(stop)));
        assert_eq!(seen, baseline[..=stop]);
    }
}
#[test]
fn observed_layout_control_refusals_have_exact_original_prefix() {
    let (fragment, _, _, _, _) = repeated_projection_fragment();
    let good = Trace::default();
    construct(&fragment, &mut |_| Ok(()), &good).unwrap();
    let baseline = good.calls.into_inner().unwrap();
    for stop in 0..baseline.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Trace {
                calls: Mutex::default(),
                stop: Some((stop, cause)),
            };
            assert_eq!(
                construct(&fragment, &mut |_| Ok(()), &control),
                Err(Failure::Control(cause))
            );
            assert_eq!(*control.calls.lock().unwrap(), baseline[..=stop]);
        }
    }
}
#[test]
fn observed_layout_sparse_maximum_node_id_allocates_by_entry_count() {
    use novarocks_physical_plan::{FragmentId, NodeId};
    let mut builder = FragmentBuilder::new(FragmentId::new(u32::MAX));
    let node = NodeId::new(u32::MAX);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: Box::default(),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let mut max_request = 0;
    let layout = construct(
        &fragment,
        &mut |request| {
            max_request = max_request.max(request.size());
            Ok(())
        },
        &Trace::default(),
    )
    .unwrap();
    assert_eq!(layout.fragment(), FragmentId::new(u32::MAX));
    assert!(max_request < 1024);
}
