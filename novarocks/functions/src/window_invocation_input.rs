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

//! Exact whole-invocation source loans. Multiple semantic partitions are
//! never relabelled as one partition. Geometry is supplied by the one host
//! ORDER/frame author; this checker performs no comparison or frame math.
use crate::{
    WindowCallContract, WindowRowRange, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
};
use crate::window_call::validate_full_window_arguments;
use crate::kernel_control::invalid;
use crate::kernel_input::EvaluationCheckpoints;
use novarocks_type_contract::WindowFrameExclusion;
#[derive(Clone, Copy, Debug)]
pub struct FullWindowInvocationInput<'input> {
    contract: &'input WindowCallContract,
    rows: usize,
    logical_arguments: &'input [EvaluatedArgument<'input>],
    order_arguments: &'input [EvaluatedArgument<'input>],
}
impl<'input> FullWindowInvocationInput<'input> {
    pub fn try_new(
        contract: &'input WindowCallContract,
        rows: usize,
        logical_arguments: &'input [EvaluatedArgument<'input>],
        order_arguments: &'input [EvaluatedArgument<'input>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        validate_full_window_arguments(
            contract,
            rows,
            logical_arguments,
            order_arguments,
            control,
        )?;
        Ok(Self {
            contract,
            rows,
            logical_arguments,
            order_arguments,
        })
    }
    pub const fn contract(self) -> &'input WindowCallContract {
        self.contract
    }
    pub const fn invocation_rows(self) -> usize {
        self.rows
    }
    pub const fn logical_arguments(self) -> &'input [EvaluatedArgument<'input>] {
        self.logical_arguments
    }
    pub const fn order_arguments(self) -> &'input [EvaluatedArgument<'input>] {
        self.order_arguments
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowFrameOrigin {
    pub partition_ordinal: usize,
    pub output_row: usize,
    pub partition: WindowRowRange,
}
#[derive(Clone, Copy, Debug)]
pub struct WindowInvocationInput<'input> {
    input: FullWindowInvocationInput<'input>,
    partitions: &'input [WindowRowRange],
    peers: &'input [WindowRowRange],
    frames: &'input [WindowRowRange],
}
impl<'input> WindowInvocationInput<'input> {
    pub fn try_new(
        input: FullWindowInvocationInput<'input>,
        partitions: &'input [WindowRowRange],
        peers: &'input [WindowRowRange],
        frames: &'input [WindowRowRange],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if input
            .contract()
            .options()
            .frame()
            .is_some_and(|frame| frame.exclusion != WindowFrameExclusion::NoOthers)
        {
            return Err(invalid(
                "contiguous window invocation frames require NO OTHERS",
            ));
        }
        let rows = input.invocation_rows();
        if frames.len() != rows {
            return Err(invalid(
                "window invocation frame table differs from complete input",
            ));
        }
        let mut work = EvaluationCheckpoints::new(control);
        let mut end = 0;
        let mut peer_ordinal = 0;
        for partition in partitions {
            if partition.start != end || partition.start >= partition.end || partition.end > rows {
                return Err(invalid(
                    "window semantic partitions do not continuously cover invocation",
                ));
            }
            let mut peer_end = partition.start;
            while let Some(peer) = peers.get(peer_ordinal) {
                if peer.start >= partition.end {
                    break;
                }
                if peer.start != peer_end || peer.start >= peer.end || peer.end > partition.end {
                    return Err(invalid(
                        "window invocation peers cross their actual semantic partition",
                    ));
                }
                peer_end = peer.end;
                peer_ordinal += 1;
                work.step()?;
            }
            if peer_end != partition.end {
                return Err(invalid("window invocation peers omit a semantic partition"));
            }
            for frame in &frames[partition.start..partition.end] {
                if frame.start < partition.start
                    || frame.start > frame.end
                    || frame.end > partition.end
                {
                    return Err(invalid(
                        "window invocation frame crosses its actual semantic partition",
                    ));
                }
                work.step()?;
            }
            end = partition.end;
            work.step()?;
        }
        if end != rows || peer_ordinal != peers.len() {
            return Err(invalid(
                "window invocation geometry does not cover exact input",
            ));
        }
        work.finish()?;
        Ok(Self {
            input,
            partitions,
            peers,
            frames,
        })
    }
    pub const fn full_input(self) -> FullWindowInvocationInput<'input> {
        self.input
    }
    pub const fn partitions(self) -> &'input [WindowRowRange] {
        self.partitions
    }
    pub const fn peers(self) -> &'input [WindowRowRange] {
        self.peers
    }
    pub const fn frames(self) -> &'input [WindowRowRange] {
        self.frames
    }
    pub fn frame_origin(self, row: usize) -> Option<WindowFrameOrigin> {
        if row >= self.input.rows {
            return None;
        }
        let partition_ordinal = self
            .partitions
            .partition_point(|partition| partition.end <= row);
        let partition = *self.partitions.get(partition_ordinal)?;
        Some(WindowFrameOrigin {
            partition_ordinal,
            output_row: row,
            partition,
        })
    }
}
