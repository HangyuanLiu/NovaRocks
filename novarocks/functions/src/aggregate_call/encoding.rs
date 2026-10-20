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

//! Local installed aggregate input encoding facts. No FE or allocation authority.
use super::{AggregateCallContract, AggregateKernelPhase};
use crate::kernel_control::invalid;
use crate::kernel_input::{EvaluationCheckpoints, logical_is_null};
use crate::{
    EvaluatedArgument, FunctionValueType, KernelEvaluationControl, KernelFailure, Selection,
};
use arrow_array::types::Int32Type;
use arrow_array::{Array, DictionaryArray, LargeStringArray, StringArray};
use arrow_schema::DataType;

#[derive(Clone, Copy, Debug)]
enum AcceptedEncoding {
    Plain,
    RootInt32StringDictionary,
}

/// Borrowed fact minted by one actual installed aggregate implementation.
/// Exact call, argument and phase scope are checked before the input is used.
/// This fact grants no memory capacity and is not a semantic or wire identity.
#[derive(Clone, Copy, Debug)]
pub struct AggregateArgumentEncoding<'kernel> {
    contract: &'kernel AggregateCallContract,
    ordinal: usize,
    phase: AggregateKernelPhase,
    accepted: AcceptedEncoding,
    owner: Option<&'kernel dyn crate::aggregate_erasure::ErasedAggregateOps>,
}
impl<'kernel> AggregateArgumentEncoding<'kernel> {
    pub(crate) fn plain(contract: &'kernel AggregateCallContract, ordinal: usize) -> Self {
        Self {
            contract,
            ordinal,
            phase: contract.phase(),
            accepted: AcceptedEncoding::Plain,
            owner: None,
        }
    }
    pub(crate) fn root_int32_string_dictionary(
        contract: &'kernel AggregateCallContract,
        ordinal: usize,
    ) -> Self {
        Self {
            contract,
            ordinal,
            phase: contract.phase(),
            accepted: AcceptedEncoding::RootInt32StringDictionary,
            owner: None,
        }
    }
    pub(crate) fn bind_installed(
        mut self,
        owner: &'kernel dyn crate::aggregate_erasure::ErasedAggregateOps,
    ) -> Self {
        self.owner = Some(owner);
        self
    }
    pub(crate) fn installed_owner(
        self,
    ) -> Result<&'kernel dyn crate::aggregate_erasure::ErasedAggregateOps, KernelFailure> {
        self.owner
            .ok_or_else(|| invalid("aggregate encoding fact has no installed adapter owner"))
    }
    /// The BE asks every installed use before deciding whether to hydrate.
    pub fn accepts_root_int32_string_dictionary(self) -> bool {
        matches!(self.accepted, AcceptedEncoding::RootInt32StringDictionary)
    }
    pub(crate) fn validate_scope(
        self,
        contract: &AggregateCallContract,
        ordinal: usize,
    ) -> Result<(), KernelFailure> {
        if !std::ptr::eq(self.contract, contract)
            || self.ordinal != ordinal
            || self.phase != contract.phase()
            || ordinal
                >= contract.logical_argument_types().len() + contract.order_argument_types().len()
        {
            return Err(invalid(
                "aggregate encoding fact differs from exact installed call/argument/phase",
            ));
        }
        Ok(())
    }
}

pub(super) fn validate_typed_argument(
    argument: EvaluatedArgument<'_>,
    source: &FunctionValueType,
    expected: &FunctionValueType,
    selection: Selection<'_>,
    encoding: AggregateArgumentEncoding<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        argument.validate_shape_observed::<KernelFailure>(selection, || work.step())?;
        if !source.fits_value_type_observed(expected, || work.step())?
            || !source
                .typed_binding()
                .layout()
                .equals_observed(expected.typed_binding().layout(), || work.step())?
        {
            return Err(invalid(
                "aggregate typed source differs from complete selected semantics/layout",
            ));
        }
        if let Some(value) = argument.constant_value() {
            if !value
                .value_type()
                .fits_value_type_observed(source, || work.step())?
            {
                return Err(invalid(
                    "aggregate constant differs from caller's typed source",
                ));
            }
        }
        let actual = argument.array();
        work.step()?;
        match actual.data_type() {
            DataType::Dictionary(keys, values) => {
                if !encoding.accepts_root_int32_string_dictionary()
                    || keys.as_ref() != &DataType::Int32
                    || !matches!(values.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
                {
                    return Err(invalid(
                        "installed aggregate does not accept this dictionary encoding",
                    ));
                }
                let dictionary = actual
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .ok_or_else(|| {
                        invalid("aggregate dictionary has a foreign concrete carrier")
                    })?;
                work.step()?;
                let values = dictionary.values();
                let concrete = match values.data_type() {
                    DataType::Utf8 => values.as_any().is::<StringArray>(),
                    DataType::LargeUtf8 => values.as_any().is::<LargeStringArray>(),
                    _ => false,
                };
                if !concrete
                    || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
                        source.data_type(),
                        values.data_type(),
                        || work.step(),
                    )?
                {
                    return Err(invalid(
                        "aggregate dictionary values differ from caller's exact normalized layout",
                    ));
                }
                // Selected key addressing is checked under the original observer.
                // Nullable input still checks keys: a bad key is not a SQL NULL.
                for (ordinal, row) in selection.iter().enumerate() {
                    let address = argument.value_row(ordinal, row);
                    let null = logical_is_null(actual.as_ref(), address, 1, &mut work)?;
                    if null && !expected.nullable() {
                        return Err(invalid(
                            "non-null aggregate dictionary source contains a selected NULL",
                        ));
                    }
                }
            }
            _ => {
                // The existing exact plain boundary remains the sole plain author.
                crate::kernel_input::validate_argument_observed(
                    argument, selection, expected, control,
                )?;
            }
        }
        Ok(())
    })();
    work.finish_result(result)
}
