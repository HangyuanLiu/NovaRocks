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

//! Typed ingress delegates active-ancestor and key validation to the value owner.
use super::{ArrayNormalizationStep, ArrayProjectionFailure};
use arrow_array::ArrayRef;
use novarocks_type_contract::TypedValueBinding;
pub fn validate_required_dictionary_children<E, F>(
    source: &ArrayRef,
    binding: &TypedValueBinding,
    observe: &mut F,
) -> Result<(), E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    // This ingress has only a structural control port. It performs no payload
    // construction; row checks use the existing caller observation as before.
    let observer = std::cell::RefCell::new(observe);
    novarocks_value_arrays::hydrate::validate_required_observed(
        source,
        binding,
        &mut |units| observer.borrow_mut()(ArrayNormalizationStep::ValueWork { units }),
        &mut |event| super::observe_value_hydration(event, &mut **observer.borrow_mut()),
    )
    .map_err(super::value_hydration::value_failure)
}
