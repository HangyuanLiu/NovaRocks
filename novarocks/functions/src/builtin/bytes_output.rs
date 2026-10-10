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
//! ONE original encryption bytes-to-carrier projection and Arrow construction.
use super::md5_shared::Observation;
use arrow_array::{ArrayRef, BinaryArray, StringArray, builder::BinaryBuilder};
use arrow_schema::DataType;
use std::{convert::Infallible, sync::Arc};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextProjection {
    Latin1,
    Utf8Lossy,
}
fn infallible<T>(r: Result<T, Infallible>) -> T {
    match r {
        Ok(v) => v,
        Err(never) => match never {},
    }
}
pub fn build_binary_array_observed<E>(
    values: Vec<Option<Vec<u8>>>,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<BinaryArray, E> {
    let mut builder = BinaryBuilder::new();
    for value in values {
        observe(Observation::Step)?;
        match value {
            Some(bytes) => {
                for _ in &bytes {
                    observe(Observation::Step)?;
                }
                observe(Observation::OpaqueBoundary)?;
                builder.append_value(bytes);
                observe(Observation::OpaqueBoundary)?;
            }
            None => {
                observe(Observation::OpaqueBoundary)?;
                builder.append_null();
                observe(Observation::OpaqueBoundary)?;
            }
        }
    }
    observe(Observation::OpaqueBoundary)?;
    let out = builder.finish();
    observe(Observation::OpaqueBoundary)?;
    Ok(out)
}
pub fn build_binary_array(values: Vec<Option<Vec<u8>>>) -> BinaryArray {
    infallible(build_binary_array_observed(values, &mut |_| {
        Ok::<(), Infallible>(())
    }))
}
fn latin1_observed<E>(
    bytes: &[u8],
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<String, E> {
    if bytes.is_empty() {
        return Ok(bytes.iter().map(|b| char::from(*b)).collect());
    }
    observe(Observation::OpaqueBoundary)?;
    let out: Result<String, E> = bytes
        .iter()
        .map(|b| {
            observe(Observation::Step)?;
            Ok(char::from(*b))
        })
        .collect();
    let out = out?;
    observe(Observation::OpaqueBoundary)?;
    Ok(out)
}
pub fn build_bytes_output_observed<E>(
    values: Vec<Option<Vec<u8>>>,
    target: Option<&DataType>,
    projection: TextProjection,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<ArrayRef, E> {
    if target == Some(&DataType::Binary) {
        return Ok(Arc::new(build_binary_array_observed(values, observe)?) as ArrayRef);
    }
    observe(Observation::OpaqueBoundary)?;
    let out: Result<Vec<Option<String>>, E> = values
        .into_iter()
        .map(|v| {
            observe(Observation::Step)?;
            match v {
                None => Ok(None),
                Some(b) => Ok(Some(match projection {
                    TextProjection::Latin1 => latin1_observed(&b, observe)?,
                    TextProjection::Utf8Lossy => {
                        for _ in &b {
                            observe(Observation::Step)?;
                        }
                        if b.is_empty() {
                            String::from_utf8_lossy(&b).to_string()
                        } else {
                            observe(Observation::OpaqueBoundary)?;
                            let out = String::from_utf8_lossy(&b).to_string();
                            observe(Observation::OpaqueBoundary)?;
                            out
                        }
                    }
                })),
            }
        })
        .collect();
    let out = out?;
    observe(Observation::OpaqueBoundary)?;
    observe(Observation::OpaqueBoundary)?;
    let out = StringArray::from(out);
    observe(Observation::OpaqueBoundary)?;
    Ok(Arc::new(out))
}
pub fn build_bytes_output_latin1(
    values: Vec<Option<Vec<u8>>>,
    target: Option<&DataType>,
) -> Result<ArrayRef, String> {
    Ok(infallible(build_bytes_output_observed(
        values,
        target,
        TextProjection::Latin1,
        &mut |_| Ok::<(), Infallible>(()),
    )))
}
pub fn build_bytes_output_lossy(
    values: Vec<Option<Vec<u8>>>,
    target: Option<&DataType>,
) -> Result<ArrayRef, String> {
    Ok(infallible(build_bytes_output_observed(
        values,
        target,
        TextProjection::Utf8Lossy,
        &mut |_| Ok::<(), Infallible>(()),
    )))
}
