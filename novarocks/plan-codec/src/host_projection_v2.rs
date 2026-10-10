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

//! Borrowed-host admission refusal without another serializer or error text mapping.
//! A host payload retains its original nominal type. No stock is minted here.
use std::{convert::Infallible, fmt};

#[derive(Debug)]
pub enum AdmissionRefusal<H> {
    Control(novarocks_type_contract::CompileControlError),
    Host(H),
}
impl<H> From<novarocks_type_contract::CompileControlError> for AdmissionRefusal<H> {
    fn from(cause: novarocks_type_contract::CompileControlError) -> Self {
        Self::Control(cause)
    }
}
#[derive(Debug)]
pub enum ProjectionFailure<C, H> {
    Codec(C),
    Host(H),
}
impl<C, H> From<C> for ProjectionFailure<C, H> {
    fn from(error: C) -> Self {
        Self::Codec(error)
    }
}
impl<C> ProjectionFailure<C, Infallible> {
    /// Only the original Control-only adapter can produce this uninhabited host channel.
    pub fn without_host(self) -> C {
        match self {
            Self::Codec(error) => error,
            Self::Host(never) => match never {},
        }
    }
}
impl<C: fmt::Display, H: fmt::Display> fmt::Display for ProjectionFailure<C, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => error.fmt(f),
            Self::Host(error) => error.fmt(f),
        }
    }
}
impl<C: std::error::Error + 'static, H: std::error::Error + 'static> std::error::Error
    for ProjectionFailure<C, H>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::Host(error) => Some(error),
        }
    }
}
