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

//! Typed private-provider failures at the pure compilation boundary. Control
//! failures must not be erased into provider diagnostics or row-data errors.

use novarocks_type_contract::CompileControlError;
use std::{error::Error, fmt};

#[derive(Debug)]
pub enum PureProviderCompileError<E: Error> {
    Provider(E),
    Control(CompileControlError),
}
impl<E: Error> fmt::Display for PureProviderCompileError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider(error) => fmt::Display::fmt(error, f),
            Self::Control(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl<E: Error> Error for PureProviderCompileError<E> {}

impl<E: Error> From<CompileControlError> for PureProviderCompileError<E> {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

#[cfg(test)]
mod tests;
