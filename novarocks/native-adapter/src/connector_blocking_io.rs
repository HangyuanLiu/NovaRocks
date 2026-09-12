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

//! Server-frozen capacity for the Frontend Connector blocking-I/O runtime.

/// The process bounds for Frontend Connector blocking calls.
///
/// This is a deployment capability passed from Server composition to the
/// role-local data runtime. It is not task-protocol state or Connector
/// provider authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorBlockingIoBudget {
    total: usize,
    ordinary: usize,
}

impl ConnectorBlockingIoBudget {
    /// Builds a budget with at least one slot reserved for protected progress.
    pub fn try_new(total: usize, ordinary: usize) -> Result<Self, String> {
        if total == 0 {
            return Err("connector blocking-I/O total permits must be nonzero".to_owned());
        }
        if ordinary == 0 {
            return Err("connector blocking-I/O ordinary permits must be nonzero".to_owned());
        }
        if ordinary >= total {
            return Err(
                "connector blocking-I/O ordinary permits must leave protected capacity".to_owned(),
            );
        }
        Ok(Self { total, ordinary })
    }

    pub const fn total(self) -> usize {
        self.total
    }

    pub const fn ordinary(self) -> usize {
        self.ordinary
    }
}

impl Default for ConnectorBlockingIoBudget {
    fn default() -> Self {
        Self::try_new(16, 12).expect("the default Connector blocking-I/O budget is valid")
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectorBlockingIoBudget;

    #[test]
    fn budget_requires_total_ordinary_and_protected_capacity() {
        assert!(ConnectorBlockingIoBudget::try_new(0, 0).is_err());
        assert!(ConnectorBlockingIoBudget::try_new(2, 0).is_err());
        assert!(ConnectorBlockingIoBudget::try_new(2, 2).is_err());
        assert_eq!(
            ConnectorBlockingIoBudget::try_new(2, 1).expect("valid budget"),
            ConnectorBlockingIoBudget {
                total: 2,
                ordinary: 1,
            }
        );
    }
}
