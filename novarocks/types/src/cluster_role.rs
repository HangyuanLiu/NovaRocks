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

/// Native deployment role shared by server composition and role applications.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterRole {
    Fe,
    Be,
    #[default]
    AllInOne,
}

#[cfg(test)]
mod tests {
    use super::ClusterRole;

    #[derive(serde::Deserialize)]
    struct RoleWire {
        role: ClusterRole,
    }

    #[test]
    fn default_is_all_in_one() {
        assert_eq!(ClusterRole::default(), ClusterRole::AllInOne);
    }

    #[test]
    fn deserializes_kebab_case_roles() {
        assert_eq!(
            toml::from_str::<RoleWire>("role = \"all-in-one\"")
                .expect("parse role")
                .role,
            ClusterRole::AllInOne,
        );
        assert_eq!(
            toml::from_str::<RoleWire>("role = \"fe\"")
                .expect("parse role")
                .role,
            ClusterRole::Fe,
        );
        assert_eq!(
            toml::from_str::<RoleWire>("role = \"be\"")
                .expect("parse role")
                .role,
            ClusterRole::Be,
        );
    }
}
