<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Compat IDL

This directory contains StarRocks-compatible protocol definitions that NovaRocks
keeps for StarRocks compatibility mode and connector-private protocol handling.

- `thrift/` contains the StarRocks Thrift files used by compatibility services
  and legacy/generated types.
- `proto/` contains StarRocks protobuf files used by compatibility services and
  StarRocks connector/storage-format code.
- `staros/` contains StarOS/Starlet protobuf files used by compatibility-facing
  Starlet integration.

Native NovaRocks cluster-internal protocol definitions live under
`idl/novarocks/`. New NovaRocks protocol fields should be added there, not in
this directory. Files here should only change for compatibility fixes or when a
compatibility dependency is deliberately retired.
