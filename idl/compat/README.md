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
