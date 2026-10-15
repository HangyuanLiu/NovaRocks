# apache/iceberg-rust 0.11.0-rc.2, vendored without source patches

Upstream tag: `v0.11.0-rc.2`
Upstream commit: `859048563bc71a3167d0545cb66756d26c64e4ca`
Upstream repository: https://github.com/apache/iceberg-rust
Normalized package: `iceberg-catalog-rest-0.11.0.crate`
Package SHA-256: `fde7674c80f19dc54ae2b0b3fb95f0715a4407df615fa63fe42801bc3ec2d2d4`

Generation command (Rust/Cargo 1.98.1, from the verified upstream checkout):

```sh
cargo +1.98.1 package --no-verify --exclude-lockfile --offline \
  -p iceberg-property-macro -p iceberg -p iceberg-catalog-rest -p iceberg-catalog-hms
```

`--exclude-lockfile` avoids resolving unrelated unpublished/workspace development
dependencies while preserving Cargo's normalized manifest and original published
source files. Each extracted file was compared byte-for-byte with this package.
Every file in this directory other than this one is byte-identical to that package.

## Dependency policy

Retain upstream registry dependency versions and lints. NovaRocks uses this
unpatched package only as a connector dev-dependency for independent wire and
interoperability comparison. It is deliberately absent from the root
`[patch.crates-io]` and production normal/build dependency closure. The other
Iceberg packages retain their documented patches. Do not modify this crate's
source or add runtime/network/Arrow vendoring or resource accounting hooks.
