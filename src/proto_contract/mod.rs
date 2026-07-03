//! NIDL contract round-trip tests: hand-written internal <-> proto conversions
//! + encode/decode/assert_eq, locking each wire contract. Permanent (unlike the
//! NIDL-0 spike). One submodule per proto file; expr/plan added in later PRs.

mod common;
mod expr;
mod plan;
mod report;
