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

//! Backend producer-failure wire codec. `NRFC` belongs to Execution; this
//! six-byte `NRPU` envelope is the independent transport terminal encoding.

use novarocks_execution::runtime_filter::RuntimeFilterProducerFailure;

const MAGIC: &[u8; 4] = b"NRPU";
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerFailureCodecError {
    Malformed,
    UnknownVersion,
    UnknownReason,
}

pub(crate) fn encode_producer_failure(reason: RuntimeFilterProducerFailure) -> [u8; 6] {
    let tag = match reason {
        RuntimeFilterProducerFailure::Cancelled => 1,
        RuntimeFilterProducerFailure::ExecutionFailed => 2,
        RuntimeFilterProducerFailure::UpstreamUnavailable => 3,
    };
    [b'N', b'R', b'P', b'U', VERSION, tag]
}

pub(crate) fn decode_producer_failure(
    encoded: &[u8],
) -> Result<RuntimeFilterProducerFailure, ProducerFailureCodecError> {
    if encoded.len() != 6 || &encoded[..4] != MAGIC {
        return Err(ProducerFailureCodecError::Malformed);
    }
    if encoded[4] != VERSION {
        return Err(ProducerFailureCodecError::UnknownVersion);
    }
    match encoded[5] {
        1 => Ok(RuntimeFilterProducerFailure::Cancelled),
        2 => Ok(RuntimeFilterProducerFailure::ExecutionFailed),
        3 => Ok(RuntimeFilterProducerFailure::UpstreamUnavailable),
        _ => Err(ProducerFailureCodecError::UnknownReason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nrpu_is_byte_stable_and_strict() {
        assert_eq!(
            encode_producer_failure(RuntimeFilterProducerFailure::Cancelled),
            *b"NRPU\x01\x01"
        );
        assert_eq!(
            decode_producer_failure(b"NRPU\x02\x01"),
            Err(ProducerFailureCodecError::UnknownVersion)
        );
        assert_eq!(
            decode_producer_failure(b"NRPU\x01\x04"),
            Err(ProducerFailureCodecError::UnknownReason)
        );
    }
}
