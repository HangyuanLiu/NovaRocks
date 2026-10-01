// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{self, Read};
use tokio::io::{AsyncRead, AsyncReadExt};

/// A bounded logical-message reader. It never reads beyond the current packet.
pub struct PacketReader<R> {
    bytes: Vec<u8>,
    limit: usize,
    expected_first: Option<u8>,
    pub r: R,
}
impl<R> PacketReader<R> {
    pub fn new(r: R) -> Self {
        Self::with_limit(r, crate::ProtocolLimits::default().command_bytes)
    }
    pub fn with_limit(r: R, limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit),
            limit,
            expected_first: None,
            r,
        }
    }
    pub(crate) fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
        self.bytes = Vec::new();
        self.bytes.reserve_exact(limit);
    }
    #[cfg(feature = "tls")]
    pub(crate) fn release_packet_buffer(&mut self) {
        self.bytes = Vec::new();
        self.limit = 0;
    }
    pub(crate) fn set_expected_first(&mut self, sequence: Option<u8>) {
        self.expected_first = sequence;
    }
    fn accept_header(&mut self, header: [u8; 4], expected: Option<u8>) -> io::Result<usize> {
        if expected.is_some_and(|seq| seq != header[3]) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MySQL packet sequence mismatch",
            ));
        }
        let length =
            usize::from(header[0]) | usize::from(header[1]) << 8 | usize::from(header[2]) << 16;
        let total = self
            .bytes
            .len()
            .checked_add(length)
            .filter(|total| *total <= self.limit)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "MySQL logical packet exceeds input limit",
                )
            })?;
        // The complete input backing was reserved once before packet receipt.
        // No old+new buffer can coexist while command state is resident.
        let _ = total;
        Ok(length)
    }
}
impl<R: Read> PacketReader<R> {
    #[allow(dead_code)]
    pub fn next(&mut self) -> io::Result<Option<(u8, Packet<'_>)>> {
        self.bytes.clear();
        let mut expected = self.expected_first;
        loop {
            let mut header = [0; 4];
            if self.r.read(&mut header[..1])? == 0 {
                if self.bytes.is_empty() && expected == self.expected_first {
                    return Ok(None);
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "missing MySQL continuation",
                ));
            }
            self.r.read_exact(&mut header[1..])?;
            let length = self.accept_header(header, expected)?;
            let start = self.bytes.len();
            self.bytes.resize(start + length, 0);
            self.r.read_exact(&mut self.bytes[start..])?;
            if length != crate::U24_MAX {
                return Ok(Some((header[3], Packet(&self.bytes, Vec::new()))));
            }
            expected = Some(header[3].wrapping_add(1));
        }
    }
}
impl<R: AsyncRead + Unpin> AsyncRead for PacketReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.r).poll_read(cx, buf)
    }
}
impl<R: AsyncRead + Unpin> PacketReader<R> {
    pub async fn next_async(&mut self) -> io::Result<Option<(u8, Packet<'_>)>> {
        self.bytes.clear();
        let mut expected = self.expected_first;
        let mut continuation = false;
        loop {
            let mut header = [0; 4];
            if self.r.read(&mut header[..1]).await? == 0 {
                if !continuation {
                    return Ok(None);
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "missing MySQL continuation",
                ));
            }
            self.r.read_exact(&mut header[1..]).await?;
            let length = self.accept_header(header, expected)?;
            let start = self.bytes.len();
            self.bytes.resize(start + length, 0);
            self.r.read_exact(&mut self.bytes[start..]).await?;
            if length != crate::U24_MAX {
                return Ok(Some((header[3], Packet(&self.bytes, Vec::new()))));
            }
            expected = Some(header[3].wrapping_add(1));
            continuation = true;
        }
    }
}
#[cfg(test)]
pub fn onepacket(i: &[u8]) -> nom::IResult<&[u8], (u8, &[u8])> {
    let (i, length) = nom::number::complete::le_u24(i)?;
    let (i, seq) = nom::bytes::complete::take(1u8)(i)?;
    let (i, bytes) = nom::bytes::complete::take(length)(i)?;
    Ok((i, (seq[0], bytes)))
}

// Clone because of https://github.com/Geal/nom/issues/1008
#[derive(Clone, Debug)]
pub struct Packet<'a>(&'a [u8], Vec<u8>);

#[cfg(test)]
impl<'a> Packet<'a> {
    fn extend(&mut self, bytes: &'a [u8]) {
        if self.0.is_empty() {
            if self.1.is_empty() {
                // first extend
                self.0 = bytes;
            } else {
                // later extend
                self.1.extend(bytes);
            }
        } else {
            assert!(self.1.is_empty());
            let mut v = self.0.to_vec();
            v.extend(bytes);
            self.1 = v;
            self.0 = &[];
        }
    }
}

impl<'a> AsRef<[u8]> for Packet<'a> {
    fn as_ref(&self) -> &[u8] {
        if self.1.is_empty() {
            self.0
        } else {
            &self.1
        }
    }
}

#[cfg(test)]
use crate::U24_MAX;
use std::ops::Deref;

impl<'a> Deref for Packet<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

#[cfg(test)]
pub(crate) fn packet(i: &[u8]) -> nom::IResult<&[u8], (u8, Packet<'_>)> {
    let mut rest = i;
    let mut previous: Option<u8> = None;
    let mut output = Packet(&[], Vec::new());
    loop {
        let (next, (seq, bytes)) = onepacket(rest)?;
        if previous.is_some_and(|p| seq != p.wrapping_add(1)) {
            return Err(nom::Err::Failure(nom::error::Error::new(
                rest,
                nom::error::ErrorKind::Verify,
            )));
        }
        output.extend(bytes);
        if bytes.len() != U24_MAX {
            return Ok((next, (seq, output)));
        }
        previous = Some(seq);
        rest = next;
    }
}
