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

//! Allocation-free, borrowed Variant JSON byte generation for the default JSON
//! parser. Callers own the fallible traversal stack; every step handles bounded
//! work, and the existing default JSON recursion policy is retained.
use chrono::{DateTime, FixedOffset, Local, NaiveDate, Offset, Utc};
use std::fmt::{self, Write};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VariantJsonStep {
    Byte(u8),
    Progress,
    Push(VariantJsonFrame),
    Pop,
    End,
    Invalid,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VariantJsonFrame {
    base: usize,
    data: usize,
    offsets: usize,
    ids: usize,
    count: usize,
    next: usize,
    width: usize,
    id_width: usize,
    object: bool,
    phase: u8,
}
#[derive(Clone, Copy)]
enum Mode {
    Idle,
    Fixed,
    String {
        start: usize,
        len: usize,
        at: usize,
        phase: u8,
        key: bool,
    },
    Binary {
        start: usize,
        len: usize,
        at: usize,
        phase: u8,
    },
}
struct Buffer {
    bytes: [u8; 384],
    len: usize,
    at: usize,
}
impl Buffer {
    fn new() -> Self {
        Self {
            bytes: [0; 384],
            len: 0,
            at: 0,
        }
    }
    fn clear(&mut self) {
        self.len = 0;
        self.at = 0;
    }
}
impl Write for Buffer {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let end = self.len.checked_add(s.len()).ok_or(fmt::Error)?;
        if end > self.bytes.len() {
            return Err(fmt::Error);
        }
        self.bytes[self.len..end].copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}
/// State holds offsets only. The same immutable serialized input must be
/// borrowed again on each step; callers own its lifetime and scheduling.
pub struct VariantJsonCursor {
    depth: usize,
    push: Option<VariantJsonFrame>,
    mode: Mode,
    buffer: Buffer,
    next_value: Option<usize>,
    key: Option<(usize, usize)>,
    colon: bool,
    end: usize,
    metadata_end: usize,
    metadata_width: usize,
    dictionary: usize,
    offset: FixedOffset,
    failed: bool,
    utf8_left: u8,
    utf8_value: u32,
    utf8_min: u32,
}
fn le(raw: &[u8], at: usize, width: usize) -> Option<usize> {
    let bytes = raw.get(at..at.checked_add(width)?)?;
    let mut result = 0usize;
    for (i, b) in bytes.iter().enumerate() {
        result |= usize::from(*b) << (8 * i);
    }
    Some(result)
}
impl Default for VariantJsonCursor {
    fn default() -> Self {
        Self::new()
    }
}
impl VariantJsonCursor {
    pub fn new() -> Self {
        Self {
            depth: 0,
            push: None,
            mode: Mode::Idle,
            buffer: Buffer::new(),
            next_value: None,
            key: None,
            colon: false,
            end: 0,
            metadata_end: 0,
            metadata_width: 0,
            dictionary: 0,
            offset: FixedOffset::east_opt(0).unwrap(),
            failed: false,
            utf8_left: 0,
            utf8_value: 0,
            utf8_min: 0,
        }
    }
    /// Captures Local offset exactly once, at conversion start.
    pub fn start(&mut self, raw: &[u8]) -> bool {
        self.start_with_offset(raw, Local::now().offset().fix())
    }
    pub fn start_with_offset(&mut self, raw: &[u8], offset: FixedOffset) -> bool {
        self.depth = 0;
        self.push = None;
        self.mode = Mode::Idle;
        self.buffer.clear();
        self.next_value = None;
        self.key = None;
        self.colon = false;
        self.failed = false;
        self.offset = offset;
        let Some(size) = le(raw, 0, 4) else {
            return false;
        };
        if size > 16 * 1024 * 1024 {
            return false;
        }
        let Some(end) = size.checked_add(4).filter(|v| *v <= raw.len()) else {
            return false;
        };
        let Some(header) = raw.get(4).copied().filter(|_| size > 0) else {
            return false;
        };
        if header & 15 != 1 {
            return false;
        }
        let width = usize::from((header >> 6) + 1);
        let Some(dictionary) = le(&raw[..end], 5, width) else {
            return false;
        };
        let Some(last) = dictionary
            .checked_mul(width)
            .and_then(|n| n.checked_add(5 + width))
        else {
            return false;
        };
        let Some(data_size) = le(&raw[..end], last, width) else {
            return false;
        };
        let Some(metadata_end) = last
            .checked_add(width)
            .and_then(|v| v.checked_add(data_size))
            .filter(|v| *v <= end)
        else {
            return false;
        };
        if metadata_end < 7 {
            return false;
        }
        self.end = end;
        self.metadata_end = metadata_end;
        self.metadata_width = width;
        self.dictionary = dictionary;
        self.next_value = Some(metadata_end);
        true
    }
    fn fixed(&mut self, text: &str) -> bool {
        self.buffer.clear();
        self.mode = Mode::Fixed;
        self.buffer.write_str(text).is_ok()
    }
    fn utf8(&mut self, b: u8) -> bool {
        if self.utf8_left == 0 {
            match b {
                0..=0x7f => true,
                0xc2..=0xdf => {
                    self.utf8_left = 1;
                    self.utf8_value = u32::from(b & 31);
                    self.utf8_min = 0x80;
                    true
                }
                0xe0..=0xef => {
                    self.utf8_left = 2;
                    self.utf8_value = u32::from(b & 15);
                    self.utf8_min = 0x800;
                    true
                }
                0xf0..=0xf4 => {
                    self.utf8_left = 3;
                    self.utf8_value = u32::from(b & 7);
                    self.utf8_min = 0x10000;
                    true
                }
                _ => false,
            }
        } else {
            if b & 0xc0 != 0x80 {
                return false;
            }
            self.utf8_value = (self.utf8_value << 6) | u32::from(b & 63);
            self.utf8_left -= 1;
            self.utf8_left != 0
                || (self.utf8_value >= self.utf8_min
                    && self.utf8_value <= 0x10ffff
                    && !(0xd800..=0xdfff).contains(&self.utf8_value))
        }
    }
    pub fn step(&mut self, raw: &[u8], frame: Option<&mut VariantJsonFrame>) -> VariantJsonStep {
        if self.failed || raw.len() < self.end {
            return VariantJsonStep::Invalid;
        }
        match self.step_inner(&raw[..self.end], frame) {
            Some(step) => step,
            None => {
                self.failed = true;
                VariantJsonStep::Invalid
            }
        }
    }
    fn step_inner(
        &mut self,
        raw: &[u8],
        frame: Option<&mut VariantJsonFrame>,
    ) -> Option<VariantJsonStep> {
        if let Some(frame) = self.push.take() {
            return Some(VariantJsonStep::Push(frame));
        }
        if self.buffer.at < self.buffer.len {
            let b = self.buffer.bytes[self.buffer.at];
            self.buffer.at += 1;
            return Some(VariantJsonStep::Byte(b));
        }
        match self.mode {
            Mode::Fixed => {
                self.mode = Mode::Idle;
                return Some(VariantJsonStep::Progress);
            }
            Mode::String {
                start,
                len,
                at,
                phase,
                key,
            } => {
                if phase == 0 {
                    self.utf8_left = 0;
                    self.mode = Mode::String {
                        start,
                        len,
                        at,
                        phase: 1,
                        key,
                    };
                    return Some(VariantJsonStep::Byte(b'"'));
                }
                if at == len {
                    if key && self.utf8_left != 0 {
                        return None;
                    }
                    self.mode = Mode::Idle;
                    return Some(VariantJsonStep::Byte(b'"'));
                }
                let b = *raw.get(start + at)?;
                if key && !self.utf8(b) {
                    return None;
                }
                self.mode = Mode::String {
                    start,
                    len,
                    at: at + 1,
                    phase: 1,
                    key,
                };
                self.buffer.clear();
                match b {
                    b'"' => self.buffer.write_str("\\\"").ok()?,
                    b'\\' => self.buffer.write_str("\\\\").ok()?,
                    b'\n' => self.buffer.write_str("\\n").ok()?,
                    b'\r' => self.buffer.write_str("\\r").ok()?,
                    b'\t' => self.buffer.write_str("\\t").ok()?,
                    8 => self.buffer.write_str("\\b").ok()?,
                    12 => self.buffer.write_str("\\f").ok()?,
                    0..=31 => write!(self.buffer, "\\u{:04x}", b).ok()?,
                    _ => {
                        let mut encoded = [0u8; 4];
                        self.buffer
                            .write_str(char::from(b).encode_utf8(&mut encoded))
                            .ok()?;
                    }
                }
                return Some(VariantJsonStep::Progress);
            }
            Mode::Binary {
                start,
                len,
                at,
                phase,
            } => {
                if phase == 0 {
                    self.mode = Mode::Binary {
                        start,
                        len,
                        at,
                        phase: 1,
                    };
                    return Some(VariantJsonStep::Byte(b'"'));
                }
                if at == len {
                    self.mode = Mode::Idle;
                    return Some(VariantJsonStep::Byte(b'"'));
                }
                let n = (len - at).min(3);
                let bytes = raw.get(start + at..start + at + n)?;
                const ALPHABET: &[u8; 64] =
                    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
                let a = bytes[0];
                let b = bytes.get(1).copied().unwrap_or(0);
                let c = bytes.get(2).copied().unwrap_or(0);
                self.buffer.clear();
                self.buffer.bytes[..4].copy_from_slice(&[
                    ALPHABET[(a >> 2) as usize],
                    ALPHABET[(((a & 3) << 4) | (b >> 4)) as usize],
                    if n > 1 {
                        ALPHABET[(((b & 15) << 2) | (c >> 6)) as usize]
                    } else {
                        b'='
                    },
                    if n > 2 {
                        ALPHABET[(c & 63) as usize]
                    } else {
                        b'='
                    },
                ]);
                self.buffer.len = 4;
                self.mode = Mode::Binary {
                    start,
                    len,
                    at: at + n,
                    phase: 1,
                };
                return Some(VariantJsonStep::Progress);
            }
            Mode::Idle => {}
        }
        if let Some((start, len)) = self.key.take() {
            self.mode = Mode::String {
                start,
                len,
                at: 0,
                phase: 0,
                key: true,
            };
            return Some(VariantJsonStep::Progress);
        }
        if self.colon {
            self.colon = false;
            return Some(VariantJsonStep::Byte(b':'));
        }
        if let Some(base) = self.next_value.take() {
            self.value(raw, base)?;
            return Some(VariantJsonStep::Progress);
        }
        if self.depth == 0 {
            return Some(VariantJsonStep::End);
        }
        let f = frame?;
        if f.next == f.count {
            self.fixed(if f.object { "}" } else { "]" });
            self.depth -= 1;
            return Some(VariantJsonStep::Pop);
        }
        if f.phase == 0 && f.next != 0 {
            f.phase = 1;
            return Some(VariantJsonStep::Byte(b','));
        }
        f.phase = 0;
        let child = f.base.checked_add(f.data)?.checked_add(le(
            raw,
            f.base + f.offsets + f.next * f.width,
            f.width,
        )?)?;
        if child >= self.end {
            return None;
        }
        if f.object {
            let id = le(raw, f.base + f.ids + f.next * f.id_width, f.id_width)?;
            if id >= self.dictionary {
                return None;
            }
            let pos = 5 + self.metadata_width + id * self.metadata_width;
            let start = le(&raw[..self.metadata_end], pos, self.metadata_width)?;
            let next = le(
                &raw[..self.metadata_end],
                pos + self.metadata_width,
                self.metadata_width,
            )?;
            let len = next.saturating_sub(start);
            let start = (5 + self.metadata_width * (self.dictionary + 2)).checked_add(start)?;
            if start.checked_add(len)? > self.metadata_end {
                return None;
            }
            self.key = Some((start, len));
            self.colon = true;
        }
        f.next += 1;
        self.next_value = Some(child);
        Some(VariantJsonStep::Progress)
    }
    fn value(&mut self, raw: &[u8], base: usize) -> Option<()> {
        let header = *raw.get(base)?;
        let kind = header & 3;
        let value_header = header >> 2;
        if kind == 2 || kind == 3 {
            if self.depth == 127 {
                return None;
            }
            let width = usize::from((value_header & 3) + 1);
            let large = if kind == 2 {
                value_header & 16 != 0
            } else {
                value_header & 4 != 0
            };
            let count_width = if large { 4 } else { 1 };
            let count = le(raw, base + 1, count_width)?;
            let ids = 1 + count_width;
            let id_width = if kind == 2 {
                usize::from(((value_header >> 2) & 3) + 1)
            } else {
                0
            };
            let offsets = ids.checked_add(count.checked_mul(id_width)?)?;
            let data = offsets.checked_add((count + 1).checked_mul(width)?)?;
            if base.checked_add(data)? > self.end {
                return None;
            }
            self.push = Some(VariantJsonFrame {
                base,
                data,
                offsets,
                ids,
                count,
                next: 0,
                width,
                id_width,
                object: kind == 2,
                phase: 0,
            });
            self.depth += 1;
            self.fixed(if kind == 2 { "{" } else { "[" });
            return Some(());
        }
        if kind == 1 {
            let len = usize::from(value_header);
            raw.get(base + 1..base + 1 + len)?;
            self.mode = Mode::String {
                start: base + 1,
                len,
                at: 0,
                phase: 0,
                key: false,
            };
            return Some(());
        }
        let bytes = raw.get(base + 1..)?;
        self.buffer.clear();
        self.mode = Mode::Fixed;
        macro_rules! integer {
            ($t:ty,$n:expr) => {{
                let b: <$t as ByteInteger>::Bytes = bytes.get(..$n)?.try_into().ok()?;
                <$t>::from_le_bytes(b)
            }};
        }
        match value_header {
            0 => self.buffer.write_str("null").ok()?,
            1 => self.buffer.write_str("true").ok()?,
            2 => self.buffer.write_str("false").ok()?,
            3 => write!(self.buffer, "{}", integer!(i8, 1)).ok()?,
            4 => write!(self.buffer, "{}", integer!(i16, 2)).ok()?,
            5 => write!(self.buffer, "{}", integer!(i32, 4)).ok()?,
            6 => write!(self.buffer, "{}", integer!(i64, 8)).ok()?,
            7 | 14 => {
                let value = if value_header == 7 {
                    f64::from_le_bytes(bytes.get(..8)?.try_into().ok()?)
                } else {
                    f32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as f64
                };
                if !value.is_finite() {
                    self.buffer.write_str("null").ok()?;
                } else {
                    write!(self.buffer, "{value}").ok()?;
                    if !self.buffer.bytes[..self.buffer.len]
                        .iter()
                        .any(|b| matches!(b, b'.' | b'e' | b'E'))
                    {
                        self.buffer.write_str(".0").ok()?;
                    }
                }
            }
            8..=10 => {
                let scale = *bytes.first()?;
                let bytes = bytes.get(1..)?;
                let value = match value_header {
                    8 => i32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as i128,
                    9 => i64::from_le_bytes(bytes.get(..8)?.try_into().ok()?) as i128,
                    _ => i128::from_le_bytes(bytes.get(..16)?.try_into().ok()?),
                };
                let mut digits = Buffer::new();
                write!(digits, "{}", value.unsigned_abs()).ok()?;
                if value < 0 {
                    self.buffer.write_str("-").ok()?;
                }
                let scale = usize::from(scale);
                if scale == 0 {
                    self.buffer.bytes[self.buffer.len..self.buffer.len + digits.len]
                        .copy_from_slice(&digits.bytes[..digits.len]);
                    self.buffer.len += digits.len;
                } else {
                    if digits.len <= scale {
                        self.buffer.write_str("0.").ok()?;
                        for _ in 0..scale - digits.len {
                            self.buffer.write_str("0").ok()?;
                        }
                    }
                    for i in 0..digits.len {
                        if digits.len > scale && i == digits.len - scale {
                            self.buffer.write_str(".").ok()?;
                        }
                        let mut c = [0u8; 1];
                        c[0] = digits.bytes[i];
                        self.buffer.write_str(std::str::from_utf8(&c).ok()?).ok()?;
                    }
                    while self.buffer.bytes[self.buffer.len - 1] == b'0'
                        && self.buffer.bytes[self.buffer.len - 2] != b'.'
                    {
                        self.buffer.len -= 1;
                    }
                }
            }
            11 => {
                let days = integer!(i32, 4);
                let date = 719163i32
                    .checked_add(days)
                    .and_then(NaiveDate::from_num_days_from_ce_opt)
                    .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                self.buffer.write_str("\"").ok()?;
                date.format("%Y-%m-%d").write_to(&mut self.buffer).ok()?;
                self.buffer.write_str("\"").ok()?;
            }
            12 | 13 => {
                let micros = integer!(i64, 8);
                let fraction = micros.rem_euclid(1_000_000) as u32;
                let utc =
                    DateTime::<Utc>::from_timestamp(micros.div_euclid(1_000_000), fraction * 1000)
                        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
                self.buffer.write_str("\"").ok()?;
                if value_header == 13 {
                    utc.naive_utc()
                        .format("%Y-%m-%d %H:%M:%S")
                        .write_to(&mut self.buffer)
                        .ok()?;
                    write!(self.buffer, ".{fraction:06}").ok()?;
                } else {
                    utc.with_timezone(&self.offset)
                        .naive_local()
                        .format("%Y-%m-%d %H:%M:%S")
                        .write_to(&mut self.buffer)
                        .ok()?;
                    if fraction != 0 {
                        write!(self.buffer, ".{fraction:06}").ok()?;
                        while self.buffer.bytes[self.buffer.len - 1] == b'0' {
                            self.buffer.len -= 1;
                        }
                    }
                    // chrono's %:z rounds seconds to the nearest minute.
                    let seconds = self.offset.local_minus_utc();
                    let minutes = (seconds.unsigned_abs() + 30) / 60;
                    write!(
                        self.buffer,
                        "{}{:02}:{:02}",
                        if seconds < 0 { '-' } else { '+' },
                        minutes / 60,
                        minutes % 60
                    )
                    .ok()?;
                }
                self.buffer.write_str("\"").ok()?;
            }
            15 | 16 => {
                let len = le(raw, base + 1, 4)?;
                let start = base + 5;
                raw.get(start..start.checked_add(len)?)?;
                self.mode = if value_header == 15 {
                    Mode::Binary {
                        start,
                        len,
                        at: 0,
                        phase: 0,
                    }
                } else {
                    Mode::String {
                        start,
                        len,
                        at: 0,
                        phase: 0,
                        key: false,
                    }
                };
            }
            20 => {
                let b = bytes.get(..16)?;
                write!(self.buffer,"\"{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}\"",
                b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7],b[8],b[9],b[10],b[11],b[12],b[13],b[14],b[15]).ok()?;
            }
            _ => return None,
        }
        Some(())
    }
}
trait ByteInteger {
    type Bytes;
}
impl ByteInteger for i8 {
    type Bytes = [u8; 1];
}
impl ByteInteger for i16 {
    type Bytes = [u8; 2];
}
impl ByteInteger for i32 {
    type Bytes = [u8; 4];
}
impl ByteInteger for i64 {
    type Bytes = [u8; 8];
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::variant::{VariantMetadata, VariantValue};
    use crate::value::variant_encode::encode_json_text_to_variant_bytes;

    fn generated(raw: &[u8], offset: FixedOffset) -> Option<String> {
        let mut cursor = VariantJsonCursor::new();
        if !cursor.start_with_offset(raw, offset) {
            return None;
        }
        let mut frames = Vec::new();
        let mut bytes = Vec::new();
        for _ in 0..10_000_000 {
            match cursor.step(raw, frames.last_mut()) {
                VariantJsonStep::Byte(byte) => bytes.push(byte),
                VariantJsonStep::Push(frame) => frames.push(frame),
                VariantJsonStep::Pop => {
                    assert!(frames.pop().is_some());
                }
                VariantJsonStep::Progress => {}
                VariantJsonStep::End => return Some(String::from_utf8(bytes).unwrap()),
                VariantJsonStep::Invalid => return None,
            }
        }
        panic!("Variant generator did not finish");
    }
    fn serialized(kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut value = vec![kind << 2];
        value.extend_from_slice(payload);
        VariantValue::create(VariantMetadata::empty().raw(), &value)
            .unwrap()
            .serialize()
    }
    fn parity(raw: &[u8]) {
        let offset = FixedOffset::east_opt(5 * 3600 + 1800).unwrap();
        let expected = VariantValue::from_serialized(raw)
            .ok()
            .and_then(|v| v.to_json(Some(offset)).ok());
        assert_eq!(generated(raw, offset), expected, "raw={raw:?}");
    }
    #[test]
    fn variant_json_cursor_all_existing_primitive_renderers() {
        for kind in 0..=2 {
            parity(&serialized(kind, &[]));
        }
        parity(&serialized(3, &(-120i8).to_le_bytes()));
        parity(&serialized(4, &(-31000i16).to_le_bytes()));
        parity(&serialized(5, &i32::MIN.to_le_bytes()));
        parity(&serialized(6, &i64::MAX.to_le_bytes()));
        for value in [
            0.0,
            -0.0,
            1.0,
            1.2345678901234567,
            f64::MIN_POSITIVE,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
        ] {
            parity(&serialized(7, &value.to_le_bytes()));
        }
        for value in [0.0f32, -0.0, 1.25, f32::INFINITY, f32::NAN] {
            parity(&serialized(14, &value.to_le_bytes()));
        }
        for scale in [0, 2, 18, 38, 255] {
            let mut a = vec![scale];
            a.extend_from_slice(&(-12300i32).to_le_bytes());
            parity(&serialized(8, &a));
            let mut a = vec![scale];
            a.extend_from_slice(&12300i64.to_le_bytes());
            parity(&serialized(9, &a));
            let mut a = vec![scale];
            a.extend_from_slice(&(-12300i128).to_le_bytes());
            parity(&serialized(10, &a));
        }
        for days in [0i32, -10000, 10000, i32::MIN, i32::MAX] {
            parity(&serialized(11, &days.to_le_bytes()));
        }
        for kind in [12, 13] {
            for micros in [0i64, -1, 123456, 1_234_567_890_123_456, i64::MAX] {
                parity(&serialized(kind, &micros.to_le_bytes()));
            }
        }
        for input in [b"".as_slice(), b"a", b"ab", b"abc", b"\0\xff\x80\"\\\n"] {
            let mut payload = (input.len() as u32).to_le_bytes().to_vec();
            payload.extend_from_slice(input);
            parity(&serialized(15, &payload));
            parity(&serialized(16, &payload));
        }
        parity(&serialized(
            20,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 254, 255],
        ));
        let mut minimum = vec![2];
        minimum.extend_from_slice(&i128::MIN.to_le_bytes());
        let raw = serialized(10, &minimum);
        parity(&raw);
        assert_eq!(
            generated(&raw, FixedOffset::east_opt(0).unwrap()).as_deref(),
            Some("-1701411834604692317316873037158841057.28")
        );
        for kind in [17, 18, 19] {
            let raw = serialized(kind, &0i64.to_le_bytes());
            parity(&raw);
            assert!(generated(&raw, FixedOffset::east_opt(0).unwrap()).is_none());
        }
    }
    #[test]
    fn variant_json_cursor_nested_keys_byte_escaping_and_long_streams() {
        for text in [
            r#"{"é":{"nested":[true,42,null,"ä\\\"\n"]}}"#,
            r#"[{},[],false,1.25,"literal"]"#,
        ] {
            parity(&encode_json_text_to_variant_bytes(text).unwrap());
        }
        let long = "a".repeat(20_000);
        let text = format!("{{\"{long}\":[\"{long}\",{{\"é\":true}}]}}");
        parity(&encode_json_text_to_variant_bytes(&text).unwrap());
        let raw = encode_json_text_to_variant_bytes(r#""é""#).unwrap();
        assert_eq!(
            generated(&raw, FixedOffset::east_opt(0).unwrap()).as_deref(),
            Some("\"Ã©\"")
        );
    }
    #[test]
    fn variant_json_cursor_validates_original_offset_and_metadata_rules() {
        let source = encode_json_text_to_variant_bytes(r#"{"x":[true,false]}"#).unwrap();
        for end in 0..source.len() {
            parity(&source[..end]);
        }
        let mut trailing = source.clone();
        trailing.extend_from_slice(b"ignored");
        parity(&trailing);
        let mut bad = source.clone();
        bad[4] = 2;
        parity(&bad);
        for position in 5..source.len() {
            let mut bad = source.clone();
            bad[position] = 0xff;
            // Old decoder may fail during rendering, never during borrowed construction allocation.
            parity(&bad);
        }
    }
    #[test]
    fn variant_json_cursor_serialized_null_reaches_utf8_carrier() {
        let raw = [4, 0, 0, 0, 1, 0, 0, 0];
        assert!(std::str::from_utf8(&raw).is_ok());
        assert_eq!(
            generated(&raw, FixedOffset::east_opt(0).unwrap()).as_deref(),
            Some("null")
        );
        parity(&raw);
    }
}
