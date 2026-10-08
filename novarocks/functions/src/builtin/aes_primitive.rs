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
//! ONE original mode/key/IV/OpenSSL primitive author; observation never changes crypto calls.
use super::md5_shared::Observation;
use openssl::symm::{Cipher, Crypter, Mode};
use std::convert::Infallible;
const DEFAULT_IV: &[u8] = b"STARROCKS_16BYTE";
const GCM_TAG_SIZE: usize = 16;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AesMode {
    Aes128Ecb,
    Aes192Ecb,
    Aes256Ecb,
    Aes128Cbc,
    Aes192Cbc,
    Aes256Cbc,
    Aes128Cfb,
    Aes192Cfb,
    Aes256Cfb,
    Aes128Cfb1,
    Aes192Cfb1,
    Aes256Cfb1,
    Aes128Cfb8,
    Aes192Cfb8,
    Aes256Cfb8,
    Aes128Cfb128,
    Aes192Cfb128,
    Aes256Cfb128,
    Aes128Ofb,
    Aes192Ofb,
    Aes256Ofb,
    Aes128Ctr,
    Aes192Ctr,
    Aes256Ctr,
    Aes128Gcm,
    Aes192Gcm,
    Aes256Gcm,
}

impl AesMode {
    pub fn parse(bytes: &[u8]) -> Self {
        infallible(Self::parse_observed(bytes, &mut |_| {
            Ok::<(), Infallible>(())
        }))
    }
    pub fn parse_observed<E>(
        bytes: &[u8],
        observe: &mut dyn FnMut(Observation) -> Result<(), E>,
    ) -> Result<Self, E> {
        for _ in bytes {
            observe(Observation::Step)?;
        }
        let mode = if bytes.is_empty() {
            String::from_utf8_lossy(bytes).to_ascii_uppercase()
        } else {
            opaque(observe, || {
                String::from_utf8_lossy(bytes).to_ascii_uppercase()
            })?
        };
        Ok(match mode.as_str() {
            "AES_192_ECB" => Self::Aes192Ecb,
            "AES_256_ECB" => Self::Aes256Ecb,
            "AES_128_CBC" => Self::Aes128Cbc,
            "AES_192_CBC" => Self::Aes192Cbc,
            "AES_256_CBC" => Self::Aes256Cbc,
            "AES_128_CFB" => Self::Aes128Cfb,
            "AES_192_CFB" => Self::Aes192Cfb,
            "AES_256_CFB" => Self::Aes256Cfb,
            "AES_128_CFB1" => Self::Aes128Cfb1,
            "AES_192_CFB1" => Self::Aes192Cfb1,
            "AES_256_CFB1" => Self::Aes256Cfb1,
            "AES_128_CFB8" => Self::Aes128Cfb8,
            "AES_192_CFB8" => Self::Aes192Cfb8,
            "AES_256_CFB8" => Self::Aes256Cfb8,
            "AES_128_CFB128" => Self::Aes128Cfb128,
            "AES_192_CFB128" => Self::Aes192Cfb128,
            "AES_256_CFB128" => Self::Aes256Cfb128,
            "AES_128_OFB" => Self::Aes128Ofb,
            "AES_192_OFB" => Self::Aes192Ofb,
            "AES_256_OFB" => Self::Aes256Ofb,
            "AES_128_CTR" => Self::Aes128Ctr,
            "AES_192_CTR" => Self::Aes192Ctr,
            "AES_256_CTR" => Self::Aes256Ctr,
            "AES_128_GCM" => Self::Aes128Gcm,
            "AES_192_GCM" => Self::Aes192Gcm,
            "AES_256_GCM" => Self::Aes256Gcm,
            _ => Self::Aes128Ecb,
        })
    }

    pub fn is_gcm(self) -> bool {
        matches!(self, Self::Aes128Gcm | Self::Aes192Gcm | Self::Aes256Gcm)
    }

    pub fn is_stream(self) -> bool {
        matches!(
            self,
            Self::Aes128Cfb
                | Self::Aes192Cfb
                | Self::Aes256Cfb
                | Self::Aes128Cfb1
                | Self::Aes192Cfb1
                | Self::Aes256Cfb1
                | Self::Aes128Cfb8
                | Self::Aes192Cfb8
                | Self::Aes256Cfb8
                | Self::Aes128Cfb128
                | Self::Aes192Cfb128
                | Self::Aes256Cfb128
                | Self::Aes128Ofb
                | Self::Aes192Ofb
                | Self::Aes256Ofb
                | Self::Aes128Ctr
                | Self::Aes192Ctr
                | Self::Aes256Ctr
        )
    }

    pub fn is_ecb(self) -> bool {
        matches!(self, Self::Aes128Ecb | Self::Aes192Ecb | Self::Aes256Ecb)
    }

    fn key_len(self) -> usize {
        match self {
            Self::Aes128Ecb
            | Self::Aes128Cbc
            | Self::Aes128Cfb
            | Self::Aes128Cfb1
            | Self::Aes128Cfb8
            | Self::Aes128Cfb128
            | Self::Aes128Ofb
            | Self::Aes128Ctr
            | Self::Aes128Gcm => 16,
            Self::Aes192Ecb
            | Self::Aes192Cbc
            | Self::Aes192Cfb
            | Self::Aes192Cfb1
            | Self::Aes192Cfb8
            | Self::Aes192Cfb128
            | Self::Aes192Ofb
            | Self::Aes192Ctr
            | Self::Aes192Gcm => 24,
            Self::Aes256Ecb
            | Self::Aes256Cbc
            | Self::Aes256Cfb
            | Self::Aes256Cfb1
            | Self::Aes256Cfb8
            | Self::Aes256Cfb128
            | Self::Aes256Ofb
            | Self::Aes256Ctr
            | Self::Aes256Gcm => 32,
        }
    }

    fn cipher(self) -> Cipher {
        match self {
            Self::Aes128Ecb => Cipher::aes_128_ecb(),
            Self::Aes192Ecb => Cipher::aes_192_ecb(),
            Self::Aes256Ecb => Cipher::aes_256_ecb(),
            Self::Aes128Cbc => Cipher::aes_128_cbc(),
            Self::Aes192Cbc => Cipher::aes_192_cbc(),
            Self::Aes256Cbc => Cipher::aes_256_cbc(),
            Self::Aes128Cfb | Self::Aes128Cfb128 => Cipher::aes_128_cfb128(),
            Self::Aes192Cfb | Self::Aes192Cfb128 => Cipher::aes_192_cfb128(),
            Self::Aes256Cfb | Self::Aes256Cfb128 => Cipher::aes_256_cfb128(),
            Self::Aes128Cfb1 => Cipher::aes_128_cfb1(),
            Self::Aes192Cfb1 => Cipher::aes_192_cfb1(),
            Self::Aes256Cfb1 => Cipher::aes_256_cfb1(),
            Self::Aes128Cfb8 => Cipher::aes_128_cfb8(),
            Self::Aes192Cfb8 => Cipher::aes_192_cfb8(),
            Self::Aes256Cfb8 => Cipher::aes_256_cfb8(),
            Self::Aes128Ofb => Cipher::aes_128_ofb(),
            Self::Aes192Ofb => Cipher::aes_192_ofb(),
            Self::Aes256Ofb => Cipher::aes_256_ofb(),
            Self::Aes128Ctr => Cipher::aes_128_ctr(),
            Self::Aes192Ctr => Cipher::aes_192_ctr(),
            Self::Aes256Ctr => Cipher::aes_256_ctr(),
            Self::Aes128Gcm => Cipher::aes_128_gcm(),
            Self::Aes192Gcm => Cipher::aes_192_gcm(),
            Self::Aes256Gcm => Cipher::aes_256_gcm(),
        }
    }
}

fn infallible<T>(r: Result<T, Infallible>) -> T {
    match r {
        Ok(v) => v,
        Err(never) => match never {},
    }
}
fn opaque<E, T>(
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
    operation: impl FnOnce() -> T,
) -> Result<T, E> {
    observe(Observation::OpaqueBoundary)?;
    let value = operation();
    observe(Observation::OpaqueBoundary)?;
    Ok(value)
}
fn zero_vector<E>(
    len: usize,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Vec<u8>, E> {
    if len == 0 {
        return Ok(vec![]);
    }
    opaque(observe, || vec![0u8; len])
}
fn capacity_vector<E>(
    len: usize,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Vec<u8>, E> {
    if len == 0 {
        return Ok(Vec::with_capacity(0));
    }
    opaque(observe, || Vec::with_capacity(len))
}
fn extend<E>(
    out: &mut Vec<u8>,
    bytes: &[u8],
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<(), E> {
    for _ in bytes {
        observe(Observation::Step)?;
    }
    if !bytes.is_empty() {
        opaque(observe, || out.extend_from_slice(bytes))?;
    } else {
        out.extend_from_slice(bytes);
    }
    Ok(())
}
fn build_aes_key<E>(
    input_key: &[u8],
    key_size: usize,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Vec<u8>, E> {
    let mut key = zero_vector(key_size, observe)?;
    for (idx, b) in input_key.iter().enumerate() {
        observe(Observation::Step)?;
        key[idx % key_size] ^= b;
    }
    Ok(key)
}
fn build_iv<E>(
    iv_input: Option<&[u8]>,
    iv_length: usize,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Vec<u8>, E> {
    let mut iv = zero_vector(iv_length, observe)?;
    if iv_length == 0 {
        return Ok(iv);
    }
    if let Some(input) = iv_input.filter(|v| !v.is_empty()) {
        let copy_len = input.len().min(iv_length);
        for _ in 0..copy_len {
            observe(Observation::Step)?;
        }
        opaque(observe, || {
            iv[..copy_len].copy_from_slice(&input[..copy_len])
        })?;
    } else {
        let copy_len = DEFAULT_IV.len().min(iv_length);
        for _ in 0..copy_len {
            observe(Observation::Step)?;
        }
        opaque(observe, || {
            iv[..copy_len].copy_from_slice(&DEFAULT_IV[..copy_len])
        })?;
    }
    Ok(iv)
}
pub fn aes_encrypt_raw_observed<E>(
    mode: AesMode,
    source: &[u8],
    key: &[u8],
    iv_input: Option<&[u8]>,
    aad: Option<&[u8]>,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Option<Vec<u8>>, E> {
    let cipher = mode.cipher();
    let key = build_aes_key(key, mode.key_len(), observe)?;
    let iv_len = cipher.iv_len().unwrap_or(0);
    let iv = build_iv(iv_input, iv_len, observe)?;

    if mode.is_gcm() {
        let Some(mut crypter) = opaque(observe, || {
            Crypter::new(cipher, Mode::Encrypt, &key, Some(&iv))
        })?
        .ok() else {
            return Ok(None);
        };
        opaque(observe, || crypter.pad(false))?;

        if let Some(aad) = aad.filter(|v| !v.is_empty()) {
            for _ in aad {
                observe(Observation::Step)?;
            }
            if opaque(observe, || crypter.aad_update(aad))?.is_err() {
                return Ok(None);
            }
        }

        let mut ciphertext = zero_vector(source.len() + cipher.block_size(), observe)?;
        for _ in source {
            observe(Observation::Step)?;
        }
        let Some(count) = opaque(observe, || crypter.update(source, &mut ciphertext))?.ok() else {
            return Ok(None);
        };
        let Some(rest) = opaque(observe, || crypter.finalize(&mut ciphertext[count..]))?.ok()
        else {
            return Ok(None);
        };
        ciphertext.truncate(count + rest);

        let mut tag = [0u8; GCM_TAG_SIZE];
        if opaque(observe, || crypter.get_tag(&mut tag))?.is_err() {
            return Ok(None);
        }

        let mut out = capacity_vector(iv.len() + ciphertext.len() + tag.len(), observe)?;
        extend(&mut out, &iv, observe)?;
        extend(&mut out, &ciphertext, observe)?;
        extend(&mut out, &tag, observe)?;
        return Ok(Some(out));
    }

    let iv_opt = if iv_len > 0 {
        Some(iv.as_slice())
    } else {
        None
    };
    let Some(mut crypter) = opaque(observe, || {
        Crypter::new(cipher, Mode::Encrypt, &key, iv_opt)
    })?
    .ok() else {
        return Ok(None);
    };
    opaque(observe, || crypter.pad(!mode.is_stream()))?;

    let mut out = zero_vector(source.len() + cipher.block_size(), observe)?;
    for _ in source {
        observe(Observation::Step)?;
    }
    let Some(count) = opaque(observe, || crypter.update(source, &mut out))?.ok() else {
        return Ok(None);
    };
    let Some(rest) = opaque(observe, || crypter.finalize(&mut out[count..]))?.ok() else {
        return Ok(None);
    };
    out.truncate(count + rest);
    Ok(Some(out))
}

pub fn aes_decrypt_raw_observed<E>(
    mode: AesMode,
    encrypted: &[u8],
    key: &[u8],
    iv_input: Option<&[u8]>,
    aad: Option<&[u8]>,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Option<Vec<u8>>, E> {
    let cipher = mode.cipher();
    let key = build_aes_key(key, mode.key_len(), observe)?;
    let iv_len = cipher.iv_len().unwrap_or(0);

    if mode.is_gcm() {
        if encrypted.len() < iv_len + GCM_TAG_SIZE {
            return Ok(None);
        }

        let iv = &encrypted[..iv_len];
        let ciphertext_end = encrypted.len() - GCM_TAG_SIZE;
        let ciphertext = &encrypted[iv_len..ciphertext_end];
        let tag = &encrypted[ciphertext_end..];

        let Some(mut crypter) = opaque(observe, || {
            Crypter::new(cipher, Mode::Decrypt, &key, Some(iv))
        })?
        .ok() else {
            return Ok(None);
        };
        opaque(observe, || crypter.pad(false))?;

        if let Some(aad) = aad.filter(|v| !v.is_empty()) {
            for _ in aad {
                observe(Observation::Step)?;
            }
            if opaque(observe, || crypter.aad_update(aad))?.is_err() {
                return Ok(None);
            }
        }

        if opaque(observe, || crypter.set_tag(tag))?.is_err() {
            return Ok(None);
        }

        let mut out = zero_vector(ciphertext.len() + cipher.block_size(), observe)?;
        for _ in ciphertext {
            observe(Observation::Step)?;
        }
        let Some(count) = opaque(observe, || crypter.update(ciphertext, &mut out))?.ok() else {
            return Ok(None);
        };
        let Some(rest) = opaque(observe, || crypter.finalize(&mut out[count..]))?.ok() else {
            return Ok(None);
        };
        out.truncate(count + rest);
        return Ok(Some(out));
    }

    let iv = build_iv(iv_input, iv_len, observe)?;
    let iv_opt = if iv_len > 0 {
        Some(iv.as_slice())
    } else {
        None
    };
    let Some(mut crypter) = opaque(observe, || {
        Crypter::new(cipher, Mode::Decrypt, &key, iv_opt)
    })?
    .ok() else {
        return Ok(None);
    };
    opaque(observe, || crypter.pad(!mode.is_stream()))?;

    let mut out = zero_vector(encrypted.len() + cipher.block_size(), observe)?;
    for _ in encrypted {
        observe(Observation::Step)?;
    }
    let Some(count) = opaque(observe, || crypter.update(encrypted, &mut out))?.ok() else {
        return Ok(None);
    };
    let Some(rest) = opaque(observe, || crypter.finalize(&mut out[count..]))?.ok() else {
        return Ok(None);
    };
    out.truncate(count + rest);
    Ok(Some(out))
}

pub fn aes_encrypt_raw(
    mode: AesMode,
    source: &[u8],
    key: &[u8],
    iv: Option<&[u8]>,
    aad: Option<&[u8]>,
) -> Option<Vec<u8>> {
    infallible(aes_encrypt_raw_observed(
        mode,
        source,
        key,
        iv,
        aad,
        &mut |_| Ok::<(), Infallible>(()),
    ))
}
pub fn aes_decrypt_raw(
    mode: AesMode,
    source: &[u8],
    key: &[u8],
    iv: Option<&[u8]>,
    aad: Option<&[u8]>,
) -> Option<Vec<u8>> {
    infallible(aes_decrypt_raw_observed(
        mode,
        source,
        key,
        iv,
        aad,
        &mut |_| Ok::<(), Infallible>(()),
    ))
}
