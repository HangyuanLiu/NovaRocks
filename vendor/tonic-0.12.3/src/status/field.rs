//! Status transforms directly in the original bounded HTTP field arena.

use super::{Status, ENCODING_SET};
use base64::Engine as _;
use bytes::Bytes;
use http::header::{HeaderFieldAllocationPool, HeaderFieldFillError, HeaderValue};
use percent_encoding::{percent_decode, percent_encode};
use std::{fmt, fmt::Write as _, ops::Deref};

#[derive(Clone)]
pub(super) enum Message {
    Owned(String),
    Shared(Bytes),
    Static(&'static str),
}
impl Deref for Message {
    type Target = str;
    fn deref(&self) -> &str {
        match self {
            Self::Owned(message) => message,
            Self::Static(message) => message,
            // SAFETY: Shared is constructed only after checking the immutable arena bytes
            // as UTF-8, or from the formatter's UTF-8 string writes. Bytes aliases are immutable.
            Self::Shared(message) => unsafe { std::str::from_utf8_unchecked(message) },
        }
    }
}
impl From<String> for Message {
    fn from(message: String) -> Self {
        Self::Owned(message)
    }
}
impl fmt::Debug for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, formatter)
    }
}

fn capacity_error() -> Status {
    Status::field_error(
        super::Code::ResourceExhausted,
        "HTTP status field capacity exhausted",
    )
}
fn fill_error<E>(error: HeaderFieldFillError<E>) -> Status {
    match error {
        HeaderFieldFillError::TooLarge | HeaderFieldFillError::Exhausted => capacity_error(),
        HeaderFieldFillError::Fill(_) => Status::field_error(
            super::Code::Internal,
            "HTTP status field transformation failed",
        ),
    }
}
struct Count(usize);
impl fmt::Write for Count {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}
struct Output<'a> {
    bytes: &'a mut [u8],
    written: usize,
}
impl fmt::Write for Output<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let end = self.written.checked_add(text.len()).ok_or(fmt::Error)?;
        self.bytes
            .get_mut(self.written..end)
            .ok_or(fmt::Error)?
            .copy_from_slice(text.as_bytes());
        self.written = end;
        Ok(())
    }
}
pub(super) fn formatted(
    pool: &HeaderFieldAllocationPool,
    args: fmt::Arguments<'_>,
) -> Result<Bytes, Status> {
    let mut count = Count(0);
    count.write_fmt(args).map_err(|_| capacity_error())?;
    pool.try_fill(count.0, |output| {
        let expected = output.len();
        let mut output = Output {
            bytes: output,
            written: 0,
        };
        output.write_fmt(args)?;
        if output.written != expected {
            return Err(fmt::Error);
        }
        Ok(())
    })
    .map_err(fill_error)
}

/// Preserve permissive percent decoding, then validate UTF-8 without a String/Cow copy.
pub(super) fn decode_message(
    pool: &HeaderFieldAllocationPool,
    header: Option<&HeaderValue>,
) -> Result<(Message, bool), Status> {
    let Some(header) = header else {
        return Ok((Message::Static(""), false));
    };
    let decoded = percent_decode(header.as_bytes());
    let len = decoded.clone().count();
    match pool.try_fill(len, |output| {
        for (destination, source) in output.iter_mut().zip(decoded) {
            *destination = source;
        }
        std::str::from_utf8(output).map(|_| ())
    }) {
        Ok(message) => Ok((Message::Shared(message), false)),
        Err(HeaderFieldFillError::Fill(error)) => {
            tracing::warn!("Error deserializing status message header: {}", error);
            let message = formatted(
                pool,
                format_args!("Error deserializing status message header: {}", error),
            )?;
            Ok((Message::Shared(message), true))
        }
        Err(error) => Err(fill_error(error)),
    }
}

pub(super) fn decode_details(
    pool: &HeaderFieldAllocationPool,
    header: Option<&HeaderValue>,
) -> Result<Bytes, Status> {
    let Some(header) = header else {
        return Ok(Bytes::new());
    };
    let input = header.as_bytes();
    // Valid inputs have at most two trailing padding bytes. Count their exact
    // output length without imposing a different padding/alphabet policy: the
    // original decoder below still validates every byte and trailing bit.
    let padding = input
        .iter()
        .rev()
        .take(2)
        .take_while(|&&b| b == b'=')
        .count();
    let symbols = input.len() - padding;
    let len = (symbols / 4)
        .checked_mul(3)
        .and_then(|complete| complete.checked_add((symbols % 4) * 6 / 8))
        .ok_or_else(capacity_error)?;
    let mut written = 0;
    let bytes = pool
        .try_fill::<base64::DecodeSliceError>(len, |output| {
            written = crate::util::base64::STANDARD.decode_slice(input, output)?;
            Ok(())
        })
        .map_err(|error| match error {
            HeaderFieldFillError::Fill(_) => Status::field_error(
                super::Code::Internal,
                "Invalid grpc-status-details-bin header",
            ),
            error => fill_error(error),
        })?;
    Ok(bytes.slice(..written))
}

pub(super) fn encode_message(
    pool: &HeaderFieldAllocationPool,
    message: &str,
) -> Result<Bytes, Status> {
    let parts = percent_encode(message.as_bytes(), ENCODING_SET);
    let len = parts
        .clone()
        .try_fold(0_usize, |len, part| len.checked_add(part.len()))
        .ok_or_else(capacity_error)?;
    pool.try_fill::<fmt::Error>(len, |output| {
        let mut output = Output {
            bytes: output,
            written: 0,
        };
        for part in parts {
            output.write_str(part)?;
        }
        Ok(())
    })
    .map_err(fill_error)
}

pub(super) fn encode_details(
    pool: &HeaderFieldAllocationPool,
    details: &[u8],
) -> Result<Bytes, Status> {
    let len = base64::encoded_len(details.len(), false).ok_or_else(capacity_error)?;
    pool.try_fill(len, |output| {
        crate::util::base64::STANDARD_NO_PAD
            .encode_slice(details, output)
            .map(|_| ())
    })
    .map_err(fill_error)
}
