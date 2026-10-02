use super::compression::{
    compress, CompressionEncoding, CompressionSettings, SingleMessageCompressionOverride,
};
use super::{BufferSettings, EncodeBuf, Encoder, DEFAULT_MAX_SEND_MESSAGE_SIZE, HEADER_SIZE};
use crate::Status;
use bytes::{BufMut, Bytes, BytesMut};
use http::{header::HeaderFieldAllocationPool, HeaderMap};
use http_body::{Body, Frame};
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio_stream::{adapters::Fuse, Stream, StreamExt};

/// Combinator for efficient encoding of messages into reasonably sized buffers.
/// EncodedBytes encodes ready messages from its delegate stream into a BytesMut,
/// splitting off and yielding a buffer when either:
///  * The delegate stream polls as not ready, or
///  * The encoded buffer surpasses YIELD_THRESHOLD.
#[pin_project(project = EncodedBytesProj)]
#[derive(Debug)]
struct EncodedBytes<T, U> {
    #[pin]
    source: Fuse<U>,
    encoder: T,
    compression_encoding: Option<CompressionEncoding>,
    max_message_size: Option<usize>,
    buf: BytesMut,
    uncompression_buf: BytesMut,
    error: Option<Status>,
    diagnostic_fields: Option<HeaderFieldAllocationPool>,
}

impl<T: Encoder, U: Stream> EncodedBytes<T, U> {
    fn new(
        encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        compression_override: SingleMessageCompressionOverride,
        max_message_size: Option<usize>,
    ) -> Self {
        let buffer_settings = encoder.buffer_settings();
        let buf = BytesMut::with_capacity(buffer_settings.buffer_size);

        let compression_encoding =
            if compression_override == SingleMessageCompressionOverride::Disable {
                None
            } else {
                compression_encoding
            };

        let uncompression_buf = if compression_encoding.is_some() {
            BytesMut::with_capacity(buffer_settings.buffer_size)
        } else {
            BytesMut::new()
        };

        Self {
            source: source.fuse(),
            encoder,
            compression_encoding,
            max_message_size,
            buf,
            uncompression_buf,
            error: None,
            diagnostic_fields: None,
        }
    }
}

impl<T, U> Stream for EncodedBytes<T, U>
where
    T: Encoder<Error = Status>,
    U: Stream<Item = Result<T::Item, Status>>,
{
    type Item = Result<Bytes, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let EncodedBytesProj {
            mut source,
            encoder,
            compression_encoding,
            max_message_size,
            buf,
            uncompression_buf,
            error,
            diagnostic_fields,
        } = self.project();
        let buffer_settings = encoder.buffer_settings();

        if let Some(status) = error.take() {
            return Poll::Ready(Some(Err(status)));
        }

        loop {
            match source.as_mut().poll_next(cx) {
                Poll::Pending if buf.is_empty() => {
                    return Poll::Pending;
                }
                Poll::Ready(None) if buf.is_empty() => {
                    return Poll::Ready(None);
                }
                Poll::Pending | Poll::Ready(None) => {
                    return Poll::Ready(Some(Ok(buf.split_to(buf.len()).freeze())));
                }
                Poll::Ready(Some(Ok(item))) => {
                    if let Err(status) = encode_item(
                        encoder,
                        buf,
                        uncompression_buf,
                        EncodeItemSettings {
                            compression_encoding: *compression_encoding,
                            max_message_size: *max_message_size,
                            buffer_settings,
                            diagnostic_fields: diagnostic_fields.as_ref(),
                        },
                        item,
                    ) {
                        return Poll::Ready(Some(Err(status)));
                    }

                    if buf.len() >= buffer_settings.yield_threshold {
                        return Poll::Ready(Some(Ok(buf.split_to(buf.len()).freeze())));
                    }
                }
                Poll::Ready(Some(Err(status))) => {
                    if buf.is_empty() {
                        return Poll::Ready(Some(Err(status)));
                    }
                    *error = Some(status);
                    return Poll::Ready(Some(Ok(buf.split_to(buf.len()).freeze())));
                }
            }
        }
    }
}

struct EncodeItemSettings<'a> {
    compression_encoding: Option<CompressionEncoding>,
    max_message_size: Option<usize>,
    buffer_settings: BufferSettings,
    diagnostic_fields: Option<&'a HeaderFieldAllocationPool>,
}

fn encode_item<T>(
    encoder: &mut T,
    buf: &mut BytesMut,
    uncompression_buf: &mut BytesMut,
    settings: EncodeItemSettings<'_>,
    item: T::Item,
) -> Result<(), Status>
where
    T: Encoder<Error = Status>,
{
    let EncodeItemSettings {
        compression_encoding,
        max_message_size,
        buffer_settings,
        diagnostic_fields,
    } = settings;
    let offset = buf.len();

    buf.reserve(HEADER_SIZE);
    unsafe {
        buf.advance_mut(HEADER_SIZE);
    }

    if let Some(encoding) = compression_encoding {
        uncompression_buf.clear();

        encoder
            .encode(item, &mut EncodeBuf::new(uncompression_buf))
            .map_err(|err| {
                Status::formatted_with_pool(
                    crate::Code::Internal,
                    format_args!("Error encoding: {}", err),
                    diagnostic_fields,
                )
            })?;

        let uncompressed_len = uncompression_buf.len();

        compress(
            CompressionSettings {
                encoding,
                buffer_growth_interval: buffer_settings.buffer_size,
            },
            uncompression_buf,
            buf,
            uncompressed_len,
        )
        .map_err(|err| {
            Status::formatted_with_pool(
                crate::Code::Internal,
                format_args!("Error compressing: {}", err),
                diagnostic_fields,
            )
        })?;
    } else {
        encoder
            .encode(item, &mut EncodeBuf::new(buf))
            .map_err(|err| {
                Status::formatted_with_pool(
                    crate::Code::Internal,
                    format_args!("Error encoding: {}", err),
                    diagnostic_fields,
                )
            })?;
    }

    // now that we know length, we can write the header
    finish_encoding(
        compression_encoding,
        max_message_size,
        diagnostic_fields,
        &mut buf[offset..],
    )
}

fn finish_encoding(
    compression_encoding: Option<CompressionEncoding>,
    max_message_size: Option<usize>,
    diagnostic_fields: Option<&HeaderFieldAllocationPool>,
    buf: &mut [u8],
) -> Result<(), Status> {
    let len = buf.len() - HEADER_SIZE;
    let limit = max_message_size.unwrap_or(DEFAULT_MAX_SEND_MESSAGE_SIZE);
    if len > limit {
        return Err(Status::formatted_with_pool(
            crate::Code::OutOfRange,
            format_args!(
                "Error, encoded message length too large: found {} bytes, the limit is: {} bytes",
                len, limit
            ),
            diagnostic_fields,
        ));
    }

    if len > u32::MAX as usize {
        return Err(Status::formatted_with_pool(
            crate::Code::ResourceExhausted,
            format_args!("Cannot return body with more than 4GB of data but got {len} bytes"),
            diagnostic_fields,
        ));
    }
    {
        let mut buf = &mut buf[..HEADER_SIZE];
        buf.put_u8(compression_encoding.is_some() as u8);
        buf.put_u32(len as u32);
    }

    Ok(())
}

#[derive(Debug)]
enum Role {
    Client,
    Server,
}

/// A specialized implementation of [Body] for encoding [Result<Bytes, Status>].
#[pin_project]
#[derive(Debug)]
pub struct EncodeBody<T, U> {
    #[pin]
    inner: EncodedBytes<T, U>,
    state: EncodeState,
}

#[derive(Debug)]
struct EncodeState {
    error: Option<Status>,
    role: Role,
    is_end_stream: bool,
    trailers: Option<HeaderMap>,
}

impl<T: Encoder, U: Stream> EncodeBody<T, U> {
    /// Turns a stream of grpc messages into [EncodeBody] which is used by grpc clients for
    /// turning the messages into http frames for sending over the network.
    pub fn new_client(
        encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        max_message_size: Option<usize>,
    ) -> Self {
        Self {
            inner: EncodedBytes::new(
                encoder,
                source,
                compression_encoding,
                SingleMessageCompressionOverride::default(),
                max_message_size,
            ),
            state: EncodeState {
                error: None,
                role: Role::Client,
                is_end_stream: false,
                trailers: None,
            },
        }
    }

    /// Turns a stream of grpc results (message or error status) into [EncodeBody] which is used by grpc
    /// servers for turning the messages into http frames for sending over the network.
    pub fn new_server(
        encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        compression_override: SingleMessageCompressionOverride,
        max_message_size: Option<usize>,
    ) -> Self {
        Self {
            inner: EncodedBytes::new(
                encoder,
                source,
                compression_encoding,
                compression_override,
                max_message_size,
            ),
            state: EncodeState {
                error: None,
                role: Role::Server,
                is_end_stream: false,
                trailers: None,
            },
        }
    }

    /// Encode server messages using one caller-preallocated trailer map.
    ///
    /// The caller obtains the complete original map and field grants before
    /// constructing this body. EOF and errors consume this same map without
    /// obtaining another map or falling back to ordinary header allocation.
    /// An unpolled or canceled body retains the map until its actual drop.
    /// Message/compression buffers and body/source metadata are separate owners.
    pub fn new_server_with_trailers(
        encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        compression_override: SingleMessageCompressionOverride,
        max_message_size: Option<usize>,
        trailers: HeaderMap,
    ) -> Self {
        let mut body = Self::new_server(
            encoder,
            source,
            compression_encoding,
            compression_override,
            max_message_size,
        );
        body.inner.diagnostic_fields = trailers.field_allocation_pool().cloned();
        body.state.trailers = Some(trailers);
        body
    }
}

impl EncodeState {
    fn trailers(&mut self) -> Option<Result<HeaderMap, Status>> {
        match self.role {
            Role::Client => None,
            Role::Server => {
                if self.is_end_stream {
                    return None;
                }

                self.is_end_stream = true;
                let status = if let Some(status) = self.error.take() {
                    status
                } else if self.trailers.is_some() {
                    Status::from_static(crate::Code::Ok, "")
                } else {
                    Status::ok("")
                };
                Some(self.encode_trailers(status))
            }
        }
    }

    fn encode_trailers(&mut self, status: Status) -> Result<HeaderMap, Status> {
        match self.trailers.take() {
            Some(trailers) => status.into_trailers_with_headers(trailers),
            None => status.to_header_map(),
        }
    }
}

impl<T, U> Body for EncodeBody<T, U>
where
    T: Encoder<Error = Status>,
    U: Stream<Item = Result<T::Item, Status>>,
{
    type Data = Bytes;
    type Error = Status;

    fn is_end_stream(&self) -> bool {
        self.state.is_end_stream
    }

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut self_proj = self.project();
        if self_proj.state.is_end_stream {
            return Poll::Ready(None);
        }
        match ready!(self_proj.inner.as_mut().poll_next(cx)) {
            Some(Ok(d)) => Some(Ok(Frame::data(d))).into(),
            Some(Err(status)) => match self_proj.state.role {
                Role::Client => Some(Err(status)).into(),
                Role::Server => {
                    self_proj.state.is_end_stream = true;
                    // Encoder failure may leave a partial message in the fixed
                    // codec staging buffers. Never publish it on a later poll.
                    let inner = self_proj.inner.project();
                    inner.buf.clear();
                    inner.uncompression_buf.clear();
                    *inner.error = None;
                    Some(self_proj.state.encode_trailers(status).map(Frame::trailers)).into()
                }
            },
            None => self_proj
                .state
                .trailers()
                .map(|t| t.map(Frame::trailers))
                .into(),
        }
    }
}
