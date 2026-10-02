use crate::frame::{self, Frame, Kind, Reason};
use crate::frame::{
    DEFAULT_MAX_FRAME_SIZE, DEFAULT_SETTINGS_HEADER_TABLE_SIZE, MAX_MAX_FRAME_SIZE,
};
use crate::proto::Error;

use crate::hpack;

use futures_core::Stream;

use bytes::{Buf, BytesMut};

use std::io;

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;
use tokio_util::codec::FramedRead as InnerFramedRead;
use tokio_util::codec::{LengthDelimitedCodec, LengthDelimitedCodecError};

// 16 MB "sane default" taken from golang http2
const DEFAULT_SETTINGS_MAX_HEADER_LIST_SIZE: usize = 16 << 20;

#[derive(Debug)]
pub struct FramedRead<T> {
    inner: ReadKind<T>,

    // hpack decoder state
    hpack: hpack::Decoder,

    max_header_list_size: usize,

    max_continuation_frames: usize,

    partial: Option<Partial>,
    max_header_block_size: Option<usize>,
    receive_pool: Option<crate::ReceiveBufferPool>,
    goaway_pool: Option<crate::ReceiveBufferPool>,
    header_buffer: Option<crate::receive_header::BoundHeaderBlockBuffer>,
}

enum ReadKind<T> {
    Default(InnerFramedRead<T, LengthDelimitedCodec>),
    Fixed(crate::receive_frame::FixedFrameRead<T>),
}
impl<T> std::fmt::Debug for ReadKind<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Default(_) => "DefaultFramedRead",
            Self::Fixed(_) => "FixedFrameRead",
        })
    }
}

/// Partially loaded headers frame
#[derive(Debug)]
struct Partial {
    /// Empty frame
    frame: Continuable,

    /// Partial header payload
    buf: HeaderInput,

    continuation_frames_count: usize,
    encoded_len: usize,
}

#[derive(Debug)]
enum HeaderInput {
    Owned(BytesMut),
    Fixed,
}

#[derive(Debug)]
enum Continuable {
    Headers(frame::Headers),
    PushPromise(frame::PushPromise),
}

pub(crate) struct HeaderBuffers {
    pub(crate) encoded: crate::receive_header::BoundHeaderBlockBuffer,
    pub(crate) fields: Option<crate::ReceiveHeaderFieldPool>,
    pub(crate) max_list: usize,
    pub(crate) max_encoded: usize,
}

impl<T> FramedRead<T> {
    pub fn new(inner: InnerFramedRead<T, LengthDelimitedCodec>) -> FramedRead<T> {
        let max_frame = inner.decoder().max_frame_length();
        Self::with_reader(ReadKind::Default(inner), max_frame, None)
    }

    pub(crate) fn with_headers(
        inner: InnerFramedRead<T, LengthDelimitedCodec>,
        headers: Option<HeaderBuffers>,
    ) -> Self {
        let max_frame = inner.decoder().max_frame_length();
        Self::with_reader(ReadKind::Default(inner), max_frame, headers)
    }

    pub(crate) fn with_receive_frame_buffer(
        io: T,
        buffer: crate::receive_frame::BoundFrameBuffer,
        max_frame: usize,
        headers: Option<HeaderBuffers>,
    ) -> Self {
        Self::with_reader(
            ReadKind::Fixed(crate::receive_frame::FixedFrameRead::new(
                io, buffer, max_frame,
            )),
            max_frame,
            headers,
        )
    }

    fn with_reader(inner: ReadKind<T>, max_frame: usize, headers: Option<HeaderBuffers>) -> Self {
        let (hpack, header_buffer, max_header_list_size, max_header_block_size) = match headers {
            Some(headers) => (
                hpack::Decoder::new_bounded(
                    DEFAULT_SETTINGS_HEADER_TABLE_SIZE,
                    headers.max_list,
                    headers.max_encoded,
                    headers.fields,
                ),
                Some(headers.encoded),
                headers.max_list,
                Some(headers.max_encoded),
            ),
            None => (
                hpack::Decoder::new(DEFAULT_SETTINGS_HEADER_TABLE_SIZE),
                None,
                DEFAULT_SETTINGS_MAX_HEADER_LIST_SIZE,
                None,
            ),
        };
        let max_continuation_frames = calc_max_continuation_frames(max_header_list_size, max_frame);
        FramedRead {
            inner,
            hpack,
            max_header_list_size,
            max_continuation_frames,
            partial: None,
            max_header_block_size,
            receive_pool: None,
            goaway_pool: None,
            header_buffer,
        }
    }

    pub fn get_ref(&self) -> &T {
        match &self.inner {
            ReadKind::Default(inner) => inner.get_ref(),
            ReadKind::Fixed(inner) => inner.get_ref(),
        }
    }

    pub fn get_mut(&mut self) -> &mut T {
        match &mut self.inner {
            ReadKind::Default(inner) => inner.get_mut(),
            ReadKind::Fixed(inner) => inner.get_mut(),
        }
    }

    /// Install fixed retained DATA backing for this decoder.
    pub fn set_receive_pool(&mut self, pool: crate::ReceiveBufferPool) {
        assert!(self.max_frame_size() <= pool.buffer_capacity_bytes());
        self.receive_pool = Some(pool);
    }

    pub(crate) fn set_goaway_pool(&mut self, pool: crate::ReceiveBufferPool) {
        assert!(self.max_frame_size() <= pool.buffer_capacity_bytes());
        self.goaway_pool = Some(pool);
    }

    /// Limit a complete encoded block and each decoded field before allocation.
    pub fn set_max_header_block_size(&mut self, max: usize) {
        self.max_header_block_size = Some(max);
        self.hpack
            .set_max_field_size(self.max_header_list_size, max);
    }

    /// Returns the current max frame size setting
    #[inline]
    pub fn max_frame_size(&self) -> usize {
        match &self.inner {
            ReadKind::Default(inner) => inner.decoder().max_frame_length(),
            ReadKind::Fixed(inner) => inner.max_payload(),
        }
    }

    /// Updates the max frame size setting.
    ///
    /// Must be within 16,384 and 16,777,215.
    #[inline]
    pub fn set_max_frame_size(&mut self, val: usize) {
        if let Some(pool) = &self.receive_pool {
            assert!(val <= pool.buffer_capacity_bytes());
        }
        if let Some(pool) = &self.goaway_pool {
            assert!(val <= pool.buffer_capacity_bytes());
        }
        assert!(DEFAULT_MAX_FRAME_SIZE as usize <= val && val <= MAX_MAX_FRAME_SIZE as usize);
        match &mut self.inner {
            ReadKind::Default(inner) => inner.decoder_mut().set_max_frame_length(val),
            ReadKind::Fixed(inner) => inner.set_max_payload(val),
        }
        // Update max CONTINUATION frames too, since its based on this
        self.max_continuation_frames = calc_max_continuation_frames(self.max_header_list_size, val);
    }

    /// Update the max header list size setting.
    #[inline]
    pub fn set_max_header_list_size(&mut self, val: usize) {
        self.max_header_list_size = val;
        if let Some(max) = self.max_header_block_size {
            self.hpack.set_max_field_size(val, max);
        }
        // Update max CONTINUATION frames too, since its based on this
        self.max_continuation_frames = calc_max_continuation_frames(val, self.max_frame_size());
    }

    /// Update the header table size setting.
    #[inline]
    pub fn set_header_table_size(&mut self, val: usize) {
        self.hpack.queue_size_update(val);
    }
}

fn calc_max_continuation_frames(header_max: usize, frame_max: usize) -> usize {
    // At least this many frames needed to use max header list size
    let min_frames_for_list = (header_max / frame_max).max(1);
    // Some padding for imperfectly packed frames
    // 25% without floats
    let padding = min_frames_for_list >> 2;
    min_frames_for_list.saturating_add(padding).max(5)
}

enum FrameInput<'a> {
    Owned(BytesMut),
    Borrowed(&'a [u8]),
}
impl std::ops::Deref for FrameInput<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Borrowed(bytes) => bytes,
        }
    }
}
impl FrameInput<'_> {
    fn into_owned(self) -> BytesMut {
        match self {
            Self::Owned(bytes) => bytes,
            // Header/CONT workspace still uses the existing owned path; this
            // copy remains outside the fixed raw/DATA/diagnostic grants.
            Self::Borrowed(bytes) => BytesMut::from(bytes),
        }
    }
}
struct DecodePools<'a> {
    data: Option<&'a crate::ReceiveBufferPool>,
    goaway: Option<&'a crate::ReceiveBufferPool>,
    headers: Option<&'a mut crate::receive_header::BoundHeaderBlockBuffer>,
}

/// Decodes a frame.
///
/// This method is intentionally de-generified and outlined because it is very large.
fn decode_frame(
    hpack: &mut hpack::Decoder,
    max_header_list_size: usize,
    max_continuation_frames: usize,
    max_header_block_size: Option<usize>,
    partial_inout: &mut Option<Partial>,
    goaway_pool: Option<&crate::ReceiveBufferPool>,
    bytes: BytesMut,
) -> Result<Option<Frame>, Error> {
    decode_frame_input(
        hpack,
        max_header_list_size,
        max_continuation_frames,
        max_header_block_size,
        partial_inout,
        DecodePools {
            data: None,
            goaway: goaway_pool,
            headers: None,
        },
        FrameInput::Owned(bytes),
    )
}

fn decode_frame_input(
    hpack: &mut hpack::Decoder,
    max_header_list_size: usize,
    max_continuation_frames: usize,
    max_header_block_size: Option<usize>,
    partial_inout: &mut Option<Partial>,
    mut pools: DecodePools<'_>,
    bytes: FrameInput<'_>,
) -> Result<Option<Frame>, Error> {
    let goaway_pool = pools.goaway;
    let span = tracing::trace_span!("FramedRead::decode_frame", offset = bytes.len());
    let _e = span.enter();

    tracing::trace!("decoding frame from {}B", bytes.len());

    // Parse the head
    let head = frame::Head::parse(&bytes);

    if partial_inout.is_some() && head.kind() != Kind::Continuation {
        proto_err!(conn: "expected CONTINUATION, got {:?}", head.kind());
        return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
    }

    let kind = head.kind();

    tracing::trace!(frame.kind = ?kind);

    macro_rules! header_block {
        ($frame:ident, $head:ident, $bytes:ident) => ({
            // Drop the frame header
            $bytes.advance(frame::HEADER_LEN);

            // Parse the header frame w/o parsing the payload
            let (mut frame, mut payload) = match frame::$frame::load($head, $bytes) {
                Ok(res) => res,
                Err(frame::Error::InvalidDependencyId) => {
                    proto_err!(stream: "invalid HEADERS dependency ID");
                    // A stream cannot depend on itself. An endpoint MUST
                    // treat this as a stream error (Section 5.4.2) of type
                    // `PROTOCOL_ERROR`.
                    return Err(Error::library_reset($head.stream_id(), Reason::PROTOCOL_ERROR));
                },
                Err(e) => {
                    proto_err!(conn: "failed to load frame; err={:?}", e);
                    return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
                }
            };

            let encoded_len = payload.len();
            if max_header_block_size.is_some_and(|max| encoded_len > max) {
                return Err(Error::library_go_away(Reason::COMPRESSION_ERROR));
            }
            let is_end_headers = frame.is_end_headers();

            // Load the HPACK encoded headers
            match frame.load_hpack(&mut payload, max_header_list_size, hpack) {
                Ok(_) => {},
                Err(frame::Error::Hpack(hpack::DecoderError::NeedMore(_))) if !is_end_headers => {},
                Err(frame::Error::Hpack(hpack::DecoderError::HeaderFieldTooLarge)) => {
                    return Err(Error::library_go_away(Reason::COMPRESSION_ERROR));
                },
                Err(frame::Error::MalformedMessage) => {
                    let id = $head.stream_id();
                    proto_err!(stream: "malformed header block; stream={:?}", id);
                    return Err(Error::library_reset(id, Reason::PROTOCOL_ERROR));
                },
                Err(e) => {
                    proto_err!(conn: "failed HPACK decoding; err={:?}", e);
                    return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
                }
            }

            if is_end_headers {
                frame.into()
            } else {
                tracing::trace!("loaded partial header block");
                // Defer returning the frame
                *partial_inout = Some(Partial {
                    frame: Continuable::$frame(frame),
                    buf: HeaderInput::Owned(payload),
                    continuation_frames_count: 0,
                    encoded_len,
                });

                return Ok(None);
            }
        });
    }

    macro_rules! fixed_header_block {
        ($frame:ident, $head:ident, $bytes:ident, $buffer:ident) => {{
            let (mut frame, payload) =
                match frame::$frame::load_borrowed($head, &$bytes[frame::HEADER_LEN..]) {
                    Ok(res) => res,
                    Err(frame::Error::InvalidDependencyId) => {
                        return Err(Error::library_reset(
                            $head.stream_id(),
                            Reason::PROTOCOL_ERROR,
                        ))
                    }
                    Err(_) => return Err(Error::library_go_away(Reason::PROTOCOL_ERROR)),
                };
            $buffer.reset();
            let encoded_len = payload.len();
            if max_header_block_size.is_some_and(|max| encoded_len > max)
                || !$buffer.append(payload)
            {
                return Err(Error::library_go_away(Reason::COMPRESSION_ERROR));
            }
            let is_end_headers = frame.is_end_headers();
            let result = $buffer.decode(|src, committed| {
                frame.load_hpack_borrowed(src, committed, max_header_list_size, hpack)
            });
            check_header_decode(result, is_end_headers, $head.stream_id())?;
            if is_end_headers {
                frame.into()
            } else {
                *partial_inout = Some(Partial {
                    frame: Continuable::$frame(frame),
                    buf: HeaderInput::Fixed,
                    continuation_frames_count: 0,
                    encoded_len,
                });
                return Ok(None);
            }
        }};
    }

    let frame = match kind {
        Kind::Settings => {
            let res = frame::Settings::load(head, &bytes[frame::HEADER_LEN..]);

            res.map_err(|e| {
                proto_err!(conn: "failed to load SETTINGS frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::Ping => {
            let res = frame::Ping::load(head, &bytes[frame::HEADER_LEN..]);

            res.map_err(|e| {
                proto_err!(conn: "failed to load PING frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::WindowUpdate => {
            let res = frame::WindowUpdate::load(head, &bytes[frame::HEADER_LEN..]);

            res.map_err(|e| {
                proto_err!(conn: "failed to load WINDOW_UPDATE frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::Data => {
            let res = match pools.data {
                Some(pool) => {
                    frame::Data::load_with_payload(head, &bytes[frame::HEADER_LEN..], |payload| {
                        pool.copy_data(payload)
                    })
                }
                None => {
                    let mut bytes = bytes.into_owned();
                    bytes.advance(frame::HEADER_LEN);
                    frame::Data::load(head, bytes.freeze())
                }
            };

            // TODO: Should this always be connection level? Probably not...
            res.map_err(|e| {
                proto_err!(conn: "failed to load DATA frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::Headers => match pools.headers.as_deref_mut() {
            Some(buffer) => fixed_header_block!(Headers, head, bytes, buffer),
            None => {
                let mut bytes = bytes.into_owned();
                header_block!(Headers, head, bytes)
            }
        },
        Kind::Reset => {
            let res = frame::Reset::load(head, &bytes[frame::HEADER_LEN..]);
            res.map_err(|e| {
                proto_err!(conn: "failed to load RESET frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::GoAway => {
            let payload = &bytes[frame::HEADER_LEN..];
            let res = match goaway_pool {
                Some(pool) if payload.len() >= 8 => {
                    let debug = if payload.len() == 8 {
                        bytes::Bytes::new()
                    } else {
                        pool.try_copy_payload(&payload[8..]).ok_or_else(|| {
                            proto_err!(conn: "retained GO_AWAY diagnostic backing exhausted");
                            Error::library_go_away(Reason::ENHANCE_YOUR_CALM)
                        })?
                    };
                    frame::GoAway::load_with_debug_data(payload, debug)
                }
                _ => frame::GoAway::load(payload),
            };
            res.map_err(|e| {
                proto_err!(conn: "failed to load GO_AWAY frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::PushPromise => match pools.headers.as_deref_mut() {
            Some(buffer) => fixed_header_block!(PushPromise, head, bytes, buffer),
            None => {
                let mut bytes = bytes.into_owned();
                header_block!(PushPromise, head, bytes)
            }
        },
        Kind::Priority => {
            if head.stream_id() == 0 {
                // Invalid stream identifier
                proto_err!(conn: "invalid stream ID 0");
                return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
            }

            match frame::Priority::load(head, &bytes[frame::HEADER_LEN..]) {
                Ok(frame) => frame.into(),
                Err(frame::Error::InvalidDependencyId) => {
                    // A stream cannot depend on itself. An endpoint MUST
                    // treat this as a stream error (Section 5.4.2) of type
                    // `PROTOCOL_ERROR`.
                    let id = head.stream_id();
                    proto_err!(stream: "PRIORITY invalid dependency ID; stream={:?}", id);
                    return Err(Error::library_reset(id, Reason::PROTOCOL_ERROR));
                }
                Err(e) => {
                    proto_err!(conn: "failed to load PRIORITY frame; err={:?};", e);
                    return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
                }
            }
        }
        Kind::Continuation => {
            let is_end_headers = (head.flag() & 0x4) == 0x4;

            let mut partial = match partial_inout.take() {
                Some(partial) => partial,
                None => {
                    proto_err!(conn: "received unexpected CONTINUATION frame");
                    return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
                }
            };

            // The stream identifiers must match
            if partial.frame.stream_id() != head.stream_id() {
                proto_err!(conn: "CONTINUATION frame stream ID does not match previous frame stream ID");
                return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
            }

            // Check for CONTINUATION flood
            if is_end_headers {
                partial.continuation_frames_count = 0;
            } else {
                let cnt = partial.continuation_frames_count + 1;
                if cnt > max_continuation_frames {
                    tracing::debug!("too_many_continuations, max = {}", max_continuation_frames);
                    return Err(Error::library_go_away_data(
                        Reason::ENHANCE_YOUR_CALM,
                        "too_many_continuations",
                    ));
                } else {
                    partial.continuation_frames_count = cnt;
                }
            }

            if let Some(max) = max_header_block_size {
                let added = bytes.len() - frame::HEADER_LEN;
                let Some(total) = partial.encoded_len.checked_add(added).filter(|n| *n <= max)
                else {
                    // Refuse before extending even when previous fields fit the list.
                    return Err(Error::library_go_away(Reason::COMPRESSION_ERROR));
                };
                partial.encoded_len = total;
            }

            let result = match &mut partial.buf {
                HeaderInput::Fixed => {
                    let buffer = pools
                        .headers
                        .as_deref_mut()
                        .expect("fixed partial header buffer installed");
                    if !buffer.append(&bytes[frame::HEADER_LEN..]) {
                        return Err(Error::library_go_away(Reason::COMPRESSION_ERROR));
                    }
                    buffer.decode(|src, committed| {
                        partial.frame.load_hpack_borrowed(
                            src,
                            committed,
                            max_header_list_size,
                            hpack,
                        )
                    })
                }
                HeaderInput::Owned(buf) => {
                    let mut bytes = bytes.into_owned();
                    // Extend the buf
                    if buf.is_empty() {
                        *buf = bytes.split_off(frame::HEADER_LEN);
                    } else {
                        if partial.frame.is_over_size() {
                            // If there was left over bytes previously, they may be
                            // needed to continue decoding, even though we will
                            // be ignoring this frame. This is done to keep the HPACK
                            // decoder state up-to-date.
                            //
                            // Still, we need to be careful, because if a malicious
                            // attacker were to try to send a gigantic string, such
                            // that it fits over multiple header blocks, we could
                            // grow memory uncontrollably again, and that'd be a shame.
                            //
                            // Instead, we use a simple heuristic to determine if
                            // we should continue to ignore decoding, or to tell
                            // the attacker to go away.
                            if buf.len() + bytes.len() > max_header_list_size {
                                proto_err!(conn: "CONTINUATION frame header block size over ignorable limit");
                                return Err(Error::library_go_away(Reason::COMPRESSION_ERROR));
                            }
                        }
                        buf.extend_from_slice(&bytes[frame::HEADER_LEN..]);
                    }

                    partial.frame.load_hpack(buf, max_header_list_size, hpack)
                }
            };
            check_header_decode(result, is_end_headers, head.stream_id())?;

            if is_end_headers {
                partial.frame.into()
            } else {
                *partial_inout = Some(partial);
                return Ok(None);
            }
        }
        Kind::Unknown => {
            // Unknown frames are ignored
            return Ok(None);
        }
    };

    Ok(Some(frame))
}

fn check_header_decode(
    result: Result<(), frame::Error>,
    end: bool,
    id: frame::StreamId,
) -> Result<(), Error> {
    match result {
        Ok(()) => Ok(()),
        Err(frame::Error::Hpack(hpack::DecoderError::NeedMore(_))) if !end => Ok(()),
        Err(frame::Error::Hpack(hpack::DecoderError::HeaderFieldTooLarge)) => {
            Err(Error::library_go_away(Reason::COMPRESSION_ERROR))
        }
        Err(frame::Error::MalformedMessage) => {
            Err(Error::library_reset(id, Reason::PROTOCOL_ERROR))
        }
        Err(_) => Err(Error::library_go_away(Reason::PROTOCOL_ERROR)),
    }
}

impl<T> Stream for FramedRead<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<Frame, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let span = tracing::trace_span!("FramedRead::poll_next");
        let _e = span.enter();
        loop {
            tracing::trace!("poll");
            if let Some(pool) = &self.receive_pool {
                ready!(pool.poll_ready(cx));
            }
            let Self {
                inner,
                hpack,
                max_header_list_size,
                partial,
                max_continuation_frames,
                max_header_block_size,
                goaway_pool,
                receive_pool,
                header_buffer,
                ..
            } = &mut *self;
            let (decoded, needs_data_copy) = match inner {
                ReadKind::Default(inner) => {
                    let bytes = match ready!(Pin::new(inner).poll_next(cx)) {
                        Some(Ok(bytes)) => bytes,
                        Some(Err(e)) => return Poll::Ready(Some(Err(map_err(e)))),
                        None => return Poll::Ready(None),
                    };
                    (
                        decode_frame(
                            hpack,
                            *max_header_list_size,
                            *max_continuation_frames,
                            *max_header_block_size,
                            partial,
                            goaway_pool.as_ref(),
                            bytes,
                        ),
                        true,
                    )
                }
                ReadKind::Fixed(inner) => {
                    let decoded = inner.poll_frame_with(cx, |bytes| {
                        decode_frame_input(
                            hpack,
                            *max_header_list_size,
                            *max_continuation_frames,
                            *max_header_block_size,
                            partial,
                            DecodePools {
                                data: receive_pool.as_ref(),
                                goaway: goaway_pool.as_ref(),
                                headers: header_buffer.as_mut(),
                            },
                            FrameInput::Borrowed(bytes),
                        )
                    });
                    let decoded = match ready!(decoded) {
                        Some(Ok(decoded)) => decoded,
                        Some(Err(e)) => return Poll::Ready(Some(Err(map_err(e)))),
                        None => return Poll::Ready(None),
                    };
                    (decoded, false)
                }
            };
            if let Some(mut frame) = decoded? {
                if let (true, Some(pool), Frame::Data(data)) =
                    (needs_data_copy, &self.receive_pool, &mut frame)
                {
                    // Replace the whole original read-buffer alias before it
                    // escapes. Flow credit is independent of this pool slot.
                    let owned = pool.copy_data(data.payload());
                    *data.payload_mut() = owned;
                }
                tracing::debug!(?frame, "received");
                return Poll::Ready(Some(Ok(frame)));
            }
        }
    }
}

impl<T> Drop for FramedRead<T> {
    fn drop(&mut self) {
        if let Some(pool) = &self.receive_pool {
            pool.detach_waker();
        }
    }
}

fn map_err(err: io::Error) -> Error {
    if let io::ErrorKind::InvalidData = err.kind() {
        if let Some(custom) = err.get_ref() {
            if custom.is::<LengthDelimitedCodecError>()
                || custom.is::<crate::receive_frame::FrameSizeExceeded>()
            {
                return Error::library_go_away(Reason::FRAME_SIZE_ERROR);
            }
        }
    }
    err.into()
}

// ===== impl Continuable =====

impl Continuable {
    fn stream_id(&self) -> frame::StreamId {
        match *self {
            Continuable::Headers(ref h) => h.stream_id(),
            Continuable::PushPromise(ref p) => p.stream_id(),
        }
    }

    fn is_over_size(&self) -> bool {
        match *self {
            Continuable::Headers(ref h) => h.is_over_size(),
            Continuable::PushPromise(ref p) => p.is_over_size(),
        }
    }

    fn load_hpack_borrowed(
        &mut self,
        src: &[u8],
        committed: &mut usize,
        max: usize,
        decoder: &mut hpack::Decoder,
    ) -> Result<(), frame::Error> {
        match self {
            Self::Headers(frame) => frame.load_hpack_borrowed(src, committed, max, decoder),
            Self::PushPromise(frame) => frame.load_hpack_borrowed(src, committed, max, decoder),
        }
    }

    fn load_hpack(
        &mut self,
        src: &mut BytesMut,
        max_header_list_size: usize,
        decoder: &mut hpack::Decoder,
    ) -> Result<(), frame::Error> {
        match *self {
            Continuable::Headers(ref mut h) => h.load_hpack(src, max_header_list_size, decoder),
            Continuable::PushPromise(ref mut p) => p.load_hpack(src, max_header_list_size, decoder),
        }
    }
}

impl<T> From<Continuable> for Frame<T> {
    fn from(cont: Continuable) -> Self {
        match cont {
            Continuable::Headers(mut headers) => {
                headers.set_end_headers();
                headers.into()
            }
            Continuable::PushPromise(mut push) => {
                push.set_end_headers();
                push.into()
            }
        }
    }
}
